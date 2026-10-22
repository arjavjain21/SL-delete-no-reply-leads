const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');

async function simulate(t, options = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'drain-controller-'));
  t.after(() => fs.rmSync(root, {recursive: true, force: true}));
  const output = path.join(root, 'run-output');
  fs.mkdirSync(output);
  const events = [];
  const errors = [];
  const dispatches = [];
  const read = name => JSON.parse(fs.readFileSync(path.join(output, name), 'utf8'));
  const write = (name, value) => fs.writeFileSync(path.join(output, name), JSON.stringify(value));
  class Artifact {
    async uploadArtifact(name, files, directory) {
      events.push('upload');
      assert.equal(directory, output);
      assert.ok(files.some(file => file.endsWith('backup.csv')));
      if (options.uploadFails) throw new Error('Upload unavailable');
      return {id: 123};
    }
  }
  const spawnSync = (binary, args) => {
    assert.equal(binary, 'python');
    const command = args[1];
    events.push(command);
    if (command === 'plan') {
      write('drain-state.json', {done: false, batch_number: 0, confirmed_after: 17,
        deadline: Date.now() / 1000 + (options.expired ? -1 : 3600),
        stats: {leads_deleted_success: 0}});
    } else if (command === 'prepare') {
      const state = read('drain-state.json');
      state.done = true;
      state.batch_number = 1;
      write('drain-state.json', state);
      write('batch.json', {number: 1, backup_file: 'backup.csv', rows: 2});
      fs.writeFileSync(path.join(output, 'backup.csv'), 'id,reply_count\n1,0\n2,0\n');
    } else if (command === 'execute') {
      assert.ok(events.indexOf('upload') < events.indexOf('execute'));
      assert.ok(args.includes('123'));
      write('batch.json', {number: 1, finished: !options.partial, deleted: options.partial ? 1 : 2});
      const state = read('drain-state.json');
      state.stats.leads_deleted_success = options.partial ? 1 : 2;
      state.confirmed_after = options.partial ? 17 : 42;
      write('drain-state.json', state);
    } else if (command === 'report') {
      const status = args[args.indexOf('--status') + 1];
      write('summary.json', {status});
    }
    return {status: 0};
  };
  const core = {info() {}, notice() {}, error(message) {errors.push(message);},
    setFailed(message) {errors.push(message);},
    summary: {addHeading() {return this;}, addCodeBlock() {return this;}, async write() {}}};
  const github = {rest: {actions: {async createWorkflowDispatch(request) {dispatches.push(request);}}}};
  const context = {runId: 99, repo: {owner: 'owner', repo: 'repo'}};
  const sandbox = {module: {exports: {}}, process: {env: {GITHUB_WORKSPACE: root,
    RUNNER_TEMP: root, GITHUB_RUN_ATTEMPT: '1', DRY_RUN: options.dry ? 'true' : 'false',
    SAMPLE_CAMPAIGNS: options.sample ? '25' : '0'}},
    require(name) {
      if (name === 'node:fs') return fs;
      if (name === 'node:path') return path;
      if (name === 'node:child_process') return {spawnSync};
      if (name.endsWith('@actions/artifact')) return {DefaultArtifactClient: Artifact};
      throw new Error(`Unexpected dependency ${name}`);
    }};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../scripts/drain.cjs'), 'utf8'), sandbox);
  await sandbox.module.exports({core, github, context});
  return {events, errors, dispatches, summary: read('summary.json')};
}

test('upload completes before execution; finished cleanup queues no continuation', async t => {
  const result = await simulate(t);
  assert.deepEqual(result.events, ['plan', 'prepare', 'upload', 'execute', 'report']);
  assert.equal(result.summary.status, 'complete');
  assert.equal(result.dispatches.length, 0);
  assert.equal(result.errors.length, 0);
});

test('upload failure prevents all deletion and sends a failure report', async t => {
  const result = await simulate(t, {uploadFails: true});
  assert.ok(!result.events.includes('execute'));
  assert.equal(result.summary.status, 'failed');
  assert.equal(result.dispatches.length, 0);
  assert.ok(result.errors.length > 0);
});

test('expired work budget queues the last confirmed campaign cursor', async t => {
  const result = await simulate(t, {expired: true});
  assert.equal(result.dispatches.length, 1);
  assert.equal(result.dispatches[0].inputs.resume_after_campaign, '17');
  assert.equal(result.dispatches[0].inputs.dry_run, 'false');
  assert.equal(result.summary.status, 'continuing');
});

test('partial deletion resumes before the unfinished campaign', async t => {
  const result = await simulate(t, {partial: true});
  assert.equal(result.dispatches[0].inputs.resume_after_campaign, '17');
  assert.equal(result.summary.status, 'continuing');
});

test('an incomplete sample never expands into a full-account continuation', async t => {
  const result = await simulate(t, {expired: true, dry: true, sample: true});
  assert.equal(result.dispatches.length, 0);
  assert.ok(!result.events.includes('execute'));
  assert.equal(result.summary.status, 'failed');
});
