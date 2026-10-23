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
  const waits = [];
  const environments = [];
  let prepares = 0;
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
  const spawnSync = (binary, args, settings) => {
    assert.equal(binary, 'python');
    const command = args[1];
    events.push(command);
    environments.push(settings.env);
    if (command === 'prepare') prepares += 1;
    if (options.throttleAfterBatch && command === 'prepare' && prepares === 2) {
      write('rate-limit.json', {retry_after_seconds: 60});
      return {status: 75};
    }
    if (command === options.rateLimitedAt) {
      if (command === 'execute') {
        const state = read('drain-state.json');
        state.stats.leads_deleted_success = 1;
        write('drain-state.json', state);
      }
      write('rate-limit.json', {retry_after_seconds: options.serverDelay || 60});
      return {status: 75};
    }
    if (command === 'execute' && options.executeFails) return {status: 1};
    if (command === 'plan') {
      write('drain-state.json', {done: false, batch_number: 0, confirmed_after: 17,
        deadline: Date.now() / 1000 + (options.expired ? -1 : 3600),
        stats: {leads_deleted_success: 0}});
    } else if (command === 'prepare') {
      const state = read('drain-state.json');
      state.done = !options.throttleAfterBatch;
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
  const github = {rest: {actions: {async createWorkflowDispatch(request) {
    dispatches.push(request);
    if (options.dispatchFails) throw new Error('Continuation dispatch unavailable');
  }}}};
  const context = {runId: 99, repo: {owner: 'owner', repo: 'repo'}};
  const sandbox = {module: {exports: {}}, process: {env: {GITHUB_WORKSPACE: root,
    RUNNER_TEMP: root, GITHUB_RUN_ATTEMPT: '1', DRY_RUN: options.dry ? 'true' : 'false',
    SAMPLE_CAMPAIGNS: options.sample ? '25' : '0', RESUME_AFTER_CAMPAIGN: '17',
    RATE_LIMIT_RETRIES: String(options.retries || 0), RESUME_NOT_BEFORE: String(options.notBefore || 0),
    RESUME_REQUEST_LIMIT: String(options.requestLimit || 0),
    SMARTLEAD_REQUESTS_PER_MINUTE: String(options.configuredLimit || 5400)}},
    require(name) {
      if (name === 'node:fs') return fs;
      if (name === 'node:path') return path;
      if (name === 'node:child_process') return {spawnSync};
      if (name === 'node:timers/promises') return {async setTimeout(ms) {waits.push(ms);}};
      if (name.endsWith('@actions/artifact')) return {DefaultArtifactClient: Artifact};
      throw new Error(`Unexpected dependency ${name}`);
    }};
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../scripts/drain.cjs'), 'utf8'), sandbox);
  await sandbox.module.exports({core, github, context});
  return {events, errors, dispatches, waits, environments, summary: read('summary.json')};
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

test('persistent 429 queues a delayed slower retry without skipping the partial batch', async t => {
  const start = Date.now() / 1000;
  const result = await simulate(t, {rateLimitedAt: 'execute'});
  assert.equal(result.dispatches.length, 1);
  const inputs = result.dispatches[0].inputs;
  assert.equal(inputs.resume_after_campaign, '17');
  assert.equal(inputs.resume_request_limit, '600');
  assert.equal(inputs.rate_limit_retries, '1');
  assert.ok(Number(inputs.resume_not_before) >= start + 300);
  assert.equal(result.summary.status, 'rate-limited');
  assert.equal(result.errors.length, 0);
  assert.ok(result.events.indexOf('upload') < result.events.indexOf('execute'));
});

test('a server cooldown longer than five minutes is never shortened', async t => {
  const start = Date.now() / 1000;
  const result = await simulate(t, {rateLimitedAt: 'prepare', serverDelay: 900});
  assert.ok(Number(result.dispatches[0].inputs.resume_not_before) >= start + 900);
  assert.ok(!result.events.includes('execute'));
});

test('a throttled campaign fetch resumes the original cursor without requiring state', async t => {
  const result = await simulate(t, {rateLimitedAt: 'plan', retries: 1, requestLimit: 600});
  assert.equal(result.dispatches[0].inputs.resume_after_campaign, '17');
  assert.equal(result.dispatches[0].inputs.rate_limit_retries, '2');
  assert.equal(result.dispatches[0].inputs.resume_request_limit, '300');
  assert.equal(result.summary.status, 'rate-limited');
});

test('a retry honors the cooldown before fetching and reduces concurrency', async t => {
  const result = await simulate(t, {notBefore: Math.ceil(Date.now() / 1000) + 30,
    requestLimit: 600, retries: 1});
  assert.equal(result.waits.length, 1);
  assert.ok(result.waits[0] > 0 && result.waits[0] <= 31000);
  assert.equal(result.environments[0].SMARTLEAD_REQUESTS_PER_MINUTE, '600');
  assert.equal(result.environments[0].DELETE_WORKERS, '4');
  assert.equal(result.environments[0].ANALYSIS_WORKERS, '4');
});

test('three consecutive cooldown retries stop the chain and report failure', async t => {
  const result = await simulate(t, {rateLimitedAt: 'execute', retries: 3});
  assert.equal(result.dispatches.length, 0);
  assert.equal(result.summary.status, 'failed');
  assert.ok(result.errors.some(message => message.includes('3 cooldown continuations')));
});

test('a cooldown beyond the runner window stops instead of shortening the server delay', async t => {
  const result = await simulate(t, {rateLimitedAt: 'execute', serverDelay: 3600});
  assert.equal(result.dispatches.length, 0);
  assert.equal(result.summary.status, 'failed');
  assert.ok(result.errors.some(message => message.includes('30-minute window')));
});

test('a sampled dry run never turns throttling into a full-account continuation', async t => {
  const result = await simulate(t, {rateLimitedAt: 'prepare', dry: true, sample: true});
  assert.equal(result.dispatches.length, 0);
  assert.equal(result.summary.status, 'failed');
});

test('ordinary deletion errors still fail without queuing a cooldown retry', async t => {
  const result = await simulate(t, {executeFails: true});
  assert.equal(result.dispatches.length, 0);
  assert.equal(result.summary.status, 'failed');
  assert.ok(result.errors.length > 0);
});

test('a failed dispatch cannot be reported as a queued continuation', async t => {
  const result = await simulate(t, {rateLimitedAt: 'execute', dispatchFails: true});
  assert.equal(result.summary.status, 'failed');
  assert.ok(result.errors.some(message => message.includes('dispatch unavailable')));
});

test('recovery never raises the configured request cap', async t => {
  const result = await simulate(t, {rateLimitedAt: 'execute', configuredLimit: 120, requestLimit: 600});
  assert.equal(result.environments[0].SMARTLEAD_REQUESTS_PER_MINUTE, '120');
  assert.equal(result.dispatches[0].inputs.resume_request_limit, '60');
});

test('healthy work-budget continuations gradually restore the request cap', async t => {
  const result = await simulate(t, {expired: true, requestLimit: 600});
  assert.equal(result.dispatches[0].inputs.resume_request_limit, '1200');
  assert.equal(result.dispatches[0].inputs.rate_limit_retries, '0');
});

test('a completed live batch resets the consecutive throttle streak', async t => {
  const result = await simulate(t, {throttleAfterBatch: true, retries: 3, requestLimit: 300});
  assert.equal(result.dispatches.length, 1);
  assert.equal(result.dispatches[0].inputs.rate_limit_retries, '1');
  assert.equal(result.dispatches[0].inputs.resume_after_campaign, '42');
  assert.equal(result.summary.status, 'rate-limited');
});
