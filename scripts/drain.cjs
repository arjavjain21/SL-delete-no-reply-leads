// One workflow at a time shares the account's request budget. Upload each
// immutable backup before Python is allowed to execute that batch.
const fs = require('node:fs');
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const {setTimeout: delay} = require('node:timers/promises');

module.exports = async ({core, github, context}) => {
  const root = process.env.GITHUB_WORKSPACE;
  const output = path.join(root, 'run-output');
  fs.mkdirSync(output, {recursive: true});
  const helper = path.join(root, 'scripts', 'drain.py');
  const read = name => JSON.parse(fs.readFileSync(path.join(output, name), 'utf8'));
  const write = (name, value) => fs.writeFileSync(path.join(output, name), JSON.stringify(value, null, 2));
  const configuredLimit = Number(process.env.SMARTLEAD_REQUESTS_PER_MINUTE || 5400);
  const resumeLimit = Number(process.env.RESUME_REQUEST_LIMIT || 0);
  const requestLimit = Math.min(configuredLimit, resumeLimit || configuredLimit);
  const run = (...args) => {
    if (args[0] !== 'report') fs.rmSync(path.join(output, 'rate-limit.json'), {force: true});
    const env = {...process.env, SMARTLEAD_REQUESTS_PER_MINUTE: String(requestLimit)};
    if (resumeLimit && requestLimit <= 600) {
      env.DELETE_WORKERS = String(Math.min(Number(env.DELETE_WORKERS || 10), 4));
      env.ANALYSIS_WORKERS = String(Math.min(Number(env.ANALYSIS_WORKERS || 16), 4));
    }
    const child = spawnSync('python', [helper, ...args], {cwd: output, env, stdio: 'inherit'});
    if (child.error || child.status !== 0) {
      const error = new Error(`Cleanup ${args[0]} failed (exit ${child.status})`);
      error.rateLimited = !child.error && child.status === 75 && fs.existsSync(path.join(output, 'rate-limit.json'));
      throw error;
    }
  };
  const dryRun = process.env.DRY_RUN === 'true';
  const sample = Number(process.env.SAMPLE_CAMPAIGNS || 0);
  const resume = Number(process.env.RESUME_AFTER_CAMPAIGN || 0);
  const budget = Number(process.env.CLEANUP_BUDGET_MINUTES || 120);
  const batchSize = Number(process.env.DELETE_BATCH_SIZE || 10000);
  const notBefore = Number(process.env.RESUME_NOT_BEFORE || 0);
  let cooldownRetries = Number(process.env.RATE_LIMIT_RETRIES || 0);
  const {DefaultArtifactClient} = require(path.join(process.env.RUNNER_TEMP,
    'smartlead-artifact', 'node_modules', '@actions', 'artifact'));
  const artifacts = new DefaultArtifactClient();
  const queue = async (cursor, cooldown = {}) => {
    await github.rest.actions.createWorkflowDispatch({owner: context.repo.owner,
      repo: context.repo.repo, workflow_id: 'smartlead.yml', ref: 'main', inputs: {
        mode: 'cleanup', cleanup_scope: 'all', dry_run: String(dryRun),
        sample_campaigns: '0', resume_after_campaign: String(cursor),
        resume_not_before: String(cooldown.not_before || 0),
        rate_limit_retries: String(cooldown.rate_limit_retries || 0),
        resume_request_limit: String(cooldown.request_limit || requestLimit)
      }});
  };
  let status = 'failed';
  try {
    if (!Number.isInteger(configuredLimit) || configuredLimit < 1 || configuredLimit > 6000 ||
        !Number.isInteger(resumeLimit) || resumeLimit < 0 || resumeLimit > 6000 ||
        !Number.isInteger(cooldownRetries) || cooldownRetries < 0 || cooldownRetries > 3 ||
        !Number.isInteger(notBefore) || notBefore < 0) throw new Error('Invalid rate-limit continuation settings');
    const cooldownMs = Math.max(0, notBefore * 1000 - Date.now());
    if (cooldownMs > 1800 * 1000) throw new Error('Requested cooldown exceeds the automatic 30-minute window');
    if (cooldownMs) {
      core.notice(`Waiting for SmartLead cooldown until ${new Date(notBefore * 1000).toISOString()}`);
      await delay(cooldownMs);
    }
    const args = ['plan', '--resume-after', String(resume), '--sample-campaigns', String(sample),
      '--budget-minutes', String(budget)];
    if (dryRun) args.push('--dry-run');
    run(...args);
    while (true) {
      const current = read('drain-state.json');
      if (current.done || Date.now() / 1000 >= current.deadline || current.batch_number >= 400) break;
      // Verify the live path promptly with a small first batch, then use the
      // configured working size for the rest of this unbounded cleanup.
      run('prepare', '--batch-size', String(current.batch_number === 0 ? Math.min(1000, batchSize) : batchSize));
      let batch = read('batch.json');
      if (batch.backup_file) {
        const name = `smartlead-backup-${context.runId}-${process.env.GITHUB_RUN_ATTEMPT}-${batch.number}-after-${current.stats.leads_deleted_success}`;
        const result = await artifacts.uploadArtifact(name,
          [path.join(output, batch.backup_file), path.join(output, 'batch.json')], output, {retentionDays: 30});
        if (!result.id) throw new Error('Backup upload returned no artifact ID; deletion refused');
        core.info(`Backup uploaded for batch ${batch.number}: artifact ${result.id}; ${batch.rows} candidates`);
        run('execute', '--artifact-id', String(result.id));
        batch = read('batch.json');
        core.notice(`Batch ${batch.number}: ${batch.deleted} deleted; total this run ${read('drain-state.json').stats.leads_deleted_success}${dryRun ? ' (dry run)' : ''}`);
        if (!batch.finished) break;
        // Only a completed live batch breaks a consecutive-throttle streak.
        if (!dryRun && batch.deleted > 0) cooldownRetries = 0;
      }
      if (read('drain-state.json').done) break;
    }
    const remaining = read('drain-state.json');
    const batchPath = path.join(output, 'batch.json');
    const unfinishedBatch = fs.existsSync(batchPath) && read('batch.json').finished === false;
    if (!remaining.done || unfinishedBatch) {
      if (dryRun && sample > 0) throw new Error('Sample dry run exceeded its work budget; no broader continuation queued');
      // Recover throughput gradually after a full healthy work budget.
      await queue(remaining.confirmed_after, {rate_limit_retries: cooldownRetries,
        request_limit: Math.min(configuredLimit, requestLimit * 2)});
      status = 'continuing';
      core.notice(`Continuation queued after campaign ${remaining.confirmed_after}`);
    } else {
      status = dryRun ? 'dry-run' : 'complete';
    }
    run('report', '--status', status);
  } catch (error) {
    try {
      if (error.rateLimited && !(dryRun && sample > 0) && cooldownRetries < 3) {
        const throttle = read('rate-limit.json');
        const seconds = Math.max(300 * 2 ** cooldownRetries, Number(throttle.retry_after_seconds));
        if (!Number.isFinite(seconds) || seconds > 1800) {
          throw new Error('SmartLead cooldown exceeds the automatic 30-minute window; cleanup remains stopped');
        }
        const cursor = fs.existsSync(path.join(output, 'drain-state.json'))
          ? read('drain-state.json').confirmed_after : resume;
        const continuation = {not_before: Math.ceil(Date.now() / 1000 + seconds),
          rate_limit_retries: cooldownRetries + 1,
          request_limit: Math.max(1, Math.min(600, Math.floor(requestLimit / 2)))};
        await queue(cursor, continuation);
        write('continuation.json', continuation);
        status = 'rate-limited';
        core.notice(`SmartLead HTTP 429: slower continuation queued after campaign ${cursor}; cooldown ${seconds}s`);
        run('report', '--status', status);
      } else {
        const reason = error.rateLimited && cooldownRetries >= 3
          ? 'SmartLead HTTP 429 persisted through 3 cooldown continuations; cleanup remains stopped' : error.message;
        core.setFailed(reason);
        if (fs.existsSync(path.join(output, 'drain-state.json')) || error.rateLimited) run('report', '--status', 'failed');
      }
    } catch (recoveryError) {
      core.setFailed(recoveryError.message);
      if (fs.existsSync(path.join(output, 'drain-state.json')) || error.rateLimited) {
        try { run('report', '--status', 'failed'); }
        catch (reportError) { core.error(reportError.message); }
      }
    }
  } finally {
    const summaryPath = path.join(output, 'summary.json');
    if (fs.existsSync(summaryPath)) {
      const summary = JSON.parse(fs.readFileSync(summaryPath, 'utf8'));
      await core.summary.addHeading('SmartLead cleanup').addCodeBlock(JSON.stringify(summary, null, 2), 'json').write();
    }
    // Batch backups already reside in immutable artifacts; exclude cached exports
    // from the final log artifact to keep its size independent of account size.
    fs.rmSync(path.join(output, 'exports'), {recursive: true, force: true});
  }
};
