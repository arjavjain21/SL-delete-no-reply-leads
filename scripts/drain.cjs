// One workflow at a time shares the account's request budget. Upload each
// immutable backup before Python is allowed to execute that batch.
const fs = require('node:fs');
const path = require('node:path');
const {spawnSync} = require('node:child_process');

module.exports = async ({core, github, context}) => {
  const root = process.env.GITHUB_WORKSPACE;
  const output = path.join(root, 'run-output');
  fs.mkdirSync(output, {recursive: true});
  const helper = path.join(root, 'scripts', 'drain.py');
  const read = name => JSON.parse(fs.readFileSync(path.join(output, name), 'utf8'));
  const run = (...args) => {
    const child = spawnSync('python', [helper, ...args], {cwd: output, env: process.env, stdio: 'inherit'});
    if (child.error || child.status !== 0) throw new Error(`Cleanup ${args[0]} failed (exit ${child.status})`);
  };
  const dryRun = process.env.DRY_RUN === 'true';
  const sample = Number(process.env.SAMPLE_CAMPAIGNS || 0);
  const resume = Number(process.env.RESUME_AFTER_CAMPAIGN || 0);
  const budget = Number(process.env.CLEANUP_BUDGET_MINUTES || 120);
  const batchSize = Number(process.env.DELETE_BATCH_SIZE || 10000);
  const {DefaultArtifactClient} = require(path.join(process.env.RUNNER_TEMP,
    'smartlead-artifact', 'node_modules', '@actions', 'artifact'));
  const artifacts = new DefaultArtifactClient();
  let status = 'failed';
  try {
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
      }
      if (read('drain-state.json').done) break;
    }
    const remaining = read('drain-state.json');
    const batchPath = path.join(output, 'batch.json');
    const unfinishedBatch = fs.existsSync(batchPath) && read('batch.json').finished === false;
    if (!remaining.done || unfinishedBatch) {
      if (dryRun && sample > 0) throw new Error('Sample dry run exceeded its work budget; no broader continuation queued');
      await github.rest.actions.createWorkflowDispatch({owner: context.repo.owner,
        repo: context.repo.repo, workflow_id: 'smartlead.yml', ref: 'main', inputs: {
          mode: 'cleanup', cleanup_scope: 'all', dry_run: String(dryRun),
          sample_campaigns: '0', resume_after_campaign: String(remaining.confirmed_after)
        }});
      status = 'continuing';
      core.notice(`Continuation queued after campaign ${remaining.confirmed_after}`);
    } else {
      status = dryRun ? 'dry-run' : 'complete';
    }
    run('report', '--status', status);
  } catch (error) {
    core.setFailed(error.message);
    if (fs.existsSync(path.join(output, 'drain-state.json'))) {
      try { run('report', '--status', 'failed'); }
      catch (reportError) { core.error(reportError.message); }
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
