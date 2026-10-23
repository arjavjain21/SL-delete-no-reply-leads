# SmartLead cleanup with Resend

A daily GitHub Actions job deletes **all** no-reply leads from PAUSED or COMPLETED
campaigns created at least 30 days ago, excluding client 1598, and reports through
Resend. The age rule applies to the campaign, so newer leads within an old
campaign can qualify. Cleanup is performed in backed-up batches with automatic
continuation; it has no total lead target in the default `all` scope.
Gmail SMTP credentials are no longer used.

## Configure GitHub Actions

In this repository, open **Settings → Secrets and variables → Actions**.
Create or retain these repository secrets:

| Secret | Purpose |
| --- | --- |
| `SMARTLEAD_API_KEY` | Existing SmartLead account API key |
| `RESEND_API_KEY` | Resend API key with permission to send from your verified domain |

The workflow defaults to sender
`SmartLead Reports <reports@clientupdates.eagleinfoservice.com>` and recipient
`arjav@eagleinfoservice.com`. Verify `clientupdates.eagleinfoservice.com` in
Resend before running. Resend permits any sender local part on a verified domain;
creating a separate mailbox for `reports` is not required for sending.
Optional Actions **variables** `SENDER_EMAIL` and `RECIPIENT_EMAILS` override these
addresses. The workflow no longer reads the old email-address secrets, so an old
Gmail sender secret cannot accidentally override the Resend domain.

Store credentials only in secrets, never in source or workflow YAML. Rotate any
key previously shared in chat. `EMAIL_APP_PASSWORD` is no longer referenced and
can be removed after migration. A Gmail sender address cannot be used through
Resend unless its sending domain is verified; use an address on your own domain.

Actions variables are optional; empty or missing values use these defaults:

| Variable | Default | Meaning |
| --- | --- | --- |
| `TARGET_LEADS` | `20000` | Used only by optional legacy `target` scope; ignored by default `all` cleanup |
| `DAYS_WITHOUT_ACTIVITY` | `30` | Existing behavior: minimum **campaign age since creation**, not last activity |
| `EXCLUDE_CLIENT_IDS` | `1598` | Comma-separated client IDs excluded from cleanup |
| `SMARTLEAD_REQUESTS_PER_MINUTE` | `5400` | Shared limit for all SmartLead attempts, including retries; maximum `6000` |
| `DELETE_WORKERS` | `10` | Concurrent deletion workers; range `1`–`32` |
| `ANALYSIS_WORKERS` | `16` | Concurrent campaign exports/analysis; range `1`–`32` |
| `DELETE_BATCH_SIZE` | `10000` | Maximum candidate rows per backup/deletion batch, range `1`–`50000`; not a total cleanup limit |
| `CLEANUP_BUDGET_MINUTES` | `120` | Work budget before automatic continuation; range `1`–`140` |

The rate limit applies to this process. Other jobs or applications using the same
SmartLead account share its 6,000 requests/minute allowance; lower this setting
when necessary. Actual throughput also depends on response time and server
cooldowns. Retries honor `Retry-After` and use exponential backoff. Permanent
errors are not repeatedly retried, and authentication errors abort the run.
If SmartLead still returns HTTP 429 after those attempts, or requests a cooldown
of at least five minutes, the default `all` cleanup keeps its confirmed cursor
and queues a slower continuation after a
5-, 10-, or 20-minute cooldown. It never shortens `Retry-After`. The first
recovery run uses at most 600 requests/minute and four workers; further throttling
halves that cap. Successful work-budget continuations gradually restore the
configured cap. Throttled, unfinished campaigns are re-exported instead of
skipped, with a new durable backup before deletion. Concurrent successful
deletions are counted even when another worker is throttled.

Three consecutive cooldown continuations without a completed live batch stop
the chain and report failure; a server cooldown beyond 30 minutes also stops it
instead of retrying early. A `rate-limited` email means a delayed continuation
was queued, while `failed` means cleanup stopped. The counters distinguish
deferred HTTP 429 requests from permanent deletion failures. Other applications
can consume the account allowance even while this workflow stays below its cap.

## Start and verify

After the workflow changes are on the default branch:

1. Open **Actions → SmartLead Lead Deletion → Run workflow**. Choose
   `mode: email-test`. This sends one connection-test email through Resend and
   does not access SmartLead or delete anything.
2. Run again with `mode: cleanup`, `cleanup_scope: all`, and **dry_run enabled** (the manual default).
   For a quick connection/backup/report check, set `sample_campaigns: 25`.
   Inspect the email, selected campaigns, and downloaded backup artifacts.
   Use `sample_campaigns: 0` (the default) to analyze all eligible campaigns;
   large accounts may require substantial time even with parallel exports.
3. To delete all qualifying no-reply leads, use `cleanup_scope: all` and **dry_run disabled**.
   Keep `sample_campaigns: 0`; sampled backups cannot be executed for deletion.

The active schedule runs a real cleanup every day at **07:00 UTC / 12:30 PM IST**.
If GitHub marked the workflow disabled due to inactivity or manual disabling,
use **Enable workflow** on its Actions page. Scheduled runs can be delayed by
GitHub and only run from the default branch. Runs use one concurrency group and
do not cancel an in-progress cleanup.

Eligibility preserves the existing behavior: only `PAUSED` or `COMPLETED`
campaigns old enough by creation date and outside the excluded clients are
selected. Within those campaigns, only leads with `reply_count == 0` are backed
up for deletion. No eligible campaigns is a successful no-op, not a job failure.
Scheduled cleanup always analyzes all eligible campaigns. Sampling is restricted
to dry-run validation, labels unexamined eligible campaigns `Not sampled`, and
labels its report as a sample. Even a separate execute command refuses to delete
from a sampled backup. Parallel analysis keeps at most `ANALYSIS_WORKERS` exports
in flight, shares the request limiter with deletion, and preserves CSV row order.

In the default `all` scope, the workflow prefetches at most 16 campaign exports
and prepares at most 10,000 candidate rows at a time. It **uploads each complete
batch backup before deleting that batch**; failed uploads prevent deletion.
Backups include campaign age/status/client metadata, are checked for integrity,
and are retained as immutable artifacts. Local copies are removed after the
backed-up batch completes to bound memory and disk use.
The first batch contains at most 1,000 candidates to verify the live path promptly;
subsequent batches use the configured size.

SmartLead exports can repeat the same lead. In `all` scope, each complete campaign
export is checked before batching, and each campaign/lead pair is selected once.
If any occurrence has replies or an unknown reply count, that lead is kept, even
when the conflicting occurrence appears after a batch boundary. Reports include
duplicate-row and conflicting-lead counts. The final duplicate-backup safety check
still refuses unexpected duplicates before making deletion requests.

At its work budget or artifact-count limit, the workflow dispatches a continuation
using the last fully committed campaign ID. A partial campaign is re-exported on
continuation, so unfinished leads are not skipped. One concurrency group prevents
runs sharing this account from overlapping. Errors stop cleanup and send a failure
report; a later fresh run can safely revisit remaining server-side leads. The
workflow needs `actions: write` only to queue its own continuation. Leave the
advanced `resume_after_campaign` input at `0` for a fresh cleanup. The additional
`resume_not_before`, `rate_limit_retries`, and `resume_request_limit` inputs carry
automatic recovery state; leave them at `0` for a fresh cleanup. Each run emails
its own deletion counts and whether more work was queued. Large backlogs can
require multiple runs and several hours; 100,000 DELETE requests alone take at
least about 19 minutes at 5,400 requests/minute, before latency and retry costs.

The optional legacy `target` scope keeps the previous whole-campaign selection
and one-backup execution path. Its target can be exceeded by a large campaign. Final exports and
logs are also uploaded on failure. Artifacts are retained for 30 days, subject to
repository retention settings; they contain lead data and should be access
controlled. CSV backups preserve data but do not automatically restore deleted
leads. All-leads reports attach logs and link to the run for all batch backups.
Legacy target reports attach CSVs/logs up to the 20 MiB raw attachment budget. Resend API acceptance is not a guarantee of inbox
delivery; check Resend delivery events if an accepted message does not arrive.

## Local development

Python 3.11 and Node are used by the workflow. The all-leads controller installs
`@actions/artifact@6.3.1` in the temporary runner directory for durable uploads.
Offline development needs the existing Python requirements and Node; the controller
tests use Node's built-in test runner and mocked uploads.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
node --test tests/drain-controller.test.cjs
```

Tests use simulated HTTP responses and temporary files. They never send email
or delete real leads. For a local live connection test, securely export
`RESEND_API_KEY`, set `SENDER_EMAIL` to
`SmartLead Reports <reports@clientupdates.eagleinfoservice.com>` and
`RECIPIENT_EMAILS` to `arjav@eagleinfoservice.com`, then run:

```bash
python smartlead_consolidated_git.py --mode email-test
```

Use GitHub Actions for the default all-leads cleanup and automatic continuations.
The local command below exercises the legacy target-based Python entry point.
For local cleanup, also securely export `SMARTLEAD_API_KEY`, create an output
directory, and run with `--dry-run` first:

```bash
mkdir -p run-output
cd run-output
python ../smartlead_consolidated_git.py --dry-run --sample-campaigns 25
```

The script reads exported process variables; it does not load `.env` files.
A local run does not upload GitHub artifacts. For live deletion with durable
backups, use the workflow or separately retain the prepared files before running
`--mode execute`.
