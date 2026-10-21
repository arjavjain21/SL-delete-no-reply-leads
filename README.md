# SmartLead cleanup with Resend

A daily GitHub Actions job exports and deletes no-reply leads from eligible
SmartLead campaigns, then sends a report through the Resend HTTPS API.
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
| `TARGET_LEADS` | `20000` | Approximate target; whole campaigns may exceed it |
| `DAYS_WITHOUT_ACTIVITY` | `30` | Existing behavior: minimum **campaign age since creation**, not last activity |
| `EXCLUDE_CLIENT_IDS` | `1598` | Comma-separated client IDs excluded from cleanup |
| `SMARTLEAD_REQUESTS_PER_MINUTE` | `5400` | Shared limit for all SmartLead attempts, including retries; maximum `6000` |
| `DELETE_WORKERS` | `10` | Concurrent deletion workers; range `1`–`32` |
| `ANALYSIS_WORKERS` | `16` | Concurrent campaign exports/analysis; range `1`–`32` |

The rate limit applies to this process. Other jobs or applications using the same
SmartLead account share its 6,000 requests/minute allowance; lower this setting
when necessary. Actual throughput also depends on response time and server
cooldowns. Retries honor `Retry-After` and use exponential backoff. Permanent
errors are not repeatedly retried, and authentication errors abort the run.

## Start and verify

After the workflow changes are on the default branch:

1. Open **Actions → SmartLead Lead Deletion → Run workflow**. Choose
   `mode: email-test`. This sends one connection-test email through Resend and
   does not access SmartLead or delete anything.
2. Run again with `mode: cleanup` and **dry_run enabled** (the manual default).
   For a quick connection/backup/report check, set `sample_campaigns: 25`.
   Inspect the email, selected campaigns, and downloaded backup artifacts.
   Use `sample_campaigns: 0` (the default) to analyze all eligible campaigns;
   large accounts may require substantial time even with parallel exports.
3. To delete the selected no-reply leads, run with **dry_run disabled**.
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

The workflow splits preparation and execution. It **uploads the complete backup
before deletion begins**; a failed upload prevents deletion. Final exports and
logs are also uploaded on failure. Artifacts are retained for 30 days, subject to
repository retention settings; they contain lead data and should be access
controlled. CSV backups preserve data but do not automatically restore deleted
leads. Emails include CSVs/logs when their combined raw size fits the 20 MiB
attachment budget; larger files remain available in Actions artifacts and are
listed as omitted in the email. Resend API acceptance is not a guarantee of inbox
delivery; check Resend delivery events if an accepted message does not arrive.

## Local development

Python 3.11 is used by the workflow. No new runtime dependencies are required.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Tests use simulated HTTP responses and temporary files. They never send email
or delete real leads. For a local live connection test, securely export
`RESEND_API_KEY`, set `SENDER_EMAIL` to
`SmartLead Reports <reports@clientupdates.eagleinfoservice.com>` and
`RECIPIENT_EMAILS` to `arjav@eagleinfoservice.com`, then run:

```bash
python smartlead_consolidated_git.py --mode email-test
```

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
