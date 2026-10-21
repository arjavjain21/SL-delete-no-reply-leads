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
| `SENDER_EMAIL` | Address on that verified domain, optionally `Name <address@example.com>` |
| `RECIPIENT_EMAILS` | Comma-separated report recipient addresses |

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
   Inspect the email, selected campaigns, and downloaded backup artifacts.
3. To delete the selected no-reply leads, run with **dry_run disabled**.

The active schedule runs a real cleanup every day at **07:00 UTC / 12:30 PM IST**.
If GitHub marked the workflow disabled due to inactivity or manual disabling,
use **Enable workflow** on its Actions page. Scheduled runs can be delayed by
GitHub and only run from the default branch. Runs use one concurrency group and
do not cancel an in-progress cleanup.

Eligibility preserves the existing behavior: only `PAUSED` or `COMPLETED`
campaigns old enough by creation date and outside the excluded clients are
selected. Within those campaigns, only leads with `reply_count == 0` are backed
up for deletion. No eligible campaigns is a successful no-op, not a job failure.

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
or delete real leads. For a local live connection test, securely export the three
email configuration variables above, then run:

```bash
python smartlead_consolidated_git.py --mode email-test
```

For local cleanup, also securely export `SMARTLEAD_API_KEY`, create an output
directory, and run with `--dry-run` first:

```bash
mkdir -p run-output
cd run-output
python ../smartlead_consolidated_git.py --dry-run
```

The script reads exported process variables; it does not load `.env` files.
A local run does not upload GitHub artifacts. For live deletion with durable
backups, use the workflow or separately retain the prepared files before running
`--mode execute`.
