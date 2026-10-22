#!/usr/bin/env python3
"""Bounded, resumable cleanup of all qualifying campaign leads.

Each backup must be durably uploaded by the Actions controller before execute.
Only the last completely committed campaign is used as a continuation cursor.
"""
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import itertools
import json
import logging
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import smartlead_consolidated_git as app

STATE = Path('drain-state.json')
BATCH = Path('batch.json')
LOG = logging.getLogger('drain')
csv.field_size_limit(16 * 1024 * 1024)


def save(path, data):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, indent=2), encoding='utf-8')
    temporary.replace(path)


def load(path=STATE):
    return json.loads(path.read_text(encoding='utf-8'))


def plan(resume_after=0, dry_run=False, sample=0, budget_minutes=120):
    if sample < 0 or (sample and not dry_run):
        raise ValueError('Sampling requires dry run')
    if resume_after < 0 or not 1 <= budget_minutes <= 140:
        raise ValueError('Invalid continuation cursor or work budget')
    cutoff = datetime.now(timezone.utc) - timedelta(days=app.DAYS_WITHOUT_ACTIVITY)
    campaigns = app.fetch_all_campaigns(app.API_KEY, LOG)
    eligible = []
    seen = set()
    for campaign in campaigns:
        campaign_id = int(campaign['id'])
        if campaign_id in seen or campaign_id <= 0:
            raise ValueError('Invalid or duplicate campaign ID')
        seen.add(campaign_id)
        created = datetime.fromisoformat(campaign['created_at'].replace('Z', '+00:00'))
        if created.tzinfo is None:
            raise ValueError('Campaign creation timestamp requires timezone')
        client = campaign.get('client_id')
        client = int(client) if client is not None else None
        if (campaign_id > resume_after and client not in app.EXCLUDE_CLIENT_IDS
                and campaign.get('status') in ('PAUSED', 'COMPLETED') and created <= cutoff):
            eligible.append({'id': campaign_id, 'name': campaign.get('name', ''),
                             'client_id': client, 'status': campaign['status'],
                             'created_at': created.isoformat()})
    eligible.sort(key=lambda campaign: campaign['id'])
    eligible_count = len(eligible)
    if sample:
        eligible = eligible[:sample]
    state = {'campaigns': eligible, 'index': 0, 'row_offset': 0,
             'confirmed_after': resume_after, 'scan_after': resume_after,
             'dry_run': dry_run, 'sample': sample, 'cutoff': cutoff.isoformat(),
             'deadline': time.time() + budget_minutes * 60, 'done': not eligible,
             'batch_number': 0, 'stats': {'campaigns_fetched': len(campaigns),
             'eligible_remaining_at_start': eligible_count, 'campaigns_scanned': 0,
             'leads_backed_up': 0, 'leads_deleted_success': 0, 'leads_deleted_failed': 0,
             'unknown_reply_counts': 0, 'backups_uploaded': 0}}
    save(STATE, state)
    LOG.info('All-leads plan: %s campaigns total; %s eligible remaining; %s in this run%s',
             len(campaigns), eligible_count, len(eligible), ' (sample dry run)' if sample else '')
    return state


def prefetch(state):
    upcoming = state['campaigns'][state['index']:state['index'] + app.ANALYSIS_WORKERS]
    missing = [campaign for campaign in upcoming
               if not Path(f"exports/leads_campaign_{campaign['id']}.csv").is_file()]
    with ThreadPoolExecutor(max_workers=app.ANALYSIS_WORKERS) as pool:
        futures = [pool.submit(app.export_leads_csv, app.API_KEY, campaign['id'], LOG)
                   for campaign in missing]
        for future in futures:
            if not future.result():
                raise RuntimeError('Campaign export failed')


def prepare(batch_size=10000):
    if not 1 <= batch_size <= 50000:
        raise ValueError('Batch size must be between 1 and 50000')
    state = load()
    rows = []
    while len(rows) < batch_size and state['index'] < len(state['campaigns']):
        if time.time() >= state['deadline']:
            break
        prefetch(state)
        campaign = state['campaigns'][state['index']]
        export = Path(f"exports/leads_campaign_{campaign['id']}.csv")
        exhausted = True
        with export.open(encoding='utf-8-sig', newline='') as handle:
            reader = csv.DictReader(handle)
            if not {'id', 'reply_count'}.issubset(reader.fieldnames or []):
                raise ValueError('Lead export is missing id or reply_count')
            for row in itertools.islice(reader, state['row_offset'], None):
                if time.time() >= state['deadline'] or len(rows) >= batch_size:
                    exhausted = False
                    break
                state['row_offset'] += 1
                try:
                    count = Decimal(row['reply_count'])
                    if not count.is_finite():
                        raise InvalidOperation
                except (InvalidOperation, TypeError, ValueError):
                    state['stats']['unknown_reply_counts'] += 1
                    continue
                if count != 0:
                    continue
                if not row['id'] or not row['id'].isdigit():
                    raise ValueError('Lead export has an invalid lead ID')
                row.update({'Campaign ID': str(campaign['id']), 'Campaign Name': campaign['name'],
                            'Campaign Status': campaign['status'], 'Client ID': campaign['client_id'],
                            'Campaign Created At': campaign['created_at'],
                            'Backup Timestamp': datetime.now(timezone.utc).isoformat()})
                rows.append(row)
        if exhausted:
            state['scan_after'] = campaign['id']
            state['index'] += 1
            state['row_offset'] = 0
            state['stats']['campaigns_scanned'] += 1
            export.unlink()
    state['done'] = state['index'] >= len(state['campaigns'])
    state['batch_number'] += 1
    manifest = {'backup_file': None, 'rows': len(rows), 'done': state['done'],
                'dry_run_only': state['dry_run'], 'number': state['batch_number']}
    if rows:
        backup = Path(f"backup-batch-{state['batch_number']:04d}.csv")
        columns = list(dict.fromkeys(key for row in rows for key in row))
        with backup.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        manifest.update(backup_file=str(backup), sha256=app.file_checksum(backup))
    else:
        # Scanned campaigns with no candidates can be committed without deletion.
        state['confirmed_after'] = state['scan_after']
    save(STATE, state)
    save(BATCH, manifest)
    LOG.info('Prepared batch %s: %s candidates; %s/%s campaigns fully scanned',
             state['batch_number'], len(rows), state['index'], len(state['campaigns']))
    return manifest


def validate_backup(frame, state):
    required = {'Campaign ID', 'Campaign Status', 'Client ID', 'Campaign Created At', 'id', 'reply_count'}
    if not required.issubset(frame.columns):
        raise ValueError('Backup lacks cleanup policy metadata')
    cutoff = datetime.fromisoformat(state['cutoff'])
    for row in frame[['Campaign Status', 'Client ID', 'Campaign Created At']].itertuples(index=False, name=None):
        status, client, created = row
        client = None if app.pd.isna(client) else int(client)
        created = datetime.fromisoformat(created)
        if (created.tzinfo is None or created > cutoff or status not in ('PAUSED', 'COMPLETED')
                or client in app.EXCLUDE_CLIENT_IDS):
            raise ValueError('Backup contains a campaign outside the approved policy')


def execute(artifact_id):
    state, manifest = load(), load(BATCH)
    if manifest.get('dry_run_only') and not state['dry_run']:
        raise ValueError('A dry-run backup cannot be executed for live deletion')
    if not artifact_id or int(artifact_id) <= 0:
        raise ValueError('A successful durable artifact upload is required before execution')
    backup = manifest['backup_file']
    if not backup or app.file_checksum(backup) != manifest['sha256']:
        raise ValueError('Backup missing or checksum changed')
    frame = app.pd.read_csv(backup, dtype={'id': 'string', 'Campaign ID': 'string'})
    if len(frame) != manifest['rows']:
        raise ValueError('Backup row count changed')
    validate_backup(frame, state)
    state['stats']['leads_backed_up'] += len(frame)
    state['stats']['backups_uploaded'] += 1
    processor = app.SmartLeadProcessor(app.API_KEY, LOG, 'cleanup.log')
    finished = True
    try:
        if not state['dry_run']:
            finished = processor.delete_leads(frame, deadline=state['deadline'], stop_on_failure=True)
    finally:
        state['stats']['leads_deleted_success'] += processor.execution_stats['leads_deleted_success']
        state['stats']['leads_deleted_failed'] += processor.execution_stats['leads_deleted_failed']
        save(STATE, state)
    if finished:
        state['confirmed_after'] = state['scan_after']
    manifest.update(artifact_id=int(artifact_id), finished=finished,
                    deleted=processor.execution_stats['leads_deleted_success'])
    save(STATE, state)
    save(BATCH, manifest)
    LOG.info('Batch %s committed: %s deleted%s', manifest['number'], manifest['deleted'],
             ' (dry run)' if state['dry_run'] else '')
    # The immutable artifact retains the backup; keep local storage bounded.
    Path(backup).unlink()
    return manifest


def report(status):
    state = load()
    stats = state['stats']
    summary = {'status': status, 'stats_this_run': stats, 'resume_after': state['confirmed_after'],
               'dry_run': state['dry_run'], 'sample_campaigns': state['sample']}
    save(Path('summary.json'), summary)
    body = (f"SmartLead all-eligible cleanup: {status}\n\n"
            f"Run: {app.GITHUB_SERVER_URL}/{app.GITHUB_REPOSITORY}/actions/runs/{app.GITHUB_RUN_ID}\n"
            f"Policy: no replies; PAUSED/COMPLETED campaigns created at least {app.DAYS_WITHOUT_ACTIVITY} days ago; "
            f"excluded clients {app.EXCLUDE_CLIENT_IDS}. Age applies to campaigns, not individual leads.\n"
            f"No total lead target; processing uses backed-up batches.\n\n"
            + '\n'.join(f'{key}: {value:,}' for key, value in stats.items())
            + f"\n\nResume after campaign: {state['confirmed_after']}\n"
            + ('Dry run: no leads were deleted.\n' if state['dry_run'] else '')
            + (f"Sample validation: up to {state['sample']} eligible campaigns.\n" if state['sample'] else '')
            + ('A continuation run is queued to process the remaining campaigns.\n' if status == 'continuing' else '')
            + 'Each batch backup was uploaded before deletion. Download backups from Actions artifacts; retained 30 days.\n')
    if not app.send_email(f'SmartLead cleanup: {status}', body, ['cleanup.log'], LOG):
        raise RuntimeError('Resend could not send the cleanup report')
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['plan', 'prepare', 'execute', 'report'])
    parser.add_argument('--resume-after', type=int, default=0)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--sample-campaigns', type=int, default=0)
    parser.add_argument('--budget-minutes', type=int, default=120)
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--artifact-id', type=int)
    parser.add_argument('--status', choices=['complete', 'continuing', 'failed', 'dry-run'], default='complete')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('cleanup.log'), logging.StreamHandler()])
    try:
        app.validate_environment(target_mode=False)
        if args.command == 'plan':
            plan(args.resume_after, args.dry_run, args.sample_campaigns, args.budget_minutes)
        elif args.command == 'prepare':
            prepare(args.batch_size)
        elif args.command == 'execute':
            execute(args.artifact_id)
        else:
            report(args.status)
    except Exception as exc:
        LOG.error('%s', exc)
        message = str(exc).replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
        print(f'::error::{message}')
        raise SystemExit(1)


if __name__ == '__main__':
    main()
