#!/usr/bin/env python3
"""
SmartLead Lead Deletion Script - GitHub Actions Version
Modified to use GitHub Secrets for sensitive information
"""

import requests
import csv
import time
import logging
import os
import argparse
import base64
import json
import hashlib
import math
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import pandas as pd
from requests.exceptions import RequestException
import pytz

# === CONFIGURATION FROM ENVIRONMENT VARIABLES (GitHub Secrets) ===
API_KEY = os.environ.get('SMARTLEAD_API_KEY')
BASE_URL = "https://server.smartlead.ai/api/v1"

# Runtime settings from environment or defaults
TARGET_LEADS = int(os.environ.get('TARGET_LEADS') or '20000')
DAYS_WITHOUT_ACTIVITY = int(os.environ.get('DAYS_WITHOUT_ACTIVITY') or '30')
EXCLUDE_CLIENT_IDS = [int(x.strip()) for x in (os.environ.get('EXCLUDE_CLIENT_IDS') or '1598').split(',') if x.strip()]
SMARTLEAD_REQUESTS_PER_MINUTE = int(os.environ.get('SMARTLEAD_REQUESTS_PER_MINUTE') or '5400')
DELETE_WORKERS = int(os.environ.get('DELETE_WORKERS') or '10')
ANALYSIS_WORKERS = int(os.environ.get('ANALYSIS_WORKERS') or '16')

# Email configuration from environment (GitHub Secrets)
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
RESEND_API_KEY = os.environ.get('RESEND_API_KEY')
RECIPIENT_EMAILS = [email.strip() for email in os.environ.get('RECIPIENT_EMAILS', '').split(',') if email.strip()]
RESEND_URL = "https://api.resend.com/emails"
# Keep below Resend's total message size limit, allowing for base64 and MIME overhead.
MAX_EMAIL_ATTACHMENT_BYTES = 20 * 1024 * 1024

# GitHub Actions specific
GITHUB_REPOSITORY = os.environ.get('GITHUB_REPOSITORY', '')
GITHUB_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
GITHUB_SERVER_URL = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')

# === RETRY CONFIGURATION ===
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

def validate_environment(require_smartlead=True):
    """Validate that all required environment variables are set"""
    required_vars = {
        'SENDER_EMAIL': SENDER_EMAIL,
        'RESEND_API_KEY': RESEND_API_KEY,
        'RECIPIENT_EMAILS': RECIPIENT_EMAILS[0] if RECIPIENT_EMAILS else None
    }
    if require_smartlead:
        required_vars['SMARTLEAD_API_KEY'] = API_KEY
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    if TARGET_LEADS <= 0 or DAYS_WITHOUT_ACTIVITY < 0:
        raise ValueError('TARGET_LEADS must be positive and DAYS_WITHOUT_ACTIVITY must be nonnegative')
    if not 1 <= SMARTLEAD_REQUESTS_PER_MINUTE <= 6000:
        raise ValueError('SMARTLEAD_REQUESTS_PER_MINUTE must be between 1 and 6000')
    if not 1 <= DELETE_WORKERS <= 32:
        raise ValueError('DELETE_WORKERS must be between 1 and 32')
    if not 1 <= ANALYSIS_WORKERS <= 32:
        raise ValueError('ANALYSIS_WORKERS must be between 1 and 32')
    
    print(f"✅ Configuration loaded from environment variables")
    print(f"✅ Target leads: {TARGET_LEADS:,}")
    print(f"✅ Days filter: {DAYS_WITHOUT_ACTIVITY}")
    print(f"✅ Excluded client IDs: {EXCLUDE_CLIENT_IDS}")
    print(f"✅ Recipients: {len(RECIPIENT_EMAILS)}")

# === LOGGING SETUP ===
def setup_logging():
    """Setup logging with both file and console handlers"""
    log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"smartlead_deletion_{log_timestamp}.log"
    
    # Create formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    # Setup logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger, log_filename

# === HTTP REQUEST UTILITY ===
class RateLimiter:
    """Space every attempt across workers; share server-requested cooldowns."""
    def __init__(self, requests_per_minute):
        self.interval = 60.0 / requests_per_minute
        self.next_request = 0.0
        self.lock = threading.Lock()

    def wait(self):
        while True:
            with self.lock:
                now = time.monotonic()
                delay = self.next_request - now
                if delay <= 0:
                    self.next_request = now + self.interval
                    return
            time.sleep(delay)

    def defer(self, seconds):
        with self.lock:
            self.next_request = max(self.next_request, time.monotonic() + seconds)


smartlead_limiter = RateLimiter(max(1, SMARTLEAD_REQUESTS_PER_MINUTE))
http_sessions = threading.local()


def get_http_session():
    if not hasattr(http_sessions, 'session'):
        http_sessions.session = requests.Session()
    return http_sessions.session


def retry_delay(response, attempt):
    """Support both forms of Retry-After, never shorten the server's delay."""
    fallback = BACKOFF_FACTOR ** attempt
    value = response.headers.get('Retry-After') if response is not None else None
    if value:
        try:
            seconds = float(value)
            if math.isfinite(seconds):
                return max(fallback, seconds)
        except ValueError:
            try:
                return max(fallback, (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return fallback


def send_request(method, url, params=None, data=None, logger=None):
    """Rate-limit SmartLead attempts and retry only transient failures.

    Never log exception strings or response bodies: SmartLead URLs contain keys.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        smartlead_limiter.wait()
        response = None
        try:
            response = get_http_session().request(method, url, params=params, json=data, timeout=(10, 60))
            if response.status_code in (401, 403):
                raise RuntimeError(f'SmartLead authentication/authorization failed (HTTP {response.status_code})')
            if response.status_code != 429 and response.status_code < 500:
                if response.status_code >= 400 and logger:
                    logger.error('SmartLead %s failed (HTTP %s)', method, response.status_code)
                return response
            if logger:
                logger.warning('SmartLead %s attempt %s: HTTP %s', method, attempt, response.status_code)
        except RequestException as exc:
            if logger:
                logger.warning('SmartLead %s attempt %s: %s', method, attempt, type(exc).__name__)
        # Also protect other workers after this request exhausts its retries.
        smartlead_limiter.defer(retry_delay(response, attempt))
    return None


# === EMAIL FUNCTIONALITY ===
def send_email(subject, body, attachments=None, logger=None):
    """Send via Resend, preserving one idempotency key across retry attempts."""
    payload = {'from': SENDER_EMAIL, 'to': RECIPIENT_EMAILS, 'subject': subject, 'text': body}
    encoded_attachments = []
    total_bytes = 0
    omitted = []
    for path in dict.fromkeys(attachments or []):
        if not os.path.isfile(path):
            continue
        size = os.path.getsize(path)
        if total_bytes + size > MAX_EMAIL_ATTACHMENT_BYTES:
            omitted.append(os.path.basename(path))
            continue
        with open(path, 'rb') as handle:
            encoded_attachments.append({'filename': os.path.basename(path), 'content': base64.b64encode(handle.read()).decode('ascii')})
        total_bytes += size
    if encoded_attachments:
        payload['attachments'] = encoded_attachments
    if omitted:
        payload['text'] += '\n\nAttachments omitted due to email size limits: ' + ', '.join(omitted)
        payload['text'] += '\nDownload the complete backup and logs from this GitHub Actions run\'s artifacts.'
    headers = {'Authorization': f'Bearer {RESEND_API_KEY}', 'Idempotency-Key': str(uuid.uuid4())}
    for attempt in range(1, MAX_RETRIES + 1):
        response = None
        try:
            response = requests.post(RESEND_URL, headers=headers, json=payload, timeout=(10, 60))
            if 200 <= response.status_code < 300:
                if logger:
                    logger.info('Resend accepted email for delivery')
                return True
            if response.status_code != 429 and response.status_code < 500:
                if logger:
                    logger.error('Resend rejected email (HTTP %s); check API key, verified sender and recipients', response.status_code)
                return False
            if logger:
                logger.warning('Resend attempt %s: HTTP %s', attempt, response.status_code)
        except RequestException as exc:
            if logger:
                logger.warning('Resend attempt %s: %s', attempt, type(exc).__name__)
        if attempt < MAX_RETRIES:
            time.sleep(retry_delay(response, attempt))
    if logger:
        logger.error('Resend email failed after %s attempts', MAX_RETRIES)
    return False


def checked_response(response, operation):
    if response is None or not 200 <= response.status_code < 300:
        raise RuntimeError(f'SmartLead {operation} failed')
    return response


def file_checksum(path):
    with open(path, 'rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()

# === CAMPAIGN MANAGEMENT ===
def fetch_all_campaigns(api_key, logger):
    """Fetch all campaigns from SmartLead API"""
    logger.info("Fetching all campaigns...")
    url = f"{BASE_URL}/campaigns"
    params = {"api_key": api_key}
    response = send_request("GET", url, params=params, logger=logger)
    
    campaigns = checked_response(response, 'campaign fetch').json()
    if not isinstance(campaigns, list):
        raise RuntimeError('SmartLead campaigns response must be a list')
    logger.info(f"Retrieved {len(campaigns)} campaigns")
    return campaigns

def export_leads_csv(api_key, campaign_id, logger, export_folder="exports"):
    """Export leads CSV for a specific campaign"""
    os.makedirs(export_folder, exist_ok=True)
    csv_filename = os.path.join(export_folder, f"leads_campaign_{campaign_id}.csv")
    
    # Remove stale file
    if os.path.isfile(csv_filename):
        os.remove(csv_filename)
        logger.info(f"Removed stale CSV for campaign {campaign_id}")

    url = f"{BASE_URL}/campaigns/{campaign_id}/leads-export"
    params = {"api_key": api_key}
    response = send_request("GET", url, params=params, logger=logger)
    
    checked_response(response, f'lead export for campaign {campaign_id}')
    if response.status_code == 200 and 'text/csv' in response.headers.get('Content-Type', ''):
        with open(csv_filename, "wb") as f:
            f.write(response.content)
        logger.info(f"Exported leads to {csv_filename} for campaign {campaign_id}")
        return csv_filename
    
    raise RuntimeError(f'Invalid CSV export for campaign {campaign_id}')

def analyze_campaign_leads(csv_file, logger):
    """Analyze leads CSV and return statistics"""
    try:
        df = pd.read_csv(csv_file, dtype={'id': 'string'})
        total_leads = len(df)
        no_reply_leads = df[df['reply_count'] == 0].copy()
        no_reply_count = len(no_reply_leads)
        
        logger.info(f"Campaign analysis: {total_leads} total leads, {no_reply_count} no-reply leads")
        return no_reply_leads, total_leads, no_reply_count
    except Exception as e:
        logger.error(f"Error analyzing {csv_file}: {e}")
        raise RuntimeError(f'Cannot analyze lead CSV {csv_file}') from e

# === MAIN PROCESSING LOGIC ===
class SmartLeadProcessor:
    def __init__(self, api_key, logger, log_filename):
        self.api_key = api_key
        self.logger = logger
        self.log_filename = log_filename
        self.execution_stats = {
            'campaigns_fetched': 0,
            'campaigns_filtered': 0,
            'campaigns_analyzed': 0,
            'campaigns_selected': 0,
            'total_leads_exported': 0,
            'no_reply_leads_found': 0,
            'leads_backed_up': 0,
            'leads_deleted_success': 0,
            'leads_deleted_failed': 0,
            'execution_time': 0
        }
        self.output_files = []
        self.sample_campaigns = 0

    def filter_and_analyze_campaigns(self, campaigns, sample_campaigns=0):
        """Parse eligibility once, then export concurrently with bounded memory."""
        self.logger.info('Filtering %s campaigns...', len(campaigns))
        ist_tz = pytz.timezone('Asia/Kolkata')
        now = datetime.now(ist_tz)
        cutoff_date = now - timedelta(days=DAYS_WITHOUT_ACTIVITY)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        campaigns_csv = f'all_campaigns_analysis_{timestamp}.csv'
        self.output_files.append(campaigns_csv)
        rows = []
        eligible_indices = []
        seen_ids = set()
        for campaign in campaigns:
            try:
                campaign_id = int(campaign['id'])
                if campaign_id in seen_ids:
                    raise ValueError('Duplicate campaign ID')
                seen_ids.add(campaign_id)
                client_id = campaign.get('client_id')
                if client_id is not None:
                    client_id = int(client_id)
                created_utc = datetime.fromisoformat(campaign['created_at'].replace('Z', '+00:00'))
                updated_utc = datetime.fromisoformat(campaign['updated_at'].replace('Z', '+00:00'))
                if created_utc.tzinfo is None or updated_utc.tzinfo is None:
                    raise ValueError('Campaign timestamps must include a timezone')
                created_ist = created_utc.astimezone(ist_tz)
                updated_ist = updated_utc.astimezone(ist_tz)
                eligible = (client_id not in EXCLUDE_CLIENT_IDS
                            and campaign.get('status') in ('PAUSED', 'COMPLETED')
                            and created_ist <= cutoff_date)
                if eligible:
                    eligible_indices.append(len(rows))
                rows.append([campaign_id, campaign.get('name', ''), campaign.get('status', ''),
                             client_id, created_utc, created_ist, updated_utc, updated_ist,
                             (now - created_ist).days, 'No', 0, 0, '0.0%'])
            except Exception as exc:
                raise RuntimeError(f"Cannot safely prepare campaign {campaign.get('id')}: {exc}") from exc

        self.execution_stats['campaigns_fetched'] = len(campaigns)
        self.execution_stats['campaigns_filtered'] = len(eligible_indices)
        sampled = eligible_indices[:sample_campaigns] if sample_campaigns else eligible_indices
        sampled_set = set(sampled)
        for index in eligible_indices:
            rows[index][9] = 'Yes' if index in sampled_set else 'Not sampled'
        self.logger.info('Preparing %s of %s eligible campaigns using %s workers',
                         len(sampled), len(eligible_indices), ANALYSIS_WORKERS)
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            print(f'::notice::Preparing {len(sampled)} of {len(eligible_indices)} eligible campaigns; {len(campaigns)} total campaigns', flush=True)

        def analyze(index):
            campaign_id = rows[index][0]
            try:
                path = export_leads_csv(self.api_key, campaign_id, self.logger)
                if not path:
                    raise RuntimeError('Missing CSV export')
                _, total, no_reply = analyze_campaign_leads(path, self.logger)
                return total, no_reply
            except Exception as exc:
                raise RuntimeError(f'Cannot safely prepare campaign {campaign_id}: {exc}') from exc

        indices = iter(sampled)
        with ThreadPoolExecutor(max_workers=ANALYSIS_WORKERS) as executor:
            pending = {}
            for _ in range(min(ANALYSIS_WORKERS, len(sampled))):
                index = next(indices)
                pending[executor.submit(analyze, index)] = index
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    index = pending.pop(future)
                    total, no_reply = future.result()
                    rate = (total - no_reply) / total * 100 if total else 0
                    rows[index][10:] = [total, no_reply, f'{rate:.1f}%']
                    self.execution_stats['campaigns_analyzed'] += 1
                    count = self.execution_stats['campaigns_analyzed']
                    if count % 100 == 0 or count == len(sampled):
                        self.logger.info('Analyzed %s/%s eligible campaigns', count, len(sampled))
                    following = next(indices, None)
                    if following is not None:
                        pending[executor.submit(analyze, following)] = following

        with open(campaigns_csv, 'w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(['Campaign ID', 'Campaign Name', 'Status', 'Client ID',
                             'Created At (UTC)', 'Created At (IST)', 'Updated At (UTC)',
                             'Updated At (IST)', 'Days Since Creation', 'Included in Filter',
                             'Total Leads', 'No Reply Leads', 'Reply Rate %'])
            writer.writerows(rows)
        self.logger.info('Created campaigns analysis: %s', campaigns_csv)
        return [campaigns[index] for index in sampled]

    def select_campaigns_for_deletion(self, filtered_campaigns):
        """Select campaigns to reach target lead count"""
        self.logger.info(f"Selecting campaigns to reach ~{TARGET_LEADS:,} no-reply leads...")
        
        # Read campaign analysis to get lead counts
        campaigns_df = pd.read_csv(self.output_files[-1])  # Latest campaigns CSV
        campaigns_df = campaigns_df[campaigns_df["Included in Filter"] == "Yes"]
        campaigns_df["No Reply Leads"] = pd.to_numeric(campaigns_df["No Reply Leads"], errors="coerce")
        
        # Sort by no-reply leads (descending)
        campaigns_sorted = campaigns_df.sort_values(by="No Reply Leads", ascending=False)
        
        # Select campaigns to reach target
        selected_indices = []
        cumulative_leads = 0
        
        for idx, row in campaigns_sorted.iterrows():
            no_reply_count = int(row["No Reply Leads"])
            if no_reply_count <= 0:
                continue
                
            selected_indices.append(idx)
            cumulative_leads += no_reply_count
            
            if cumulative_leads >= TARGET_LEADS:
                break
        
        # Return DataFrame slice instead of list
        selected_campaigns = campaigns_sorted.loc[selected_indices]
        
        self.execution_stats['campaigns_selected'] = len(selected_campaigns)
        
        self.logger.info(f"Selected {len(selected_campaigns)} campaigns with {cumulative_leads:,} total no-reply leads")
        return selected_campaigns

    def create_deletion_backup(self, selected_campaigns):
        """Export and backup all leads to be deleted"""
        self.logger.info("Creating backup of leads to be deleted...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_csv = f"leads_deletion_backup_{timestamp}.csv"
        self.output_files.append(backup_csv)
        
        all_deletion_leads = []
        
        for _, campaign_row in selected_campaigns.iterrows():
            campaign_id = campaign_row["Campaign ID"]
            campaign_name = campaign_row["Campaign Name"]
            
            self.logger.info(f"Processing campaign {campaign_id}: {campaign_name}")
            
            # Export leads for this campaign
            leads_file = export_leads_csv(self.api_key, campaign_id, self.logger)
            if not leads_file:
                self.logger.error(f"Failed to export leads for campaign {campaign_id}")
                continue
                
            # Filter no-reply leads
            no_reply_df, total_leads, no_reply_count = analyze_campaign_leads(leads_file, self.logger)
            
            if not no_reply_df.empty:
                # Add campaign metadata
                no_reply_df["Campaign ID"] = campaign_id
                no_reply_df["Campaign Name"] = campaign_name
                no_reply_df["Backup Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                all_deletion_leads.append(no_reply_df)
                self.execution_stats['total_leads_exported'] += total_leads
                self.execution_stats['no_reply_leads_found'] += no_reply_count
        
        if all_deletion_leads:
            # Combine all deletion leads
            backup_df = pd.concat(all_deletion_leads, ignore_index=True)
            backup_df.to_csv(backup_csv, index=False)
            
            self.execution_stats['leads_backed_up'] = len(backup_df)
            self.logger.info(f"Backup created: {backup_csv} with {len(backup_df):,} leads")
            return backup_csv, backup_df
        
        self.logger.warning("No leads found for backup")
        return None, pd.DataFrame()

    def delete_leads(self, backup_df, deadline=None, stop_on_failure=False):
        """Delete leads using SmartLead API"""
        if backup_df.empty:
            self.logger.info("No leads to delete")
            return
            
        self.logger.info(f"Starting deletion of {len(backup_df):,} leads...")
        
        success_count = 0
        failed_count = 0
        
        required = {'Campaign ID', 'id', 'reply_count'}
        if not required.issubset(backup_df.columns):
            raise ValueError('Deletion backup is missing Campaign ID, id or reply_count')
        if backup_df[['Campaign ID', 'id']].isna().any().any() or not (backup_df['reply_count'] == 0).all():
            raise ValueError('Deletion backup has missing IDs or leads with replies; refusing deletion')
        pairs = [(str(campaign), str(lead)) for campaign, lead in zip(backup_df['Campaign ID'], backup_df['id'])]
        if any(not campaign.isdigit() or not lead.isdigit() for campaign, lead in pairs):
            raise ValueError('Deletion backup must contain integer campaign and lead IDs')
        if len(set(pairs)) != len(pairs):
            raise ValueError('Deletion backup contains duplicate campaign/lead IDs')

        with ThreadPoolExecutor(max_workers=DELETE_WORKERS) as executor:
            # Bound each batch so an authentication failure cannot queue the whole backup.
            for offset in range(0, len(pairs), DELETE_WORKERS):
                if deadline is not None and time.time() >= deadline:
                    return False
                batch = pairs[offset:offset + DELETE_WORKERS]
                results = executor.map(lambda pair: self.delete_single_lead(*pair), batch)
                for success in results:
                    success_count += int(success)
                    failed_count += int(not success)
                    self.execution_stats['leads_deleted_success'] = success_count
                    self.execution_stats['leads_deleted_failed'] = failed_count
                if stop_on_failure and failed_count:
                    raise RuntimeError('Lead deletion failed; stopping this batch for review')
                processed = success_count + failed_count
                if processed // 100 > offset // 100 or processed == len(pairs):
                    self.logger.info('Progress: %s/%s processed', processed, len(pairs))
        
        self.execution_stats['leads_deleted_success'] = success_count
        self.execution_stats['leads_deleted_failed'] = failed_count
        
        self.logger.info(f"Deletion completed: {success_count:,} successful, {failed_count:,} failed")
        return True

    def delete_single_lead(self, campaign_id, lead_id):
        """Delete a single lead via API"""
        url = f"{BASE_URL}/campaigns/{campaign_id}/leads/{lead_id}"
        params = {"api_key": self.api_key}
        
        response = send_request("DELETE", url, params=params, logger=self.logger)
        
        if response is not None and response.status_code in (200, 204):
            return True
        elif response is not None and response.status_code == 404:
            self.logger.warning(f"Lead {lead_id} already deleted")
            return True
        else:
            self.logger.error(f"Failed to delete lead {lead_id} from campaign {campaign_id}")
            return False

    def send_completion_email(self, dry_run=False):
        """Send detailed completion email with attachments"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        label = 'Dry Run Complete' if dry_run else 'Deletion Complete'
        subject = f"SmartLead {label} - {datetime.now().strftime('%Y-%m-%d')}"
        
        # GitHub Actions specific info
        github_info = ""
        if GITHUB_REPOSITORY and GITHUB_RUN_ID:
            run_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"
            github_info = f"""
=== GITHUB ACTIONS INFO ===
Repository: {GITHUB_REPOSITORY}
Run ID: {GITHUB_RUN_ID}
Run URL: {run_url}
"""
        
        # Create detailed summary
        body = f"""SmartLead Lead Deletion Process Completed
Execution Time: {timestamp}
Total Processing Time: {self.execution_stats['execution_time']:.2f} seconds
{github_info}
=== EXECUTION SUMMARY ===
✓ Campaigns Fetched: {self.execution_stats['campaigns_fetched']:,}
✓ Campaigns Filtered (eligible): {self.execution_stats['campaigns_filtered']:,}
✓ Campaigns Analyzed: {self.execution_stats['campaigns_analyzed']:,}
✓ Campaigns Selected for deletion: {self.execution_stats['campaigns_selected']:,}
✓ Total Leads Exported: {self.execution_stats['total_leads_exported']:,}
✓ No-Reply Leads Found: {self.execution_stats['no_reply_leads_found']:,}
✓ Leads Backed Up: {self.execution_stats['leads_backed_up']:,}
✓ Leads Successfully Deleted: {self.execution_stats['leads_deleted_success']:,}
✗ Leads Failed to Delete: {self.execution_stats['leads_deleted_failed']:,}

=== CONFIGURATION USED ===
• Target Leads: {TARGET_LEADS:,}
• Campaign Age Filter: {DAYS_WITHOUT_ACTIVITY} days
• Excluded Client IDs: {EXCLUDE_CLIENT_IDS}

=== ATTACHMENTS ===
• Complete execution log
• All campaigns analysis CSV
• Leads deletion backup CSV

{'Dry run: no leads were deleted.' if dry_run else 'Processing completed. See deletion counts above; CSV backups are not an automatic restore mechanism.'}
{f'Dry-run sample covering up to {self.sample_campaigns} eligible campaigns. Scheduled cleanup analyzes all eligible campaigns.' if self.sample_campaigns else ''}
Complete backups and logs are retained in this GitHub Actions run's artifacts.
"""

        # Prepare attachments
        attachments = [self.log_filename] + self.output_files
        
        # Send email
        if send_email(subject, body, attachments, self.logger):
            self.logger.info("Completion email sent successfully")
        else:
            raise RuntimeError('Resend could not send the completion report')

    def run_full_process(self, dry_run=False, prepare_only=False, state_file='run-state.json', sample_campaigns=0):
        """Prepare backups, optionally pausing for durable artifact upload."""
        start_time = time.time()
        if sample_campaigns < 0 or (sample_campaigns and not dry_run):
            raise ValueError('Campaign sampling is permitted only with --dry-run')
        self.sample_campaigns = sample_campaigns
        
        try:
            # Step 1: Fetch all campaigns
            self.logger.info("=== STEP 1: Fetching Campaigns ===")
            campaigns = fetch_all_campaigns(self.api_key, self.logger)
            
            # Step 2: Filter and analyze campaigns
            self.logger.info("=== STEP 2: Filtering and Analyzing Campaigns ===")
            filtered_campaigns = self.filter_and_analyze_campaigns(campaigns, sample_campaigns)
            
            # Step 3: Select campaigns for deletion
            self.logger.info("=== STEP 3: Selecting Campaigns for Deletion ===")
            selected_campaigns = self.select_campaigns_for_deletion(filtered_campaigns)
            
            # Step 4: Create backup
            self.logger.info("=== STEP 4: Creating Deletion Backup ===")
            backup_file, backup_df = self.create_deletion_backup(selected_campaigns)
            self.execution_stats['execution_time'] = time.time() - start_time
            # Do not include a nonexistent backup when no eligible leads remain.
            self.output_files = [path for path in self.output_files if os.path.isfile(path)]
            with open(state_file, 'w', encoding='utf-8') as handle:
                json.dump({'backup_file': backup_file,
                           'sample_campaigns': sample_campaigns, 'dry_run_only': bool(sample_campaigns),
                           'backup_sha256': file_checksum(backup_file) if backup_file else None,
                           'output_files': self.output_files,
                           'log_filename': self.log_filename, 'execution_stats': self.execution_stats}, handle, indent=2)
            self.logger.info('Preparation complete: %s leads backed up', len(backup_df))
            if not prepare_only:
                self.run_prepared_process(state_file, dry_run)
            
        except Exception as e:
            self.logger.error(f"Process failed: {e}")
            # Send failure email
            self.send_failure_email(str(e))
            raise  # Re-raise to make GitHub Action fail

    def run_prepared_process(self, state_file='run-state.json', dry_run=False):
        """Execute only after the workflow has successfully uploaded the backup."""
        start_time = time.time()
        with open(state_file, encoding='utf-8') as handle:
            state = json.load(handle)
        if state.get('dry_run_only') and not dry_run:
            raise ValueError('A sampled backup is for dry-run validation only; refusing deletion')
        self.sample_campaigns = state.get('sample_campaigns', 0)
        self.execution_stats = state['execution_stats']
        self.output_files = state['output_files']
        if state['log_filename'] != self.log_filename:
            self.output_files = self.output_files + [state['log_filename']]
        backup_file = state['backup_file']
        if backup_file:
            if file_checksum(backup_file) != state['backup_sha256']:
                raise ValueError('Prepared backup checksum changed; refusing deletion')
            backup_df = pd.read_csv(backup_file, dtype={'id': 'string', 'Campaign ID': 'string'})
            if len(backup_df) != self.execution_stats['leads_backed_up']:
                raise ValueError('Prepared backup row count changed; refusing deletion')
            if not dry_run:
                self.delete_leads(backup_df)
        if self.execution_stats['leads_deleted_failed']:
            raise RuntimeError(f"{self.execution_stats['leads_deleted_failed']} lead deletions failed; review logs and backup")
        self.execution_stats['execution_time'] += time.time() - start_time
        self.send_completion_email(dry_run=dry_run)
        self.logger.info('Process completed successfully%s', ' (dry run)' if dry_run else '')
    
    def send_failure_email(self, error_message):
        """Send failure notification email"""
        github_info = ""
        if GITHUB_REPOSITORY and GITHUB_RUN_ID:
            run_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"
            github_info = f"""
GitHub Repository: {GITHUB_REPOSITORY}
GitHub Run URL: {run_url}
"""
        
        subject = f"SmartLead Deletion FAILED - {datetime.now().strftime('%Y-%m-%d')}"
        body = f"""SmartLead Lead Deletion Process FAILED
Error: {error_message}
Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{github_info}
Please check the attached log file for detailed error information.
"""
        attachments = [self.log_filename] + self.output_files
        send_email(subject, body, attachments, self.logger)

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='SmartLead cleanup with Resend reporting')
    parser.add_argument('--mode', choices=['cleanup', 'prepare', 'execute', 'email-test'], default='cleanup')
    parser.add_argument('--dry-run', action='store_true', help='Export and report without deleting leads')
    parser.add_argument('--sample-campaigns', type=int, default=0,
                        help='Dry-run only: analyze up to this many eligible campaigns (0 = all)')
    args = parser.parse_args()
    if args.sample_campaigns < 0 or (args.sample_campaigns and
            (not args.dry_run or args.mode not in ('prepare', 'cleanup'))):
        parser.error('--sample-campaigns requires --dry-run and cleanup/prepare mode')
    processor = None
    try:
        # Validate environment variables
        validate_environment(require_smartlead=args.mode != 'email-test')
        
        # Setup logging
        logger, log_filename = setup_logging()
        
        logger.info("=== SmartLead Deletion Process Started ===")
        logger.info(f"Target: {TARGET_LEADS:,} leads, Campaign age: {DAYS_WITHOUT_ACTIVITY} days")
        
        # Initialize processor
        processor = SmartLeadProcessor(API_KEY, logger, log_filename)
        
        # Run the complete process
        if args.mode == 'email-test':
            if not send_email('SmartLead Resend connection test',
                              'Resend is connected. This test did not access or delete SmartLead leads.', logger=logger):
                raise RuntimeError('Resend test email failed')
        elif args.mode == 'execute':
            processor.run_prepared_process(dry_run=args.dry_run)
        else:
            processor.run_full_process(dry_run=args.dry_run, prepare_only=args.mode == 'prepare',
                                       sample_campaigns=args.sample_campaigns)
        
    except Exception as e:
        if processor is not None and args.mode == 'execute':
            processor.send_failure_email(str(e))
        print(f"Critical error: {e}")
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            # Surface the precise failure on the run page, even without log access.
            message = str(e).replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
            print(f'::error::{message}')
        exit(1)

if __name__ == "__main__":
    main()
