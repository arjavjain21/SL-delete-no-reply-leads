#!/usr/bin/env python3
"""
SmartLead Lead Deletion Script - GitHub Actions Version
Combines all functionality: fetch campaigns, export leads, backup, and delete
Modified to work with .env files and environment variables
"""

import requests
import csv
import time
import logging
import os
import smtplib
import ssl
from datetime import datetime, timedelta
import pandas as pd
from requests.exceptions import HTTPError, ConnectionError, Timeout, RequestException
import pytz
from email.message import EmailMessage
from dotenv import load_dotenv

# === LOAD ENVIRONMENT VARIABLES ===
# Load from .env file first, then environment variables take precedence
load_dotenv()

# === CONFIGURATION FROM ENVIRONMENT VARIABLES ===
API_KEY = '2fbf4f7d-44af-4ff1-8e25-5655f5483fd0_94zyakr'
BASE_URL = "https://server.smartlead.ai/api/v1"

# Runtime settings from environment or defaults
TARGET_LEADS = 20000
DAYS_WITHOUT_ACTIVITY = 30
EXCLUDE_CLIENT_IDS = [1598]

# Email configuration from environment
SENDER_EMAIL = 'arjavjain777@gmail.com'
APP_PASSWORD = 'whregjzxhpkbnata'
RECIPIENT_EMAILS = 'arjav@eagleinfoservice.com'
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465

# GitHub Actions specific
GITHUB_REPOSITORY = os.environ.get('GITHUB_REPOSITORY', '')
GITHUB_RUN_ID = os.environ.get('GITHUB_RUN_ID', '')
GITHUB_SERVER_URL = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')

# === RETRY CONFIGURATION ===
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

def validate_environment():
    """Validate that all required environment variables are set"""
    required_vars = {
        'SMARTLEAD_API_KEY': API_KEY,
        'EMAIL_SENDER': SENDER_EMAIL,
        'EMAIL_PASSWORD': APP_PASSWORD,
        'EMAIL_RECIPIENTS': RECIPIENT_EMAILS[0] if RECIPIENT_EMAILS else None
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    print(f"✅ Configuration loaded from .env file and environment variables")
    print(f"✅ Target leads: {TARGET_LEADS:,}")
    print(f"✅ Days filter: {DAYS_WITHOUT_ACTIVITY}")
    print(f"✅ Excluded client IDs: {EXCLUDE_CLIENT_IDS}")
    print(f"✅ Recipients: {len([r for r in RECIPIENT_EMAILS if r.strip()])}")

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
def send_request(method, url, params=None, data=None, logger=None):
    """Send HTTP request with retries and exponential backoff"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, params=params, json=data, timeout=30)
            response.raise_for_status()
            return response
        except (HTTPError, ConnectionError, Timeout, RequestException) as e:
            if logger:
                logger.error(f"Request error on attempt {attempt} for URL {url}: {e}")
            time.sleep(BACKOFF_FACTOR ** attempt)
    return None

# === EMAIL FUNCTIONALITY ===
def send_email(subject, body, attachments=[], logger=None):
    """Send email with attachments"""
    try:
        msg = EmailMessage()
        msg["From"] = SENDER_EMAIL
        msg["To"] = ", ".join([r.strip() for r in RECIPIENT_EMAILS if r.strip()])
        msg["Subject"] = subject
        msg.set_content(body)

        for file_path in attachments:
            if os.path.isfile(file_path):
                with open(file_path, "rb") as f:
                    file_data = f.read()
                    file_name = os.path.basename(file_path)
                msg.add_attachment(file_data, maintype="application", subtype="octet-stream", filename=file_name)
            else:
                if logger:
                    logger.warning(f"Attachment {file_path} not found")

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, APP_PASSWORD)
            server.send_message(msg)
        
        if logger:
            logger.info("Email sent successfully")
        return True
    except Exception as e:
        if logger:
            logger.error(f"Failed to send email: {e}")
        return False

# === CAMPAIGN MANAGEMENT ===
def fetch_all_campaigns(api_key, logger):
    """Fetch all campaigns from SmartLead API"""
    logger.info("Fetching all campaigns...")
    url = f"{BASE_URL}/campaigns"
    params = {"api_key": api_key}
    response = send_request("GET", url, params=params, logger=logger)
    
    if response:
        campaigns = response.json()
        logger.info(f"Retrieved {len(campaigns)} campaigns")
        return campaigns
    
    logger.error("Failed to retrieve campaigns")
    return []

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
    
    if response and response.status_code == 200 and 'text/csv' in response.headers.get('Content-Type', ''):
        with open(csv_filename, "wb") as f:
            f.write(response.content)
        logger.info(f"Exported leads to {csv_filename} for campaign {campaign_id}")
        return csv_filename
    
    logger.error(f"Failed to export leads for campaign {campaign_id}")
    return None

def analyze_campaign_leads(csv_file, logger):
    """Analyze leads CSV and return statistics"""
    try:
        df = pd.read_csv(csv_file)
        total_leads = len(df)
        no_reply_leads = df[df['reply_count'] == 0]
        no_reply_count = len(no_reply_leads)
        
        logger.info(f"Campaign analysis: {total_leads} total leads, {no_reply_count} no-reply leads")
        return no_reply_leads, total_leads, no_reply_count
    except Exception as e:
        logger.error(f"Error analyzing {csv_file}: {e}")
        return pd.DataFrame(), 0, 0

# === MAIN PROCESSING LOGIC ===
class SmartLeadProcessor:
    def __init__(self, api_key, logger, log_filename):
        self.api_key = api_key
        self.logger = logger
        self.log_filename = log_filename
        self.execution_stats = {
            'campaigns_fetched': 0,
            'campaigns_filtered': 0,
            'campaigns_selected': 0,
            'total_leads_exported': 0,
            'no_reply_leads_found': 0,
            'leads_backed_up': 0,
            'leads_deleted_success': 0,
            'leads_deleted_failed': 0,
            'execution_time': 0
        }
        self.output_files = []

    def filter_and_analyze_campaigns(self, campaigns):
        """Filter campaigns and create comprehensive analysis"""
        self.logger.info(f"Filtering {len(campaigns)} campaigns...")
        
        # Setup timezone and cutoff date
        ist_tz = pytz.timezone("Asia/Kolkata")
        cutoff_date = datetime.now(ist_tz) - timedelta(days=DAYS_WITHOUT_ACTIVITY)
        
        # Output CSV for all campaigns
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        campaigns_csv = f"all_campaigns_analysis_{timestamp}.csv"
        self.output_files.append(campaigns_csv)
        
        filtered_campaigns = []
        
        with open(campaigns_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Campaign ID", "Campaign Name", "Status", "Client ID",
                "Created At (UTC)", "Created At (IST)", 
                "Updated At (UTC)", "Updated At (IST)",
                "Days Since Creation", "Included in Filter",
                "Total Leads", "No Reply Leads", "Reply Rate %"
            ])
            
            for campaign in campaigns:
                try:
                    campaign_id = campaign.get("id")
                    campaign_name = campaign.get("name", "")
                    status = campaign.get("status", "")
                    client_id = campaign.get("client_id")
                    
                    # Parse timestamps
                    created_utc = datetime.strptime(campaign["created_at"], "%Y-%m-%dT%H:%M:%S.%f%z")
                    updated_utc = datetime.strptime(campaign["updated_at"], "%Y-%m-%dT%H:%M:%S.%f%z")
                    created_ist = created_utc.astimezone(ist_tz)
                    updated_ist = updated_utc.astimezone(ist_tz)
                    
                    days_since_creation = (datetime.now(ist_tz) - created_ist).days
                    
                    # Determine if campaign should be included
                    include_campaign = (
                        client_id not in EXCLUDE_CLIENT_IDS and
                        status in ("PAUSED", "COMPLETED") and
                        created_ist <= cutoff_date
                    )
                    
                    total_leads = 0
                    no_reply_leads = 0
                    reply_rate = 0
                    
                    if include_campaign:
                        filtered_campaigns.append(campaign)
                        # Export and analyze leads for included campaigns
                        leads_file = export_leads_csv(self.api_key, campaign_id, self.logger)
                        if leads_file:
                            _, total_leads, no_reply_leads = analyze_campaign_leads(leads_file, self.logger)
                            reply_rate = ((total_leads - no_reply_leads) / total_leads * 100) if total_leads > 0 else 0
                    
                    writer.writerow([
                        campaign_id, campaign_name, status, client_id,
                        created_utc, created_ist, updated_utc, updated_ist,
                        days_since_creation, "Yes" if include_campaign else "No",
                        total_leads, no_reply_leads, f"{reply_rate:.1f}%"
                    ])
                    
                except Exception as e:
                    self.logger.error(f"Error processing campaign {campaign.get('id')}: {e}")
        
        self.execution_stats['campaigns_fetched'] = len(campaigns)
        self.execution_stats['campaigns_filtered'] = len(filtered_campaigns)
        
        self.logger.info(f"Created comprehensive campaigns analysis: {campaigns_csv}")
        self.logger.info(f"Filtered to {len(filtered_campaigns)} eligible campaigns")
        
        return filtered_campaigns

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

    def delete_leads(self, backup_df):
        """Delete leads using SmartLead API"""
        if backup_df.empty:
            self.logger.info("No leads to delete")
            return
            
        self.logger.info(f"Starting deletion of {len(backup_df):,} leads...")
        
        success_count = 0
        failed_count = 0
        
        for idx, row in backup_df.iterrows():
            campaign_id = row.get("Campaign ID")
            lead_id = row.get("id")
            
            if pd.isna(campaign_id) or pd.isna(lead_id):
                self.logger.error(f"Row {idx}: Missing Campaign ID or Lead ID")
                failed_count += 1
                continue
            
            # Delete lead via API
            if self.delete_single_lead(campaign_id, lead_id):
                success_count += 1
            else:
                failed_count += 1
            
            # Rate limiting
            time.sleep(0.5)
            
            # Progress logging every 100 deletions
            if (success_count + failed_count) % 100 == 0:
                self.logger.info(f"Progress: {success_count + failed_count:,}/{len(backup_df):,} processed")
        
        self.execution_stats['leads_deleted_success'] = success_count
        self.execution_stats['leads_deleted_failed'] = failed_count
        
        self.logger.info(f"Deletion completed: {success_count:,} successful, {failed_count:,} failed")

    def delete_single_lead(self, campaign_id, lead_id):
        """Delete a single lead via API"""
        url = f"{BASE_URL}/campaigns/{campaign_id}/leads/{lead_id}"
        params = {"api_key": self.api_key}
        
        response = send_request("DELETE", url, params=params, logger=self.logger)
        
        if response and response.status_code == 200:
            return True
        elif response and response.status_code == 404:
            self.logger.warning(f"Lead {lead_id} already deleted")
            return True
        else:
            self.logger.error(f"Failed to delete lead {lead_id} from campaign {campaign_id}")
            return False

    def send_completion_email(self):
        """Send detailed completion email with attachments"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        subject = f"SmartLead Deletion Complete - {datetime.now().strftime('%Y-%m-%d')}"
        
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

The deletion process has been completed successfully. All deleted leads have been backed up for recovery if needed.
"""

        # Prepare attachments
        attachments = [self.log_filename] + self.output_files
        
        # Send email
        if send_email(subject, body, attachments, self.logger):
            self.logger.info("Completion email sent successfully")
        else:
            self.logger.error("Failed to send completion email")

    def run_full_process(self):
        """Execute the complete deletion process"""
        start_time = time.time()
        
        try:
            # Step 1: Fetch all campaigns
            self.logger.info("=== STEP 1: Fetching Campaigns ===")
            campaigns = fetch_all_campaigns(self.api_key, self.logger)
            if not campaigns:
                raise Exception("No campaigns retrieved")
            
            # Step 2: Filter and analyze campaigns
            self.logger.info("=== STEP 2: Filtering and Analyzing Campaigns ===")
            filtered_campaigns = self.filter_and_analyze_campaigns(campaigns)
            if not filtered_campaigns:
                raise Exception("No campaigns match filter criteria")
            
            # Step 3: Select campaigns for deletion
            self.logger.info("=== STEP 3: Selecting Campaigns for Deletion ===")
            selected_campaigns = self.select_campaigns_for_deletion(filtered_campaigns)
            if len(selected_campaigns) == 0:
                raise Exception("No campaigns selected for deletion")
            
            # Step 4: Create backup
            self.logger.info("=== STEP 4: Creating Deletion Backup ===")
            backup_file, backup_df = self.create_deletion_backup(selected_campaigns)
            if backup_df.empty:
                raise Exception("No leads found for backup")
            
            # Step 5: Delete leads
            self.logger.info("=== STEP 5: Deleting Leads ===")
            self.delete_leads(backup_df)
            
            # Calculate execution time
            self.execution_stats['execution_time'] = time.time() - start_time
            
            # Step 6: Send completion email
            self.logger.info("=== STEP 6: Sending Completion Email ===")
            self.send_completion_email()
            
            self.logger.info(f"=== PROCESS COMPLETED SUCCESSFULLY in {self.execution_stats['execution_time']:.2f} seconds ===")
            
        except Exception as e:
            self.logger.error(f"Process failed: {e}")
            # Send failure email
            self.send_failure_email(str(e))
            raise  # Re-raise to make GitHub Action fail
    
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
    try:
        # Validate environment variables
        validate_environment()
        
        # Setup logging
        logger, log_filename = setup_logging()
        
        logger.info("=== SmartLead Deletion Process Started ===")
        logger.info(f"Target: {TARGET_LEADS:,} leads, Campaign age: {DAYS_WITHOUT_ACTIVITY} days")
        
        # Initialize processor
        processor = SmartLeadProcessor(API_KEY, logger, log_filename)
        
        # Run the complete process
        processor.run_full_process()
        
    except Exception as e:
        print(f"Critical error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
