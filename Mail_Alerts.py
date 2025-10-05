import imaplib
import email
from email.header import decode_header
from dotenv import dotenv_values
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from IPython.display import display
import logging
import sys
import urllib.parse
import pyarrow.parquet as pq
import pyarrow as pa
from zoneinfo import ZoneInfo
import time
import re

# Configure logging to show only WARNING and ERROR levels
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger()
logger.setLevel(logging.WARNING)

# Suppress DEBUG logs from libraries
for name in ['requests', 'urllib3', 'imaplib']:
    logging.getLogger(name).setLevel(logging.WARNING)
    logging.getLogger(name).propagate = False

# Load .env file
DOTENV_PATH = os.getenv('DOTENV_PATH', r'E:\OPS_Streamline\Variable\mail_analytics.env')
try:
    config = dotenv_values(DOTENV_PATH)
    if not config:
        logger.error(f"Failed to load .env file at {DOTENV_PATH}")
        raise ValueError("Failed to load .env file")
except Exception as e:
    logger.error(f"Error loading .env file: {str(e)}")
    raise

# Extract required variables
required_vars = [
    'IMAP_SERVER', 'SENDER_EMAIL', 'EMAIL_PASSWORD', 'NOT_ALERT_API_URL',
    'RECEIVED_ALERT_API_URL', 'CEO_STATS_API_URL', 'NON_CEO_STATS_API_URL',
    'MAIL_DELIVERY_STATUS_PATH', 'MAIL_DELIVERY_STATUS_HOUR', 'MAIL_DELIVERY_STATUS_MINUTE',
    'EXCEL_PATH', 'NOT_DELIVERED', 'P0_API_URL'  # Added P0_API_URL
]
for var in required_vars:
    if not config.get(var):
        logger.error(f"Missing or empty variable '{var}' in .env file")
        raise ValueError(f"Missing variable '{var}'")

IMAP_SERVER = config['IMAP_SERVER']
IMAP_PORT = int(config.get('IMAP_PORT', '993'))
USERNAME = config['SENDER_EMAIL']
PASSWORD = config['EMAIL_PASSWORD']
NOT_ALERT_API_URL = config['NOT_ALERT_API_URL']
RECEIVED_ALERT_API_URL = config['RECEIVED_ALERT_API_URL']
CEO_STATS_API_URL = config['CEO_STATS_API_URL']
NON_CEO_STATS_API_URL = config['NON_CEO_STATS_API_URL']
P0_API_URL = config['P0_API_URL']  # New P0 API URL
EMAIL_CHECK_WINDOW_HOURS = int(config.get('EMAIL_CHECK_WINDOW_HOURS', '3'))
ALERT_START_HOUR = int(config.get('ALERT_START_HOUR', '5'))
ALERT_END_HOUR = int(config.get('ALERT_END_HOUR', '22'))
SENT_ALERTS_FILE = r"E:\OPS_Streamline\sent_alerts_history.parquet"
MAIL_DELIVERY_STATUS_PATH = config['MAIL_DELIVERY_STATUS_PATH']
MAIL_DELIVERY_STATUS_HOUR = int(config['MAIL_DELIVERY_STATUS_HOUR'])
MAIL_DELIVERY_STATUS_MINUTE = int(config['MAIL_DELIVERY_STATUS_MINUTE'])
EMAIL_OUTPUT_DIR = r"E:\OPS_Streamline\Email Op"
NOT_DELIVERED_DIR = config['NOT_DELIVERED']

# Helper functions
def normalize_string(s):
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r'\s+', ' ', s)
    return s

def normalize_subject(subject):
    if not subject:
        return ""
    subject = normalize_string(subject)
    subject = re.sub(r'(\d+)(am|pm)', r'\1 \2', subject, flags=re.IGNORECASE)
    return subject.replace(' ', '')

def is_valid_day_or_date(prefix, current_time):
    if not prefix or prefix == 'all days':
        return True
    current_day = current_time.strftime('%A').lower()
    current_date = current_time.day
    days_of_week = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
    
    if prefix in days_of_week:
        logger.info(f"Checking prefix '{prefix}' against current day '{current_day}'")
        return prefix == current_day
    
    if prefix.startswith('date '):
        try:
            day_str = prefix.replace('date ', '').strip()
            day_num = int(day_str)
            if 1 <= day_num <= 31:
                logger.info(f"Checking prefix '{prefix}' against current date '{current_date}'")
                return day_num == current_date
            else:
                logger.warning(f"Invalid day number in prefix '{prefix}'")
                return False
        except ValueError:
            logger.warning(f"Invalid date format in prefix '{prefix}'")
            return False
    
    logger.warning(f"Unknown prefix '{prefix}'")
    return False

def load_sent_alerts_history():
    try:
        if os.path.exists(SENT_ALERTS_FILE):
            df = pd.read_parquet(SENT_ALERTS_FILE)
            if 'not_received_count' not in df.columns:
                df['not_received_count'] = 0
            return df
        return pd.DataFrame(columns=['date', 'subject', 'alert_type', 'not_received_count'])
    except Exception as e:
        logger.error(f"Error loading sent alerts history: {str(e)}")
        return pd.DataFrame(columns=['date', 'subject', 'alert_type', 'not_received_count'])

def save_sent_alerts_history(df):
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, SENT_ALERTS_FILE)
        logger.info(f"Saved sent alerts history to {SENT_ALERTS_FILE}")
    except Exception as e:
        logger.error(f"Error saving sent alerts history: {str(e)}")

def save_mail_delivery_status(subjects_data, emails_data, current_date, current_time):
    if current_time.hour != MAIL_DELIVERY_STATUS_HOUR or current_time.minute != MAIL_DELIVERY_STATUS_MINUTE:
        return

    delivery_status_data = []
    for entry in subjects_data:
        if not is_valid_day_or_date(entry.get('prefix'), current_time):
            continue
        base_subject = entry['base_subject']
        running = entry['running']

        if running == 'hourly' and 'specific_hours' in entry:
            for subj in entry['subjects_for_hours']:
                two_digit_subject = subj['two_digit']
                single_digit_subject = subj['single_digit']
                email_found = None
                for email_entry in emails_data:
                    if normalize_subject(email_entry['Subject']) in [normalize_subject(two_digit_subject), normalize_subject(single_digit_subject)]:
                        email_found = email_entry
                        break
                delivery_status_data.append({
                    'date': current_date,
                    'subject': two_digit_subject,
                    'status': 'received' if email_found else 'not received',
                    'received_time': email_found['Received Time'] if email_found else None,
                    'priority': entry.get('priority', '')
                })
        else:
            two_digit_subject = entry['two_digit']
            single_digit_subject = entry['single_digit']
            email_found = None
            for email_entry in emails_data:
                if normalize_subject(email_entry['Subject']) in [normalize_subject(two_digit_subject), normalize_subject(single_digit_subject)]:
                    email_found = email_entry
                    break
            delivery_status_data.append({
                'date': current_date,
                'subject': two_digit_subject,
                'status': 'received' if email_found else 'not received',
                'received_time': email_found['Received Time'] if email_found else None,
                'priority': entry.get('priority', '')
            })

    delivery_status_df = pd.DataFrame(delivery_status_data)
    parquet_file = os.path.join(MAIL_DELIVERY_STATUS_PATH, f"mail_delivery_status_{current_date.replace('-', '')}.parquet")
    try:
        table = pa.Table.from_pandas(delivery_status_df)
        pq.write_table(table, parquet_file)
        logger.info(f"Saved mail delivery status to {parquet_file}")
    except Exception as e:
        logger.error(f"Error saving mail delivery status: {str(e)}")

def has_not_received_alert_been_sent(sent_alerts_df, subject, current_date):
    return not sent_alerts_df[
        (sent_alerts_df['date'] == current_date) &
        (sent_alerts_df['subject'] == subject) &
        (sent_alerts_df['alert_type'] == 'NOT received')
    ].empty

def has_alert_been_sent(sent_alerts_df, subject, current_date, running, received):
    received_sent = not sent_alerts_df[
        (sent_alerts_df['date'] == current_date) &
        (sent_alerts_df['subject'] == subject) &
        (sent_alerts_df['alert_type'] == 'received')
    ].empty
    if received_sent:
        return True

    if running == 'hourly':
        not_received_alerts = sent_alerts_df[
            (sent_alerts_df['date'] == current_date) &
            (sent_alerts_df['subject'] == subject) &
            (sent_alerts_df['alert_type'] == 'NOT received')
        ]
        count = len(not_received_alerts)
        return count >= 3
    return False

def reset_history_if_new_day(sent_alerts_df, current_date):
    if sent_alerts_df.empty:
        return sent_alerts_df
    latest_date = sent_alerts_df['date'].max()
    if latest_date != current_date:
        return pd.DataFrame(columns=['date', 'subject', 'alert_type', 'not_received_count'])
    return sent_alerts_df

def parse_time_range(hours_str, subject="Unknown"):
    try:
        hours_str = normalize_string(hours_str)
        hours_str = re.sub(r'(\d+)(am|pm)', r'\1 \2', hours_str, flags=re.IGNORECASE)
        logger.info(f"Parsing hours string: '{hours_str}' for subject '{subject}'")

        prefix = None
        time_part = hours_str
        possible_prefixes = ['all days', 'sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'] + [f'date {str(i).zfill(2)}' for i in range(1, 32)]
        for p in possible_prefixes:
            if hours_str.startswith(p + ','):
                prefix = p
                time_part = hours_str[len(p) + 1:].strip()
                break
            elif hours_str.startswith(p.replace(' ', '')):
                prefix = p
                time_part = hours_str[len(p.replace(' ', '')) + 1:].strip()
                break
        logger.info(f"Parsed prefix: '{prefix}', time part: '{time_part}'")

        single_time_pattern = r'^\d{1,2}\s*(am|pm)$'
        if re.match(single_time_pattern, time_part, re.IGNORECASE):
            try:
                time_obj = datetime.strptime(time_part, '%I %p')
            except ValueError:
                time_obj = datetime.strptime(time_part.replace(' ', ''), '%I%p')
            logger.info(f"Parsed single time: hour={time_obj.hour}, prefix={prefix}")
            return [time_obj.hour], None, None, prefix

        if ',' in time_part:
            time_parts = [t.strip() for t in time_part.split(',')]
            hours = []
            for part in time_parts:
                try:
                    time_obj = datetime.strptime(part, '%I %p')
                    hours.append(time_obj.hour)
                except ValueError:
                    time_obj = datetime.strptime(part.replace(' ', ''), '%I%p')
                    hours.append(time_obj.hour)
            logger.info(f"Parsed comma-separated hours: hours={hours}, prefix={prefix}")
            return hours, None, None, prefix
        elif 'from' in time_part:
            time_part = time_part.replace('from', '').strip()
            try:
                start_time = datetime.strptime(time_part, '%I %p')
            except ValueError:
                start_time = datetime.strptime(time_part.replace(' ', ''), '%I%p')
            end_time = datetime.strptime('10 PM', '%I %p')
            logger.info(f"Parsed 'from' format: start_hour={start_time.hour}, end_hour={end_time.hour}, after_time=True, prefix={prefix}")
            return start_time.hour, end_time.hour, True, prefix
        elif 'to' in time_part:
            try:
                start_str, end_str = re.split(r'\s*to\s*', time_part)
                start_str = start_str.strip()
                end_str = end_str.strip()
                if not start_str or not end_str:
                    logger.error(f"Invalid time range format in '{time_part}' for subject '{subject}'")
                    return None, None, False, prefix
                try:
                    start_time = datetime.strptime(start_str, '%I %p')
                except ValueError:
                    start_time = datetime.strptime(start_str.replace(' ', ''), '%I%p')
                try:
                    end_time = datetime.strptime(end_str, '%I %p')
                except ValueError:
                    end_time = datetime.strptime(end_str.replace(' ', ''), '%I%p')
                logger.info(f"Parsed range: start_hour={start_time.hour}, end_hour={end_time.hour}, after_time=False, prefix={prefix}")
                return start_time.hour, end_time.hour, False, prefix
            except Exception as e:
                logger.error(f"Error parsing time range '{time_part}' for subject '{subject}': {str(e)}")
                return None, None, False, prefix
        else:
            logger.error(f"Unknown time format '{time_part}' for subject '{subject}'")
            return None, None, False, prefix
    except Exception as e:
        logger.error(f"Error parsing hours '{hours_str}' for subject '{subject}': {str(e)}")
        return None, None, False, prefix

def is_within_time_range(current_hour, start_hour, end_hour, after_time=False):
    if start_hour is None or end_hour is None:
        return False
    if not isinstance(start_hour, int) or not isinstance(end_hour, int):
        logger.warning(f"Invalid hour types: start_hour={type(start_hour)}, end_hour={type(end_hour)}")
        return False
    if after_time:
        return start_hour <= current_hour <= end_hour
    else:
        if start_hour <= end_hour:
            return start_hour <= current_hour <= end_hour
        else:
            return current_hour >= start_hour or current_hour <= end_hour

def load_subjects_from_excel():
    excel_path = config['EXCEL_PATH']
    try:
        df = pd.read_excel(excel_path, sheet_name="Data")
        subjects_data = []
        local_tz = ZoneInfo("Asia/Kolkata")
        current_time = datetime.now(local_tz)
        current_hour = current_time.hour

        for _, row in df.iterrows():
            category = str(row['Category']).strip()
            subject = str(row['Subject']).strip()
            hours = normalize_string(str(row['Hours']))
            running = normalize_string(str(row['Running']))
            priority = str(row.get('Priority', '')).strip().upper()

            if running in ['once a day', 'onceaday']:
                running = 'once a day'

            time_data = parse_time_range(hours, subject=subject)
            start_hour, end_hour, after_time, prefix = time_data

            if not is_valid_day_or_date(prefix, current_time):
                logger.info(f"Skipping subject '{subject}' due to prefix '{prefix}' mismatch")
                continue

            subject_entry = {
                'category': category,
                'base_subject': subject,
                'hours': hours,
                'running': running,
                'prefix': prefix,
                'priority': priority
            }

            if running == 'hourly':
                if isinstance(start_hour, list):
                    specific_hours = start_hour
                    subject_entry['specific_hours'] = specific_hours
                    subject_entry['start_hour'] = None
                    subject_entry['end_hour'] = None
                    subject_entry['after_time'] = False
                    subjects_for_hours = []
                    for hour in specific_hours:
                        if hour > current_hour:
                            continue
                        prev_hour = (hour - 1) % 24
                        prev_hour_time = current_time.replace(hour=prev_hour, minute=0, second=0, microsecond=0)
                        hour_with_leading_zero = prev_hour_time.strftime("%I")
                        hour_without_leading_zero = hour_with_leading_zero.lstrip('0') or '12'
                        ampm = prev_hour_time.strftime("%p")
                        two_digit_subject = f"{subject} [{hour_with_leading_zero} {ampm}]"
                        single_digit_subject = f"{subject} [{hour_without_leading_zero} {ampm}]"
                        subjects_for_hours.append({
                            'hour': hour,
                            'two_digit': two_digit_subject,
                            'single_digit': single_digit_subject
                        })
                    subject_entry['subjects_for_hours'] = subjects_for_hours
                else:
                    subject_entry['start_hour'] = start_hour
                    subject_entry['end_hour'] = end_hour
                    subject_entry['after_time'] = after_time
                    previous_hour_time = current_time - timedelta(hours=1)
                    hour_with_leading_zero = previous_hour_time.strftime("%I")
                    hour_without_leading_zero = hour_with_leading_zero.lstrip('0') or '12'
                    ampm = previous_hour_time.strftime("%p")
                    subject_entry['two_digit'] = f"{subject} [{hour_with_leading_zero} {ampm}]"
                    subject_entry['single_digit'] = f"{subject} [{hour_without_leading_zero} {ampm}]"
            else:
                subject_entry['start_hour'] = start_hour
                subject_entry['end_hour'] = end_hour
                subject_entry['after_time'] = after_time
                subject_entry['two_digit'] = subject
                subject_entry['single_digit'] = subject

            subjects_data.append(subject_entry)

        logger.info(f"Loaded {len(subjects_data)} subjects from Excel")
        return subjects_data
    except Exception as e:
        logger.error(f"Error reading Excel file: {str(e)}")
        return []

def fetch_emails():
    emails_data = []
    imap = None
    local_tz = ZoneInfo("Asia/Kolkata")
    try:
        imap = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT, timeout=30)
        imap.login(USERNAME, PASSWORD)
        imap.select("INBOX")

        current_time = datetime.now(local_tz)
        search_start = current_time - timedelta(hours=EMAIL_CHECK_WINDOW_HOURS)
        search_end = current_time

        since_date = search_start.strftime('%d-%b-%Y')
        status, email_ids = imap.search(None, f'SINCE "{since_date}"')
        if status != "OK":
            logger.error("Failed to search emails")
            return emails_data

        email_id_list = email_ids[0].split()
        if not email_id_list:
            logger.info("No emails found in search window")
            return emails_data

        for email_id in email_id_list:
            try:
                status, msg_data = imap.fetch(email_id, "(RFC822)")
                if status != "OK":
                    logger.warning(f"Failed to fetch email ID {email_id}")
                    continue

                msg = email.message_from_bytes(msg_data[0][1])
                subject, encoding = decode_header(msg["Subject"])[0]
                subject = subject.decode(encoding or "utf-8", errors="replace") if isinstance(subject, bytes) else subject
                received_time = msg.get("Date", None)

                if not received_time:
                    logger.warning(f"No Date header for email ID {email_id}")
                    continue

                parsed_time = None
                try:
                    parsed_time = email.utils.parsedate_to_datetime(received_time)
                    if parsed_time:
                        if parsed_time.tzinfo is None:
                            parsed_time = parsed_time.replace(tzinfo=local_tz)
                        else:
                            parsed_time = parsed_time.astimezone(local_tz)
                    else:
                        try:
                            parsed_time = datetime.strptime(received_time, '%a, %d %b %Y %H:%M:%S')
                            parsed_time = parsed_time.replace(tzinfo=local_tz)
                        except ValueError:
                            logger.warning(f"Cannot parse time '{received_time}' for email ID {email_id}")
                            continue

                    if not (search_start <= parsed_time <= search_end):
                        continue

                except Exception as e:
                    logger.warning(f"Error parsing date for email ID {email_id}: {str(e)}")
                    continue

                emails_data.append({
                    "Subject": subject.strip(),
                    "Received Time": parsed_time
                })

            except Exception as e:
                logger.warning(f"Error processing email ID {email_id}: {str(e)}")
                continue

        logger.info(f"Fetched {len(emails_data)} emails from IMAP")
        return emails_data

    except Exception as e:
        logger.error(f"Error fetching emails: {str(e)}")
        return emails_data
    finally:
        if imap:
            try:
                imap.logout()
            except Exception as e:
                logger.warning(f"Error during IMAP logout: {str(e)}")

def save_emails_to_parquet(emails_data, current_date):
    parquet_file = os.path.join(EMAIL_OUTPUT_DIR, f"Email_Output_{current_date}.parquet")
    existing_data = []
    try:
        if os.path.exists(parquet_file):
            existing_df = pd.read_parquet(parquet_file)
            existing_df['Received Time'] = pd.to_datetime(existing_df['Received Time'], errors='coerce').dt.tz_convert('Asia/Kolkata')
            existing_data = existing_df.to_dict('records')
    except Exception as e:
        logger.error(f"Error loading existing Parquet file {parquet_file}: {str(e)}")

    combined_data = existing_data + emails_data
    if not combined_data:
        display("No emails to save for today")
        return

    df = pd.DataFrame(combined_data, columns=["Subject", "Received Time"])
    df = df.drop_duplicates(subset=['Subject', 'Received Time'], keep='first')

    os.makedirs(EMAIL_OUTPUT_DIR, exist_ok=True)
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file)
        display(f"Saved {len(df)} unique emails to {parquet_file}")
    except Exception as e:
        logger.error(f"Failed to save Parquet file: {str(e)}")

def load_emails_from_parquet(current_date):
    parquet_file = os.path.join(EMAIL_OUTPUT_DIR, f"Email_Output_{current_date}.parquet")
    try:
        if os.path.exists(parquet_file):
            df = pd.read_parquet(parquet_file)
            df['Received Time'] = pd.to_datetime(df['Received Time'], errors='coerce').dt.tz_convert('Asia/Kolkata')
            return df.to_dict('records')
        return []
    except Exception as e:
        logger.error(f"Error loading emails from {parquet_file}: {str(e)}")
        return []

def send_alert(params, api_url):
    if isinstance(params, (tuple, list)):
        display(f"Sending stats alert with params: {str(params)[:50]}...")
        logger.info(f"Preparing to send stats alert with params: {str(params)[:100]}...")
        try:
            encoded_params = [urllib.parse.quote(str(param), safe='*') for param in params]
            formatted_url = api_url.format(*encoded_params)
            response = requests.get(formatted_url, timeout=15)
            logger.info(f"API response status: {response.status_code}, response text: {response.text[:100]}...")
            if response.status_code != 200:
                logger.error(f"Failed to send stats alert. Status: {response.status_code}, Response: {response.text}")
                display(f"Stats alert failed with status: {response.status_code}")
        except Exception as e:
            logger.error(f"Error sending stats alert: {str(e)}")
            display(f"Error sending stats alert: {str(e)}")
    else:
        alert_message = params
        display(f"Sending alert: {alert_message[:50]}...")
        try:
            encoded_msg = urllib.parse.quote(alert_message)
            formatted_url = api_url.format(encoded_msg)
            response = requests.get(formatted_url, timeout=15)
            if response.status_code != 200:
                logger.error(f"Failed to send alert. Status: {response.status_code}")
        except Exception as e:
            logger.error(f"Error sending alert: {str(e)}")
            try:
                filename = f"alert_{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')}.txt"
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(str(alert_message))
                logger.info(f"Saved alert to fallback file: {filename}")
            except Exception as e:
                logger.error(f"Failed to save fallback file: {str(e)}")

    display("Alert attempt completed")

def save_stats_to_excel(subjects_data, emails_data, current_date, current_time):
    debug_dir = r"E:\OPS_Streamline\Debug"
    excel_filename = f"expected_vs_received_{current_date.replace('-', '_')}_{current_time.hour:02d}.xlsx"
    excel_path = os.path.join(debug_dir, excel_filename)
    
    os.makedirs(debug_dir, exist_ok=True)
    
    local_tz = ZoneInfo("Asia/Kolkata")
    day_start = datetime(current_time.year, current_time.month, current_time.day, 0, 0, 0, tzinfo=local_tz)
    current_hour = current_time.hour
    
    daily_emails = [
        email for email in emails_data
        if email['Received Time'] is not None and day_start <= email['Received Time'] <= current_time
    ]
    
    ceo_data = []
    non_ceo_data = []
    ceo_sl_no = 1
    non_ceo_sl_no = 1
    
    for entry in subjects_data:
        if not is_valid_day_or_date(entry.get('prefix'), current_time):
            continue
        category = entry['category']
        base_subject = entry['base_subject']
        running = entry['running']
        priority = entry.get('priority', '')
        
        if running == 'hourly' and 'specific_hours' in entry:
            for subj in entry['subjects_for_hours']:
                if subj['hour'] > current_hour:
                    continue
                expected_two_digit = subj['two_digit']
                expected_single_digit = subj['single_digit']
                received = False
                received_time = None
                norm_two = normalize_subject(expected_two_digit)
                norm_single = normalize_subject(expected_single_digit)
                for email in daily_emails:
                    if normalize_subject(email['Subject']) in [norm_two, norm_single]:
                        received = True
                        received_time = email['Received Time']
                        break
                data_entry = {
                    'Sl.No': ceo_sl_no if category == 'CEO' else non_ceo_sl_no,
                    'Expected Subject': expected_two_digit,
                    'Received Status': 'Received' if received else 'Not Received',
                    'Received Time': received_time,
                    'Priority': priority
                }
                if category == 'CEO':
                    ceo_data.append(data_entry)
                    ceo_sl_no += 1
                else:
                    non_ceo_data.append(data_entry)
                    non_ceo_sl_no += 1
        else:
            start_hour = entry['start_hour']
            end_hour = entry['end_hour']
            after_time = entry.get('after_time', False)
            if running == 'hourly':
                if is_within_time_range(current_hour, start_hour, end_hour, after_time) and current_hour > start_hour - 1:
                    for hour in range(start_hour - 1, current_hour):
                        if hour < 0:
                            continue
                        hour_time = day_start + timedelta(hours=hour)
                        hour_with_leading_zero = hour_time.strftime("%I")
                        hour_without_leading_zero = hour_with_leading_zero.lstrip('0') or '12'
                        ampm = hour_time.strftime("%p")
                        expected_two_digit = f"{base_subject} [{hour_with_leading_zero} {ampm}]"
                        expected_single_digit = f"{base_subject} [{hour_without_leading_zero} {ampm}]"
                        received = False
                        received_time = None
                        norm_two = normalize_subject(expected_two_digit)
                        norm_single = normalize_subject(expected_single_digit)
                        for email in daily_emails:
                            if normalize_subject(email['Subject']) in [norm_two, norm_single]:
                                received = True
                                received_time = email['Received Time']
                                break
                        data_entry = {
                            'Sl.No': ceo_sl_no if category == 'CEO' else non_ceo_sl_no,
                            'Expected Subject': expected_two_digit,
                            'Received Status': 'Received' if received else 'Not Received',
                            'Received Time': received_time,
                            'Priority': priority
                        }
                        if category == 'CEO':
                            ceo_data.append(data_entry)
                            ceo_sl_no += 1
                        else:
                            non_ceo_data.append(data_entry)
                            non_ceo_sl_no += 1
            else:
                if is_within_time_range(current_hour, start_hour, end_hour, after_time):
                    expected_subject = base_subject
                    received = False
                    received_time = None
                    norm_subject = normalize_subject(expected_subject)
                    for email in daily_emails:
                        if normalize_subject(email['Subject']) == norm_subject:
                            received = True
                            received_time = email['Received Time']
                            break
                    data_entry = {
                        'Sl.No': ceo_sl_no if category == 'CEO' else non_ceo_sl_no,
                        'Expected Subject': expected_subject,
                        'Received Status': 'Received' if received else 'Not Received',
                        'Received Time': received_time,
                        'Priority': priority
                    }
                    if category == 'CEO':
                        ceo_data.append(data_entry)
                        ceo_sl_no += 1
                    else:
                        non_ceo_data.append(data_entry)
                        non_ceo_sl_no += 1
    
    ceo_df = pd.DataFrame(ceo_data)
    non_ceo_df = pd.DataFrame(non_ceo_data)
    
    if not ceo_df.empty:
        ceo_df['Received Time'] = ceo_df['Received Time'].apply(
            lambda x: x.replace(tzinfo=None) if pd.notna(x) and isinstance(x, datetime) else x
        )
    if not non_ceo_df.empty:
        non_ceo_df['Received Time'] = non_ceo_df['Received Time'].apply(
            lambda x: x.replace(tzinfo=None) if pd.notna(x) and isinstance(x, datetime) else x
        )
    
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl', mode='w') as writer:
            ceo_df.to_excel(writer, sheet_name='CEO', index=False)
            non_ceo_df.to_excel(writer, sheet_name='Non-CEO', index=False)
        display(f"Saved stats to {excel_path}")
        logger.info(f"Saved stats to {excel_path}")
    except Exception as e:
        logger.error(f"Failed to save Excel file to {excel_path}: {str(e)}")
        display(f"Error: {str(e)}")

def send_statistics_alerts(subjects_data, emails_data, current_date, current_time):
    logger.info("Starting send_statistics_alerts function")
    local_tz = ZoneInfo("Asia/Kolkata")
    day_start = datetime(current_time.year, current_time.month, current_time.day, 0, 0, 0, tzinfo=local_tz)
    current_hour = current_time.hour
    checked_time_str = current_time.strftime('%d/%m/%Y %H:%M:%S')
    logger.info(f"Current time: {checked_time_str}, Current hour: {current_hour}")

    daily_emails = [
        email for email in emails_data
        if email['Received Time'] is not None and day_start <= email['Received Time'] <= current_time
    ]
    normalized_emails = {normalize_subject(email['Subject']): email for email in daily_emails}
    logger.info(f"Filtered {len(daily_emails)} daily emails")

    # Load existing non-delivered data
    formatted_date = current_time.strftime('%d_%m_%Y')
    parquet_file = os.path.join(NOT_DELIVERED_DIR, f"mail_not_delivered_{formatted_date}.parquet")
    existing_non_delivered = []
    if os.path.exists(parquet_file):
        try:
            existing_df = pd.read_parquet(parquet_file)
            existing_non_delivered = existing_df.to_dict('records')
            logger.info(f"Loaded {len(existing_non_delivered)} existing non-delivered records")
        except Exception as e:
            logger.error(f"Error loading existing non-delivered parquet file: {str(e)}")

    # CEO Statistics
    logger.info("Computing CEO statistics")
    non_delivered = []
    ceo_expected = 0
    ceo_received = 0
    for entry in subjects_data:
        if not is_valid_day_or_date(entry.get('prefix'), current_time):
            continue
        if entry['category'] != 'CEO':
            continue
        running = entry['running']
        base_subject = entry['base_subject']
        logger.info(f"Processing CEO subject: {base_subject}")

        if running == 'hourly' and 'specific_hours' in entry:
            for subj in entry['subjects_for_hours']:
                if subj['hour'] > current_hour:
                    continue
                expected_two = subj['two_digit']
                expected_single = subj['single_digit']
                ceo_expected += 1
                norm_two = normalize_subject(expected_two)
                norm_single = normalize_subject(expected_single)
                if norm_two in normalized_emails or norm_single in normalized_emails:
                    ceo_received += 1
                else:
                    non_delivered.append({
                        'category': 'CEO',
                        'subject': expected_two,
                        'checked_time': checked_time_str,
                        'priority': entry.get('priority', '')
                    })
        else:
            start_hour = entry['start_hour']
            end_hour = entry['end_hour']
            after_time = entry.get('after_time', False)
            if running == 'hourly':
                if is_within_time_range(current_hour, start_hour, end_hour, after_time) and current_hour > start_hour - 1:
                    for hour in range(start_hour - 1, current_hour):
                        if hour < 0:
                            continue
                        hour_time = day_start + timedelta(hours=hour)
                        hour_with_leading_zero = hour_time.strftime("%I")
                        hour_without_leading_zero = hour_with_leading_zero.lstrip('0') or '12'
                        ampm = hour_time.strftime("%p")
                        expected_two = f"{base_subject} [{hour_with_leading_zero} {ampm}]"
                        expected_single = f"{base_subject} [{hour_without_leading_zero} {ampm}]"
                        ceo_expected += 1
                        norm_two = normalize_subject(expected_two)
                        norm_single = normalize_subject(expected_single)
                        if norm_two in normalized_emails or norm_single in normalized_emails:
                            ceo_received += 1
                        else:
                            non_delivered.append({
                                'category': 'CEO',
                                'subject': expected_two,
                                'checked_time': checked_time_str,
                                'priority': entry.get('priority', '')
                            })
            else:
                if is_within_time_range(current_hour, start_hour, end_hour, after_time):
                    expected_two = base_subject
                    norm_two = normalize_subject(expected_two)
                    ceo_expected += 1
                    if norm_two in normalized_emails:
                        ceo_received += 1
                    else:
                        non_delivered.append({
                            'category': 'CEO',
                            'subject': expected_two,
                            'checked_time': checked_time_str,
                            'priority': entry.get('priority', '')
                        })

    ceo_pending = ceo_expected - ceo_received
    ceo_params = (current_hour, ceo_expected, ceo_received, ceo_pending)
    logger.info(f"CEO params: {ceo_params}")
    display(f"CEO Stats: Hour={current_hour}, Expected={ceo_expected}, Received={ceo_received}, Pending={ceo_pending}")
    send_alert(ceo_params, CEO_STATS_API_URL)

    display("Waiting for 3 seconds before Non-CEO stats...")
    time.sleep(3)

    # Non-CEO Statistics
    logger.info("Computing Non-CEO statistics")
    non_ceo_expected = 0
    non_ceo_received = 0
    for entry in subjects_data:
        if not is_valid_day_or_date(entry.get('prefix'), current_time):
            continue
        if entry['category'] == 'CEO':
            continue
        running = entry['running']
        base_subject = entry['base_subject']
        priority = entry.get('priority', '')

        if running == 'hourly' and 'specific_hours' in entry:
            for subj in entry['subjects_for_hours']:
                if subj['hour'] > current_hour:
                    continue
                expected_two = subj['two_digit']
                expected_single = subj['single_digit']
                non_ceo_expected += 1
                norm_two = normalize_subject(expected_two)
                norm_single = normalize_subject(expected_single)
                if norm_two in normalized_emails or norm_single in normalized_emails:
                    non_ceo_received += 1
                else:
                    non_delivered.append({
                        'category': entry['category'],
                        'subject': expected_two,
                        'checked_time': checked_time_str,
                        'priority': priority,
                    })
        else:
            start_hour = entry['start_hour']
            end_hour = entry['end_hour']
            after_time = entry.get('after_time', False)
            if running == 'hourly':
                if is_within_time_range(current_hour, start_hour, end_hour, after_time) and current_hour > start_hour - 1:
                    for hour in range(start_hour - 1, current_hour):
                        if hour < 0:
                            continue
                        hour_time = day_start + timedelta(hours=hour)
                        hour_with_leading_zero = hour_time.strftime("%I")
                        hour_without_leading_zero = hour_with_leading_zero.lstrip('0') or '12'
                        ampm = hour_time.strftime("%p")
                        expected_two = f"{base_subject} [{hour_with_leading_zero} {ampm}]"
                        expected_single = f"{base_subject} [{hour_without_leading_zero} {ampm}]"
                        non_ceo_expected += 1
                        norm_two = normalize_subject(expected_two)
                        norm_single = normalize_subject(expected_single)
                        if norm_two in normalized_emails or norm_single in normalized_emails:
                            non_ceo_received += 1
                        else:
                            non_delivered.append({
                                'category': entry['category'],
                                'subject': expected_two,
                                'checked_time': checked_time_str,
                                'priority': priority,
                            })
            else:
                if is_within_time_range(current_hour, start_hour, end_hour, after_time):
                    expected_two = base_subject
                    norm_two = normalize_subject(expected_two)
                    non_ceo_expected += 1
                    if norm_two in normalized_emails:
                        non_ceo_received += 1
                    else:
                        non_delivered.append({
                            'category': entry['category'],
                            'subject': expected_two,
                            'checked_time': checked_time_str,
                            'priority': priority,
                        })

    non_ceo_pending = non_ceo_expected - non_ceo_received
    non_ceo_params = (current_hour, non_ceo_expected, non_ceo_received, non_ceo_pending)
    logger.info(f"Non-CEO params: {non_ceo_params}")
    display(f"Non-CEO Stats: Hour={current_hour}, Expected={non_ceo_expected}, Received={non_ceo_received}, Pending={non_ceo_pending}")
    send_alert(non_ceo_params, NON_CEO_STATS_API_URL)

    # Update non-delivered list by removing subjects that are now received
    updated_non_delivered = []
    for item in non_delivered:
        norm_subject = normalize_subject(item['subject'])
        if norm_subject not in normalized_emails:
            updated_non_delivered.append(item)
        else:
            logger.info(f"Removing {item['subject']} from non-delivered list as it was received")

    # Combine with existing non-delivered data, keeping only the latest status
    non_delivered_dict = {(item['subject'], item['checked_time']): item for item in updated_non_delivered}
    existing_dict = {(item['subject'], item['checked_time']): item for item in existing_non_delivered}
    combined_dict = {**existing_dict, **non_delivered_dict}  # Newer entries overwrite older ones
    final_non_delivered = list(combined_dict.values())

    # Save non-delivered Parquet file
    if final_non_delivered:
        non_delivered_df = pd.DataFrame(final_non_delivered)
        try:
            os.makedirs(NOT_DELIVERED_DIR, exist_ok=True)
            table = pa.Table.from_pandas(non_delivered_df)
            pq.write_table(table, parquet_file)
            logger.info(f"Saved {len(non_delivered_df)} non-delivered records to {parquet_file}")
        except Exception as e:
            logger.error(f"Error saving non-delivered parquet file: {str(e)}")
    else:
        logger.info("No non-delivered emails to save")
        if os.path.exists(parquet_file):
            try:
                os.remove(parquet_file)
                logger.info(f"Deleted empty non-delivered parquet file: {parquet_file}")
            except Exception as e:
                logger.error(f"Error deleting non-delivered parquet file: {str(e)}")

    # High Alert: CEO + P0 reports
    try:
        high_priority_missing = []
        if final_non_delivered:
            non_delivered_df = pd.DataFrame(final_non_delivered)
            # Filter for CEO or P0 reports and get unique subjects
            high_priority_missing = non_delivered_df[
                (non_delivered_df['category'] == 'CEO') | (non_delivered_df['priority'] == 'P0')
            ]['subject'].unique().tolist()
            
            # Verify against latest emails_data to ensure subjects are still missing
            high_priority_missing = [
                subject for subject in high_priority_missing
                if normalize_subject(subject) not in normalized_emails
            ]

            if high_priority_missing:
                base_message = "*High Alert!* The following expected CEO or P0 reports were not delivered for the day until now: "
                subjects_list = [f"{i+1}. *{subject}*" for i, subject in enumerate(high_priority_missing)]
                subjects_str = ", ".join(subjects_list).rstrip(',')
                alert_message = base_message + subjects_str
                if len(alert_message) > 500:
                    alert_message = alert_message[:497] + "..."
                display(f"Sending high priority alert: {alert_message[:50]}...")
                logger.info(f"Sending high priority alert: {alert_message[:100]}...")
                send_alert(alert_message, RECEIVED_ALERT_API_URL)  # Reverted to original URL
            else:
                logger.info("No missing CEO/P0 reports for high alert")
        else:
            logger.info("No non-delivered records for high alert")
    except Exception as e:
        logger.error(f"Error processing high priority alert: {str(e)}")

    save_stats_to_excel(subjects_data, emails_data, current_date, current_time)
    logger.info("Completed send_statistics_alerts function")

def main():
    display("Running email alert job...")
    logger.info("Starting main function")

    local_tz = ZoneInfo("Asia/Kolkata")
    current_time = datetime.now(local_tz)
    current_hour = current_time.hour
    current_date = current_time.strftime('%Y-%m-%d')
    logger.info(f"Current date: {current_date}, Current hour: {current_hour}, Alert window: {ALERT_START_HOUR}-{ALERT_END_HOUR}")

    display(f"Fetching emails from the last {EMAIL_CHECK_WINDOW_HOURS} hour(s)...")
    emails_data = fetch_emails()
    save_emails_to_parquet(emails_data, current_date)

    display("Waiting for 3 seconds before comparing emails...")
    time.sleep(3)

    emails_data = load_emails_from_parquet(current_date)
    if not emails_data:
        display(f"No emails loaded from Parquet")
        logger.warning(f"No emails loaded from Parquet for {current_date}")
    else:
        logger.info(f"Loaded {len(emails_data)} emails from Parquet")

    sent_alerts_df = load_sent_alerts_history()
    sent_alerts_df = reset_history_if_new_day(sent_alerts_df, current_date)

    subjects_data = load_subjects_from_excel()
    if not subjects_data:
        logger.error("No subjects found in Excel. Job aborted")
        return

    if ALERT_START_HOUR <= current_hour <= ALERT_END_HOUR:
        logger.info(f"Within alert window, processing {len(subjects_data)} subjects for alerts")
        for i, entry in enumerate(subjects_data):
            if not is_valid_day_or_date(entry.get('prefix'), current_time):
                continue
            base_subject = entry['base_subject']
            logger.info(f"Processing subject: {base_subject}")
            category = entry['category']
            running = entry['running']
            priority = entry.get('priority', '')

            if running == 'hourly' and 'specific_hours' in entry:
                for subj in entry['subjects_for_hours']:
                    if subj['hour'] != current_hour:
                        continue
                    two_digit_subject = subj['two_digit']
                    single_digit_subject = subj['single_digit']
                    alert_subject = two_digit_subject
                    received = False
                    received_time = None
                    norm_two = normalize_subject(two_digit_subject)
                    norm_single = normalize_subject(single_digit_subject)
                    for email in emails_data:
                        if normalize_subject(email['Subject']) in [norm_two, norm_single]:
                            received = True
                            received_time = email['Received Time']
                            break
                    if has_alert_been_sent(sent_alerts_df, alert_subject, current_date, running, received):
                        logger.info(f"Alert already sent for '{alert_subject}'")
                        continue
                    not_received_sent = has_not_received_alert_been_sent(sent_alerts_df, alert_subject, current_date)
                    if received:
                        if not_received_sent:
                            try:
                                formatted_time = received_time.strftime('%d/%m/%Y %H:%M')
                                alert_message = f"*Follow-up*: The *{category}* report of *{alert_subject}* was received at {formatted_time}"
                            except Exception as e:
                                logger.warning(f"Error formatting time for '{alert_subject}': {str(e)}")
                                alert_message = f"*Follow-up*: The *{category}* report of *{alert_subject}* was received"
                        else:
                            if priority == 'P0':
                                try:
                                    formatted_time = received_time.strftime('%d/%m/%Y %H:%M')
                                    alert_message = f"The *P0* report of *{alert_subject}* was received at {formatted_time}"
                                except Exception as e:
                                    logger.warning(f"Error formatting time for '{alert_subject}': {str(e)}")
                                    alert_message = f"The *P0* report of *{alert_subject}* was received"
                            else:
                                continue
                    else:
                        alert_message = f"*Warning*: *{category}*: The report *{alert_subject}* was NOT received"
                    alert_type = 'received' if received else 'NOT received'
                    
                    # Determine API URL based on priority
                    if priority == 'P0':
                        api_url = P0_API_URL
                    else:
                        # For non-P0 alerts, use original URLs
                        if received:
                            api_url = NOT_ALERT_API_URL  # For follow-up messages
                        else:
                            api_url = NOT_ALERT_API_URL  # For not received alerts
                    
                    display(f'{alert_message}')
                    logger.info(f"Sending alert for subject '{alert_subject}': {alert_message[:100]}...")
                    send_alert(alert_message, api_url)
                    new_alert = pd.DataFrame({
                        'date': [current_date],
                        'subject': [alert_subject],
                        'alert_type': [alert_type],
                        'not_received_count': [1 if alert_type == 'NOT received' else 0]
                    })
                    sent_alerts_df = pd.concat([sent_alerts_df, new_alert], ignore_index=True)
            else:
                start_hour = entry['start_hour']
                end_hour = entry['end_hour']
                after_time = entry.get('after_time')
                two_digit_subject = entry['two_digit']
                single_digit_subject = entry['single_digit']
                if not is_within_time_range(current_hour, start_hour, end_hour, after_time):
                    logger.info(f"Skipping '{base_subject}' due to time window mismatch")
                    continue
                received = False
                received_time = None
                norm_two = normalize_subject(two_digit_subject)
                norm_single = normalize_subject(single_digit_subject)
                for email in emails_data:
                    if normalize_subject(email['Subject']) in [norm_two, norm_single]:
                        received = True
                        received_time = email['Received Time']
                        break
                alert_subject = two_digit_subject if running == 'hourly' else base_subject
                if has_alert_been_sent(sent_alerts_df, alert_subject, current_date, running, received):
                    logger.info(f"Alert already sent for '{alert_subject}'")
                    continue
                not_received_sent = has_not_received_alert_been_sent(sent_alerts_df, alert_subject, current_date)
                if received:
                    if not_received_sent:
                        try:
                            formatted_time = received_time.strftime('%d/%m/%Y %H:%M')
                            alert_message = f"*Follow-up*: The *{category}* report of *{alert_subject}* was received at {formatted_time}"
                        except Exception as e:
                            logger.warning(f"Error formatting time for '{alert_subject}': {str(e)}")
                            alert_message = f"*Follow-up*: The *{category}* report of *{alert_subject}* was received"
                    else:
                        if priority == 'P0':
                            try:
                                formatted_time = received_time.strftime('%d/%m/%Y %H:%M')
                                alert_message = f"The *P0* report of *{alert_subject}* was received at {formatted_time}"
                            except Exception as e:
                                logger.warning(f"Error formatting time for '{alert_subject}': {str(e)}")
                                alert_message = f"The *P0* report of *{alert_subject}* was received"
                        else:
                            continue
                else:
                    alert_message = f"*Warning*: *{category}*: The report *{alert_subject}* was NOT received"
                alert_type = 'received' if received else 'NOT received'
                
                # Determine API URL based on priority
                if priority == 'P0':
                    api_url = P0_API_URL
                else:
                    # For non-P0 alerts, use original URLs
                    if received:
                        api_url = NOT_ALERT_API_URL  # For follow-up messages
                    else:
                        api_url = NOT_ALERT_API_URL  # For not received alerts
                    
                display(f'{alert_message}')
                logger.info(f"Sending alert for subject '{alert_subject}': {alert_message[:100]}...")
                send_alert(alert_message, api_url)
                new_alert = pd.DataFrame({
                    'date': [current_date],
                    'subject': [alert_subject],
                    'alert_type': [alert_type],
                    'not_received_count': [1 if alert_type == 'NOT received' else 0]
                })
                sent_alerts_df = pd.concat([sent_alerts_df, new_alert], ignore_index=True)

            if (i + 1) % 50 == 0:
                logger.info(f"Processed {i+1}/{len(subjects_data)} subjects")

        save_mail_delivery_status(subjects_data, emails_data, current_date, current_time)
        save_sent_alerts_history(sent_alerts_df)

        display("Waiting for 3 seconds before sending statistics alerts...")
        logger.info("Calling send_statistics_alerts")
        time.sleep(3)
        send_statistics_alerts(subjects_data, emails_data, current_date, current_time)

    else:
        display(f"Alert job skipped - outside allowed time window ({ALERT_START_HOUR:02d}:00-{ALERT_END_HOUR:02d}:00)")
        logger.warning(f"Current hour {current_hour} is outside alert window for {current_date}")
        save_mail_delivery_status(subjects_data, emails_data, current_date, current_time)

    display("Email alert job completed")
    logger.info("Main function completed")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        display(f"Error: {str(e)}")