
#Tasks for ACARS project

from celery import shared_task, chain
from exchangelib import Credentials, Account, Configuration, EWSDateTime, EWSTimeZone
from .utils import process_email_attachment, process_airport_file, process_flight_schedule_file, process_acars_message, process_cargo_email_attachment, process_cargo_flight_schedule_file,process_fdm_email_attachment,process_fdm_flight_schedule_file,process_fdm_crew_email_attachment,process_crew_details_file,process_tableau_data_email_attachment,process_tableau_data_file
import logging
from django.conf import settings
from datetime import datetime
import stat

logger = logging.getLogger(__name__)

def get_exchange_account():
    credentials = Credentials(
        username=settings.EXCHANGE_EMAIL_USER,
        password=settings.EXCHANGE_EMAIL_PASSWORD
    )
    config = Configuration(
        server=settings.EXCHANGE_EMAIL_SERVER,
        credentials=credentials
    )
    account = Account(
        primary_smtp_address=settings.EXCHANGE_EMAIL_USER,
        credentials=credentials,
        autodiscover=False,
        config=config,
        access_type='delegate'
    )
    return account




@shared_task
def fetch_airport_data():
    account = get_exchange_account()
    logger.info("Fetching the most recent airport data email...")

    emails = account.inbox.filter(
        subject__contains="AIMS JOB : #1003 Airport details Feed to WB server + ' file attached"
    ).order_by('-datetime_received')

    
    
    try:
        email = emails[0]
        logger.info(f"Processing airport data email with subject: {email.subject}")
        process_email_attachment(email, process_airport_file)
        # Trigger flight schedule task immediately after processing airport data
        fetch_flight_schedules.apply_async()
    except IndexError:
        logger.info("No airport data email found.")
    except Exception as e:
        logger.error(f"Error processing airport data email: {e}")
        raise




@shared_task
def fetch_flight_schedules():
    account = get_exchange_account()
    logger.info("Fetching the most recent flight schedule email...")

    emails = account.inbox.filter(
        subject__contains="AIMS JOB : #1002 Flight schedule feed to WB server + ' file attached"
    ).order_by('-datetime_received')
    
    try:
        email = emails[0]
        logger.info(f"Processing the most recent flight schedule email with subject: {email.subject}")
        process_email_attachment(email, process_flight_schedule_file)
    except IndexError:
        logger.info("No new flight schedule email found.")
    except Exception as e:
        logger.error(f"Error processing flight schedule email: {e}")
        raise

@shared_task
def cargo_fetch_flight_schedules():
    account = get_exchange_account()
    logger.info("Fetching the most recent cargo flight schedule email for cargo website...")

    emails = account.inbox.filter(
        subject__contains='AIMS JOB : #1002 Flight schedule feed to Cargo website file attached'
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing the most recent cargo flight schedule email with subject: {email.subject}")
        process_cargo_email_attachment(email, process_cargo_flight_schedule_file)
    else:
        logger.info("No new cargo flight schedule email found.")


# from celery import shared_task
# from celery.utils.log import get_task_logger
# import os
# from .utils import  process_acars_message

# logger = get_task_logger(__name__)

# @shared_task(bind=True)
# def fetch_acars_messages(self):
#     account = get_exchange_account()
#     logger.info("Fetching and processing ACARS messages...")

#     # Define the file path for JOB1.txt
#     file_path = os.path.join(settings.MEDIA_ROOT, "JOB1.txt")

#     # Start with a fresh file for each task execution
#     open(file_path, 'w').close()  # Clear file contents

#     # Fetch all unread ACARS messages
#     emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')

#     if not emails.exists():
#         logger.info("No unread ACARS messages found. Skipping processing.")
#         return

#     # Process each email
#     for item in emails:
#         if "M16" in item.body:
#             logger.info(f"Skipping 'M16' ACARS message: {item.subject}")
#             item.is_read = True
#             item.save(update_fields=['is_read'])
#             continue

#         logger.info(f"Processing ACARS email with subject: {item.subject}")
#         process_acars_message(item, file_path)

#         # Mark the email as read to avoid reprocessing
#         item.is_read = True
#         item.save(update_fields=['is_read'])

#     # Upload the file to the AIMS server after processing the batch
#     if os.path.getsize(file_path) > 0:  # Ensure the file is not empty
#         logger.info(f"Uploading {file_path} to AIMS server...")
#         upload_acars_to_aims_server(file_path)
#     else:
#         logger.info(f"{file_path} is empty. Skipping upload.")

#     logger.info("Batch processing of ACARS emails completed.")


import os
import shutil
from django.conf import settings
from django.utils.timezone import now
from celery import shared_task
import logging

logger = logging.getLogger(__name__)

@shared_task(bind=True)
def fetch_acars_messages(self):
    account = get_exchange_account()
    logger.info("Fetching and processing ACARS messages...")

    # 1) Define the “live” file and the backup directory
    file_path = os.path.join(settings.MEDIA_ROOT, "JOB1.txt")
    backup_dir = os.path.join(settings.MEDIA_ROOT, "job1_backups")

    # 2) Create the backup directory if it doesn’t exist
    os.makedirs(backup_dir, exist_ok=True)

    # 3) If there's an existing non-empty JOB1.txt, back it up
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        ts = now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"JOB1_{ts}.txt"
        backup_path = os.path.join(backup_dir, backup_name)
        shutil.copy(file_path, backup_path)
        logger.info(f"Backed up previous JOB1.txt to {backup_path}")

    # 4) Clear out the live file so you start fresh
    open(file_path, 'w').close()

    # 5) Fetch and process unread ACARS messages
    emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')
    if not emails.exists():
        logger.info("No unread ACARS messages found. Skipping processing.")
        return

    for item in emails:
        if "M16" in item.body:
            logger.info(f"Skipping 'M16' ACARS message: {item.subject}")
            item.is_read = True
            item.save(update_fields=['is_read'])
            continue

        logger.info(f"Processing ACARS email with subject: {item.subject}")
        process_acars_message(item, file_path)
        item.is_read = True
        item.save(update_fields=['is_read'])

    # 6) Upload the new file if it has content
    if os.path.getsize(file_path) > 0:
        logger.info(f"Uploading {file_path} to AIMS server...")
        upload_acars_to_aims_server(file_path)
    else:
        logger.info(f"{file_path} is empty. Skipping upload.")

    logger.info("Batch processing of ACARS emails completed.")



import os
import paramiko
import time
import logging
from paramiko.ssh_exception import SSHException, NoValidConnectionsError

# Initialize logger
logger = logging.getLogger(__name__)

def upload_acars_to_aims_server(local_file_path):
    # Server credentials
    aims_host = settings.AIMS_SERVER_ADDRESS
    aims_port = int(settings.AIMS_PORT)
    aims_username = settings.AIMS_SERVER_USER
    aims_password = settings.AIMS_SERVER_PASSWORD
    destination_dir = settings.AIMS_SERVER_DESTINATION_PATH

    # Retry configuration
    attempts = 3  # Number of retries
    delay = 5  # Delay in seconds between retries

    try:
        # Ensure the local file exists before proceeding
        if not os.path.exists(local_file_path):
            logger.error(f"File {local_file_path} does not exist.")
            return

        # Ensure the remote path includes the file name
        remote_path = os.path.join(destination_dir, os.path.basename(local_file_path))
        logger.info(f"Preparing to upload file to: {remote_path}")

        # Attempt to upload with retries
        for attempt in range(attempts):
            try:
                # Create SSH transport and SFTP session
                transport = paramiko.Transport((aims_host, aims_port))
                transport.connect(username=aims_username, password=aims_password)
                sftp = paramiko.SFTPClient.from_transport(transport)

                # Upload the file
                sftp.put(local_file_path, remote_path)
                logger.info(f"File successfully uploaded to {remote_path} on AIMS server.")
                
                # Close the SFTP session and transport after successful upload
                sftp.close()
                transport.close()
                break  # Exit the retry loop on success

            except (SSHException, NoValidConnectionsError) as e:
                # Specific exceptions related to SSH or SFTP errors
                logger.error(f"SSH/SFTP error on attempt {attempt + 1}: {e}", exc_info=True)
            except Exception as e:
                # Generic exception handling
                logger.error(f"Failed to upload on attempt {attempt + 1}: {e}", exc_info=True)
            
            # Retry logic with delay
            if attempt < attempts - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to upload file after {attempts} attempts.")

    except Exception as e:
        logger.error(f"Error during file upload process: {e}", exc_info=True)





#Tasks for CPAT project

import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from django.conf import settings
from .models import CompletionRecord
import paramiko
import logging
from celery import shared_task

logger = logging.getLogger(__name__)

# Settings
LMS_BASE_URL = settings.LMS_BASE_URL
LMS_KEY = settings.LMS_KEY
API_TOKEN = settings.API_TOKEN
DAYS = settings.DAYS
AIMS_HOST = settings.AIMS_SERVER_ADDRESS
AIMS_PORT = int(settings.AIMS_PORT)
AIMS_USERNAME = settings.AIMS_SERVER_USER
AIMS_PASSWORD = settings.AIMS_SERVER_PASSWORD
CPAT_AIMS_PATH = settings.CPAT_AIMS_PATH

# Validity periods for courses
VALIDITY_PERIODS = {
    "FRMS": 12,     # Fatigue Education & Awareness Training
    "ETPG": 36,     # ETOPS Ground
    "LVO-G": 36,    # LVO Ground    
    "PBNGRN": 12,   # PBN Ground
    "RVSMGS": 0,    # RVSM Ground (never expires)
    "A330C1": 6,
    "FAS": 12,
    "B737C1": 6,
    "Q400C1": 6,
    "TCAS": 36,
    "ADW": 36,
    "PWS": 36,
}


from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar

def calculate_expiry_date(completion_date_str, course_code):
    """
    Calculate expiry date based on completion date in DDMMYYYY format
    and adjust to the last day of the expiry month.
    """
    if not completion_date_str:
        return ""  # No completion date available

    try:
        # Parse the input date in DDMMYYYY format
        completion_date = datetime.strptime(completion_date_str, "%d%m%Y")
        validity_period = VALIDITY_PERIODS.get(course_code, 0)
        
        if validity_period == 0:
            return ""  # No expiry date (never expires)

        # Calculate the tentative expiry date
        expiry_date = completion_date + relativedelta(months=validity_period)
        # Adjust to the last day of the expiry month
        last_day = calendar.monthrange(expiry_date.year, expiry_date.month)[1]
        expiry_date = expiry_date.replace(day=last_day)
        return expiry_date.strftime("%d%m%Y")  # Return as DDMMYYYY
    except ValueError:
        logger.error(f"Invalid completion date format: {completion_date_str}")
        return ""



def format_date(date_str):
    """Convert date from DDMMYYYY to DDMMYYYY format or return an empty string."""
    if date_str:
        try:
            # Parse DDMMYYYY format and return it as is (since no transformation is needed)
            datetime.strptime(date_str, "%d%m%Y")
            return date_str  # Already in DDMMYYYY format
        except ValueError:
            logger.error(f"Invalid date format: {date_str}")
            return ""
    return ""


def remove_wb_prefix(employee_id):
    """Remove 'WB' prefix from the employee ID."""
    return employee_id.replace("WB", "") if employee_id else ""

def generate_job8_file(records):
    """Generate the JOB8.txt file."""
    file_path = os.path.join(settings.MEDIA_ROOT, "JOB8.txt")
    with open(file_path, "w", newline="") as file:
        for record in records:
            # Extract fields
            staff_number = remove_wb_prefix(record["StaffNumber"])  # Strip 'WB' prefix
            expiry_code = record["ExpiryCode"]  # Write as-is
            last_done_date = record["LastDoneDate"] or ""  # Leave blank if missing
            expiry_date = record["ExpiryDate"] or ""  # Leave blank if missing

            # Write the row to the file
            file.write(f"{staff_number},{expiry_code},{last_done_date},{expiry_date}\n")

    logger.info(f"JOB8.txt file created at: {file_path}")
    return file_path

@shared_task
def fetch_and_store_completion_records():
    """Fetch CPAT completion records, update database, generate JOB8.txt, and send it."""
    url = f"{LMS_BASE_URL}/lms/api/IntegrationAPI/comp/v2/{LMS_KEY}/{DAYS}"
    headers = {
        "apitoken": API_TOKEN,
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        records_for_file = []

        # Process each record
        for record in data:
            try:
                employee_id = record.get("employeeID")
                course_code = record.get("courseCode")
                completion_date = record.get("completionDate")
                start_date = record.get("startDate")
                end_date = record.get("endDate")

                # Skip records with missing employeeID or courseCode
                if not employee_id or not course_code:
                    logger.warning(f"Skipping record due to missing EmployeeID or CourseCode: {record}")
                    continue

                # Parse dates in DDMMYYYY format
                parsed_completion_date = datetime.strptime(completion_date, "%d%m%Y").date() if completion_date else None
                parsed_start_date = datetime.strptime(start_date, "%d%m%Y").date() if start_date else None
                parsed_end_date = datetime.strptime(end_date, "%d%m%Y").date() if end_date else None

                # Check for existing record
                existing_record = CompletionRecord.objects.filter(
                    employee_id=employee_id,
                    course_code=course_code,
                    completion_date=parsed_completion_date,
                ).first()

                if existing_record:
                    is_identical = (
                        existing_record.employee_email == record.get("employeeEmail") and
                        existing_record.score == record.get("score") and
                        existing_record.time_in_seconds == record.get("timeInSecond") and
                        existing_record.start_date == parsed_start_date and
                        existing_record.end_date == parsed_end_date
                    )
                    if is_identical:
                        logger.info(f"Skipping identical record: {employee_id} - {course_code}")
                        continue

                # Update or create the record in the database
                CompletionRecord.objects.update_or_create(
                    employee_id=employee_id,
                    course_code=course_code,
                    completion_date=parsed_completion_date,
                    defaults={
                        "employee_email": record.get("employeeEmail"),
                        "score": record.get("score"),
                        "time_in_seconds": record.get("timeInSecond"),
                        "start_date": parsed_start_date,
                        "end_date": parsed_end_date,
                    },
                )

                # Prepare record for JOB8.txt
                records_for_file.append({
                    "StaffNumber": employee_id.replace("WB", ""),  # Strip "WB" prefix
                    "ExpiryCode": course_code,
                    "LastDoneDate": format_date(completion_date),
                    "ExpiryDate": calculate_expiry_date(completion_date, course_code),
                })
            except ValueError as e:
                logger.error(f"Date parsing error for record: {record} - {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing record: {record} - {e}", exc_info=True)
                continue

        # Generate JOB8.txt only if records exist
        if not records_for_file:
            logger.info("No valid records to process for JOB8.txt.")
            return

        file_path = generate_job8_file(records_for_file)
        upload_cpat_to_aims_server(file_path)

        logger.info("CPAT completion records processed successfully.")
    else:
        logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")


def upload_cpat_to_aims_server(local_file_path):
    """Upload JOB8.txt to the AIMS server."""
    attempts = 3
    delay = 5

    try:
        if not os.path.exists(local_file_path):
            logger.error(f"File {local_file_path} does not exist.")
            return

        cpat_remote_path = os.path.join(CPAT_AIMS_PATH, os.path.basename(local_file_path))
        logger.info(f"Uploading JOB8.txt to: {cpat_remote_path}")

        for attempt in range(attempts):
            try:
                transport = paramiko.Transport((AIMS_HOST, AIMS_PORT))
                transport.connect(username=AIMS_USERNAME, password=AIMS_PASSWORD)
                sftp = paramiko.SFTPClient.from_transport(transport)

                sftp.put(local_file_path, cpat_remote_path)
                logger.info(f"JOB8.txt uploaded successfully to {cpat_remote_path}.")
                
                sftp.close()
                transport.close()
                break
            except Exception as e:
                logger.error(f"Upload attempt {attempt + 1} failed: {e}", exc_info=True)
                if attempt < attempts - 1:
                    time.sleep(delay)
    except Exception as e:
        logger.error(f"Error during JOB8.txt upload: {e}", exc_info=True)



# Send CPAT expiry notifications

# aimsintegration/tasks.py

from datetime import date
from dateutil.relativedelta import relativedelta

from celery import shared_task
from django.core.mail import send_mail
from django.conf import settings

from .models import CompletionRecord
from .tasks import VALIDITY_PERIODS, calculate_expiry_date

@shared_task
def send_cpat_expiry_notifications():
    """
    Runs daily. For each 24- or 36-month CPAT course whose expiry falls
    in this calendar year, if today == (expiry_date - 2 months), send one email.
    """
    today = date.today()
    notices = []

    for rec in CompletionRecord.objects.all():
        months = VALIDITY_PERIODS.get(rec.course_code, 0)
        if months not in (24, 36):
            continue

        # calculate expiry as date
        comp_str = rec.completion_date.strftime("%d%m%Y") if rec.completion_date else ""
        expiry_str = calculate_expiry_date(comp_str, rec.course_code)
        if not expiry_str:
            continue

        exp_day = int(expiry_str[:2])
        exp_mon = int(expiry_str[2:4])
        exp_yr  = int(expiry_str[4:])
        expiry_date = date(exp_yr, exp_mon, exp_day)

        # only courses expiring this year
        if expiry_date.year != today.year:
            continue

        # send exactly 2 months prior
        if expiry_date - relativedelta(months=2) == today:
            notices.append((rec.employee_id, rec.course_code, rec.completion_date, expiry_date))

    if not notices:
        return f"No expiry notifications to send on {today}."

    # Build professional email
    lines = [
        "Dear Training Record Team,",
        "",
        "**Please do not reply to this automated message.**",
        "",
        "The following CPAT courses (24- & 36-month validity) will expire in two months:",
        ""
    ]
    for emp_id, code, comp_date, exp_date in notices:
        lines.append(f"- Employee: {emp_id} | Course: {code} | Completed: {comp_date} → Expires: {exp_date}")
    lines += [
        "",
        "Kindly arrange refresher training so that all records remain current.",
        "",
        "Best regards,",
        "CPAT Automated Notification System"
    ]

    subject = "CPAT Expiry Reminder – Action Required"
    message = "\n".join(lines)

    send_mail(
        subject=subject,
        message=message,
        from_email=settings.EXCHANGE_EMAIL_USER,
        recipient_list=[
            settings.FIRST_EMAIL_RECEIVER,
            settings.THIRD_EMAIL_RECEIVER,
            settings.FOURTH_EMAIL_RECEIVER,
        ],
        fail_silently=False,
    )

    return f"Sent {len(notices)} CPAT expiry reminder(s) on {today}."



#TASK for FDM project

# @shared_task
# def fetch_fdm_flight_schedules():
#     account = get_exchange_account()
#     logger.info("Fetching the most recent fdm flight schedule email...")

#     emails = account.inbox.filter(
#         subject__contains="AIMS JOB : #1002.C Flight schedule feed to FDM + ' file attached'"
#     ).order_by('-datetime_received')
    
#     email = emails[0] if emails else None

#     if email:
#         logger.info(f"Processing the most recent fdm flight schedule email with subject: {email.subject}")
#         process_fdm_email_attachment(email, process_fdm_flight_schedule_file)
#     else:
#         logger.info("No new fdm flight schedule email found.")

@shared_task
def fetch_fdm_flight_schedules():
    account = get_exchange_account()
    logger.info("Fetching the most recent fdm flight schedule email...")

    emails = account.inbox.filter(
        subject__contains="AIMS JOB : #1002.C Flight schedule feed to FDM + ' file attached"
    ).order_by('-datetime_received')
    
    # Check if emails exist before accessing
    if emails.exists():
        email = emails[0]
        logger.info(f"Processing the most recent  fdm flight schedule email with subject: {email.subject}")
        process_fdm_email_attachment(email, process_fdm_flight_schedule_file)
    else:
        logger.info("No new fdm flight schedule email found.")



@shared_task
def fetch_fdm_crew_data():
    account = get_exchange_account()
    logger.info("Fetching the most recent fdm crew data...")

    emails = account.inbox.filter(
        subject__contains="AIMS JOB : #16.A Job 16.A: Download List of flight legs with crew names to FDM + ' file attached"
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing the most recent fdm crew data email with subject: {email.subject}")
        process_fdm_crew_email_attachment(email, process_crew_details_file)
    else:
        logger.info("No new fdm flight schedule email found.")






# FDM project tasks



import csv
import os
import logging
from django.conf import settings
from django.utils.timezone import now, timedelta
from .models import FdmFlightData, CrewMember

logger = logging.getLogger(__name__)

def fetch_recent_flights_and_crew():
    """
    Fetch flights updated within the last hour 
    and corresponding crew data, matched on (flight_no, sd_date_utc, origin, destination).
    """
    one_hour_ago = now() - timedelta(hours=1)
    
    # 1) Flights updated in the last hour
    flight_data = FdmFlightData.objects.filter(updated_at__gte=one_hour_ago)
    
    # 2) Crew for those flights
    crew_data = CrewMember.objects.filter(
        flight_no__in=flight_data.values_list('flight_no', flat=True),
        sd_date_utc__in=flight_data.values_list('sd_date_utc', flat=True),
        origin__in=flight_data.values_list('dep_code_icao', flat=True),
        destination__in=flight_data.values_list('arr_code_icao', flat=True)
    ).order_by("id")  # Ensure crew members are ordered by database insertion ID
    
    return flight_data, crew_data

def generate_csv_for_fdm(flight_data, crew_data):
    """
    Generate a CSV with columns: DAY, FLT, FLTYPE, REG, DEP, ARR, STD, STA, TKOF, TDOWN, BLOF, BLON,
    ETD, ETA, ATD, OFF, ON, ATA, CP, FO,3P
    """
    file_name = f"aims_{now().strftime('%Y%m%d%H%M')}.csv"
    file_path = os.path.join(settings.MEDIA_ROOT, file_name)

    # Build a lookup: flight_key -> { "CP": [...], "FO": [...] }
    flight_crew_lookup = {}
    for c in crew_data:
        key = (c.flight_no, c.sd_date_utc, c.origin, c.destination)
        if key not in flight_crew_lookup:
            flight_crew_lookup[key] = {"CP": [], "FO": []}
        
        # Only store CP/FO
        flight_crew_lookup[key][c.role].append((c.id, str(c.crew_id)))  # Store tuple (id, crew_id)

    # Define the header
    header = [
        "DAY", "FLT", "FLTYPE", "REG", "DEP", "ARR", "STD", "STA", "TKOF", "TDOWN", "BLOF", "BLON",
        "ETD", "ETA", "ATD", "OFF", "ON", "ATA", "CP", "FO","3P"
    ]

    # Write the CSV
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)  # Header row

        def format_time(t):
            return t.strftime("%H:%M") if t else ""

        for flight in flight_data:
            # Build the lookup key
            key = (
                flight.flight_no,
                flight.sd_date_utc,
                flight.dep_code_icao,
                flight.arr_code_icao
            )

            # Get any CP/FO crew for this flight
            crew_dict = flight_crew_lookup.get(key, {"CP": [], "FO": []})
            
            # Sort by database ID to maintain insertion order
            cp_list = sorted(crew_dict["CP"], key=lambda x: x[0])  # Sort by ID
            fo_list = sorted(crew_dict["FO"], key=lambda x: x[0])  # Sort by ID

            # Assign CP and FO values based on the specified cases
            cp = cp_list[0][1] if len(cp_list) > 0 else ""
            fo = fo_list[0][1] if len(fo_list) > 0 else ""
            
            # Special case: If no FO but two CPs, assign second CP to FO
            if not fo and len(cp_list) > 1:
                fo = cp_list[1][1]

            # Recheck if there is a CP with a lower ID than the selected one
            if cp_list:
                lowest_cp = min(cp_list, key=lambda x: x[0])[1]  # Get crew_id of lowest ID
                cp = lowest_cp

            # Recheck if there is an FO with a lower ID than the selected one
            if fo_list:
                lowest_fo = min(fo_list, key=lambda x: x[0])[1]  # Get crew_id of lowest ID
                fo = lowest_fo

            # Build row
            row = [
                flight.sd_date_utc,                 # DAY
                flight.flight_no,                   # FLT
                flight.flight_type,                 # FLTYPE
                flight.tail_no,                     # REG
                flight.dep_code_icao,               # DEP
                flight.arr_code_icao,               # ARR
                format_time(flight.std_utc),        # STD
                format_time(flight.sta_utc),        # STA
                format_time(flight.takeoff_utc),    # TKOF
                format_time(flight.touchdown_utc),  # TDOWN
                format_time(flight.atd_utc),        # BLOF
                format_time(flight.ata_utc),        # BLON
                format_time(flight.etd_utc),        # ETD
                format_time(flight.eta_utc),        # ETA
                format_time(flight.atd_utc),        # ATD
                format_time(flight.takeoff_utc),    # OFF
                format_time(flight.touchdown_utc),  # ON
                format_time(flight.ata_utc),        # ATA
                cp,                                 # CP
                fo,                                  # FO
                 "" 
            ]
            writer.writerow(row)

    logger.info(f"CSV file generated at: {file_path}")
    return file_path



import os
import paramiko
import logging
from django.utils.timezone import now, timedelta
from django.conf import settings
from paramiko.ssh_exception import SSHException, NoValidConnectionsError
from celery import shared_task

from .models import FdmFlightData, CrewMember


logger = logging.getLogger(__name__)

@shared_task
def hourly_upload_csv_to_fdm():
    """
    Celery task to generate and upload a CSV to the FDM server,
    focusing only on flight records with complete actual timings,
    including up to 3 CPs and 2 FOs in separate columns.
    Then clean up old files except the one just uploaded.
    """
    # Calculate the time range
    one_hour_ago = now() - timedelta(hours=1)

    # Fetch flights with complete actual timings, updated in the last hour
    flight_data = FdmFlightData.objects.filter(
        updated_at__gte=one_hour_ago,
        atd_utc__isnull=False,
        takeoff_utc__isnull=False,
        touchdown_utc__isnull=False,
        ata_utc__isnull=False
    )

    # If no flights qualify, skip
    if not flight_data.exists():
        logger.info("No flight records with complete actual timings to process. Skipping upload.")
        return

    # Fetch the matching crew data for these flights
    crew_data = CrewMember.objects.filter(
        flight_no__in=flight_data.values_list('flight_no', flat=True),
        sd_date_utc__in=flight_data.values_list('sd_date_utc', flat=True),
        origin__in=flight_data.values_list('dep_code_icao', flat=True),
        destination__in=flight_data.values_list('arr_code_icao', flat=True)
    )

    # Generate the CSV file (using your new function that has CP1..CP3, FO1..FO2 columns)
    local_file_path = generate_csv_for_fdm(flight_data, crew_data)

    # Fetch server details from settings
    fdm_host = settings.FDM_HOST
    fdm_port = int(settings.FDM_PORT)
    fdm_username = settings.FDM_USERNAME
    fdm_password = settings.FDM_PASSWORD
    fdm_destination_dir = settings.FDM_DESTINATION_DIR

    # Ensure the file exists
    if not os.path.exists(local_file_path):
        logger.error(f"File {local_file_path} does not exist. Skipping upload.")
        return

    try:
        # SFTP upload logic
        transport = paramiko.Transport((fdm_host, fdm_port))
        transport.connect(username=fdm_username, password=fdm_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Define remote path
        remote_path = os.path.join(fdm_destination_dir, os.path.basename(local_file_path))
        logger.info(f"Uploading file to {remote_path}...")

        # Upload file
        sftp.put(local_file_path, remote_path)
        logger.info(f"File successfully uploaded to {remote_path}.")

        # Close connections
        sftp.close()
        transport.close()

        # Clean up local files (exclude the one we just created)
        local_dir = os.path.dirname(local_file_path)
        for file in os.listdir(local_dir):
            file_path = os.path.join(local_dir, file)
            # Skip the currently uploaded file
            if file_path != local_file_path and file.startswith("aims_") and file.endswith(".csv"):
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted old local file: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete old file {file_path}: {e}")

    except (SSHException, NoValidConnectionsError) as e:
        logger.error(f"SFTP upload failed: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during file upload: {e}", exc_info=True)





#Tableau Project

@shared_task
def fetch_tableau():
    account = get_exchange_account()
    logger.info("Fetching the most recent tableau email...")

    emails = account.inbox.filter(
        subject__contains="AIMS JOB : #1011 Flight OPS stat dashboard + ' file attached"
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing the most recent tableau data email with subject: {email.subject}")
        process_tableau_data_email_attachment(email, process_tableau_data_file)
    else:
        logger.info("No new tableau email found.")





# Recommended imports
import csv
import os
import time
import logging
import uuid
from datetime import datetime, timedelta
import pytz

# Django imports
from django.conf import settings
from django.core.mail import EmailMessage, get_connection
from django.db import transaction
from django.utils import timezone as django_timezone

# Celery imports
from celery import shared_task
from celery.utils.log import get_task_logger
from exchangelib.errors import ErrorServerBusy
# Your models
from .models import DreammilesCampaign, DreamilesEmailRecord

# Initialize loggers
logger = logging.getLogger(__name__)
task_logger = get_task_logger(__name__)

@shared_task(bind=True, max_retries=5)
def delete_old_emails(self):
    """
    Deletes all emails that are older than 7 days in the inbox,
    with a retry strategy to handle ErrorServerBusy.
    """
    account = get_exchange_account()
    days_to_keep = 10  # Fixed from 'l7'

    # Create a Python datetime in UTC and convert to EWSDateTime
    cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
    cutoff_date = cutoff_date.replace(tzinfo=pytz.UTC)  # Use pytz.UTC instead of timezone.utc
    threshold_datetime = EWSDateTime.from_datetime(cutoff_date)

    try:
        logger.info(f"Filtering emails older than {days_to_keep} days (before {threshold_datetime})...")
        old_emails = account.inbox.filter(datetime_received__lt=threshold_datetime)
        
        count_old = old_emails.count()
        logger.info(f"Found {count_old} email(s) older than {days_to_keep} days. Proceeding to delete them...")

        if count_old > 0:
            old_emails.delete()
            logger.info(f"Deleted {count_old} old email(s).")
        else:
            logger.info("No old emails to delete.")

        return f"Purged {count_old} email(s) older than {days_to_keep} days."

    except ErrorServerBusy as e:
        # If the Exchange server is busy, wait and retry
        wait_time = 2 ** self.request.retries * 5  # Exponential backoff
        logger.warning(
            f"Server is busy (attempt {self.request.retries + 1}/{self.max_retries}). "
            f"Retrying in {wait_time} seconds..."
        )

        # Retry the task after `wait_time` seconds
        raise self.retry(exc=e, countdown=wait_time)

# =======================================================================

# DREAMMILES 

# =======================================================================


# Best practice: Define constants at the top
DEFAULT_EMAIL_BATCH_SIZE = 2500  # More conservative approach
EMAIL_CHUNK_SIZE = 100
EMAIL_MICRO_BATCH_SIZE = 20
BATCH_DELAY_SECONDS = 1800  # 30 minutes
CHUNK_DELAY_SECONDS = 1
MICRO_BATCH_DELAY_SECONDS = 0.2


def extract_first_name(email, tier=None):
    """Extract first name from email with fallbacks"""
    try:
        username = email.split('@')[0].lower()
        
        for separator in ['.', '-', '_']:
            if separator in username:
                first_part = username.split(separator)[0]
                if len(first_part) > 1:
                    return first_part.capitalize()
        
        if 2 < len(username) < 30 and username.isalpha():
            return username.capitalize()
        
        if tier and tier not in ['', 'None', 'NULL']:
            return f"{tier} Member"
            
    except Exception:
        pass
    
    return "Dreammiles Member"


@shared_task
def check_and_start_dreammiles_campaign():
    """
    Checks if an active campaign exists; if not, start a new one.
    This task runs every hour.
    """
    # Check for active campaigns
    active_campaign = DreammilesCampaign.objects.filter(
        status__in=['importing', 'processing']
    ).first()
    
    if active_campaign:
        # Best practice: Check for stalled campaigns
        if active_campaign.last_sent_at:
            stalled_threshold = django_timezone.now() - timedelta(hours=4)
            if active_campaign.last_sent_at < stalled_threshold:
                logger.warning(f"Campaign {active_campaign.name} appears stalled. Restarting...")
                process_dreammiles_batch.delay(str(active_campaign.id))
                return {"status": "restarted", "message": "Stalled campaign restarted"}
        
        logger.info(f"Active campaign exists: {active_campaign.name} (ID: {active_campaign.id})")
        return {"status": "skipped", "message": "Active campaign exists"}
    
    # Best practice: Use settings for data paths
    csv_path = getattr(settings, 'DREAMMILES_CSV_PATH', 
                      os.path.join(settings.BASE_DIR, 'data', 'dreamilesnotice.csv'))
    
    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found at {csv_path}")
        return {"status": "error", "message": "CSV file not found"}
    
    # Start a new campaign
    try:
        # Best practice: Use timezone-aware datetimes
        campaign = DreammilesCampaign.objects.create(
            name=f"Dreammiles Business Lite Promotion - {django_timezone.now().strftime('%Y-%m-%d')}",
            subject="Upgrade to Business Lite and Fly in Style",
            email_body="""Dear {first_name},

We have something exciting for you

Did you know that you can now stretch out in style, sip the finest drinks, savour gourmet meals, skip the queues, and lounge like a VIP — Book now and enjoy our Business Lite"""
        )
        
        # Start import process
        import_dreammiles_csv.delay(str(campaign.id), csv_path)
        
        logger.info(f"Started new campaign: {campaign.name} (ID: {campaign.id})")
        return {"status": "success", "message": "New campaign started", "campaign_id": str(campaign.id)}
        
    except Exception as e:
        logger.error(f"Error starting campaign: {str(e)}")
        return {"status": "error", "message": str(e)}


@shared_task
def import_dreammiles_csv(campaign_id, csv_path, batch_size=5000):
    """Process the CSV file and create email records"""
    try:
        campaign = DreammilesCampaign.objects.get(id=campaign_id)
        logger.info(f"Importing CSV for campaign: {campaign.name}")
        
        # Track stats
        total_records = 0
        valid_records = 0
        duplicate_records = 0
        
        # Best practice: Use context manager for file operations
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            
            # Check if first row might be a header
            first_row = next(reader, None)
            if first_row and not any('@' in col for col in first_row if col):
                logger.info("Detected header row in CSV file")
            else:
                # Process as data row
                csvfile.seek(0)  # Reset to beginning
                
            # Process all rows
            records_to_create = []
            batch_number = 0
            
            for row in reader:
                if len(row) < 2:
                    continue
                
                total_records += 1
                
                member_id = row[0].strip() if row[0] else ''
                email = row[1].strip().lower() if row[1] else ''
                tier = row[2].strip() if len(row) > 2 and row[2] else None
                
                # Validate email
                if not email or '@' not in email:
                    continue
                
                # Best practice: More conservative batch sizing for better delivery
                # Each batch contains DEFAULT_EMAIL_BATCH_SIZE emails (2500)
                batch_number = total_records // DEFAULT_EMAIL_BATCH_SIZE
                
                # Extract first name
                first_name = extract_first_name(email, tier)
                
                # Add to batch
                records_to_create.append(DreamilesEmailRecord(
                    campaign=campaign,
                    member_id=member_id,
                    email=email,
                    tier=tier,
                    first_name=first_name,
                    batch_number=batch_number,
                    status='pending'
                ))
                
                valid_records += 1
                
                # Process in batches to manage memory
                if len(records_to_create) >= batch_size:
                    # Best practice: Better error handling with specific exception types
                    try:
                        with transaction.atomic():
                            # Best practice: Use smaller batch_size for bulk_create
                            DreamilesEmailRecord.objects.bulk_create(
                                records_to_create,
                                batch_size=1000,
                                ignore_conflicts=True
                            )
                    except Exception as e:
                        logger.error(f"Error creating email records: {str(e)}")
                        # Fallback to one-by-one if bulk create fails
                        for record in records_to_create:
                            try:
                                record.save()
                            except Exception:
                                duplicate_records += 1
                    
                    # Performance monitoring
                    logger.info(f"Processed {valid_records} records so far...")
                    records_to_create = []
            
            # Process any remaining records
            if records_to_create:
                try:
                    with transaction.atomic():
                        DreamilesEmailRecord.objects.bulk_create(
                            records_to_create,
                            batch_size=1000,
                            ignore_conflicts=True
                        )
                except Exception as e:
                    logger.error(f"Error creating remaining email records: {str(e)}")
                    # Fallback
                    for record in records_to_create:
                        try:
                            record.save()
                        except Exception:
                            duplicate_records += 1
        
        # Update campaign
        total_recipients = DreamilesEmailRecord.objects.filter(campaign=campaign).count()
        campaign.total_recipients = total_recipients
        campaign.csv_processed = True
        campaign.status = 'processing'  # Ready for sending
        campaign.save(update_fields=['total_recipients', 'csv_processed', 'status'])
        
        logger.info(f"CSV import completed: {valid_records} valid records, {duplicate_records} duplicates")
        
        # Start processing the first batch immediately
        process_dreammiles_batch.delay(campaign_id, 0)
        
        return {
            "status": "success",
            "total_records": total_records,
            "valid_records": valid_records,
            "duplicate_records": duplicate_records
        }
        
    except DreammilesCampaign.DoesNotExist:
        logger.error(f"Campaign not found with ID: {campaign_id}")
        return {"status": "error", "message": "Campaign not found"}
    
    except Exception as e:
        logger.error(f"Error importing CSV: {str(e)}")
        return {"status": "error", "message": str(e)}


@shared_task
def process_dreammiles_batch(campaign_id, batch_number=None, max_emails=None):
    """
    Process a batch of emails for sending.
    If batch_number is None, it will use the campaign's current_batch.
    """
    start_time = time.time()
    
    try:
        # Best practice: Respect email provider limits
        if max_emails is None:
            max_emails = DEFAULT_EMAIL_BATCH_SIZE
        
        # Get provider rate limit if configured
        provider_hourly_limit = getattr(settings, 'EMAIL_HOURLY_LIMIT', None)
        if provider_hourly_limit:
            max_emails = min(max_emails, provider_hourly_limit)
        
        # Get campaign and validate status
        campaign = DreammilesCampaign.objects.get(id=campaign_id)
        
        if campaign.status != 'processing':
            logger.info(f"Campaign {campaign.name} is not in processing state: {campaign.status}")
            return {"status": "skipped", "message": f"Campaign status is {campaign.status}"}
        
        # Determine which batch to process
        if batch_number is None:
            batch_number = campaign.current_batch
        
        logger.info(f"Processing batch {batch_number} for campaign {campaign.name}")
        
        # Get records for this batch
        with transaction.atomic():
            # Best practice: Select only needed fields
            pending_records = list(DreamilesEmailRecord.objects.select_for_update().filter(
                campaign=campaign,
                batch_number=batch_number,
                status='pending'
            ).values('id', 'email', 'first_name')[:max_emails])
            
            if pending_records:
                record_ids = [record['id'] for record in pending_records]
                DreamilesEmailRecord.objects.filter(id__in=record_ids).update(status='processing')
        
        if not pending_records:
            logger.info(f"No pending emails in batch {batch_number}")
            
            # Check if there are any pending emails in other batches
            next_pending = DreamilesEmailRecord.objects.filter(
                campaign=campaign,
                status='pending'
            ).order_by('batch_number').first()
            
            if next_pending:
                # Move to the next batch
                next_batch = next_pending.batch_number
                campaign.current_batch = next_batch
                campaign.save(update_fields=['current_batch'])
                
                logger.info(f"Moving to batch {next_batch}")
                return process_dreammiles_batch(campaign_id, next_batch, max_emails)
            else:
                # Check if any emails are still processing
                processing = DreamilesEmailRecord.objects.filter(
                    campaign=campaign, 
                    status='processing'
                ).count()
                
                if processing == 0:
                    # Campaign complete
                    campaign.status = 'completed'
                    campaign.save(update_fields=['status'])
                    
                    # Send final report
                    send_dreammiles_report.delay(campaign_id)
                    
                    logger.info(f"Campaign {campaign.name} completed!")
                
                return {"status": "success", "message": "No more emails to process"}
        
        # Best practice: Use connection pooling
        connection = get_connection()
        connection.open()
        
        # Process emails
        success_count = 0
        failure_count = 0
        
        # Process in smaller chunks
        chunk_size = EMAIL_CHUNK_SIZE
        for i in range(0, len(pending_records), chunk_size):
            chunk = pending_records[i:i+chunk_size]
            
            # Prepare messages
            messages = []
            chunk_records = []
            
            for record in chunk:
                try:
                    # Get the full record
                    db_record = DreamilesEmailRecord.objects.get(id=record['id'])
                    
                    email_body = campaign.email_body.format(
                        first_name=record['first_name'] or "Dreammiles Member"
                    )
                    
                    message = EmailMessage(
                        subject=campaign.subject,
                        body=email_body,
                        from_email=settings.EMAIL_HOST_USER,
                        to=[record['email']],
                        connection=connection
                    )
                    
                    messages.append((db_record, message))
                    chunk_records.append(db_record)
                    
                except Exception as e:
                    logger.error(f"Error preparing email for {record['email']}: {str(e)}")
                    try:
                        db_record = DreamilesEmailRecord.objects.get(id=record['id'])
                        db_record.status = 'failed'
                        db_record.error_message = str(e)
                        db_record.save(update_fields=['status', 'error_message'])
                        failure_count += 1
                    except Exception as inner_e:
                        logger.error(f"Could not update record {record['id']}: {str(inner_e)}")
            
            # Send messages in micro-batches
            for j in range(0, len(messages), EMAIL_MICRO_BATCH_SIZE):
                micro_batch = messages[j:j+EMAIL_MICRO_BATCH_SIZE]
                micro_batch_messages = [msg for _, msg in micro_batch]
                micro_batch_records = [rec for rec, _ in micro_batch]
                
                try:
                    # Best practice: Measure sending time
                    send_start = time.time()
                    connection.send_messages(micro_batch_messages)
                    send_duration = time.time() - send_start
                    
                    # Log performance metrics
                    logger.debug(f"Sent {len(micro_batch_messages)} emails in {send_duration:.2f}s")
                    
                    # Best practice: Use bulk_update instead of individual saves
                    for record in micro_batch_records:
                        record.status = 'sent'
                        record.sent_at = django_timezone.now()
                    
                    DreamilesEmailRecord.objects.bulk_update(
                        micro_batch_records,
                        ['status', 'sent_at'],
                        batch_size=EMAIL_MICRO_BATCH_SIZE
                    )
                    
                    success_count += len(micro_batch_records)
                    
                except Exception as e:
                    logger.error(f"Error sending email batch: {str(e)}")
                    
                    # Mark as failed
                    for record in micro_batch_records:
                        record.status = 'failed'
                        record.error_message = str(e)
                        record.retry_count = record.retry_count + 1
                    
                    # Best practice: Use bulk_update for failures too
                    DreamilesEmailRecord.objects.bulk_update(
                        micro_batch_records,
                        ['status', 'error_message', 'retry_count'],
                        batch_size=EMAIL_MICRO_BATCH_SIZE
                    )
                    
                    failure_count += len(micro_batch_records)
                
                # Small delay between micro-batches to prevent throttling
                time.sleep(MICRO_BATCH_DELAY_SECONDS)
            
            # Delay between chunks
            if i + chunk_size < len(pending_records):
                time.sleep(CHUNK_DELAY_SECONDS)
        
        # Close connection
        connection.close()
        
        # Update campaign stats
        with transaction.atomic():
            campaign.refresh_from_db()
            campaign.emails_sent += success_count
            campaign.emails_failed += failure_count
            campaign.last_sent_at = django_timezone.now()
            campaign.save(update_fields=['emails_sent', 'emails_failed', 'last_sent_at'])
        
        # Best practice: Log comprehensive batch information
        duration = time.time() - start_time
        logger.info(f"Batch {batch_number} complete: {success_count} sent, {failure_count} failed in {duration:.2f}s")
        logger.info(f"Campaign progress: {campaign.emails_sent}/{campaign.total_recipients} emails " 
                   f"({campaign.emails_sent/campaign.total_recipients*100:.2f}%)")
        
        # Check if more emails in current batch
        remaining_in_batch = DreamilesEmailRecord.objects.filter(
            campaign=campaign,
            batch_number=batch_number,
            status='pending'
        ).exists()
        
        if remaining_in_batch:
            # Best practice: Use a reasonable delay for the same batch
            process_dreammiles_batch.apply_async(
                args=[campaign_id, batch_number, max_emails],
                countdown=300  # 5 minutes delay before continuing same batch
            )
        else:
            # Move to next batch with a longer delay
            next_batch = batch_number + 1
            next_batch_exists = DreamilesEmailRecord.objects.filter(
                campaign=campaign,
                batch_number=next_batch,
                status='pending'
            ).exists()
            
            if next_batch_exists:
                campaign.current_batch = next_batch
                campaign.save(update_fields=['current_batch'])
                logger.info(f"Batch {batch_number} complete. Moving to batch {next_batch} in {BATCH_DELAY_SECONDS // 60} minutes")
                
                # Best practice: Schedule the next batch with a controlled delay
                process_dreammiles_batch.apply_async(
                    args=[campaign_id, next_batch, max_emails],
                    countdown=BATCH_DELAY_SECONDS  # 30 minutes between batches
                )
            else:
                # Double-check if there are any pending records in other batches
                next_pending = DreamilesEmailRecord.objects.filter(
                    campaign=campaign,
                    status='pending'
                ).order_by('batch_number').first()
                
                if next_pending:
                    next_batch = next_pending.batch_number
                    campaign.current_batch = next_batch
                    campaign.save(update_fields=['current_batch'])
                    logger.info(f"Moving to batch {next_batch} in {BATCH_DELAY_SECONDS // 60} minutes")
                    
                    process_dreammiles_batch.apply_async(
                        args=[campaign_id, next_batch, max_emails],
                        countdown=BATCH_DELAY_SECONDS
                    )
                else:
                    # Final check for campaign completion
                    processing = DreamilesEmailRecord.objects.filter(
                        campaign=campaign, 
                        status='processing'
                    ).count()
                    
                    if processing == 0:
                        campaign.status = 'completed'
                        campaign.save(update_fields=['status'])
                        
                        # Send final report
                        send_dreammiles_report.delay(campaign_id)
                        
                        logger.info(f"Campaign {campaign.name} completed!")
        
        return {
            "status": "success",
            "batch": batch_number,
            "sent": success_count,
            "failed": failure_count,
            "duration": f"{duration:.2f}s",
            "progress": f"{campaign.emails_sent}/{campaign.total_recipients} ({campaign.emails_sent/campaign.total_recipients*100:.2f}%)"
        }
        
    except DreammilesCampaign.DoesNotExist:
        logger.error(f"Campaign not found with ID: {campaign_id}")
        return {"status": "error", "message": "Campaign not found"}
    
    except Exception as e:
        logger.error(f"Error processing email batch: {str(e)}")
        
        # Best practice: Reset processing emails back to pending
        try:
            DreamilesEmailRecord.objects.filter(
                campaign_id=campaign_id,
                status='processing'
            ).update(status='pending')
        except Exception as reset_error:
            logger.error(f"Error resetting processing emails: {str(reset_error)}")
        
        return {"status": "error", "message": str(e)}


@shared_task
def send_dreammiles_report(campaign_id):
    """Generate and send campaign completion report"""
    try:
        campaign = DreammilesCampaign.objects.get(id=campaign_id)
        
        # Get statistics
        total = campaign.total_recipients
        sent = campaign.emails_sent
        failed = campaign.emails_failed
        
        # Calculate completion time
        duration = django_timezone.now() - campaign.created_at
        hours = duration.total_seconds() / 3600
        
        # Best practice: More detailed and professional report
        report = f"""
DREAMMILES EMAIL CAMPAIGN REPORT
===============================

Campaign Details:
---------------
Name: {campaign.name}
Subject: {campaign.subject}
Started: {campaign.created_at.strftime('%Y-%m-%d %H:%M:%S')}
Completed: {django_timezone.now().strftime('%Y-%m-%d %H:%M:%S')}
Duration: {int(hours)} hours, {int((hours % 1) * 60)} minutes

SUMMARY
-------
Total Recipients: {total:,}
Successfully Sent: {sent:,} ({round(sent/total*100 if total > 0 else 0, 2)}%)
Failed: {failed:,} ({round(failed/total*100 if total > 0 else 0, 2)}%)

"""
        
        # Add tier breakdown if available
        tier_stats = {}
        tier_records = DreamilesEmailRecord.objects.filter(
            campaign=campaign,
            status='sent',
            tier__isnull=False
        ).exclude(tier__exact='')
        
        for record in tier_records:
            tier = record.tier
            if tier in tier_stats:
                tier_stats[tier] += 1
            else:
                tier_stats[tier] = 1
        
        if tier_stats:
            report += "\nBreakdown by Member Tier:\n"
            report += "------------------------\n"
            
            for tier, count in sorted(tier_stats.items(), key=lambda x: x[1], reverse=True):
                report += f"{tier}: {count:,} emails ({(count/sent*100):.2f}%)\n"
        
        # Add details of failed emails if any
        if failed > 0:
            common_errors = {}
            failed_samples = DreamilesEmailRecord.objects.filter(
                campaign=campaign,
                status='failed'
            ).order_by('email')[:50]
            
            for record in failed_samples:
                error_type = record.error_message.split(':')[0] if record.error_message else 'Unknown'
                if error_type in common_errors:
                    common_errors[error_type] += 1
                else:
                    common_errors[error_type] = 1
            
            report += "\nError Analysis:\n"
            report += "--------------\n"
            
            for error_type, count in sorted(common_errors.items(), key=lambda x: x[1], reverse=True):
                report += f"{error_type}: {count} occurrences ({(count/failed*100):.2f}%)\n"
            
            report += "\nSample Failed Emails:\n"
            report += "-------------------\n"
            
            for i, record in enumerate(failed_samples[:10]):
                report += f"{i+1}. {record.email}: {record.error_message[:100]}\n"
        
        # Best practice: Add additional performance metrics
        report += "\nPerformance Metrics:\n"
        report += "-------------------\n"
        report += f"Average send rate: {sent/hours:.2f} emails per hour\n"
        if campaign.last_sent_at and campaign.created_at:
            active_duration = (campaign.last_sent_at - campaign.created_at).total_seconds() / 3600
            report += f"Active sending time: {active_duration:.2f} hours\n"
            report += f"Effective send rate: {sent/active_duration:.2f} emails per hour\n"
        
        report += "\n===============================\n"
        report += "This is an automated report - please do not reply.\n"
        
        # Send report
        recipients = [
            'winnie.gashumba@rwandair.com',
            'pacifique.byiringiro@rwandair.com',
            'elie.kayitare@rwandair.com'
        ]
        
        email = EmailMessage(
            subject=f"Dreammiles Campaign Report: {campaign.name}",
            body=report,
            from_email=settings.EMAIL_HOST_USER,
            to=recipients
        )
        
        email.send(fail_silently=False)
        
        logger.info(f"Campaign report sent to {', '.join(recipients)}")
        return {"status": "success", "message": "Campaign report sent"}
        
    except DreammilesCampaign.DoesNotExist:
        logger.error(f"Campaign not found with ID: {campaign_id}")
        return {"status": "error", "message": "Campaign not found"}
    
    except Exception as e:
        logger.error(f"Error sending campaign report: {str(e)}")
        return {"status": "error", "message": str(e)}


@shared_task
def check_stalled_campaigns():
    """Check for campaigns that might be stalled"""
    threshold = django_timezone.now() - timedelta(hours=4)
    stalled_campaigns = DreammilesCampaign.objects.filter(
        status='processing',
        last_sent_at__lt=threshold
    )
    
    for campaign in stalled_campaigns:
        logger.warning(f"Campaign {campaign.name} appears stalled. Restarting...")
        process_dreammiles_batch.delay(str(campaign.id))
    
    return f"Checked for stalled campaigns: {stalled_campaigns.count()} found and restarted"




# Delete job1 files in job1_backups folder that are older than 30 days

@shared_task
def cleanup_old_job1_backups():
    """
    Delete JOB1 backup files older than one month.
    This task runs every 24 hours to maintain disk space.
    """
    try:
        backup_dir = os.path.join(settings.MEDIA_ROOT, "job1_backups")
        
        # Check if backup directory exists
        if not os.path.exists(backup_dir):
            logger.info("JOB1 backup directory does not exist. No cleanup needed.")
            return {"status": "skipped", "message": "Backup directory not found"}
        
        # Calculate cutoff date (one month ago)
        cutoff_date = datetime.now() - timedelta(days=30)
        
        deleted_files = []
        total_size_freed = 0
        
        # Iterate through all files in backup directory
        for filename in os.listdir(backup_dir):
            file_path = os.path.join(backup_dir, filename)
            
            # Skip if it's not a file or doesn't match expected pattern
            if not os.path.isfile(file_path) or not filename.startswith("JOB1_"):
                continue
            
            try:
                # Get file modification time
                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                
                # Check if file is older than cutoff date
                if file_mtime < cutoff_date:
                    # Get file size before deletion
                    file_size = os.path.getsize(file_path)
                    
                    # Delete the file
                    os.remove(file_path)
                    
                    deleted_files.append({
                        "filename": filename,
                        "size": file_size,
                        "date": file_mtime.strftime("%Y-%m-%d %H:%M:%S")
                    })
                    total_size_freed += file_size
                    
                    logger.info(f"Deleted old JOB1 backup: {filename} ({file_size} bytes)")
                    
            except Exception as e:
                logger.error(f"Error processing file {filename}: {str(e)}")
                continue
        
        # Log summary
        if deleted_files:
            logger.info(f"Cleanup completed: {len(deleted_files)} files deleted, "
                       f"{total_size_freed:,} bytes freed")
        else:
            logger.info("No old JOB1 backup files found to delete")
        
        return {
            "status": "success",
            "deleted_files": len(deleted_files),
            "total_size_freed": total_size_freed,
            "files": deleted_files
        }
        
    except Exception as e:
        logger.error(f"Error during JOB1 backup cleanup: {str(e)}", exc_info=True)
        return {"status": "error", "message": str(e)}
    




from celery import shared_task
from aimsintegration.models import FlightData
import logging

logger = logging.getLogger(__name__)

@shared_task(bind=True)
def delete_flights_no_actual_timings(self, dry_run=False):
    """
    Delete all flight records that have no actual timing data.
    
    Args:
        dry_run (bool): If True, only shows count without deleting
    """
    try:
        logger.info("Starting delete flights with no actual timings...")
        
        # Find all flights with no actual timings
        flights_to_delete = FlightData.objects.filter(
            atd_utc__isnull=True,
            takeoff_utc__isnull=True,
            touchdown_utc__isnull=True,
            ata_utc__isnull=True
        )
        
        count = flights_to_delete.count()
        logger.info(f"Found {count} flights with no actual timings")
        
        if dry_run:
            logger.info(f"DRY RUN: Would delete {count} records")
            return {
                'status': 'success',
                'mode': 'dry_run',
                'records_found': count,
                'records_deleted': 0
            }
        else:
            # Delete the records
            deleted_count = flights_to_delete.delete()[0]
            logger.info(f"✅ Deleted {deleted_count} flight records with no actual timings")
            
            return {
                'status': 'success',
                'mode': 'live',
                'records_found': count,
                'records_deleted': deleted_count
            }
            
    except Exception as e:
        error_msg = f"❌ Error deleting flights: {e}"
        logger.error(error_msg, exc_info=True)
        return {
            'status': 'error',
            'error': str(e)
        }




#=================================================================================================================

# QATAR APIS TASKS

#=================================================================================================================

from .utils import *
from .models import *
from django.db.models import Q
import logging



@shared_task
def fetch_job97():
    """
    Fetch AIMS JOB #97 (DOH KGL / KGL DOH), process assignments,
    then immediately build the Qatar APIS EDIFACT file.
    Updated to handle both flight directions and new parser return format.
    """
    account = get_exchange_account()
    logger.info("Starting Job 97 email fetch process...")
    
    # Only get emails from the last 7 days to avoid processing thousands of emails
    from datetime import timedelta
    from django.utils import timezone
    cutoff_date = timezone.now() - timedelta(days=7)
    
    logger.info(f"Searching for Job 97 emails from {cutoff_date.strftime('%Y-%m-%d')} onwards...")
    
    # Filter emails by date first, then check subjects
    recent_emails = account.inbox.filter(
        datetime_received__gte=cutoff_date
    ).order_by('-datetime_received')
    
    # Count emails being checked
    email_count = 0
    job97_emails = []
    
    for email in recent_emails:
        email_count += 1
        if email.subject and ('DOH KGL' in email.subject.upper() or 'KGL DOH' in email.subject.upper()):
            logger.info(f"✓ Found Job 97 email after checking {email_count} emails: {email.subject}")
            job97_emails.append(email)
            break  # Get the most recent one
    
    if not job97_emails and email_count > 0:
        logger.info(f"✗ No Job 97 emails found after checking {email_count} recent emails")

    try:
        if not job97_emails:
            logger.info("❌ No Job 97 emails found. Task completed.")
            return
                     
        email = job97_emails[0]
        subject = email.subject.upper()
        logger.info(f"📧 Processing Job 97 email: '{email.subject}'")
        logger.info(f"📅 Email received: {email.datetime_received.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        # Ingest and process attachments - this now returns (flight_date, crew_entries)
        logger.info("📎 Processing email attachments...")
        result = process_email_attachment(email, process_job97_file)
        
        # ✅ CRITICAL: Unpack the tuple
        if result:
            flight_date, crew_entries = result
            logger.info("✅ Email attachments processed successfully")
            logger.info(f"📊 Extracted {len(crew_entries)} crew entries from RTF")
        else:
            logger.error("❌ Failed to process Job 97 attachment")
            return

        # Determine direction based on subject line
        if 'DOH KGL' in subject:
            direction = 'I'  # Inbound (DOH to KGL)
            logger.info("🛬 Detected INBOUND flight direction (DOH → KGL)")
        elif 'KGL DOH' in subject:
            direction = 'O'  # Outbound (KGL to DOH)
            logger.info("🛫 Detected OUTBOUND flight direction (KGL → DOH)")
        else:
            # Fallback: try to detect from email content or use default
            logger.warning("⚠️  Could not determine flight direction from subject, defaulting to INBOUND")
            direction = 'I'
                 
        # Use the actual flight date from the RTF file, not today's date
        if flight_date:
            run_date = flight_date
            logger.info(f"📋 Using flight date from RTF: {run_date}")
        else:
            # Fallback to today's date if we couldn't parse the flight date
            run_date = datetime.utcnow().date()
            logger.warning(f"⚠️  Could not determine flight date from RTF, using today: {run_date}")

        # Generate EDIFACT file and save locally
        logger.info("🔧 Building Qatar APIS EDIFACT file...")
        edi_path = build_qatar_apis_edifact(direction, run_date)
        
        if edi_path:
            logger.info(f"✅ EDIFACT file generated successfully: {edi_path}")
            logger.info("🎉 Job 97 processing completed successfully!")
        else:
            logger.warning("⚠️  EDIFACT file generation returned None - check crew assignments for the flight date")

    except Exception as e:
        logger.error(f"❌ Error in fetch_job97: {e}")
        logger.error(f"🔍 Error details: {str(e)}")
        raise



@shared_task
def fetch_job1008():
    """
    Fetch AIMS JOB #1008 (Feeds APIS interface), process static crew details.
    """
    account = get_exchange_account()
    logger.info("Fetching the most recent Job 1008 email...")

    emails = account.inbox.filter(
        subject__contains="#1008 Feeds APIS interface"
    ).order_by('-datetime_received')

    try:
        email = emails[0]
        logger.info(f"Processing Job 1008 email: {email.subject}")

        # Ingest and process attachments
        process_email_attachment(email, process_job1008_file)

    except IndexError:
        logger.info("No new Job 1008 email found.")
    except Exception as e:
        logger.error(f"Error in fetch_job1008: {e}")
        raise




#==========================================================

# Free brave email storages
#==========================================================


def get_new_email_exchange_account():
    credentials = Credentials(
        username=settings.BRAVE_EXCHANGE_EMAIL_USER,
        password=settings.BRAVE_EXCHANGE_EMAIL_PASSWORD
    )
    config = Configuration(
        server=settings.EXCHANGE_EMAIL_SERVER,
        credentials=credentials
    )
    account = Account(
        primary_smtp_address=settings.BRAVE_EXCHANGE_EMAIL_USER,
        credentials=credentials,
        autodiscover=False,
        config=config,
        access_type='delegate'
    )
    return account




@shared_task(bind=True, max_retries=5)
def delete_emails_by_subject_list(self):
    """
    Deletes all emails that match any of the specified subject patterns
    OR come from any of the specified senders,
    with a retry strategy to handle ErrorServerBusy.
    """
    # Specify the subject patterns to delete (all emails regardless of age)
    subject_patterns = [
        "Edno",
        "METAR",
        "TAFs",
        "TAF",
        "TAKE OFF DATA",
        "OPMET",
        "DoNotReply"
        # "EZFW",
    ]  # Add or modify subjects as needed
    
    # Specify the sender emails to delete from (all emails regardless of age)
    sender_emails = [
        "dcscm-no-reply@amadeus.com"
        # Add your specific sender emails here
    ]
    
    # Specify sender emails to delete only if older than 24 hours
    time_sensitive_senders = [
        "noreply@doc.mail.amadeus.com",
        "DoNotReply@fi.boeingservices.com",

    ] 
    
    exact_match = False  # Set to True for exact matches, False for partial matches
    account = get_new_email_exchange_account()
    total_deleted = 0
    
    # Create cutoff date for time-sensitive senders (24 hours ago)
    from datetime import datetime, timedelta
    import pytz
    from exchangelib import EWSDateTime
    
    cutoff_date = datetime.utcnow() - timedelta(hours=24)
    cutoff_date = cutoff_date.replace(tzinfo=pytz.UTC)
    threshold_datetime = EWSDateTime.from_datetime(cutoff_date)
    
    try:
        # First, delete by subject patterns (all emails regardless of age)
        for subject_pattern in subject_patterns:
            if exact_match:
                logger.info(f"Filtering emails with exact subject: '{subject_pattern}'...")
                target_emails = account.inbox.filter(subject=subject_pattern)
            else:
                logger.info(f"Filtering emails containing subject text: '{subject_pattern}'...")
                target_emails = account.inbox.filter(subject__icontains=subject_pattern)
            
            count_emails = target_emails.count()
            logger.info(f"Found {count_emails} email(s) matching subject pattern '{subject_pattern}'.")
            
            if count_emails > 0:
                target_emails.delete()
                logger.info(f"Deleted {count_emails} email(s) with subject pattern '{subject_pattern}'.")
                total_deleted += count_emails
        
        # Second, delete by sender emails (all emails regardless of age)
        for sender_email in sender_emails:
            logger.info(f"Filtering emails from sender: '{sender_email}'...")
            target_emails = account.inbox.filter(sender=sender_email)
            
            count_emails = target_emails.count()
            logger.info(f"Found {count_emails} email(s) from sender '{sender_email}'.")
            
            if count_emails > 0:
                target_emails.delete()
                logger.info(f"Deleted {count_emails} email(s) from sender '{sender_email}'.")
                total_deleted += count_emails
        
        # Third, delete by time-sensitive sender emails only if older than 24 hours
        for sender_email in time_sensitive_senders:
            logger.info(f"Filtering emails from sender: '{sender_email}' older than 24 hours...")
            target_emails = account.inbox.filter(
                sender=sender_email,
                datetime_received__lt=threshold_datetime
            )
            
            count_emails = target_emails.count()
            logger.info(f"Found {count_emails} email(s) from sender '{sender_email}' older than 24 hours.")
            
            if count_emails > 0:
                target_emails.delete()
                logger.info(f"Deleted {count_emails} email(s) from sender '{sender_email}' older than 24 hours.")
                total_deleted += count_emails
        
        match_type = "exact" if exact_match else "partial"
        logger.info(f"Total deleted: {total_deleted} email(s)")
        return f"Purged {total_deleted} email(s) with {match_type} subject matches from {len(subject_patterns)} pattern(s), {len(sender_emails)} sender(s), and {len(time_sensitive_senders)} time-sensitive sender(s)"
    
    except ErrorServerBusy as e:
        # If the Exchange server is busy, wait and retry
        wait_time = 2 ** self.request.retries * 5  # Exponential backoff
        logger.warning(
            f"Server is busy (attempt {self.request.retries + 1}/{self.max_retries}). "
            f"Retrying in {wait_time} seconds..."
        )
        
        # Retry the task after `wait_time` seconds
        raise self.retry(exc=e, countdown=wait_time)
    







# ============================================================================
# JEPPESSEN GENERAL DECLARATION (GD) INTEGRATION
# ============================================================================

import time as time_module


@shared_task
def fetch_jeppessen_gd():
    """
    Fetch and process Jeppessen General Declaration (GD) from Job 97 emails.
    Format: 212/BGF DLA/11112025
    
    IMPORTANT: Does NOT mark DOH emails as read (Qatar APIS needs them)
    ✨ UPDATED: Processes emails from last 24 hours only
    """
    start_time = time_module.time()
    
    try:
        logger.info("=" * 80)
        logger.info("Starting Jeppessen GD fetch task...")
        logger.info("=" * 80)
        
        account = get_exchange_account()
        
        # ✅ CHANGED: Get emails from last 24 hours only (runs every 5 min)
        from datetime import timedelta
        from django.utils import timezone
        cutoff_date = timezone.now() - timedelta(hours=24)  # ← Changed from 7 days
        
        # Still limit to 500 for safety
        recent_emails = list(account.inbox.filter(
            datetime_received__gte=cutoff_date
        ).order_by('-datetime_received')[:500])
        
        logger.info(f"Retrieved {len(recent_emails)} emails from last 24 hours")
        
        # ✅ IMPROVED: Better GD pattern matching
        gd_emails = []
        email_count = 0
        
        for email in recent_emails:
            email_count += 1
            
            # Progress indicator every 50 emails
            if email_count % 50 == 0:
                logger.info(f"Scanned {email_count} emails so far...")
            
            if email.subject:
                # ✅ IMPROVED: More flexible pattern matching
                # Matches: 212/BGF DLA/11112025 or 212/BGF  DLA/11112025 (extra spaces)
                # Pattern: DIGITS/LETTERS LETTERS/DIGITS(8)
                gd_pattern = r'\d{1,4}/[A-Z]{3}\s+[A-Z]{3}/\d{8}'
                
                if re.search(gd_pattern, email.subject):
                    if not email.is_read:
                        gd_emails.append(email)
                        logger.info(f"✓ Found GD email: {email.subject}")
        
        logger.info(f"Scanned {email_count} emails, found {len(gd_emails)} unread GD emails")
        
        if not gd_emails:
            logger.info("No new GD emails to process")
            return 0
        
        processed_count = 0
        
        for email in gd_emails:
            try:
                logger.info("-" * 80)
                logger.info(f"Processing GD: {email.subject}")
                logger.info(f"Received: {email.datetime_received}")
                
                # Check if this is a DOH route
                is_doh_route = 'DOH' in email.subject.upper()
                
                if is_doh_route:
                    logger.info("⚠️  DOH route detected - will NOT mark as read (Qatar APIS needs it)")
                
                if email.attachments:
                    for attachment in email.attachments:
                        if isinstance(attachment, FileAttachment):
                            logger.info(f"Processing attachment: {attachment.name}")
                            
                            # ✅ IMPROVED: Extract GD identifier with flexible pattern
                            gd_match = re.search(r'(\d{1,4}/[A-Z]{3}\s+[A-Z]{3}/\d{8})', email.subject)
                            gd_identifier = gd_match.group(1) if gd_match else email.subject
                            
                            result = process_jeppessen_gd_attachment(
                                attachment=attachment,
                                email_subject=email.subject,
                                gd_identifier=gd_identifier
                            )
                            
                            if result:
                                processed_count += 1
                                logger.info(f"✓ Successfully processed: {gd_identifier}")
                else:
                    logger.warning("No attachments found")
                
                # CRITICAL: Only mark as read if NOT a DOH route
                if not is_doh_route and not email.is_read:
                    email.is_read = True
                    email.save()
                    logger.info("✓ Email marked as read")
                elif is_doh_route:
                    logger.info("ℹ️  Email left unread for Qatar APIS")
                
            except Exception as e:
                logger.error(f"Error processing GD email: {e}", exc_info=True)
                continue
        
        duration = time_module.time() - start_time
        
        logger.info("=" * 80)
        logger.info(f"Jeppessen GD fetch completed")
        logger.info(f"Processed: {processed_count}/{len(gd_emails)}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info("=" * 80)
        
        return processed_count
        
    except Exception as e:
        duration = time_module.time() - start_time
        logger.error(f"Fatal error in Jeppessen GD fetch: {e}", exc_info=True)
        return 0


def process_jeppessen_gd_attachment(attachment, email_subject, gd_identifier):
    """
    Process Jeppessen GD RTF attachment.
    Parses crew details from RTF, fetches emails from ERP, gets STD/STA from FlightData.
    ✨ NEW: Auto-submits to Flitelink after successful processing
    ✨ IMPROVED: Flexible pattern matching for flight numbers (1-4 digits)
    """
    from .models import JEPPESSENGDFlight, JEPPESSENGDCrew, JEPPESSENGDProcessingLog, JEPPESSENGDCrewDetail
    from .utils import rtf_to_text
    
    start_time = time_module.time()
    
    try:
        # ✅ IMPROVED: Parse GD identifier with flexible flight number (1-4 digits)
        # Matches: 1/ABC XYZ/01012025, 212/BGF DLA/11112025, 9999/KGL DOH/31122025
        match = re.match(r'(\d{1,4})/([A-Z]{3})\s+([A-Z]{3})/(\d{8})', gd_identifier)
        
        if not match:
            logger.error(f"Could not parse GD identifier: {gd_identifier}")
            return False
        
        flight_no = match.group(1)
        origin_iata = match.group(2)
        dest_iata = match.group(3)
        date_str = match.group(4)
        
        try:
            flight_date = datetime.strptime(date_str, '%d%m%Y').date()
        except ValueError:
            logger.error(f"Invalid date format: {date_str}")
            return False
        
        logger.info(f"Processing: Flight {flight_no}, {origin_iata}→{dest_iata}, {flight_date}")
        
        # Process RTF
        raw = attachment.content.decode('utf-8', errors='ignore')
        text = rtf_to_text(raw)
        lines = [ln for ln in text.splitlines() if ln.strip()]
        
        # Extract tail number
        tail_number = None
        for line in lines[:25]:
            if re.match(r'^\d[A-Z]{2}-[A-Z]{2}$', line.strip()):
                tail_number = line.strip()
                logger.info(f"Found tail: {tail_number}")
                break
        
        # Parse crew entries with FULL details from RTF
        crew_entries = parse_jeppessen_gd_crew_full(lines)
        logger.info(f"Found {len(crew_entries)} crew members")
        
        # Convert IATA to ICAO
        origin_icao = get_icao_from_iata(origin_iata)
        destination_icao = get_icao_from_iata(dest_iata)
        
        origin_code = origin_icao if origin_icao else origin_iata
        destination_code = destination_icao if destination_icao else dest_iata
        
        # Find FlightData record
        flight_record = FlightData.objects.filter(
            flight_no=flight_no,
            sd_date_utc=flight_date,
            tail_no=tail_number
        ).first()
        
        if not flight_record:
            flight_record = FlightData.objects.filter(
                flight_no=flight_no,
                sd_date_utc=flight_date
            ).first()
        
        flight_found = flight_record is not None
        
        # Get STD and STA from FlightData
        std_utc = None
        sta_utc = None
        
        if flight_found:
            std_utc = flight_record.std_utc
            sta_utc = flight_record.sta_utc
            logger.info(f"✓ Linked to FlightData: {flight_record}")
            logger.info(f"  STD: {std_utc}, STA: {sta_utc}")
        else:
            logger.warning("⚠ No FlightData record found - STD/STA unavailable")
        
        # Identify PIC and SIC
        pic_crew = None
        sic_crew = None
        
        for i, entry in enumerate(crew_entries):
            if entry.get('is_pic') or entry.get('role') == 'PIC':
                pic_crew = entry
                logger.info(f"✓ PIC: {entry['crew_id']} - {entry['name']}")
                
                # Find SIC (next CP or FO after PIC)
                for j in range(i + 1, len(crew_entries)):
                    next_entry = crew_entries[j]
                    if next_entry.get('role') in ['CP', 'FO']:
                        sic_crew = next_entry
                        logger.info(f"✓ SIC: {next_entry['crew_id']} - {next_entry['name']}")
                        break
                break
        
        # Track email statistics
        emails_found = 0
        emails_not_found = 0
        
        # Save to database
        with transaction.atomic():
            # Create/update GD flight with STD/STA
            gd_flight, created = JEPPESSENGDFlight.objects.update_or_create(
                flight_no=flight_no,
                flight_date=flight_date,
                origin_iata=origin_iata,
                destination_iata=dest_iata,
                defaults={
                    'flight': flight_record,
                    'origin_icao': origin_icao,
                    'destination_icao': destination_icao,
                    'tail_no': tail_number,
                    'raw_filename': gd_identifier,
                    'std_utc': std_utc,
                    'sta_utc': sta_utc,
                }
            )
            
            logger.info(f"{'Created' if created else 'Updated'} GD flight")
            
            # Delete existing crew
            JEPPESSENGDCrew.objects.filter(gd_flight=gd_flight).delete()
            
            # Create crew assignments WITHOUT overwriting position
            for entry in crew_entries:
                is_pic = (entry == pic_crew)
                is_sic = (entry == sic_crew)
                
                # KEEP the original role (CP, FO, FP, etc.)
                position = entry.get('role') or 'AC'
                # DON'T overwrite with PIC/SIC - just use flags
                
                # Fetch email from ERP
                crew_email = get_jeppessen_crew_email_from_erp(entry['crew_id'])
                
                if crew_email:
                    emails_found += 1
                else:
                    emails_not_found += 1
                
                # Create crew assignment
                JEPPESSENGDCrew.objects.create(
                    crew_id=entry['crew_id'],
                    gd_flight=gd_flight,
                    flight=flight_record,
                    position=position,
                    role=entry.get('role'),
                    is_pic=is_pic,
                    is_sic=is_sic,
                    email=crew_email or '',
                    flight_no=flight_no,
                    flight_date=flight_date,
                    origin=origin_code,
                    destination=destination_code,
                    tail_no=tail_number,
                    dep_date_utc=flight_date,
                    arr_date_utc=flight_date,
                    std_utc=std_utc,
                    sta_utc=sta_utc,
                )
                
                # Update crew detail from RTF data (including email)
                update_jeppessen_crew_detail(entry, flight_date, crew_email)
        
        # Create log
        duration = time_module.time() - start_time
        
        JEPPESSENGDProcessingLog.objects.create(
            email_subject=email_subject,
            attachment_name=attachment.name,
            gd_identifier=gd_identifier,
            total_crew=len(crew_entries),
            emails_found=emails_found,
            emails_not_found=emails_not_found,
            pic_identified=(pic_crew is not None),
            sic_identified=(sic_crew is not None),
            flight_found=flight_found,
            status='SUCCESS',
            processing_duration=duration
        )
        
        logger.info(f"✅ Success in {duration:.2f}s - Emails: {emails_found}/{len(crew_entries)}")
        
        # ============================================================================
        # ✨ AUTO-SUBMIT TO FLITELINK
        # ============================================================================
        if getattr(settings, 'FLITELINK_AUTO_SUBMIT', True):
            # Check if flight can be submitted
            if gd_flight.can_submit_to_flitelink:
                logger.info(f"🚀 Queuing flight {gd_flight.flight_no} for Flitelink submission")
                try:
                    # Queue for submission with 60-second delay
                    submit_flight_to_flitelink.apply_async(
                        args=[gd_flight.id],
                        countdown=60
                    )
                    logger.info(f"✓ Flight {gd_flight.flight_no} queued for Flitelink (will submit in 60s)")
                except Exception as e:
                    logger.error(f"✗ Error queuing Flitelink submission: {e}", exc_info=True)
            else:
                # Log why it can't be submitted
                missing = []
                if not gd_flight.origin_icao:
                    missing.append("origin_icao")
                if not gd_flight.destination_icao:
                    missing.append("destination_icao")
                if not gd_flight.std_utc:
                    missing.append("std_utc")
                if not gd_flight.sta_utc:
                    missing.append("sta_utc")
                
                logger.warning(
                    f"⚠️  Flight {gd_flight.flight_no} NOT queued for Flitelink - "
                    f"Missing: {', '.join(missing)}"
                )
        else:
            logger.info("ℹ️  Flitelink auto-submit is disabled")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing GD: {e}", exc_info=True)
        
        JEPPESSENGDProcessingLog.objects.create(
            email_subject=email_subject,
            attachment_name=attachment.name if attachment else "Unknown",
            gd_identifier=gd_identifier,
            total_crew=0,
            emails_found=0,
            emails_not_found=0,
            pic_identified=False,
            sic_identified=False,
            flight_found=False,
            status='FAILED',
            error_message=str(e)[:1000],
            processing_duration=time_module.time() - start_time
        )
        
        return False

from django.db import connections, DatabaseError
def get_jeppessen_crew_email_from_erp(crew_id):
    """
    Fetch crew email from ERP (MSSQL) using crew_id.
    Same logic as old Jeppessen integration.
    
    Args:
        crew_id (str): Crew ID (2-4 digits from RTF)
    
    Returns:
        str: Email address or None
    """
    try:
        # Format crew_id as WB + 4 digits with leading zeros
        wb_formatted = f"WB{int(crew_id):04d}"
        
        logger.debug(f"Querying ERP for crew: {crew_id} → {wb_formatted}")
        
        with connections['mssql'].cursor() as cursor:
            cursor.execute("""
                SELECT [Company E-Mail]
                FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
                WHERE [No_] = %s
            """, [wb_formatted])
            
            row = cursor.fetchone()
            
            if row and row[0]:
                crew_email = row[0].strip()
                logger.debug(f"✓ Found email for {wb_formatted}: {crew_email}")
                return crew_email
            else:
                logger.debug(f"✗ No email for {wb_formatted} in ERP")
                return None
                
    except Exception as e:
        logger.warning(f"Error fetching email from ERP for crew {crew_id}: {e}")
        return None


def parse_jeppessen_gd_crew_full(lines):
    """
    Parse crew entries from GD RTF with FULL details.
    Returns list of crew dicts with all available information.
    """
    crew_entries = []
    crew_data_started = False
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Start crew data section
        if not crew_data_started:
            if "EXPIRY" in line or "ON THIS STAGE" in line:
                crew_data_started = True
            i += 1
            continue
        
        # Stop at declaration
        if "DECLARATION OF HEALTH" in line or "FOR OFFICIAL USE" in line:
            break
        
        # Skip markers
        if line in ['DOH', 'KGL', 'BGF', 'DLA', 'EBB', 'NBO', 'JNB', '...', '*'] or not line:
            i += 1
            continue
        
        # Check for crew ID (2-4 digits)
        if line.isdigit() and 2 <= len(line) <= 4:
            crew_id = line
            name = None
            role = None
            passport = None
            birth_date = None
            gender = None
            nationality = None
            expiry = None
            is_pic = False
            
            j = i + 1
            
            # Get name (and check for embedded PIC marker)
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line and next_line not in ['*', '...', 'DOH', 'KGL', 'BGF', 'DLA']:
                    # Check for embedded PIC marker (e.g., "FREDRICK OCHIENG PIC")
                    if re.search(r'\s+PIC$', next_line):
                        parts = next_line.rsplit(None, 1)
                        if len(parts) == 2:
                            name = parts[0]
                            is_pic = True  # ✅ Set PIC flag
                            # ✅ DON'T set role = 'PIC' - continue to get actual role
                        else:
                            name = next_line
                    # Check for embedded role that's NOT PIC (CP, FO, FP, SA, FA, FE)
                    elif re.search(r'\s+(CP|FO|FP|SA|FA|FE)$', next_line):
                        parts = next_line.rsplit(None, 1)
                        if len(parts) == 2:
                            name = parts[0]
                            role = parts[1]
                        else:
                            name = next_line
                    else:
                        name = next_line
                    j += 1
                    break
                elif next_line == '*':
                    j += 1
                    continue
                j += 1
            
            # Get role if not embedded (or if we only found PIC marker)
            if j < len(lines) and not role:
                next_line = lines[j].strip()
                if next_line == '*':
                    j += 1
                    if j < len(lines):
                        next_line = lines[j].strip()
                
                if len(next_line) <= 3 and next_line.isupper() and next_line.isalpha():
                    role = next_line
                    # ✅ Don't set is_pic here - it's already set from name parsing
                    j += 1
            
            # Get passport (6+ alphanumeric chars)
            if j < len(lines):
                next_line = lines[j].strip()
                if len(next_line) >= 6 and next_line.replace(' ', '').isalnum():
                    passport = next_line
                    j += 1
            
            # Get birth date (dd/mm/yy)
            if j < len(lines):
                next_line = lines[j].strip()
                if re.match(r'^\d{2}/\d{2}/\d{2}$', next_line):
                    birth_date = next_line
                    j += 1
            
            # Get gender (M or F)
            if j < len(lines):
                next_line = lines[j].strip()
                if len(next_line) == 1 and next_line.upper() in ['M', 'F']:
                    gender = next_line.upper()
                    j += 1
            
            # Get nationality (2-3 letters)
            if j < len(lines):
                next_line = lines[j].strip()
                if 2 <= len(next_line) <= 3 and next_line.isupper() and next_line.isalpha():
                    nationality = next_line
                    j += 1
            
            # Get expiry (dd/mm/yy)
            if j < len(lines):
                next_line = lines[j].strip()
                if re.match(r'^\d{2}/\d{2}/\d{2}$', next_line):
                    expiry = next_line
                    j += 1
            
            if crew_id and name:
                crew_entries.append({
                    'crew_id': crew_id,
                    'name': name,
                    'role': role,
                    'passport': passport,
                    'birth_date': birth_date,
                    'gender': gender,
                    'nationality': nationality,
                    'expiry': expiry,
                    'is_pic': is_pic
                })
                logger.debug(f"Parsed crew {crew_id}: {name}, role={role}, is_pic={is_pic}")
            
            i = j
        else:
            i += 1
    
    return crew_entries
# ============================================================================
# CREW DOCUMENTS BACKUP FROM AIMS
# ============================================================================

from .models import Backup
from datetime import datetime
from django.db import connection, transaction
import math

def get_directory_size(path):
    """Get total size of a directory and all its contents in bytes"""
    total_size = 0
    
    # Check if it's a file
    if os.path.isfile(path):
        return os.path.getsize(path)
    
    # If it's a directory, walk through it
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            # Skip if it's a symbolic link to avoid counting twice
            if not os.path.islink(filepath):
                try:
                    total_size += os.path.getsize(filepath)
                except (OSError, FileNotFoundError):
                    # Handle permission errors or files that disappeared
                    pass
    
    return total_size

from .utils import send_backup_complete_email, send_backup_failed_email, format_file_size

@shared_task()
def monthly_crew_documents_backup_task():
    download_crew_documents_from_aims('monthly')

@shared_task()
def weekly_crew_documents_backup_task():
    download_crew_documents_from_aims('weekly')

def download_crew_documents_from_aims(folder):
    logger.info(f"================== Starting {folder} backup task ==================")
    # Server credentials
    today_str = datetime.now().strftime("%d%b%Y").upper()
    start_time = django_timezone.now()
    backup_folder_existed = False
    backup_name = today_str if folder == 'monthly' else 'Crew Documents'

    # check if backup already exists, looking by name and backup type
    backup_exists = Backup.objects.filter(name=backup_name, backup_type=folder).exists()
    if backup_exists:
        logger.info(f"{folder} Backup for {today_str} already exists. Skipping.")
        backup = Backup.objects.filter(name=backup_name, backup_type=folder).first()
        backup_folder_existed = True
    else :
        backup = Backup.objects.create(
            name=today_str,
            start_time=start_time,
            backup_type=folder,
            backup_date=datetime.now().date(),
            status="running"
        )

    backup_id = backup.id

    aims_host = settings.AIMS_SERVER_ADDRESS
    aims_port = int(settings.AIMS_PORT)
    aims_username = settings.AIMS_SERVER_USER
    aims_password = settings.AIMS_SERVER_PASSWORD
    local_destination_path = os.path.join(settings.BACKUP_CREW_DOCUMENTS_PATH, folder, backup_name)
    remote_folder_path = settings.AIMS_SERVER_CREW_DOCUMENTS_PATH

    # Retry configuration
    attempts = 10  # Number of retries
    delay = 5  # Delay in seconds between retries


    def _recursive_download(sftp, remote_path, local_path, check_file_already_exist):
        """Recursively download folder contents"""
        os.makedirs(local_path, exist_ok=True)
        for item in sftp.listdir_attr(remote_path):
            remote_item = os.path.join(remote_path, item.filename)
            local_item = os.path.join(local_path, item.filename)

            if stat.S_ISDIR(item.st_mode):
                # Recurse into subdirectory
                _recursive_download(sftp, remote_item, local_item)
            else:
                # Check if file already exists
                if check_file_already_exist and os.path.exists(local_item):
                    logger.info(f"File already exists: {local_item}")
                    continue
                # Download file
                logger.info(f"Downloading: {remote_item} -> {local_item}")
                sftp.get(remote_item, local_item)

    try:
        for attempt in range(attempts):
            try:
                # Create transport and SFTP session
                transport = paramiko.Transport((aims_host, aims_port))
                transport.connect(username=aims_username, password=aims_password)
                sftp = paramiko.SFTPClient.from_transport(transport)


                logger.info(f"Starting recursive download from {remote_folder_path} to {local_destination_path}")
                _recursive_download(sftp, remote_folder_path, local_destination_path, True)
                # _recursive_download(sftp, remote_folder_path, local_destination_path, backup_folder_existed or attempt != 0)
                logger.info(f"Successfully downloaded folder {remote_folder_path}")

                end_time = django_timezone.now()
                duration = end_time - start_time
                duration_minutes = (end_time - start_time).total_seconds() / 60
                minutes = math.floor(duration_minutes)
                logger.info(f"=== Backup finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
                logger.info(f"=== Total duration: {duration} ({minutes:.2f} minutes) ===")

                size = get_directory_size(local_destination_path)
                backup.end_time = end_time
                backup.duration_minutes = minutes
                backup.status = "success"
                backup.message = "Backup completed successfully"
                backup.backup_size = size
                backup.save()
                logger.info(f"Successfully saved backup record")

                send_backup_complete_email(backup, folder, format_file_size(size))

                sftp.close()
                transport.close()
                break  # Success, break out of retry loop

            except (SSHException, NoValidConnectionsError) as e:
                logger.error(f"SFTP error on attempt {attempt + 1}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Failed on attempt {attempt + 1}: {e}", exc_info=True)

            # Retry logic
            if attempt < attempts - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                end_time = django_timezone.now()
                duration = end_time - start_time
                logger.info(f"=== Total duration before failure: {duration} ({duration.total_seconds():.2f} seconds) ===")
                logger.error(f"Failed to download folder after {attempts} attempts.")

                duration_minutes = (end_time - start_time).total_seconds() / 60
                minutes = math.floor(duration_minutes)
                backup.end_time = end_time
                backup.duration_minutes = minutes
                backup.status = "failed"
                backup.message = "Failed to download folder after {attempts} attempts."
                backup.save()
                logger.info(f"Error saved backup record")

                send_backup_failed_email(backup, folder)

    except Exception as e:
        end_time = django_timezone.now()
        duration = end_time - start_time

        duration_minutes = (end_time - start_time).total_seconds() / 60
        minutes = math.floor(duration_minutes)
        backup.end_time = end_time
        backup.duration_minutes = minutes
        backup.status = "failed"
        backup.message = f"Unexpected error during download: {e}"
        logger.info(f"Error savv saved backup record")
        backup.save()
        send_backup_failed_email(backup, folder)
        logger.error(f"Unexpected error during download: {e}", exc_info=True)
        logger.info(f"=== Total duration before failure: {duration} ({duration.total_seconds():.2f} seconds) ===")


# ============================================================================
# CREW DOCUMENTS ARCHIVE
# ============================================================================
from .models import CrewDocumentsArchive
def fetch_crew_who_left():
    all_crew_who_left = CrewDocumentsArchive.objects.all()
    crew_ids = [crew.wb_number for crew in all_crew_who_left]

    # If There are no records in the database, fetch all crew who left
    if len(crew_ids) == 0:
        query = f"""
            SELECT [No_], [First Name], [Last Name], [Job Title], [Date Of Leaving]
            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [Status] = 1 AND [Responsibility Center] = 'FLIGHT OPERATIONS';
        """
        with connections['mssql'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        logger.info("Fetching all")

        for no_, first_name, last_name, job_title, date_of_leaving in rows:
            CrewDocumentsArchive.objects.create(
                wb_number=int(no_[2:]),
                crew_name=f"{first_name} {last_name}",
                date_of_leaving=date_of_leaving,
                position=job_title,
                archive_path=f"{int(no_[2:])}",
                archive_date=date_of_leaving + relativedelta(months=24),
            )

        logger.info("Done Fetching all")
    # If There are records in the database, fetch all crew who are not in the database
    else:
        wb_formatted_ids = [f"WB{int(cid):04d}" for cid in crew_ids]
        placeholders = ", ".join(["%s"] * len(wb_formatted_ids))
        query = f"""
            SELECT [No_], [First Name], [Last Name], [Job Title], [Date Of Leaving]
            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [Status] = 1 AND [Responsibility Center] = 'FLIGHT OPERATIONS' AND LEN([No_]) = 6 AND [No_] NOT IN ({placeholders});
        """
        with connections['mssql'].cursor() as cursor:
            cursor.execute(query, wb_formatted_ids)
            rows = cursor.fetchall()

        for no_, first_name, last_name, job_title, date_of_leaving in rows:
            CrewDocumentsArchive.objects.create(
                wb_number=int(no_[2:]),
                crew_name=f"{first_name} {last_name}",
                date_of_leaving=date_of_leaving.strftime,
                position=job_title,
                archive_path=f"{int(no_[2:])}",
                archive_date=(date_of_leaving + relativedelta(months=24)).strftime('%Y-%m-%d'),
            )

from .utils import archive_crew_documents_by_wb, send_archive_complete_email
import json

def archive_crew_who_left():
    archived_crew = []

    # 0) Get crew who left and the archive date is before today
    crew_who_left_earlier = CrewDocumentsArchive.objects.filter(archive_date__lt=timezone.now().date(), archived=False).all()
    count = 1
    logger.info("Crew who left earlier: %d", len(crew_who_left_earlier))
    for crew in crew_who_left_earlier:
        response = archive_crew_documents_by_wb(f"{int(crew.wb_number):04d}", wrapper_folder="Initial")
        data = json.loads(response.content)
        if (data["status"] == "success"):
            count += 1
            # archived_crew.append(crew)
            # 3) Send email to records and coorporate library
            # send_archive_complete_email(crew)

            # 4) Update the database to Set archived to True
            crew.archived = True
            crew.archive_path = f"Initial&&{crew.wb_number}"
            crew.save()

    # 1) Get crew who left and the archive date is today
    all_crew_who_left = CrewDocumentsArchive.objects.filter(archive_date__year=timezone.now().year, archive_date__month=timezone.now().month, archive_date__day=timezone.now().day, archived=False).all()
    logger.info("Crew who left today: %d", len(all_crew_who_left))
    for crew in all_crew_who_left:

        # 2) Move their files inside the archive folder
        response = archive_crew_documents_by_wb(f"{crew.wb_number}")
        data = json.loads(response.content)
        if (data["status"] == "success"):
            logger.info("Success archiving crew ", crew.wb_number)
            archived = True
    
            # 3) Send email to records and coorporate library
            send_archive_complete_email(crew)

            # 4) Update the database to Set archived to True
            crew.archived = True
            crew.save()
    logger.info("done archiving")
# ============================================================================
# OPTIONAL: Manual task to update emails for existing crew records
# ============================================================================

def update_jeppessen_crew_detail(entry, flight_date, crew_email):
    """
    Update JEPPESSENGDCrewDetail from RTF entry.
    Includes email from ERP.
    """
    from .models import JEPPESSENGDCrewDetail
    
    try:
        crew_id = entry['crew_id']
        
        # Parse dates
        birth_date = None
        passport_expiry = None
        
        if entry.get('birth_date'):
            try:
                birth_date = datetime.strptime(entry['birth_date'], '%d/%m/%y').date()
            except:
                pass
        
        if entry.get('expiry'):
            try:
                passport_expiry = datetime.strptime(entry['expiry'], '%d/%m/%y').date()
            except:
                pass
        
        # Parse name
        surname = None
        firstname = None
        full_name = entry.get('name')
        
        if full_name:
            parts = full_name.split()
            if parts:
                surname = parts[-1]
                firstname = ' '.join(parts[:-1]) if len(parts) > 1 else ''
        
        # Get or create crew detail
        crew_detail, created = JEPPESSENGDCrewDetail.objects.get_or_create(
            crew_id=crew_id,
            defaults={
                'surname': surname,
                'firstname': firstname,
                'full_name': full_name,
                'passport_number': entry.get('passport'),
                'birth_date': birth_date,
                'sex': entry.get('gender'),
                'passport_expiry': passport_expiry,
                'nationality_code': entry.get('nationality'),
                'email': crew_email,
                'last_seen_flight_date': flight_date,
            }
        )
        
        if not created:
            # Update if new data is available
            updated = False
            
            if full_name and not crew_detail.full_name:
                crew_detail.full_name = full_name
                crew_detail.surname = surname
                crew_detail.firstname = firstname
                updated = True
            
            if birth_date and not crew_detail.birth_date:
                crew_detail.birth_date = birth_date
                updated = True
            
            if entry.get('gender') and not crew_detail.sex:
                crew_detail.sex = entry['gender']
                updated = True
            
            if passport_expiry and not crew_detail.passport_expiry:
                crew_detail.passport_expiry = passport_expiry
                updated = True
            
            if entry.get('nationality') and not crew_detail.nationality_code:
                crew_detail.nationality_code = entry['nationality']
                updated = True
            
            if entry.get('passport') and not crew_detail.passport_number:
                crew_detail.passport_number = entry['passport']
                updated = True
            
            if crew_email and not crew_detail.email:
                crew_detail.email = crew_email
                updated = True
            
            # Always update last seen date
            if flight_date > (crew_detail.last_seen_flight_date or date(2000, 1, 1)):
                crew_detail.last_seen_flight_date = flight_date
                updated = True
            
            if updated:
                crew_detail.save()
        
    except Exception as e:
        logger.warning(f"Could not update crew detail for {entry.get('crew_id')}: {e}")


def get_icao_from_iata(iata_code):
    """Convert IATA to ICAO using AirportData."""
    try:
        airport = AirportData.objects.filter(iata_code=iata_code).first()
        return airport.icao_code if airport else None
    except:
        return None
    




# ============================================================================
# FLITELINK API INTEGRATION TASKS
# ============================================================================

import uuid
import requests
from datetime import datetime, timedelta
from django.conf import settings
from django.utils import timezone
from django.db import transaction

@shared_task
def submit_flight_to_flitelink(gd_flight_id):
    """
    Submit a GD flight to Flitelink API.
    Updates the existing JEPPESSENGDFlight record with submission status.
    
    Args:
        gd_flight_id: ID of JEPPESSENGDFlight record
    
    Returns:
        str: Request ID if successful, None if failed
    """
    from .models import JEPPESSENGDFlight, JEPPESSENGDCrew, FlitelinkAPILog
    
    try:
        # Get GD flight
        gd_flight = JEPPESSENGDFlight.objects.get(id=gd_flight_id)
        
        logger.info(f"Preparing Flitelink submission for flight {gd_flight.flight_no}")
        
        # Check if already submitted successfully
        if gd_flight.flitelink_status in ['QUEUED', 'COMPLETED']:
            logger.info(f"Flight already submitted with status {gd_flight.flitelink_status}")
            return str(gd_flight.flitelink_request_id)
        
        # Validate required fields
        if not gd_flight.can_submit_to_flitelink:
            error_msg = f"Missing required fields: "
            missing = []
            if not gd_flight.origin_icao: missing.append("origin_icao")
            if not gd_flight.destination_icao: missing.append("destination_icao")
            if not gd_flight.std_utc: missing.append("std_utc")
            if not gd_flight.sta_utc: missing.append("sta_utc")
            
            error_msg += ", ".join(missing)
            logger.error(error_msg)
            
            gd_flight.flitelink_status = 'FAILED'
            gd_flight.flitelink_error_message = error_msg
            gd_flight.save()
            return None
        
        # Get crew
        crew_assignments = JEPPESSENGDCrew.objects.filter(
            gd_flight=gd_flight
        ).select_related('gd_flight')
        
        if not crew_assignments.exists():
            logger.warning(f"No crew found for flight {gd_flight.flight_no}")
        
        # ✅ FIX: Generate UUID for tracking (even though API doesn't use it in headers)
        if not gd_flight.flitelink_request_id:
            tracking_id = uuid.uuid4()
            gd_flight.flitelink_request_id = tracking_id
            gd_flight.save()
        else:
            tracking_id = gd_flight.flitelink_request_id
        
        # Build payload
        payload = build_flitelink_payload(gd_flight, crew_assignments)
        
        # Update status to PENDING
        gd_flight.flitelink_status = 'PENDING'
        gd_flight.save()
        
        # Submit to API (API will return its own request_id in response)
        success = submit_to_flitelink_api(gd_flight, payload)
        
        if success:
            logger.info(f"✓ Successfully submitted flight {gd_flight.flight_no}")
            return str(gd_flight.flitelink_request_id)
        else:
            logger.error(f"✗ Failed to submit flight {gd_flight.flight_no}")
            return None
            
    except JEPPESSENGDFlight.DoesNotExist:
        logger.error(f"GD Flight {gd_flight_id} not found")
        return None
    except Exception as e:
        logger.error(f"Error submitting flight to Flitelink: {e}", exc_info=True)
        return None


def build_flitelink_payload(gd_flight, crew_assignments):
    """
    Build Flitelink API payload matching exact Postman structure.
    
    Args:
        gd_flight: JEPPESSENGDFlight instance
        crew_assignments: QuerySet of JEPPESSENGDCrew
    
    Returns:
        dict: Flitelink API payload
    """
    from .models import JEPPESSENGDCrewDetail
    
    # Combine date and time for STD/STA
    std_datetime = combine_date_time(gd_flight.flight_date, gd_flight.std_utc)
    sta_datetime = combine_date_time(gd_flight.flight_date, gd_flight.sta_utc)
    
    # Build crew list
    crew_list = []
    
    for crew_assignment in crew_assignments:
        # Get crew details
        crew_detail = JEPPESSENGDCrewDetail.objects.filter(
            crew_id=crew_assignment.crew_id
        ).first()
        
        # Map position to Flitelink format
        pos_mapping = {
            'CP': 'CPT',
            'FO': 'FO',
            'FP': 'CM',
            'SA': 'CM',
            'FA': 'CC',
            'FE': 'OTHER',
            'MX': 'OTHER',
            'AC': 'OTHER',
        }
        
        pos = pos_mapping.get(crew_assignment.position, 'OTHER')
        
        # Map function
        if crew_assignment.is_pic:
            function = 'PIC'
        elif crew_assignment.is_sic:
            function = 'SIC'
        elif crew_assignment.position in ['FP', 'SA']:
            function = 'SA'
        elif crew_assignment.position == 'FA':
            function = 'FA'
        else:
            function = 'OTHER'
        
        # Parse name
        given_name = crew_detail.firstname if crew_detail else ''
        family_name = crew_detail.surname if crew_detail else crew_assignment.crew_id
        
        # Staff ID - format as needed
        staff_id = f"WB{int(crew_assignment.crew_id):04d}"
        
        # Email
        email = crew_assignment.email or ''
        
        # Only include crew with email
        if email:
            crew_obj = {
                "pos": pos,
                "function": function,
                "givenName": given_name or "Unknown",
                "familyName": family_name or "Unknown",
                "staffId": staff_id,
                "email": email,
                "deadheading": False,
                "onDuty": None,
                "offDuty": None
            }
            crew_list.append(crew_obj)
    
    # Build sector - EXACT structure from Postman
    sector = {
        "customerUniqueId": None,  # Can use str(gd_flight.id) if needed
        "carrier": "WB",
        "flightNumber": f"WB{gd_flight.flight_no}",
        "std": std_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') if std_datetime else None,
        "sta": sta_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') if sta_datetime else None,
        "departure": {
            "icao": gd_flight.origin_icao,
            "iata": gd_flight.origin_iata,
            "domesticCode": None  # Not "name"
        },
        "destination": {
            "icao": gd_flight.destination_icao,
            "iata": gd_flight.destination_iata,
            "domesticCode": None  # Not "name"
        },
        "crew": crew_list,
        "callSign": None,
        "flightStatus": None,
        "aircraftRegistration": gd_flight.tail_no,
        "etd": None,
        "eta": None,
        "remarks": None,
        "typeOfFlight": None
    }
    
    # Final payload
    payload = {
        "sectors": [sector]
    }
    
    return payload


def submit_to_flitelink_api(gd_flight, payload):
    """
    Make HTTP POST request to Flitelink API.
    Uses exact headers from working Postman request.
    
    Args:
        gd_flight: JEPPESSENGDFlight instance
        payload: dict payload to submit
    
    Returns:
        bool: True if successful, False otherwise
    """
    from .models import FlitelinkAPILog
    
    start_time = timezone.now()
    
    # ✅ EXACT headers from Postman
    headers = {
        'x-api-key': settings.FLITELINK_API_KEY,
        'request-id': str(gd_flight.flitelink_request_id),  # UUID in headers
        'Content-Type': 'application/json'
    }
    
    try:
        logger.info(f"Sending request to Flitelink API")
        logger.info(f"Request ID: {gd_flight.flitelink_request_id}")
        logger.info(f"Payload: {payload}")
        
        response = requests.post(
            settings.FLITELINK_SUBMIT_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        end_time = timezone.now()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        
        # Parse response
        try:
            response_data = response.json()
        except:
            response_data = {'error': response.text}
        
        # Log API call
        FlitelinkAPILog.objects.create(
            gd_flight=gd_flight,
            request_type='SUBMIT',
            request_id=gd_flight.flitelink_request_id,
            endpoint=settings.FLITELINK_SUBMIT_ENDPOINT,
            http_method='POST',
            request_payload=payload,
            response_status_code=response.status_code,
            response_data=response_data,
            response_time=end_time,
            duration_ms=duration_ms,
            success=(response.status_code == 202),
            error_message=None if response.status_code == 202 else response.text
        )
        
        if response.status_code == 202:
            # Success
            logger.info(f"✓ Flight submitted successfully - HTTP 202 Accepted")
            logger.info(f"Response: {response_data}")
            
            with transaction.atomic():
                gd_flight.flitelink_status = 'QUEUED'
                gd_flight.flitelink_submitted_at = timezone.now()
                gd_flight.flitelink_response = response_data
                gd_flight.flitelink_error_message = None
                gd_flight.save()
            
            return True
            
        elif response.status_code == 400:
            error_msg = f"HTTP 400: {response_data}"
            logger.error(f"✗ Validation failed: {error_msg}")
            
            with transaction.atomic():
                gd_flight.flitelink_status = 'FAILED'
                gd_flight.flitelink_error_message = error_msg
                gd_flight.save()
            
            return False
            
        elif response.status_code == 401:
            error_msg = f"HTTP 401: Invalid API Key"
            logger.error(f"✗ {error_msg}")
            
            with transaction.atomic():
                gd_flight.flitelink_status = 'FAILED'
                gd_flight.flitelink_error_message = error_msg
                gd_flight.save()
            
            return False
            
        else:
            error_msg = f"HTTP {response.status_code}: {response.text}"
            logger.error(f"✗ API error: {error_msg}")
            
            with transaction.atomic():
                gd_flight.flitelink_status = 'FAILED'
                gd_flight.flitelink_error_message = error_msg
                gd_flight.save()
            
            return False
            
    except requests.exceptions.Timeout:
        error_msg = "Request timeout after 30 seconds"
        
        with transaction.atomic():
            gd_flight.flitelink_status = 'FAILED'
            gd_flight.flitelink_error_message = error_msg
            gd_flight.save()
        
        logger.error(f"✗ {error_msg}")
        return False
        
    except Exception as e:
        error_msg = str(e)
        
        with transaction.atomic():
            gd_flight.flitelink_status = 'FAILED'
            gd_flight.flitelink_error_message = error_msg
            gd_flight.save()
        
        logger.error(f"✗ Error: {e}", exc_info=True)
        return False


@shared_task
def check_flitelink_status():
    """
    Check status of queued Flitelink submissions.
    Runs periodically to update flight statuses.
    """
    from .models import JEPPESSENGDFlight
    
    logger.info("Checking Flitelink submission statuses...")
    
    # Get flights with QUEUED status
    queued_flights = JEPPESSENGDFlight.objects.filter(
        flitelink_status='QUEUED'
    )
    
    if not queued_flights.exists():
        logger.info("No queued submissions to check")
        return 0
    
    updated_count = 0
    
    for gd_flight in queued_flights:
        try:
            # Check if enough time has passed since last check (avoid rate limiting)
            if gd_flight.flitelink_last_check:
                time_since_check = (timezone.now() - gd_flight.flitelink_last_check).total_seconds()
                if time_since_check < 60:  # Wait at least 1 minute between checks
                    continue
            
            # Check status
            if check_submission_status(gd_flight):
                updated_count += 1
            
        except Exception as e:
            logger.error(f"Error checking status for flight {gd_flight.id}: {e}")
            continue
    
    logger.info(f"Checked {updated_count} submissions")
    return updated_count


def check_submission_status(gd_flight):
    """
    Check status via GET request.
    Uses requestId in URL path as shown in Postman.
    
    Args:
        gd_flight: JEPPESSENGDFlight instance
    
    Returns:
        bool: True if status was checked, False otherwise
    """
    from .models import FlitelinkAPILog
    
    start_time = timezone.now()
    
    # ✅ Only x-api-key header for status check
    headers = {
        'x-api-key': settings.FLITELINK_API_KEY
    }
    
    # ✅ requestId in URL path
    url = f"{settings.FLITELINK_STATUS_ENDPOINT}/{gd_flight.flitelink_request_id}/status"
    
    try:
        logger.info(f"Checking status at: {url}")
        
        response = requests.get(
            url,
            headers=headers,
            timeout=15
        )
        
        end_time = timezone.now()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        
        # Parse response
        try:
            response_data = response.json()
        except:
            response_data = {'error': response.text}
        
        # Log API call
        FlitelinkAPILog.objects.create(
            gd_flight=gd_flight,
            request_type='STATUS',
            request_id=gd_flight.flitelink_request_id,
            endpoint=url,
            http_method='GET',
            request_payload=None,
            response_status_code=response.status_code,
            response_data=response_data,
            response_time=end_time,
            duration_ms=duration_ms,
            success=(response.status_code == 200),
            error_message=None if response.status_code == 200 else response.text
        )
        
        if response.status_code == 200:
            status = response_data.get('status', 'QUEUED').upper()
            
            with transaction.atomic():
                gd_flight.flitelink_last_check = timezone.now()
                
                if status == 'COMPLETED':
                    gd_flight.flitelink_status = 'COMPLETED'
                    gd_flight.flitelink_completed_at = timezone.now()
                    logger.info(f"✓ Flight {gd_flight.flight_no} completed")
                    
                elif status in ['FAILED', 'ERROR']:
                    gd_flight.flitelink_status = 'FAILED'
                    gd_flight.flitelink_error_message = response_data.get('message', 'Unknown error')
                    logger.warning(f"✗ Flight {gd_flight.flight_no} failed: {gd_flight.flitelink_error_message}")
                    
                else:
                    gd_flight.flitelink_status = 'QUEUED'
                
                gd_flight.flitelink_response = response_data
                gd_flight.save()
            
            return True
        else:
            logger.warning(f"Status check failed with HTTP {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking status: {e}")
        return False


@shared_task
def auto_submit_recent_flights():
    """
    Automatically submit recently processed GD flights to Flitelink.
    Runs periodically to catch new flights.
    """
    from .models import JEPPESSENGDFlight
    
    if not getattr(settings, 'FLITELINK_AUTO_SUBMIT', True):
        logger.info("Auto-submit disabled")
        return 0
    
    logger.info("Checking for flights to auto-submit...")
    
    # Get flights processed in the last hour that haven't been submitted
    delay_minutes = getattr(settings, 'FLITELINK_SUBMIT_DELAY_MINUTES', 5)
    cutoff_time = timezone.now() - timedelta(minutes=delay_minutes)
    
    flights = JEPPESSENGDFlight.objects.filter(
        processed_at__gte=timezone.now() - timedelta(hours=1),
        processed_at__lte=cutoff_time,
        flitelink_status='NOT_SUBMITTED'
    )
    
    # Filter to only flights that can be submitted
    submittable = [f for f in flights if f.can_submit_to_flitelink]
    
    submitted_count = 0
    
    for flight in submittable:
        try:
            result = submit_flight_to_flitelink.delay(flight.id)
            if result:
                submitted_count += 1
                logger.info(f"✓ Auto-submitted flight {flight.flight_no}")
        except Exception as e:
            logger.error(f"Error auto-submitting flight {flight.id}: {e}")
            continue
    
    logger.info(f"Auto-submitted {submitted_count} flights")
    return submitted_count


# @shared_task
# def retry_failed_submissions():
#     """
#     Retry failed Flitelink submissions (up to 3 times).
#     Runs periodically to retry failures.
#     """
#     from .models import JEPPESSENGDFlight
    
#     logger.info("Checking for failed submissions to retry...")
    
#     max_retries = 3
    
#     # Get failed flights that haven't exceeded retry limit
#     failed_flights = JEPPESSENGDFlight.objects.filter(
#         flitelink_status='FAILED',
#         flitelink_retry_count__lt=max_retries
#     )
    
#     if not failed_flights.exists():
#         logger.info("No failed submissions to retry")
#         return 0
    
#     retried_count = 0
    
#     for flight in failed_flights:
#         try:
#             # Increment retry count
#             flight.flitelink_retry_count += 1
#             flight.flitelink_status = 'PENDING'
#             flight.flitelink_error_message = None
#             flight.save()
            
#             # Submit
#             result = submit_flight_to_flitelink.delay(flight.id)
#             if result:
#                 retried_count += 1
#                 logger.info(f"✓ Retried flight {flight.flight_no} (attempt {flight.flitelink_retry_count})")
            
#         except Exception as e:
#             logger.error(f"Error retrying flight {flight.id}: {e}")
#             continue
    
#     logger.info(f"Retried {retried_count} failed submissions")
#     return retried_count



@shared_task
def retry_failed_submissions():
    """
    Retry failed Flitelink submissions (up to 3 times).
    Also handles stuck PENDING flights.
    Runs periodically to retry failures.
    """
    from .models import JEPPESSENGDFlight
    
    logger.info("Checking for failed/pending submissions to retry...")
    
    max_retries = 3
    
    # ✅ CHANGED: Get FAILED and stuck PENDING flights
    failed_flights = JEPPESSENGDFlight.objects.filter(
        flitelink_status__in=['FAILED', 'PENDING'],  # ← Added PENDING
        flitelink_retry_count__lt=max_retries
    )
    
    # ✅ NEW: Also get stuck PENDING flights (older than 10 minutes)
    from django.utils import timezone
    stuck_pending = JEPPESSENGDFlight.objects.filter(
        flitelink_status='PENDING',
        flitelink_submitted_at__lt=timezone.now() - timedelta(minutes=10),
        flitelink_retry_count__lt=max_retries
    )
    
    all_flights = (failed_flights | stuck_pending).distinct()
    
    if not all_flights.exists():
        logger.info("No failed/pending submissions to retry")
        return 0
    
    retried_count = 0
    
    for flight in all_flights:
        try:
            # Increment retry count
            flight.flitelink_retry_count += 1
            flight.flitelink_status = 'PENDING'
            flight.flitelink_error_message = None
            flight.save()
            
            # Submit
            result = submit_flight_to_flitelink.delay(flight.id)
            if result:
                retried_count += 1
                logger.info(f"✓ Retried flight {flight.flight_no} (attempt {flight.flitelink_retry_count})")
            
        except Exception as e:
            logger.error(f"Error retrying flight {flight.id}: {e}")
            continue
    
    logger.info(f"Retried {retried_count} failed/pending submissions")
    return retried_count


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def combine_date_time(date_obj, time_obj):
    """
    Combine date and time into datetime with UTC timezone.
    
    Args:
        date_obj: date object
        time_obj: time object
    
    Returns:
        datetime: Combined datetime in UTC
    """
    if date_obj and time_obj:
        # Import UTC from Python's datetime module (NOT django.utils.timezone)
        from datetime import timezone as dt_timezone
        
        # Combine date and time
        naive_datetime = datetime.combine(date_obj, time_obj)
        
        # Add UTC timezone info
        return naive_datetime.replace(tzinfo=dt_timezone.utc)
    
    return None