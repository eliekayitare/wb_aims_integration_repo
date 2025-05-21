
#Tasks for ACARS project

from celery import shared_task, chain
from exchangelib import Credentials, Account, Configuration, EWSDateTime, EWSTimeZone
from .utils import process_email_attachment, process_airport_file, process_flight_schedule_file, process_acars_message, process_cargo_email_attachment, process_cargo_flight_schedule_file,process_fdm_email_attachment,process_fdm_flight_schedule_file,process_fdm_crew_email_attachment,process_crew_details_file,process_tableau_data_email_attachment,process_tableau_data_file
import logging
from django.conf import settings
from datetime import datetime

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
        subject__contains='AIMS JOB : #1003 Airport details Feed to WB server file attached'
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing airport data email with subject: {email.subject}")
        process_email_attachment(email, process_airport_file)
        # Trigger flight schedule task immediately after processing airport data
        fetch_flight_schedules.apply_async()
    else:
        logger.info("No airport data email found.")

@shared_task
def fetch_flight_schedules():
    account = get_exchange_account()
    logger.info("Fetching the most recent flight schedule email...")

    emails = account.inbox.filter(
        subject__contains='AIMS JOB : #1002 Flight schedule feed to WB server file attached'
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing the most recent flight schedule email with subject: {email.subject}")
        process_email_attachment(email, process_flight_schedule_file)
    else:
        logger.info("No new flight schedule email found.")


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


# @shared_task
# def fetch_acars_messages():
#     account = get_exchange_account()
#     logger.info("Fetching and processing ACARS messages...")

#     # Fetch a batch of the oldest unread messages
#     # emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')[:5]
#     emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')
#     # email = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received').first()

#     # Process each email in the batch
#     for item in emails:
#         if "M16" in item.body:
#             logger.info(f"Skipping 'M16' ACARS message: {item.subject}")
#             item.is_read = True
#             item.save(update_fields=['is_read'])
#             continue

#         # Log and process the ACARS message
#         logger.info(f"Processing ACARS email with subject: {item.subject}")
#         process_acars_message(item)

#         # Mark as read to avoid reprocessing
#         item.is_read = True
#         item.save(update_fields=['is_read'])

#     # Check if more unread messages are available and re-trigger task if needed
#     if account.inbox.filter(subject__icontains='ARR', is_read=False).exists():
#         fetch_acars_messages.apply_async(countdown=1)  # Re-run task with a short delay

#     logger.info("Batch processing of ACARS emails completed.")



from celery import shared_task
from celery.utils.log import get_task_logger
import os
from .utils import  process_acars_message

logger = get_task_logger(__name__)

@shared_task(bind=True)
def fetch_acars_messages(self):
    account = get_exchange_account()
    logger.info("Fetching and processing ACARS messages...")

    # Define the file path for JOB1.txt
    file_path = os.path.join(settings.MEDIA_ROOT, "JOB1.txt")

    # Start with a fresh file for each task execution
    open(file_path, 'w').close()  # Clear file contents

    # Fetch all unread ACARS messages
    emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')

    if not emails.exists():
        logger.info("No unread ACARS messages found. Skipping processing.")
        return

    # Process each email
    for item in emails:
        if "M16" in item.body:
            logger.info(f"Skipping 'M16' ACARS message: {item.subject}")
            item.is_read = True
            item.save(update_fields=['is_read'])
            continue

        logger.info(f"Processing ACARS email with subject: {item.subject}")
        process_acars_message(item, file_path)

        # Mark the email as read to avoid reprocessing
        item.is_read = True
        item.save(update_fields=['is_read'])

    # Upload the file to the AIMS server after processing the batch
    if os.path.getsize(file_path) > 0:  # Ensure the file is not empty
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

@shared_task
def fetch_fdm_flight_schedules():
    account = get_exchange_account()
    logger.info("Fetching the most recent fdm flight schedule email...")

    emails = account.inbox.filter(
        subject__contains='AIMS JOB : #1002 Flight schedule feed to FDM file attached'
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing the most recent fdm flight schedule email with subject: {email.subject}")
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
        subject__contains='AIMS JOB : #1011 Flight OPS stat dashboard file attached'
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing the most recent tableau data email with subject: {email.subject}")
        process_tableau_data_email_attachment(email, process_tableau_data_file)
    else:
        logger.info("No new tableau email found.")



from datetime import datetime, timedelta, timezone
from exchangelib.errors import ErrorServerBusy

@shared_task(bind=True, max_retries=5)
def delete_old_emails(self):
    """
    Deletes all emails that are older than 10 days in the inbox,
    with a retry strategy to handle ErrorServerBusy.
    """
    account = get_exchange_account()
    days_to_keep = 7

    # Create a Python datetime in UTC and convert to EWSDateTime
    cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
    cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
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

import csv
import os
import time
import logging
import uuid
from datetime import datetime, timedelta

# Django imports
from django.conf import settings
from django.core.mail import EmailMessage, get_connection
from django.db import transaction
from django.utils import timezone

# Celery imports
from celery import shared_task
from celery.utils.log import get_task_logger

# Your models - make sure to import the new Dreammiles models
from .models import *

# Initialize loggers
logger = logging.getLogger(__name__)
task_logger = get_task_logger(__name__)
# Dreammiles email campaign tasks
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
        logger.info(f"Active campaign already exists: {active_campaign.name} (ID: {active_campaign.id})")
        return {"status": "skipped", "message": "Active campaign exists"}
    
    # Check for CSV file
    csv_path = os.path.join(settings.DATA_DIR, 'dreamilesnotice.csv')
    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found at {csv_path}")
        return {"status": "error", "message": "CSV file not found"}
    
    # Start a new campaign
    try:
        campaign = DreammilesCampaign.objects.create(
            name=f"Dreammiles Business Lite Promotion - {datetime.now().strftime('%Y-%m-%d')}",
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
        
        # Open CSV file
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
                
                # Determine batch number (3000 emails per batch)
                batch_number = total_records // 3000
                
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
                    try:
                        with transaction.atomic():
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
def process_dreammiles_batch(campaign_id, batch_number=None, max_emails=3000):
    """
    Process a batch of emails for sending.
    If batch_number is None, it will use the campaign's current_batch.
    """
    try:
        # Get campaign and validate status
        campaign = DreammilesCampaign.objects.get(id=campaign_id)
        
        if campaign.status not in ['processing']:
            logger.info(f"Campaign {campaign.name} is not in processing state: {campaign.status}")
            return {"status": "skipped", "message": f"Campaign status is {campaign.status}"}
        
        # Determine which batch to process
        if batch_number is None:
            batch_number = campaign.current_batch
        
        logger.info(f"Processing batch {batch_number} for campaign {campaign.name}")
        
        # Get records for this batch
        with transaction.atomic():
            pending_records = list(DreamilesEmailRecord.objects.select_for_update().filter(
                campaign=campaign,
                batch_number=batch_number,
                status='pending'
            )[:max_emails])
            
            if pending_records:
                record_ids = [record.id for record in pending_records]
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
        
        # Get email connection
        connection = get_connection()
        connection.open()
        
        # Process emails
        success_count = 0
        failure_count = 0
        
        # Process in smaller chunks
        chunk_size = 100
        for i in range(0, len(pending_records), chunk_size):
            chunk = pending_records[i:i+chunk_size]
            
            # Prepare messages
            messages = []
            for record in chunk:
                try:
                    email_body = campaign.email_body.format(
                        first_name=record.first_name or "Dreammiles Member"
                    )
                    
                    message = EmailMessage(
                        subject=campaign.subject,
                        body=email_body,
                        from_email=settings.EMAIL_HOST_USER,
                        to=[record.email],
                        connection=connection
                    )
                    
                    messages.append((record, message))
                    
                except Exception as e:
                    logger.error(f"Error preparing email for {record.email}: {str(e)}")
                    record.status = 'failed'
                    record.error_message = str(e)
                    record.save(update_fields=['status', 'error_message'])
                    failure_count += 1
            
            # Send messages in micro-batches
            for j in range(0, len(messages), 20):
                micro_batch = messages[j:j+20]
                micro_batch_messages = [msg for _, msg in micro_batch]
                
                try:
                    connection.send_messages(micro_batch_messages)
                    
                    # Update records
                    for record, _ in micro_batch:
                        record.status = 'sent'
                        record.sent_at = datetime.now()
                        record.save(update_fields=['status', 'sent_at'])
                        success_count += 1
                    
                except Exception as e:
                    logger.error(f"Error sending email batch: {str(e)}")
                    
                    # Mark as failed
                    for record, _ in micro_batch:
                        record.status = 'failed'
                        record.error_message = str(e)
                        record.retry_count = record.retry_count + 1
                        record.save(update_fields=['status', 'error_message', 'retry_count'])
                        failure_count += 1
                
                # Small delay between micro-batches
                time.sleep(0.2)
            
            # Delay between chunks
            if i + chunk_size < len(pending_records):
                time.sleep(1)
        
        # Close connection
        connection.close()
        
        # Update campaign stats
        with transaction.atomic():
            campaign.refresh_from_db()
            campaign.emails_sent += success_count
            campaign.emails_failed += failure_count
            campaign.last_sent_at = datetime.now()
            campaign.save(update_fields=['emails_sent', 'emails_failed', 'last_sent_at'])
        
        logger.info(f"Batch {batch_number} complete: {success_count} sent, {failure_count} failed")
        
        # Check if more emails in current batch
        remaining_in_batch = DreamilesEmailRecord.objects.filter(
            campaign=campaign,
            batch_number=batch_number,
            status='pending'
        ).exists()
        
        if remaining_in_batch:
            # Process next chunk in this batch after a short delay
            process_dreammiles_batch.apply_async(
                args=[campaign_id, batch_number, max_emails],
                countdown=300  # 5 minutes delay before continuing same batch
            )
        else:
            # Move to next batch if exists
            next_batch = batch_number + 1
            next_batch_exists = DreamilesEmailRecord.objects.filter(
                campaign=campaign,
                batch_number=next_batch,
                status='pending'
            ).exists()
            
            if next_batch_exists:
                campaign.current_batch = next_batch
                campaign.save(update_fields=['current_batch'])
                logger.info(f"Batch {batch_number} complete. Moving to batch {next_batch}")
            else:
                # Check if there are any pending records in other batches
                next_pending = DreamilesEmailRecord.objects.filter(
                    campaign=campaign,
                    status='pending'
                ).order_by('batch_number').first()
                
                if next_pending:
                    next_batch = next_pending.batch_number
                    campaign.current_batch = next_batch
                    campaign.save(update_fields=['current_batch'])
                    logger.info(f"Moving to batch {next_batch}")
                else:
                    # Check if campaign is complete
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
            "failed": failure_count
        }
        
    except DreammilesCampaign.DoesNotExist:
        logger.error(f"Campaign not found with ID: {campaign_id}")
        return {"status": "error", "message": "Campaign not found"}
    
    except Exception as e:
        logger.error(f"Error processing email batch: {str(e)}")
        
        # Mark processing emails back as pending
        try:
            DreamilesEmailRecord.objects.filter(
                campaign_id=campaign_id,
                status='processing'
            ).update(status='pending')
        except Exception:
            pass
        
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
        duration = datetime.now() - campaign.created_at
        hours = duration.total_seconds() / 3600
        
        # Generate report
        report = f"""
DREAMMILES EMAIL CAMPAIGN REPORT
===============================

Campaign: {campaign.name}
Started: {campaign.created_at.strftime('%Y-%m-%d %H:%M:%S')}
Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Duration: {int(hours)} hours, {int((hours % 1) * 60)} minutes

SUMMARY
-------
Total Recipients: {total:,}
Successfully Sent: {sent:,} ({round(sent/total*100 if total > 0 else 0, 2)}%)
Failed: {failed:,} ({round(failed/total*100 if total > 0 else 0, 2)}%)

"""
        
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
                report += f"{error_type}: {count} occurrences\n"
            
            report += "\nSample Failed Emails:\n"
            report += "-------------------\n"
            
            for i, record in enumerate(failed_samples[:10]):
                report += f"{i+1}. {record.email}: {record.error_message[:100]}\n"
        
        # Send report
        recipients = [
            'winnie.gashumba@rwandair.com',
            'pacifique.byiringiro@rwandair.com'
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