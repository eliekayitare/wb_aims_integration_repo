
#Tasks for ACARS project

from celery import shared_task, chain
from exchangelib import Credentials, Account, Configuration, EWSDateTime
from .utils import process_email_attachment, process_airport_file, process_flight_schedule_file, process_acars_message, process_cargo_email_attachment, process_cargo_flight_schedule_file,process_fdm_email_attachment,process_fdm_flight_schedule_file,process_fdm_crew_email_attachment,process_crew_details_file
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





# @shared_task
# def fetch_acars_messages():
#     account = get_exchange_account()
#     logger.info("Fetching and processing ACARS messages...")

#     # Fetch the oldest unread email safely
#     emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')
#     email = next(iter(emails), None)  # Use `next` to get the first email or `None` if empty

#     while email:
#         # Check if the email contains "M16" in the body
#         if "M16" in email.body:
#             logger.info(f"Skipping 'M16' ACARS message: {email.subject}")
#             email.is_read = True
#             email.save(update_fields=['is_read'])
#             logger.info("Marked 'M16' message as read. Proceeding to the next message.")
#         else:
#             # Log and process the ACARS message
#             logger.info(f"Processing ACARS email with subject: {email.subject}")
#             process_acars_message(email)

#             # Mark as read to avoid reprocessing
#             email.is_read = True
#             email.save(update_fields=['is_read'])

#             logger.info("Processed an ACARS message. Exiting after handling one message.")
#             break  # Exit after processing one non-"M16" message

#         # Fetch the next oldest unread email
#         emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')
#         email = next(iter(emails), None)  # Get the next email or `None` if no more emails

#     if not email:
#         logger.info("No more unread ACARS messages or only 'M16' messages left. Task will run again at the next schedule.")

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
import time
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
    "FRMS": 12,
    "ETPG": 36,
    "LVO-G": 36,
    "PBNGRN": 12,
    "RVSMGS": 0,
}


def parse_date(date_str):
    """Safely parse date from 'YYYY-MM-DD' format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def calculate_expiry_date(completion_date, course_code):
    """Calculate expiry date based on course validity."""
    if not completion_date:
        return ""
    validity_period = VALIDITY_PERIODS.get(course_code, 0)
    if validity_period == 0:
        return ""
    expiry_date = completion_date + relativedelta(months=validity_period)
    return expiry_date.strftime("%d%m%Y")


def format_date(date_str):
    """Convert date from 'YYYY-MM-DD' to 'DDMMYYYY' format."""
    date = parse_date(date_str)
    return date.strftime("%d%m%Y") if date else ""


def generate_job8_file(records):
    """Generate the JOB8.txt file."""
    file_path = os.path.join(settings.MEDIA_ROOT, "JOB8.txt")
    with open(file_path, "w", newline="") as file:
        for record in records:
            staff_number = record.get("StaffNumber", "")
            expiry_code = record.get("ExpiryCode", "")
            last_done_date = record.get("LastDoneDate", "")
            expiry_date = record.get("ExpiryDate", "")
            file.write(f"{staff_number},{expiry_code},{last_done_date},{expiry_date}\n")
    logger.info(f"JOB8.txt file created: {file_path}")
    return file_path


def upload_cpat_to_aims_server(local_file_path):
    """Upload file to AIMS server with retries."""
    attempts = 3
    delay = 5
    if not os.path.exists(local_file_path):
        logger.error(f"File {local_file_path} does not exist.")
        return

    remote_path = os.path.join(CPAT_AIMS_PATH, os.path.basename(local_file_path))
    for attempt in range(1, attempts + 1):
        try:
            with paramiko.Transport((AIMS_HOST, AIMS_PORT)) as transport:
                transport.connect(username=AIMS_USERNAME, password=AIMS_PASSWORD)
                with paramiko.SFTPClient.from_transport(transport) as sftp:
                    sftp.put(local_file_path, remote_path)
            logger.info(f"File successfully uploaded to {remote_path}")
            return
        except Exception as e:
            logger.error(f"Upload attempt {attempt} failed: {e}")
            time.sleep(delay)
    logger.error("All upload attempts failed.")


@shared_task
def fetch_and_store_completion_records():
    """Fetch CPAT completion records, update database, generate JOB8.txt, and send it."""
    url = f"{LMS_BASE_URL}/lms/api/IntegrationAPI/comp/v2/{LMS_KEY}/{DAYS}"
    headers = {"apitoken": API_TOKEN, "Content-Type": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data: {e}")
        return

    data = response.json()
    records_for_file = []

    for record in data:
        try:
            # Extract required fields
            employee_id = record.get("EmployeeID")
            course_code = record.get("CourseCode")
            completion_date = record.get("CompletionDate")
            start_date = record.get("StartDate")
            end_date = record.get("EndDate")

            # Validate critical fields
            if not employee_id or not course_code or not completion_date:
                logger.warning(f"Skipping record due to missing data: {record}")
                continue

            # Parse completion date
            try:
                completion_date_parsed = datetime.strptime(completion_date, "%d%m%Y").date()
            except ValueError:
                logger.warning(f"Skipping record due to invalid completionDate format: {record}")
                continue

            # Parse start and end dates safely
            start_date_parsed = datetime.strptime(start_date, "%d%m%Y").date() if start_date else None
            end_date_parsed = datetime.strptime(end_date, "%d%m%Y").date() if end_date else None

            # Check for existing record
            existing_record = CompletionRecord.objects.filter(
                employee_id=employee_id,
                course_code=course_code,
                completion_date=completion_date_parsed,
            ).first()

            if existing_record:
                logger.info(f"Skipping identical record: {employee_id} - {course_code}")
                continue

            # Insert or update record
            CompletionRecord.objects.update_or_create(
                employee_id=employee_id,
                course_code=course_code,
                completion_date=completion_date_parsed,
                defaults={
                    "employee_email": record.get("EmployeeEmail"),
                    "score": record.get("Score", 0.0),
                    "time_in_seconds": record.get("TimeInSecond", 0),
                    "start_date": start_date_parsed,
                    "end_date": end_date_parsed,
                },
            )

            # Prepare record for JOB8.txt
            expiry_date = calculate_expiry_date(completion_date_parsed.strftime("%Y-%m-%d"), course_code)
            records_for_file.append({
                "StaffNumber": employee_id,
                "ExpiryCode": course_code,
                "LastDoneDate": completion_date_parsed.strftime("%d%m%Y"),
                "ExpiryDate": expiry_date,
            })

        except Exception as e:
            logger.error(f"Error processing record: {record} - {e}", exc_info=True)

    if not records_for_file:
        logger.info("No new records to process for JOB8.txt.")
        return

    # Generate JOB8.txt
    file_path = generate_job8_file(records_for_file)
    if not os.path.exists(file_path) or os.stat(file_path).st_size == 0:
        logger.error("Generated JOB8.txt is empty. Aborting upload.")
        return

    # Upload file
    upload_cpat_to_aims_server(file_path)

    logger.info("CPAT completion records processed successfully.")





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
        subject__contains="AIMS JOB : #16.A Download List of flight legs with crew names to FDM + ' file attached"
    ).order_by('-datetime_received')
    
    email = emails[0] if emails else None

    if email:
        logger.info(f"Processing the most recent fdm crew data email with subject: {email.subject}")
        process_fdm_crew_email_attachment(email, process_crew_details_file)
    else:
        logger.info("No new fdm flight schedule email found.")




# FDM project tasks


#Fetch flight and crew data modified within the last hour
from django.utils.timezone import now, timedelta
from django.db.models import F
from .models import FdmFlightData, CrewMember
# Calculate time range
one_hour_ago = now() - timedelta(hours=1)

# Fetch flights updated in the last hour
flight_data = FdmFlightData.objects.filter(updated_at__gte=one_hour_ago)

# Fetch crew details for those flights
crew_data = CrewMember.objects.filter(
    flight_no__in=flight_data.values_list('flight_no', flat=True),
    sd_date_utc__in=flight_data.values_list('sd_date_utc', flat=True)
)



import csv
import os
import paramiko
import logging
from django.utils.timezone import now
from django.conf import settings
from paramiko.ssh_exception import SSHException, NoValidConnectionsError

# Initialize logger
logger = logging.getLogger(__name__)

def generate_csv_for_fdm(flight_data):
    """
    Generate a CSV file for flight data without crew details.
    """
    file_name = f"aims_{now().strftime('%Y%m%d%H%M')}.csv"
    file_path = os.path.join(settings.MEDIA_ROOT, file_name)

    # Header row
    header = [
        "DAY", "FLT", "FLTYPE", "REG", "DEP", "ARR", "STD", "STA",
        "TKOF", "TDOWN", "BLOF", "BLON", "ETD", "ETA","ATD","OFF","ON","ATA"
    ]

    # Create the CSV file
    with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)  # Write header row

        # Write flight data rows
        for flight in flight_data:
            # Format times as HH:MM
            format_time = lambda t: t.strftime("%H:%M") if t else None

            # Flight data row
            flight_row = [
                flight.sd_date_utc,  # DAY
                flight.flight_no,  # FLT
                flight.flight_type,  # FLTYPE
                flight.tail_no,  # REG
                flight.dep_code_icao,  # DEP
                flight.arr_code_icao,  # ARR
                format_time(flight.std_utc),  # STD
                format_time(flight.sta_utc),  # STA
                format_time(flight.takeoff_utc),  # TKOF
                format_time(flight.touchdown_utc),  # TDOWN
                format_time(flight.atd_utc),  # BLOF
                format_time(flight.ata_utc),  # BLON
                format_time(flight.etd_utc),  # ETD
                format_time(flight.eta_utc),  # ETA
                format_time(flight.atd_utc),  # ATD
                format_time(flight.takeoff_utc),  # OFF
                format_time(flight.touchdown_utc),  # ON
                format_time(flight.ata_utc),  # ATA
            ]
            writer.writerow(flight_row)

    logger.info(f"CSV file generated at: {file_path}")
    return file_path





# import os
# import paramiko
# import logging
# from django.utils.timezone import now, timedelta
# from django.conf import settings
# from paramiko.ssh_exception import SSHException, NoValidConnectionsError
# from celery import shared_task

# # Initialize logger
# logger = logging.getLogger(__name__)

# @shared_task
# def hourly_upload_csv_to_fdm():
#     """
#     Celery task to generate, upload CSV to FDM server, and clean up old files except the current one.
#     """
#     # Calculate the time range
#     one_hour_ago = now() - timedelta(hours=1)

#     # Fetch flight data
#     flight_data = FdmFlightData.objects.filter(updated_at__gte=one_hour_ago)

#     # Generate CSV file (new version: without crew data)
#     local_file_path = generate_csv_for_fdm(flight_data)

#     # Fetch server details from settings
#     fdm_host = settings.FDM_HOST
#     fdm_port = int(settings.FDM_PORT)
#     fdm_username = settings.FDM_USERNAME
#     fdm_password = settings.FDM_PASSWORD
#     fdm_destination_dir = settings.FDM_DESTINATION_DIR

#     # Ensure the file exists
#     if not os.path.exists(local_file_path):
#         logger.error(f"File {local_file_path} does not exist. Skipping upload.")
#         return

#     try:
#         # SFTP Upload Logic
#         transport = paramiko.Transport((fdm_host, fdm_port))
#         transport.connect(username=fdm_username, password=fdm_password)
#         sftp = paramiko.SFTPClient.from_transport(transport)

#         # Define remote path
#         remote_path = os.path.join(fdm_destination_dir, os.path.basename(local_file_path))
#         logger.info(f"Uploading file to {remote_path}...")

#         # Upload file
#         sftp.put(local_file_path, remote_path)
#         logger.info(f"File successfully uploaded to {remote_path}.")

#         # Close connections
#         sftp.close()
#         transport.close()

#         # Clean up local files, excluding the current file
#         local_dir = os.path.dirname(local_file_path)
#         for file in os.listdir(local_dir):
#             file_path = os.path.join(local_dir, file)
#             # Skip the currently uploaded file
#             if file_path != local_file_path and file.startswith("aims_") and file.endswith(".csv"):
#                 try:
#                     os.remove(file_path)
#                     logger.info(f"Deleted old local file: {file_path}")
#                 except Exception as e:
#                     logger.error(f"Failed to delete old file {file_path}: {e}")

#     except (SSHException, NoValidConnectionsError) as e:
#         logger.error(f"SFTP upload failed: {e}", exc_info=True)
#     except Exception as e:
#         logger.error(f"An unexpected error occurred during file upload: {e}", exc_info=True)



import os
import paramiko
import logging
from django.utils.timezone import now, timedelta
from django.conf import settings
from paramiko.ssh_exception import SSHException, NoValidConnectionsError
from celery import shared_task
from .models import FdmFlightData

# Initialize logger
logger = logging.getLogger(__name__)

@shared_task
def hourly_upload_csv_to_fdm():
    """
    Celery task to generate and upload CSV to FDM server, 
    focusing only on records with complete actual timings,
    and clean up old files except the current one.
    """
    # Calculate the time range
    one_hour_ago = now() - timedelta(hours=1)

    # Fetch only flight data with complete actual timings
    flight_data = FdmFlightData.objects.filter(
        updated_at__gte=one_hour_ago,
        atd_utc__isnull=False,
        takeoff_utc__isnull=False,
        touchdown_utc__isnull=False,
        ata_utc__isnull=False
    )

    if not flight_data.exists():
        logger.info("No flight records with complete actual timings to process. Skipping upload.")
        return

    # Generate CSV file (new version: without crew data)
    local_file_path = generate_csv_for_fdm(flight_data)

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
        # SFTP Upload Logic
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

        # Clean up local files, excluding the current file
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
