
#Tasks for ACARS project

from celery import shared_task, chain
from exchangelib import Credentials, Account, Configuration, EWSDateTime
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


# #Fetch flight and crew data modified within the last hour
# from django.utils.timezone import now, timedelta
# from django.db.models import F
# from .models import FdmFlightData, CrewMember
# # Calculate time range
# one_hour_ago = now() - timedelta(hours=1)

# # Fetch flights updated in the last hour
# flight_data = FdmFlightData.objects.filter(updated_at__gte=one_hour_ago)

# # Fetch crew details for those flights
# crew_data = CrewMember.objects.filter(
#     flight_no__in=flight_data.values_list('flight_no', flat=True),
#     sd_date_utc__in=flight_data.values_list('sd_date_utc', flat=True)
# )



# import csv
# import os
# import paramiko
# import logging
# from django.utils.timezone import now
# from django.conf import settings
# from paramiko.ssh_exception import SSHException, NoValidConnectionsError

# # Initialize logger
# logger = logging.getLogger(__name__)

# def generate_csv_for_fdm(flight_data):
#     """
#     Generate a CSV file for flight data without crew details.
#     """
#     file_name = f"aims_{now().strftime('%Y%m%d%H%M')}.csv"
#     file_path = os.path.join(settings.MEDIA_ROOT, file_name)

#     # Header row
#     header = [
#         "DAY", "FLT", "FLTYPE", "REG", "DEP", "ARR", "STD", "STA",
#         "TKOF", "TDOWN", "BLOF", "BLON", "ETD", "ETA","ATD","OFF","ON","ATA"
#     ]

#     # Create the CSV file
#     with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerow(header)  # Write header row

#         # Write flight data rows
#         for flight in flight_data:
#             # Format times as HH:MM
#             format_time = lambda t: t.strftime("%H:%M") if t else None

#             # Flight data row
#             flight_row = [
#                 flight.sd_date_utc,  # DAY
#                 flight.flight_no,  # FLT
#                 flight.flight_type,  # FLTYPE
#                 flight.tail_no,  # REG
#                 flight.dep_code_icao,  # DEP
#                 flight.arr_code_icao,  # ARR
#                 format_time(flight.std_utc),  # STD
#                 format_time(flight.sta_utc),  # STA
#                 format_time(flight.takeoff_utc),  # TKOF
#                 format_time(flight.touchdown_utc),  # TDOWN
#                 format_time(flight.atd_utc),  # BLOF
#                 format_time(flight.ata_utc),  # BLON
#                 format_time(flight.etd_utc),  # ETD
#                 format_time(flight.eta_utc),  # ETA
#                 format_time(flight.atd_utc),  # ATD
#                 format_time(flight.takeoff_utc),  # OFF
#                 format_time(flight.touchdown_utc),  # ON
#                 format_time(flight.ata_utc),  # ATA
#             ]
#             writer.writerow(flight_row)

#     logger.info(f"CSV file generated at: {file_path}")
#     return file_path




# import os
# import paramiko
# import logging
# from django.utils.timezone import now, timedelta
# from django.conf import settings
# from paramiko.ssh_exception import SSHException, NoValidConnectionsError
# from celery import shared_task
# from .models import FdmFlightData

# # Initialize logger
# logger = logging.getLogger(__name__)

# @shared_task
# def hourly_upload_csv_to_fdm():
#     """
#     Celery task to generate and upload CSV to FDM server, 
#     focusing only on records with complete actual timings,
#     and clean up old files except the current one.
#     """
#     # Calculate the time range
#     one_hour_ago = now() - timedelta(hours=1)

#     # Fetch only flight data with complete actual timings
#     flight_data = FdmFlightData.objects.filter(
#         updated_at__gte=one_hour_ago,
#         atd_utc__isnull=False,
#         takeoff_utc__isnull=False,
#         touchdown_utc__isnull=False,
#         ata_utc__isnull=False
#     )

#     if not flight_data.exists():
#         logger.info("No flight records with complete actual timings to process. Skipping upload.")
#         return

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
    )
    
    return flight_data, crew_data


def generate_csv_for_fdm(flight_data, crew_data):
    """
    Generate a CSV with columns:
      DAY, FLT, FLTYPE, REG, DEP, ARR, STD, STA, TKOF, TDOWN, BLOF, BLON,
      ETD, ETA, ATD, OFF, ON, ATA, CP, FO
    The 'CP' and 'FO' columns will contain comma-separated crew IDs if multiple exist.
    """
    file_name = f"aims_{now().strftime('%Y%m%d%H%M')}.csv"
    file_path = os.path.join(settings.MEDIA_ROOT, file_name)

    #
    # 1) Build a lookup: flight_key -> { "CP": [...], "FO": [...] }
    #
    #    flight_key = (flight_no, sd_date_utc, origin, destination)
    #
    flight_crew_lookup = {}
    for c in crew_data:
        key = (c.flight_no, c.sd_date_utc, c.origin, c.destination)
        if key not in flight_crew_lookup:
            flight_crew_lookup[key] = {"CP": [], "FO": []}
        
        # Only store CP/FO
        if c.role == "CP":
            flight_crew_lookup[key]["CP"].append(str(c.crew_id))
        elif c.role == "FO":
            flight_crew_lookup[key]["FO"].append(str(c.crew_id))

    #
    # 2) Define the fixed header (base flight columns plus CP, FO)
    #
    header = [
        "DAY", "FLT", "FLTYPE", "REG", "DEP", "ARR",
        "STD", "STA", "TKOF", "TDOWN", "BLOF", "BLON",
        "ETD", "ETA", "ATD", "OFF", "ON", "ATA",
        "CP",
        "FO"
    ]

    #
    # 3) Write the CSV
    #
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
            cp_str = ",".join(crew_dict["CP"])
            fo_str = ",".join(crew_dict["FO"])

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
                cp_str,                              # CP
                fo_str                               # FO
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