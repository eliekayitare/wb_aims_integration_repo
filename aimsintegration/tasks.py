
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



#Tasks for CPAT project

import os
import csv
import requests
from datetime import datetime
from django.conf import settings
from .models import CompletionRecord
import paramiko
import logging
from celery import shared_task
from django.core.mail import send_mail
import time
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
            existing_record = CompletionRecord.objects.filter(
                employee_id=record.get("EmployeeID"),
                course_code=record.get("CourseCode"),
                completion_date=record.get("CompletionDate"),
            ).first()

            if existing_record:
                is_identical = (
                    existing_record.employee_email == record.get("EmployeeEmail") and
                    existing_record.score == record.get("Score") and
                    existing_record.time_in_seconds == record.get("TimeInSecond") and
                    existing_record.start_date == record.get("StartDate") and
                    existing_record.end_date == record.get("EndDate")
                )
                if is_identical:
                    logger.info(f"Skipping identical record: {record.get('EmployeeID')} - {record.get('CourseCode')}")
                    continue

            CompletionRecord.objects.update_or_create(
                employee_id=record.get("EmployeeID"),
                course_code=record.get("CourseCode"),
                completion_date=record.get("CompletionDate"),
                defaults={
                    "employee_email": record.get("EmployeeEmail"),
                    "score": record.get("Score"),
                    "time_in_seconds": record.get("TimeInSecond"),
                    "start_date": record.get("StartDate"),
                    "end_date": record.get("EndDate"),
                },
            )

            # Prepare record for JOB8.txt
            records_for_file.append({
                "StaffNumber": record.get("EmployeeID"),
                "ExpiryCode": record.get("CourseCode"),
                "LastDoneDate": format_date(record.get("CompletionDate")),
                "ExpiryDate": format_date(record.get("EndDate")),
            })

        if not records_for_file:
            logger.info("No new data to process.")
            return

        file_path = generate_job8_file(records_for_file)
        upload_cpat_to_aims_server(file_path)
        email_job8_file(file_path, data)

        logger.info("CPAT completion records processed successfully.")
    else:
        logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")




# def fixed_length(value, length):
#     """Ensure the value is right-padded or truncated to the fixed length."""
#     if value is None:
#         value = ""
#     value = str(value)
#     return value[:length].ljust(length)


def format_date(date_str):
    """Convert date from YYYY-MM-DD to DDMMYYYY format or return an empty string."""
    if date_str:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d%m%Y")
    return ""


# def generate_job8_file(records):
#     """Generate the JOB8.txt file in the specified format."""
#     file_path = os.path.join(settings.MEDIA_ROOT, "JOB8.txt")
#     with open(file_path, "w", newline="") as file:
#         for record in records:
#             staff_number = fixed_length(record["StaffNumber"], 8)
#             expiry_code = fixed_length(record["ExpiryCode"], 6)
#             last_done_date = record["LastDoneDate"] or ""
#             expiry_date = record["ExpiryDate"] or ""

#             # Write formatted row to file
#             file.write(f"{staff_number},{expiry_code},{last_done_date},{expiry_date}\n")

#     logger.info(f"JOB8.txt file created at: {file_path}")
#     return file_path

def generate_job8_file(records):
    """Generate the JOB8.txt file without adding padding to fields."""
    file_path = os.path.join(settings.MEDIA_ROOT, "JOB8.txt")
    with open(file_path, "w", newline="") as file:
        for record in records:
            # Extract fields without padding
            staff_number = str(record["StaffNumber"])  # Write as-is
            expiry_code = str(record["ExpiryCode"])  # Write as-is
            last_done_date = record["LastDoneDate"] or ""  # Leave blank if missing
            expiry_date = record["ExpiryDate"] or ""  # Leave blank if missing

            # Write the row to the file
            file.write(f"{staff_number},{expiry_code},{last_done_date},{expiry_date}\n")

    logger.info(f"JOB8.txt file created at: {file_path}")
    return file_path


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



def email_job8_file(file_path, data):
    """Email the JOB8.txt file to flight dispatch."""
    subject = "CPAT Completion Records (JOB8.txt)"
    body = (
        "Dear Team,\n\n"
        "Attached is the JOB8.txt file containing CPAT completion records.\n\n"
        "Best regards,\nFlightOps Team"
    )
    recipient_list = [
        settings.FIRST_CPAT_EMAIL_RECEIVER,
        settings.SECOND_CPAT_EMAIL_RECEIVER,
    ]

    summary_file_path = None
    if data:
        summary_file_path = os.path.join(settings.MEDIA_ROOT, "CPAT_Summary.csv")
        with open(summary_file_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    attachments = [file_path]
    if summary_file_path:
        attachments.append(summary_file_path)

    send_mail(
        subject=subject,
        message=body,
        from_email=settings.EMAIL_HOST_USER,
        recipient_list=recipient_list,
        fail_silently=False,
    )
    logger.info("JOB8.txt emailed to flight dispatch.")





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




#Create a CSV file with flight and crew data
# import csv
# from django.conf import settings

# def generate_csv_for_fdm(flight_data, crew_data):
#     file_name = f"aims_{now().strftime('%Y%m%d%H%M')}.csv"
#     file_path = f"{settings.MEDIA_ROOT}/{file_name}"  # Save in media folder

#     # Open the file for writing
#     with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.writer(csvfile)
#         # Write header
#         writer.writerow([
#             "DAY", "FLT", "FLTYPE", "REG", "DEP", "ARR", "STD", "STA",
#             "TKOF", "TDOWN", "BLOF", "BLON", "ETD", "ETA", "CREW_ID", "NAME", "ROLE"
#         ])

#         # Write flight data
#         for flight in flight_data:
#             flight_row = [
#                 flight.sd_date_utc,  # DAY
#                 flight.flight_no,  # FLT
#                 flight.flight_type,  # FLTYPE
#                 flight.tail_no,  # REG
#                 flight.dep_code_icao,  # DEP
#                 flight.arr_code_icao,  # ARR
#                 flight.std_utc,  # STD
#                 flight.sta_utc,  # STA
#                 flight.takeoff_utc,  # TKOF
#                 flight.touchdown_utc,  # TDOWN
#                 flight.atd_utc,  # BLOF
#                 flight.ata_utc,  # BLON
#                 flight.etd_utc,  # ETD
#                 flight.eta_utc,  # ETA
#                 None,  # Placeholder for crew
#                 None,  # Placeholder for name
#                 None,  # Placeholder for role
#             ]
#             writer.writerow(flight_row)

#             # Add crew details
#             flight_crews = crew_data.filter(
#                 flight_no=flight.flight_no,
#                 sd_date_utc=flight.sd_date_utc
#             )
#             for crew in flight_crews:
#                 crew_row = [
#                     None, None, None, None, None, None, None, None,
#                     None, None, None, None, None, None,  # Placeholder for flight columns
#                     crew.crew_id,  # CREW_ID
#                     crew.name,  # NAME
#                     crew.get_role_display(),  # ROLE
#                 ]
#                 writer.writerow(crew_row)
#     return file_path


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
        "TKOF", "TDOWN", "BLOF", "BLON", "ETD", "ETA"
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
            ]
            writer.writerow(flight_row)

    logger.info(f"CSV file generated at: {file_path}")
    return file_path





# # Upload the CSV File to the AIMS Server

# import os
# import paramiko
# import logging
# from django.conf import settings
# from paramiko.ssh_exception import SSHException, NoValidConnectionsError

# # Initialize logger
# logger = logging.getLogger(__name__)

# @shared_task
# def hourly_upload_csv_to_fdm():
#     from django.utils.timezone import now, timedelta

#     # Calculate the time range
#     one_hour_ago = now() - timedelta(hours=1)

#     # Fetch data
#     flight_data = FdmFlightData.objects.filter(updated_at__gte=one_hour_ago)
#     crew_data = CrewMember.objects.filter(
#         flight_no__in=flight_data.values_list('flight_no', flat=True),
#         sd_date_utc__in=flight_data.values_list('sd_date_utc', flat=True)
#     )

#     # Generate CSV
#     local_file_path = generate_csv_for_aims(flight_data, crew_data)

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

#         # Log success
#         logger.info(f"Upload completed successfully: {local_file_path} -> {remote_path}")

#         # Close connections
#         sftp.close()
#         transport.close()

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

# Initialize logger
logger = logging.getLogger(__name__)

@shared_task
def hourly_upload_csv_to_fdm():
    """
    Celery task to generate, upload CSV to FDM server, and clean up old files.
    """
    # Calculate the time range
    one_hour_ago = now() - timedelta(hours=1)

    # Fetch flight data
    flight_data = FdmFlightData.objects.filter(updated_at__gte=one_hour_ago)

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

        # Clean up local files after successful upload
        local_dir = os.path.dirname(local_file_path)
        for file in os.listdir(local_dir):
            if file.startswith("aims_") and file.endswith(".csv"):
                file_path = os.path.join(local_dir, file)
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted local file: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete file {file_path}: {e}")

    except (SSHException, NoValidConnectionsError) as e:
        logger.error(f"SFTP upload failed: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during file upload: {e}", exc_info=True)
