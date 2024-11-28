
#Tasks for ACARS project

from celery import shared_task, chain
from exchangelib import Credentials, Account, Configuration, EWSDateTime
from .utils import process_email_attachment, process_airport_file, process_flight_schedule_file, process_acars_message, process_cargo_email_attachment, process_cargo_flight_schedule_file
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
        subject__contains='AIMS JOB : #1002 Flight schedule feed to WB server file attached'
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
#     emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')[:5]
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

@shared_task
def fetch_acars_messages():
    account = get_exchange_account()
    logger.info("Fetching and processing ACARS messages...")

    # Fetch the oldest unread email safely
    emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')
    email = next(iter(emails), None)  # Use `next` to get the first email or `None` if empty

    while email:
        # Check if the email contains "M16" in the body
        if "M16" in email.body:
            logger.info(f"Skipping 'M16' ACARS message: {email.subject}")
            email.is_read = True
            email.save(update_fields=['is_read'])
            logger.info("Marked 'M16' message as read. Proceeding to the next message.")
        else:
            # Log and process the ACARS message
            logger.info(f"Processing ACARS email with subject: {email.subject}")
            process_acars_message(email)

            # Mark as read to avoid reprocessing
            email.is_read = True
            email.save(update_fields=['is_read'])

            logger.info("Processed an ACARS message. Exiting after handling one message.")
            break  # Exit after processing one non-"M16" message

        # Fetch the next oldest unread email
        emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')
        email = next(iter(emails), None)  # Get the next email or `None` if no more emails

    if not email:
        logger.info("No more unread ACARS messages or only 'M16' messages left. Task will run again at the next schedule.")



#Tasks for CPAT project

import requests
from decouple import config
from .models import CompletionRecord

# Fetch secrets from .env
BASE_URL = config('LMS_BASE_URL')
LMS_KEY = config('LMS_KEY')
API_TOKEN = config('API_TOKEN')
DAYS = config('DAYS', cast=int, default=1)

@shared_task
def fetch_and_store_completion_records():
    url = f"{BASE_URL}/lms/api/IntegrationAPI/comp/v2/{LMS_KEY}/{DAYS}"
    headers = {
        "apitoken": API_TOKEN,
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        for record in data:
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
        print("Completion records successfully updated.")
        logger.info("Completion records successfully updated.")
    else:
        print(f"Failed to fetch data: {response.status_code} - {response.text}")
        logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")

