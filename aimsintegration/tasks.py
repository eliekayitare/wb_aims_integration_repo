
# from exchangelib import Credentials, Account, Configuration, EWSDateTime, EWSTimeZone
# from .utils import process_email_attachment, process_airport_file, process_flight_schedule_file, process_acars_message
# import logging
# from django.conf import settings
# from datetime import datetime, timedelta

# logger = logging.getLogger(__name__)

# def fetch_emails(process_airport=False, process_schedule=False, process_acars=False, email_limit=None, acars_date=None, acars_start_date=None, acars_end_date=None):
#     """
#     Fetch and process emails for airport data, flight schedules, and ACARS messages.
#     """
#     try:
#         credentials = Credentials(
#             username=settings.EXCHANGE_EMAIL_USER,
#             password=settings.EXCHANGE_EMAIL_PASSWORD
#         )
#         config = Configuration(
#             server=settings.EXCHANGE_EMAIL_SERVER,
#             credentials=credentials
#         )
#         account = Account(
#             primary_smtp_address=settings.EXCHANGE_EMAIL_USER,
#             credentials=credentials,
#             autodiscover=False,
#             config=config,
#             access_type='delegate'
#         )

#         # Define the account's timezone
#         timezone = account.default_timezone

#         logger.info("Starting email fetch...")

#         # Process airport details if requested
#         if process_airport:
#             logger.info("Fetching and processing airport data emails...")
#             emails = account.inbox.filter(subject__contains='AIMS JOB : #1003 Airport details Feed to WB server file attached').order_by('-datetime_received')
            
#             for item in emails[:email_limit] if email_limit else emails:
#                 logger.info(f"Processing airport data email with subject: {item.subject}")
#                 process_email_attachment(item, process_airport_file)

#         # Process flight schedules if requested
#         if process_schedule:
#             logger.info("Fetching and processing flight schedule emails...")
#             emails = account.inbox.filter(subject__contains='AIMS JOB : #1002 Flight schedule feed to WB server file attached').order_by('datetime_received')

#             for item in emails[:email_limit] if email_limit else emails:
#                 logger.info(f"Processing flight schedule email with subject: {item.subject}")
#                 process_email_attachment(item, process_flight_schedule_file)

#         # Process ACARS messages if requested
#         if process_acars:
#             logger.info("Fetching and processing ACARS messages...")

#             # Define a timezone-aware datetime range for filtering
#             if acars_date:
#                 start_datetime = EWSDateTime.from_datetime(datetime(acars_date.year, acars_date.month, acars_date.day, 0, 0, tzinfo=timezone))
#                 end_datetime = EWSDateTime.from_datetime(datetime(acars_date.year, acars_date.month, acars_date.day, 23, 59, 59, tzinfo=timezone))
#             elif acars_start_date and acars_end_date:
#                 start_datetime = EWSDateTime.from_datetime(datetime(acars_start_date.year, acars_start_date.month, acars_start_date.day, 0, 0, tzinfo=timezone))
#                 end_datetime = EWSDateTime.from_datetime(datetime(acars_end_date.year, acars_end_date.month, acars_end_date.day, 23, 59, 59, tzinfo=timezone))
#             else:
#                 start_datetime = end_datetime = None

#             # Filter emails based on the specified date or date range
#             if start_datetime and end_datetime:
#                 emails = account.inbox.filter(
#                     subject__icontains='ARR', 
#                     datetime_received__range=(start_datetime, end_datetime)
#                 ).order_by('datetime_received')
#             else:
#                 emails = account.inbox.filter(subject__icontains='ARR').order_by('datetime_received')

#             # Apply limit if specified
#             emails = emails[:email_limit] if email_limit else emails

#             processed_count = 0
#             for item in emails:
#                 if "M16" in item.body:
#                     logger.info(f"Skipping 'M16' ACARS message: {item.subject}")
#                     continue

#                 logger.info(f"Processing ACARS email with subject: {item.subject}")
#                 process_acars_message(item)
#                 processed_count += 1

#             logger.info(f"Finished processing {processed_count} ACARS emails.")
    
#     except Exception as e:
#         logger.error(f"Error fetching emails: {e}")


from celery import shared_task, chain
from exchangelib import Credentials, Account, Configuration, EWSDateTime
from .utils import process_email_attachment, process_airport_file, process_flight_schedule_file, process_acars_message
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

# @shared_task
# def fetch_airport_data():
#     account = get_exchange_account()
#     logger.info("Fetching the most recent airport data email...")

#     # Use list indexing to get the most recent email if it exists
#     emails = account.inbox.filter(
#         subject__contains='AIMS JOB : #1003 Airport details Feed to WB server file attached'
#     ).order_by('-datetime_received')
    
#     # Access the first email with indexing if any emails are found
#     email = emails[0] if emails else None

#     if email:
#         logger.info(f"Processing airport data email with subject: {email.subject}")
#         process_email_attachment(email, process_airport_file)
#     else:
#         logger.info("No airport data email found.")



# @shared_task
# def fetch_flight_schedules():
#     account = get_exchange_account()
#     logger.info("Fetching the most recent flight schedule email...")

#     # Fetch only the most recent email matching the subject filter
#     emails = account.inbox.filter(
#         subject__contains='AIMS JOB : #1002 Flight schedule feed to WB server file attached'
#     ).order_by('-datetime_received')  # Get emails ordered by the most recent

#     # Access the first email using list indexing
#     email = emails[0] if emails else None

#     if email:
#         logger.info(f"Processing the most recent flight schedule email with subject: {email.subject}")
#         process_email_attachment(email, process_flight_schedule_file)
#     else:
#         logger.info("No new flight schedule email found.")


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
def fetch_acars_messages():
    account = get_exchange_account()
    logger.info("Fetching and processing ACARS messages...")

    # Fetch a batch of the oldest unread messages
    emails = account.inbox.filter(subject__icontains='ARR', is_read=False).order_by('datetime_received')[:5]
    
    # Process each email in the batch
    for item in emails:
        if "M16" in item.body:
            logger.info(f"Skipping 'M16' ACARS message: {item.subject}")
            item.is_read = True
            item.save(update_fields=['is_read'])
            continue

        # Log and process the ACARS message
        logger.info(f"Processing ACARS email with subject: {item.subject}")
        process_acars_message(item)

        # Mark as read to avoid reprocessing
        item.is_read = True
        item.save(update_fields=['is_read'])

    # Check if more unread messages are available and re-trigger task if needed
    if account.inbox.filter(subject__icontains='ARR', is_read=False).exists():
        fetch_acars_messages.apply_async(countdown=1)  # Re-run task with a short delay

    logger.info("Batch processing of ACARS emails completed.")


