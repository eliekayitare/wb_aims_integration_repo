
from celery import shared_task, chain
from exchangelib import Credentials, Account, Configuration, EWSDateTime
from .utils import process_email_attachment, process_airport_file, process_flight_schedule_file, process_acars_message
import logging
from django.conf import settings
from datetime import datetime
from django.core.mail import send_mail
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

    message_body = "QU ANPOCWB .DDLXCXA 260720 ARRFI WB0/AN 9XR-WP/DA HRYR/AD LFPG/IN 0719/FB    7/LA  /LR DT DDL XXF 260720 M40A"
    flight_no = 0
    send_mail(
        subject=f"No matching flights found  for flight number: {flight_no}",
        message=f"Dear All,\n\n The Acars message for flight number: {flight_no} is incorrectly formatted.\n\n Manually update it with the following acars message:\n\n{message_body} \n\n Regards,\n FlightOps Team",
        from_email=settings.EMAIL_HOST_USER,
        recipient_list=[settings.EMAIL_RECEIVER] if isinstance(settings.EMAIL_RECEIVER, str) else settings.EMAIL_RECEIVER,
        fail_silently=False,
        )

    logger.info("Batch processing of ACARS emails completed.")


