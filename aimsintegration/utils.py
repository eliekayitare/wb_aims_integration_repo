import logging
from exchangelib import FileAttachment
from aimsintegration.models import AirportData, FlightData, AcarsMessage, CargoFlightData,FdmFlightData
from datetime import datetime
import re
from django.core.mail import send_mail
logger = logging.getLogger(__name__)

def process_airport_file(attachment):
    """
    Process the airport data file and store records in the AirportData table.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()

        logger.info(f"Processing {len(content)} lines of airport data.")

        for line in content:
            fields = line.split()
            logger.info(f"Processing line: {line}")

            if len(fields) >= 3:
                icao_code = fields[0]
                iata_code = fields[1]
                airport_name = " ".join(fields[2:])
                
                # Log the data extracted from the line
                logger.info(f"Extracted data - ICAO: {icao_code}, IATA: {iata_code}, Airport Name: {airport_name}")

                # Insert or update the airport data
                airport, created = AirportData.objects.get_or_create(
                    iata_code=iata_code,
                    defaults={
                        'icao_code': icao_code,
                        'airport_name': airport_name,
                        'raw_content': line  # Store raw line for reference
                    }
                )

                if created:
                    logger.info(f"Inserted new airport record for {iata_code} - {airport_name}")
                else:
                    airport.icao_code = icao_code
                    airport.airport_name = airport_name
                    airport.save()
                    logger.info(f"Updated existing airport record for {iata_code}")

        logger.info("Airport data processed successfully.")
    except Exception as e:
        logger.error(f"Error processing airport data file: {e}")




from datetime import datetime
from aimsintegration.models import FlightData, AirportData
import logging

logger = logging.getLogger(__name__)


def process_flight_schedule_file(attachment):
    """
    Process the entire flight schedule file and update the FlightData table.
    Avoid updating records with ACARS data in `atd_utc`, `takeoff_utc`, `touchdown_utc`, or `ata_utc`.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the flight schedule file...")

        for line_num, line in enumerate(content, start=1):
            fields = line.split()
            
            # Skip line if insufficient fields
            if len(fields) < 8:
                logger.error(f"Skipping line {line_num} due to insufficient fields: {fields}")
                print("----------------------------------------------")
                print(len(fields))
                print("----------------------------------------------")
                continue

            try:
                # Extract fields
                flight_date = fields[0]
                tail_no = fields[1]
                flight_no = fields[2]
                dep_code_icao = fields[3]
                arr_code_icao = fields[4]
                std = fields[5]
                sta = fields[6]
                arrival_date = fields[-1]

                # Parse dates
                sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date()
                sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date()

                # Parse times
                try:
                    std_utc = datetime.strptime(std, "%H:%M").time()
                    sta_utc = datetime.strptime(sta, "%H:%M").time()
                except ValueError:
                    logger.error(f"Skipping line {line_num} due to time format error in STD or STA: {std}, {sta}")
                    continue
                
                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                if not dep_airport or not arr_airport:
                    logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
                    continue

                dep_code_iata = dep_airport.iata_code
                arr_code_iata = arr_airport.iata_code

                # Define unique criteria
                unique_criteria = {
                    'flight_no': flight_no,
                    'tail_no': tail_no,
                    'sd_date_utc': sd_date_utc,
                    'dep_code_icao': dep_code_icao,
                    'arr_code_icao': arr_code_icao,
                    'sa_date_utc': sa_date_utc,
                    'std_utc': std_utc,
                    'sta_utc': sta_utc,
                }

                # Check if any record exists with the same unique criteria
                existing_record = FlightData.objects.filter(**unique_criteria).first()

                if existing_record:
                    logger.info(f"Record for flight {flight_no} on {sd_date_utc} Exists,no update needed")
                    continue  # Skip insertion if a record exists, regardless of ACARS data

                # Create a new record if no matching record exists
                FlightData.objects.create(
                    flight_no=flight_no,
                    tail_no=tail_no,
                    dep_code_iata=dep_code_iata,
                    dep_code_icao=dep_code_icao,
                    arr_code_iata=arr_code_iata,
                    arr_code_icao=arr_code_icao,
                    sd_date_utc=sd_date_utc,
                    std_utc=std_utc,
                    sta_utc=sta_utc,
                    sa_date_utc=sa_date_utc,
                    raw_content=line
                )
                logger.info(f"Created new flight record: {flight_no} on {sd_date_utc}")

            except ValueError as ve:
                logger.error(f"Error processing flight record on line {line_num}: {ve} - {line}")
                continue

        logger.info("Flight schedule file processed successfully.")

    except Exception as e:
        logger.error(f"Error processing flight schedule file: {e}", exc_info=True)



#CARGO Website

def process_cargo_flight_schedule_file(attachment):
    """
    Process the entire flight schedule file and update the FlightData table.
    Avoid updating records with ACARS data in `atd_utc`, `takeoff_utc`, `touchdown_utc`, or `ata_utc`.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the flight schedule file...")

        for line_num, line in enumerate(content, start=1):
            fields = line.split()
            
            # Skip line if insufficient fields
            if len(fields) < 8:
                logger.error(f"Skipping line {line_num} due to insufficient fields: {fields}")
                print("----------------------------------------------")
                print(len(fields))
                print("----------------------------------------------")
                continue

            try:
                # Extract fields
                flight_date = fields[0]
                tail_no = fields[1]
                flight_no = fields[2]
                dep_code_icao = fields[3]
                arr_code_icao = fields[4]
                std = fields[5]
                sta = fields[6]
                arrival_date = fields[-1]

                # Parse dates
                sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date()
                sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date()

                # Parse times
                try:
                    std_utc = datetime.strptime(std, "%H:%M").time()
                    sta_utc = datetime.strptime(sta, "%H:%M").time()
                except ValueError:
                    logger.error(f"Skipping line {line_num} due to time format error in STD or STA: {std}, {sta}")
                    continue
                
                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                if not dep_airport or not arr_airport:
                    logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
                    continue

                dep_code_iata = dep_airport.iata_code
                arr_code_iata = arr_airport.iata_code

                # Define unique criteria
                unique_criteria = {
                    'flight_no': flight_no,
                    'tail_no': tail_no,
                    'sd_date_utc': sd_date_utc,
                    'dep_code_icao': dep_code_icao,
                    'arr_code_icao': arr_code_icao,
                    'sa_date_utc': sa_date_utc,
                    'std_utc': std_utc,
                    'sta_utc': sta_utc,
                }

                # Check if any record exists with the same unique criteria
                existing_record = CargoFlightData.objects.filter(**unique_criteria).first()

                if existing_record:
                    logger.info(f"Record for flight {flight_no} on {sd_date_utc} Exists,no update needed")
                    continue  # Skip insertion if a record exists, regardless of ACARS data

                # Create a new record if no matching record exists
                CargoFlightData.objects.create(
                    flight_no=flight_no,
                    tail_no=tail_no,
                    dep_code_iata=dep_code_iata,
                    dep_code_icao=dep_code_icao,
                    arr_code_iata=arr_code_iata,
                    arr_code_icao=arr_code_icao,
                    sd_date_utc=sd_date_utc,
                    std_utc=std_utc,
                    sta_utc=sta_utc,
                    sa_date_utc=sa_date_utc,
                    raw_content=line
                )
                logger.info(f"Created new Cargo flight record: {flight_no} on {sd_date_utc}")

            except ValueError as ve:
                logger.error(f"Error processing cargo flight record on line {line_num}: {ve} - {line}")
                continue

        logger.info("Cargo Flight schedule file processed successfully.")

    except Exception as e:
        logger.error(f"Error processing cargo flight schedule file: {e}", exc_info=True)



from datetime import datetime
import os
from aimsintegration.models import FlightData
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

# Tail number to aircraft type mapping for ACARS enabled aircraft
TAIL_TO_AIRCRAFT_TYPE = {
    "9XR-WJ": "73G",
    "9XR-WK": "73G",
    "9XR-WF": "738",
    "9XR-WG": "738",
    "9XR-WQ": "738",
    "9XR-WR": "738",
    "9XR-WW": "738",
    "9XR-WY": "738",
    "9XR-WN": "332",
    "9XR-WX": "332",
    "9XR-WP": "333",
    "9XR-WH": "CRJ",
    "9XR-WI": "CRJ",
    "9XR-WL": "DH8",
    "9XR-WM": "DH8",
    "9XR-WT": "DH8",
}

def get_aircraft_type(tail_number):
    return TAIL_TO_AIRCRAFT_TYPE.get(tail_number, "   ")  # Default to 3 spaces if not found

# Function to format a single row based on the event type and highlighted fields
def format_acars_data_to_job_one(flight_data, acars_event, event_time, email_arrival_time):
    # Set up the initial base fields with appropriate padding and spacing
    carrier_code = " WB"
    flight_no = f"{flight_data.flight_no:>4}"  # Right-aligned with padding
    leg_code = " "  # Single letter leg code (empty space)
    service_type = "  "  # Two spaces for service type
    dep_code_icao = flight_data.dep_code_icao if flight_data.dep_code_icao else "   "  # 3-letter departure airport code
    arr_code_icao = flight_data.arr_code_icao if flight_data.arr_code_icao else "   "  # 3-letter arrival airport code
    aircraft_type = get_aircraft_type(flight_data.tail_no)  # 3-letter aircraft type
    tail_number = f"{flight_data.tail_no:<12}"  # Left-aligned, filled to 12 characters with spaces
    scheduled_departure_day = flight_data.sd_date_utc.strftime("%Y%m%d") if flight_data.sd_date_utc else " " * 8
    scheduled_departure_time = flight_data.std_utc.strftime("%H%M") if flight_data.std_utc else "    "
    scheduled_arrival_day = flight_data.sa_date_utc.strftime("%Y%m%d") if flight_data.sa_date_utc else "        "
    scheduled_arrival_time = flight_data.sta_utc.strftime("%H%M") if flight_data.sta_utc else "    "
    email_arrival_day = email_arrival_time.strftime("%Y%m%d")
    # Fields for estimated and actual times, initially empty (filled with spaces)
    estimated_departure_day = " " * 8
    estimated_departure_time = " " * 4
    estimated_arrival_day = " " * 8
    estimated_arrival_time = " " * 4
    block_off_day = " " * 8
    block_off_time = " " * 4
    block_on_day = " " * 8
    block_on_time = " " * 4
    airborne_day = " " * 8
    airborne_time = " " * 4
    touch_down_day = " " * 8
    touch_down_time = " " * 4

    # Populate actual values based on the ACARS event type
    if acars_event == "OT":
        block_off_day = email_arrival_day
        block_off_time = event_time.strftime("%H%M")
    elif acars_event == "IN":
        block_on_day = email_arrival_day
        block_on_time = event_time.strftime("%H%M")
    elif acars_event == "OF":
        airborne_day = email_arrival_day
        airborne_time = event_time.strftime("%H%M")
    elif acars_event == "ON":
        touch_down_day = email_arrival_day
        touch_down_time = event_time.strftime("%H%M")

    # Define U operation code placement and final padding
    operation_code = "U"
    free_text_remarks = " " * 24  # 24 spaces for remarks
    crew_id_takeoff = " " * 8
    crew_id_landing = " " * 8
    rule_set = " "  # One space for rule set
    log_page_number = " " * 7  # 7 spaces for log page number

    # Construct the row according to the required field lengths
    row = (
        f"{carrier_code}"
        f"{flight_no}"
        f"{leg_code}"
        f"{service_type}"
        f"{dep_code_icao:<3}"
        f"{aircraft_type}"
        f"{tail_number:<12}"
        f"{arr_code_icao:<3}"
        f"{scheduled_departure_day}"
        f"{scheduled_departure_time}"
        f"{scheduled_arrival_day}"
        f"{scheduled_arrival_time}"
        f"{estimated_departure_day}"
        f"{estimated_departure_time}"
        f"{estimated_arrival_day}"
        f"{estimated_arrival_time}"
        f"{block_off_day}"
        f"{block_off_time}"
        f"{block_on_day}"
        f"{block_on_time}"
        f"{airborne_day}"
        f"{airborne_time}"
        f"{touch_down_day}"
        f"{touch_down_time}"
        f"  "  # Flight status code (2 spaces, optional)
        f"{free_text_remarks}"  # 24 spaces for general remarks
        f"{operation_code}"  # Operation code U
        f"{crew_id_takeoff}"
        f"{crew_id_landing}"
        f"{rule_set}"
        f"{log_page_number}"
    )

    # Ensure the row is exactly 172 characters long
    row = row.ljust(172)
    return row



def write_job_one_row(file_path, flight_data, acars_event, event_time, email_arrival_time):
    try:
        # Format the row
        row = format_acars_data_to_job_one(flight_data, acars_event, event_time, email_arrival_time)
        
        # Append the row to the file
        with open(file_path, 'a') as file:  # Use 'a' to append
            file.write(row + '\n')

        logger.info(f"Successfully wrote row for flight {flight_data.flight_no} to job file.")
    
    except Exception as e:
        logger.error(f"Error writing to job file: {e}", exc_info=True)






import re
import logging

# Initialize logger
logger = logging.getLogger(__name__)

def extract_departure_and_arrival_codes(message_body):
    # Pattern to match the departure code after 'DA'
    dep_code_pattern = r'DA\s+([A-Z]{4})'
    
    # Pattern to capture the arrival code after 'DS' or 'AD'
    arr_code_pattern = r'(?:DS|AD)\s+([A-Z]{4})'

    # Match departure code
    dep_code_match = re.search(dep_code_pattern, message_body)
    # Match arrival code
    arr_code_match = re.search(arr_code_pattern, message_body)

    # Extract codes if matched
    dep_code_iata = dep_code_match.group(1) if dep_code_match else None
    arr_code_iata = arr_code_match.group(1) if arr_code_match else None

    # Log warnings for missing codes
    if not dep_code_iata:
        logger.warning("Departure code not found in message.")
    if not arr_code_iata:
        logger.warning("Arrival code not found in message.")

    return dep_code_iata, arr_code_iata




from datetime import datetime, timedelta
import os
import logging
import re
from django.conf import settings
from aimsintegration.models import FlightData

logger = logging.getLogger(__name__)

# def process_acars_message(item):
#     try:
#         email_received_date = item.datetime_received.date()  # Get only the date part
#         message_body = item.body

#         # Skip processing if "M16" appears in the message
#         if "M16" in message_body:
#             logger.info("Skipping 'M16' ACARS message.")
#             return

#         logger.info(f"ACARS message received at: {email_received_date} UTC")
#         logger.info(f"ACARS message body: {message_body}")

#         # Extract fields from ACARS message
#         flight_no = extract_flight_number(message_body)
#         acars_event, event_time_str = extract_acars_event(message_body)
#         dep_code, arr_code = extract_departure_and_arrival_codes(message_body)
#         tail_number = extract_tail_number(message_body)

#         # Validate extracted time format
#         if not re.match(r'^\d{2}:\d{2}$', event_time_str):
#             logger.error("Invalid time format in ACARS message.")
#             return

#         # Log extracted fields
#         logger.info(f"Extracted Flight Number: {flight_no}")
#         logger.info(f"Extracted Tail Number: {tail_number}")
#         logger.info(f"Extracted ACARS Event: {acars_event}")
#         logger.info(f"Extracted Event Time: {event_time_str}")
#         logger.info(f"Extracted Departure Code (IATA): {dep_code}")
#         logger.info(f"Extracted Arrival Code (IATA): {arr_code}")

#         # Ensure all required fields were extracted
#         if not (flight_no and acars_event and event_time_str and dep_code and arr_code):
#             logger.error("Unable to extract complete flight details from ACARS message.")
#             return

#         # Convert event time to a time object
#         event_time = datetime.strptime(event_time_str, "%H:%M").time()

#         # Fetch all flights with the specified flight number, origin, and destination
#         flights = FlightData.objects.filter(
#             flight_no=flight_no,
#             tail_no=tail_number,
#             dep_code_iata=dep_code,
#             arr_code_iata=arr_code
#         )

#         if not flights.exists():
#             logger.info(f"No matching flights found in database for flight number: {flight_no}")
#             # Send an email notification to the email receiver if no matching flights are found
#             send_mail(
#             subject=f"No matching flights found for flight number: {flight_no}",
#             message=f"Dear All,\n\n The Acars message for flight number: {flight_no} is incorrectly formatted.\n\n Manually update it with the following acars message:\n\n{message_body} \n\n Regards,\n FlightOps Team",
#             from_email=settings.EMAIL_HOST_USER,
#             recipient_list=[
#                 settings.FIRST_EMAIL_RECEIVER
#                 if isinstance(settings.FIRST_EMAIL_RECEIVER, str) else settings.FIRST_EMAIL_RECEIVER,
#                 settings.SECOND_EMAIL_RECEIVER
#                 if isinstance(settings.SECOND_EMAIL_RECEIVER, str) else settings.SECOND_EMAIL_RECEIVER
#             ],
#             fail_silently=False,
#              )


            
#             return
        
#         # Find the closest `sd_date_utc` to `email_received_date`
#         closest_flight = min(
#             flights,
#             key=lambda flight: abs((flight.sd_date_utc - email_received_date).days)
#         )

#         # Update the closest flight based on the ACARS event type
#         if acars_event == "OT":
#             closest_flight.atd_utc = event_time
#         elif acars_event == "OF":
#             closest_flight.takeoff_utc = event_time
#         elif acars_event == "ON":
#             closest_flight.touchdown_utc = event_time
#         elif acars_event == "IN":
#             closest_flight.ata_utc = event_time

#         # Save the updated flight record
#         closest_flight.save()
#         logger.info(f"Flight {flight_no} updated with event {acars_event} at {event_time}")

#         # Write updated data to the job file
#         file_path = os.path.join(settings.MEDIA_ROOT, 'JOB1.txt')
#         write_job_one_row(file_path, closest_flight, acars_event, event_time, email_received_date)
#         upload_to_aims_server(file_path)

#     except Exception as e:
#         logger.error(f"Error processing ACARS message: {e}", exc_info=True)

def process_acars_message(item, file_path):
    try:
        email_received_date = item.datetime_received.date()  # Get only the date part
        message_body = item.body

        if "M16" in message_body:
            logger.info("Skipping 'M16' ACARS message.")
            return

        logger.info(f"ACARS message received at: {email_received_date} UTC")
        logger.info(f"ACARS message body: {message_body}")

        # Extract fields from the message
        flight_no = extract_flight_number(message_body)
        acars_event, event_time_str = extract_acars_event(message_body)
        dep_code, arr_code = extract_departure_and_arrival_codes(message_body)
        tail_number = extract_tail_number(message_body)

        if not re.match(r'^\d{2}:\d{2}$', event_time_str):
            logger.error("Invalid time format in ACARS message.")
            return

        event_time = datetime.strptime(event_time_str, "%H:%M").time()

        flights = FlightData.objects.filter(
            flight_no=flight_no,
            tail_no=tail_number,
            dep_code_iata=dep_code,
            arr_code_iata=arr_code
        )

        #FDM Project
        fdm_flights = FdmFlightData.objects.filter(
            flight_no=flight_no,
            tail_no=tail_number,
            dep_code_iata=dep_code,
            arr_code_iata=arr_code
        )
        

        if not flights.exists():
            logger.info(f"No matching flights found for flight number: {flight_no}")
            send_mail(
                subject=f"No matching flights found for flight number: {flight_no}",
                message=(
                    f"Dear Team,\n\n"
                    f"The ACARS message for flight {flight_no} could not be matched.\n"
                    f"Message details:\n\n{message_body}\n\n"
                    f"Please review and update manually.\n\n"
                    f"Regards,\nFlightOps Team"
                ),
                from_email=settings.EMAIL_HOST_USER,
                recipient_list=[
                    settings.FIRST_EMAIL_RECEIVER,
                    settings.SECOND_EMAIL_RECEIVER,
                ],
                fail_silently=False,
            )
            return
        
        if not fdm_flights.exists():
            logger.info(f"No matching FDM flights found for flight number: {flight_no}")
            send_mail(
                subject=f"No matching flights found for flight number: {flight_no}",
                message=(
                    f"Dear Team,\n\n"
                    f"The ACARS message for flight {flight_no} could not be matched.\n"
                    f"Message details:\n\n{message_body}\n\n"
                    f"Please review and update manually.\n\n"
                    f"Regards,\nFlightOps Team"
                ),
                from_email=settings.EMAIL_HOST_USER,
                recipient_list=[
                    settings.FIRST_EMAIL_RECEIVER,
                    settings.SECOND_EMAIL_RECEIVER,
                ],
                fail_silently=False,
            )
            return

        closest_flight = min(
            flights,
            key=lambda flight: abs((flight.sd_date_utc - email_received_date).days)
        )

        closest_fdm_flight = min(
            fdm_flights,
            key=lambda fl: abs((fl.sd_date_utc - email_received_date).days)
        ) 

        if acars_event == "OT":
            closest_flight.atd_utc = event_time
            closest_fdm_flight.atd_utc = event_time
        elif acars_event == "OF":
            closest_flight.takeoff_utc = event_time
            closest_fdm_flight.takeoff_utc = event_time
        elif acars_event == "ON":
            closest_flight.touchdown_utc = event_time
            closest_fdm_flight.touchdown_utc = event_time
        elif acars_event == "IN":
            closest_flight.ata_utc = event_time
            closest_fdm_flight.ata_utc = event_time

        closest_flight.save()
        closest_fdm_flight.save()

        # Append the updated flight details to the job file
        write_job_one_row(file_path, closest_flight, acars_event, event_time, email_received_date)

    except Exception as e:
        logger.error(f"Error processing ACARS message: {e}", exc_info=True)



def extract_flight_number(message_body):
    """
    Extracts the numeric flight number (e.g., 304) from the message body.
    """
    match = re.search(r'FI\sWB(\d+)', message_body)
    return match.group(1) if match else None


import re

def extract_tail_number(message_body):
    """
    Extracts the tail number (e.g., 9XR-XX) from the message body.
    Assumes the tail number is always in the format '9XR-XX' where X is any alphanumeric character.
    """
    match = re.search(r'\b9XR-\w{2}\b', message_body)  # Match '9XR-' followed by exactly two alphanumeric characters
    return match.group(0) if match else None





def extract_acars_event(message_body):
    """
    Extracts the ACARS event type (OT, OF, ON, IN) and corresponding time from the message.
    """
    match = re.search(r'(OT|OF|ON|IN)\s(\d{4})', message_body)
    if match:
        event_type = match.group(1)  # OT, OF, ON, IN
        event_time = match.group(2)  # Time in HHMM format
        # Convert event_time (HHMM) to a string in HH:MM format
        event_time_formatted = datetime.strptime(event_time, '%H%M').strftime('%H:%M')
        return event_type, event_time_formatted
    return None, None



def process_email_attachment(item, process_function):
    """
    Generalized function to handle processing of email attachments.
    `process_function` is the specific function that processes the attachment (e.g., process_airport_file).
    """
    try:
        if item.attachments:
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    logger.info(f"Processing attachment: {attachment.name}")
                    process_function(attachment)  # Call the relevant function for each attachment
    except Exception as e:
        logger.error(f"Error processing email attachment: {e}")




def process_cargo_email_attachment(item, process_function):
    """
    Generalized function to handle processing of email attachments.
    `process_function` is the specific function that processes the attachment (e.g., process_airport_file).
    """
    try:
        if item.attachments:
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    logger.info(f"Processing attachment: {attachment.name}")
                    process_function(attachment)  # Call the relevant function for each attachment
    except Exception as e:
        logger.error(f"Error processing cargo email attachment: {e}")






#FMD Project

def process_fdm_flight_schedule_file(attachment):
    """
    Process the entire fdm flight schedule file and update the FlightData table.
    Avoid updating records with ACARS data in `atd_utc`, `takeoff_utc`, `touchdown_utc`, or `ata_utc`.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the fdm flight schedule file...")

        for line_num, line in enumerate(content, start=1):
            fields = line.split()
            
            # Skip line if insufficient fields
            if len(fields) < 8:
                logger.error(f"Skipping line {line_num} due to insufficient fields: {fields}")
                continue

            try:
                # Extract fields
                flight_date = fields[0]
                tail_no = fields[1]
                flight_no = fields[2]
                dep_code_icao = fields[3]
                arr_code_icao = fields[4]
                std = fields[5]
                sta = fields[6]
                flight_type = fields[7] if len(fields) > 7 and fields[7] else " "
                etd = fields[8] if len(fields) > 8 and fields[8] else " "
                eta = fields[9] if len(fields) > 9 and fields[9] else " "
                arrival_date = fields[-1]

                # Parse dates
                try:
                    sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date()
                    sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date()
                except ValueError as ve:
                    logger.error(f"Skipping line {line_num} due to date format error: {flight_date}, {arrival_date}")
                    continue

                # Parse times with individual error handling
                std_utc = None
                sta_utc = None
                etd_utc = None
                eta_utc = None

                try:
                    std_utc = datetime.strptime(std, "%H:%M").time()
                except ValueError:
                    logger.warning(f"STD time format error on line {line_num}: {std}")

                try:
                    sta_utc = datetime.strptime(sta, "%H:%M").time()
                except ValueError:
                    logger.warning(f"STA time format error on line {line_num}: {sta}")

                try:
                    etd_utc = datetime.strptime(etd, "%H:%M").time() if etd.strip() else None
                except ValueError:
                    logger.warning(f"ETD time format error on line {line_num}: {etd}")

                try:
                    eta_utc = datetime.strptime(eta, "%H:%M").time() if eta.strip() else None
                except ValueError:
                    logger.warning(f"ETA time format error on line {line_num}: {eta}")

                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                if not dep_airport or not arr_airport:
                    logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
                    continue

                dep_code_iata = dep_airport.iata_code
                arr_code_iata = arr_airport.iata_code

                # Define unique criteria
                unique_criteria = {
                    'flight_no': flight_no,
                    'tail_no': tail_no,
                    'sd_date_utc': sd_date_utc,
                    'dep_code_icao': dep_code_icao,
                    'arr_code_icao': arr_code_icao,
                    'sa_date_utc': sa_date_utc,
                    'std_utc': std_utc,
                    'sta_utc': sta_utc,
                }

                # Check if any record exists with the same unique criteria
                existing_record = FdmFlightData.objects.filter(**unique_criteria).first()

                if existing_record:
                    logger.info(f"Record for flight {flight_no} on {sd_date_utc} exists; no update needed.")
                    continue  # Skip insertion if a record exists

                # Create a new record
                FdmFlightData.objects.create(
                    flight_no=flight_no,
                    tail_no=tail_no,
                    dep_code_iata=dep_code_iata,
                    dep_code_icao=dep_code_icao,
                    arr_code_iata=arr_code_iata,
                    arr_code_icao=arr_code_icao,
                    sd_date_utc=sd_date_utc,
                    std_utc=std_utc,
                    sta_utc=sta_utc,
                    sa_date_utc=sa_date_utc,
                    flight_type=flight_type,
                    etd_utc=etd_utc,
                    eta_utc=eta_utc,
                    raw_content=line
                )
                logger.info(f"Created new FDM flight record: {flight_no} on {sd_date_utc}")

            except Exception as ve:
                logger.error(f"Error processing FDM flight record on line {line_num}: {ve} - {line}")
                continue

        logger.info("FDM Flight schedule file processed successfully.")

    except Exception as e:
        logger.error(f"Error processing FDM flight schedule file: {e}", exc_info=True)





def process_fdm_email_attachment(item, process_function):
    """
    Generalized function to handle processing of email attachments.
    `process_function` is the specific function that processes the attachment (e.g., process_airport_file).
    """
    try:
        if item.attachments:
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    logger.info(f"Processing attachment: {attachment.name}")
                    process_function(attachment)  # Call the relevant function for each attachment
    except Exception as e:
        logger.error(f"Error processing fdm email attachment: {e}")


from datetime import datetime
from .models import CrewMember
def preprocess_crew_file(content):
    """
    Preprocess the crew file to ensure each flight's data is on a single row.
    Rows should start with the flight number.
    """
    formatted_lines = []
    current_line = ""

    for line in content.splitlines():
        stripped_line = line.strip()

        # Check if the line starts with a flight number (numeric)
        if stripped_line[:3].isdigit():  # Assuming flight numbers are at least 3 digits
            if current_line:
                formatted_lines.append(current_line.strip())  # Add the previous line
            current_line = stripped_line  # Start a new record
        else:
            current_line += f" {stripped_line}"  # Append to the current line

    if current_line:
        formatted_lines.append(current_line.strip())  # Add the last record

    return formatted_lines






from datetime import datetime
from .models import CrewMember

def process_crew_details_file(attachment):
    """
    Process the crew details file and update the CrewMember table.
    """
    try:
        # Preprocess the file content
        raw_content = attachment.content.decode('utf-8')
        formatted_content = preprocess_crew_file(raw_content)

        logger.info("Starting to process the crew details file...")

        for line_num, line in enumerate(formatted_content, start=1):
            fields = line.split()

            # Extract mandatory flight and route details
            try:
                if len(fields) < 4:
                    raise ValueError("Insufficient flight details")

                flight_no = fields[0].strip()
                flight_date = fields[1].strip()  # Format: DDMMYYYY
                origin = fields[2].strip()
                destination = fields[3].strip()

                if not flight_no.isdigit():
                    raise ValueError("Invalid flight number format")

                sd_date_utc = datetime.strptime(flight_date, "%d%m%Y").date()
            except ValueError as e:
                logger.error(f"Skipping line {line_num} due to invalid flight details: {line} - {e}")
                continue

            # Process the rest of the line for crew details
            crew_data = fields[4:]
            if len(crew_data) % 2 != 0:
                logger.warning(f"Skipping line {line_num} due to incomplete crew data: {line}")
                continue

            for i in range(0, len(crew_data), 2):
                try:
                    role = crew_data[i].strip()
                    raw_data = crew_data[i + 1].strip()

                    if len(raw_data) < 8:
                        raise ValueError("Invalid crew ID or name format")

                    crew_id = raw_data[:8].strip()
                    name = raw_data[8:].strip()

                    # Validate role
                    if role not in dict(CrewMember.ROLE_CHOICES):
                        raise ValueError(f"Invalid role: {role}")

                    # Update or create CrewMember
                    CrewMember.objects.update_or_create(
                        crew_id=crew_id,
                        defaults={
                            'flight_no': flight_no,
                            'sd_date_utc': sd_date_utc,
                            'origin': origin,
                            'destination': destination,
                            'name': name,
                            'role': role,
                        }
                    )
                    logger.info(f"Processed crew member {name} ({role}) for flight {flight_no}.")

                except ValueError as e:
                    logger.error(f"Error parsing crew data on line {line_num}: {line} - {e}")
                    continue

        logger.info("Crew details file processed successfully.")

    except Exception as e:
        logger.error(f"Error processing crew details file: {e}", exc_info=True)






def process_fdm_crew_email_attachment(item, process_function):
    """
    Generalized function to handle processing of email attachments.
    `process_function` is the specific function that processes the attachment (e.g., process_crew_details_file).
    """
    try:
        if item.attachments:
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    logger.info(f"Processing attachment: {attachment.name}")
                    process_function(attachment)  # Call the relevant function for each attachment
    except Exception as e:
        logger.error(f"Error processing fdm email attachment: {e}")
