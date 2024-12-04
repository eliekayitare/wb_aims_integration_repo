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

from datetime import datetime
from .models import FdmFlightData, AirportData

def process_fdm_flight_schedule_file(attachment):
    """
    Process the FDM flight schedule file using fixed column positions, accounting for spaces.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the FDM flight schedule file...")

        for line_num, line in enumerate(content, start=1):
            # Ensure the line has enough columns (length)
            if len(line) < 84:
                logger.error(f"Skipping line {line_num} due to insufficient length: {line}")
                continue

            try:
                # Extract fields based on fixed-width columns including spaces
                # Extract fields based on fixed-width columns including spaces
                flight_date = line[0:10].strip()  # Column 1-10
                tail_no = line[10:27].strip()[:10]  # Column 11-26, max 10 characters
                flight_no = line[26:33].strip() if line[26] != " " else line[27:33].strip()[:6]  # Ensure max length
                dep_code_icao = line[33:38].strip()[:4]  # Max 4 characters for ICAO codes
                arr_code_icao = line[38:43].strip()[:4]  # Max 4 characters for ICAO codes
                std_utc = line[43:50].strip()  # Column 43-49
                sta_utc = line[50:57].strip()  # Column 50-56
                flight_type = line[57:60].strip()[:10]  # Max 10 characters
                etd_utc = line[60:67].strip()  # Column 60-66
                eta_utc = line[67:74].strip()  # Column 67-73
                arrival_date = line[74:84].strip()  # Column 74-83


                # Parse dates
                sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date() if flight_date else None
                sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None

                # Parse times
                std_utc_time = datetime.strptime(std_utc, "%H:%M").time() if std_utc else None
                sta_utc_time = datetime.strptime(sta_utc, "%H:%M").time() if sta_utc else None
                etd_utc_time = datetime.strptime(etd_utc, "%H:%M").time() if etd_utc else None
                eta_utc_time = datetime.strptime(eta_utc, "%H:%M").time() if eta_utc else None

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
                    'std_utc': std_utc_time,
                    'sta_utc': sta_utc_time,
                }

                # Check if record exists
                existing_record = FdmFlightData.objects.filter(**unique_criteria).first()

                if existing_record:
                    logger.info(f"FDM Record for flight {flight_no} on {sd_date_utc} exists; no update needed.")
                    continue

                # Insert new record
                FdmFlightData.objects.create(
                    flight_no=flight_no,
                    tail_no=tail_no,
                    dep_code_iata=dep_code_iata,
                    dep_code_icao=dep_code_icao,
                    arr_code_iata=arr_code_iata,
                    arr_code_icao=arr_code_icao,
                    sd_date_utc=sd_date_utc,
                    std_utc=std_utc_time,
                    sta_utc=sta_utc_time,
                    sa_date_utc=sa_date_utc,
                    flight_type=flight_type,
                    etd_utc=etd_utc_time,
                    eta_utc=eta_utc_time,
                    raw_content=line
                )
                logger.info(f"Created new FDM flight record: {flight_no} on {sd_date_utc}")

            except Exception as e:
                logger.error(f"Error processing line {line_num}: {e} - {line}")
                continue

        logger.info("FDM flight schedule file processed successfully.")

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
import pandas as pd

import re
import pandas as pd
from datetime import datetime
from .models import CrewMember

import pandas as pd
from datetime import datetime

def process_crew_details_file(attachment):
    """
    Parse and process crew details from a structured file, handling multi-row flights.
    """
    try:
        # Read the file content
        raw_content = attachment.content.decode('utf-8').splitlines()
        rows = [line.strip() for line in raw_content if line.strip()]  # Remove empty lines

        parsed_data = []
        current_flight = None  # Track the current flight details
        valid_roles = dict(CrewMember.ROLE_CHOICES)

        for line_num, line in enumerate(rows, start=1):
            try:
                # Detect if this line starts a new flight
                if line[:4].strip().isdigit():
                    # Extract flight details
                    flight_no = line[:4].strip()
                    flight_date_str = line[4:12].strip()
                    origin = line[12:15].strip()
                    destination = line[15:18].strip()
                    print("==================================")
                    print(flight_no)
                    print(flight_date_str)
                    print(origin)
                    print(destination)
                    print("==================================")
                    # Convert date to a valid format
                    try:
                        sd_date_utc = datetime.strptime(flight_date_str, "%d%m%Y").date()
                    except ValueError:
                        raise ValueError(f"Invalid date format: {flight_date_str}")

                    # Update current flight context
                    current_flight = {
                        "flight_no": flight_no,
                        "sd_date_utc": sd_date_utc,
                        "origin": origin,
                        "destination": destination,
                    }
                    crew_data = line[18:].strip()
                else:
                    # Continuation of the previous flight
                    if not current_flight:
                        raise ValueError(f"Invalid continuation line without flight context: {line}")
                    crew_data = line.strip()

                # Process crew data
                i = 0
                while i < len(crew_data):
                    try:
                        # Debug: Print current segment being processed
                        print(f"Processing crew segment: {crew_data[i:i+20]}")

                        # Extract role (2 characters)
                        role = crew_data[i:i+2].strip()
                        if role not in valid_roles:
                            raise ValueError(f"Invalid role: {role}")

                        # Extract crew ID (next 8 characters)
                        crew_id = crew_data[i+2:i+10].strip()
                        if not (crew_id.isdigit() and len(crew_id) == 8):
                            raise ValueError(f"Invalid crew ID: {crew_id}")

                        # Extract name
                        name_start = i + 10
                        next_role_index = crew_data.find("CP", name_start)  # Look for next role
                        if next_role_index == -1:
                            name = crew_data[name_start:].strip()
                            i = len(crew_data)  # End of line
                        else:
                            name = crew_data[name_start:next_role_index].strip()
                            i = next_role_index

                        if len(name) > 100:
                            name = name[:100]  # Truncate if needed

                        # Debug: Print extracted details
                        print("++++++++++++++++++++++++++++++++++++++++++++++++++")
                        print(f"Extracted: role={role}, crew_id={crew_id}, name={name}")
                        print("++++++++++++++++++++++++++++++++++++++++++++++++++")
                        # Append parsed data
                        parsed_data.append({
                            **current_flight,
                            "role": role,
                            "crew_id": crew_id,
                            "name": name,
                        })

                    except ValueError as ve:
                        logger.warning(f"Skipping invalid segment on line {line_num}: {ve}")
                        break

            except Exception as e:
                logger.error(f"Error processing line {line_num}: {line} - {e}")
                continue

        # Convert parsed data to DataFrame
        crew_df = pd.DataFrame(parsed_data)
        if crew_df.empty:
            logger.error("No valid data extracted.")
            return

        # Print for debugging
        print("Parsed DataFrame:\n", crew_df.head())

        # Save to database
        for _, row in crew_df.iterrows():
            try:
                CrewMember.objects.update_or_create(
                    crew_id=row["crew_id"],
                    defaults={
                        "flight_no": row["flight_no"],
                        "sd_date_utc": row["sd_date_utc"],
                        "origin": row["origin"],
                        "destination": row["destination"],
                        "role": row["role"],
                        "name": row["name"],
                    }
                )
            except Exception as db_err:
                logger.error(f"Database error for {row['crew_id']}: {db_err}")

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
