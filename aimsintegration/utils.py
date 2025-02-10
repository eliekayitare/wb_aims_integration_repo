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
from django.db import transaction
from .models import AirportData, FlightData
import logging

logger = logging.getLogger(__name__)

def process_flight_schedule_file(attachment):
    """
    Process the flight schedule file, preventing duplicates using logic checks and transaction handling.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the flight schedule file...")

        for line_num, line in enumerate(content, start=1):
            try:
                # Split the line based on comma delimiter
                fields = line.split(',')

                # Ensure all fields are stripped of surrounding quotes
                fields = [field.strip().replace('"', '') for field in fields]

                # Extract fields
                flight_date = fields[0]
                tail_no = fields[1]
                flight_no = fields[2]
                dep_code_icao = fields[3]
                arr_code_icao = fields[4]
                std = fields[5]
                sta = fields[6]
                flight_service_type = fields[7] if len(fields) > 7 else None
                etd = fields[8] if len(fields) > 8 else None
                eta = fields[9] if len(fields) > 9 else None
                atd = fields[10] if len(fields) > 10 else None
                takeoff = fields[11] if len(fields) > 11 else None
                touchdown = fields[12] if len(fields) > 12 else None
                ata = fields[13] if len(fields) > 13 else None
                arrival_date = fields[14] if len(fields) > 14 else None

                print("\n-------------------------------------------------------------\n")
                print(f"\nFlight Date: {flight_date}\nTail No: {tail_no}\nFlight No: {flight_no}\n Dep Code ICAO: {dep_code_icao}\n Arr Code ICAO: {arr_code_icao}\nSTD: {std}\nSTA: {sta}\nflight service type: {flight_service_type}\n ED: {etd}\n ESTA: {eta}\n ATD: {atd}\n Takeoff: {takeoff}\n Touchdown: {touchdown}\n ATA: {ata}\n Arrival Date: {arrival_date}")
                print("\n-------------------------------------------------------------\n")

                # Parse dates and times
                try:
                    sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date()
                    sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
                    std_utc = datetime.strptime(std, "%H:%M").time()
                    sta_utc = datetime.strptime(sta, "%H:%M").time()
                    atd_utc = datetime.strptime(atd, "%H:%M").time() if atd else None
                    takeoff_utc = datetime.strptime(takeoff, "%H:%M").time() if takeoff else None
                    touchdown_utc = datetime.strptime(touchdown, "%H:%M").time() if touchdown else None
                    ata_utc = datetime.strptime(ata, "%H:%M").time() if ata else None
                    etd_utc = datetime.strptime(etd, "%H:%M").time() if etd else None
                    eta_utc = datetime.strptime(eta, "%H:%M").time() if eta else None
                except ValueError:
                    logger.error(f"Skipping line {line_num} due to date/time format error: {line}")
                    continue

                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                if not dep_airport or not arr_airport:
                    logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
                    continue

                dep_code_iata = dep_airport.iata_code
                arr_code_iata = arr_airport.iata_code

                # Define criteria to check for existing record
                existing_record = FlightData.objects.filter(
                    flight_no=flight_no,
                    tail_no=tail_no,
                    sd_date_utc=sd_date_utc,
                    dep_code_icao=dep_code_icao,
                    arr_code_icao=arr_code_icao,
                    std_utc=std_utc,
                    sta_utc=sta_utc,
                ).first()

                # Prevent duplicate insertions using transaction.atomic()
                with transaction.atomic():
                    if existing_record:
                        logger.info(f"Record already exists for flight {flight_no} on {sd_date_utc}. Checking for updates...")
                        
                        # Check if any actual times need updating
                        updated = False
                        if std_utc and existing_record.std_utc != std_utc:
                            existing_record.std_utc = std_utc
                            updated = True
                        if sta_utc and existing_record.sta_utc != sta_utc:
                            existing_record.sta_utc = sta_utc
                            updated = True
                        if atd_utc and existing_record.atd_utc != atd_utc:
                            existing_record.atd_utc = atd_utc
                            updated = True
                        if takeoff_utc and existing_record.takeoff_utc != takeoff_utc:
                            existing_record.takeoff_utc = takeoff_utc
                            updated = True
                        if touchdown_utc and existing_record.touchdown_utc != touchdown_utc:
                            existing_record.touchdown_utc = touchdown_utc
                            updated = True
                        if ata_utc and existing_record.ata_utc != ata_utc:
                            existing_record.ata_utc = ata_utc
                            updated = True
                     
                        if updated:
                            existing_record.save()
                            logger.info(f"Updated FlightData record for flight {flight_no} on {sd_date_utc}.")
                        else:
                            logger.info(f"No changes for FlightData record {flight_no} on {sd_date_utc}.")
                    else:
                        # Create a new record if no existing one matches
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
                            atd_utc=atd_utc,
                            takeoff_utc=takeoff_utc,
                            touchdown_utc=touchdown_utc,
                            ata_utc=ata_utc,
                            sa_date_utc=sa_date_utc,
                            source_type="FDM",
                            raw_content=",".join(fields),
                        )
                        logger.info(f"Inserted new flight record: {flight_no} on {sd_date_utc}.")
            except Exception as e:
                logger.error(f"Error processing line {line_num}: {e} - {fields}", exc_info=True)
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
    dep_code_iata = dep_code_match.group(1).strip() if dep_code_match else None
    arr_code_iata = arr_code_match.group(1).strip() if arr_code_match else None

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

        # #FDM Project
        # fdm_flights = FdmFlightData.objects.filter(
        #     flight_no=flight_no,
        #     tail_no=tail_number,
        #     dep_code_iata=dep_code,
        #     arr_code_iata=arr_code
        # )
        

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
                    settings.THIRD_EMAIL_RECEIVER,
                ],
                fail_silently=False,
            )
            return
        
        # if not fdm_flights.exists():
        #     logger.info(f"No matching FDM flights found for flight number: {flight_no}")
        #     send_mail(
        #         subject=f"No matching FDM flights found for flight number: {flight_no}",
        #         message=(
        #             f"Dear Team,\n\n"
        #             f"The ACARS message for flight {flight_no} could not be matched.\n"
        #             f"Message details:\n\n{message_body}\n\n"
        #             f"Please review and update manually.\n\n"
        #             f"Regards,\nFlightOps Team"
        #         ),
        #         from_email=settings.EMAIL_HOST_USER,
        #         recipient_list=[
        #             # settings.FIRST_EMAIL_RECEIVER,
        #             # settings.SECOND_EMAIL_RECEIVER,
        #             settings.THIRD_EMAIL_RECEIVER,
        #         ],
        #         fail_silently=False,
        #     )
        #     return

        closest_flight = min(
            flights,
            key=lambda flight: abs((flight.sd_date_utc - email_received_date).days)
        )

        # closest_fdm_flight = min(
        #     fdm_flights,
        #     key=lambda fl: abs((fl.sd_date_utc - email_received_date).days)
        # ) 

        if acars_event == "OT":
            closest_flight.atd_utc = event_time
            # closest_fdm_flight.atd_utc = event_time
        elif acars_event == "OF":
            closest_flight.takeoff_utc = event_time
            # closest_fdm_flight.takeoff_utc = event_time
        elif acars_event == "ON":
            closest_flight.touchdown_utc = event_time
            # closest_fdm_flight.touchdown_utc = event_time
        elif acars_event == "IN":
            closest_flight.ata_utc = event_time
            # closest_fdm_flight.ata_utc = event_time

        closest_flight.save()
        # closest_fdm_flight.save()

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
from .models import FdmFlightData, AirportData, FlightData
import logging
import csv

logger = logging.getLogger(__name__)

def process_fdm_flight_schedule_file(attachment):
    """
    Process the FDM flight schedule file using a comma delimiter.
    Insert into FdmFlightData and update missing actual timings in FlightData.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the FDM flight schedule file...")

        reader = csv.reader(content)

        for line_num, row in enumerate(reader, start=1):
            try:
                # Extract fields
                flight_date = row[0].strip() if len(row) > 0 else ""
                tail_no = row[1].strip()[:10] if len(row) > 1 else ""
                flight_no = row[2].strip()[:6] if len(row) > 2 else ""
                dep_code_icao = row[3].strip()[:4] if len(row) > 3 else ""
                arr_code_icao = row[4].strip()[:4] if len(row) > 4 else ""
                std_utc = row[5].strip() if len(row) > 5 else ""
                sta_utc = row[6].strip() if len(row) > 6 else ""
                flight_type = row[7].strip()[:10] if len(row) > 7 else ""
                etd_utc = row[8].strip() if len(row) > 8 else ""
                eta_utc = row[9].strip() if len(row) > 9 else ""
                atd_utc = row[10].strip() if len(row) > 10 else ""
                takeoff_utc = row[11].strip() if len(row) > 11 else ""
                touchdown_utc = row[12].strip() if len(row) > 12 else ""
                ata_utc = row[13].strip() if len(row) > 13 else ""
                arrival_date = row[14].strip() if len(row) > 14 else ""

                # Parse dates and times
                sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date() if flight_date else None
                sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
                std_utc_time = datetime.strptime(std_utc, "%H:%M").time() if std_utc else None
                sta_utc_time = datetime.strptime(sta_utc, "%H:%M").time() if sta_utc else None
                etd_utc_time = datetime.strptime(etd_utc, "%H:%M").time() if etd_utc else None
                eta_utc_time = datetime.strptime(eta_utc, "%H:%M").time() if eta_utc else None
                atd_utc_time = datetime.strptime(atd_utc, "%H:%M").time() if atd_utc else None
                takeoff_utc_time = datetime.strptime(takeoff_utc, "%H:%M").time() if takeoff_utc else None
                touchdown_utc_time = datetime.strptime(touchdown_utc, "%H:%M").time() if touchdown_utc else None
                ata_utc_time = datetime.strptime(ata_utc, "%H:%M").time() if ata_utc else None

                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                dep_code_iata = dep_airport.iata_code if dep_airport else ""
                arr_code_iata = arr_airport.iata_code if arr_airport else ""

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

                # Insert or Update FDM FlightData
                existing_record = FdmFlightData.objects.filter(**unique_criteria).first()

                if existing_record:
                    # Update fields if actual timings have changed
                    updated = False
                    if std_utc_time and existing_record.std_utc != std_utc_time:
                        existing_record.std_utc = std_utc_time
                        updated = True
                    if sta_utc_time and existing_record.sta_utc != sta_utc_time:
                        existing_record.sta_utc = sta_utc_time
                        updated = True

                    if atd_utc_time and existing_record.atd_utc != atd_utc_time:
                        existing_record.atd_utc = atd_utc_time
                        updated = True
                    if takeoff_utc_time and existing_record.takeoff_utc != takeoff_utc_time:
                        existing_record.takeoff_utc = takeoff_utc_time
                        updated = True
                    if touchdown_utc_time and existing_record.touchdown_utc != touchdown_utc_time:
                        existing_record.touchdown_utc = touchdown_utc_time
                        updated = True
                    if ata_utc_time and existing_record.ata_utc != ata_utc_time:
                        existing_record.ata_utc = ata_utc_time
                        updated = True
                    if etd_utc_time and existing_record.etd_utc != etd_utc_time:
                        existing_record.etd_utc = etd_utc_time
                        updated = True
                    if eta_utc_time and existing_record.eta_utc != eta_utc_time:
                        existing_record.eta_utc = eta_utc_time
                        updated = True

                    if flight_type and existing_record.flight_type != flight_type:
                        existing_record.flight_type = flight_type
                        updated = True

                    if updated:
                        existing_record.save()
                        logger.info(f"Updated FDM record for flight {flight_no} on {sd_date_utc}.")
                    else:
                        logger.info(f"No changes for FDM record {flight_no} on {sd_date_utc}.")
                else:
                    # Create a new FdmFlightData record
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
                        atd_utc=atd_utc_time,
                        takeoff_utc=takeoff_utc_time,
                        touchdown_utc=touchdown_utc_time,
                        ata_utc=ata_utc_time,
                        raw_content=','.join(row)
                    )
                    logger.info(f"Created new FDM flight record: {flight_no} on {sd_date_utc}.")

            except Exception as e:
                logger.error(f"Error processing line {line_num}: {e} - {row}", exc_info=True)
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


# from datetime import datetime
# from .models import CrewMember
# def preprocess_crew_file(content):
#     """
#     Preprocess the crew file to ensure each flight's data is on a single row.
#     Rows should start with the flight number.
#     """
#     formatted_lines = []
#     current_line = ""

#     for line in content.splitlines():
#         stripped_line = line.strip()

#         # Check if the line starts with a flight number (numeric)
#         if stripped_line[:3].isdigit():  # Assuming flight numbers are at least 3 digits
#             if current_line:
#                 formatted_lines.append(current_line.strip())  # Add the previous line
#             current_line = stripped_line  # Start a new record
#         else:
#             current_line += f" {stripped_line}"  # Append to the current line

#     if current_line:
#         formatted_lines.append(current_line.strip())  # Add the last record

#     return formatted_lines





# import re
# import pandas as pd
# from datetime import datetime
# from .models import CrewMember



# def process_crew_details_file(attachment):
#     """
#     Parse and process crew details from an unstructured file.
#     """
#     try:
#         # Read file content
#         raw_content = attachment.content.decode('utf-8').splitlines()
#         rows = [line.strip() for line in raw_content if line.strip()]  # Remove empty lines

#         parsed_data = []
#         valid_roles = {'CP', 'FO', 'FP', 'SA', 'FA', 'FE', 'AC'}  # Valid roles
#         flight_context = {}

#         for line_num, line in enumerate(rows, start=1):
#             try:
#                 # Detect flight header
#                 flight_no = line[:4].strip()
#                 flight_date_str = line[4:13].strip()
#                 origin = line[13:17].strip()
#                 destination = line[17:20].strip()

#                 print("\n=======================================================")
#                 print(f"\nFlight Number: {flight_no}\nDate: {flight_date_str}\nOrigin: {origin}\nDestination: {destination}")
#                 print("\n=======================================================\n")

#                 # Convert date
#                 try:
#                     sd_date_utc = datetime.strptime(flight_date_str, "%d%m%Y").date()
#                 except ValueError:
#                     raise ValueError(f"Invalid date format: {flight_date_str}")

#                 # Update flight context
#                 flight_context = {
#                     "flight_no": flight_no,
#                     "sd_date_utc": sd_date_utc,
#                     "origin": origin,
#                     "destination": destination,
#                 }

#                 # Crew data starts after position 20
#                 crew_data = line[20:].strip()

#                 # Regex to match role, crew ID, and name
#                 crew_pattern = re.compile(r"(?P<role>\b(?:CP|FO|FP|SA|FA|FE|AC)\b)\s+(?P<crew_id>\d{8})(?P<name>.+?)(?=\b(?:CP|FO|FP|SA|FA|FE|AC)\b|$)")

#                 for match in crew_pattern.finditer(crew_data):
#                     role = match.group("role")
#                     crew_id = match.group("crew_id")
#                     name = match.group("name").strip()

#                     if not role or not crew_id or not name:
#                         print(f"Skipping malformed entry in line {line_num}: Role={role}, Crew ID={crew_id}, Name={name}")
#                         continue

#                     # Append parsed data
#                     parsed_data.append({
#                         **flight_context,
#                         "role": role,
#                         "crew_id": crew_id,
#                         "name": name,
#                     })

#                     print("\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
#                     print(f"\nCrew ID: {crew_id}\nRole: {role}\nName: {name}")
#                     print("\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

#             except ValueError as ve:
#                 print(f"Error in crew data on line {line_num}: {ve}")
#             except Exception as e:
#                 print(f"Error processing line {line_num}: {e}")

#         # Convert parsed data to DataFrame
#         crew_df = pd.DataFrame(parsed_data)
#         if crew_df.empty:
#             print("No valid data extracted.")
#             return

#         # Save to the database
#         for _, row in crew_df.iterrows():
#             try:
#                 CrewMember.objects.update_or_create(
#                     crew_id=row["crew_id"],
#                     defaults={
#                         "flight_no": row["flight_no"],
#                         "sd_date_utc": row["sd_date_utc"],
#                         "origin": row["origin"],
#                         "destination": row["destination"],
#                         "role": row["role"],
#                         "name": row["name"],
#                     }
#                 )
#             except Exception as db_err:
#                 print(f"Database error for {row['crew_id']}: {db_err}")

#         print("Crew details file processed successfully.")

#     except Exception as e:
#         print(f"Error processing crew details file: {e}")



import re
import pandas as pd
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

        # Check if the line starts with a flight number (3 or 4 digits)
        if re.match(r"^\d{3,4}\s+\d{8}[A-Z]{3}", stripped_line):  
            if current_line:
                formatted_lines.append(current_line.strip())  # Save the previous line
            current_line = stripped_line  # Start a new record
        else:
            current_line += f" {stripped_line}"  # Append to the current line

    if current_line:
        formatted_lines.append(current_line.strip())  # Save the last record

    return formatted_lines

def process_crew_details_file(attachment):
    """
    Parse and process crew details from an unstructured file.
    Extracts only CP and FO roles.
    """
    try:
        # Read file content
        raw_content = attachment.content.decode('utf-8')
        formatted_lines = preprocess_crew_file(raw_content)  # Preprocess data
        parsed_data = []

        valid_roles = {'CP', 'FO'}  # Only extract CP and FO
        flight_context = {}

        for line_num, line in enumerate(formatted_lines, start=1):
            try:
                # Detect flight header
                flight_no = line[:4].strip()
                flight_date_str = line[4:12].strip()
                origin = line[12:15].strip()
                destination = line[15:18].strip()

                print("\n=======================================================")
                print(f"📌 Processing Line {line_num} -> Flight {flight_no} | {flight_date_str} | {origin} → {destination}")
                print("=======================================================\n")

                # Convert date
                try:
                    sd_date_utc = datetime.strptime(flight_date_str, "%d%m%Y").date()
                except ValueError:
                    raise ValueError(f"Invalid date format: {flight_date_str}")

                # Update flight context
                flight_context = {
                    "flight_no": flight_no,
                    "sd_date_utc": sd_date_utc,
                    "origin": origin,
                    "destination": destination,
                }

                # Crew data starts after position 18
                crew_data = line[18:].strip()

                # **Improved Regex for CP/FO crew members**  
                # ✅ Captures roles CP or FO  
                # ✅ Allows for flexible spacing between role, ID, and name  
                # ✅ Ignores other roles (FA, FE, etc.)  
                crew_pattern = re.compile(
                    r"\b(?P<role>CP|FO)\s+D?(?P<crew_id>\d{8})\s+(?P<name>[A-Z][A-Z\s]+?)(?=\s+(?:CP|FO|\b|$))"
                )

                crew_found = False  # Track if any CP/FO crew is found

                for match in crew_pattern.finditer(crew_data):
                    role = match.group("role")
                    crew_id = match.group("crew_id")
                    name = match.group("name").strip()

                    if role not in valid_roles:
                        continue  # Skip non-CP/FO roles

                    parsed_data.append({
                        **flight_context,
                        "role": role,
                        "crew_id": crew_id,
                        "name": name,
                    })

                    crew_found = True  # At least one CP/FO crew found

                    print("\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                    print(f"✅ Crew Found! ID: {crew_id} | Role: {role} | Name: {name}")
                    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

                if not crew_found:
                    print(f"⚠️ WARNING [Line {line_num}]: No CP/FO crew found in -> {crew_data}")

            except ValueError as ve:
                print(f"❌ Error in crew data on line {line_num}: {ve}")
            except Exception as e:
                print(f"❌ Error processing line {line_num}: {e}")

        # Convert parsed data to DataFrame
        crew_df = pd.DataFrame(parsed_data)
        if crew_df.empty:
            print("❌ No valid CP/FO data extracted.")
            return

        # Save to the database
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
                print(f"❌ Database error for {row['crew_id']}: {db_err}")

        print("✅ Crew details file processed successfully!")

    except Exception as e:
        print(f"❌ Error processing crew details file: {e}")










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




# Tableau project
from .models import TableauData
from datetime import datetime, time
import logging

logger = logging.getLogger(__name__)

def process_tableau_data_file(attachment):
    """
    Process the tableau file, ensuring proper parsing of all fields based on the dataset structure.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the Tableau file...")

        def parse_date(value, field_name):
            value = value.strip()
            if not value or value in ["0000", "00000000", None]:
                logger.warning(f"{field_name} is empty or invalid. Defaulting to None.")
                return None  # Default to None
            try:
                return datetime.strptime(value, "%d%m%Y").date()
            except (ValueError, TypeError):
                logger.warning(f"Invalid {field_name}: {value}. Defaulting to None.")
                return None

        def parse_time(value, field_name):
            value = value.strip()
            if not value or value == "0000":
                logger.warning(f"{field_name} is empty or invalid. Defaulting to None.")
                return None
            try:
                return datetime.strptime(value, "%H%M").time()
            except (ValueError, TypeError):
                logger.warning(f"Invalid {field_name}: {value}. Defaulting to None.")
                return None

        def format_time(time_obj):
            return time_obj.strftime("%H:%M") if isinstance(time_obj, time) else None

        def parse_string(value, field_name):
            value = value.strip()
            if not value:
                logger.warning(f"{field_name} is empty or invalid. Defaulting to an empty string.")
                return ""
            return value

        for line_num, line in enumerate(content, start=1):
            if not line.strip():
                continue

            try:
                # Extract the first field (aircraft_config) before the 4th comma
                first_field_end = line.find(',', line.find(',', line.find(',', line.find(',') + 1) + 1) + 1)
                aircraft_config = line[:first_field_end].strip()

                # Extract the second field (operation_day) right after the 4th comma
                remaining_line = line[first_field_end + 1:].strip()
                operation_day_field_end = remaining_line.find(',')
                operation_day = parse_date(remaining_line[:operation_day_field_end].strip(), "Operation Day")

                # Extract the remaining fields
                remaining_fields = remaining_line[operation_day_field_end + 1:].split(",")
                remaining_fields = [field.strip() for field in remaining_fields]

                # Parse the fields
                departure_station = remaining_fields[0]
                flight_no = remaining_fields[1]
                flight_leg_code = remaining_fields[2] if len(remaining_fields) > 2 else " "
                cancelled_deleted = remaining_fields[3]
                arrival_station = remaining_fields[4]
                aircraft_reg_id = remaining_fields[5]
                aircraft_type_index = remaining_fields[6] or None
                aircraft_category = remaining_fields[7] or None
                flight_service_type = remaining_fields[8]
                std = parse_time(remaining_fields[9], "STD")
                sta = parse_time(remaining_fields[10], "STA")
                original_operation_day = parse_date(remaining_fields[11], "Original Operation Day") if remaining_fields[11] != "0000" else " "
                original_std = parse_time(remaining_fields[12], "Original STD") if remaining_fields[12] != "0000" else time(0, 0)
                original_sta = parse_time(remaining_fields[13], "Original STA") if remaining_fields[13] != "0000" else time(0, 0)
                departure_delay_time = parse_string(remaining_fields[14], "Departure Delay Time")
                delay_code_kind = parse_string(remaining_fields[15], "Delay Code Kind")
                delay_number = parse_string(remaining_fields[16], "Delay Number") if len(remaining_fields) > 16 else ""
                seat_type_config = parse_string(remaining_fields[17], "Seat Type Config") if len(remaining_fields) > 17 else ""
                atd = parse_time(remaining_fields[18], "ATD") if len(remaining_fields) > 18 else None
                takeoff = parse_time(remaining_fields[19], "Takeoff") if len(remaining_fields) > 19 else None
                touchdown = parse_time(remaining_fields[20], "Touchdown") if len(remaining_fields) > 20 else None
                ata = parse_time(remaining_fields[21], "ATA") if len(remaining_fields) > 21 else None

                print("\n=======================================================")
                print(f"\nAircraft Config: {aircraft_config}\nOperation Day: {operation_day}\nDeparture Station: {departure_station}\nFlight No: {flight_no}\nFlight Leg Code: {flight_leg_code}\nCancelled/Deleted: {cancelled_deleted}\nArrival Station: {arrival_station}\nAircraft Reg ID: {aircraft_reg_id}\nAircraft Type Index: {aircraft_type_index}\nAircraft Category: {aircraft_category}\nFlight Service Type: {flight_service_type}\nSTD: {format_time(std)}\nSTA: {format_time(sta)}\nOriginal Operation Day: {original_operation_day}\nOriginal STD: {format_time(original_std)}\nOriginal STA: {format_time(original_sta)}\nDeparture Delay Time: {departure_delay_time}\nDelay Code Kind: {delay_code_kind}\nDelay Number: {delay_number}\nSeat Type Config: {seat_type_config}\nATD: {format_time(atd)}\nTakeoff: {format_time(takeoff)}\nTouchdown: {format_time(touchdown)}\nATA: {format_time(ata)}")
                print()

                # Database operations
                unique_criteria = {
                    'operation_day': operation_day,
                    'departure_station': departure_station,
                    'flight_no': flight_no,
                    'arrival_station': arrival_station,
                    'flight_leg_code': flight_leg_code,
                }

                existing_record = TableauData.objects.filter(**unique_criteria).first()

                if existing_record:
                    updated = False
                    fields_to_update = {
                        'cancelled_deleted': cancelled_deleted,
                        'aircraft_reg_id': aircraft_reg_id,
                        'aircraft_type_index': aircraft_type_index,
                        'aircraft_category': aircraft_category,
                        'flight_service_type': flight_service_type,
                        'std': std,
                        'sta': sta,
                        'original_operation_day': original_operation_day,
                        'original_std': original_std,
                        'original_sta': original_sta,
                        'departure_delay_time': departure_delay_time,
                        'atd': atd,
                        'takeoff': takeoff,
                        'touchdown': touchdown,
                        'ata': ata,
                        'delay_code_kind': delay_code_kind,
                        'delay_number': delay_number,
                        'aircraft_config': aircraft_config,
                        'seat_type_config': seat_type_config,
                    }

                    for field, new_value in fields_to_update.items():
                        if getattr(existing_record, field, None) != new_value:
                            setattr(existing_record, field, new_value)
                            updated = True

                    if updated:
                        existing_record.save()
                        logger.info(f"Updated record for flight {flight_no} on {operation_day}.")
                    else:
                        logger.info(f"No changes detected for flight {flight_no} on {operation_day}.")
                else:
                    TableauData.objects.create(
                        operation_day=operation_day,
                        departure_station=departure_station,
                        flight_no=flight_no,
                        flight_leg_code=flight_leg_code,
                        cancelled_deleted=cancelled_deleted,
                        arrival_station=arrival_station,
                        aircraft_reg_id=aircraft_reg_id,
                        aircraft_type_index=aircraft_type_index,
                        aircraft_category=aircraft_category,
                        flight_service_type=flight_service_type,
                        std=format_time(std),
                        sta=format_time(sta),
                        original_operation_day=original_operation_day,
                        original_std=format_time(original_std),
                        original_sta=format_time(original_sta),
                        departure_delay_time=departure_delay_time,
                        atd=format_time(atd),
                        takeoff=format_time(takeoff),
                        touchdown=format_time(touchdown),
                        ata=format_time(ata),
                        delay_code_kind=delay_code_kind,
                        delay_number=delay_number,
                        aircraft_config=aircraft_config,
                        seat_type_config=seat_type_config
                    )
                    logger.info(f"Created new record for flight {flight_no} on {operation_day}.")
            except Exception as e:
                logger.error(f"Error processing line {line_num}: {line}\n{e}")
                continue

        logger.info("Tableau data file processed successfully.")

    except Exception as e:
        logger.error(f"Error processing tableau data file: {e}")






















def process_tableau_data_email_attachment(item, process_function):
    """
    Generalized function to handle processing of email attachments.
    `process_function` is the specific function that processes the attachment (e.g., process_tableau_data_file).
    """
    try:
        if item.attachments:
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    logger.info(f"Processing attachment: {attachment.name}")
                    process_function(attachment)  # Call the relevant function for each attachment
    except Exception as e:
        logger.error(f"Error processing fdm email attachment: {e}")