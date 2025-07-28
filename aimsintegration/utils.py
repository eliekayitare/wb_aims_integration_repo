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







# from datetime import datetime
# from django.db import transaction
# from .models import AirportData, FlightData
# import logging

# logger = logging.getLogger(__name__)

# def process_flight_schedule_file(attachment):
#     """
#     Process the flight schedule file, preventing duplicates using logic checks and transaction handling.
#     """
#     try:
#         content = attachment.content.decode('utf-8').splitlines()
#         logger.info("Starting to process the flight schedule file...")

#         for line_num, line in enumerate(content, start=1):
#             try:
#                 # Split the line based on comma delimiter
#                 fields = line.split(',')

#                 # Ensure all fields are stripped of surrounding quotes
#                 fields = [field.strip().replace('"', '') for field in fields]

#                 # Extract fields
#                 flight_date = fields[0]
#                 tail_no = fields[1]
#                 flight_no = fields[2]
#                 dep_code_icao = fields[3]
#                 arr_code_icao = fields[4]
#                 std = fields[5]
#                 sta = fields[6]
#                 flight_service_type = fields[7] if len(fields) > 7 else None
#                 etd = fields[8] if len(fields) > 8 else None
#                 eta = fields[9] if len(fields) > 9 else None
#                 atd = fields[10] if len(fields) > 10 else None
#                 takeoff = fields[11] if len(fields) > 11 else None
#                 touchdown = fields[12] if len(fields) > 12 else None
#                 ata = fields[13] if len(fields) > 13 else None
#                 arrival_date = fields[14] if len(fields) > 14 else None

#                 print("\n-------------------------------------------------------------\n")
#                 print(f"\nFlight Date: {flight_date}\nTail No: {tail_no}\nFlight No: {flight_no}\n Dep Code ICAO: {dep_code_icao}\n Arr Code ICAO: {arr_code_icao}\nSTD: {std}\nSTA: {sta}\nflight service type: {flight_service_type}\n ED: {etd}\n ESTA: {eta}\n ATD: {atd}\n Takeoff: {takeoff}\n Touchdown: {touchdown}\n ATA: {ata}\n Arrival Date: {arrival_date}")
#                 print("\n-------------------------------------------------------------\n")

#                 # Parse dates and times
#                 try:
#                     # FIXED: Changed from "%m/%d/%Y" to "%m%d%Y" to match your CSV format
#                     sd_date_utc = datetime.strptime(flight_date, "%m%d%Y").date()
#                     sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
#                     std_utc = datetime.strptime(std, "%H:%M").time()
#                     sta_utc = datetime.strptime(sta, "%H:%M").time()
#                     atd_utc = datetime.strptime(atd, "%H:%M").time() if atd else None
#                     takeoff_utc = datetime.strptime(takeoff, "%H:%M").time() if takeoff else None
#                     touchdown_utc = datetime.strptime(touchdown, "%H:%M").time() if touchdown else None
#                     ata_utc = datetime.strptime(ata, "%H:%M").time() if ata else None
#                     # Removed etd_utc and eta_utc parsing since they're not in your model
#                 except ValueError:
#                     logger.error(f"Skipping line {line_num} due to date/time format error: {line}")
#                     continue

#                 # Fetch airport data
#                 dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
#                 arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

#                 if not dep_airport or not arr_airport:
#                     logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
#                     continue

#                 dep_code_iata = dep_airport.iata_code
#                 arr_code_iata = arr_airport.iata_code

#                 # Define criteria to check for existing record
#                 existing_record = FlightData.objects.filter(
#                     flight_no=flight_no,
#                     tail_no=tail_no,
#                     sd_date_utc=sd_date_utc,
#                     dep_code_icao=dep_code_icao,
#                     arr_code_icao=arr_code_icao,
#                     std_utc=std_utc,
#                     sta_utc=sta_utc,
#                 ).first()

#                 # Prevent duplicate insertions using transaction.atomic()
#                 with transaction.atomic():
#                     if existing_record:
#                         logger.info(f"Record already exists for flight {flight_no} on {sd_date_utc}. Checking for updates...")
                        
#                         # Check if any actual times need updating
#                         updated = False
#                         if std_utc and existing_record.std_utc != std_utc:
#                             existing_record.std_utc = std_utc
#                             updated = True
#                         if sta_utc and existing_record.sta_utc != sta_utc:
#                             existing_record.sta_utc = sta_utc
#                             updated = True
#                         # if atd_utc and existing_record.atd_utc != atd_utc:
#                         #     existing_record.atd_utc = atd_utc
#                         #     updated = True
#                         # if takeoff_utc and existing_record.takeoff_utc != takeoff_utc:
#                         #     existing_record.takeoff_utc = takeoff_utc
#                         #     updated = True
#                         # if touchdown_utc and existing_record.touchdown_utc != touchdown_utc:
#                         #     existing_record.touchdown_utc = touchdown_utc
#                         #     updated = True
#                         # if ata_utc and existing_record.ata_utc != ata_utc:
#                         #     existing_record.ata_utc = ata_utc
#                         #     updated = True
                     
#                         if updated:
#                             existing_record.save()
#                             logger.info(f"Updated FlightData record for flight {flight_no} on {sd_date_utc}.")
#                         else:
#                             logger.info(f"No changes for FlightData record {flight_no} on {sd_date_utc}.")
#                     else:
#                         # Create a new record if no existing one matches
#                         FlightData.objects.create(
#                             flight_no=flight_no,
#                             tail_no=tail_no,
#                             dep_code_iata=dep_code_iata,
#                             dep_code_icao=dep_code_icao,
#                             arr_code_iata=arr_code_iata,
#                             arr_code_icao=arr_code_icao,
#                             sd_date_utc=sd_date_utc,
#                             std_utc=std_utc,
#                             sta_utc=sta_utc,
#                             # atd_utc=atd_utc,
#                             # takeoff_utc=takeoff_utc,
#                             # touchdown_utc=touchdown_utc,
#                             # ata_utc=ata_utc,
#                             sa_date_utc=sa_date_utc,
#                             source_type="FDM",
#                             raw_content=",".join(fields),
#                         )
#                         logger.info(f"Inserted new flight record: {flight_no} on {sd_date_utc}.")
#             except Exception as e:
#                 logger.error(f"Error processing line {line_num}: {e} - {fields}", exc_info=True)
#                 continue

#         logger.info("Flight schedule file processed successfully.")

#     except Exception as e:
#         logger.error(f"Error processing flight schedule file: {e}", exc_info=True)



from datetime import datetime
from django.db import transaction
from .models import AirportData, FlightData
import logging

logger = logging.getLogger(__name__)

def process_flight_schedule_file(attachment):
    """
    Process the flight schedule file with proper duplicate prevention.
    Ensures one unique flight number per day per route.
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

                logger.info(f"Processing: Flight {flight_no}, Tail {tail_no}, Date {flight_date}, Route {dep_code_icao}-{arr_code_icao}")

                # Parse dates and times
                try:
                    sd_date_utc = datetime.strptime(flight_date, "%m%d%Y").date()
                    sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
                    std_utc = datetime.strptime(std, "%H:%M").time()
                    sta_utc = datetime.strptime(sta, "%H:%M").time()
                    atd_utc = datetime.strptime(atd, "%H:%M").time() if atd else None
                    takeoff_utc = datetime.strptime(takeoff, "%H:%M").time() if takeoff else None
                    touchdown_utc = datetime.strptime(touchdown, "%H:%M").time() if touchdown else None
                    ata_utc = datetime.strptime(ata, "%H:%M").time() if ata else None
                except ValueError as e:
                    logger.error(f"Skipping line {line_num} due to date/time format error: {e} - {line}")
                    continue

                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                if not dep_airport or not arr_airport:
                    logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
                    continue

                dep_code_iata = dep_airport.iata_code
                arr_code_iata = arr_airport.iata_code

                # FIXED: Proper duplicate detection - flight number should be unique per day per route
                # Don't include tail_no, std_utc, sta_utc in the filter as these can change during rescheduling
                existing_record = FlightData.objects.filter(
                    flight_no=flight_no,
                    sd_date_utc=sd_date_utc,
                    dep_code_icao=dep_code_icao,
                    arr_code_icao=arr_code_icao,
                ).first()

                # Process record with transaction.atomic()
                with transaction.atomic():
                    if existing_record:
                        logger.info(f"Found existing record for flight {flight_no} on {sd_date_utc}. Updating...")
                        
                        # Always update scheduled data (this handles reschedules)
                        updated = False
                        
                        # Update tail number (aircraft swap)
                        if existing_record.tail_no != tail_no:
                            logger.info(f"Updating tail number: {existing_record.tail_no} → {tail_no}")
                            existing_record.tail_no = tail_no
                            updated = True
                        
                        # Update scheduled times (reschedule)
                        if existing_record.std_utc != std_utc:
                            logger.info(f"Updating STD: {existing_record.std_utc} → {std_utc}")
                            existing_record.std_utc = std_utc
                            updated = True
                            
                        if existing_record.sta_utc != sta_utc:
                            logger.info(f"Updating STA: {existing_record.sta_utc} → {sta_utc}")
                            existing_record.sta_utc = sta_utc
                            updated = True
                        
                        # Update scheduled arrival date
                        if sa_date_utc and existing_record.sa_date_utc != sa_date_utc:
                            logger.info(f"Updating SA date: {existing_record.sa_date_utc} → {sa_date_utc}")
                            existing_record.sa_date_utc = sa_date_utc
                            updated = True
                        
                        # Update IATA codes if they changed (unlikely but possible)
                        if existing_record.dep_code_iata != dep_code_iata:
                            existing_record.dep_code_iata = dep_code_iata
                            updated = True
                        if existing_record.arr_code_iata != arr_code_iata:
                            existing_record.arr_code_iata = arr_code_iata
                            updated = True
                        
                        # IMPORTANT: Only update actual times if they don't exist yet
                        # This prevents overwriting ACARS data with schedule data
                        if atd_utc and not existing_record.atd_utc:
                            logger.info(f"Setting initial ATD from schedule: {atd_utc}")
                            existing_record.atd_utc = atd_utc
                            updated = True
                        elif atd_utc and existing_record.atd_utc:
                            logger.info(f"Preserving existing ATD: {existing_record.atd_utc} (ignoring schedule ATD: {atd_utc})")
                        
                        if takeoff_utc and not existing_record.takeoff_utc:
                            logger.info(f"Setting initial takeoff from schedule: {takeoff_utc}")
                            existing_record.takeoff_utc = takeoff_utc
                            updated = True
                        elif takeoff_utc and existing_record.takeoff_utc:
                            logger.info(f"Preserving existing takeoff: {existing_record.takeoff_utc} (ignoring schedule: {takeoff_utc})")
                        
                        if touchdown_utc and not existing_record.touchdown_utc:
                            logger.info(f"Setting initial touchdown from schedule: {touchdown_utc}")
                            existing_record.touchdown_utc = touchdown_utc
                            updated = True
                        elif touchdown_utc and existing_record.touchdown_utc:
                            logger.info(f"Preserving existing touchdown: {existing_record.touchdown_utc} (ignoring schedule: {touchdown_utc})")
                        
                        if ata_utc and not existing_record.ata_utc:
                            logger.info(f"Setting initial ATA from schedule: {ata_utc}")
                            existing_record.ata_utc = ata_utc
                            updated = True
                        elif ata_utc and existing_record.ata_utc:
                            logger.info(f"Preserving existing ATA: {existing_record.ata_utc} (ignoring schedule: {ata_utc})")
                        
                        # Update raw content for audit trail
                        new_raw_content = ",".join(fields)
                        if existing_record.raw_content != new_raw_content:
                            existing_record.raw_content = new_raw_content
                            updated = True
                        
                        if updated:
                            existing_record.save()
                            logger.info(f"✅ Updated flight {flight_no} on {sd_date_utc}")
                        else:
                            logger.info(f"ℹ️  No changes needed for flight {flight_no} on {sd_date_utc}")
                    
                    else:
                        # Create a new record - this should only happen for truly new flights
                        new_flight = FlightData.objects.create(
                            flight_no=flight_no,
                            tail_no=tail_no,
                            dep_code_iata=dep_code_iata,
                            dep_code_icao=dep_code_icao,
                            arr_code_iata=arr_code_iata,
                            arr_code_icao=arr_code_icao,
                            sd_date_utc=sd_date_utc,
                            sa_date_utc=sa_date_utc,
                            std_utc=std_utc,
                            sta_utc=sta_utc,
                            atd_utc=atd_utc,
                            takeoff_utc=takeoff_utc,
                            touchdown_utc=touchdown_utc,
                            ata_utc=ata_utc,
                            source_type="FDM",
                            raw_content=",".join(fields),
                        )
                        logger.info(f"✨ Created new flight record: {flight_no} on {sd_date_utc} (ID: {new_flight.id})")

            except Exception as e:
                logger.error(f"❌ Error processing line {line_num}: {e} - {fields}", exc_info=True)
                continue

        logger.info("✅ Flight schedule file processed successfully.")

    except Exception as e:
        logger.error(f"❌ Error processing flight schedule file: {e}", exc_info=True)




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
from django.db import models
from aimsintegration.models import FlightData

logger = logging.getLogger(__name__)

# def process_acars_message(item, file_path):
#     try:
#         email_received_date = item.datetime_received.date()  # Get only the date part
#         message_body = item.body

#         if "M16" in message_body:
#             logger.info("Skipping 'M16' ACARS message.")
#             return

#         logger.info(f"ACARS message received at: {email_received_date} UTC")
#         logger.info(f"ACARS message body: {message_body}")

#         # Extract fields from the message
#         flight_no = extract_flight_number(message_body)
#         acars_event, event_time_str = extract_acars_event(message_body)
#         dep_code, arr_code = extract_departure_and_arrival_codes(message_body)
#         tail_number = extract_tail_number(message_body)

#         if not re.match(r'^\d{2}:\d{2}$', event_time_str):
#             logger.error("Invalid time format in ACARS message.")
#             return

#         event_time = datetime.strptime(event_time_str, "%H:%M").time()

#         # Define comprehensive date range for flight matching
#         # Cover all possible scenarios: early/late arrivals, delays, cross-day operations
#         search_dates = [
#             email_received_date + timedelta(days=i) 
#             for i in range(-3, 4)  # -3, -2, -1, 0, 1, 2, 3 days
#         ]

#         # First, try to get matching flights with flight number and expanded date range
#         flights = FlightData.objects.filter(
#             flight_no=flight_no,
#             tail_no=tail_number,
#             dep_code_iata=dep_code,
#             arr_code_iata=arr_code,
#             sd_date_utc__in=search_dates
#         )

#         # If no flights found, try matching without flight number but with date range
#         if not flights.exists():
#             flights = FlightData.objects.filter(
#                 tail_no=tail_number,
#                 dep_code_iata=dep_code,
#                 arr_code_iata=arr_code,
#                 sd_date_utc__in=search_dates
#             )

#         # If still no flights found, send an email and return
#         if not flights.exists():
#             logger.info(f"No matching flights found for flight number: {flight_no}")
#             send_mail(
#                 subject=f"No matching flights found for flight number: {flight_no}",
#                 message=(
#                     f"Dear Team,\n\n"
#                     f"The ACARS message for flight {flight_no} could not be matched.\n"
#                     f"Message details:\n\n{message_body}\n\n"
#                     f"ACARS received date: {email_received_date}\n"
#                     f"Flight details: {flight_no}, Tail: {tail_number}, Route: {dep_code}-{arr_code}\n"
#                     f"ACARS Event: {acars_event}\n\n"
#                     f"Please review and update manually.\n\n"
#                     f"Regards,\nFlightOps Team"
#                 ),
#                 from_email=settings.EMAIL_HOST_USER,
#                 recipient_list=[
#                     settings.FIRST_EMAIL_RECEIVER,
#                     settings.SECOND_EMAIL_RECEIVER,
#                     settings.THIRD_EMAIL_RECEIVER,
#                 ],
#                 fail_silently=False,
#             )
#             return

#         # Comprehensive flight selection logic covering all scenarios
#         selected_flight = select_best_flight_match(flights, acars_event, email_received_date)

#         if not selected_flight:
#             logger.error("No flight could be selected for update")
#             return

#         logger.info(f"Selected flight: {selected_flight.flight_no} ({selected_flight.tail_no}) on {selected_flight.sd_date_utc} for {acars_event} event")

#         # Update the appropriate field based on ACARS event
#         if acars_event == "OT":
#             selected_flight.atd_utc = event_time
#             logger.info(f"Updated ATD to {event_time}")
#         elif acars_event == "OF":
#             selected_flight.takeoff_utc = event_time
#             logger.info(f"Updated takeoff to {event_time}")
#         elif acars_event == "ON":
#             selected_flight.touchdown_utc = event_time
#             logger.info(f"Updated touchdown to {event_time}")
#         elif acars_event == "IN":
#             selected_flight.ata_utc = event_time
#             logger.info(f"Updated ATA to {event_time}")

#         selected_flight.save()

#         # Append the updated flight details to the job file
#         write_job_one_row(file_path, selected_flight, acars_event, event_time, email_received_date)

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

        # FIXED: Only search yesterday and today (2 days only)
        # ACARS should never update flights that haven't happened yet
        search_dates = [
            email_received_date + timedelta(days=i) 
            for i in range(-1, 1)  # -1, 0 days (yesterday + today only)
        ]

        logger.info(f"Searching flights from {search_dates[0]} to {search_dates[-1]} for ACARS event {acars_event}")

        # First, try to get matching flights with flight number and date range
        flights = FlightData.objects.filter(
            flight_no=flight_no,
            tail_no=tail_number,
            dep_code_iata=dep_code,
            arr_code_iata=arr_code,
            sd_date_utc__in=search_dates  # Now only includes yesterday + today
        )

        # If no flights found, try matching without flight number but with date range
        if not flights.exists():
            flights = FlightData.objects.filter(
                tail_number=tail_number,
                dep_code_iata=dep_code,
                arr_code_iata=arr_code,
                sd_date_utc__in=search_dates
            )

        # Additional safety check: Never update flights scheduled for future dates
        if flights.exists():
            # Filter out any flights scheduled for future dates as an extra safety measure
            flights = flights.filter(sd_date_utc__lte=email_received_date)
            
        if not flights.exists():
            logger.info(f"No matching YESTERDAY/TODAY flights found for flight number: {flight_no}")
            send_mail(
                subject=f"No matching flights found for flight number: {flight_no}",
                message=(
                    f"Dear Team,\n\n"
                    f"The ACARS message for flight {flight_no} could not be matched to any yesterday or today flights.\n"
                    f"Message details:\n\n{message_body}\n\n"
                    f"ACARS received date: {email_received_date}\n"
                    f"Flight details: {flight_no}, Tail: {tail_number}, Route: {dep_code}-{arr_code}\n"
                    f"ACARS Event: {acars_event}\n"
                    f"Search range: {search_dates[0]} to {search_dates[-1]} (yesterday and today only)\n\n"
                    f"Note: ACARS only processes flights from yesterday and today, not future flights.\n\n"
                    f"Please review and update manually if needed.\n\n"
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

        # Comprehensive flight selection logic covering all scenarios
        selected_flight = select_best_flight_match(flights, acars_event, email_received_date)

        if not selected_flight:
            logger.error("No flight could be selected for update")
            return

        # Additional verification: Ensure we're not updating a future flight
        if selected_flight.sd_date_utc > email_received_date:
            logger.error(f"SAFETY CHECK FAILED: Attempted to update future flight {selected_flight.flight_no} "
                        f"scheduled for {selected_flight.sd_date_utc} with ACARS received on {email_received_date}")
            return

        logger.info(f"Selected flight: {selected_flight.flight_no} ({selected_flight.tail_no}) on {selected_flight.sd_date_utc} for {acars_event} event")

        # Update the appropriate field based on ACARS event
        if acars_event == "OT":
            selected_flight.atd_utc = event_time
            logger.info(f"Updated ATD to {event_time}")
        elif acars_event == "OF":
            selected_flight.takeoff_utc = event_time
            logger.info(f"Updated takeoff to {event_time}")
        elif acars_event == "ON":
            selected_flight.touchdown_utc = event_time
            logger.info(f"Updated touchdown to {event_time}")
        elif acars_event == "IN":
            selected_flight.ata_utc = event_time
            logger.info(f"Updated ATA to {event_time}")

        selected_flight.save()

        # Append the updated flight details to the job file
        write_job_one_row(file_path, selected_flight, acars_event, event_time, email_received_date)

    except Exception as e:
        logger.error(f"Error processing ACARS message: {e}", exc_info=True)


def select_best_flight_match(flights, acars_event, email_received_date):
    """
    Comprehensive flight selection logic covering ALL possible scenarios:
    
    Departure Events (OT, OF):
    - OT early, OF same day
    - OT late night, OF next day
    - OF received before OT
    - Multiple flights same day
    
    Arrival Events (ON, IN):
    - Same day arrival
    - Next day arrival (long flights)
    - ON/IN on different days
    - Late arrival ACARS
    
    Mixed Scenarios:
    - Out-of-order ACARS reception
    - Multiple flights with same tail/route
    - Delayed/cancelled flights
    """
    
    logger.info(f"Selecting best flight from {flights.count()} candidates for {acars_event} event")
    
    if acars_event == "OT":  # Pushback from gate
        return select_flight_for_ot(flights, email_received_date)
    
    elif acars_event == "OF":  # Takeoff
        return select_flight_for_of(flights, email_received_date)
    
    elif acars_event == "ON":  # Landing
        return select_flight_for_on(flights, email_received_date)
    
    elif acars_event == "IN":  # Arrival at gate
        return select_flight_for_in(flights, email_received_date)
    
    return None


def select_flight_for_ot(flights, email_received_date):
    """Select flight for OT (pushback) event"""
    
    # Priority 1: Flights without any departure data (fresh flight)
    fresh_flights = flights.filter(
        atd_utc__isnull=True,
        takeoff_utc__isnull=True
    )
    
    if fresh_flights.exists():
        logger.info(f"Found {fresh_flights.count()} fresh flights for OT")
        # Pick the closest scheduled date to email received date
        return min(fresh_flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))
    
    # Priority 2: Flights with only takeoff data (OF received before OT scenario)
    flights_with_only_of = flights.filter(
        atd_utc__isnull=True,
        takeoff_utc__isnull=False
    )
    
    if flights_with_only_of.exists():
        logger.info(f"Found {flights_with_only_of.count()} flights with only OF data - updating same flight")
        return flights_with_only_of.first()
    
    # Priority 3: Fall back to closest flight by date
    logger.warning("All flights have OT data, selecting closest by date")
    return min(flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))


def select_flight_for_of(flights, email_received_date):
    """Select flight for OF (takeoff) event"""
    
    # Priority 1: Flights that already have OT but no OF (normal sequence)
    flights_with_ot_only = flights.filter(
        atd_utc__isnull=False,
        takeoff_utc__isnull=True
    )
    
    if flights_with_ot_only.exists():
        logger.info(f"Found {flights_with_ot_only.count()} flights with OT but no OF - normal sequence")
        return flights_with_ot_only.first()
    
    # Priority 2: Fresh flights without any departure data (OF before OT scenario)
    fresh_flights = flights.filter(
        atd_utc__isnull=True,
        takeoff_utc__isnull=True
    )
    
    if fresh_flights.exists():
        logger.info(f"Found {fresh_flights.count()} fresh flights for OF (OF before OT scenario)")
        return min(fresh_flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))
    
    # Priority 3: Fall back to closest flight by date
    logger.warning("Complex OF scenario, selecting closest by date")
    return min(flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))


def select_flight_for_on(flights, email_received_date):
    """Select flight for ON (landing) event"""
    
    # Priority 1: Flights with complete departure data (OT and OF)
    flights_with_complete_departure = flights.filter(
        atd_utc__isnull=False,
        takeoff_utc__isnull=False,
        touchdown_utc__isnull=True
    )
    
    if flights_with_complete_departure.exists():
        logger.info(f"Found {flights_with_complete_departure.count()} flights with complete departure data")
        return flights_with_complete_departure.first()
    
    # Priority 2: Flights with partial departure data (either OT or OF)
    flights_with_partial_departure = flights.filter(
        models.Q(atd_utc__isnull=False) | models.Q(takeoff_utc__isnull=False),
        touchdown_utc__isnull=True
    )
    
    if flights_with_partial_departure.exists():
        logger.info(f"Found {flights_with_partial_departure.count()} flights with partial departure data")
        return flights_with_partial_departure.first()
    
    # Priority 3: Flights with only IN data (ON before IN scenario)
    flights_with_only_in = flights.filter(
        atd_utc__isnull=True,
        takeoff_utc__isnull=True,
        touchdown_utc__isnull=True,
        ata_utc__isnull=False
    )
    
    if flights_with_only_in.exists():
        logger.info(f"Found {flights_with_only_in.count()} flights with only IN data - ON before IN scenario")
        return flights_with_only_in.first()
    
    # Priority 4: Fresh flights (unusual but possible)
    fresh_flights = flights.filter(
        atd_utc__isnull=True,
        takeoff_utc__isnull=True,
        touchdown_utc__isnull=True,
        ata_utc__isnull=True
    )
    
    if fresh_flights.exists():
        logger.warning(f"Found {fresh_flights.count()} fresh flights for ON (unusual scenario)")
        return min(fresh_flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))
    
    # Priority 5: Fall back to closest flight by date
    logger.warning("Complex ON scenario, selecting closest by date")
    return min(flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))


def select_flight_for_in(flights, email_received_date):
    """Select flight for IN (gate arrival) event"""
    
    # Priority 1: Flights with landing data but no gate arrival
    flights_with_on_only = flights.filter(
        touchdown_utc__isnull=False,
        ata_utc__isnull=True
    )
    
    if flights_with_on_only.exists():
        logger.info(f"Found {flights_with_on_only.count()} flights with ON but no IN - normal sequence")
        return flights_with_on_only.first()
    
    # Priority 2: Flights with departure data but no arrival data
    flights_with_departure_only = flights.filter(
        models.Q(atd_utc__isnull=False) | models.Q(takeoff_utc__isnull=False),
        touchdown_utc__isnull=True,
        ata_utc__isnull=True
    )
    
    if flights_with_departure_only.exists():
        logger.info(f"Found {flights_with_departure_only.count()} flights with departure but no arrival data")
        return flights_with_departure_only.first()
    
    # Priority 3: Fresh flights (IN before ON scenario)
    fresh_flights = flights.filter(
        atd_utc__isnull=True,
        takeoff_utc__isnull=True,
        touchdown_utc__isnull=True,
        ata_utc__isnull=True
    )
    
    if fresh_flights.exists():
        logger.warning(f"Found {fresh_flights.count()} fresh flights for IN (IN before ON scenario)")
        return min(fresh_flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))
    
    # Priority 4: Fall back to closest flight by date
    logger.warning("Complex IN scenario, selecting closest by date")
    return min(flights, key=lambda f: abs((f.sd_date_utc - email_received_date).days))



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

# from datetime import datetime
# from .models import FdmFlightData, AirportData, FlightData
# import logging
# import csv

# logger = logging.getLogger(__name__)

# def process_fdm_flight_schedule_file(attachment):
#     """
#     Process the FDM flight schedule file using a comma delimiter.
#     Insert into FdmFlightData and update missing actual timings in FlightData.
#     """
#     try:
#         content = attachment.content.decode('utf-8').splitlines()
#         logger.info("Starting to process the FDM flight schedule file...")

#         reader = csv.reader(content)

#         for line_num, row in enumerate(reader, start=1):
#             try:
#                 # Extract fields
#                 flight_date = row[0].strip() if len(row) > 0 else ""
#                 tail_no = row[1].strip()[:10] if len(row) > 1 else ""
#                 flight_no = row[2].strip()[:6] if len(row) > 2 else ""
#                 dep_code_icao = row[3].strip()[:4] if len(row) > 3 else ""
#                 arr_code_icao = row[4].strip()[:4] if len(row) > 4 else ""
#                 std_utc = row[5].strip() if len(row) > 5 else ""
#                 sta_utc = row[6].strip() if len(row) > 6 else ""
#                 flight_type = row[7].strip()[:10] if len(row) > 7 else ""
#                 etd_utc = row[8].strip() if len(row) > 8 else ""
#                 eta_utc = row[9].strip() if len(row) > 9 else ""
#                 atd_utc = row[10].strip() if len(row) > 10 else ""
#                 takeoff_utc = row[11].strip() if len(row) > 11 else ""
#                 touchdown_utc = row[12].strip() if len(row) > 12 else ""
#                 ata_utc = row[13].strip() if len(row) > 13 else ""
#                 arrival_date = row[14].strip() if len(row) > 14 else ""

#                 # Parse dates and times
#                 sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date() if flight_date else None
#                 sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
#                 std_utc_time = datetime.strptime(std_utc, "%H:%M").time() if std_utc else None
#                 sta_utc_time = datetime.strptime(sta_utc, "%H:%M").time() if sta_utc else None
#                 etd_utc_time = datetime.strptime(etd_utc, "%H:%M").time() if etd_utc else None
#                 eta_utc_time = datetime.strptime(eta_utc, "%H:%M").time() if eta_utc else None
#                 atd_utc_time = datetime.strptime(atd_utc, "%H:%M").time() if atd_utc else None
#                 takeoff_utc_time = datetime.strptime(takeoff_utc, "%H:%M").time() if takeoff_utc else None
#                 touchdown_utc_time = datetime.strptime(touchdown_utc, "%H:%M").time() if touchdown_utc else None
#                 ata_utc_time = datetime.strptime(ata_utc, "%H:%M").time() if ata_utc else None

#                 # Fetch airport data
#                 dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
#                 arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

#                 dep_code_iata = dep_airport.iata_code if dep_airport else ""
#                 arr_code_iata = arr_airport.iata_code if arr_airport else ""

#                 # Define unique criteria
#                 unique_criteria = {
#                     'flight_no': flight_no,
#                     'tail_no': tail_no,
#                     'sd_date_utc': sd_date_utc,
#                     'dep_code_icao': dep_code_icao,
#                     'arr_code_icao': arr_code_icao,
#                     'sa_date_utc': sa_date_utc,
#                     'std_utc': std_utc_time,
#                     'sta_utc': sta_utc_time,
#                 }

#                 # Insert or Update FDM FlightData
#                 existing_record = FdmFlightData.objects.filter(**unique_criteria).first()

#                 if existing_record:
#                     # Update fields if actual timings have changed
#                     updated = False
#                     if std_utc_time and existing_record.std_utc != std_utc_time:
#                         existing_record.std_utc = std_utc_time
#                         updated = True
#                     if sta_utc_time and existing_record.sta_utc != sta_utc_time:
#                         existing_record.sta_utc = sta_utc_time
#                         updated = True

#                     if atd_utc_time and existing_record.atd_utc != atd_utc_time:
#                         existing_record.atd_utc = atd_utc_time
#                         updated = True
#                     if takeoff_utc_time and existing_record.takeoff_utc != takeoff_utc_time:
#                         existing_record.takeoff_utc = takeoff_utc_time
#                         updated = True
#                     if touchdown_utc_time and existing_record.touchdown_utc != touchdown_utc_time:
#                         existing_record.touchdown_utc = touchdown_utc_time
#                         updated = True
#                     if ata_utc_time and existing_record.ata_utc != ata_utc_time:
#                         existing_record.ata_utc = ata_utc_time
#                         updated = True
#                     if etd_utc_time and existing_record.etd_utc != etd_utc_time:
#                         existing_record.etd_utc = etd_utc_time
#                         updated = True
#                     if eta_utc_time and existing_record.eta_utc != eta_utc_time:
#                         existing_record.eta_utc = eta_utc_time
#                         updated = True

#                     if flight_type and existing_record.flight_type != flight_type:
#                         existing_record.flight_type = flight_type
#                         updated = True

#                     if updated:
#                         existing_record.save()
#                         logger.info(f"Updated FDM record for flight {flight_no} on {sd_date_utc}.")
#                     else:
#                         logger.info(f"No changes for FDM record {flight_no} on {sd_date_utc}.")
#                 else:
#                     # Create a new FdmFlightData record
#                     FdmFlightData.objects.create(
#                         flight_no=flight_no,
#                         tail_no=tail_no,
#                         dep_code_iata=dep_code_iata,
#                         dep_code_icao=dep_code_icao,
#                         arr_code_iata=arr_code_iata,
#                         arr_code_icao=arr_code_icao,
#                         sd_date_utc=sd_date_utc,
#                         std_utc=std_utc_time,
#                         sta_utc=sta_utc_time,
#                         sa_date_utc=sa_date_utc,
#                         flight_type=flight_type,
#                         etd_utc=etd_utc_time,
#                         eta_utc=eta_utc_time,
#                         atd_utc=atd_utc_time,
#                         takeoff_utc=takeoff_utc_time,
#                         touchdown_utc=touchdown_utc_time,
#                         ata_utc=ata_utc_time,
#                         raw_content=','.join(row)
#                     )
#                     logger.info(f"Created new FDM flight record: {flight_no} on {sd_date_utc}.")

#             except Exception as e:
#                 logger.error(f"Error processing line {line_num}: {e} - {row}", exc_info=True)
#                 continue

#         logger.info("FDM flight schedule file processed successfully.")

#     except Exception as e:
#         logger.error(f"Error processing FDM flight schedule file: {e}", exc_info=True)


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

                # Parse dates and times - FIXED DATE FORMAT
                # Changed from "%m/%d/%Y" to "%m%d%Y" for flight_date to match CSV format
                sd_date_utc = datetime.strptime(flight_date, "%m%d%Y").date() if flight_date else None
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






import re
import pandas as pd
from datetime import datetime
from .models import CrewMember

def process_crew_details_file(attachment):
    """
    Parse and process crew details from an unstructured file,
    but ONLY store records for CP and FO (including 'D'-prefixed IDs).
    Overwrites any existing matching records instead of updating.
    """
    try:
        raw_content = attachment.content.decode('utf-8').splitlines()
        rows = [line.strip() for line in raw_content if line.strip()]  # Remove empty lines

        parsed_data = []
        roles_we_care_about = {'CP', 'FO'}

        # Regex: find any role + optional 'D' + 8 digits + name, up until the next role or end of line
        crew_pattern = re.compile(
            r'(?P<role>(?<!\S)(?:CP|FO|FP|SA|FA|FE|AC)(?!\S))\s+'
            r'(?P<crew_id>D?\d{8})'  # optional 'D' plus 8 digits
            r'(?P<name>.*?)'        # non-greedy for name
            r'(?=(?<!\S)(?:CP|FO|FP|SA|FA|FE|AC)(?!\S)|$)',
            re.DOTALL
        )

        for line_num, line in enumerate(rows, start=1):
            try:
                # Extract flight details
                flight_no       = line[:4].strip()
                flight_date_str = line[4:13].strip()
                origin          = line[13:17].strip()
                destination     = line[17:20].strip()

                try:
                    sd_date_utc = datetime.strptime(flight_date_str, "%d%m%Y").date()
                except ValueError:
                    raise ValueError(f"Invalid date format: '{flight_date_str}'")

                flight_context = {
                    "flight_no": flight_no,
                    "sd_date_utc": sd_date_utc,
                    "origin": origin,
                    "destination": destination,
                }

                # Get crew data
                crew_data = line[20:].strip()

                # Remove consecutive repeated roles like 'CP CP'
                crew_data = re.sub(r'\b(CP|FO|FP|SA|FA|FE|AC)\b\s+\1', r'\1', crew_data)

                # Find all matches
                for m in crew_pattern.finditer(crew_data):
                    role    = m.group("role")
                    crew_id = m.group("crew_id")
                    name    = m.group("name").strip()

                    if role in roles_we_care_about:
                        parsed_data.append({
                            **flight_context,
                            "role": role,
                            "crew_id": crew_id,
                            "name": name,
                        })

            except ValueError as ve:
                print(f"Error in crew data on line {line_num}: {ve}")
            except Exception as e:
                print(f"Error processing line {line_num}: {e}")

        # Convert parsed data to DataFrame
        if not parsed_data:
            print("No valid data extracted.")
            return

        crew_df = pd.DataFrame(parsed_data)

        # Delete existing records for flights before inserting new ones
        for (flight_no, sd_date_utc, origin, destination) in crew_df[['flight_no', 'sd_date_utc', 'origin', 'destination']].drop_duplicates().values:
            try:
                CrewMember.objects.filter(
                    flight_no=flight_no,
                    sd_date_utc=sd_date_utc,
                    origin=origin,
                    destination=destination
                ).delete()
                print(f"Deleted old records for Flight {flight_no} on {sd_date_utc}")
            except Exception as e:
                print(f"Error deleting records for Flight {flight_no}: {e}")

        # Insert new records
        for _, row in crew_df.iterrows():
            try:
                CrewMember.objects.create(
                    flight_no=row["flight_no"],
                    sd_date_utc=row["sd_date_utc"],
                    origin=row["origin"],
                    destination=row["destination"],
                    crew_id=row["crew_id"],
                    role=row["role"],
                    name=row["name"]
                )
                print(f"Inserted Crew: {row['crew_id']} - {row['name']} ({row['role']})")
            except Exception as e:
                print(f"Database error for {row['crew_id']}: {e}")

        print("Crew details file processed successfully.")

    except Exception as e:
        print(f"Error processing crew details file: {e}")







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




# # Tableau project
# from .models import TableauData
# from datetime import datetime, time
# import logging

# logger = logging.getLogger(__name__)

# def process_tableau_data_file(attachment):
#     """
#     Process the tableau file, ensuring proper parsing of all fields based on the dataset structure.
#     """
#     try:
#         content = attachment.content.decode('utf-8').splitlines()
#         logger.info("Starting to process the Tableau file...")

#         def parse_date(value, field_name):
#             value = value.strip()
#             if not value or value in ["0000", "00000000", None]:
#                 logger.warning(f"{field_name} is empty or invalid. Defaulting to None.")
#                 return None  # Default to None
#             try:
#                 return datetime.strptime(value, "%d%m%Y").date()
#             except (ValueError, TypeError):
#                 logger.warning(f"Invalid {field_name}: {value}. Defaulting to None.")
#                 return None

#         def parse_time(value, field_name):
#             value = value.strip()
#             if not value or value == "0000":
#                 logger.warning(f"{field_name} is empty or invalid. Defaulting to None.")
#                 return None
#             try:
#                 return datetime.strptime(value, "%H%M").time()
#             except (ValueError, TypeError):
#                 logger.warning(f"Invalid {field_name}: {value}. Defaulting to None.")
#                 return None

#         def format_time(time_obj):
#             return time_obj.strftime("%H:%M") if isinstance(time_obj, time) else None

#         def parse_string(value, field_name):
#             value = value.strip()
#             if not value:
#                 logger.warning(f"{field_name} is empty or invalid. Defaulting to an empty string.")
#                 return ""
#             return value

#         for line_num, line in enumerate(content, start=1):
#             if not line.strip():
#                 continue
#             # Skip the header line that starts with "ACCONFIG,DAY,DEP"
#             if line.startswith("ACCONFIG,DAY,DEP"):
#                 continue

#             try:
#                 # Extract the first field (aircraft_config) before the 4th comma
#                 first_field_end = line.find(',', line.find(',', line.find(',', line.find(',') + 1) + 1) + 1)
#                 aircraft_config = line[:first_field_end].strip()

#                 # Extract the second field (operation_day) right after the 4th comma
#                 remaining_line = line[first_field_end + 1:].strip()
#                 operation_day_field_end = remaining_line.find(',')
#                 operation_day = parse_date(remaining_line[:operation_day_field_end].strip(), "Operation Day")

#                 # Extract the remaining fields
#                 remaining_fields = remaining_line[operation_day_field_end + 1:].split(",")
#                 remaining_fields = [field.strip() for field in remaining_fields]

#                 # Parse the fields
#                 departure_station = remaining_fields[0]
#                 flight_no = remaining_fields[1]
#                 flight_leg_code = remaining_fields[2] if len(remaining_fields) > 2 else " "
#                 cancelled_deleted = remaining_fields[3]
#                 arrival_station = remaining_fields[4]
#                 aircraft_reg_id = remaining_fields[5]
#                 aircraft_type_index = remaining_fields[6] or None
#                 aircraft_category = remaining_fields[7] or None
#                 flight_service_type = remaining_fields[8]
#                 std = parse_time(remaining_fields[9], "STD")
#                 sta = parse_time(remaining_fields[10], "STA")
#                 original_operation_day = parse_date(remaining_fields[11], "Original Operation Day") if remaining_fields[11] != "0000" else " "
#                 original_std = parse_time(remaining_fields[12], "Original STD") if remaining_fields[12] != "0000" else time(0, 0)
#                 original_sta = parse_time(remaining_fields[13], "Original STA") if remaining_fields[13] != "0000" else time(0, 0)
#                 departure_delay_time = parse_string(remaining_fields[14], "Departure Delay Time")
#                 delay_code_kind = parse_string(remaining_fields[15], "Delay Code Kind")
#                 delay_number = parse_string(remaining_fields[16], "Delay Number") if len(remaining_fields) > 16 else ""
#                 seat_type_config = parse_string(remaining_fields[17], "Seat Type Config") if len(remaining_fields) > 17 else ""
#                 atd = parse_time(remaining_fields[18], "ATD") if len(remaining_fields) > 18 else None
#                 takeoff = parse_time(remaining_fields[19], "Takeoff") if len(remaining_fields) > 19 else None
#                 touchdown = parse_time(remaining_fields[20], "Touchdown") if len(remaining_fields) > 20 else None
#                 ata = parse_time(remaining_fields[21], "ATA") if len(remaining_fields) > 21 else None

#                 print("\n=======================================================")
#                 print(f"\nAircraft Config: {aircraft_config}\nOperation Day: {operation_day}\nDeparture Station: {departure_station}\nFlight No: {flight_no}\nFlight Leg Code: {flight_leg_code}\nCancelled/Deleted: {cancelled_deleted}\nArrival Station: {arrival_station}\nAircraft Reg ID: {aircraft_reg_id}\nAircraft Type Index: {aircraft_type_index}\nAircraft Category: {aircraft_category}\nFlight Service Type: {flight_service_type}\nSTD: {format_time(std)}\nSTA: {format_time(sta)}\nOriginal Operation Day: {original_operation_day}\nOriginal STD: {format_time(original_std)}\nOriginal STA: {format_time(original_sta)}\nDeparture Delay Time: {departure_delay_time}\nDelay Code Kind: {delay_code_kind}\nDelay Number: {delay_number}\nSeat Type Config: {seat_type_config}\nATD: {format_time(atd)}\nTakeoff: {format_time(takeoff)}\nTouchdown: {format_time(touchdown)}\nATA: {format_time(ata)}")
#                 print()

#                 # Database operations
#                 unique_criteria = {
#                     'operation_day': operation_day,
#                     'departure_station': departure_station,
#                     'flight_no': flight_no,
#                     'arrival_station': arrival_station,
#                     'flight_leg_code': flight_leg_code,
#                 }

#                 existing_record = TableauData.objects.filter(**unique_criteria).first()

#                 if existing_record:
#                     updated = False
#                     fields_to_update = {
#                         'cancelled_deleted': cancelled_deleted,
#                         'aircraft_reg_id': aircraft_reg_id,
#                         'aircraft_type_index': aircraft_type_index,
#                         'aircraft_category': aircraft_category,
#                         'flight_service_type': flight_service_type,
#                         'std': std,
#                         'sta': sta,
#                         'original_operation_day': original_operation_day,
#                         'original_std': original_std,
#                         'original_sta': original_sta,
#                         'departure_delay_time': departure_delay_time,
#                         'atd': atd,
#                         'takeoff': takeoff,
#                         'touchdown': touchdown,
#                         'ata': ata,
#                         'delay_code_kind': delay_code_kind,
#                         'delay_number': delay_number,
#                         'aircraft_config': aircraft_config,
#                         'seat_type_config': seat_type_config,
#                     }

#                     for field, new_value in fields_to_update.items():
#                         if getattr(existing_record, field, None) != new_value:
#                             setattr(existing_record, field, new_value)
#                             updated = True

#                     if updated:
#                         existing_record.save()
#                         logger.info(f"Updated record for flight {flight_no} on {operation_day}.")
#                     else:
#                         logger.info(f"No changes detected for flight {flight_no} on {operation_day}.")
#                 else:
#                     TableauData.objects.create(
#                         operation_day=operation_day,
#                         departure_station=departure_station,
#                         flight_no=flight_no,
#                         flight_leg_code=flight_leg_code,
#                         cancelled_deleted=cancelled_deleted,
#                         arrival_station=arrival_station,
#                         aircraft_reg_id=aircraft_reg_id,
#                         aircraft_type_index=aircraft_type_index,
#                         aircraft_category=aircraft_category,
#                         flight_service_type=flight_service_type,
#                         std=format_time(std),
#                         sta=format_time(sta),
#                         original_operation_day=original_operation_day,
#                         original_std=format_time(original_std),
#                         original_sta=format_time(original_sta),
#                         departure_delay_time=departure_delay_time,
#                         atd=format_time(atd),
#                         takeoff=format_time(takeoff),
#                         touchdown=format_time(touchdown),
#                         ata=format_time(ata),
#                         delay_code_kind=delay_code_kind,
#                         delay_number=delay_number,
#                         aircraft_config=aircraft_config,
#                         seat_type_config=seat_type_config
#                     )
#                     logger.info(f"Created new record for flight {flight_no} on {operation_day}.")
#             except Exception as e:
#                 logger.error(f"Error processing line {line_num}: {line}\n{e}")
#                 continue

#         logger.info("Tableau data file processed successfully.")

#     except Exception as e:
#         logger.error(f"Error processing tableau data file: {e}")



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
                return None
            try:
                # Try DD/MM/YYYY format first (like "07/05/2025")
                if "/" in value:
                    return datetime.strptime(value, "%d/%m/%Y").date()
                # Fall back to DDMMYYYY format (like "07052025")
                else:
                    return datetime.strptime(value, "%d%m%Y").date()
            except (ValueError, TypeError):
                logger.warning(f"Invalid {field_name}: {value}. Defaulting to None.")
                return None

        def parse_time(value, field_name):
            value = value.strip()
            if not value or value in ["0000", "00:00"]:
                logger.warning(f"{field_name} is empty or invalid. Defaulting to None.")
                return None
            try:
                # Try HH:MM format first (like "13:20")
                if ":" in value:
                    return datetime.strptime(value, "%H:%M").time()
                # Fall back to HHMM format (like "1320")
                else:
                    return datetime.strptime(value, "%H%M").time()
            except (ValueError, TypeError):
                logger.warning(f"Invalid {field_name}: {value}. Defaulting to None.")
                return None

        def format_time(time_obj):
            return time_obj.strftime("%H:%M") if isinstance(time_obj, time) else None

        def parse_delay_time(value, field_name):
            """Parse delay time and convert to minutes (integer)"""
            value = value.strip()
            if not value or value in ["0000", "00:00", ""]:
                return None
            try:
                # Parse HH:MM format and convert to total minutes
                if ":" in value:
                    hours, minutes = map(int, value.split(":"))
                    return hours * 60 + minutes
                # If it's just a number, assume it's already minutes
                else:
                    return int(value) if value.isdigit() else None
            except (ValueError, TypeError):
                logger.warning(f"Invalid {field_name}: {value}. Defaulting to None.")
                return None

        def parse_boolean(value, field_name):
            """Parse boolean field - convert string to boolean"""
            value = value.strip()
            if value in ["1", "True", "true", "YES", "yes"]:
                return True
            elif value in ["0", "False", "false", "NO", "no", ""]:
                return False
            else:
                logger.warning(f"Invalid {field_name}: {value}. Defaulting to False.")
                return False

        def parse_string(value, field_name):
            value = value.strip()
            if not value:
                logger.warning(f"{field_name} is empty or invalid. Defaulting to an empty string.")
                return ""
            return value

        for line_num, line in enumerate(content, start=1):
            if not line.strip():
                continue
            # Skip the header line that starts with "ACCONFIG,DAY,DEP"
            if line.startswith("ACCONFIG,DAY,DEP"):
                continue

            try:
                # SIMPLE CSV PARSING - split all fields directly
                fields = line.split(',')
                fields = [field.strip() for field in fields]
                
                if len(fields) < 29:  # Ensure we have enough fields
                    logger.warning(f"Line {line_num} has insufficient fields ({len(fields)}). Skipping.")
                    continue

                # Parse the fields directly from the CSV (CORRECTED INDICES)
                aircraft_config = fields[0]
                operation_day = parse_date(fields[1], "Operation Day")
                departure_station = fields[2]
                flight_no = fields[3]
                flight_leg_code = fields[4] if fields[4] else " "
                cancelled_deleted = parse_boolean(fields[5], "Cancelled/Deleted")  # FIXED: Convert to boolean
                arrival_station = fields[6]
                aircraft_reg_id = fields[7]
                aircraft_type_index = fields[8] or None
                aircraft_category = fields[9] or None
                flight_service_type = fields[10]
                std = parse_time(fields[11], "STD")
                sta = parse_time(fields[12], "STA")
                original_operation_day = parse_date(fields[13], "Original Operation Day") if fields[13] and fields[13] not in ["0000", ""] else None
                original_std = parse_time(fields[14], "Original STD") if fields[14] and fields[14] not in ["0000", "00:00", ""] else None
                original_sta = parse_time(fields[15], "Original STA") if fields[15] and fields[15] not in ["0000", "00:00", ""] else None
                departure_delay_time = parse_delay_time(fields[16], "Departure Delay Time")
                delay_code_kind = parse_string(fields[17], "Delay Code Kind")
                # Fields 18-24 are unused in this format
                # Actual times are at the end
                atd = parse_time(fields[25], "ATD") if len(fields) > 25 and fields[25] else None
                takeoff = parse_time(fields[26], "Takeoff") if len(fields) > 26 and fields[26] else None
                touchdown = parse_time(fields[27], "Touchdown") if len(fields) > 27 and fields[27] else None
                ata = parse_time(fields[28], "ATA") if len(fields) > 28 and fields[28] else None

                print("\n=======================================================")
                print(f"\nAircraft Config: {aircraft_config}\nOperation Day: {operation_day}\nDeparture Station: {departure_station}\nFlight No: {flight_no}\nFlight Leg Code: {flight_leg_code}\nCancelled/Deleted: {cancelled_deleted}\nArrival Station: {arrival_station}\nAircraft Reg ID: {aircraft_reg_id}\nAircraft Type Index: {aircraft_type_index}\nAircraft Category: {aircraft_category}\nFlight Service Type: {flight_service_type}\nSTD: {format_time(std)}\nSTA: {format_time(sta)}\nOriginal Operation Day: {original_operation_day}\nOriginal STD: {format_time(original_std)}\nOriginal STA: {format_time(original_sta)}\nDeparture Delay Time: {departure_delay_time}\nDelay Code Kind: {delay_code_kind}\nATD: {format_time(atd)}\nTakeoff: {format_time(takeoff)}\nTouchdown: {format_time(touchdown)}\nATA: {format_time(ata)}")
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
                        'aircraft_config': aircraft_config,
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
                        aircraft_config=aircraft_config
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






#=================================================================================

# QATAR APIS UTILS 

#==================================================================================
#=================================================================================
# QATAR APIS UTILS - UPDATED VERSION FOR RWANDAIR TO DOHA (3-LETTER CODES)
#==================================================================================

import re
import csv
import os
from datetime import datetime, timedelta
from django.conf import settings
from django.db import models
from django.utils import timezone
from exchangelib import FileAttachment, Credentials, Configuration, Account
from .models import QatarCrewBasic, QatarCrewDetailed, QatarApisRecord, FlightData
import logging

logger = logging.getLogger(__name__)


def get_exchange_account():
    """Get the Exchange account for email processing"""
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


def get_filtered_emails(account, start_date, limit=100):
    """
    Get filtered emails with compatibility for different exchangelib versions
    """
    try:
        # Try newer exchangelib syntax first
        emails = list(
            account.inbox.only('subject', 'datetime_received', 'has_attachments')
            .filter(datetime_received__gte=start_date)
            .order_by('-datetime_received')[:limit]
        )
        logger.info(f"Using newer exchangelib syntax - found {len(emails)} emails")
        return emails
    except AttributeError:
        # Fallback to older exchangelib syntax
        logger.info("Using fallback method for email fetching (older exchangelib version)")
        try:
            all_emails = list(account.inbox.all().order_by('-datetime_received')[:limit * 2])
            filtered_emails = [e for e in all_emails if e.datetime_received >= start_date][:limit]
            logger.info(f"Using older exchangelib syntax - found {len(filtered_emails)} emails")
            return filtered_emails
        except Exception as e:
            logger.error(f"Error with fallback email fetching: {e}")
            # Last resort - get all emails and filter manually
            try:
                all_emails = list(account.inbox.all()[:limit])
                filtered_emails = [e for e in all_emails if hasattr(e, 'datetime_received') and e.datetime_received >= start_date][:limit]
                logger.info(f"Using basic email fetching - found {len(filtered_emails)} emails")
                return filtered_emails
            except Exception as e2:
                logger.error(f"All email fetching methods failed: {e2}")
                return []


def search_emails_by_subject(account, search_term, limit=10):
    """
    Search emails by subject with compatibility for different exchangelib versions
    """
    try:
        # Try newer syntax first
        emails = list(
            account.inbox.filter(subject__contains=search_term)
            .order_by('-datetime_received')[:limit]
        )
        logger.info(f"Found {len(emails)} emails with newer syntax for term '{search_term}'")
        return emails
    except AttributeError:
        # Fallback for older exchangelib versions
        logger.info(f"Using fallback search for term '{search_term}'")
        try:
            emails = []
            # Get more emails to search through
            all_emails = list(account.inbox.all().order_by('-datetime_received')[:100])
            for item in all_emails:
                if search_term.lower() in (item.subject or '').lower():
                    emails.append(item)
                    if len(emails) >= limit:
                        break
            logger.info(f"Found {len(emails)} emails with fallback search for term '{search_term}'")
            return emails
        except Exception as e:
            logger.error(f"Error in fallback email search: {e}")
            return []


def parse_job97_subject(subject: str):
    """
    Parse RwandAir JOB 97 subjects like:
      - 300/KGL DOH/28072025
      - 301/DOH KGL/29072025  
      - 300 / KGL-DOH / 28/07/2025
      - WB300/KGL DOH/28072025
    
    FIXED: Keep 3-letter IATA codes as they are (don't convert to ICAO)
    Returns: (flight_no, origin, destination, flight_date) or (None, None, None, None)
    """
    if not subject:
        return None, None, None, None

    # Normalize the subject
    s = subject.strip().upper()
    s = re.sub(r'\s+', ' ', s)  # collapse multiple spaces
    s = s.replace('-', ' ')     # treat hyphen like a space between airport codes

    # Pattern to handle 3-letter IATA codes
    # Pattern: <optional WB><flight>/<ORIGIN> <DEST>/<date>
    m = re.search(
        r'(?:WB)?(\d{2,4})\s*/\s*([A-Z]{3})\s+([A-Z]{3})\s*/\s*(\d{8}|\d{2}/\d{2}/\d{4})',
        s
    )
    
    if not m:
        # Try alternative pattern with different separators or spacing
        m = re.search(
            r'(?:WB)?(\d{2,4})\s*[/\-]\s*([A-Z]{3})\s*[/\-]?\s*([A-Z]{3})\s*[/\-]\s*(\d{8}|\d{2}/\d{2}/\d{4})',
            s
        )
    
    if not m:
        logger.error(f"Could not parse JOB 97 subject: {subject}")
        return None, None, None, None

    flight_no, origin, destination, date_str = m.groups()

    # Keep IATA codes as they are - don't convert to ICAO
    # Just validate that they are the expected routes
    valid_combinations = [
        ('KGL', 'DOH'),  # Kigali to Doha
        ('DOH', 'KGL'),  # Doha to Kigali
    ]
    
    if (origin, destination) not in valid_combinations:
        logger.debug(f"Route {origin} -> {destination} not in valid combinations: {valid_combinations}")

    # Parse date (DDMMYYYY or DD/MM/YYYY)
    try:
        if len(date_str) == 8 and date_str.isdigit():
            flight_date = datetime.strptime(date_str, "%d%m%Y").date()
        else:
            flight_date = datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError:
        logger.error(f"JOB 97 subject date parse failed: {date_str} in '{subject}'")
        return None, None, None, None

    logger.info(f"Parsed JOB 97 subject: Flight {flight_no}, {origin} -> {destination}, Date: {flight_date}")
    return flight_no, origin, destination, flight_date


def process_job97_file(attachment):
    """
    Process JOB 97 file containing basic crew information for RwandAir flights
    ENHANCED: Better parsing for various file formats and improved debugging
    """
    try:
        # Parse the subject to get flight details
        subject = attachment.parent_item.subject if hasattr(attachment, 'parent_item') else ""
        flight_no, origin, destination, flight_date = parse_job97_subject(subject)
        
        if not all([flight_no, origin, destination, flight_date]):
            logger.error(f"Could not extract flight details from subject: {subject}")
            return 0
        
        # Check if this is a valid KGL-DOH or DOH-KGL route
        valid_routes = [
            ('KGL', 'DOH'),  # KGL to DOH (IATA)
            ('DOH', 'KGL'),  # DOH to KGL (IATA)
        ]
        
        route_valid = any((origin, destination) == route for route in valid_routes)
        
        if not route_valid:
            logger.info(f"Skipping flight {flight_no} - not a KGL-DOH or DOH-KGL route ({origin} -> {destination})")
            return 0
            
        # Decode file content with better encoding handling
        try:
            content = attachment.content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                content = attachment.content.decode('latin-1')
            except UnicodeDecodeError:
                content = attachment.content.decode('cp1252')
        
        lines = content.splitlines()
        
        logger.info(f"Processing JOB 97 for flight {flight_no} from {origin} to {destination} on {flight_date}")
        logger.info(f"File has {len(lines)} lines, {len(content)} characters")
        
        # Debug: Show first few lines to understand format
        logger.info("First 5 lines of file:")
        for i, line in enumerate(lines[:5]):
            logger.info(f"  Line {i+1}: {repr(line)}")
        
        # Analyze file format
        is_csv_format = ',' in content and content.count(',') > len(lines)
        is_tab_separated = '\t' in content
        
        logger.info(f"File format detected: {'CSV' if is_csv_format else 'Tab-separated' if is_tab_separated else 'Space-separated or Fixed-width'}")
        
        processed_crew_count = 0
        
        for line_num, line in enumerate(lines, start=1):
            try:
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Skip header lines (enhanced detection)
                line_upper = line.upper()
                header_keywords = [
                    'FLIGHT', 'CREW', 'NAME', 'PASSPORT', 'ID', 'NO', 'TYPE', 
                    'GENERAL DECLARATION', 'STAFF', 'EMPLOYEE', 'POSITION', 
                    'BIRTH', 'GENDER', 'NATIONALITY', 'DATE', 'SURNAME', 'FIRSTNAME',
                    'DOCUMENT', 'NUMBER', 'EXPIRY', 'ISSUING'
                ]
                
                if any(keyword in line_upper for keyword in header_keywords):
                    logger.info(f"Skipping header line {line_num}: {line[:50]}...")
                    continue
                
                # Parse based on detected format
                if is_csv_format:
                    fields = [field.strip().strip('"') for field in line.split(',')]
                elif is_tab_separated:
                    fields = [field.strip() for field in line.split('\t')]
                else:
                    # Space-separated or fixed-width - handle multiple spaces
                    fields = [field for field in line.split() if field.strip()]
                
                if len(fields) < 2:  # Need at least some basic fields
                    logger.debug(f"Skipping line {line_num} - insufficient fields: {len(fields)}")
                    continue
                
                logger.info(f"Line {line_num}: {len(fields)} fields - {fields}")
                
                # Initialize variables for crew data
                crew_id = None
                passport_number = None
                position = None
                birth_date = None
                gender = None
                nationality_code = None
                surname = None
                given_name = None
                
                # Enhanced field detection with multiple strategies
                for i, field in enumerate(fields):
                    field = field.strip()
                    if not field or field == '-' or field.lower() == 'null':
                        continue
                    
                    # Strategy 1: Crew ID - look for patterns like staff numbers
                    if not crew_id and (
                        (field.isdigit() and len(field) >= 3) or 
                        (field.isalnum() and any(c.isdigit() for c in field) and len(field) >= 3)
                    ):
                        crew_id = field
                        logger.debug(f"  Found crew_id: {crew_id}")
                    
                    # Strategy 2: Passport number - various formats
                    elif not passport_number and (
                        (field.startswith(('PC', 'PP', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z')) and len(field) >= 6 and field.isalnum()) or
                        (len(field) >= 8 and field.isalnum())  # Generic alphanumeric passport
                    ):
                        passport_number = field
                        logger.debug(f"  Found passport: {passport_number}")
                    
                    # Strategy 3: Crew position codes
                    elif not position and field.upper() in [
                        'CP', 'FO', 'FP', 'SA', 'FA', 'AC', 'FE', 'CC', 'PIC', 'SIC', 
                        'CAPTAIN', 'PILOT', 'CREW', 'ATTENDANT', 'CR'
                    ]:
                        position = field.upper()[:10]  # Limit length
                        logger.debug(f"  Found position: {position}")
                    
                    # Strategy 4: Gender
                    elif not gender and field.upper() in ['M', 'F', 'MALE', 'FEMALE']:
                        gender = field.upper()[:1]
                        logger.debug(f"  Found gender: {gender}")
                    
                    # Strategy 5: Date parsing (birth date)
                    elif not birth_date and ('/' in field or (field.isdigit() and len(field) == 8)):
                        try:
                            if '/' in field:
                                # Try different date formats
                                for date_format in ["%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]:
                                    try:
                                        parsed_date = datetime.strptime(field, date_format).date()
                                        # Validate reasonable birth date (between 1950 and 2005)
                                        if 1950 <= parsed_date.year <= 2005:
                                            birth_date = parsed_date
                                            logger.debug(f"  Found birth_date: {birth_date}")
                                            break
                                    except ValueError:
                                        continue
                            elif len(field) == 8 and field.isdigit():
                                # DDMMYYYY or YYYYMMDD format
                                for date_format in ["%d%m%Y", "%Y%m%d"]:
                                    try:
                                        parsed_date = datetime.strptime(field, date_format).date()
                                        if 1950 <= parsed_date.year <= 2005:
                                            birth_date = parsed_date
                                            logger.debug(f"  Found birth_date: {birth_date}")
                                            break
                                    except ValueError:
                                        continue
                        except Exception:
                            pass
                    
                    # Strategy 6: Nationality/Country code
                    elif not nationality_code and len(field) == 3 and field.isalpha():
                        nationality_code = field.upper()
                        logger.debug(f"  Found nationality: {nationality_code}")
                    
                    # Strategy 7: Names (if they contain only letters and some punctuation)
                    elif field.replace(' ', '').replace('-', '').replace("'", '').replace('.', '').isalpha():
                        if not surname and len(field) > 1:
                            surname = field.upper()
                            logger.debug(f"  Found surname: {surname}")
                        elif not given_name and len(field) > 1 and field != surname:
                            given_name = field.upper()
                            logger.debug(f"  Found given_name: {given_name}")
                
                # Fallback: Try positional parsing if we're missing critical fields
                if (not crew_id or not passport_number) and len(fields) >= 2:
                    logger.debug(f"  Attempting positional parsing for line {line_num}")
                    
                    # Try to identify crew_id and passport in first few positions
                    for i, field in enumerate(fields[:5]):
                        field = field.strip()
                        if not field:
                            continue
                            
                        # First numeric or alphanumeric field could be crew_id
                        if not crew_id and (field.isdigit() or (field.isalnum() and len(field) >= 3)):
                            crew_id = field
                            logger.debug(f"  Positional crew_id: {crew_id}")
                        # Look for passport-like field
                        elif not passport_number and field.isalnum() and len(field) >= 6:
                            passport_number = field
                            logger.debug(f"  Positional passport: {passport_number}")
                
                # Generate crew_id if still missing
                if not crew_id:
                    crew_id = f"CREW_{line_num:03d}"
                    logger.warning(f"  Generated crew_id: {crew_id} for line {line_num}")
                    
                if not passport_number:
                    logger.warning(f"  No passport number found for line {line_num}, skipping")
                    continue
                    
                # Set defaults for missing fields
                if not position:
                    position = "CR"  # Default crew
                if not gender:
                    gender = "M"  # Default
                if not nationality_code:
                    nationality_code = "RWA"  # Default Rwanda
                if not birth_date:
                    birth_date = datetime(1980, 1, 1).date()  # Default date
                
                # Clean and validate fields
                crew_id = str(crew_id)[:20]
                passport_number = str(passport_number)[:20]
                position = str(position)[:10]
                gender = str(gender)[:1].upper()
                nationality_code = str(nationality_code)[:10]
                
                logger.info(f"  Final parsed data for line {line_num}:")
                logger.info(f"    Crew ID: {crew_id}")
                logger.info(f"    Passport: {passport_number}")
                logger.info(f"    Position: {position}")
                logger.info(f"    Birth Date: {birth_date}")
                logger.info(f"    Gender: {gender}")
                logger.info(f"    Nationality: {nationality_code}")
                
                # Create or update record
                crew_record, created = QatarCrewBasic.objects.update_or_create(
                    flight_no=flight_no,
                    flight_date=flight_date,
                    origin=origin,
                    destination=destination,
                    crew_id=crew_id,
                    defaults={
                        'tail_no': "",  # Will be filled from flight data if available
                        'position': position,
                        'passport_number': passport_number,
                        'birth_date': birth_date,
                        'gender': gender,
                        'nationality_code': nationality_code,
                    }
                )
                
                action = "Created" if created else "Updated"
                logger.info(f"✓ {action} crew record: {crew_id} for flight {flight_no}")
                processed_crew_count += 1
                
            except Exception as e:
                logger.error(f"Error processing line {line_num} in JOB 97: {e}")
                logger.error(f"Problematic line: {line}")
                continue
                
        logger.info(f"JOB 97 processing completed for flight {flight_no}: {processed_crew_count} crew members processed")
        return processed_crew_count
        
    except Exception as e:
        logger.error(f"Error processing JOB 97 file: {e}")
        return 0


def process_job1008_file(attachment):
    """
    Process JOB 1008 file containing detailed crew passport information
    FIXED: Proper field mapping based on actual data structure from logs
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Processing JOB 1008 - Detailed crew passport information")
        
        processed_count = 0
        
        for line_num, line in enumerate(content, start=1):
            try:
                # Skip empty lines or headers
                if not line.strip():
                    continue
                
                # Skip header lines
                line_upper = line.upper()
                if any(keyword in line_upper for keyword in ['CREW', 'NAME', 'PASSPORT', 'ID', 'SURNAME', 'FIRSTNAME']):
                    logger.info(f"Skipping header line {line_num}: {line[:50]}...")
                    continue
                
                # Parse CSV format - based on actual data from logs
                fields = [field.strip() for field in line.split(',')]
                if len(fields) < 5:  # Ensure minimum required fields
                    continue
                
                # CORRECTED field mapping based on the actual data structure from your logs:
                # Format: crew_id,passport_number,,surname,given_name,middle_name,nationality,,date_field,,location,,country_code
                crew_id = fields[0] if fields[0] else None
                passport_number = fields[1] if fields[1] else None
                # Field 2 is usually empty
                surname = fields[3] if len(fields) > 3 and fields[3] else ""
                given_name = fields[4] if len(fields) > 4 and fields[4] else ""
                middle_name = fields[5] if len(fields) > 5 and fields[5] else None
                nationality = fields[6] if len(fields) > 6 and fields[6] else ""
                # Field 7 is usually empty
                date_field = fields[8] if len(fields) > 8 and fields[8] else None  # This could be birth date or issuing date
                # Field 9 is usually empty
                location = fields[10] if len(fields) > 10 and fields[10] else ""
                # Field 11 is usually empty
                nationality_country_code = fields[12] if len(fields) > 12 and fields[12] else nationality[:3] if nationality else ""
                
                # Skip if essential fields are missing
                if not crew_id or not passport_number:
                    continue
                
                # Parse date field - it appears to be mixed format, avoid country names
                passport_issuing_date = None
                if date_field and date_field not in ['RWANDA', 'RWANDAN', '+250', 'NEPALESE', 'INDIAN', 'KENYAN', 'SUDAN', 'MALAWIAN', 'RWA', 'UNITED KINGDOM', 'FRANCE', 'Rwanda', 'Rwandan', 'SOUTH AFRICA']:
                    try:
                        # Try different date formats
                        if '/' in date_field:
                            passport_issuing_date = datetime.strptime(date_field, "%d/%m/%Y").date()
                        elif len(date_field) == 8 and date_field.isdigit():
                            passport_issuing_date = datetime.strptime(date_field, "%d%m%Y").date()
                        elif len(date_field) == 10 and date_field.replace('/', '').isdigit():
                            passport_issuing_date = datetime.strptime(date_field, "%d/%m/%Y").date()
                    except ValueError:
                        logger.warning(f"Could not parse date: {date_field}")
                
                # Clean up nationality and country codes - limit to appropriate lengths
                if nationality and len(nationality) > 50:
                    nationality = nationality[:50]
                    
                if nationality_country_code and len(nationality_country_code) > 3:
                    nationality_country_code = nationality_country_code[:3]
                
                # For issuing state, use the location or derive from nationality
                issuing_state = nationality_country_code if nationality_country_code else "RWA"
                if location and len(location) <= 10:
                    issuing_state = location[:3] if len(location) > 3 else location
                
                # Ensure issuing_state is not too long
                if issuing_state and len(issuing_state) > 10:
                    issuing_state = issuing_state[:3]
                
                # Create or update record
                detailed_record, created = QatarCrewDetailed.objects.update_or_create(
                    crew_id=crew_id,
                    passport_number=passport_number,
                    defaults={
                        'surname': surname,
                        'given_name': given_name,
                        'middle_name': middle_name,
                        'nationality': nationality,
                        'passport_issuing_date': passport_issuing_date,
                        'passport_issuing_state': issuing_state,
                        'nationality_country_code': nationality_country_code,
                    }
                )
                
                action = "Created" if created else "Updated"
                logger.info(f"{action} detailed crew record: {crew_id} - {given_name} {surname}")
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing line {line_num} in JOB 1008: {e} - Line: {line}")
                continue
                
        logger.info(f"JOB 1008 processing completed - {processed_count} records processed")
        return processed_count
        
    except Exception as e:
        logger.error(f"Error processing JOB 1008 file: {e}")
        return 0


def generate_qatar_apis_file():
    """
    Generate Qatar APIS file by combining data from all sources
    FIXED: Better error handling, debugging, and more flexible matching
    """
    try:
        # Create output directory
        output_dir = os.path.join(settings.MEDIA_ROOT, "qatar_apis")
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"QATAR_APIS_{timestamp}.txt"
        file_path = os.path.join(output_dir, filename)
        
        # Debug: Check what data we have
        basic_crew_count = QatarCrewBasic.objects.count()
        detailed_crew_count = QatarCrewDetailed.objects.count()
        doha_kgl_basic = QatarCrewBasic.objects.filter(
            models.Q(origin='DOH', destination='KGL') | 
            models.Q(origin='KGL', destination='DOH')
        ).count()
        
        logger.info(f"DEBUG: Basic crew records: {basic_crew_count}")
        logger.info(f"DEBUG: Detailed crew records: {detailed_crew_count}")
        logger.info(f"DEBUG: DOH-KGL/KGL-DOH basic records: {doha_kgl_basic}")
        
        # Get all basic crew records for DOH-KGL and KGL-DOH flights
        basic_crew_records = QatarCrewBasic.objects.filter(
            models.Q(origin='DOH', destination='KGL') | 
            models.Q(origin='KGL', destination='DOH')
        )
        
        if not basic_crew_records.exists():
            logger.warning("No basic crew records found for DOH-KGL or KGL-DOH flights")
            return None
        
        apis_records = []
        processed_count = 0
        missing_details_count = 0
        missing_flight_data_count = 0
        
        for basic_record in basic_crew_records:
            try:
                logger.info(f"Processing crew {basic_record.crew_id} for flight {basic_record.flight_no}")
                
                # Get detailed information for this crew member
                # Try crew_id first, then crew_id + passport
                detailed_record = QatarCrewDetailed.objects.filter(
                    crew_id=basic_record.crew_id
                ).first()
                
                if not detailed_record:
                    # Try matching by passport number
                    detailed_record = QatarCrewDetailed.objects.filter(
                        passport_number=basic_record.passport_number
                    ).first()
                
                if not detailed_record:
                    logger.warning(f"No detailed record found for crew {basic_record.crew_id}")
                    missing_details_count += 1
                    # Create a minimal detailed record
                    detailed_record = QatarCrewDetailed(
                        crew_id=basic_record.crew_id,
                        passport_number=basic_record.passport_number,
                        surname="UNKNOWN",
                        given_name="CREW",
                        nationality="RWANDAN",
                        nationality_country_code="RWA",
                        passport_issuing_state="RWA"
                    )
                
                # Get flight schedule information for timing
                flight_data = FlightData.objects.filter(
                    flight_no=basic_record.flight_no,
                    sd_date_utc=basic_record.flight_date,
                    dep_code_icao=basic_record.origin,
                    arr_code_icao=basic_record.destination
                ).first()
                
                if not flight_data:
                    logger.warning(f"No flight schedule found for {basic_record.flight_no} on {basic_record.flight_date}")
                    missing_flight_data_count += 1
                    # Continue anyway with default times
                    dep_time = datetime.time(12, 0)
                    arr_time = datetime.time(14, 0)
                    arr_date = basic_record.flight_date
                else:
                    dep_time = flight_data.std_utc or datetime.time(12, 0)
                    arr_time = flight_data.sta_utc or datetime.time(14, 0)
                    arr_date = flight_data.sa_date_utc or basic_record.flight_date
                
                # Determine direction (O = Outbound from KGL, I = Inbound to KGL)
                direction = 'O' if basic_record.origin == 'KGL' else 'I'
                
                # Calculate document expiry (assuming 10 years from issue date if available)
                document_expiry = None
                if detailed_record.passport_issuing_date:
                    try:
                        document_expiry = detailed_record.passport_issuing_date.replace(
                            year=detailed_record.passport_issuing_date.year + 10
                        )
                    except ValueError:
                        document_expiry = datetime(2030, 12, 31).date()
                else:
                    # Default to a future date if no issue date available
                    document_expiry = datetime(2030, 12, 31).date()
                
                # Clean up nationality codes to ensure they're 3 characters for APIS format
                nationality = detailed_record.nationality_country_code[:3] if detailed_record.nationality_country_code else basic_record.nationality_code[:3]
                issuing_state = detailed_record.passport_issuing_state[:3] if detailed_record.passport_issuing_state else nationality
                country_of_birth = basic_record.nationality_code[:3] if basic_record.nationality_code else nationality
                
                # Create APIS record
                apis_record = QatarApisRecord.objects.update_or_create(
                    flight_no=basic_record.flight_no,
                    dep_date=basic_record.flight_date,
                    crew_id=basic_record.crew_id,
                    direction=direction,
                    defaults={
                        'dep_port': basic_record.origin,
                        'dep_time': dep_time,
                        'arr_port': basic_record.destination,
                        'arr_date': arr_date,
                        'arr_time': arr_time,
                        'document_number': basic_record.passport_number,
                        'nationality': nationality,
                        'document_type': 'P',  # Passport
                        'issuing_state': issuing_state,
                        'family_name': detailed_record.surname,
                        'given_name': detailed_record.given_name,
                        'date_of_birth': basic_record.birth_date,
                        'sex': basic_record.gender,
                        'country_of_birth': country_of_birth,
                        'document_expiry_date': document_expiry,
                        'file_generated': filename,
                    }
                )[0]
                
                apis_records.append(apis_record)
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing crew record {basic_record.crew_id}: {e}")
                continue
        
        # Write APIS file
        if apis_records:
            with open(file_path, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([
                    'TYPE', 'DIRECTION', 'FLIGHT', 'DEP_PORT', 'DEP_DATE', 'DEP_TIME',
                    'ARR_PORT', 'ARR_DATE', 'ARR_TIME', 'DOCUMENT_NUMBER', 'NATIONALITY',
                    'DOCUMENT_TYPE', 'ISSUING_STATE', 'FAMILY_NAME', 'GIVEN_NAME',
                    'DATE_OF_BIRTH', 'SEX', 'COUNTRY_OF_BIRTH', 'DOCUMENT_EXPIRY_DATE'
                ])
                
                # Write data rows
                for record in apis_records:
                    writer.writerow([
                        'C',  # Always 'C' for Crew
                        record.direction,
                        record.flight_no,
                        record.dep_port,
                        record.dep_date.strftime('%Y%m%d'),
                        record.dep_time.strftime('%H%M'),
                        record.arr_port,
                        record.arr_date.strftime('%Y%m%d'),
                        record.arr_time.strftime('%H%M'),
                        record.document_number,
                        record.nationality,
                        record.document_type,
                        record.issuing_state,
                        record.family_name,
                        record.given_name,
                        record.date_of_birth.strftime('%Y%m%d'),
                        record.sex,
                        record.country_of_birth,
                        record.document_expiry_date.strftime('%Y%m%d'),
                    ])
            
            logger.info(f"Generated Qatar APIS file: {filename}")
            logger.info(f"Processed {processed_count} crew records")
            if missing_details_count > 0:
                logger.warning(f"Missing detailed information for {missing_details_count} crew members")
            if missing_flight_data_count > 0:
                logger.warning(f"Missing flight data for {missing_flight_data_count} flights")
            
            return file_path
        else:
            logger.warning("No APIS records to generate")
            return None
            
    except Exception as e:
        logger.error(f"Error generating Qatar APIS file: {e}")
        return None


def process_qatar_job97_email_attachment(item, process_function):
    """
    Process JOB 97 email attachments
    Enhanced with better error handling and logging
    """
    try:
        logger.info(f"Processing email with subject: {item.subject}")
        
        if hasattr(item, 'attachments') and item.attachments:
            attachment_count = 0
            try:
                attachment_count = len(list(item.attachments))
                logger.info(f"Email has {attachment_count} attachments")
            except Exception as e:
                logger.warning(f"Could not count attachments: {e}")
            
            total_processed = 0
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    # Store reference to parent item for subject parsing
                    attachment.parent_item = item
                    logger.info(f"Processing JOB 97 attachment: {attachment.name} (size: {len(attachment.content)} bytes)")
                    
                    try:
                        result = process_function(attachment)
                        total_processed += result if isinstance(result, int) else 0
                        logger.info(f"Processed attachment {attachment.name}: {result} records")
                    except Exception as e:
                        logger.error(f"Error processing attachment {attachment.name}: {e}")
                        continue
                else:
                    logger.info(f"Skipping non-file attachment: {type(attachment)}")
            
            return total_processed
        else:
            logger.warning(f"Email has no attachments or attachments are not accessible")
            return 0
            
    except Exception as e:
        logger.error(f"Error processing JOB 97 email attachment: {e}")
        return 0


def process_qatar_job1008_email_attachment(item, process_function):
    """
    Process JOB 1008 email attachments
    Enhanced with better error handling and logging
    """
    try:
        logger.info(f"Processing JOB 1008 email with subject: {item.subject}")
        
        if hasattr(item, 'attachments') and item.attachments:
            total_processed = 0
            for attachment in item.attachments:
                if isinstance(attachment, FileAttachment):
                    logger.info(f"Processing JOB 1008 attachment: {attachment.name}")
                    try:
                        result = process_function(attachment)
                        total_processed += result if isinstance(result, int) else 0
                        logger.info(f"Processed JOB 1008 attachment {attachment.name}: {result} records")
                    except Exception as e:
                        logger.error(f"Error processing JOB 1008 attachment {attachment.name}: {e}")
                        continue
            return total_processed
        else:
            logger.warning(f"JOB 1008 email has no attachments")
            return 0
    except Exception as e:
        logger.error(f"Error processing JOB 1008 email attachment: {e}")
        return 0


# Additional utility functions for better debugging

def validate_crew_data(crew_data):
    """
    Validate crew data before saving
    """
    errors = []
    
    if not crew_data.get('crew_id'):
        errors.append("Missing crew_id")
    
    if not crew_data.get('passport_number'):
        errors.append("Missing passport_number")
    
    if crew_data.get('birth_date'):
        try:
            birth_year = crew_data['birth_date'].year
            if birth_year < 1950 or birth_year > 2005:
                errors.append(f"Invalid birth year: {birth_year}")
        except:
            errors.append("Invalid birth_date format")
    
    return errors


def debug_file_content(content, max_lines=10):
    """
    Debug helper to analyze file content structure
    """
    lines = content.splitlines()
    logger.info(f"File analysis:")
    logger.info(f"  Total lines: {len(lines)}")
    logger.info(f"  Total characters: {len(content)}")
    logger.info(f"  Has commas: {content.count(',')}")
    logger.info(f"  Has tabs: {content.count(chr(9))}")
    
    logger.info(f"  First {min(max_lines, len(lines))} lines:")
    for i, line in enumerate(lines[:max_lines]):
        logger.info(f"    Line {i+1}: {repr(line[:100])}")  # First 100 chars with quotes
    
    if len(lines) > max_lines:
        logger.info(f"  ... and {len(lines) - max_lines} more lines")


def test_subject_parsing():
    """
    Test function to validate subject parsing with various formats
    """
    test_subjects = [
        "300/KGL DOH/28072025",
        "301/DOH KGL/29072025",
        "WB300/KGL DOH/30072025",
        "300 / KGL DOH / 28/07/2025",
        "301/DOH-KGL/29072025",
        "Invalid subject format",
        ""
    ]
    
    logger.info("Testing subject parsing:")
    for subject in test_subjects:
        result = parse_job97_subject(subject)
        logger.info(f"  '{subject}' -> {result}")
    
    return True


# Database cleanup and verification functions

def cleanup_old_airport_codes():
    """
    Clean up crew records that were stored with wrong airport codes (ICAO instead of IATA)
    """
    try:
        # Count records with wrong codes
        wrong_basic = QatarCrewBasic.objects.filter(
            models.Q(origin__in=['HRYR', 'OTHH']) | 
            models.Q(destination__in=['HRYR', 'OTHH'])
        ).count()
        
        wrong_apis = QatarApisRecord.objects.filter(
            models.Q(dep_port__in=['HRYR', 'OTHH']) | 
            models.Q(arr_port__in=['HRYR', 'OTHH'])
        ).count()
        
        logger.info(f"Found {wrong_basic} basic crew records with ICAO codes")
        logger.info(f"Found {wrong_apis} APIS records with ICAO codes")
        
        # Delete records with wrong airport codes
        deleted_basic = QatarCrewBasic.objects.filter(
            models.Q(origin__in=['HRYR', 'OTHH']) | 
            models.Q(destination__in=['HRYR', 'OTHH'])
        ).delete()
        
        deleted_apis = QatarApisRecord.objects.filter(
            models.Q(dep_port__in=['HRYR', 'OTHH']) | 
            models.Q(arr_port__in=['HRYR', 'OTHH'])
        ).delete()
        
        logger.info(f"Deleted {deleted_basic[0]} basic crew records with ICAO codes")
        logger.info(f"Deleted {deleted_apis[0]} APIS records with ICAO codes")
        
        return {
            "status": "success",
            "deleted_basic": deleted_basic[0] if deleted_basic else 0,
            "deleted_apis": deleted_apis[0] if deleted_apis else 0,
            "message": f"Cleaned up {deleted_basic[0] if deleted_basic else 0} basic + {deleted_apis[0] if deleted_apis else 0} APIS records"
        }
        
    except Exception as e:
        logger.error(f"Error cleaning up old airport codes: {e}")
        return {"status": "error", "message": str(e)}


def verify_current_data():
    """
    Verify what data we currently have in the database
    """
    try:
        # Count all records
        basic_total = QatarCrewBasic.objects.count()
        detailed_total = QatarCrewDetailed.objects.count()
        apis_total = QatarApisRecord.objects.count()
        
        # Count by airport codes
        basic_iata = QatarCrewBasic.objects.filter(
            models.Q(origin__in=['KGL', 'DOH']) | 
            models.Q(destination__in=['KGL', 'DOH'])
        ).count()
        
        basic_icao = QatarCrewBasic.objects.filter(
            models.Q(origin__in=['HRYR', 'OTHH']) | 
            models.Q(destination__in=['HRYR', 'OTHH'])
        ).count()
        
        # Show sample records
        sample_basic = list(QatarCrewBasic.objects.all()[:5].values(
            'flight_no', 'origin', 'destination', 'flight_date', 'crew_id'
        ))
        
        sample_detailed = list(QatarCrewDetailed.objects.all()[:5].values(
            'crew_id', 'passport_number', 'surname', 'given_name'
        ))
        
        logger.info(f"DATABASE VERIFICATION:")
        logger.info(f"  Basic crew records total: {basic_total}")
        logger.info(f"  Basic crew with IATA codes (KGL/DOH): {basic_iata}")
        logger.info(f"  Basic crew with ICAO codes (HRYR/OTHH): {basic_icao}")
        logger.info(f"  Detailed crew records: {detailed_total}")
        logger.info(f"  APIS records: {apis_total}")
        logger.info(f"  Sample basic records: {sample_basic}")
        logger.info(f"  Sample detailed records: {sample_detailed}")
        
        return {
            "status": "success",
            "basic_total": basic_total,
            "basic_iata": basic_iata,
            "basic_icao": basic_icao,
            "detailed_total": detailed_total,
            "apis_total": apis_total,
            "sample_basic": sample_basic,
            "sample_detailed": sample_detailed
        }
        
    except Exception as e:
        logger.error(f"Error verifying current data: {e}")
        return {"status": "error", "message": str(e)}