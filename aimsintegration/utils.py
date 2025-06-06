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
#                     sd_date_utc = datetime.strptime(flight_date, "%m/%d/%Y").date()
#                     sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
#                     std_utc = datetime.strptime(std, "%H:%M").time()
#                     sta_utc = datetime.strptime(sta, "%H:%M").time()
#                     atd_utc = datetime.strptime(atd, "%H:%M").time() if atd else None
#                     takeoff_utc = datetime.strptime(takeoff, "%H:%M").time() if takeoff else None
#                     touchdown_utc = datetime.strptime(touchdown, "%H:%M").time() if touchdown else None
#                     ata_utc = datetime.strptime(ata, "%H:%M").time() if ata else None
#                     etd_utc = datetime.strptime(etd, "%H:%M").time() if etd else None
#                     eta_utc = datetime.strptime(eta, "%H:%M").time() if eta else None
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
#                         if atd_utc and existing_record.atd_utc != atd_utc:
#                             existing_record.atd_utc = atd_utc
#                             updated = True
#                         if takeoff_utc and existing_record.takeoff_utc != takeoff_utc:
#                             existing_record.takeoff_utc = takeoff_utc
#                             updated = True
#                         if touchdown_utc and existing_record.touchdown_utc != touchdown_utc:
#                             existing_record.touchdown_utc = touchdown_utc
#                             updated = True
#                         if ata_utc and existing_record.ata_utc != ata_utc:
#                             existing_record.ata_utc = ata_utc
#                             updated = True
                     
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
#                             atd_utc=atd_utc,
#                             takeoff_utc=takeoff_utc,
#                             touchdown_utc=touchdown_utc,
#                             ata_utc=ata_utc,
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


def process_flight_schedule_file(attachment):
    """
    Process the flight schedule file with comprehensive protection for initiated flights.
    Prevents updates to core flight data once flight has any actual timing data.
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

                logger.debug(f"Processing line {line_num}: Flight {flight_no}, Tail {tail_no}, Date {flight_date}")

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
                except ValueError as ve:
                    logger.error(f"Skipping line {line_num} due to date/time format error: {ve} - {line}")
                    continue

                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                if not dep_airport or not arr_airport:
                    logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
                    continue

                dep_code_iata = dep_airport.iata_code
                arr_code_iata = arr_airport.iata_code

                # UPDATED MATCHING CRITERIA - Remove tail_no to avoid circular matching issues
                # This is crucial for preventing the tail number change problem
                existing_record = FlightData.objects.filter(
                    flight_no=flight_no,
                    sd_date_utc=sd_date_utc,
                    dep_code_icao=dep_code_icao,
                    arr_code_icao=arr_code_icao,
                    std_utc=std_utc,
                    # Removed tail_no from matching criteria
                ).first()

                # Prevent duplicate insertions using transaction.atomic()
                with transaction.atomic():
                    if existing_record:
                        logger.info(f"Found existing record for flight {flight_no} on {sd_date_utc}")
                        
                        # CHECK IF FLIGHT HAS BEEN INITIATED (Critical Protection Check)
                        flight_initiated = any([
                            existing_record.atd_utc,      # Actual Time of Departure
                            existing_record.takeoff_utc,  # Takeoff time
                            existing_record.touchdown_utc, # Landing time
                            existing_record.ata_utc       # Actual Time of Arrival
                        ])
                        
                        if flight_initiated:
                            logger.warning(
                                f"🛡️  PROTECTION ACTIVE: Flight {flight_no} on {sd_date_utc} has been initiated. "
                                f"Actual timings present: ATD={bool(existing_record.atd_utc)}, "
                                f"TO={bool(existing_record.takeoff_utc)}, "
                                f"TD={bool(existing_record.touchdown_utc)}, "
                                f"ATA={bool(existing_record.ata_utc)}"
                            )
                            
                            # LOG BLOCKED CHANGES TO CORE FIELDS
                            blocked_changes = []
                            
                            if existing_record.tail_no != tail_no:
                                blocked_changes.append(f"tail_no: '{existing_record.tail_no}' -> '{tail_no}'")
                            if existing_record.flight_no != flight_no:
                                blocked_changes.append(f"flight_no: '{existing_record.flight_no}' -> '{flight_no}'")
                            if existing_record.dep_code_iata != dep_code_iata:
                                blocked_changes.append(f"dep_code_iata: '{existing_record.dep_code_iata}' -> '{dep_code_iata}'")
                            if existing_record.arr_code_iata != arr_code_iata:
                                blocked_changes.append(f"arr_code_iata: '{existing_record.arr_code_iata}' -> '{arr_code_iata}'")
                            if existing_record.std_utc != std_utc:
                                blocked_changes.append(f"std_utc: '{existing_record.std_utc}' -> '{std_utc}'")
                            if existing_record.sta_utc != sta_utc:
                                blocked_changes.append(f"sta_utc: '{existing_record.sta_utc}' -> '{sta_utc}'")
                            
                            if blocked_changes:
                                logger.warning(
                                    f"🚫 BLOCKED {len(blocked_changes)} core field changes for initiated flight {flight_no}:\n" +
                                    "\n".join([f"   - {change}" for change in blocked_changes]) +
                                    f"\n   Source: Flight Schedule File (Line {line_num})"
                                )
                            
                            # ONLY UPDATE ACTUAL TIMING FIELDS (and only if currently null)
                            timing_updated = False
                            
                            if atd_utc and not existing_record.atd_utc:
                                existing_record.atd_utc = atd_utc
                                timing_updated = True
                                logger.info(f"✅ Updated ATD for initiated flight {flight_no}: {atd_utc}")
                            elif atd_utc and existing_record.atd_utc != atd_utc:
                                logger.info(f"⚠️  ATD already set for flight {flight_no}: {existing_record.atd_utc}, ignoring {atd_utc}")
                            
                            if takeoff_utc and not existing_record.takeoff_utc:
                                existing_record.takeoff_utc = takeoff_utc
                                timing_updated = True
                                logger.info(f"✅ Updated takeoff for initiated flight {flight_no}: {takeoff_utc}")
                            elif takeoff_utc and existing_record.takeoff_utc != takeoff_utc:
                                logger.info(f"⚠️  Takeoff already set for flight {flight_no}: {existing_record.takeoff_utc}, ignoring {takeoff_utc}")
                            
                            if touchdown_utc and not existing_record.touchdown_utc:
                                existing_record.touchdown_utc = touchdown_utc
                                timing_updated = True
                                logger.info(f"✅ Updated touchdown for initiated flight {flight_no}: {touchdown_utc}")
                            elif touchdown_utc and existing_record.touchdown_utc != touchdown_utc:
                                logger.info(f"⚠️  Touchdown already set for flight {flight_no}: {existing_record.touchdown_utc}, ignoring {touchdown_utc}")
                            
                            if ata_utc and not existing_record.ata_utc:
                                existing_record.ata_utc = ata_utc
                                timing_updated = True
                                logger.info(f"✅ Updated ATA for initiated flight {flight_no}: {ata_utc}")
                            elif ata_utc and existing_record.ata_utc != ata_utc:
                                logger.info(f"⚠️  ATA already set for flight {flight_no}: {existing_record.ata_utc}, ignoring {ata_utc}")
                            
                            if timing_updated:
                                existing_record.save()
                                logger.info(f"💾 Saved timing updates for initiated flight {flight_no}")
                            else:
                                logger.info(f"📋 No timing updates needed for initiated flight {flight_no}")
                            
                            # Skip to next record - don't update core fields
                            continue
                        
                        # FLIGHT NOT INITIATED - Allow all updates as before
                        logger.info(f"✅ Flight {flight_no} not yet initiated. Updating all fields as normal.")
                        
                        updated = False
                        changes_made = []
                        
                        # Update core fields (only for non-initiated flights)
                        if existing_record.tail_no != tail_no:
                            changes_made.append(f"tail_no: '{existing_record.tail_no}' -> '{tail_no}'")
                            existing_record.tail_no = tail_no
                            updated = True
                        
                        if existing_record.dep_code_iata != dep_code_iata:
                            changes_made.append(f"dep_code_iata: '{existing_record.dep_code_iata}' -> '{dep_code_iata}'")
                            existing_record.dep_code_iata = dep_code_iata
                            updated = True
                        
                        if existing_record.arr_code_iata != arr_code_iata:
                            changes_made.append(f"arr_code_iata: '{existing_record.arr_code_iata}' -> '{arr_code_iata}'")
                            existing_record.arr_code_iata = arr_code_iata
                            updated = True
                        
                        # Update scheduled times
                        if std_utc and existing_record.std_utc != std_utc:
                            changes_made.append(f"std_utc: '{existing_record.std_utc}' -> '{std_utc}'")
                            existing_record.std_utc = std_utc
                            updated = True
                        
                        if sta_utc and existing_record.sta_utc != sta_utc:
                            changes_made.append(f"sta_utc: '{existing_record.sta_utc}' -> '{sta_utc}'")
                            existing_record.sta_utc = sta_utc
                            updated = True
                        
                        if sa_date_utc and existing_record.sa_date_utc != sa_date_utc:
                            changes_made.append(f"sa_date_utc: '{existing_record.sa_date_utc}' -> '{sa_date_utc}'")
                            existing_record.sa_date_utc = sa_date_utc
                            updated = True
                        
                        # Update actual timing fields (if provided and different)
                        if atd_utc and existing_record.atd_utc != atd_utc:
                            changes_made.append(f"atd_utc: '{existing_record.atd_utc}' -> '{atd_utc}'")
                            existing_record.atd_utc = atd_utc
                            updated = True
                        
                        if takeoff_utc and existing_record.takeoff_utc != takeoff_utc:
                            changes_made.append(f"takeoff_utc: '{existing_record.takeoff_utc}' -> '{takeoff_utc}'")
                            existing_record.takeoff_utc = takeoff_utc
                            updated = True
                        
                        if touchdown_utc and existing_record.touchdown_utc != touchdown_utc:
                            changes_made.append(f"touchdown_utc: '{existing_record.touchdown_utc}' -> '{touchdown_utc}'")
                            existing_record.touchdown_utc = touchdown_utc
                            updated = True
                        
                        if ata_utc and existing_record.ata_utc != ata_utc:
                            changes_made.append(f"ata_utc: '{existing_record.ata_utc}' -> '{ata_utc}'")
                            existing_record.ata_utc = ata_utc
                            updated = True
                        
                        if updated:
                            existing_record.save()
                            logger.info(
                                f"💾 Updated FlightData record for flight {flight_no} on {sd_date_utc}. "
                                f"Changes: {len(changes_made)} fields updated"
                            )
                            if logger.isEnabledFor(logging.DEBUG):
                                for change in changes_made:
                                    logger.debug(f"   - {change}")
                        else:
                            logger.info(f"📋 No changes needed for FlightData record {flight_no} on {sd_date_utc}")
                    
                    else:
                        # CREATE NEW RECORD - No existing record found
                        logger.info(f"➕ Creating new flight record: {flight_no} on {sd_date_utc}")
                        
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
                            source_type="Flight_Schedule",  # Changed from "FDM" to be more accurate
                            raw_content=",".join(fields),
                        )
                        logger.info(f"✅ Successfully inserted new flight record: {flight_no} on {sd_date_utc}")
                        
            except Exception as e:
                logger.error(f"❌ Error processing line {line_num}: {e} - Fields: {fields}", exc_info=True)
                continue

        logger.info("🎉 Flight schedule file processed successfully with data protection active")

    except Exception as e:
        logger.error(f"❌ Error processing flight schedule file: {e}", exc_info=True)


# Optional: Add a helper function to check flight initiation status
def is_flight_initiated(flight_record):
    """
    Helper function to check if a flight has been initiated
    Returns True if flight has any actual timing data
    """
    return any([
        flight_record.atd_utc,      # Actual Time of Departure
        flight_record.takeoff_utc,  # Takeoff time
        flight_record.touchdown_utc, # Landing time
        flight_record.ata_utc       # Actual Time of Arrival
    ])


# Optional: Add a function to get flight initiation summary
def get_flight_timing_summary(flight_record):
    """
    Get a summary of which actual timings are present for a flight
    """
    return {
        'initiated': is_flight_initiated(flight_record),
        'atd_present': bool(flight_record.atd_utc),
        'takeoff_present': bool(flight_record.takeoff_utc),
        'touchdown_present': bool(flight_record.touchdown_utc),
        'ata_present': bool(flight_record.ata_utc),
        'completion_percentage': sum([
            bool(flight_record.atd_utc),
            bool(flight_record.takeoff_utc),
            bool(flight_record.touchdown_utc),
            bool(flight_record.ata_utc)
        ]) * 25  # Each timing represents 25% completion
    }


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

#         # flights = FlightData.objects.filter(
#         #     flight_no=flight_no,
#         #     tail_no=tail_number,
#         #     dep_code_iata=dep_code,
#         #     arr_code_iata=arr_code
#         # )

#         # First, try to get matching flights with flight number
#         flights = FlightData.objects.filter(
#             flight_no=flight_no,
#             tail_no=tail_number,
#             dep_code_iata=dep_code,
#             arr_code_iata=arr_code
#         )

#         # If no flights found, try matching without flight number
#         if not flights.exists():
#             flights = FlightData.objects.filter(
#                 tail_no=tail_number,
#                 dep_code_iata=dep_code,
#                 arr_code_iata=arr_code
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

#         closest_flight = min(
#             flights,
#             key=lambda flight: abs((flight.sd_date_utc - email_received_date).days)
#         )

#         # closest_fdm_flight = min(
#         #     fdm_flights,
#         #     key=lambda fl: abs((fl.sd_date_utc - email_received_date).days)
#         # ) 

#         if acars_event == "OT":
#             closest_flight.atd_utc = event_time
#             # closest_fdm_flight.atd_utc = event_time
#         elif acars_event == "OF":
#             closest_flight.takeoff_utc = event_time
#             # closest_fdm_flight.takeoff_utc = event_time
#         elif acars_event == "ON":
#             closest_flight.touchdown_utc = event_time
#             # closest_fdm_flight.touchdown_utc = event_time
#         elif acars_event == "IN":
#             closest_flight.ata_utc = event_time
#             # closest_fdm_flight.ata_utc = event_time

#         closest_flight.save()
#         # closest_fdm_flight.save()

#         # Append the updated flight details to the job file
#         write_job_one_row(file_path, closest_flight, acars_event, event_time, email_received_date)

#     except Exception as e:
#         logger.error(f"Error processing ACARS message: {e}", exc_info=True)


# Start of updated process acars messages
def process_acars_message(item, file_path):
    """
    Enhanced ACARS processing with protection against timing overwrites.
    ACARS should only update actual timing fields and only if they're currently null.
    """
    try:
        email_received_date = item.datetime_received.date()  # Get only the date part
        message_body = item.body

        if "M16" in message_body:
            logger.info("Skipping 'M16' ACARS message.")
            return

        logger.info(f"📡 ACARS message received at: {email_received_date} UTC")
        logger.debug(f"ACARS message body: {message_body}")

        # Extract fields from the message
        flight_no = extract_flight_number(message_body)
        acars_event, event_time_str = extract_acars_event(message_body)
        dep_code, arr_code = extract_departure_and_arrival_codes(message_body)
        tail_number = extract_tail_number(message_body)

        # Validate extracted data
        if not flight_no:
            logger.error("❌ Could not extract flight number from ACARS message")
            return
        
        if not acars_event or not event_time_str:
            logger.error("❌ Could not extract ACARS event or time from message")
            return
        
        if not tail_number:
            logger.warning("⚠️  Could not extract tail number from ACARS message")
        
        if not dep_code or not arr_code:
            logger.warning("⚠️  Could not extract airport codes from ACARS message")

        if not re.match(r'^\d{2}:\d{2}$', event_time_str):
            logger.error(f"❌ Invalid time format in ACARS message: {event_time_str}")
            return

        event_time = datetime.strptime(event_time_str, "%H:%M").time()
        
        logger.info(f"🔍 Processing ACARS: Flight {flight_no}, Event {acars_event}, Time {event_time}, Tail {tail_number}")

        # IMPROVED FLIGHT MATCHING STRATEGY
        flights = None
        matching_strategy = ""
        
        # Strategy 1: Match with all available criteria
        if tail_number and dep_code and arr_code:
            flights = FlightData.objects.filter(
                flight_no=flight_no,
                tail_no=tail_number,
                dep_code_iata=dep_code,
                arr_code_iata=arr_code
            )
            matching_strategy = "full criteria (flight_no + tail_no + airports)"
        
        # Strategy 2: Match without tail number (in case tail number changed)
        if not flights or not flights.exists():
            if dep_code and arr_code:
                flights = FlightData.objects.filter(
                    flight_no=flight_no,
                    dep_code_iata=dep_code,
                    arr_code_iata=arr_code
                )
                matching_strategy = "flight_no + airports only"
        
        # Strategy 3: Match with tail number and flight number only
        if not flights or not flights.exists():
            if tail_number:
                flights = FlightData.objects.filter(
                    flight_no=flight_no,
                    tail_no=tail_number
                )
                matching_strategy = "flight_no + tail_no only"
        
        # Strategy 4: Match with just flight number (last resort)
        if not flights or not flights.exists():
            flights = FlightData.objects.filter(flight_no=flight_no)
            matching_strategy = "flight_no only (last resort)"

        if not flights or not flights.exists():
            logger.error(f"❌ No matching flights found for ACARS flight {flight_no}")
            send_mail(
                subject=f"No matching flights found for ACARS flight: {flight_no}",
                message=(
                    f"Dear Team,\n\n"
                    f"The ACARS message for flight {flight_no} could not be matched to any flight record.\n\n"
                    f"ACARS Details:\n"
                    f"- Flight: {flight_no}\n"
                    f"- Event: {acars_event}\n" 
                    f"- Time: {event_time}\n"
                    f"- Tail: {tail_number}\n"
                    f"- Route: {dep_code} -> {arr_code}\n"
                    f"- Received: {email_received_date}\n\n"
                    f"Message body:\n{message_body}\n\n"
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

        # Find the closest flight by date
        closest_flight = min(
            flights,
            key=lambda flight: abs((flight.sd_date_utc - email_received_date).days)
        )
        
        date_diff = abs((closest_flight.sd_date_utc - email_received_date).days)
        logger.info(
            f"✅ Matched ACARS to flight using {matching_strategy}. "
            f"Flight: {closest_flight.flight_no} on {closest_flight.sd_date_utc} "
            f"(date difference: {date_diff} days)"
        )
        
        # Check current flight timing status
        current_timings = {
            'atd_utc': closest_flight.atd_utc,
            'takeoff_utc': closest_flight.takeoff_utc,
            'touchdown_utc': closest_flight.touchdown_utc,
            'ata_utc': closest_flight.ata_utc
        }
        
        logger.debug(
            f"Current flight timings: ATD={bool(current_timings['atd_utc'])}, "
            f"TO={bool(current_timings['takeoff_utc'])}, "
            f"TD={bool(current_timings['touchdown_utc'])}, "
            f"ATA={bool(current_timings['ata_utc'])}"
        )

        # ACARS TIMING UPDATE LOGIC WITH PROTECTION
        timing_updated = False
        update_blocked = False
        update_message = ""
        
        if acars_event == "OT":  # Off blocks (departure)
            if not closest_flight.atd_utc:
                closest_flight.atd_utc = event_time
                timing_updated = True
                update_message = f"✅ ACARS set ATD (Off blocks): {event_time}"
                logger.info(update_message)
            else:
                update_blocked = True
                update_message = f"🛡️  ACARS ATD blocked - already set to {closest_flight.atd_utc}, ignoring new value {event_time}"
                logger.warning(update_message)
        
        elif acars_event == "OF":  # Airborne (takeoff)
            if not closest_flight.takeoff_utc:
                closest_flight.takeoff_utc = event_time
                timing_updated = True
                update_message = f"✅ ACARS set Takeoff (Airborne): {event_time}"
                logger.info(update_message)
            else:
                update_blocked = True
                update_message = f"🛡️  ACARS Takeoff blocked - already set to {closest_flight.takeoff_utc}, ignoring new value {event_time}"
                logger.warning(update_message)
        
        elif acars_event == "ON":  # Touchdown (landing)
            if not closest_flight.touchdown_utc:
                closest_flight.touchdown_utc = event_time
                timing_updated = True
                update_message = f"✅ ACARS set Touchdown (Landing): {event_time}"
                logger.info(update_message)
            else:
                update_blocked = True
                update_message = f"🛡️  ACARS Touchdown blocked - already set to {closest_flight.touchdown_utc}, ignoring new value {event_time}"
                logger.warning(update_message)
        
        elif acars_event == "IN":  # On blocks (arrival)
            if not closest_flight.ata_utc:
                closest_flight.ata_utc = event_time
                timing_updated = True
                update_message = f"✅ ACARS set ATA (On blocks): {event_time}"
                logger.info(update_message)
            else:
                update_blocked = True
                update_message = f"🛡️  ACARS ATA blocked - already set to {closest_flight.ata_utc}, ignoring new value {event_time}"
                logger.warning(update_message)
        
        else:
            logger.error(f"❌ Unknown ACARS event type: {acars_event}")
            return

        # Save changes if any timing was updated
        if timing_updated:
            try:
                closest_flight.save()
                logger.info(f"💾 Successfully saved ACARS timing update for flight {flight_no}")
                
                # Check if flight is now complete
                flight_complete = all([
                    closest_flight.atd_utc,
                    closest_flight.takeoff_utc,
                    closest_flight.touchdown_utc,
                    closest_flight.ata_utc
                ])
                
                if flight_complete:
                    logger.info(f"🎉 Flight {flight_no} is now complete with all actual timings!")
                
                # Write to job file only if update was successful
                write_job_one_row(file_path, closest_flight, acars_event, event_time, email_received_date)
                logger.info(f"📝 Added ACARS data to job file for flight {flight_no}")
                
            except Exception as save_error:
                logger.error(f"❌ Failed to save ACARS timing update: {save_error}", exc_info=True)
                return
        
        elif update_blocked:
            logger.info(f"📋 ACARS timing update blocked for flight {flight_no} - timing already exists")
            # Could still write to job file for audit purposes, but with a note
            # write_job_one_row(file_path, closest_flight, acars_event, event_time, email_received_date)
        
        else:
            logger.warning(f"⚠️  No ACARS timing update performed for flight {flight_no}")

        # Log final flight timing status
        logger.info(
            f"📊 Final timing status for flight {flight_no}: "
            f"ATD={bool(closest_flight.atd_utc)}, "
            f"TO={bool(closest_flight.takeoff_utc)}, "
            f"TD={bool(closest_flight.touchdown_utc)}, "
            f"ATA={bool(closest_flight.ata_utc)}"
        )

    except Exception as e:
        logger.error(f"❌ Error processing ACARS message: {e}", exc_info=True)
        logger.error(f"ACARS message details - Flight: {flight_no if 'flight_no' in locals() else 'Unknown'}, "
                    f"Event: {acars_event if 'acars_event' in locals() else 'Unknown'}, "
                    f"Time: {event_time_str if 'event_time_str' in locals() else 'Unknown'}")


# Optional: Helper function to validate ACARS data
def validate_acars_timing(flight_record, acars_event, new_time):
    """
    Validate ACARS timing against existing flight data
    Returns (is_valid, reason)
    """
    current_timings = {
        'OT': flight_record.atd_utc,
        'OF': flight_record.takeoff_utc, 
        'ON': flight_record.touchdown_utc,
        'IN': flight_record.ata_utc
    }
    
    current_value = current_timings.get(acars_event)
    
    if current_value is None:
        return True, "Field is empty, update allowed"
    
    if current_value == new_time:
        return False, f"Same value already exists ({current_value})"
    
    return False, f"Different value already exists ({current_value}), cannot overwrite with {new_time}"


# Optional: Get ACARS event description
def get_acars_event_description(acars_event):
    """
    Get human-readable description of ACARS events
    """
    descriptions = {
        'OT': 'Off blocks (Departure)',
        'OF': 'Airborne (Takeoff)',
        'ON': 'Touchdown (Landing)', 
        'IN': 'On blocks (Arrival)'
    }
    return descriptions.get(acars_event, f'Unknown event ({acars_event})')


# Optional: ACARS processing summary
def get_acars_processing_summary(flight_record):
    """
    Get summary of ACARS timing data for a flight
    """
    return {
        'flight_no': flight_record.flight_no,
        'tail_no': flight_record.tail_no,
        'route': f"{flight_record.dep_code_iata} -> {flight_record.arr_code_iata}",
        'acars_timings': {
            'departure_blocks': flight_record.atd_utc,
            'airborne': flight_record.takeoff_utc,
            'touchdown': flight_record.touchdown_utc,
            'arrival_blocks': flight_record.ata_utc
        },
        'acars_complete': all([
            flight_record.atd_utc,
            flight_record.takeoff_utc,
            flight_record.touchdown_utc,
            flight_record.ata_utc
        ]),
        'missing_acars': [
            event for event, timing in [
                ('OT - Off blocks', flight_record.atd_utc),
                ('OF - Airborne', flight_record.takeoff_utc), 
                ('ON - Touchdown', flight_record.touchdown_utc),
                ('IN - On blocks', flight_record.ata_utc)
            ] if not timing
        ]
    }

# End of updated process acars messages

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


def process_fdm_flight_schedule_file(attachment):
    """
    Process the FDM flight schedule file with comprehensive protection for initiated flights.
    Insert into FdmFlightData and update missing actual timings in FlightData.
    Prevents updates to core flight data once flight has any actual timing data.
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

                logger.debug(f"Processing FDM line {line_num}: Flight {flight_no}, Tail {tail_no}, Date {flight_date}")

                # Parse dates and times
                try:
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
                except ValueError as ve:
                    logger.error(f"Skipping FDM line {line_num} due to date/time format error: {ve} - {row}")
                    continue

                # Fetch airport data
                dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
                arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

                dep_code_iata = dep_airport.iata_code if dep_airport else ""
                arr_code_iata = arr_airport.iata_code if arr_airport else ""

                # UPDATED MATCHING CRITERIA - Remove tail_no to prevent circular matching issues
                # This is crucial for preventing the tail number change problem in FDM data
                unique_criteria = {
                    'flight_no': flight_no,
                    'sd_date_utc': sd_date_utc,
                    'dep_code_icao': dep_code_icao,
                    'arr_code_icao': arr_code_icao,
                    'std_utc': std_utc_time,
                    # Removed tail_no from matching criteria
                    # Removed sa_date_utc and sta_utc for more flexible matching
                }

                # Find existing FDM record
                existing_record = FdmFlightData.objects.filter(**unique_criteria).first()

                if existing_record:
                    logger.info(f"Found existing FDM record for flight {flight_no} on {sd_date_utc}")
                    
                    # CHECK IF FLIGHT HAS BEEN INITIATED (Critical Protection Check)
                    flight_initiated = any([
                        existing_record.atd_utc,      # Actual Time of Departure
                        existing_record.takeoff_utc,  # Takeoff time
                        existing_record.touchdown_utc, # Landing time
                        existing_record.ata_utc       # Actual Time of Arrival
                    ])
                    
                    if flight_initiated:
                        logger.warning(
                            f"🛡️  FDM PROTECTION ACTIVE: Flight {flight_no} on {sd_date_utc} has been initiated. "
                            f"Actual timings present: ATD={bool(existing_record.atd_utc)}, "
                            f"TO={bool(existing_record.takeoff_utc)}, "
                            f"TD={bool(existing_record.touchdown_utc)}, "
                            f"ATA={bool(existing_record.ata_utc)}"
                        )
                        
                        # LOG BLOCKED CHANGES TO CORE FIELDS
                        blocked_changes = []
                        
                        if existing_record.tail_no != tail_no:
                            blocked_changes.append(f"tail_no: '{existing_record.tail_no}' -> '{tail_no}'")
                        if existing_record.flight_no != flight_no:
                            blocked_changes.append(f"flight_no: '{existing_record.flight_no}' -> '{flight_no}'")
                        if existing_record.dep_code_iata != dep_code_iata:
                            blocked_changes.append(f"dep_code_iata: '{existing_record.dep_code_iata}' -> '{dep_code_iata}'")
                        if existing_record.arr_code_iata != arr_code_iata:
                            blocked_changes.append(f"arr_code_iata: '{existing_record.arr_code_iata}' -> '{arr_code_iata}'")
                        if existing_record.std_utc != std_utc_time:
                            blocked_changes.append(f"std_utc: '{existing_record.std_utc}' -> '{std_utc_time}'")
                        if existing_record.sta_utc != sta_utc_time:
                            blocked_changes.append(f"sta_utc: '{existing_record.sta_utc}' -> '{sta_utc_time}'")
                        if existing_record.flight_type != flight_type:
                            blocked_changes.append(f"flight_type: '{existing_record.flight_type}' -> '{flight_type}'")
                        
                        if blocked_changes:
                            logger.warning(
                                f"🚫 BLOCKED {len(blocked_changes)} core field changes for initiated FDM flight {flight_no}:\n" +
                                "\n".join([f"   - {change}" for change in blocked_changes]) +
                                f"\n   Source: FDM Flight Schedule File (Line {line_num})"
                            )
                        
                        # ONLY UPDATE ACTUAL TIMING FIELDS (and only if currently null)
                        timing_updated = False
                        
                        if atd_utc_time and not existing_record.atd_utc:
                            existing_record.atd_utc = atd_utc_time
                            timing_updated = True
                            logger.info(f"✅ FDM Updated ATD for initiated flight {flight_no}: {atd_utc_time}")
                        elif atd_utc_time and existing_record.atd_utc != atd_utc_time:
                            logger.info(f"⚠️  FDM ATD already set for flight {flight_no}: {existing_record.atd_utc}, ignoring {atd_utc_time}")
                        
                        if takeoff_utc_time and not existing_record.takeoff_utc:
                            existing_record.takeoff_utc = takeoff_utc_time
                            timing_updated = True
                            logger.info(f"✅ FDM Updated takeoff for initiated flight {flight_no}: {takeoff_utc_time}")
                        elif takeoff_utc_time and existing_record.takeoff_utc != takeoff_utc_time:
                            logger.info(f"⚠️  FDM Takeoff already set for flight {flight_no}: {existing_record.takeoff_utc}, ignoring {takeoff_utc_time}")
                        
                        if touchdown_utc_time and not existing_record.touchdown_utc:
                            existing_record.touchdown_utc = touchdown_utc_time
                            timing_updated = True
                            logger.info(f"✅ FDM Updated touchdown for initiated flight {flight_no}: {touchdown_utc_time}")
                        elif touchdown_utc_time and existing_record.touchdown_utc != touchdown_utc_time:
                            logger.info(f"⚠️  FDM Touchdown already set for flight {flight_no}: {existing_record.touchdown_utc}, ignoring {touchdown_utc_time}")
                        
                        if ata_utc_time and not existing_record.ata_utc:
                            existing_record.ata_utc = ata_utc_time
                            timing_updated = True
                            logger.info(f"✅ FDM Updated ATA for initiated flight {flight_no}: {ata_utc_time}")
                        elif ata_utc_time and existing_record.ata_utc != ata_utc_time:
                            logger.info(f"⚠️  FDM ATA already set for flight {flight_no}: {existing_record.ata_utc}, ignoring {ata_utc_time}")
                        
                        # Allow estimated time updates (ETD/ETA) even for initiated flights
                        if etd_utc_time and existing_record.etd_utc != etd_utc_time:
                            existing_record.etd_utc = etd_utc_time
                            timing_updated = True
                            logger.info(f"✅ FDM Updated ETD for initiated flight {flight_no}: {etd_utc_time}")
                        
                        if eta_utc_time and existing_record.eta_utc != eta_utc_time:
                            existing_record.eta_utc = eta_utc_time
                            timing_updated = True
                            logger.info(f"✅ FDM Updated ETA for initiated flight {flight_no}: {eta_utc_time}")
                        
                        if timing_updated:
                            existing_record.save()
                            logger.info(f"💾 Saved FDM timing updates for initiated flight {flight_no}")
                        else:
                            logger.info(f"📋 No FDM timing updates needed for initiated flight {flight_no}")
                        
                        # Skip to next record - don't update core fields
                        continue
                    
                    # FLIGHT NOT INITIATED - Allow all updates as before
                    logger.info(f"✅ FDM Flight {flight_no} not yet initiated. Updating all fields as normal.")
                    
                    updated = False
                    changes_made = []
                    
                    # Update core fields (only for non-initiated flights)
                    if existing_record.tail_no != tail_no:
                        changes_made.append(f"tail_no: '{existing_record.tail_no}' -> '{tail_no}'")
                        existing_record.tail_no = tail_no
                        updated = True
                    
                    if existing_record.dep_code_iata != dep_code_iata:
                        changes_made.append(f"dep_code_iata: '{existing_record.dep_code_iata}' -> '{dep_code_iata}'")
                        existing_record.dep_code_iata = dep_code_iata
                        updated = True
                    
                    if existing_record.arr_code_iata != arr_code_iata:
                        changes_made.append(f"arr_code_iata: '{existing_record.arr_code_iata}' -> '{arr_code_iata}'")
                        existing_record.arr_code_iata = arr_code_iata
                        updated = True
                    
                    if existing_record.sa_date_utc != sa_date_utc:
                        changes_made.append(f"sa_date_utc: '{existing_record.sa_date_utc}' -> '{sa_date_utc}'")
                        existing_record.sa_date_utc = sa_date_utc
                        updated = True
                    
                    # Update scheduled times
                    if std_utc_time and existing_record.std_utc != std_utc_time:
                        changes_made.append(f"std_utc: '{existing_record.std_utc}' -> '{std_utc_time}'")
                        existing_record.std_utc = std_utc_time
                        updated = True
                    
                    if sta_utc_time and existing_record.sta_utc != sta_utc_time:
                        changes_made.append(f"sta_utc: '{existing_record.sta_utc}' -> '{sta_utc_time}'")
                        existing_record.sta_utc = sta_utc_time
                        updated = True
                    
                    # Update flight type
                    if flight_type and existing_record.flight_type != flight_type:
                        changes_made.append(f"flight_type: '{existing_record.flight_type}' -> '{flight_type}'")
                        existing_record.flight_type = flight_type
                        updated = True
                    
                    # Update estimated times
                    if etd_utc_time and existing_record.etd_utc != etd_utc_time:
                        changes_made.append(f"etd_utc: '{existing_record.etd_utc}' -> '{etd_utc_time}'")
                        existing_record.etd_utc = etd_utc_time
                        updated = True
                    
                    if eta_utc_time and existing_record.eta_utc != eta_utc_time:
                        changes_made.append(f"eta_utc: '{existing_record.eta_utc}' -> '{eta_utc_time}'")
                        existing_record.eta_utc = eta_utc_time
                        updated = True
                    
                    # Update actual timing fields (if provided and different)
                    if atd_utc_time and existing_record.atd_utc != atd_utc_time:
                        changes_made.append(f"atd_utc: '{existing_record.atd_utc}' -> '{atd_utc_time}'")
                        existing_record.atd_utc = atd_utc_time
                        updated = True
                    
                    if takeoff_utc_time and existing_record.takeoff_utc != takeoff_utc_time:
                        changes_made.append(f"takeoff_utc: '{existing_record.takeoff_utc}' -> '{takeoff_utc_time}'")
                        existing_record.takeoff_utc = takeoff_utc_time
                        updated = True
                    
                    if touchdown_utc_time and existing_record.touchdown_utc != touchdown_utc_time:
                        changes_made.append(f"touchdown_utc: '{existing_record.touchdown_utc}' -> '{touchdown_utc_time}'")
                        existing_record.touchdown_utc = touchdown_utc_time
                        updated = True
                    
                    if ata_utc_time and existing_record.ata_utc != ata_utc_time:
                        changes_made.append(f"ata_utc: '{existing_record.ata_utc}' -> '{ata_utc_time}'")
                        existing_record.ata_utc = ata_utc_time
                        updated = True
                    
                    if updated:
                        existing_record.save()
                        logger.info(
                            f"💾 Updated FDM record for flight {flight_no} on {sd_date_utc}. "
                            f"Changes: {len(changes_made)} fields updated"
                        )
                        if logger.isEnabledFor(logging.DEBUG):
                            for change in changes_made:
                                logger.debug(f"   - {change}")
                    else:
                        logger.info(f"📋 No changes needed for FDM record {flight_no} on {sd_date_utc}")
                
                else:
                    # CREATE NEW FDM RECORD - No existing record found
                    logger.info(f"➕ Creating new FDM flight record: {flight_no} on {sd_date_utc}")
                    
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
                        source_type="FDM",
                        raw_content=','.join(row)
                    )
                    logger.info(f"✅ Successfully created new FDM flight record: {flight_no} on {sd_date_utc}")

            except Exception as e:
                logger.error(f"❌ Error processing FDM line {line_num}: {e} - Row: {row}", exc_info=True)
                continue

        logger.info("🎉 FDM flight schedule file processed successfully with data protection active")

    except Exception as e:
        logger.error(f"❌ Error processing FDM flight schedule file: {e}", exc_info=True)


# Optional: FDM-specific helper function
def is_fdm_flight_initiated(fdm_flight_record):
    """
    Helper function to check if an FDM flight has been initiated
    Returns True if flight has any actual timing data
    """
    return any([
        fdm_flight_record.atd_utc,      # Actual Time of Departure
        fdm_flight_record.takeoff_utc,  # Takeoff time
        fdm_flight_record.touchdown_utc, # Landing time
        fdm_flight_record.ata_utc       # Actual Time of Arrival
    ])


# Optional: FDM flight timing summary
def get_fdm_flight_timing_summary(fdm_flight_record):
    """
    Get a comprehensive summary of FDM flight timing status
    """
    return {
        'initiated': is_fdm_flight_initiated(fdm_flight_record),
        'scheduled_complete': bool(fdm_flight_record.std_utc and fdm_flight_record.sta_utc),
        'estimated_complete': bool(fdm_flight_record.etd_utc and fdm_flight_record.eta_utc),
        'actual_timings': {
            'atd_present': bool(fdm_flight_record.atd_utc),
            'takeoff_present': bool(fdm_flight_record.takeoff_utc),
            'touchdown_present': bool(fdm_flight_record.touchdown_utc),
            'ata_present': bool(fdm_flight_record.ata_utc),
        },
        'completion_percentage': sum([
            bool(fdm_flight_record.atd_utc),
            bool(fdm_flight_record.takeoff_utc),
            bool(fdm_flight_record.touchdown_utc),
            bool(fdm_flight_record.ata_utc)
        ]) * 25,  # Each timing represents 25% completion
        'flight_type': fdm_flight_record.flight_type
    }



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
            # Skip the header line that starts with "ACCONFIG,DAY,DEP"
            if line.startswith("ACCONFIG,DAY,DEP"):
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




        