import logging
from exchangelib import FileAttachment
from aimsintegration.models import AirportData, FlightData, AcarsMessage, CargoFlightData,FdmFlightData
from datetime import datetime
import re
from django.core.mail import send_mail
logger = logging.getLogger(__name__)

# def process_airport_file(attachment):
#     """
#     Process the airport data file and store records in the AirportData table.
#     """
#     try:
#         content = attachment.content.decode('utf-8').splitlines()

#         logger.info(f"Processing {len(content)} lines of airport data.")

#         for line in content:
#             fields = line.split()
#             logger.info(f"Processing line: {line}")

#             if len(fields) >= 3:
#                 icao_code = fields[0]
#                 iata_code = fields[1]
#                 airport_name = " ".join(fields[2:])
                
#                 # Log the data extracted from the line
#                 logger.info(f"Extracted data - ICAO: {icao_code}, IATA: {iata_code}, Airport Name: {airport_name}")

#                 # Insert or update the airport data
#                 airport, created = AirportData.objects.get_or_create(
#                     iata_code=iata_code,
#                     defaults={
#                         'icao_code': icao_code,
#                         'airport_name': airport_name,
#                         'raw_content': line  # Store raw line for reference
#                     }
#                 )

#                 if created:
#                     logger.info(f"Inserted new airport record for {iata_code} - {airport_name}")
#                 else:
#                     airport.icao_code = icao_code
#                     airport.airport_name = airport_name
#                     airport.save()
#                     logger.info(f"Updated existing airport record for {iata_code}")

#         logger.info("Airport data processed successfully.")
#     except Exception as e:
#         logger.error(f"Error processing airport data file: {e}")

import csv
from io import StringIO

import csv
from io import StringIO

def process_airport_file(attachment):
    """
    Process the airport data file and store records in the AirportData table.
    Updated to handle CSV format with quoted fields.
    """
    try:
        content = attachment.content.decode('utf-8')
        logger.info(f"Processing airport data file with {len(content)} characters.")
        
        # Split content into lines and process each line
        lines = content.strip().splitlines()
        processed_count = 0
        error_count = 0
        
        for line_num, line in enumerate(lines, 1):
            try:
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Split by comma
                fields = line.split(',')
                
                # Skip rows with insufficient data
                if len(fields) < 4:
                    logger.warning(f"Line {line_num}: Insufficient data - {line}")
                    continue
                
                # Extract fields and strip any quotes
                iata_code = fields[0].strip().strip('"') if fields[0] else None
                icao_code = fields[1].strip().strip('"') if fields[1] else None
                airport_name = fields[2].strip().strip('"') if fields[2] else None
                country_code = fields[3].strip().strip('"') if fields[3] else None
                
                # Skip rows with missing essential data
                if not iata_code or not airport_name:
                    logger.warning(f"Line {line_num}: Missing IATA code or airport name - IATA: {iata_code}, Name: {airport_name}")
                    continue
                
                # Handle empty ICAO codes (some entries have empty ICAO)
                if not icao_code:
                    icao_code = None
                
                # Clean up airport name (remove extra spaces)
                airport_name = ' '.join(airport_name.split())
                
                # Log the data extracted from the line
                logger.info(f"Line {line_num}: IATA: {iata_code}, ICAO: {icao_code}, Airport: {airport_name}, Country: {country_code}")
                
                # Insert or update the airport data
                airport, created = AirportData.objects.get_or_create(
                    iata_code=iata_code,
                    defaults={
                        'icao_code': icao_code,
                        'airport_name': airport_name,
                        'raw_content': line  # Store original line
                    }
                )
                
                if created:
                    logger.info(f"Inserted new airport record for {iata_code} - {airport_name}")
                    processed_count += 1
                else:
                    # Update existing record
                    updated = False
                    if airport.icao_code != icao_code:
                        airport.icao_code = icao_code
                        updated = True
                    if airport.airport_name != airport_name:
                        airport.airport_name = airport_name
                        updated = True
                    
                    if updated:
                        airport.save()
                        logger.info(f"Updated existing airport record for {iata_code}")
                        processed_count += 1
                    else:
                        logger.info(f"No changes needed for airport {iata_code}")
                        
            except Exception as e:
                error_count += 1
                logger.error(f"Line {line_num}: Error processing line '{line}': {e}")
        
        logger.info(f"Airport data processing complete: {processed_count} processed, {error_count} errors")
        
    except Exception as e:
        logger.error(f"Error processing airport data file: {e}")
        raise


from datetime import datetime
from django.db import transaction
from .models import AirportData, FlightData
import logging

logger = logging.getLogger(__name__)

# def process_flight_schedule_file(attachment):
#     """
#     Process the flight schedule file with proper duplicate prevention.
#     Ensures one unique flight number per day per route.
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
#                 flight_leg_code = fields[3] if len(fields) > 3 else None
#                 dep_code_icao = fields[4]
#                 arr_code_icao = fields[5]
#                 std = fields[6]
#                 sta = fields[7]
#                 flight_service_type = fields[8] if len(fields) > 8 else None
#                 etd = fields[9] if len(fields) > 9 else None
#                 eta = fields[10] if len(fields) > 10 else None
#                 atd = fields[11] if len(fields) > 11 else None
#                 takeoff = fields[12] if len(fields) > 12 else None
#                 touchdown = fields[13] if len(fields) > 13 else None
#                 ata = fields[14] if len(fields) > 14 else None
#                 arrival_date = fields[15] if len(fields) > 15 else None

#                 logger.info(f"Processing: Flight {flight_no}, Tail {tail_no}, Date {flight_date}, Route {dep_code_icao}-{arr_code_icao}")

#                 # Parse dates and times
#                 try:
#                     sd_date_utc = datetime.strptime(flight_date, "%m%d%Y").date()
#                     sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
#                     std_utc = datetime.strptime(std, "%H:%M").time()
#                     sta_utc = datetime.strptime(sta, "%H:%M").time()
#                     atd_utc = datetime.strptime(atd, "%H:%M").time() if atd else None
#                     takeoff_utc = datetime.strptime(takeoff, "%H:%M").time() if takeoff else None
#                     touchdown_utc = datetime.strptime(touchdown, "%H:%M").time() if touchdown else None
#                     ata_utc = datetime.strptime(ata, "%H:%M").time() if ata else None
#                 except ValueError as e:
#                     logger.error(f"Skipping line {line_num} due to date/time format error: {e} - {line}")
#                     continue

#                 # Fetch airport data
#                 dep_airport = AirportData.objects.filter(icao_code=dep_code_icao).first()
#                 arr_airport = AirportData.objects.filter(icao_code=arr_code_icao).first()

#                 if not dep_airport or not arr_airport:
#                     logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code_icao} or {arr_code_icao}")
#                     continue

#                 dep_code_iata = dep_airport.iata_code
#                 arr_code_iata = arr_airport.iata_code

#                 # FIXED: Proper duplicate detection - flight number should be unique per day per route
#                 # Don't include tail_no, std_utc, sta_utc in the filter as these can change during rescheduling
#                 flight_no = flight_no+flight_leg_code if flight_leg_code else flight_no
#                 existing_record = FlightData.objects.filter(
#                     flight_no=flight_no,
#                     sd_date_utc=sd_date_utc,
#                     dep_code_icao=dep_code_icao,
#                     arr_code_icao=arr_code_icao,
#                 ).first()

#                 # Process record with transaction.atomic()
#                 with transaction.atomic():
#                     if existing_record:
#                         logger.info(f"Found existing record for flight {flight_no} on {sd_date_utc}. Updating...")
                        
#                         # Always update scheduled data (this handles reschedules)
#                         updated = False
                        
#                         # Update tail number (aircraft swap)
#                         if existing_record.tail_no != tail_no:
#                             logger.info(f"Updating tail number: {existing_record.tail_no} → {tail_no}")
#                             existing_record.tail_no = tail_no
#                             updated = True
                        
#                         # Update scheduled times (reschedule)
#                         if existing_record.std_utc != std_utc:
#                             logger.info(f"Updating STD: {existing_record.std_utc} → {std_utc}")
#                             existing_record.std_utc = std_utc
#                             updated = True
                            
#                         if existing_record.sta_utc != sta_utc:
#                             logger.info(f"Updating STA: {existing_record.sta_utc} → {sta_utc}")
#                             existing_record.sta_utc = sta_utc
#                             updated = True
                        
#                         # Update scheduled arrival date
#                         if sa_date_utc and existing_record.sa_date_utc != sa_date_utc:
#                             logger.info(f"Updating SA date: {existing_record.sa_date_utc} → {sa_date_utc}")
#                             existing_record.sa_date_utc = sa_date_utc
#                             updated = True
                        
#                         # Update IATA codes if they changed (unlikely but possible)
#                         if existing_record.dep_code_iata != dep_code_iata:
#                             existing_record.dep_code_iata = dep_code_iata
#                             updated = True
#                         if existing_record.arr_code_iata != arr_code_iata:
#                             existing_record.arr_code_iata = arr_code_iata
#                             updated = True
                        
#                         # IMPORTANT: Only update actual times if they don't exist yet
#                         # This prevents overwriting ACARS data with schedule data
#                         if atd_utc and not existing_record.atd_utc:
#                             logger.info(f"Setting initial ATD from schedule: {atd_utc}")
#                             existing_record.atd_utc = atd_utc
#                             updated = True
#                         elif atd_utc and existing_record.atd_utc:
#                             logger.info(f"Preserving existing ATD: {existing_record.atd_utc} (ignoring schedule ATD: {atd_utc})")
                        
#                         if takeoff_utc and not existing_record.takeoff_utc:
#                             logger.info(f"Setting initial takeoff from schedule: {takeoff_utc}")
#                             existing_record.takeoff_utc = takeoff_utc
#                             updated = True
#                         elif takeoff_utc and existing_record.takeoff_utc:
#                             logger.info(f"Preserving existing takeoff: {existing_record.takeoff_utc} (ignoring schedule: {takeoff_utc})")
                        
#                         if touchdown_utc and not existing_record.touchdown_utc:
#                             logger.info(f"Setting initial touchdown from schedule: {touchdown_utc}")
#                             existing_record.touchdown_utc = touchdown_utc
#                             updated = True
#                         elif touchdown_utc and existing_record.touchdown_utc:
#                             logger.info(f"Preserving existing touchdown: {existing_record.touchdown_utc} (ignoring schedule: {touchdown_utc})")
                        
#                         if ata_utc and not existing_record.ata_utc:
#                             logger.info(f"Setting initial ATA from schedule: {ata_utc}")
#                             existing_record.ata_utc = ata_utc
#                             updated = True
#                         elif ata_utc and existing_record.ata_utc:
#                             logger.info(f"Preserving existing ATA: {existing_record.ata_utc} (ignoring schedule: {ata_utc})")
                        
#                         # Update raw content for audit trail
#                         new_raw_content = ",".join(fields)
#                         if existing_record.raw_content != new_raw_content:
#                             existing_record.raw_content = new_raw_content
#                             updated = True
                        
#                         if updated:
#                             existing_record.save()
#                             logger.info(f"✅ Updated flight {flight_no} on {sd_date_utc}")
#                         else:
#                             logger.info(f"ℹ️  No changes needed for flight {flight_no} on {sd_date_utc}")
                    
#                     else:
#                         # Create a new record - this should only happen for truly new flights
#                         new_flight = FlightData.objects.create(
#                             flight_no=flight_no,
#                             tail_no=tail_no,
#                             dep_code_iata=dep_code_iata,
#                             dep_code_icao=dep_code_icao,
#                             arr_code_iata=arr_code_iata,
#                             arr_code_icao=arr_code_icao,
#                             sd_date_utc=sd_date_utc,
#                             sa_date_utc=sa_date_utc,
#                             std_utc=std_utc,
#                             sta_utc=sta_utc,
#                             atd_utc=atd_utc,
#                             takeoff_utc=takeoff_utc,
#                             touchdown_utc=touchdown_utc,
#                             ata_utc=ata_utc,
#                             source_type="FDM",
#                             raw_content=",".join(fields),
#                         )
#                         logger.info(f"✨ Created new flight record: {flight_no} on {sd_date_utc} (ID: {new_flight.id})")

#             except Exception as e:
#                 logger.error(f"❌ Error processing line {line_num}: {e} - {fields}", exc_info=True)
#                 continue

#         logger.info("✅ Flight schedule file processed successfully.")

#     except Exception as e:
#         logger.error(f"❌ Error processing flight schedule file: {e}", exc_info=True)



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
                flight_leg_code = fields[3] if len(fields) > 3 else None
                dep_code = fields[4]  # Could be IATA or ICAO
                arr_code = fields[5]  # Could be IATA or ICAO
                std = fields[6]
                sta = fields[7]
                flight_service_type = fields[8] if len(fields) > 8 else None
                etd = fields[9] if len(fields) > 9 else None
                eta = fields[10] if len(fields) > 10 else None
                atd = fields[11] if len(fields) > 11 else None
                takeoff = fields[12] if len(fields) > 12 else None
                touchdown = fields[13] if len(fields) > 13 else None
                ata = fields[14] if len(fields) > 14 else None
                arrival_date = fields[15] if len(fields) > 15 else None

                logger.info(f"Processing: Flight {flight_no}, Tail {tail_no}, Date {flight_date}, Route {dep_code}-{arr_code}")

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

                # Fetch airport data - try IATA first, then ICAO as fallback
                dep_airport = AirportData.objects.filter(iata_code=dep_code).first()
                if not dep_airport:
                    dep_airport = AirportData.objects.filter(icao_code=dep_code).first()

                arr_airport = AirportData.objects.filter(iata_code=arr_code).first()
                if not arr_airport:
                    arr_airport = AirportData.objects.filter(icao_code=arr_code).first()

                if not dep_airport or not arr_airport:
                    logger.warning(f"Skipping line {line_num} due to missing airport data: {dep_code} or {arr_code}")
                    continue

                # Get both IATA and ICAO codes for storage
                dep_code_iata = dep_airport.iata_code
                dep_code_icao = dep_airport.icao_code
                arr_code_iata = arr_airport.iata_code
                arr_code_icao = arr_airport.icao_code

                # FIXED: Proper duplicate detection - flight number should be unique per day per route
                # Don't include tail_no, std_utc, sta_utc in the filter as these can change during rescheduling
                flight_no = flight_no+flight_leg_code if flight_leg_code else flight_no
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

#         # FIXED: Only search yesterday and today (2 days only)
#         # ACARS should never update flights that haven't happened yet
#         search_dates = [
#             email_received_date + timedelta(days=i) 
#             for i in range(-1, 1)  # -1, 0 days (yesterday + today only)
#         ]

#         logger.info(f"Searching flights from {search_dates[0]} to {search_dates[-1]} for ACARS event {acars_event}")

#         # First, try to get matching flights with flight number and date range
#         flights = FlightData.objects.filter(
#             flight_no=flight_no,
#             tail_no=tail_number,
#             dep_code_iata=dep_code,
#             arr_code_iata=arr_code,
#             sd_date_utc__in=search_dates  # Now only includes yesterday + today
#         )

#         # If no flights found, try matching without flight number but with date range
#         if not flights.exists():
#             flights = FlightData.objects.filter(
#                 tail_no=tail_number,
#                 dep_code_iata=dep_code,
#                 arr_code_iata=arr_code,
#                 sd_date_utc__in=search_dates
#             )

#         # Additional safety check: Never update flights scheduled for future dates
#         if flights.exists():
#             # Filter out any flights scheduled for future dates as an extra safety measure
#             flights = flights.filter(sd_date_utc__lte=email_received_date)
            
#         if not flights.exists():
#             logger.info(f"No matching YESTERDAY/TODAY flights found for flight number: {flight_no}")
#             send_mail(
#                 subject=f"No matching flights found for flight number: {flight_no}",
#                 message=(
#                     f"Dear Team,\n\n"
#                     f"The ACARS message for flight {flight_no} could not be matched to any yesterday or today flights.\n"
#                     f"Message details:\n\n{message_body}\n\n"
#                     f"ACARS received date: {email_received_date}\n"
#                     f"Flight details: {flight_no}, Tail: {tail_number}, Route: {dep_code}-{arr_code}\n"
#                     f"ACARS Event: {acars_event}\n"
#                     f"Search range: {search_dates[0]} to {search_dates[-1]} (yesterday and today only)\n\n"
#                     f"Note: ACARS only processes flights from yesterday and today, not future flights.\n\n"
#                     f"Please review and update manually if needed.\n\n"
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

#         # Additional verification: Ensure we're not updating a future flight
#         if selected_flight.sd_date_utc > email_received_date:
#             logger.error(f"SAFETY CHECK FAILED: Attempted to update future flight {selected_flight.flight_no} "
#                         f"scheduled for {selected_flight.sd_date_utc} with ACARS received on {email_received_date}")
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
        dep_code, arr_code = extract_departure_and_arrival_codes(message_body)  # These are ICAO codes
        tail_number = extract_tail_number(message_body)

        logger.info(f"Extracted: Flight={flight_no}, Tail={tail_number}, Event={acars_event}")
        logger.info(f"Extracted codes: DEP={dep_code}, ARR={arr_code} (ICAO format)")

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

        # FIXED: Search using ICAO fields instead of IATA fields
        # First, try to get matching flights with flight number and date range
        flights = FlightData.objects.filter(
            flight_no=flight_no,
            tail_no=tail_number,
            dep_code_icao=dep_code,  # Changed from dep_code_iata to dep_code_icao
            arr_code_icao=arr_code,  # Changed from arr_code_iata to arr_code_icao
            sd_date_utc__in=search_dates  # Now only includes yesterday + today
        )

        logger.info(f"First search found {flights.count()} flights matching all criteria")

        # If no flights found, try matching without flight number but with date range
        if not flights.exists():
            flights = FlightData.objects.filter(
                tail_no=tail_number,
                dep_code_icao=dep_code,  # Changed from dep_code_iata to dep_code_icao
                arr_code_icao=arr_code,  # Changed from arr_code_iata to arr_code_icao
                sd_date_utc__in=search_dates
            )
            logger.info(f"Second search (without flight number) found {flights.count()} flights")

        # Additional safety check: Never update flights scheduled for future dates
        if flights.exists():
            # Filter out any flights scheduled for future dates as an extra safety measure
            flights = flights.filter(sd_date_utc__lte=email_received_date)
            logger.info(f"After safety filter: {flights.count()} flights remain")
            
        if not flights.exists():
            logger.info(f"No matching YESTERDAY/TODAY flights found for flight number: {flight_no}")
            send_mail(
                subject=f"No matching flights found for flight number: {flight_no}",
                message=(
                    f"Dear Team,\n\n"
                    f"The ACARS message for flight {flight_no} could not be matched to any yesterday or today flights.\n"
                    f"Message details:\n\n{message_body}\n\n"
                    f"ACARS received date: {email_received_date}\n"
                    f"Flight details: {flight_no}, Tail: {tail_number}, Route: {dep_code}-{arr_code} (ICAO)\n"
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
        logger.info(f"Successfully processed ACARS message for flight {flight_no}")

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

#                 # Parse dates and times - FIXED DATE FORMAT
#                 # Changed from "%m/%d/%Y" to "%m%d%Y" for flight_date to match CSV format
#                 sd_date_utc = datetime.strptime(flight_date, "%m%d%Y").date() if flight_date else None
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
    Process the FDM flight schedule file using a comma delimiter.
    Insert into FdmFlightData and update missing actual timings in FlightData.
    """
    try:
        content = attachment.content.decode('utf-8').splitlines()
        logger.info("Starting to process the FDM flight schedule file...")

        reader = csv.reader(content)

        for line_num, row in enumerate(reader, start=1):
            try:
                # Extract fields - CORRECTED INDICES after adding flight leg code
                flight_date = row[0].strip() if len(row) > 0 else ""
                tail_no = row[1].strip()[:10] if len(row) > 1 else ""         # KEPT ORIGINAL SIZE
                flight_no = row[2].strip()[:6] if len(row) > 2 else ""        # KEPT ORIGINAL SIZE
                flight_leg_code = row[3].strip()[:1] if len(row) > 3 else ""  # NEW FIELD
                dep_code_icao = row[4].strip()[:4] if len(row) > 4 else ""    # INDEX SHIFTED
                arr_code_icao = row[5].strip()[:4] if len(row) > 5 else ""    # INDEX SHIFTED
                std_utc = row[6].strip() if len(row) > 6 else ""              # INDEX SHIFTED
                sta_utc = row[7].strip() if len(row) > 7 else ""              # INDEX SHIFTED
                flight_type = row[8].strip()[:10] if len(row) > 8 else ""     # KEPT ORIGINAL SIZE
                etd_utc = row[9].strip() if len(row) > 9 else ""              # INDEX SHIFTED
                eta_utc = row[10].strip() if len(row) > 10 else ""            # INDEX SHIFTED
                atd_utc = row[11].strip() if len(row) > 11 else ""            # INDEX SHIFTED
                takeoff_utc = row[12].strip() if len(row) > 12 else ""        # INDEX SHIFTED
                touchdown_utc = row[13].strip() if len(row) > 13 else ""      # INDEX SHIFTED
                ata_utc = row[14].strip() if len(row) > 14 else ""            # INDEX SHIFTED
                arrival_date = row[15].strip() if len(row) > 15 else ""       # INDEX SHIFTED

                # Parse dates and times - FIXED DATE FORMATS
                # Using MMddyyyy format for flight_date and MM/dd/yyyy for arrival_date as per table
                sd_date_utc = datetime.strptime(flight_date, "%m%d%Y").date() if flight_date else None
                sa_date_utc = datetime.strptime(arrival_date, "%m/%d/%Y").date() if arrival_date else None
                
                # Parse time fields - all use HH:mm format
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


                flight_no = flight_no+flight_leg_code if flight_leg_code else flight_no
                # Define unique criteria - ADDED flight_leg_code
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
                        logger.info(f"Updated FDM record for flight {flight_no}{flight_leg_code} on {sd_date_utc}.")
                    else:
                        logger.info(f"No changes for FDM record {flight_no}{flight_leg_code} on {sd_date_utc}.")
                else:
                    # Create a new FdmFlightData record - ADDED flight_leg_code
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
                    logger.info(f"Created new FDM flight record: {flight_no}{flight_leg_code} on {sd_date_utc}.")

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

                # NEW FIELDS - Parse the two additional fields (indices 29 and 30)
                actual_block_time_mvt = parse_time(fields[29], "Actual Block Time MVT") if len(fields) > 29 and fields[29] else None
                flight_time_mvt = parse_time(fields[30], "Flight Time MVT") if len(fields) > 30 and fields[30] else None

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
                        # ADD THE NEW FIELDS TO UPDATE DICTIONARY
                        'actual_block_time_mvt': actual_block_time_mvt,
                        'flight_time_mvt': flight_time_mvt,
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
                        aircraft_config=aircraft_config,
                        # ADD THE NEW FIELDS TO CREATE STATEMENT
                        actual_block_time_mvt=format_time(actual_block_time_mvt),
                        flight_time_mvt=format_time(flight_time_mvt),
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
# CLEAN QATAR APIS UTILS 
#==================================================================================
import re
import csv
import logging
from datetime import datetime
from django.db import transaction
from exchangelib import FileAttachment
from django.conf import settings
from .models import *

logger = logging.getLogger(__name__)

def extract_plain_from_rtf(rtf_bytes):
    """
    Naively strip RTF control words and braces to get plain text lines.
    """
    raw = rtf_bytes.decode('utf-8', errors='ignore')
    # Remove RTF groups
    text = re.sub(r'{\\[^}]+}', '', raw)
    # Remove control words (e.g., \pard, \f0, etc.)
    text = re.sub(r'\\[a-zA-Z]+\d* ?', '', text)
    # Remove leftover braces
    text = re.sub(r'[{}]', '', text)
    # Split and return non-empty lines
    return [line.strip() for line in text.splitlines() if line.strip()]


def rtf_to_text(rtf_content):
    """
    Convert RTF content (string or bytes) into a plain text string.
    """
    if isinstance(rtf_content, str):
        rtf_bytes = rtf_content.encode('utf-8', errors='ignore')
    else:
        rtf_bytes = rtf_content
    lines = extract_plain_from_rtf(rtf_bytes)
    return "\n".join(lines)




def process_email_attachment(item, process_function):
    """
    General handler to process FileAttachment items.
    Now returns the result from the process_function.
    """
    try:
        result = None
        for attachment in getattr(item, 'attachments', []):
            if isinstance(attachment, FileAttachment):
                logger.info(f"Processing attachment: {attachment.name}")
                result = process_function(attachment)
        return result
    except Exception as e:
        logger.error(f"Error processing email attachment: {e}")
        return None


def process_job1008_file(attachment):
    """
    Process Job #1008 CSV crew details - CLEAN VERSION (NO ISSUING_STATE)
    Job 1008 contains exactly 7 fields from the specification
    """
    raw = attachment.content.decode('utf-8', errors='ignore')
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    
    logger.info(f"Processing Job 1008 with {len(lines)} lines")
    
    # Column mapping for Job 1008 - NO issuing_state field
    column_mapping = {
        0: 'crew_id',               # Crew Member ID
        1: 'passport_number',       # Passport Number
        2: None,                    # Empty column
        3: 'surname',               # Surname
        4: 'firstname',             # Firstname
        5: 'middlename',            # Middlename
        6: 'nationality',           # Nationality Of Person
        7: None,                    # Empty column
        8: 'passport_issue_date',   # Passport Issue Date
        9: None,                    # Empty column
        10: 'place_of_issue',       # Passport Place Of Issue
        11: None,                   # Empty column
        12: 'birth_place_cc'        # Birth Place Country Code
    }
    
    processed_count = 0
    error_count = 0
    
    for line_num, line in enumerate(lines, 1):
        try:
            # Split by comma
            parts = line.split(',')
            
            # Create row dictionary using our column mapping
            row = {}
            for idx, column_name in column_mapping.items():
                if column_name and idx < len(parts):
                    row[column_name] = parts[idx].strip()
                elif column_name:
                    row[column_name] = ''
            
            # Skip if no crew_id
            crew_id = row.get('crew_id', '').strip()
            if not crew_id:
                logger.warning(f"Line {line_num}: Skipping row with missing crew_id")
                continue
                
            # Process the data from Job 1008
            passport_number = row.get('passport_number', '').strip() or None
            surname = row.get('surname', '').strip() or None
            firstname = row.get('firstname', '').strip() or None
            middlename = row.get('middlename', '').strip() or None
            
            # Handle nationality - keep full text like "RWANDAN"
            nationality_raw = row.get('nationality', '').strip()
            nationality = nationality_raw[:20] if nationality_raw else None
            
            # Parse passport issue date dd/MM/yyyy
            issue_date_str = row.get('passport_issue_date', '').strip()
            passport_issue_date = None
            if issue_date_str:
                try:
                    passport_issue_date = datetime.strptime(issue_date_str, '%d/%m/%Y').date()
                except ValueError:
                    try:
                        # Try without century
                        passport_issue_date = datetime.strptime(issue_date_str, '%d/%m/%y').date()
                    except ValueError:
                        logger.warning(f"Line {line_num}: Invalid passport_issue_date '{issue_date_str}' for crew {crew_id}")
            
            # Handle place_of_issue - truncate to 16 chars max
            place_of_issue_raw = row.get('place_of_issue', '').strip()
            place_of_issue = place_of_issue_raw[:16] if place_of_issue_raw else None
            
            # Handle birth_place_cc - 3-letter country code
            birth_place_cc_raw = row.get('birth_place_cc', '').strip()
            birth_place_cc = birth_place_cc_raw[:3] if birth_place_cc_raw else None
            
            try:
                # Create/update crew detail with Job 1008 fields including passport_issue_date
                obj, created = QatarCrewDetail.objects.update_or_create(
                    crew_id=crew_id,
                    defaults={
                        'passport_number': passport_number,
                        'surname': surname,
                        'firstname': firstname,
                        'middlename': middlename,
                        'nationality': nationality,
                        'place_of_issue': place_of_issue,
                        'birth_place_cc': birth_place_cc,
                        'passport_issue_date': passport_issue_date,  # Now included since field exists
                    }
                )
                logger.info(f"Line {line_num}: {'Created' if created else 'Updated'} crew detail for {crew_id}")
                processed_count += 1
                
            except Exception as e:
                error_count += 1
                logger.error(f"Line {line_num}: Upsert failed for crew {crew_id}: {e}")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Line {line_num}: Error processing line '{line}': {e}")
    
    logger.info(f"Job 1008 processing complete: {processed_count} processed, {error_count} errors")



# def process_job97_file(attachment):
#     """
#     Parse Job #97 RTF for crew assignments and update QatarCrewDetail records.
#     Also create QatarFlightCrewAssignment records for the flight.
#     Uses Job 97 data for crew/tail/date and FlightData for operational details.
#     """
#     raw = attachment.content.decode('utf-8', errors='ignore')
#     logger.debug(f"RTF raw size: {len(raw)} characters")
#     text = rtf_to_text(raw)
#     lines = [ln for ln in text.splitlines() if ln.strip()]
#     logger.debug(f"Extracted {len(lines)} non-empty lines")

#     logger.info(f"Total lines in RTF: {len(lines)}")

#     # Extract flight information from RTF header
#     flight_number = None
#     flight_date = None
#     tail_number = None
    
#     # Parse flight details from header
#     for i, line in enumerate(lines[:25]):  # Check first 25 lines for flight info
#         line = line.strip()
        
#         # Look for flight number (e.g., "WB301")
#         if re.match(r'^WB\d+$', line):
#             flight_number = line
#             logger.info(f"Found flight number: {flight_number}")
        
#         # Look for date (e.g., "03/08/2025" or "24/07/2025")
#         elif re.match(r'^\d{2}/\d{2}/\d{4}$', line):
#             try:
#                 flight_date = datetime.strptime(line, '%d/%m/%Y').date()
#                 logger.info(f"Found flight date: {flight_date}")
#             except ValueError:
#                 pass
        
#         # Look for tail number (e.g., "9XR-WQ")
#         elif re.match(r'^\d[A-Z]{2}-[A-Z]{2}$', line):
#             tail_number = line
#             logger.info(f"Found tail number: {tail_number}")

#     logger.info(f"Job 97 flight info - Number: {flight_number}, Date: {flight_date}, Tail: {tail_number}")

#     crew_entries = []
    
#     # Look for the start of crew data - after "EXPIRY" header
#     crew_data_started = False
#     i = 0
    
#     while i < len(lines):
#         line = lines[i].strip()
        
#         # Skip until we find the crew data section
#         if not crew_data_started:
#             if "EXPIRY" in line:
#                 crew_data_started = True
#                 logger.info(f"Crew data section starts after line {i}: {line}")
#             i += 1
#             continue
        
#         # Skip location markers, empty lines, and section dividers
#         if (line in ['DOH', 'KGL', 'Departure Place:', 'Arrival Place:', 'Embarking:', 
#                     'Disembarking:', 'Through on same flight', 'ON THIS STAGE'] or 
#             line.startswith('...') or 
#             not line or
#             'DECLARATION OF HEALTH' in line or
#             'FOR OFFICIAL USE' in line):
#             i += 1
#             continue
        
#         # Check if this line is a crew ID (3-4 digits)
#         if line.isdigit() and len(line) >= 3 and len(line) <= 4:
#             crew_id = line
#             logger.info(f"Found crew ID: {crew_id} at line {i}")
            
#             # Initialize crew data variables
#             name = None
#             role = None
#             passport = None
#             birth_date = None
#             gender = None
#             nationality = None
#             expiry = None
            
#             # Parse the next lines for this crew member's data
#             j = i + 1
            
#             # Look for name line (next non-empty line that's not a section marker)
#             while j < len(lines):
#                 next_line = lines[j].strip()
#                 if next_line and not next_line.startswith('...') and \
#                    next_line not in ['DOH', 'KGL', 'Departure Place:', 'Arrival Place:', 
#                                    'Embarking:', 'Disembarking:', 'Through on same flight']:
#                     # This should be the name line
#                     # Check if it contains an embedded role (uppercase 2-3 letter word at the end)
#                     name_parts = next_line.split()
#                     if len(name_parts) > 1 and len(name_parts[-1]) <= 3 and name_parts[-1].isupper():
#                         # Likely has embedded role
#                         embedded_role = name_parts[-1]
#                         name = ' '.join(name_parts[:-1]).strip()
#                         logger.info(f"  Found name with embedded role: {name} ({embedded_role})")
#                     else:
#                         name = next_line.strip()
#                         logger.info(f"  Found name: {name}")
#                     j += 1
#                     break
#                 j += 1
            
#             # Look for role line (if not embedded in name)
#             if j < len(lines):
#                 next_line = lines[j].strip()
#                 # Check if it's a short uppercase string (likely a role)
#                 if len(next_line) <= 3 and next_line.isupper() and next_line.isalpha():
#                     role = next_line
#                     logger.info(f"  Found role: {role}")
#                     j += 1
#                 elif 'embedded_role' in locals():
#                     role = embedded_role
#                     logger.info(f"  Using embedded role: {role}")
            
#             # Look for passport line (alphanumeric string, often starting with letters)
#             if j < len(lines):
#                 next_line = lines[j].strip()
#                 # Passport: alphanumeric, usually 6+ chars, may start with letters
#                 if len(next_line) >= 6 and next_line.isalnum():
#                     passport = next_line
#                     logger.info(f"  Found passport: {passport}")
#                     j += 1
            
#             # Look for birth date line (dd/mm/yy format)
#             if j < len(lines):
#                 next_line = lines[j].strip()
#                 if re.match(r'^\d{2}/\d{2}/\d{2}$', next_line):
#                     birth_date = next_line
#                     logger.info(f"  Found birth date: {birth_date}")
#                     j += 1
            
#             # Look for gender line (single character M or F)
#             if j < len(lines):
#                 next_line = lines[j].strip()
#                 if len(next_line) == 1 and next_line.upper() in ['M', 'F']:
#                     gender = next_line.upper()
#                     logger.info(f"  Found gender: {gender}")
#                     j += 1
            
#             # Look for nationality line (2-3 letter code or full country name)
#             if j < len(lines):
#                 next_line = lines[j].strip()
#                 # Nationality: 2-3 letter code OR longer country name (all uppercase/title case)
#                 if (len(next_line) >= 2 and next_line.isalpha() and 
#                     (next_line.isupper() or next_line.istitle())):
#                     nationality = next_line
#                     logger.info(f"  Found nationality: {nationality}")
#                     j += 1
            
#             # Look for expiry date line (dd/mm/yy format)
#             if j < len(lines):
#                 next_line = lines[j].strip()
#                 if re.match(r'^\d{2}/\d{2}/\d{2}$', next_line):
#                     expiry = next_line
#                     logger.info(f"  Found expiry: {expiry}")
#                     j += 1
            
#             # Create crew entry if we have minimum required data
#             if crew_id and name:
#                 crew_entry = {
#                     'crew_id': crew_id,
#                     'name': name,
#                     'role': role,
#                     'passport': passport,
#                     'birth_date': birth_date,
#                     'gender': gender,
#                     'nationality': nationality,
#                     'expiry': expiry
#                 }
#                 crew_entries.append(crew_entry)
#                 logger.info(f"Successfully parsed crew entry: {crew_entry}")
            
#             # Move to the next position
#             i = j
#         else:
#             i += 1

#     logger.info(f"Found {len(crew_entries)} crew entries in Job 97")

#     # Find the flight record using Job 97 data (tail, flight number, date)
#     flight_record = None
#     if flight_number and flight_date and tail_number:
#         try:
#             # Extract numeric flight number (remove WB prefix if present)
#             numeric_flight_no = flight_number[2:] if flight_number.startswith('WB') else flight_number
#             logger.info(f"Searching for flight using Job 97 data - Flight: {numeric_flight_no}, Date: {flight_date}, Tail: {tail_number}")
            
#             # Primary search: Match flight number, date, and tail number
#             flight_record = FlightData.objects.filter(
#                 flight_no=numeric_flight_no,
#                 sd_date_utc=flight_date,
#                 tail_no=tail_number
#             ).first()
            
#             if not flight_record:
#                 # Fallback: Try without tail number (in case tail numbers don't match exactly)
#                 flight_record = FlightData.objects.filter(
#                     flight_no=numeric_flight_no,
#                     sd_date_utc=flight_date
#                 ).first()
                
#                 if flight_record:
#                     logger.info(f"Found flight record without tail match: {flight_record}")
#                 else:
#                     logger.warning(f"Flight record not found for {numeric_flight_no} on {flight_date}")
#             else:
#                 logger.info(f"Found exact flight record match: {flight_record}")
                
#         except Exception as e:
#             logger.error(f"Error finding flight record: {e}")

#     # Update crew details and create flight assignments
#     updated_count = 0
#     assignment_count = 0
    
#     for entry in crew_entries:
#         try:
#             # Parse dates
#             birth = None
#             expiry = None
            
#             if entry['birth_date']:
#                 try:
#                     birth = datetime.strptime(entry['birth_date'], '%d/%m/%y').date()
#                 except Exception as e:
#                     logger.warning(f"Could not parse birth date '{entry['birth_date']}': {e}")
            
#             if entry['expiry']:
#                 try:
#                     expiry = datetime.strptime(entry['expiry'], '%d/%m/%y').date()
#                 except Exception as e:
#                     logger.warning(f"Could not parse expiry date '{entry['expiry']}': {e}")

#             # Update the existing crew detail record with Job 97 data
#             crew_detail = QatarCrewDetail.objects.filter(crew_id=entry['crew_id']).first()
#             if crew_detail:
#                 updated = False
#                 if birth and not crew_detail.birth_date:
#                     crew_detail.birth_date = birth
#                     updated = True
#                 if entry['gender'] and not crew_detail.sex:
#                     crew_detail.sex = entry['gender']
#                     updated = True
#                 if expiry and not crew_detail.passport_expiry:
#                     crew_detail.passport_expiry = expiry
#                     updated = True
#                 # Update nationality_code from Job 97 (3-letter code like RWA, UKR)
#                 if entry['nationality'] and (not crew_detail.nationality_code or crew_detail.nationality_code != entry['nationality']):
#                     crew_detail.nationality_code = entry['nationality']
#                     updated = True
                
#                 if updated:
#                     crew_detail.save()
#                     updated_count += 1
#                     logger.info(f"Updated crew detail for {entry['crew_id']} - {entry['name']} ({entry['role']}) - Nationality Code: {entry['nationality']}")
#                 else:
#                     logger.info(f"No updates needed for crew detail {entry['crew_id']} - {entry['name']} ({entry['role']})")
#             else:
#                 logger.warning(f"No crew detail found for {entry['crew_id']} from Job 97")

#             # Create flight crew assignment if we have a flight record
#             if flight_record:
#                 try:
#                     # Use Job 97 data for crew assignment and FlightData for operational details
#                     assignment, created = QatarFlightDetails.objects.update_or_create(
#                         crew_id=entry['crew_id'],
#                         flight=flight_record,
#                         defaults={
#                             # From Job 97 (RTF)
#                             'tail_no': tail_number,  # From Job 97
#                             'dep_date_utc': flight_date,  # From Job 97
#                             'arr_date_utc': flight_date,  # Assume same day
#                             # From FlightData (operational details)
#                             'std_utc': flight_record.std_utc,  # From FlightData
#                             'sta_utc': flight_record.sta_utc,  # From FlightData
#                         }
#                     )
#                     if created:
#                         assignment_count += 1
#                         logger.info(f"Created flight assignment for {entry['crew_id']} - Job97: {tail_number}/{flight_date}, FlightData: {flight_record.dep_code_iata}->{flight_record.arr_code_iata} {flight_record.std_utc}-{flight_record.sta_utc}")
#                     else:
#                         assignment_count += 1  # Count updates too
#                         logger.info(f"Updated flight assignment for {entry['crew_id']} - Job97: {tail_number}/{flight_date}, FlightData: {flight_record.dep_code_iata}->{flight_record.arr_code_iata} {flight_record.std_utc}-{flight_record.sta_utc}")
#                 except Exception as e:
#                     logger.error(f"Failed to create flight assignment for {entry['crew_id']}: {e}")
                
#         except Exception as e:
#             logger.error(f"Failed to process crew member {entry['crew_id']}: {e}")

#     logger.info(f"Job 97 processing complete: Updated {updated_count} crew records, processed {assignment_count} flight assignments")
    
#     if flight_record:
#         logger.info(f"Final flight details - Job97: {flight_number}({tail_number}) on {flight_date} | FlightData: {flight_record.dep_code_iata}->{flight_record.arr_code_iata} STD:{flight_record.std_utc} STA:{flight_record.sta_utc}")

#     return flight_date


def process_job97_file(attachment):
    """
    Parse Job #97 RTF for crew assignments and update QatarCrewDetail records.
    RTF converts to multi-line format with each field on a separate line.
    """
    raw = attachment.content.decode('utf-8', errors='ignore')
    text = rtf_to_text(raw)
    lines = [ln for ln in text.splitlines() if ln.strip()]

    logger.info(f"Total lines in RTF: {len(lines)}")

    # Extract flight information from RTF header
    flight_number = None
    flight_date = None
    tail_number = None
    
    for i, line in enumerate(lines[:25]):
        line = line.strip()
        
        if re.match(r'^WB\d+$', line):
            flight_number = line
            logger.info(f"Found flight number: {flight_number}")
        
        elif re.match(r'^\d{2}/\d{2}/\d{4}$', line):
            try:
                flight_date = datetime.strptime(line, '%d/%m/%Y').date()
                logger.info(f"Found flight date: {flight_date}")
            except ValueError:
                pass
        
        elif re.match(r'^\d[A-Z]{2}-[A-Z]{2}$', line):
            tail_number = line
            logger.info(f"Found tail number: {tail_number}")

    logger.info(f"Job 97 flight info - Number: {flight_number}, Date: {flight_date}, Tail: {tail_number}")

    crew_entries = []
    crew_data_started = False
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Start looking for crew after "EXPIRY" or "ON THIS STAGE"
        if not crew_data_started:
            if "EXPIRY" in line or "ON THIS STAGE" in line:
                crew_data_started = True
                logger.info(f"Crew data section started at line {i}")
            i += 1
            continue
        
        # Stop at declaration section
        if "DECLARATION OF HEALTH" in line or "FOR OFFICIAL USE" in line:
            break
        
        # Skip section markers
        if (line in ['DOH', 'KGL', 'Departure Place:', 'Arrival Place:', 
                    'Embarking:', 'Disembarking:', 'Through on same flight', 'Arrival Place:'] or 
            line.startswith('...') or 
            line == '*' or
            not line):
            i += 1
            continue
        
        # Check if this line is a crew ID (2-4 digits)
        if line.isdigit() and 2 <= len(line) <= 4:
            crew_id = line
            logger.info(f"\n🔍 Crew ID: {crew_id} at line {i}")
            
            # Initialize crew data
            name = None
            role = None
            passport = None
            birth_date = None
            gender = None
            nationality = None
            expiry = None
            
            j = i + 1
            
            # STEP 1: Get name (next non-empty, non-marker line)
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line and next_line not in ['*', '...', '......................'] and \
                   not next_line.startswith('...') and \
                   next_line not in ['DOH', 'KGL', 'Departure Place:', 'Arrival Place:', 
                                   'Embarking:', 'Disembarking:', 'Through on same flight']:
                    # Check if name has embedded role (e.g., "DURAN NAMIK KEMAL  PIC")
                    if '  ' in next_line:  # Double space might indicate embedded role
                        parts = next_line.split()
                        # Check if last part looks like a role (short, uppercase)
                        if len(parts) > 1 and len(parts[-1]) <= 3 and parts[-1].isupper():
                            embedded_role = parts[-1]
                            name = ' '.join(parts[:-1]).strip()
                            logger.info(f"   Name: {name} (embedded role: {embedded_role})")
                        else:
                            name = next_line
                            logger.info(f"   Name: {name}")
                    else:
                        name = next_line
                        logger.info(f"   Name: {name}")
                    j += 1
                    break
                elif next_line == '*':
                    j += 1  # Skip asterisk
                    continue
                j += 1
            
            # STEP 2: Get role (2-3 letter uppercase)
            if j < len(lines):
                next_line = lines[j].strip()
                if next_line == '*':
                    j += 1  # Skip asterisk
                    if j < len(lines):
                        next_line = lines[j].strip()
                
                if len(next_line) <= 3 and next_line.isupper() and next_line.isalpha():
                    role = next_line
                    logger.info(f"   Role: {role}")
                    j += 1
                elif 'embedded_role' in locals():
                    role = embedded_role
            
            # STEP 3: Get passport (alphanumeric, 6+ chars)
            if j < len(lines):
                next_line = lines[j].strip()
                if len(next_line) >= 6 and next_line.isalnum():
                    passport = next_line
                    logger.info(f"   Passport: {passport}")
                    j += 1
            
            # STEP 4: Get birth date (dd/mm/yy)
            if j < len(lines):
                next_line = lines[j].strip()
                if re.match(r'^\d{2}/\d{2}/\d{2}$', next_line):
                    birth_date = next_line
                    logger.info(f"   Birth: {birth_date}")
                    j += 1
            
            # STEP 5: Get gender (M or F)
            if j < len(lines):
                next_line = lines[j].strip()
                if len(next_line) == 1 and next_line.upper() in ['M', 'F']:
                    gender = next_line.upper()
                    logger.info(f"   Gender: {gender}")
                    j += 1
            
            # STEP 6: Get nationality (2-3 letters)
            if j < len(lines):
                next_line = lines[j].strip()
                if 2 <= len(next_line) <= 3 and next_line.isupper() and next_line.isalpha():
                    nationality = next_line
                    logger.info(f"   Nationality: {nationality}")
                    j += 1
            
            # STEP 7: Get expiry (dd/mm/yy)
            if j < len(lines):
                next_line = lines[j].strip()
                if re.match(r'^\d{2}/\d{2}/\d{2}$', next_line):
                    expiry = next_line
                    logger.info(f"   Expiry: {expiry}")
                    j += 1
            
            # Create crew entry
            if crew_id and name:
                crew_entry = {
                    'crew_id': crew_id,
                    'name': name,
                    'role': role,
                    'passport': passport,
                    'birth_date': birth_date,
                    'gender': gender,
                    'nationality': nationality,
                    'expiry': expiry
                }
                crew_entries.append(crew_entry)
                logger.info(f"   ✅ Parsed successfully")
            else:
                logger.warning(f"   ⚠️  Skipped - missing name")
            
            i = j
        else:
            i += 1

    logger.info(f"\n📊 Found {len(crew_entries)} crew entries in Job 97\n")

    # Find the flight record
    flight_record = None
    if flight_number and flight_date and tail_number:
        try:
            numeric_flight_no = flight_number[2:] if flight_number.startswith('WB') else flight_number
            logger.info(f"Searching for flight - Flight: {numeric_flight_no}, Date: {flight_date}, Tail: {tail_number}")
            
            flight_record = FlightData.objects.filter(
                flight_no=numeric_flight_no,
                sd_date_utc=flight_date,
                tail_no=tail_number
            ).first()
            
            if not flight_record:
                flight_record = FlightData.objects.filter(
                    flight_no=numeric_flight_no,
                    sd_date_utc=flight_date
                ).first()
                
            if flight_record:
                logger.info(f"✓ Found flight record: {flight_record}")
            else:
                logger.warning(f"⚠️  No flight record found")
                
        except Exception as e:
            logger.error(f"Error finding flight record: {e}")

    # Update crew details and create flight assignments
    updated_count = 0
    created_count = 0
    assignment_count = 0
    
    for entry in crew_entries:
        try:
            # Parse dates
            birth = None
            expiry_date = None
            
            if entry['birth_date']:
                try:
                    birth = datetime.strptime(entry['birth_date'], '%d/%m/%y').date()
                except Exception as e:
                    logger.warning(f"Could not parse birth date '{entry['birth_date']}': {e}")
            
            if entry['expiry']:
                try:
                    expiry_date = datetime.strptime(entry['expiry'], '%d/%m/%y').date()
                except Exception as e:
                    logger.warning(f"Could not parse expiry date '{entry['expiry']}': {e}")

            # Parse name
            surname = None
            firstname = None
            if entry['name']:
                name_parts = entry['name'].split()
                if len(name_parts) > 0:
                    surname = name_parts[-1]
                    firstname = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''

            # Get or create crew detail
            crew_detail = QatarCrewDetail.objects.filter(crew_id=entry['crew_id']).first()
            
            if crew_detail:
                # Update only if RTF has non-None data
                updated = False
                
                if birth and crew_detail.birth_date != birth:
                    crew_detail.birth_date = birth
                    updated = True
                
                if entry.get('gender') and crew_detail.sex != entry['gender']:
                    crew_detail.sex = entry['gender']
                    updated = True
                
                if expiry_date and crew_detail.passport_expiry != expiry_date:
                    crew_detail.passport_expiry = expiry_date
                    updated = True
                
                if entry.get('nationality') and crew_detail.nationality_code != entry['nationality']:
                    crew_detail.nationality_code = entry['nationality']
                    updated = True
                
                if entry.get('passport') and crew_detail.passport_number != entry['passport']:
                    crew_detail.passport_number = entry['passport']
                    updated = True
                
                if surname and crew_detail.surname != surname:
                    crew_detail.surname = surname
                    updated = True
                
                if firstname and crew_detail.firstname != firstname:
                    crew_detail.firstname = firstname
                    updated = True
                
                if updated:
                    crew_detail.save()
                    updated_count += 1
                    logger.info(f"✅ Updated crew {entry['crew_id']} - {entry['name']}")
                else:
                    logger.info(f"ℹ️  No updates for crew {entry['crew_id']}")
            else:
                # Create new crew member
                crew_detail = QatarCrewDetail.objects.create(
                    crew_id=entry['crew_id'],
                    surname=surname or '',
                    firstname=firstname or '',
                    passport_number=entry.get('passport'),
                    birth_date=birth,
                    sex=entry.get('gender'),
                    passport_expiry=expiry_date,
                    nationality_code=entry.get('nationality')
                )
                created_count += 1
                logger.info(f"✅ Created crew {entry['crew_id']}")

            # Create flight assignment
            if flight_record:
                try:
                    assignment, created = QatarFlightDetails.objects.update_or_create(
                        crew_id=entry['crew_id'],
                        flight=flight_record,
                        defaults={
                            'tail_no': tail_number,
                            'dep_date_utc': flight_date,
                            'arr_date_utc': flight_date,
                            'std_utc': flight_record.std_utc,
                            'sta_utc': flight_record.sta_utc,
                        }
                    )
                    assignment_count += 1
                except Exception as e:
                    logger.error(f"Failed to create assignment for {entry['crew_id']}: {e}")
                
        except Exception as e:
            logger.error(f"Failed to process crew {entry['crew_id']}: {e}")

    logger.info(f"\n✅ Job 97 complete: Updated {updated_count}, created {created_count}, {assignment_count} assignments")
    
    if flight_record:
        logger.info(f"   Flight: {flight_number}({tail_number}) on {flight_date}")
        logger.info(f"   Route: {flight_record.dep_code_iata}->{flight_record.arr_code_iata}\n")

    return flight_date, crew_entries




# def build_qatar_apis_edifact(direction, date):
#     """
#     Constructs and writes the EDIFACT PAXLST file for the given direction and date.
#     Updated to include all required crew fields and proper EDIFACT formatting.
#     Fixes applied based on Qatar Airways feedback:
#     - Added COM segment for reporting party contact info (CRITICAL FIX)
#     - Use 'F' for complete crew lists in UNH segment
#     - Remove extra separators in NAD segment when no address info
#     - Ensure all uppercase characters
#     - Enhanced validation and error handling
#     """
#     from django.core.mail import send_mail
#     from django.conf import settings
#     import logging
    
#     logger = logging.getLogger(__name__)
    
#     lines = ["UNA:+.?*'"]
#     ts = datetime.utcnow().strftime("%y%m%d:%H%M")
#     ctrl_ref = datetime.utcnow().strftime("%y%m%d%H%M")
#     sender = "RWANDAIR"
    
#     lines.append(f"UNB+UNOA:4+{sender}:ZZ+QATAPIS:ZZ+{ts}+{ctrl_ref}'")
#     lines.append(f"UNG+PAXLST+{sender}:ZZ+QATAPIS:ZZ+{ts}+{ctrl_ref}+UN+D:05B'")
    
#     msg_count = 0
#     validation_errors = []
    
#     # Get crew assignments for the specified direction and date
#     if direction == 'O':  # Outbound (KGL to DOH)
#         dep_codes = ['KGL', 'HRYR']  # IATA and ICAO for Kigali
#         arr_codes = ['DOH', 'OTHH']  # IATA and ICAO for Doha
#     else:  # Inbound (DOH to KGL)
#         dep_codes = ['DOH', 'OTHH']  # IATA and ICAO for Doha
#         arr_codes = ['KGL', 'HRYR']  # IATA and ICAO for Kigali
    
#     assignments = QatarFlightDetails.objects.filter(
#         dep_date_utc=date,
#         flight__dep_code_iata__in=dep_codes,
#         flight__arr_code_iata__in=arr_codes
#     ).select_related('flight')
    
#     # If no assignments found with IATA codes, try with ICAO codes
#     if not assignments.exists():
#         assignments = QatarFlightDetails.objects.filter(
#             dep_date_utc=date,
#             flight__dep_code_icao__in=dep_codes,
#             flight__arr_code_icao__in=arr_codes
#         ).select_related('flight')
    
#     # Final fallback - just use the date and let's see what flights we have
#     if not assignments.exists():
#         logger.warning(f"No crew assignments found with airport codes. Checking all flights on {date}")
#         all_assignments = QatarFlightDetails.objects.filter(
#             dep_date_utc=date
#         ).select_related('flight')
        
#         for asg in all_assignments:
#             logger.info(f"Available flight: {asg.flight.flight_no} - {asg.flight.dep_code_iata}/{asg.flight.dep_code_icao} -> {asg.flight.arr_code_iata}/{asg.flight.arr_code_icao}")
        
#         # Use all assignments if direction matching fails
#         assignments = all_assignments
    
#     if not assignments.exists():
#         validation_errors.append(f"No crew assignments found for direction {direction} on {date}")
#         send_validation_error_email(validation_errors, direction, date)
#         return None
    
#     # Group assignments by flight to create one message per flight
#     flights_processed = set()
#     total_crew_count = 0
    
#     for asg in assignments:
#         # Create one message per unique flight (avoid duplicates)
#         flight_key = f"{asg.flight.flight_no}_{asg.flight.sd_date_utc}_{asg.std_utc}"
#         if flight_key in flights_processed:
#             continue
#         flights_processed.add(flight_key)
        
#         msg_count += 1
#         msg_ref = f"{ctrl_ref}{msg_count:02d}"
#         segments = []
        
#         # Message header with flight info - Use 'F' for complete crew list
#         segments.append(
#             f"UNH+{msg_ref}+PAXLST:D:05B:UN:IATA+WB{asg.flight.flight_no}"
#             f"{date.strftime('%y%m%d')}{asg.std_utc.strftime('%H%M')}+01:F'"
#         )
        
#         # Beginning of message - crew list
#         segments.append("BGM+250'")
        
#         # Sender information
#         segments.append(f"NAD+MS+++{sender}:CREW APIS TEAM'")
        
        
#         # segments.append("COM+AIMS@RWANDAIR.COM:EM'")  # Email contact
#         # segments.append("COM+250781442755:TE'")  # Phone contact (adjust number as needed)
#         segments.append("COM+AIMS@RWANDAIR.COM:EM+250781442755:TE'")
        
#         # Transport information
#         segments.append(f"TDT+20+WB{asg.flight.flight_no}'")
        
#         # Departure location and time (convert to IATA codes for EDIFACT)
#         if 'OTHH' in str(asg.flight.dep_code_icao) or 'OTHH' in str(asg.flight.dep_code_iata):
#             dep_iata = 'DOH'
#         elif 'HRYR' in str(asg.flight.dep_code_icao) or 'HRYR' in str(asg.flight.dep_code_iata):
#             dep_iata = 'KGL'
#         else:
#             dep_iata = asg.flight.dep_code_iata or 'DOH'
#         segments.append(f"LOC+125+{dep_iata}'")
#         segments.append(f"DTM+189:{date.strftime('%y%m%d')}{asg.std_utc.strftime('%H%M')}:201'")
        
#         # Arrival location and time (convert to IATA codes for EDIFACT)
#         if 'OTHH' in str(asg.flight.arr_code_icao) or 'OTHH' in str(asg.flight.arr_code_iata):
#             arr_iata = 'DOH'
#         elif 'HRYR' in str(asg.flight.arr_code_icao) or 'HRYR' in str(asg.flight.arr_code_iata):
#             arr_iata = 'KGL'
#         else:
#             arr_iata = asg.flight.arr_code_iata or 'KGL'
#         segments.append(f"LOC+87+{arr_iata}'")
#         segments.append(f"DTM+232:{date.strftime('%y%m%d')}{asg.sta_utc.strftime('%H%M')}:201'")
        
#         # Get all crew members for this specific flight
#         flight_crew = QatarFlightDetails.objects.filter(
#             flight=asg.flight,
#             dep_date_utc=date
#         ).select_related('flight')
        
#         crew_count = 0
#         flight_validation_errors = []
        
#         # Process each crew member with enhanced validation
#         for crew_asg in flight_crew:
#             crew_detail = QatarCrewDetail.objects.filter(crew_id=crew_asg.crew_id).first()
            
#             if not crew_detail:
#                 flight_validation_errors.append(f"No crew detail found for crew_id: {crew_asg.crew_id}")
#                 continue
            
#             # Mandatory field validation
#             surname = (crew_detail.surname or '').upper().strip()
#             firstname = (crew_detail.firstname or '').upper().strip() 
#             middlename = (crew_detail.middlename or '').upper().strip()
#             passport_number = (crew_detail.passport_number or '').upper()
            
#             # Skip crew members with insufficient mandatory data
#             if not surname:
#                 flight_validation_errors.append(f"Missing mandatory surname for crew {crew_asg.crew_id}")
#                 continue
                
#             # Check if we have at least one given name (firstname OR middlename)
#             if not firstname and not middlename:
#                 flight_validation_errors.append(f"Missing mandatory given name for crew {crew_asg.crew_id} (both firstname and middlename are empty)")
#                 continue
                
#             if not passport_number:
#                 flight_validation_errors.append(f"Missing mandatory passport number for crew {crew_asg.crew_id}")
#                 continue
            
#             # Combine first and middle names, or use whichever is available
#             if firstname and middlename:
#                 full_given_name = f"{firstname} {middlename}".strip()
#             elif firstname:
#                 full_given_name = firstname
#             else:
#                 full_given_name = middlename  # Use middlename if firstname is empty
            
#             # Format NAD segment without extra separators - only add what's needed
#             if full_given_name:
#                 segments.append(f"NAD+FM+++{surname}:{full_given_name}'")
#             else:
#                 segments.append(f"NAD+FM+++{surname}'")
            
#             crew_count += 1
            
#             # Gender/Sex (mandatory)
#             sex = (crew_detail.sex or 'M').upper()  # Default to M if not specified, ensure uppercase
#             segments.append(f"ATT+2++{sex}'")
            
#             # Birth date (mandatory)
#             if crew_detail.birth_date:
#                 birth_date_str = crew_detail.birth_date.strftime('%y%m%d')
#                 segments.append(f"DTM+329:{birth_date_str}'")
#             else:
#                 flight_validation_errors.append(f"Missing mandatory birth date for crew {crew_asg.crew_id}")
#                 segments.append("DTM+329:'")  # Empty if no birth date
            
#             # Location information (departure and arrival ports for crew positioning)
#             segments.append(f"LOC+22+{arr_iata}'")  # Destination
#             segments.append(f"LOC+178+{dep_iata}'")  # Origin
#             segments.append(f"LOC+179+{arr_iata}'")  # Final destination
            
#             # Nationality - use nationality_code from Job 97 (3-letter code, uppercase)
#             nationality_code = (crew_detail.nationality_code or 'RWA').upper()  # Default to RWA if not specified
#             segments.append(f"NAT+2+{nationality_code}'")
            
#             # Document (Passport) information - Keep as per your original implementation (no issuing state)
#             segments.append(f"DOC+P:110:111+{passport_number}'")
            
#             # Passport expiry date (mandatory)
#             if crew_detail.passport_expiry:
#                 expiry_str = crew_detail.passport_expiry.strftime('%y%m%d')
#                 segments.append(f"DTM+36:{expiry_str}'")
#             else:
#                 flight_validation_errors.append(f"Missing mandatory passport expiry for crew {crew_asg.crew_id}")
#                 segments.append("DTM+36:'")
            
#             # Birth place (country code) - use nationality_code as fallback, uppercase
#             birth_place = (crew_detail.birth_place_cc or crew_detail.nationality_code or 'RWA').upper()
#             segments.append(f"LOC+91+{birth_place}'")
        
#         # Validate minimum crew count (5 for certification)
#         if crew_count < 5:
#             flight_validation_errors.append(f"Flight WB{asg.flight.flight_no} has only {crew_count} crew members, minimum 5 required for Qatar certification")
        
#         validation_errors.extend(flight_validation_errors)
#         total_crew_count += crew_count
        
#         # Crew count
#         segments.append(f"CNT+41:{crew_count:04d}'")
        
#         # Add all segments to main lines
#         lines.extend(segments)
        
#         # Message trailer
#         lines.append(f"UNT+{len(segments)+1:04d}+{msg_ref}'")
    
#     # Group trailer
#     lines.append(f"UNE+{msg_count}+{ctrl_ref}'")
    
#     # Interchange trailer
#     lines.append(f"UNZ+{msg_count}+{ctrl_ref}'")
    
#     # Final validation check
#     if total_crew_count < 5:
#         validation_errors.append(f"Total crew count ({total_crew_count}) is less than minimum requirement of 5")
    
#     # If validation errors exist, send email and don't generate file
#     if validation_errors:
#         send_validation_error_email(validation_errors, direction, date)
#         return None
    
#     # Create output directory and file using settings.QATAR_APIS_OUTPUT_PATH
#     try:
#         out_dir = settings.QATAR_APIS_OUTPUT_PATH
#         out_dir.mkdir(parents=True, exist_ok=True)
#         filename = f"QATAPIS_{direction}_{date:%Y%m%d}.edi"
#         out_path = out_dir / filename
        
#         with open(out_path, 'w', encoding='utf-8') as f:
#             f.write("\n".join(lines))
        
#         logger.info(f"Generated EDIFACT file: {out_path}")
#         logger.info(f"File contains {msg_count} flight(s) with {total_crew_count} crew members")
        
#         return out_path
        
#     except Exception as e:
#         logger.error(f"Error writing EDIFACT file: {e}")
#         validation_errors.append(f"File generation error: {str(e)}")
#         send_validation_error_email(validation_errors, direction, date)
#         return None


def build_qatar_apis_edifact(direction, date):
    """
    Constructs and writes the EDIFACT PAXLST file for the given direction and date.
    Updated to include all required crew fields and proper EDIFACT formatting.
    
    UPDATES:
    - Sender/receiver addresses from settings (.env)
    - Success email notification when file is generated
    
    Fixes applied based on Qatar Airways feedback:
    - Added COM segment for reporting party contact info (CRITICAL FIX)
    - Use 'F' for complete crew lists in UNH segment
    - Remove extra separators in NAD segment when no address info
    - Ensure all uppercase characters
    - Enhanced validation and error handling
    """
    from django.core.mail import send_mail
    from django.conf import settings
    import logging
    
    logger = logging.getLogger(__name__)
    
    lines = ["UNA:+.?*'"]
    ts = datetime.utcnow().strftime("%y%m%d:%H%M")
    ctrl_ref = datetime.utcnow().strftime("%y%m%d%H%M")
    
    # Get sender and receiver from settings
    sender = settings.QATAR_APIS_SENDER  # KGLCAWB or RWANDAIR
    receiver = settings.QATAR_APIS_RECEIVER  # DOHQAXS or QATAPIS
    
    logger.info(f"📤 Building EDIFACT - Sender: {sender}, Receiver: {receiver}, Direction: {direction}, Date: {date}")
    
    lines.append(f"UNB+UNOA:4+{sender}:ZZ+{receiver}:ZZ+{ts}+{ctrl_ref}'")
    lines.append(f"UNG+PAXLST+{sender}:ZZ+{receiver}:ZZ+{ts}+{ctrl_ref}+UN+D:05B'")
    
    msg_count = 0
    validation_errors = []
    
    # Get crew assignments for the specified direction and date
    if direction == 'O':  # Outbound (KGL to DOH)
        dep_codes = ['KGL', 'HRYR']  # IATA and ICAO for Kigali
        arr_codes = ['DOH', 'OTHH']  # IATA and ICAO for Doha
    else:  # Inbound (DOH to KGL)
        dep_codes = ['DOH', 'OTHH']  # IATA and ICAO for Doha
        arr_codes = ['KGL', 'HRYR']  # IATA and ICAO for Kigali
    
    assignments = QatarFlightDetails.objects.filter(
        dep_date_utc=date,
        flight__dep_code_iata__in=dep_codes,
        flight__arr_code_iata__in=arr_codes
    ).select_related('flight')
    
    # If no assignments found with IATA codes, try with ICAO codes
    if not assignments.exists():
        assignments = QatarFlightDetails.objects.filter(
            dep_date_utc=date,
            flight__dep_code_icao__in=dep_codes,
            flight__arr_code_icao__in=arr_codes
        ).select_related('flight')
    
    # Final fallback - just use the date and let's see what flights we have
    if not assignments.exists():
        logger.warning(f"No crew assignments found with airport codes. Checking all flights on {date}")
        all_assignments = QatarFlightDetails.objects.filter(
            dep_date_utc=date
        ).select_related('flight')
        
        for asg in all_assignments:
            logger.info(f"Available flight: {asg.flight.flight_no} - {asg.flight.dep_code_iata}/{asg.flight.dep_code_icao} -> {asg.flight.arr_code_iata}/{asg.flight.arr_code_icao}")
        
        # Use all assignments if direction matching fails
        assignments = all_assignments
    
    if not assignments.exists():
        validation_errors.append(f"No crew assignments found for direction {direction} on {date}")
        send_validation_error_email(validation_errors, direction, date)
        return None
    
    # Group assignments by flight to create one message per flight
    flights_processed = set()
    total_crew_count = 0
    
    for asg in assignments:
        # Create one message per unique flight (avoid duplicates)
        flight_key = f"{asg.flight.flight_no}_{asg.flight.sd_date_utc}_{asg.std_utc}"
        if flight_key in flights_processed:
            continue
        flights_processed.add(flight_key)
        
        msg_count += 1
        msg_ref = f"{ctrl_ref}{msg_count:02d}"
        segments = []
        
        # Message header with flight info - Use 'F' for complete crew list
        segments.append(
            f"UNH+{msg_ref}+PAXLST:D:05B:UN:IATA+WB{asg.flight.flight_no}"
            f"{date.strftime('%y%m%d')}{asg.std_utc.strftime('%H%M')}+01:F'"
        )
        
        # Beginning of message - crew list
        segments.append("BGM+250'")
        
        # Sender information
        segments.append(f"NAD+MS+++{sender}:CREW APIS TEAM'")
        
        # Contact information - CRITICAL FIX
        segments.append("COM+AIMS@RWANDAIR.COM:EM+250781442755:TE'")
        
        # Transport information
        segments.append(f"TDT+20+WB{asg.flight.flight_no}'")
        
        # Departure location and time (convert to IATA codes for EDIFACT)
        if 'OTHH' in str(asg.flight.dep_code_icao) or 'OTHH' in str(asg.flight.dep_code_iata):
            dep_iata = 'DOH'
        elif 'HRYR' in str(asg.flight.dep_code_icao) or 'HRYR' in str(asg.flight.dep_code_iata):
            dep_iata = 'KGL'
        else:
            dep_iata = asg.flight.dep_code_iata or 'DOH'
        segments.append(f"LOC+125+{dep_iata}'")
        segments.append(f"DTM+189:{date.strftime('%y%m%d')}{asg.std_utc.strftime('%H%M')}:201'")
        
        # Arrival location and time (convert to IATA codes for EDIFACT)
        if 'OTHH' in str(asg.flight.arr_code_icao) or 'OTHH' in str(asg.flight.arr_code_iata):
            arr_iata = 'DOH'
        elif 'HRYR' in str(asg.flight.arr_code_icao) or 'HRYR' in str(asg.flight.arr_code_iata):
            arr_iata = 'KGL'
        else:
            arr_iata = asg.flight.arr_code_iata or 'KGL'
        segments.append(f"LOC+87+{arr_iata}'")
        segments.append(f"DTM+232:{date.strftime('%y%m%d')}{asg.sta_utc.strftime('%H%M')}:201'")
        
        # Get all crew members for this specific flight
        flight_crew = QatarFlightDetails.objects.filter(
            flight=asg.flight,
            dep_date_utc=date
        ).select_related('flight')
        
        crew_count = 0
        flight_validation_errors = []
        
        # Process each crew member with enhanced validation
        for crew_asg in flight_crew:
            crew_detail = QatarCrewDetail.objects.filter(crew_id=crew_asg.crew_id).first()
            
            if not crew_detail:
                flight_validation_errors.append(f"No crew detail found for crew_id: {crew_asg.crew_id}")
                continue
            
            # Mandatory field validation
            surname = (crew_detail.surname or '').upper().strip()
            firstname = (crew_detail.firstname or '').upper().strip() 
            middlename = (crew_detail.middlename or '').upper().strip()
            passport_number = (crew_detail.passport_number or '').upper()
            
            # Skip crew members with insufficient mandatory data
            if not surname:
                flight_validation_errors.append(f"Missing mandatory surname for crew {crew_asg.crew_id}")
                continue
                
            # Check if we have at least one given name (firstname OR middlename)
            if not firstname and not middlename:
                flight_validation_errors.append(f"Missing mandatory given name for crew {crew_asg.crew_id} (both firstname and middlename are empty)")
                continue
                
            if not passport_number:
                flight_validation_errors.append(f"Missing mandatory passport number for crew {crew_asg.crew_id}")
                continue
            
            # Combine first and middle names, or use whichever is available
            if firstname and middlename:
                full_given_name = f"{firstname} {middlename}".strip()
            elif firstname:
                full_given_name = firstname
            else:
                full_given_name = middlename  # Use middlename if firstname is empty
            
            # Format NAD segment without extra separators - only add what's needed
            if full_given_name:
                segments.append(f"NAD+FM+++{surname}:{full_given_name}'")
            else:
                segments.append(f"NAD+FM+++{surname}'")
            
            crew_count += 1
            
            # Gender/Sex (mandatory)
            sex = (crew_detail.sex or 'M').upper()  # Default to M if not specified, ensure uppercase
            segments.append(f"ATT+2++{sex}'")
            
            # Birth date (mandatory)
            if crew_detail.birth_date:
                birth_date_str = crew_detail.birth_date.strftime('%y%m%d')
                segments.append(f"DTM+329:{birth_date_str}'")
            else:
                flight_validation_errors.append(f"Missing mandatory birth date for crew {crew_asg.crew_id}")
                segments.append("DTM+329:'")  # Empty if no birth date
            
            # Location information (departure and arrival ports for crew positioning)
            segments.append(f"LOC+22+{arr_iata}'")  # Destination
            segments.append(f"LOC+178+{dep_iata}'")  # Origin
            segments.append(f"LOC+179+{arr_iata}'")  # Final destination
            
            # Nationality - use nationality_code from Job 97 (3-letter code, uppercase)
            nationality_code = (crew_detail.nationality_code or 'RWA').upper()  # Default to RWA if not specified
            segments.append(f"NAT+2+{nationality_code}'")
            
            # Document (Passport) information
            segments.append(f"DOC+P:110:111+{passport_number}'")
            
            # Passport expiry date (mandatory)
            if crew_detail.passport_expiry:
                expiry_str = crew_detail.passport_expiry.strftime('%y%m%d')
                segments.append(f"DTM+36:{expiry_str}'")
            else:
                flight_validation_errors.append(f"Missing mandatory passport expiry for crew {crew_asg.crew_id}")
                segments.append("DTM+36:'")
            
            # Birth place (country code) - use nationality_code as fallback, uppercase
            birth_place = (crew_detail.birth_place_cc or crew_detail.nationality_code or 'RWA').upper()
            segments.append(f"LOC+91+{birth_place}'")
        
        # Validate minimum crew count (5 for certification)
        if crew_count < 5:
            flight_validation_errors.append(f"Flight WB{asg.flight.flight_no} has only {crew_count} crew members, minimum 5 required for Qatar certification")
        
        validation_errors.extend(flight_validation_errors)
        total_crew_count += crew_count
        
        # Crew count
        segments.append(f"CNT+41:{crew_count:04d}'")
        
        # Add all segments to main lines
        lines.extend(segments)
        
        # Message trailer
        lines.append(f"UNT+{len(segments)+1:04d}+{msg_ref}'")
    
    # Group trailer
    lines.append(f"UNE+{msg_count}+{ctrl_ref}'")
    
    # Interchange trailer
    lines.append(f"UNZ+{msg_count}+{ctrl_ref}'")
    
    # Final validation check
    if total_crew_count < 5:
        validation_errors.append(f"Total crew count ({total_crew_count}) is less than minimum requirement of 5")
    
    # If validation errors exist, send email and don't generate file
    if validation_errors:
        send_validation_error_email(validation_errors, direction, date)
        return None
    
    # Create output directory and file using settings.QATAR_APIS_OUTPUT_PATH
    try:
        out_dir = settings.QATAR_APIS_OUTPUT_PATH
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = f"QATAPIS_{direction}_{date:%Y%m%d}.edi"
        out_path = out_dir / filename

        if out_path.exists():
            logger.info(f"⚠️ File already exists, skipping: {out_path}")
            return out_path
        
        with open(out_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
        
        logger.info(f"✅ Generated EDIFACT file: {out_path}")
        logger.info(f"📊 File contains {msg_count} flight(s) with {total_crew_count} crew members")
        logger.info(f"📤 Sender: {sender}, Receiver: {receiver}")
        
        # Send success email notification
        send_success_email(out_path, direction, date, msg_count, total_crew_count)
        
        return out_path
        
    except Exception as e:
        logger.error(f"Error writing EDIFACT file: {e}")
        validation_errors.append(f"File generation error: {str(e)}")
        send_validation_error_email(validation_errors, direction, date)
        return None
    


def send_success_email(file_path, direction, date, flight_count, crew_count):
    """
    Send email notification about successful EDIFACT file generation.
    """
    from django.core.mail import send_mail
    from django.conf import settings
    import logging
    
    logger = logging.getLogger(__name__)
    
    direction_text = 'Outbound KGL→DOH' if direction == 'O' else 'Inbound DOH→KGL'
    filename = file_path.name if hasattr(file_path, 'name') else str(file_path).split('/')[-1]
    
    try:
        send_mail(
            subject=f"✅ Qatar APIS File Generated Successfully - {direction_text} {date}",
            message=(
                f"Dear Team,\n\n"
                f"The Qatar APIS EDIFACT file has been generated successfully!\n\n"
                f"File Details:\n"
                f"• Filename: {filename}\n"
                f"• Direction: {direction_text}\n"
                f"• Flight Date: {date}\n"
                f"• Number of Flights: {flight_count}\n"
                f"• Total Crew Members: {crew_count}\n"
                f"• File Path: {file_path}\n"
                f"• Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"The file is ready for transmission to Qatar Airways APIS system.\n\n"
                f"Regards,\n"
                f"WBHUB System"
            ),
            from_email=settings.EMAIL_HOST_USER,
            recipient_list=[
                'elie.kayitare@rwandair.com',
                'saif.zawahreh@rwandair.com',
                'training&records@rwandair.com',
            ],
            fail_silently=False,
        )
        logger.info("✅ Success notification email sent")
    except Exception as e:
        logger.error(f"Failed to send success email: {e}")


def send_validation_error_email(validation_errors, direction, date):
    """
    Send email notification about validation errors that prevent file generation.
    """
    from django.core.mail import send_mail
    from django.conf import settings
    import logging
    
    logger = logging.getLogger(__name__)
    
    error_summary = "\n".join([f"• {error}" for error in validation_errors])
    direction_text = 'Outbound KGL→DOH' if direction == 'O' else 'Inbound DOH→KGL'
    
    try:
        send_mail(
            subject=f"Qatar APIS File Generation Failed - {direction_text} {date}",
            message=(
                f"Dear Team,\n\n"
                f"The Qatar APIS EDIFACT file generation failed due to the following validation issues:\n\n"
                f"{error_summary}\n\n"
                f"Direction: {direction_text}\n"
                f"Date: {date}\n"
                f"Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
                f"Required for Qatar Airways certification:\n"
                f"• All mandatory fields: surname, given name, birth date, passport number, expiry date\n\n"
                f"Please review the crew data and ensure all mandatory fields are populated.\n"
                f"The file will be generated automatically once all validation requirements are met.\n\n"
                f"Regards,\nWBHUB System"
            ),
            from_email=settings.EMAIL_HOST_USER,
            recipient_list=[
                'elie.kayitare@rwandair.com',
                'saif.zawahreh@rwandair.com',
                'training&records@rwandair.com',
                # settings.FIRST_EMAIL_RECEIVER,
                # settings.SECOND_EMAIL_RECEIVER,
                # settings.THIRD_EMAIL_RECEIVER,
            ],
            fail_silently=False,
        )
        logger.info("Validation error email sent successfully")
    except Exception as e:
        logger.error(f"Failed to send validation error email: {e}")