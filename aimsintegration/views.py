
from django.shortcuts import render
from django.http import JsonResponse
from .models import FlightData,FdmFlightData
from datetime import date, datetime

def dashboard_view(request):
    query = request.GET.get('query', '')
    selected_date = request.GET.get('date', '')

    # Set filter date based on selected date or default to today
    filter_date = date.today() if not selected_date else datetime.strptime(selected_date, "%Y-%m-%d").date()

    # Check if it's an AJAX request
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        # Filter based on query and selected date, then order by std_utc
        schedules = (
            FlightData.objects.filter(sd_date_utc=filter_date).filter(
                flight_no__icontains=query
            ) | FlightData.objects.filter(
                sd_date_utc=filter_date
            ).filter(
                dep_code_iata__icontains=query
            ) | FlightData.objects.filter(
                sd_date_utc=filter_date
            ).filter(
                arr_code_iata__icontains=query
            ) | FlightData.objects.filter(
                sd_date_utc=filter_date
            ).filter(
                dep_code_icao__icontains=query
            ) | FlightData.objects.filter(
                sd_date_utc=filter_date
            ).filter(
                arr_code_icao__icontains=query
            ) | FlightData.objects.filter(
                sd_date_utc=filter_date
            ).filter(
                tail_no__icontains=query)

        ).order_by('std_utc')

        # Serialize data for JSON response
        data = list(schedules.values('sd_date_utc', 'flight_no', 'tail_no','dep_code_iata', 'dep_code_icao',
                                      'arr_code_iata', 'arr_code_icao', 'std_utc', 'atd_utc',
                                      'takeoff_utc', 'touchdown_utc', 'ata_utc', 'sta_utc'))
        return JsonResponse(data, safe=False)
    
    # Non-AJAX request loads all today's flights, ordered by std_utc
    schedules = FlightData.objects.filter(sd_date_utc=filter_date).order_by('std_utc')
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})


#Cargo Project API

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.utils.dateparse import parse_date
from .models import FlightData, CargoFlightData
from .serializers import FlightDataSerializer
from datetime import datetime

class FlightDataListView(APIView):
    def get(self, request, *args, **kwargs):
        # Extract query parameters
        origin_icao = request.query_params.get('origin_icao')
        destination_icao = request.query_params.get('destination_icao')
        scheduled_departure_date = request.query_params.get('scheduled_departure_date', None)

        # If no scheduled_departure_date is provided, use today's date
        if scheduled_departure_date:
            try:
                scheduled_departure_date = parse_date(scheduled_departure_date)
                if not scheduled_departure_date:
                    raise ValueError("Invalid date format")
            except ValueError:
                return Response({"error": "Invalid date format. Please use YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)
        else:
            scheduled_departure_date = datetime.today().date()

        # Query the database based on provided filters
        flight_data = CargoFlightData.objects.filter(
            dep_code_icao=origin_icao,
            arr_code_icao=destination_icao,
            sd_date_utc__gte=scheduled_departure_date
        )

        # Serialize the data using the updated serializer
        serializer = FlightDataSerializer(flight_data, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)



#CPAT Project API
from django.http import JsonResponse
from django.shortcuts import render
from django.utils.timezone import now
from .models import CompletionRecord
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar

# Validity periods for courses
VALIDITY_PERIODS = {
    "FRMS": 12,     # Fatigue Education & Awareness Training
    "ETPG": 36,     # ETOPS Ground
    "LVO-G": 36,    # LVO Ground
    "PBNGRN": 12,   # PBN Ground
    "RVSMGS": 0,    # RVSM Ground (never expires)
}

def calculate_expiry_date(completion_date_str, course_code):
    """
    Calculate expiry date based on completion date in DDMMYYYY format
    and adjust to the last day of the expiry month.
    """
    if not completion_date_str:
        return "--"  # No completion date available

    try:
        # Parse the input date in YYYY-MM-DD format
        completion_date = datetime.strptime(completion_date_str, "%Y-%m-%d")
        validity_period = VALIDITY_PERIODS.get(course_code, 0)

        if validity_period == 0:
            return "--"  # No expiry date (never expires)

        # Calculate the tentative expiry date
        expiry_date = completion_date + relativedelta(months=validity_period)
        # Adjust to the last day of the expiry month
        last_day = calendar.monthrange(expiry_date.year, expiry_date.month)[1]
        expiry_date = expiry_date.replace(day=last_day)
        return expiry_date.strftime("%Y-%m-%d")  # Return as YYYY-MM-DD
    except ValueError:
        return "--"

def todays_completion_records_view(request):
    today = now().date()
    query = request.GET.get('query', '').strip()
    selected_date = request.GET.get('date', '').strip()

    # Base query to filter records
    records_query = CompletionRecord.objects.all()

    # Apply date filter if selected_date is provided
    if selected_date:
        try:
            date_object = datetime.strptime(selected_date, "%Y-%m-%d").date()
            records_query = records_query.filter(completion_date=date_object)
        except ValueError:
            records_query = records_query.none()

    # Apply search query
    if query:
        records_query = records_query.filter(
            employee_id__icontains=query
        ) | records_query.filter(
            employee_email__icontains=query
        ) | records_query.filter(
            course_code__icontains=query
        )

    # Order records
    records = records_query.order_by('completion_date')

    # If it's an AJAX request, return JSON response with expiry date and validity period
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        data = []
        for record in records:
            completion_date_str = record.completion_date.strftime("%Y-%m-%d") if record.completion_date else ""
            expiry_date = calculate_expiry_date(completion_date_str, record.course_code)
            validity_period = VALIDITY_PERIODS.get(record.course_code, "--")

            data.append({
                'id': record.id,
                'employee_id': record.employee_id,
                'employee_email': record.employee_email,
                'course_code': record.course_code,
                'completion_date': completion_date_str,
                'expiry_date': expiry_date,
                'validity_period': validity_period,
                'score': record.score or "--",
                'time_in_seconds': record.time_in_seconds or "--",
                'start_date': record.start_date.strftime("%Y-%m-%d") if record.start_date else "--",
                'end_date': record.end_date.strftime("%Y-%m-%d") if record.end_date else "--",
            })
        return JsonResponse(data, safe=False)

    # Otherwise, render the template with expiry date and validity period
    enriched_records = []
    for record in records:
        completion_date_str = record.completion_date.strftime("%Y-%m-%d") if record.completion_date else ""
        record.expiry_date = calculate_expiry_date(completion_date_str, record.course_code)
        record.validity_period = VALIDITY_PERIODS.get(record.course_code, "--")
        enriched_records.append(record)

    return render(request, 'aimsintegration/cpat_completion_records.html', {
        'records': enriched_records,
        'today': today,
        'query': query,
    })




#FDM

from django.http import JsonResponse
from django.shortcuts import render
from django.utils.timezone import make_aware
from datetime import datetime, date, timezone
from .models import FdmFlightData
import logging

logger = logging.getLogger(__name__)

def fdm_dashboard_view(request):
    query = request.GET.get('query', '').strip()  # Get the query string, remove leading/trailing spaces
    selected_date = request.GET.get('date', '').strip()  # Get the selected date, remove spaces

    # Set the filter date
    if selected_date:
        try:
            filter_date = datetime.strptime(selected_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format received: {selected_date}")
            return JsonResponse({"error": "Invalid date format. Use 'YYYY-MM-DD'."}, status=400)
    else:
        filter_date = date.today()

    # Convert filter_date to a timezone-aware datetime object (UTC)
    filter_date = make_aware(datetime.combine(filter_date, datetime.min.time()), timezone=timezone.utc)

    logger.info(f"Selected Date: {selected_date}, Filter Date (UTC): {filter_date}")

    # Check if it's an AJAX request
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        # Build the filter for the query
        filters = (
            FdmFlightData.objects.filter(sd_date_utc=filter_date).filter(flight_no__icontains=query) |
            FdmFlightData.objects.filter(sd_date_utc=filter_date).filter(dep_code_iata__icontains=query) |
            FdmFlightData.objects.filter(sd_date_utc=filter_date).filter(arr_code_iata__icontains=query) |
            FdmFlightData.objects.filter(sd_date_utc=filter_date).filter(dep_code_icao__icontains=query) |
            FdmFlightData.objects.filter(sd_date_utc=filter_date).filter(arr_code_icao__icontains=query) |
            FdmFlightData.objects.filter(sd_date_utc=filter_date).filter(tail_no__icontains=query)
        ).distinct().order_by('std_utc')

        # Serialize data for JSON response
        data = list(filters.values(
            'sd_date_utc', 'flight_no', 'tail_no', 'dep_code_icao', 'arr_code_icao',
            'std_utc', 'atd_utc', 'takeoff_utc', 'touchdown_utc', 'ata_utc',
            'sta_utc', 'flight_type', 'etd_utc', 'eta_utc'
        ))

        logger.info(f"Filtered schedules (AJAX): {len(data)} records found.")
        return JsonResponse(data, safe=False)

    # Handle normal (non-AJAX) requests
    fdm_schedules = FdmFlightData.objects.filter(sd_date_utc=filter_date).order_by('std_utc')
    logger.info(f"Filtered schedules (non-AJAX): {len(fdm_schedules)} records found.")

    return render(request, 'aimsintegration/fdm.html', {'fdm_schedules': fdm_schedules})




from django.http import JsonResponse
from .models import CrewMember
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def get_crew_details(request):
    # Get parameters from the frontend
    flight_no = request.GET.get('flight_no')
    origin = request.GET.get('origin')
    destination = request.GET.get('destination')
    date = request.GET.get('date')  # Expected as 'YYYY-MM-DD'

    logger.info(f"Fetching crew details for flight_no={flight_no}, origin={origin}, destination={destination}, sd_date_utc={date}")

    # Validate and parse the date
    try:
        date_obj = datetime.strptime(date, '%Y-%m-%d').date()
    except ValueError:
        logger.error(f"Invalid date format received: {date}")
        return JsonResponse({"error": "Invalid date format. Use 'YYYY-MM-DD'."}, status=400)

    # Fetch all matching crew members
    crew_members = CrewMember.objects.filter(
        flight_no=flight_no,
        origin=origin,
        destination=destination,
        sd_date_utc=date_obj
    ).values('name', 'role', 'flight_no', 'origin', 'destination', 'crew_id')

    # Log the query results
    logger.info(f"Crew members found: {list(crew_members)}")

    # Return the response
    return JsonResponse(list(crew_members), safe=False)





# Crew Allowance project
# from django.shortcuts import render, get_object_or_404, redirect
# from django.http import JsonResponse
# from .models import Zone, Destination

# def add_zone(request):
#     """
#     Create a new zone and add its destinations.
#     """
#     if request.method == 'POST':
#         name = request.POST.get('name')
#         daily_allowance = request.POST.get('daily_allowance')
#         hourly_allowance = request.POST.get('hourly_allowance', 0)
#         destinations_input = request.POST.get('destinations', '')  # Comma-separated destinations

#         # Validate input
#         if not name or not daily_allowance:
#             return JsonResponse({'status': 'error', 'message': 'Name and daily allowance are required.'})

#         # Create the zone
#         zone = Zone.objects.create(
#             name=name,
#             daily_allowance=float(daily_allowance),
#             hourly_allowance=float(hourly_allowance) if hourly_allowance else 0
#         )

#         # Add destinations
#         destinations = [code.strip() for code in destinations_input.split(',') if code.strip()]
#         Destination.objects.bulk_create([
#             Destination(zone=zone, airport_code=airport_code) for airport_code in destinations
#         ])

#         return JsonResponse({'status': 'success', 'message': 'Zone created successfully.', 'zone_id': zone.id})

#     return render(request, 'aimsintegration/add_zone.html')


# def list_zones(request):
#     """
#     Display all zones along with their destinations.
#     """
#     zones = Zone.objects.prefetch_related('destinations').all()
#     return render(request, 'aimsintegration/list_zones.html', {'zones': zones})


# # This view lists all zones with their associated destinations in a paginated format.
# from django.core.paginator import Paginator

# def list_zones_with_destinations(request):
#     """
#     Display all zones with their associated destinations, paginated.
#     """
#     zones = Zone.objects.prefetch_related('destinations').all()
#     paginator = Paginator(zones, 10)  # 10 zones per page
#     page_number = request.GET.get('page')
#     page_obj = paginator.get_page(page_number)

#     return render(request, 'aimsintegration/list_zones_with_destinations.html', {'page_obj': page_obj})



# def edit_zone(request, zone_id):
#     """
#     Edit a zone and its destinations.
#     """
#     zone = get_object_or_404(Zone, id=zone_id)

#     if request.method == 'POST':
#         # Update zone details
#         zone.name = request.POST.get('name', zone.name)
#         zone.daily_allowance = request.POST.get('daily_allowance', zone.daily_allowance)
#         zone.hourly_allowance = request.POST.get('hourly_allowance', zone.hourly_allowance)
#         zone.save()

#         # Update destinations
#         destinations_input = request.POST.get('destinations', '')
#         updated_destinations = [code.strip() for code in destinations_input.split(',') if code.strip()]

#         # Add new destinations and delete removed ones
#         existing_destinations = set(zone.destinations.values_list('airport_code', flat=True))
#         new_destinations = set(updated_destinations) - existing_destinations
#         removed_destinations = existing_destinations - set(updated_destinations)

#         # Add new destinations
#         Destination.objects.bulk_create([
#             Destination(zone=zone, airport_code=airport_code) for airport_code in new_destinations
#         ])

#         # Remove old destinations
#         zone.destinations.filter(airport_code__in=removed_destinations).delete()

#         return JsonResponse({'status': 'success', 'message': 'Zone updated successfully.', 'zone_id': zone.id})

#     # Prepare data for the form
#     destinations = ', '.join(zone.destinations.values_list('airport_code', flat=True))
#     return render(request, 'aimsintegration/edit_zone.html', {'zone': zone, 'destinations': destinations})


# #Allows updating an individual destination in a zone.

# def edit_destination(request, zone_id, destination_id):
#     """
#     Edit a specific destination in a zone.
#     """
#     zone = get_object_or_404(Zone, id=zone_id)
#     destination = get_object_or_404(Destination, id=destination_id, zone=zone)

#     if request.method == 'POST':
#         new_airport_code = request.POST.get('airport_code')
#         if not new_airport_code:
#             return JsonResponse({'status': 'error', 'message': 'Airport code is required.'})

#         # Check for uniqueness within the zone
#         if zone.destinations.filter(airport_code=new_airport_code).exclude(id=destination.id).exists():
#             return JsonResponse({'status': 'error', 'message': 'This destination already exists in the zone.'})

#         # Update the destination
#         destination.airport_code = new_airport_code
#         destination.save()

#         return JsonResponse({'status': 'success', 'message': f"Destination updated to '{new_airport_code}'."})

#     return render(request, 'aimsintegration/edit_destination.html', {'zone': zone, 'destination': destination})


# # This view provides detailed information about a single zone, including all its destinations.
# def zone_details(request, zone_id):
#     """
#     Display details of a specific zone along with its destinations.
#     """
#     zone = get_object_or_404(Zone.objects.prefetch_related('destinations'), id=zone_id)
#     return render(request, 'aimsintegration/zone_details.html', {'zone': zone})




# def delete_zone(request, zone_id):
#     """
#     Delete a zone and its associated destinations.
#     """
#     zone = get_object_or_404(Zone, id=zone_id)
#     zone.delete()
#     return JsonResponse({'status': 'success', 'message': f"Zone '{zone.name}' deleted successfully."})


# def add_destination(request, zone_id):
#     """
#     Add a destination to a specific zone.
#     """
#     zone = get_object_or_404(Zone, id=zone_id)

#     if request.method == 'POST':
#         airport_code = request.POST.get('airport_code')
#         if not airport_code:
#             return JsonResponse({'status': 'error', 'message': 'Airport code is required.'})

#         # Check if destination already exists
#         if zone.destinations.filter(airport_code=airport_code).exists():
#             return JsonResponse({'status': 'error', 'message': 'Destination already exists in this zone.'})

#         # Add the new destination
#         Destination.objects.create(zone=zone, airport_code=airport_code)
#         return JsonResponse({'status': 'success', 'message': f"Destination '{airport_code}' added successfully."})

#     return JsonResponse({'status': 'error', 'message': 'Invalid request method.'})


# # Allows adding multiple destinations to a zone in one go
# def batch_add_destinations(request, zone_id):
#     """
#     Add multiple destinations to a zone in batch.
#     """
#     zone = get_object_or_404(Zone, id=zone_id)

#     if request.method == 'POST':
#         destinations_input = request.POST.get('destinations', '')  # Comma-separated airport codes
#         destinations = [code.strip() for code in destinations_input.split(',') if code.strip()]

#         # Filter out duplicates and already existing destinations
#         existing_destinations = set(zone.destinations.values_list('airport_code', flat=True))
#         new_destinations = [code for code in destinations if code not in existing_destinations]

#         # Add new destinations in bulk
#         Destination.objects.bulk_create([Destination(zone=zone, airport_code=code) for code in new_destinations])

#         return JsonResponse({'status': 'success', 'message': f"Added {len(new_destinations)} new destinations."})

#     return render(request, 'aimsintegration/batch_add_destinations.html', {'zone': zone})



# def delete_destination(request, zone_id, destination_id):
#     """
#     Delete a specific destination from a zone.
#     """
#     zone = get_object_or_404(Zone, id=zone_id)
#     destination = get_object_or_404(Destination, id=destination_id, zone=zone)

#     # Delete the destination
#     destination.delete()
#     return JsonResponse({'status': 'success', 'message': f"Destination '{destination.airport_code}' deleted successfully."})




# import csv
# from datetime import datetime
# from django.http import JsonResponse
# from django.shortcuts import render
# from .models import Crew, FlightCrewRecord

# def upload_file(request):
#     """
#     Upload a file and process it to populate Crew and FlightRecord models.
#     """
#     if request.method == 'POST' and request.FILES.get('file'):
#         uploaded_file = request.FILES['file']
#         file_data = uploaded_file.read().decode('utf-8').splitlines()
#         csv_reader = csv.reader(file_data)

#         for row in csv_reader:
#             # Map columns from the file to the required fields
#             duty_date = row[0]  # Column 5: Duty Date
#             crew_id = row[1]  # Column 2: Crew ID
#             first_name = row[2]  # Column 59: Crew First Name
#             last_name = row[3]  # Column 58: Crew Surname
#             position = row[4]  # Column 3: Crew Pos
#             flight_number = row[5]  # Column 18: Flight Number
#             tail_number = row[6]  # Column 16: Tail Number
#             departure_airport = row[7]  # Column 21: Departure Airport
#             arrival_airport = row[8]  # Column 30: Arrival Airport
#             layover_time = row[9]  # Column 49: Layover Time for This Day

#             # Add crew if not already in the database
#             crew, created = Crew.objects.get_or_create(
#                 crew_id=crew_id,
#                 defaults={'first_name': first_name, 'last_name': last_name, 'position': position}
#             )

#             # Add flight record
#             FlightCrewRecord.objects.create(
#                 duty_date=datetime.strptime(duty_date, '%d/%m/%y'),
#                 flight_number=flight_number,
#                 tail_number=tail_number,
#                 departure_airport=departure_airport,
#                 arrival_airport=arrival_airport,
#                 layover_time=layover_time
#             )

#         return JsonResponse({'status': 'success', 'message': 'File processed and records saved successfully.'})

#     return render(request, 'aimsintegration/upload_file.html')



# from django.shortcuts import render
# from django.core.paginator import Paginator
# from .models import FlightRecord

# def list_crew_flights(request):
#     """
#     Display all crew flights with assigned crew members and pagination.
#     """
#     # Get all flight records
#     flight_records = FlightRecord.objects.prefetch_related('crew').order_by('-duty_date')

#     # Pagination: 10 flights per page
#     paginator = Paginator(flight_records, 10)
#     page_number = request.GET.get('page')
#     page_obj = paginator.get_page(page_number)

#     return render(request, 'aimsintegration/list_crew_flights.html', {'page_obj': page_obj})
