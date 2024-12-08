
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

from django.shortcuts import render
from django.utils.timezone import now
from .models import CompletionRecord

def todays_completion_records_view(request):
    # Get today's date in the server's timezone
    today = now().date()

    # Fetch the search query from the request, if provided
    query = request.GET.get('query', '').strip()

    # Base query to filter records for today
    records_query = CompletionRecord.objects.filter(completion_date__date=today)

    # Apply search filter if a query is provided
    if query:
        records_query = records_query.filter(
            employee_id__icontains=query
        ) | CompletionRecord.objects.filter(
            employee_email__icontains=query
        ) | CompletionRecord.objects.filter(
            course_code__icontains=query
        )

    # Order the records by completion_date
    records = records_query.order_by('completion_date')

    # Render the template with the filtered records
    return render(request, 'aimsintegration/cpat_completion_records.html', {
        'records': records,
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
