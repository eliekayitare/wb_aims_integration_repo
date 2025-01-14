
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
from .models import FdmFlightData
import logging
import datetime as dt
from django.utils.timezone import make_aware


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
    # filter_date = make_aware(datetime.combine(filter_date, datetime.min.time()), timezone=timezone.utc)
    
    filter_date = make_aware(dt.datetime.combine(filter_date, dt.datetime.min.time()),dt.timezone.utc)

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
# import csv
# from datetime import datetime, date
# from django.shortcuts import render, redirect, get_object_or_404
# from django.http import JsonResponse, HttpResponse
# from django.utils import timezone
# from django.db.models import Sum
# from decimal import Decimal

# from .models import Crew, Duty, Airport, Invoice, InvoiceItem
# from .forms import CSVUploadForm


# def upload_callowance_file(request):
#     """
#     Allows user to upload a CSV file with Duty records.
#     """
#     if request.method == 'POST':
#         form = CSVUploadForm(request.POST, request.FILES)
#         if form.is_valid():
#             csv_file = request.FILES['file']
#             handle_callowance_csv(csv_file)
#             # After processing, redirect to the list where user can see results
#             return redirect('crew_allowance_list')
#     else:
#         form = CSVUploadForm()

#     return render(request, 'callowance_upload.html', {
#         'form': form,
#     })


# def handle_callowance_csv(csv_file):
#     """
#     Parses the CSV and creates/updates Duty records (and related Crew/Airports).
#     Example CSV columns (adjust indices as needed):
#       Date, CrewID, FName, LName, FlightNo, Dep, Arr, Layover(min), TailNo
#     """
#     decoded_file = csv_file.read().decode('utf-8', errors='replace').splitlines()
#     reader = csv.reader(decoded_file)

#     # skip header if needed
#     # next(reader, None)

#     for row in reader:
#         # Adjust indices to match your CSV
#         duty_date_str = row[0].strip()
#         crew_id_str = row[1].strip()
#         first_name = row[2].strip()
#         last_name = row[3].strip()
#         flight_no = row[4].strip()
#         dep_code = row[5].strip()
#         arr_code = row[6].strip()
#         layover_str = row[7].strip()
#         tail_no = row[8].strip() if len(row) > 8 else ""

#         # Convert date
#         # Expected format "YYYY-MM-DD" or "DD/MM/YY" etc. Adjust to your CSV
#         try:
#             duty_date = datetime.strptime(duty_date_str, '%Y-%m-%d').date()
#         except ValueError:
#             # fallback or handle differently if format is different
#             duty_date = datetime.strptime(duty_date_str, '%d/%m/%y').date()

#         # Layover in minutes
#         try:
#             layover_minutes = int(layover_str)
#         except ValueError:
#             layover_minutes = 0

#         # Crew
#         crew, _ = Crew.objects.get_or_create(
#             crew_id=crew_id_str,
#             defaults={'first_name': first_name, 'last_name': last_name}
#         )
#         # Optionally update the crew info if it changes:
#         # crew.first_name = first_name
#         # crew.last_name = last_name
#         # crew.save()

#         # Airport (dep)
#         departure_airport, _ = Airport.objects.get_or_create(iata_code=dep_code)
#         # Airport (arr)
#         arrival_airport, _ = Airport.objects.get_or_create(iata_code=arr_code)

#         # Create the Duty
#         Duty.objects.create(
#             duty_date=duty_date,
#             crew=crew,
#             flight_number=flight_no,
#             departure_airport=departure_airport,
#             arrival_airport=arrival_airport,
#             layover_time_minutes=layover_minutes,
#             tail_number=tail_no
#         )


# def crew_allowance_list(request):
#     """
#     Shows a table with:
#       - One row per Crew for the selected month
#       - The sum of allowances for that month
#       - A button to 'View' details of that crew's duties
#       - A button to 'Generate Overall Invoice'
#     Filters by month/year. Default: previous month.
#     """
#     # 1) Determine which month/year to display
#     #    We can receive something like ?month=2025-01 for January 2025
#     #    If none, default to previous month
#     month_str = request.GET.get('month')
#     if month_str:
#         # parse the string
#         year, mo = map(int, month_str.split('-'))
#         filter_month = date(year, mo, 1)
#     else:
#         # default: previous month from now
#         today = date.today()
#         first_of_this_month = date(today.year, today.month, 1)
#         # Subtract 1 day from the first of this month => end of previous month
#         # and then take that month's "first day"
#         prev_month_end = first_of_this_month.replace(day=1) - timezone.timedelta(days=1)
#         filter_month = date(prev_month_end.year, prev_month_end.month, 1)

#     # 2) For each crew, find if there's an existing invoice for that month.
#     #    If no invoice, we can compute an "on-the-fly" total or just show 0.
#     #    Alternatively, we can do a query that sums the duties for each crew.

#     # Let’s see if we already have Invoices for that month
#     invoices_qs = Invoice.objects.filter(month=filter_month)
#     existing_invoices = {inv.crew_id: inv for inv in invoices_qs}

#     # We'll build a list of crew_data objects for the template
#     crews = Crew.objects.all()
#     crew_data_list = []
#     for cr in crews:
#         invoice = existing_invoices.get(cr.id)
#         if invoice:
#             total_amount = invoice.total_amount
#             invoice_id = invoice.id
#         else:
#             # no invoice found, compute the sum of all duties for this month on the fly
#             total_amount = compute_crew_allowance_for_month(cr, filter_month)
#             invoice_id = None

#         crew_data_list.append({
#             'crew': cr,
#             'total_amount': total_amount,
#             'invoice_id': invoice_id
#         })

#     context = {
#         'selected_month': filter_month,
#         'crew_data_list': crew_data_list,
#     }
#     return render(request, 'crew_allowance_list.html', context)


# def compute_crew_allowance_for_month(crew, month_first_day):
#     """
#     Compute the allowance for a crew for a given month (on the fly),
#     by summing the duties in that month * some rate logic.
#     (Adjust your daily/hourly logic here.)
#     """
#     year = month_first_day.year
#     month = month_first_day.month

#     duties = Duty.objects.filter(crew=crew, duty_date__year=year, duty_date__month=month)
#     # Example: let's assume a flat $2/hour logic
#     # If layover_time_minutes is used, we convert to hours, multiply by rate
#     total = Decimal('0.00')
#     hourly_rate = Decimal('2.00')  # just an example

#     for d in duties:
#         hours = Decimal(d.layover_time_minutes) / Decimal(60)
#         total += hours * hourly_rate

#     return total.quantize(Decimal('0.00'))


# def generate_overall_invoice(request):
#     """
#     Generate or update Invoices for every crew in the specified month (GET param).
#     If none provided, uses the default (previous month).
#     """
#     month_str = request.GET.get('month')
#     if month_str:
#         year, mo = map(int, month_str.split('-'))
#         filter_month = date(year, mo, 1)
#     else:
#         # same logic as above: default to previous month
#         today = date.today()
#         first_of_this_month = date(today.year, today.month, 1)
#         prev_month_end = first_of_this_month - timezone.timedelta(days=1)
#         filter_month = date(prev_month_end.year, prev_month_end.month, 1)

#     # For each crew, gather all duties in that month, create or update Invoice
#     # Then create InvoiceItems for each Duty
#     all_crews = Crew.objects.all()
#     for cr in all_crews:
#         # fetch or create invoice
#         invoice, _ = Invoice.objects.get_or_create(
#             crew=cr,
#             month=filter_month,
#         )
#         # Clear out old invoice items if needed (re-generate)
#         invoice.invoiceitem_set.all().delete()

#         # find all duties in that month
#         year = filter_month.year
#         month = filter_month.month
#         duties = Duty.objects.filter(crew=cr, duty_date__year=year, duty_date__month=month)

#         total_for_crew = Decimal('0.00')
#         for d in duties:
#             # Example logic: $2/hour
#             hours = Decimal(d.layover_time_minutes) / Decimal(60)
#             line_amount = hours * Decimal('2.00')
#             InvoiceItem.objects.create(
#                 invoice=invoice,
#                 duty=d,
#                 allowance_amount=line_amount.quantize(Decimal('0.00'))
#             )
#             total_for_crew += line_amount

#         invoice.total_amount = total_for_crew.quantize(Decimal('0.00'))
#         invoice.save()

#     # Redirect back to the crew allowance list
#     return redirect('crew_allowance_list')


# def crew_allowance_details(request, crew_id, year, month):
#     """
#     Show a detail page (or modal) listing all the duties for one crew in the specified month,
#     plus a total allowance.
#     """
#     cr = get_object_or_404(Crew, id=crew_id)
#     filter_month = date(year, month, 1)

#     # either get existing invoice or compute on the fly
#     try:
#         invoice = Invoice.objects.get(crew=cr, month=filter_month)
#         total_amount = invoice.total_amount
#         # we can also get the invoice items
#         invoice_items = invoice.invoiceitem_set.select_related('duty').all()
#         duties_list = [item.duty for item in invoice_items]
#     except Invoice.DoesNotExist:
#         # no invoice, compute on the fly
#         duties_list = Duty.objects.filter(
#             crew=cr,
#             duty_date__year=year,
#             duty_date__month=month
#         )
#         total_amount = compute_crew_allowance_for_month(cr, filter_month)

#     context = {
#         'crew': cr,
#         'filter_month': filter_month,
#         'duties_list': duties_list,
#         'total_amount': total_amount,
#     }
#     return render(request, 'crew_allowance_details.html', context)
