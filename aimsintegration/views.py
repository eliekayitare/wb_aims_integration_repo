
from django.shortcuts import render
from django.http import JsonResponse
from .models import FlightData,FdmFlightData
from datetime import date, datetime
from .models import *
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
from django.db.models import Case, When, Value, IntegerField
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
    # crew_members = CrewMember.objects.filter(
    #     flight_no=flight_no,
    #     origin=origin,
    #     destination=destination,
    #     sd_date_utc=date_obj
    # ).values('name', 'role', 'flight_no', 'origin', 'destination', 'crew_id')
    

    crew_members = CrewMember.objects.filter(
        flight_no=flight_no,
        origin=origin,
        destination=destination,
        sd_date_utc=date_obj
    ).values('name', 'role', 'flight_no', 'origin', 'destination', 'crew_id').order_by(
        Case(
            When(role='CP', then=Value(1)),  # Assign priority 1 to CP (Captain)
            default=Value(2),  # Assign priority 2 to all other roles
            output_field=IntegerField()
        )
    )


    # Log the query results
    logger.info(f"Crew members found: {list(crew_members)}")

    # Return the response
    return JsonResponse(list(crew_members), safe=False)





# Crew Allowance project
import csv
import io
from datetime import datetime, date
from decimal import Decimal

from django.shortcuts import render, redirect, get_object_or_404
from django.http import JsonResponse, HttpResponse
from django.utils import timezone
from django.db.models import Sum
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage

import openpyxl

from .models import Crew, Duty, Airport, Invoice, InvoiceItem
from .forms import CSVUploadForm


def get_display_pages(page_obj, num_links=2):
    """
    Returns a list of page numbers around the current page,
    plus '...' if there's a gap, plus the very first and last pages if needed.
    """
    current_page = page_obj.number
    total_pages = page_obj.paginator.num_pages

    # If total_pages is small, just show all pages.
    if total_pages <= num_links * 2 + 6:
        return list(range(1, total_pages + 1))

    # Otherwise, build a “window” around current_page.
    pages = []
    left_bound = current_page - num_links
    right_bound = current_page + num_links

    # Always show page #1
    pages.append(1)

    # "..." if there's a gap between 1 and left_bound
    if left_bound > 2:
        pages.append("...")

    # Pages in [left_bound..right_bound], clipped to [2..(total_pages-1)]
    for p in range(max(left_bound, 2), min(right_bound, total_pages - 1) + 1):
        pages.append(p)

    # "..." if there's a gap between right_bound and total_pages - 1
    if right_bound < total_pages - 1:
        pages.append("...")

    # Always show the last page
    pages.append(total_pages)

    return pages


def upload_callowance_file(request):
    """
    Allows user to upload a CSV or TXT file that may contain multiple months of data.
    """
    if request.method == 'POST':
        form = CSVUploadForm(request.POST, request.FILES)
        if form.is_valid():
            csv_file = request.FILES['file']
            # Parse and generate invoices for all months found
            handle_callowance_csv(csv_file)
            return redirect('crew_allowance_list')
    else:
        form = CSVUploadForm()

    return render(request, 'aimsintegration/callowance_upload.html', {
        'form': form,
    })


def handle_callowance_csv(csv_file):
    """
    1. Reads CSV/TXT rows.
    2. Converts layover hh:mm -> minutes in Duty.
    3. Creates/updates Crew, Duty, etc.
    4. For each distinct (Crew, Month) in the file, automatically creates/updates
       an Invoice and its InvoiceItems. That way if the CSV has multiple months,
       each month gets its own Invoice.
    """
    decoded_file = csv_file.read().decode('utf-8', errors='replace').splitlines()
    reader = csv.reader(decoded_file)

    # next(reader, None)  # Uncomment if first row is a header

    # We'll store (crew_id, month_first_day) -> list_of_duty_ids
    duties_by_crew_month = {}

    for idx, row in enumerate(reader, start=1):
        # Safe extraction of each column; adjust indices to match your CSV
        date_str    = row[0].strip() if len(row) > 0 else ""
        crew_id_str = row[1].strip() if len(row) > 1 else ""
        first_name  = row[2].strip() if len(row) > 2 else ""
        last_name   = row[3].strip() if len(row) > 3 else ""
        position    = row[4].strip() if len(row) > 4 else ""
        flight_no   = row[5].strip() if len(row) > 5 else ""
        tail_no     = row[6].strip() if len(row) > 6 else ""
        dep_code    = row[7].strip().upper() if len(row) > 7 else ""
        arr_code    = row[8].strip().upper() if len(row) > 8 else ""
        layover_str = row[9].strip() if len(row) > 9 else ""

        # Parse the date
        duty_date = None
        if date_str:
            for fmt in ["%d/%m/%y", "%Y-%m-%d"]:
                try:
                    duty_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    pass

        if not duty_date:
            # If we can't parse the date, skip this row
            continue

        # Parse layover hh:mm => total minutes
        layover_minutes = 0
        if ":" in layover_str:
            try:
                hh, mm = layover_str.split(":")
                layover_minutes = int(hh)*60 + int(mm)
            except ValueError:
                pass

        # Crew
        if not crew_id_str:
            crew_id_str = f"no_id_{idx}"
        crew_obj, created = Crew.objects.get_or_create(
            crew_id=crew_id_str,
            defaults={
                'first_name': first_name,
                'last_name': last_name,
                'position': position,
            }
        )
        if not created:
            # Optionally update if changed
            crew_obj.first_name = first_name
            crew_obj.last_name = last_name
            crew_obj.position = position
            crew_obj.save()

        # Airports (Note: if no zone is assigned in DB, the allowance will be 0)
        dep_airport = None
        if dep_code:
            dep_airport, _ = Airport.objects.get_or_create(iata_code=dep_code)
        arr_airport = None
        if arr_code:
            arr_airport, _ = Airport.objects.get_or_create(iata_code=arr_code)

        # Create Duty
        duty = Duty.objects.create(
            duty_date=duty_date,
            crew=crew_obj,
            flight_number=flight_no,
            departure_airport=dep_airport,
            arrival_airport=arr_airport,
            layover_time_minutes=layover_minutes,
            tail_number=tail_no
        )

        # Identify the "month" -> 1st day of that month
        month_first = date(duty_date.year, duty_date.month, 1)
        key = (crew_obj.id, month_first)
        if key not in duties_by_crew_month:
            duties_by_crew_month[key] = []
        duties_by_crew_month[key].append(duty.id)

    # Now generate Invoices for each (Crew, Month) found in the file
    for (crew_id, month_day), duty_ids in duties_by_crew_month.items():
        cr = Crew.objects.get(id=crew_id)
        invoice, _ = Invoice.objects.get_or_create(
            crew=cr,
            month=month_day
        )
        # Clear old invoice items if re-running
        invoice.invoiceitem_set.all().delete()

        total_for_crew = Decimal('0.00')
        for d_id in duty_ids:
            d = Duty.objects.get(id=d_id)
            # Convert minutes -> hours
            hours = Decimal(d.layover_time_minutes) / Decimal(60)
            if d.arrival_airport and d.arrival_airport.zone:
                rate = d.arrival_airport.zone.hourly_rate
            else:
                rate = Decimal('0.00')

            line_amount = hours * rate
            InvoiceItem.objects.create(
                invoice=invoice,
                duty=d,
                allowance_amount=line_amount.quantize(Decimal('0.00'))
            )
            total_for_crew += line_amount

        invoice.total_amount = total_for_crew.quantize(Decimal('0.00'))
        invoice.save()



from django.db.models import Max
from django.db.models import Q

def crew_allowance_list(request):
    """
    Shows a paginated table of only those Invoices for the selected month,
    skipping any that are zero. If the user doesn't pick ?month=,
    we find the most recent month that has at least one invoice > 0.
    """
    month_str = request.GET.get('month')
    sort_order = request.GET.get('sort', 'asc') # default to ascending
    search_query = request.GET.get('search', '').strip() # search by first/last name
    # ------------------------
    # 1) If user picks a month, parse it
    # ------------------------
    if month_str:
        year, mo = map(int, month_str.split('-'))
        filter_month = date(year, mo, 1)
        
    else:
        # ------------------------
        # 2) Otherwise, find the "latest" month that has at least one > 0
        # ------------------------
        # We'll look at all distinct months in descending order:
        distinct_months = (
            Invoice.objects
            .values_list('month', flat=True)
            .distinct()
            .order_by('-month')
        )

        filter_month = None
        for m in distinct_months:
            # Check if this month has an Invoice with total_amount > 0
            has_nonzero = Invoice.objects.filter(month=m, total_amount__gt=0).exists()
            if has_nonzero:
                # This is our default month
                filter_month = m
                break

    # If we still have None => no data, or all zero
    if not filter_month:
        context = {
            'selected_month': None,
            'crew_data_list': [],
            'display_pages': [],
        }
        return render(request, 'aimsintegration/crew_allowance.html', context)

    # ------------------------
    # 3) Now fetch Invoices for that month, skipping total_amount=0
    # ------------------------
    invoices_qs = (
        Invoice.objects
        .filter(month=filter_month, total_amount__gt=0)  # skip zero
        .select_related('crew')
        .order_by('crew__crew_id')
    )

     # Apply search filter
    if search_query:
        invoices_qs = invoices_qs.filter(
            Q(crew__first_name__icontains=search_query) |
            Q(crew__last_name__icontains=search_query) |
            Q(crew__crew_id__icontains=search_query) |
            Q(crew__position__icontains=search_query)
        )

    # Apply sorting based on the sort order
    if sort_order == 'asc':
        invoices_qs = invoices_qs.order_by('total_amount')
    elif sort_order == 'desc':
        invoices_qs = invoices_qs.order_by('-total_amount')

    raw_crew_data_list = []
    for inv in invoices_qs:
        raw_crew_data_list.append({
            'crew': inv.crew,
            'total_amount': inv.total_amount,
            'invoice_id': inv.id,
            'invoice_month': inv.month,
        })

    # ------------------------
    # 4) Paginate
    # ------------------------
    paginator = Paginator(raw_crew_data_list, 10)
    page_number = request.GET.get('page', 1)
    try:
        crew_data_page = paginator.page(page_number)
    except PageNotAnInteger:
        crew_data_page = paginator.page(1)
    except EmptyPage:
        crew_data_page = paginator.page(paginator.num_pages)

    # ------------------------
    # 5) Windowed page range
    # ------------------------
    display_pages = get_display_pages(crew_data_page, num_links=2)

    context = {
        'selected_month': filter_month,
        'crew_data_list': crew_data_page,
        'display_pages': display_pages,
    }
    return render(request, 'aimsintegration/crew_allowance.html', context)






def compute_crew_allowance_for_month(crew, month_first_day):
    """
    On-the-fly calculation: sum each Duty's layover hours * arrival zone's rate.
    """
    year = month_first_day.year
    month = month_first_day.month

    duties = Duty.objects.filter(crew=crew, duty_date__year=year, duty_date__month=month)
    total = Decimal('0.00')
    for d in duties:
        if d.arrival_airport and d.arrival_airport.zone:
            rate = d.arrival_airport.zone.hourly_rate
        else:
            rate = Decimal('0.00')
        hours = Decimal(d.layover_time_minutes) / Decimal(60)
        total += hours * rate

    return total.quantize(Decimal('0.00'))





def crew_allowance_details(request, crew_id, year, month):
    """
    Displays (or returns JSON) all duties for one crew in the chosen month.
    For each duty:
      - Layover in HOURS (not minutes)
      - Rate per hour (based on arrival airport's zone)
      - Line amount = hours * rate
    Then, show a total at the bottom of the modal.
    """
    from decimal import Decimal

    cr = get_object_or_404(Crew, id=crew_id)
    filter_month = date(year, month, 1)

    try:
        invoice = Invoice.objects.get(crew=cr, month=filter_month)
        invoice_items = invoice.invoiceitem_set.select_related('duty').all()
        # If ignoring zero-layover:
        invoice_items = invoice_items.filter(duty__layover_time_minutes__gt=0)
        duties_list = [item.duty for item in invoice_items]
        # We can use the invoice's total_amount or compute a grand_total below
        invoice_total = invoice.total_amount
    except Invoice.DoesNotExist:
        # Fallback: no invoice => fetch from Duty
        duties_list = Duty.objects.filter(
            crew=cr,
            duty_date__year=year,
            duty_date__month=month,
            layover_time_minutes__gt=0  # if ignoring 0-layover
        )
        invoice_total = compute_crew_allowance_for_month(cr, filter_month)

    # Build a data structure that includes layover_hours, rate, line_amount
    duties_data = []
    grand_total = Decimal('0.00')  # if you want to compute on the fly

    for d in duties_list:
        # Convert minutes -> decimal hours
        layover_hours = Decimal(d.layover_time_minutes) / Decimal(60)
        # Rate from arrival's zone
        if d.arrival_airport and d.arrival_airport.zone:
            hourly_rate = d.arrival_airport.zone.hourly_rate
        else:
            hourly_rate = Decimal('0.00')

        line_amount = layover_hours * hourly_rate
        grand_total += line_amount

        duties_data.append({
            'duty_date': str(d.duty_date) if d.duty_date else "",
            'flight_number': d.flight_number,
            'departure': d.departure_airport.iata_code if d.departure_airport else "",
            'arrival': d.arrival_airport.iata_code if d.arrival_airport else "",
            'layover_hours': str(layover_hours.quantize(Decimal('0.00'))),
            'hourly_rate': str(hourly_rate.quantize(Decimal('0.00'))),
            'line_amount': str(line_amount.quantize(Decimal('0.00'))),
            'tail_number': d.tail_number,
        })

    # final total => either use invoice_total or grand_total
    # (If your invoice is already correct, just use invoice_total.)
    final_total = grand_total.quantize(Decimal('0.00'))  # or invoice_total

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        # Return JSON to the modal
        data = {
            'crew_info': f"{cr.crew_id} - {cr.first_name} {cr.last_name}",
            'duties': duties_data,
            'total_amount': str(final_total),  # So we can show at bottom of modal
        }
        return JsonResponse(data)
    else:
        # Or render a template with all that info
        context = {
            'crew': cr,
            'filter_month': filter_month,
            'duties_data': duties_data,
            'final_total': final_total,
        }
        return render(request, 'aimsintegration/crew_allowance_details.html', context)
    


# somewhere in utils/pdf_utils.py
import io
from xhtml2pdf import pisa

def convert_html_to_pdf(html_content):
    """
    Returns a bytes object of the PDF, or None if there's an error.
    """
    result = io.BytesIO()
    pdf = pisa.pisaDocument(io.BytesIO(html_content.encode("utf-8")), result)
    if not pdf.err:
        return result.getvalue()
    return None



# Suppose you're in a view that handles "Generate Overall Payslip"


from django.db import connections
from decimal import Decimal
from datetime import date
from django.db.models import Max
from django.http import HttpResponse
from django.template.loader import render_to_string
from django.templatetags.static import static
from .models import Invoice


def generate_overall_payslip(request):
    # 1) Read ?month=YYYY-MM
    month_str = request.GET.get('month')
    if not month_str:
        # Find the latest (max) month in your Invoice table if not provided
        last_invoice = (
            Invoice.objects
            .filter(total_amount__gt=0)
            .aggregate(max_month=Max('month'))
        )
        filter_month = last_invoice['max_month']
        if not filter_month:
            return HttpResponse("No invoices found at all in the database.", status=404)
    else:
        # Parse the user-provided month
        year, mo = map(int, month_str.split('-'))
        filter_month = date(year, mo, 1)

    # 2) Fetch Invoices for that month, skipping total_amount=0
    invoices = (
        Invoice.objects
        .filter(month=filter_month, total_amount__gt=0)
        .select_related('crew')
        .order_by('crew__crew_id')
    )

    if not invoices.exists():
        return HttpResponse("No nonzero invoices found for that month.", status=404)

    # 3) Connect to MSSQL to get the exchange rate
    with connections['mssql'].cursor() as cursor:
        cursor.execute("""
            SELECT [Relational Exch_ Rate Amount]
            FROM [RwandAir].[dbo].[RwandAir$Currency Exchange Rate$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [Currency Code] = 'USD'
              AND [Starting Date] = (
                  SELECT MAX([Starting Date])
                  FROM [RwandAir].[dbo].[RwandAir$Currency Exchange Rate$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
                  WHERE [Currency Code] = 'USD'
              );
        """)
        row = cursor.fetchone()

    if not row:
        return HttpResponse("No exchange rate found from that query.", status=400)

    exchange_rate = Decimal(str(row[0]))

    # 4a) Gather all unique crew WB numbers and format them to WBXXXX
    crew_ids = list({inv.crew.crew_id for inv in invoices})
    wb_formatted_ids = [f"WB{int(cid):04d}" for cid in crew_ids]

    # 4b) Fetch bank details for these WB numbers
    employee_bank_data = {}
    if wb_formatted_ids:
        placeholders = ", ".join(["%s"] * len(wb_formatted_ids))
        query = f"""
            SELECT [No_], [Bank Name], [Bank Account No]
            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [No_] IN ({placeholders})
        """
        with connections['mssql'].cursor() as cursor:
            cursor.execute(query, wb_formatted_ids)
            rows = cursor.fetchall()

        for no_, bank_name, bank_account_no in rows:
            formatted_no = no_.strip()
            employee_bank_data[formatted_no] = {
                'bank_name': bank_name.strip() if bank_name else '',
                'bank_account_no': bank_account_no.strip() if bank_account_no else '',
            }

    # 4c) Build items list for the PDF table
    items = []
    for inv in invoices:
        wb_formatted = f"WB{int(inv.crew.crew_id):04d}"
        bank_data = employee_bank_data.get(wb_formatted, {
            'bank_name': '',
            'bank_account_no': '',
        })

        usd_amount = inv.total_amount
        rwf_amount = (usd_amount * exchange_rate).quantize(Decimal('0.00'))
        bank_name = bank_data['bank_name'].strip() if bank_data['bank_name'] else '-'
        account_no = bank_data['bank_account_no'].strip() if bank_data['bank_account_no'] else '-'


        items.append({
            'wb_no': inv.crew.crew_id,
            'name': f"{inv.crew.first_name} {inv.crew.last_name}",
            'position': inv.crew.position,
            'usd_amount': usd_amount,
            'rwf_amount': f"{rwf_amount:,.2f}",  # Add commas to RWF
            'exchange_rate': exchange_rate,
            'bank_name': bank_name,
            'account_no': account_no,
        })

        print("\n================================================\n")
        print(f"wbno: {inv.crew.crew_id}\n")
        print(f"name: {inv.crew.first_name} {inv.crew.last_name}\n")
        print(f"position: {inv.crew.position}\n")
        print(f"usd_amount: {usd_amount}\n")
        print(f"rwf_amount: {rwf_amount:,.2f}\n")
        print(f"exchange_rate: {exchange_rate}\n")  
        print(f"bank_name: {bank_name}\n")
        print(f"account_no: {account_no}\n")
        print("\n================================================\n")


    # Calculate totals
    total_usd = sum([inv.total_amount for inv in invoices])
    total_rwf = sum([(inv.total_amount * exchange_rate).quantize(Decimal('0.00')) for inv in invoices])
    # Pass current date
    current_date = datetime.now().strftime("%B %d, %Y")  # Example: "January 19, 2025"
    # 5) Render to HTML via template
    context = {
        'items': items,
        'filter_month': filter_month,
        'exchange_rate': exchange_rate,
        'logo_path': request.build_absolute_uri(static('images/rwandair-logo.png')),
        'total_usd': f"{total_usd:,.2f}",  # Format with commas
        'total_rwf': f"{total_rwf:,.2f}",  # Format with commas
        'current_date': current_date,
    }
    html_string = render_to_string('aimsintegration/payslip_template.html', context)

    # Save the HTML output to a file for debugging
    # with open('test_output.html', 'w') as file:
    #     file.write(html_string)
        
    # 6) Convert the HTML to PDF
    pdf_file = convert_html_to_pdf(html_string)
    if not pdf_file:
        return HttpResponse("Error generating PDF.", status=500)

    # 7) Return PDF response
    response = HttpResponse(pdf_file, content_type='application/pdf')
    filename = f"Crew_Allowance_Payslip_{filter_month.strftime('%Y-%m')}.pdf"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response




def generate_usd_payslip(request):
    # Get the month parameter
    month_str = request.GET.get('month')
    if not month_str:
        last_invoice = Invoice.objects.filter(total_amount__gt=0, crew__position="CP").aggregate(max_month=Max('month'))
        filter_month = last_invoice['max_month']
        if not filter_month:
            return HttpResponse("No invoices found for CP in the database.", status=404)
    else:
        year, mo = map(int, month_str.split('-'))
        filter_month = date(year, mo, 1)

    # Fetch invoices for CP (USD only)
    cp_invoices = Invoice.objects.filter(
        month=filter_month, total_amount__gt=0, crew__position="CP"
    ).select_related('crew').order_by('crew__crew_id')

    if not cp_invoices.exists():
        return HttpResponse("No invoices found for CP.", status=404)

    # Fetch bank details for these WB numbers
    crew_ids = list({inv.crew.crew_id for inv in cp_invoices})
    wb_formatted_ids = [f"WB{int(cid):04d}" for cid in crew_ids]

    employee_bank_data = {}
    if wb_formatted_ids:
        placeholders = ", ".join(["%s"] * len(wb_formatted_ids))
        query = f"""
            SELECT [No_], [Bank Name], [Bank Account No]
            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [No_] IN ({placeholders})
        """
        with connections['mssql'].cursor() as cursor:
            cursor.execute(query, wb_formatted_ids)
            rows = cursor.fetchall()

        for no_, bank_name, bank_account_no in rows:
            formatted_no = no_.strip()
            employee_bank_data[formatted_no] = {
                'bank_name': bank_name.strip() if bank_name else '-',
                'bank_account_no': bank_account_no.strip() if bank_account_no else '-',
            }

    # Prepare context
    cp_items = []
    for inv in cp_invoices:
        wb_formatted = f"WB{int(inv.crew.crew_id):04d}"
        bank_data = employee_bank_data.get(wb_formatted, {
            'bank_name': '-',
            'bank_account_no': '-',
        })

        cp_items.append({
            'wb_no': inv.crew.crew_id,
            'name': f"{inv.crew.first_name} {inv.crew.last_name}",
            'position': inv.crew.position,
            'usd_amount': inv.total_amount,
            'bank_name': bank_data['bank_name'],
            'account_no': bank_data['bank_account_no'],
        })

    total_usd = sum([inv['usd_amount'] for inv in cp_items])

    context = {
        'items': cp_items,
        'filter_month': filter_month,
        'total_usd': f"{total_usd:,.2f}",
        'logo_path': request.build_absolute_uri(static('images/rwandair-logo.png')),
        'current_date': datetime.now().strftime("%B %d, %Y"),  # Example: "January 19, 2025"
    }

    # Render and generate PDF
    html_string = render_to_string('aimsintegration/usd_payslip.html', context)
    pdf_file = convert_html_to_pdf(html_string)
    if not pdf_file:
        return HttpResponse("Error generating CP PDF.", status=500)

    # Return PDF response
    response = HttpResponse(pdf_file, content_type='application/pdf')
    filename = f"USD_Payslip_{filter_month.strftime('%Y-%m')}.pdf"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response






def generate_others_payslip(request):
    # 1) Get the month parameter
    month_str = request.GET.get('month')
    if not month_str:
        last_invoice = Invoice.objects.filter(total_amount__gt=0).exclude(crew__position="CP").aggregate(max_month=Max('month'))
        filter_month = last_invoice['max_month']
        if not filter_month:
            return HttpResponse("No invoices found for others in the database.", status=404)
    else:
        year, mo = map(int, month_str.split('-'))
        filter_month = date(year, mo, 1)

    # 2) Fetch invoices for others
    other_invoices = Invoice.objects.filter(
        month=filter_month, total_amount__gt=0
    ).exclude(crew__position="CP").select_related('crew').order_by('crew__crew_id')

    if not other_invoices.exists():
        return HttpResponse("No invoices found for others.", status=404)

    # 3) Connect to MSSQL to get the exchange rate
    with connections['mssql'].cursor() as cursor:
        cursor.execute("""
            SELECT [Relational Exch_ Rate Amount]
            FROM [RwandAir].[dbo].[RwandAir$Currency Exchange Rate$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [Currency Code] = 'USD'
              AND [Starting Date] = (
                  SELECT MAX([Starting Date])
                  FROM [RwandAir].[dbo].[RwandAir$Currency Exchange Rate$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
                  WHERE [Currency Code] = 'USD'
              );
        """)
        row = cursor.fetchone()

    if not row:
        return HttpResponse("No exchange rate found from that query.", status=400)

    exchange_rate = Decimal(str(row[0]))

    # 4a) Gather all unique crew WB numbers and format them
    crew_ids = list({inv.crew.crew_id for inv in other_invoices})
    wb_formatted_ids = [f"WB{int(cid):04d}" for cid in crew_ids]

    # 4b) Fetch bank details for these WB numbers
    employee_bank_data = {}
    if wb_formatted_ids:
        placeholders = ", ".join(["%s"] * len(wb_formatted_ids))
        query = f"""
            SELECT [No_], [Bank Name], [Bank Account No]
            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [No_] IN ({placeholders})
        """
        with connections['mssql'].cursor() as cursor:
            cursor.execute(query, wb_formatted_ids)
            rows = cursor.fetchall()

        for no_, bank_name, bank_account_no in rows:
            formatted_no = no_.strip()
            employee_bank_data[formatted_no] = {
                'bank_name': bank_name.strip() if bank_name else '-',
                'bank_account_no': bank_account_no.strip() if bank_account_no else '-',
            }

    # 4c) Build items list for the PDF table
    items = []
    for inv in other_invoices:
        wb_formatted = f"WB{int(inv.crew.crew_id):04d}"
        bank_data = employee_bank_data.get(wb_formatted, {
            'bank_name': '-',
            'bank_account_no': '-',
        })

        usd_amount = inv.total_amount
        rwf_amount = (usd_amount * exchange_rate).quantize(Decimal('0.00'))
        bank_name = bank_data['bank_name']
        account_no = bank_data['bank_account_no']

        items.append({
            'wb_no': inv.crew.crew_id,
            'name': f"{inv.crew.first_name} {inv.crew.last_name}",
            'position': inv.crew.position,
            'usd_amount': usd_amount,
            'rwf_amount': f"{rwf_amount:,.2f}",
            'exchange_rate': exchange_rate,
            'bank_name': bank_name,
            'account_no': account_no,
        })

    # Calculate totals
    total_usd = sum([inv['usd_amount'] for inv in items])
    total_rwf = sum([Decimal(inv['rwf_amount'].replace(',', '')) for inv in items])

    # 5) Pass data to the context
    current_date = datetime.now().strftime("%B %d, %Y")  # Example: "January 19, 2025"
    context = {
        'items': items,
        'filter_month': filter_month,
        'exchange_rate': exchange_rate,
        'logo_path': request.build_absolute_uri(static('images/rwandair-logo.png')),
        'total_usd': f"{total_usd:,.2f}",
        'total_rwf': f"{total_rwf:,.2f}",
        'current_date': current_date,
    }

    # 6) Render and generate the PDF
    html_string = render_to_string('aimsintegration/others_payslip.html', context)
    pdf_file = convert_html_to_pdf(html_string)
    if not pdf_file:
        return HttpResponse("Error generating Others PDF.", status=500)

    # 7) Return the PDF response
    response = HttpResponse(pdf_file, content_type='application/pdf')
    filename = f"RWFs_Payslip_{filter_month.strftime('%Y-%m')}.pdf"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response




# def generate_payslip_for_bank(request):
#     # 1) Read ?month=YYYY-MM and ?bank_name
#     month_str = request.GET.get('month')
#     bank_name = request.GET.get('bank_name')
#     if not month_str or not bank_name:
#         return HttpResponse("Month and bank name are required parameters.", status=400)

#     # Parse the month
#     year, mo = map(int, month_str.split('-'))
#     filter_month = date(year, mo, 1)

#     # 2) Fetch invoices for the specific bank
#     invoices = (
#         Invoice.objects
#         .filter(month=filter_month, total_amount__gt=0)
#         .select_related('crew')
#         .order_by('crew__crew_id')
#     )

#     # Fetch bank details
#     crew_ids = list({inv.crew.crew_id for inv in invoices})
#     wb_formatted_ids = [f"WB{int(cid):04d}" for cid in crew_ids]
#     employee_bank_data = {}

#     if wb_formatted_ids:
#         placeholders = ", ".join(["%s"] * len(wb_formatted_ids))
#         query = f"""
#             SELECT [No_], [Bank Name], [Bank Account No]
#             FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
#             WHERE [No_] IN ({placeholders})
#         """
#         with connections['mssql'].cursor() as cursor:
#             cursor.execute(query, wb_formatted_ids)
#             rows = cursor.fetchall()

#         for no_, b_name, account_no in rows:
#             formatted_no = no_.strip()
#             employee_bank_data[formatted_no] = {
#                 'bank_name': b_name.strip() if b_name else '',
#                 'account_no': account_no.strip() if account_no else '',
#             }

#     # Filter invoices for the given bank name
#     items = []
#     for inv in invoices:
#         wb_formatted = f"WB{int(inv.crew.crew_id):04d}"
#         bank_data = employee_bank_data.get(wb_formatted, {'bank_name': '', 'account_no': '-'})
#         if bank_data['bank_name'] != bank_name:
#             continue

#         usd_amount = inv.total_amount
#         rwf_amount = (usd_amount * Decimal(1.0)).quantize(Decimal('0.00'))

#         items.append({
#             'wb_no': inv.crew.crew_id,
#             'name': f"{inv.crew.first_name} {inv.crew.last_name}",
#             'position': inv.crew.position,
#             'usd_amount': usd_amount,
#             'rwf_amount': f"{rwf_amount:,.2f}",
#             'bank_name': bank_data['bank_name'],
#             'account_no': bank_data['account_no'],
#         })

#     if not items:
#         return HttpResponse(f"No invoices found for bank: {bank_name}", status=404)

#     # Calculate totals
#     total_usd = sum([Decimal(item['usd_amount']) for item in items])
#     total_rwf = sum([Decimal(item['rwf_amount'].replace(',', '')) for item in items])

#     # Prepare context for the template
#     context = {
#         'items': items,
#         'filter_month': filter_month,
#         'exchange_rate': 1.0,  # Assuming exchange rate is already applied
#         'logo_path': request.build_absolute_uri(static('images/rwandair-logo.png')),
#         'total_usd': f"{total_usd:,.2f}",
#         'total_rwf': f"{total_rwf:,.2f}",
#         'current_date': datetime.now().strftime("%B %d, %Y"),
#     }

#     # Render and generate the PDF
#     html_string = render_to_string('aimsintegration/payslip_template.html', context)
#     pdf_file = convert_html_to_pdf(html_string)
#     if not pdf_file:
#         return HttpResponse(f"Error generating payslip for bank: {bank_name}", status=500)

#     # Return the PDF response
#     response = HttpResponse(pdf_file, content_type='application/pdf')
#     filename = f"{bank_name.replace(' ', '_')}_Crew Allowance Slip_{filter_month.strftime('%Y-%m')}.pdf"
#     response['Content-Disposition'] = f'attachment; filename="{filename}"'
#     return response

def generate_payslip_for_bank(request):
    # 1) Read ?month=YYYY-MM and ?bank_name
    month_str = request.GET.get('month')
    bank_name = request.GET.get('bank_name')

    # If bank_name is missing, return an error
    if not bank_name:
        return HttpResponse("Bank name is a required parameter.", status=400)

    # If month_str is None, empty, or literally the string 'null',
    # handle it like a missing month and use the latest in Invoice table.
    if not month_str or month_str.lower() == 'null':
        last_invoice = (
            Invoice.objects
            .filter(total_amount__gt=0)
            .aggregate(max_month=Max('month'))
        )
        filter_month = last_invoice['max_month']
        if not filter_month:
            return HttpResponse("No invoices found at all in the database.", status=404)
    else:
        # Otherwise, parse the user-provided month
        try:
            year, mo = map(int, month_str.split('-'))
            filter_month = date(year, mo, 1)
        except ValueError:
            return HttpResponse(
                f"Invalid month format: '{month_str}'. Expected YYYY-MM or use 'null'.",
                status=400
            )

    # 2) Fetch invoices for the specific bank (must be non-zero total_amount)
    invoices = (
        Invoice.objects
        .filter(month=filter_month, total_amount__gt=0)
        .select_related('crew')
        .order_by('crew__crew_id')
    )

    if not invoices.exists():
        return HttpResponse(f"No nonzero invoices found for month {filter_month}.", status=404)

    # 3) Connect to MSSQL to get the exchange rate
    with connections['mssql'].cursor() as cursor:
        cursor.execute("""
            SELECT [Relational Exch_ Rate Amount]
            FROM [RwandAir].[dbo].[RwandAir$Currency Exchange Rate$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [Currency Code] = 'USD'
              AND [Starting Date] = (
                  SELECT MAX([Starting Date])
                  FROM [RwandAir].[dbo].[RwandAir$Currency Exchange Rate$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
                  WHERE [Currency Code] = 'USD'
              );
        """)
        row = cursor.fetchone()

    if not row:
        return HttpResponse("No exchange rate found from that query.", status=400)

    exchange_rate = Decimal(str(row[0]))

    # 4) Gather Crew IDs, then fetch their bank details
    crew_ids = list({inv.crew.crew_id for inv in invoices})
    wb_formatted_ids = [f"WB{int(cid):04d}" for cid in crew_ids]
    employee_bank_data = {}

    if wb_formatted_ids:
        placeholders = ", ".join(["%s"] * len(wb_formatted_ids))
        query = f"""
            SELECT [No_], [Bank Name], [Bank Account No]
            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [No_] IN ({placeholders})
        """
        with connections['mssql'].cursor() as cursor:
            cursor.execute(query, wb_formatted_ids)
            rows = cursor.fetchall()

        for no_, b_name, account_no in rows:
            formatted_no = no_.strip()
            employee_bank_data[formatted_no] = {
                'bank_name': b_name.strip() if b_name else '',
                'account_no': account_no.strip() if account_no else '',
            }

    # 5) Filter invoices for the given bank name, compute amounts
    items = []
    for inv in invoices:
        wb_formatted = f"WB{int(inv.crew.crew_id):04d}"
        bank_data = employee_bank_data.get(wb_formatted, {'bank_name': '', 'account_no': '-'})
        if bank_data['bank_name'] != bank_name:
            continue

        usd_amount = inv.total_amount
        rwf_amount = (usd_amount * exchange_rate).quantize(Decimal('0.00'))

        items.append({
            'wb_no': inv.crew.crew_id,
            'name': f"{inv.crew.first_name} {inv.crew.last_name}",
            'position': inv.crew.position,
            'usd_amount': usd_amount,
            'rwf_amount': f"{rwf_amount:,.2f}",
            'bank_name': bank_data['bank_name'],
            'account_no': bank_data['account_no'],
        })

    if not items:
        return HttpResponse(f"No invoices found for bank: {bank_name}", status=404)

    # 6) Calculate totals
    total_usd = sum([Decimal(item['usd_amount']) for item in items])
    total_rwf = sum([Decimal(item['rwf_amount'].replace(',', '')) for item in items])

    # 7) Prepare context
    context = {
        'items': items,
        'filter_month': filter_month,
        'exchange_rate': exchange_rate,
        'logo_path': request.build_absolute_uri(static('images/rwandair-logo.png')),
        'total_usd': f"{total_usd:,.2f}",
        'total_rwf': f"{total_rwf:,.2f}",
        'current_date': datetime.now().strftime("%B %d, %Y"),
    }

    # 8) Render and generate the PDF
    html_string = render_to_string('aimsintegration/payslip_template.html', context)
    pdf_file = convert_html_to_pdf(html_string)
    if not pdf_file:
        return HttpResponse(f"Error generating payslip for bank: {bank_name}", status=500)

    # 9) Return the PDF response
    response = HttpResponse(pdf_file, content_type='application/pdf')
    filename = f"{bank_name.replace(' ', '_')}_Crew_Allowance_Slip_{filter_month.strftime('%Y-%m')}.pdf"
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response



from django.http import JsonResponse

def get_bank_names(request):
    invoices = Invoice.objects.filter(total_amount__gt=0).select_related('crew')
    crew_ids = {inv.crew.crew_id for inv in invoices}

    wb_formatted_ids = [f"WB{int(cid):04d}" for cid in crew_ids]
    employee_bank_data = {}

    if wb_formatted_ids:
        placeholders = ", ".join(["%s"] * len(wb_formatted_ids))
        query = f"""
            SELECT DISTINCT [Bank Name]
            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
            WHERE [No_] IN ({placeholders})
        """
        with connections['mssql'].cursor() as cursor:
            cursor.execute(query, wb_formatted_ids)
            rows = cursor.fetchall()
            bank_names = [row[0].strip() for row in rows if row[0]]

    return JsonResponse(bank_names, safe=False)





def layover_setup(request):
    zones = Zone.objects.all().prefetch_related('airports')
    # Convert each zone’s airports into a list or handle in template
    return render(request, 'aimsintegration/layover_setup.html', {
        'zones': zones
    })



from django.views.decorators.http import require_POST
from django.http import JsonResponse
import json
from .models import Zone, Airport

@require_POST
def create_zone(request):
    try:
        data = json.loads(request.body.decode('utf-8'))
        zone_name = data.get('zone_name')
        hourly_rate = data.get('hourly_rate')
        airport_codes = data.get('airports', [])

        if not zone_name or hourly_rate is None:
            return JsonResponse({'error': 'Missing zone_name or hourly_rate'}, status=400)

        # Create the Zone
        zone = Zone.objects.create(name=zone_name, hourly_rate=hourly_rate)

        # Create or update Airports for this zone
        for code in airport_codes:
            airport, created = Airport.objects.get_or_create(iata_code=code)
            airport.zone = zone
            airport.save()

        return JsonResponse({
            'message': 'Zone created successfully!',
            'zone': {
                'id': zone.id,
                'name': zone.name,
                'hourly_rate': str(zone.hourly_rate)
            }
        }, status=201)

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
from django.shortcuts import get_object_or_404
from .models import Zone, Airport

@csrf_exempt
def update_zone(request, zone_id):
    """
    Update a zone's details including its name, hourly rate, and associated airports.
    """
    if request.method == "POST":
        try:
            data = json.loads(request.body)

            # Fetch the zone
            zone = get_object_or_404(Zone, id=zone_id)

            # Update Zone name and hourly rate
            zone.name = data.get("zone_name", zone.name)
            zone.hourly_rate = data.get("hourly_rate", zone.hourly_rate)
            zone.save()

            # Handle airport updates
            if "airports" in data:
                updated_airports = data["airports"]  # List of {id, iata_code, action}

                for airport_data in updated_airports:
                    airport_id = airport_data.get("id")
                    iata_code = airport_data.get("iata_code").strip().upper()
                    action = airport_data.get("action")  # "update", "delete", "add"

                    if action == "update" and airport_id:
                        airport = get_object_or_404(Airport, id=airport_id, zone=zone)
                        airport.iata_code = iata_code
                        airport.save()

                    elif action == "delete" and airport_id:
                        Airport.objects.filter(id=airport_id, zone=zone).delete()

                    elif action == "add":
                        Airport.objects.create(iata_code=iata_code, zone=zone)

            return JsonResponse({"message": "Zone updated successfully!"}, status=200)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)

    return JsonResponse({"error": "Invalid request method"}, status=405)





from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from .models import Zone

def get_zone_airports(request, zone_id):
    """
    Fetch all airports belonging to a specific zone.
    """
    zone = get_object_or_404(Zone, id=zone_id)
    airports = list(zone.airports.values("id", "iata_code"))
    return JsonResponse({"airports": airports})



from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
import json
from .models import Airport

@csrf_exempt
def update_airport(request, airport_id):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            new_code = data.get("iata_code", "").strip().upper()
            airport = get_object_or_404(Airport, id=airport_id)
            airport.iata_code = new_code
            airport.save()
            return JsonResponse({"message": "Airport updated successfully!"})
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)
    return JsonResponse({"error": "Invalid request method"}, status=405)

@csrf_exempt
def delete_airport(request, airport_id):
    if request.method == "POST":
        airport = get_object_or_404(Airport, id=airport_id)
        airport.delete()
        return JsonResponse({"message": "Airport deleted successfully!"})
    return JsonResponse({"error": "Invalid request method"}, status=405)


from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from .models import Zone

@csrf_exempt
def delete_zone(request, zone_id):
    if request.method == "POST":
        zone = get_object_or_404(Zone, id=zone_id)
        zone.delete()  # This will cascade and delete associated airports
        return JsonResponse({"message": "Zone and associated airports deleted successfully!"})
    else:
        return JsonResponse({"error": "Invalid request method"}, status=405)


from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
import json
from .models import Zone, Airport

@csrf_exempt
def add_airport(request, zone_id):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
            airport_code = data.get("airport_code", "").strip().upper()
            if not airport_code:
                return JsonResponse({"error": "Airport code is required."}, status=400)
            
            zone = get_object_or_404(Zone, id=zone_id)
            # Create and associate a new Airport with the given zone
            Airport.objects.create(iata_code=airport_code, zone=zone)
            return JsonResponse({"message": "Airport added successfully!"}, status=201)
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)
    return JsonResponse({"error": "Invalid request method"}, status=405)
