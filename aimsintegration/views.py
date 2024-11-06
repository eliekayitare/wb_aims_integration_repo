

from django.shortcuts import render
from django.http import JsonResponse
from .models import FlightData
from datetime import date

def dashboard_view(request):
    today = date.today()
    query = request.GET.get('query', '')

    # Check if the request is an AJAX request
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        # Filter today's flights based on multiple fields
        schedules = FlightData.objects.filter(
            sd_date_utc=today
        ).filter(
            flight_no__icontains=query
        ) | FlightData.objects.filter(
            sd_date_utc=today
        ).filter(
            dep_code_iata__icontains=query
        ) | FlightData.objects.filter(
            sd_date_utc=today
        ).filter(
            arr_code_iata__icontains=query
        ) | FlightData.objects.filter(
            sd_date_utc=today
        ).filter(
            dep_code_icao__icontains=query
        ) | FlightData.objects.filter(
            sd_date_utc=today
        ).filter(
            arr_code_icao__icontains=query
        )

        # Serialize the flight data for AJAX response
        data = list(schedules.values('sd_date_utc', 'flight_no', 'dep_code_iata', 'dep_code_icao',
                                      'arr_code_iata', 'arr_code_icao', 'std_utc', 'atd_utc',
                                      'takeoff_utc', 'touchdown_utc', 'ata_utc', 'sta_utc'))
        return JsonResponse(data, safe=False)
    
    # Non-AJAX request loads all today's flights
    schedules = FlightData.objects.filter(sd_date_utc=today)
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})



# from django.shortcuts import render
# from django.http import JsonResponse
# from .models import FlightData
# from datetime import date, datetime

# def dashboard_view(request):
#     query = request.GET.get('query', '')
#     selected_date = request.GET.get('date', '')

#     # Use today's date if no specific date is provided
#     filter_date = date.today() if not selected_date else datetime.strptime(selected_date, "%Y-%m-%d").date()

#     # Check if the request is an AJAX request
#     if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
#         # Filter flights based on the search query and selected date
#         schedules = FlightData.objects.filter(
#             sd_date_utc=filter_date
#         ).filter(
#             flight_no__icontains=query
#         ) | FlightData.objects.filter(
#             sd_date_utc=filter_date
#         ).filter(
#             dep_code_iata__icontains=query
#         ) | FlightData.objects.filter(
#             sd_date_utc=filter_date
#         ).filter(
#             arr_code_iata__icontains=query
#         ) | FlightData.objects.filter(
#             sd_date_utc=filter_date
#         ).filter(
#             dep_code_icao__icontains=query
#         ) | FlightData.objects.filter(
#             sd_date_utc=filter_date
#         ).filter(
#             arr_code_icao__icontains=query
#         )

#         # Serialize the flight data for AJAX response
#         data = list(schedules.values('sd_date_utc', 'flight_no', 'dep_code_iata', 'dep_code_icao',
#                                       'arr_code_iata', 'arr_code_icao', 'std_utc', 'atd_utc',
#                                       'takeoff_utc', 'touchdown_utc', 'ata_utc', 'sta_utc'))
#         return JsonResponse(data, safe=False)
    
#     # Non-AJAX request loads all today's flights
#     schedules = FlightData.objects.filter(sd_date_utc=filter_date)
#     return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})













