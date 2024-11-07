
# from django.shortcuts import render
# from django.http import JsonResponse
# from .models import FlightData
# from datetime import date, datetime

# def dashboard_view(request):
#     query = request.GET.get('query', '')
#     selected_date = request.GET.get('date', '')

#     # Set filter date based on selected date or default to today
#     filter_date = date.today() if not selected_date else datetime.strptime(selected_date, "%Y-%m-%d").date()

#     # Check if it's an AJAX request
#     if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
#         # Filter based on query and selected date
#         schedules = FlightData.objects.filter(sd_date_utc=filter_date).filter(
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
#         ).order_by('-std_utc')

#         # Serialize data for JSON response
#         data = list(schedules.values('sd_date_utc', 'flight_no', 'dep_code_iata', 'dep_code_icao',
#                                       'arr_code_iata', 'arr_code_icao', 'std_utc', 'atd_utc',
#                                       'takeoff_utc', 'touchdown_utc', 'ata_utc', 'sta_utc'))
#         return JsonResponse(data, safe=False)
    
#     # Non-AJAX request loads all today's flights
#     schedules = FlightData.objects.filter(sd_date_utc=filter_date)
#     return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})


from django.shortcuts import render
from django.http import JsonResponse
from .models import FlightData
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
            )
        ).order_by('std_utc')

        # Serialize data for JSON response
        data = list(schedules.values('sd_date_utc', 'flight_no', 'dep_code_iata', 'dep_code_icao',
                                      'arr_code_iata', 'arr_code_icao', 'std_utc', 'atd_utc',
                                      'takeoff_utc', 'touchdown_utc', 'ata_utc', 'sta_utc'))
        return JsonResponse(data, safe=False)
    
    # Non-AJAX request loads all today's flights, ordered by std_utc
    schedules = FlightData.objects.filter(sd_date_utc=filter_date).order_by('std_utc')
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
