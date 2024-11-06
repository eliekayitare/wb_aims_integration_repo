

# from django.shortcuts import render
# from .models import FlightData
# from datetime import date
# from datetime import timedelta
# from django.http import JsonResponse

# def dashboard_view(request):
#     # Fetch only today's flight data from the database
#     today = date.today()  # Get today's date
#     #display only today's flights
#     schedules = FlightData.objects.filter(sd_date_utc=today)
#     return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})


from django.shortcuts import render
from django.http import JsonResponse
from .models import FlightData
from datetime import date

def dashboard_view(request):
    today = date.today()
    if request.is_ajax():
        query = request.GET.get('query', '')
        # Filter based on search query for today's flights
        schedules = FlightData.objects.filter(sd_date_utc=today, flight_no__icontains=query)
        # Serialize the flight data for AJAX response
        data = list(schedules.values('sd_date_utc', 'flight_no', 'dep_code_iata', 'dep_code_icao',
                                      'arr_code_iata', 'arr_code_icao', 'std_utc', 'atd_utc',
                                      'takeoff_utc', 'touchdown_utc', 'ata_utc', 'sta_utc'))
        return JsonResponse(data, safe=False)
    
    # Non-AJAX request loads all today's flights
    schedules = FlightData.objects.filter(sd_date_utc=today)
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
