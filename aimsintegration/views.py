

from django.shortcuts import render
from .models import FlightData
from datetime import date
from datetime import timedelta
from django.http import JsonResponse

def dashboard_view(request):
    # Fetch only today's flight data from the database
    today = date.today()  # Get today's date
    #display only today's flights
    schedules = FlightData.objects.filter(sd_date_utc=today)
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})





def search_flights(request):
    query = request.GET.get('query', '')
    today = date.today()
    
    # Filter flights based on today's date and the search query for flight number or any other field
    flights = FlightData.objects.filter(
        sd_date_utc=today,
        flight_no__icontains=query
    )[:10]  # Limit results for performance
    
    # Prepare data for JSON response
    flight_data = [
        {
            'sd_date_utc': flight.sd_date_utc.strftime('%Y-%m-%d'),
            'flight_no': flight.flight_no,
            'dep_code_iata': flight.dep_code_iata,
            'dep_code_icao': flight.dep_code_icao,
            'arr_code_iata': flight.arr_code_iata,
            'arr_code_icao': flight.arr_code_icao,
            'std_utc': flight.std_utc.strftime('%H:%M'),
            'atd_utc': flight.atd_utc.strftime('%H:%M') if flight.atd_utc else '',
            'takeoff_utc': flight.takeoff_utc.strftime('%H:%M') if flight.takeoff_utc else '',
            'touchdown_utc': flight.touchdown_utc.strftime('%H:%M') if flight.touchdown_utc else '',
            'ata_utc': flight.ata_utc.strftime('%H:%M') if flight.ata_utc else '',
            'sta_utc': flight.sta_utc.strftime('%H:%M') if flight.sta_utc else '',
        }
        for flight in flights
    ]
    return JsonResponse({'flights': flight_data})
