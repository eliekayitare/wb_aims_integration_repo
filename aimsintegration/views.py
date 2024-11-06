

from django.shortcuts import render
from .models import FlightData
from datetime import date
from datetime import timedelta

def dashboard_view(request):
    # Fetch only today's flight data from the database
    today = date.today()  # Get today's date
    print("=================================")
    print(today)
    print("==================================")
    #display only today's flights
    schedules = FlightData.objects.filter(sd_date_utc=today).order_by('-sd_date_utc')
    print("==================================")
    print(schedules.sd_date_utc)
    print("==================================")
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
    
