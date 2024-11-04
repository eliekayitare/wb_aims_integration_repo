

from django.shortcuts import render
from .models import FlightData
from datetime import date
from datetime import timedelta

# def dashboard_view(request):
#     # Fetch only today's flight data from the database
#     today = date.today()  # Get today's date
#     yesterday = today - timedelta(days=1)  # Get yesterday's date

# #    display only today's flightsl and yesterday's flights

#     schedules = FlightData.objects.filter(date_utc__in=[today, yesterday]).order_by('-date_utc')
#     return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})


def dashboard_view(request):
    # Fetch only today's flight data from the database
    today = date.today()  # Get today's date

    #display only today's flights
    schedules = FlightData.objects.filter(date_utc=today).order_by('-date_utc')
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
    

# def dashboard_view(request):
#     # Fetch only today's flight data from the database
#     today = date.today()  # Get today's date
#     yesterday = today - timedelta(days=1)  # Get yesterday's date
#     #display only today's flights
#     schedules = FlightData.objects.filter(date_utc=yesterday)
#     return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
