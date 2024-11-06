

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
