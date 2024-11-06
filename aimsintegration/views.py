

from django.utils import timezone
from django.shortcuts import render
from .models import FlightData

def dashboard_view(request):
    # Fetch only today's flight data in UTC
    today = timezone.now().date()  # Get today's date in UTC
    print("====================")
    print(f"Today's date: {today}")
    print("====================")
    # Display only today's flights
    schedules = FlightData.objects.filter(sd_date_utc=today).order_by('-sd_date_utc')
    print("====================")
    print(f"sd_date_utc: {schedules.sd_date_utc}")
    print("====================")
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
