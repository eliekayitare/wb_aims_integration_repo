

# from django.shortcuts import render
# from .models import FlightData
# from datetime import date
# from datetime import timedelta

# def dashboard_view(request):
#     # Fetch only today's flight data from the database
#     today = date.today()  # Get today's date
#     print("=================================")
#     print(today)
#     print("==================================")
#     #display only today's flights
#     schedules = FlightData.objects.filter(sd_date_utc=today)
#     print("==================================")
#     print(schedules.sd_date_utc)
#     print("==================================")
#     return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
    
from django.shortcuts import render
from .models import FlightData
from datetime import date, timedelta

def dashboard_view(request):
    # Fetch only today's flight data from the database
    today = date.today()  # Get today's date
    print("=================================")
    print(today)
    print("==================================")
    
    # Display only today's flights
    schedules = FlightData.objects.filter(sd_date_utc=today)
    print("==================================")
    for schedule in schedules:
        print(schedule.sd_date_utc)  # Access each record's sd_date_utc attribute
    print("==================================")
    
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
