

# from django.shortcuts import render
# from .models import FlightData
# from datetime import date
# from datetime import timedelta



# def dashboard_view(request):
#     # Fetch only today's flight data from the database
#     today = date.today()  # Get today's date

#     #display only today's flights
#     schedules = FlightData.objects.filter(sd_date_utc=today).order_by('-sd_date_utc')
#     return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
    
from django.shortcuts import render
from .models import FlightData
from datetime import date

def dashboard_view(request):
    # Fetch only today's flight data from the database
    today = date.today()  # Get today's date

    # Display only today's flights
    schedules = FlightData.objects.filter(sd_date_utc=today).order_by('-sd_date_utc')
    
    return render(request, 'aimsintegration/dashboard.html', {'schedules': schedules})
