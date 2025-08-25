from rest_framework import serializers
from .models import FlightData, CargoFlightData

class FlightDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = CargoFlightData
        fields = [
            'flight_no', 
            'tail_no', 
            'dep_code_iata', 
            'dep_code_icao', 
            'arr_code_iata', 
            'arr_code_icao', 
            'sd_date_utc', 
            'sa_date_utc', 
            'std_utc', 
            'sta_utc'  # Include sta_utc field
        ]




# serializers.py
from rest_framework import serializers
from .models import TableauData

class TableauDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = TableauData
        fields = '__all__'