from rest_framework import serializers
from .models import FlightData

class FlightDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = FlightData
        fields = '__all__'  # Include all fields from the FlightData model
