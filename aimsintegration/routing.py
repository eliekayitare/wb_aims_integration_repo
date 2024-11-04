from django.urls import path
from aimsintegration.consumers import FlightDataConsumer

websocket_urlpatterns = [
    path('ws/flight-data/', FlightDataConsumer.as_asgi()),
]
