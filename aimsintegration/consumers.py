import json
from channels.generic.websocket import AsyncWebsocketConsumer
from aimsintegration.models import FlightData

class FlightDataConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add(
            "flight_data_group",  # Group for broadcasting
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            "flight_data_group",
            self.channel_name
        )

    async def receive(self, text_data):
        flight_data = json.loads(text_data)
        # Broadcast the flight data to the group
        await self.channel_layer.group_send(
            "flight_data_group",
            {
                'type': 'flight_data_message',
                'flight_data': flight_data
            }
        )

    # Receive message from the group
    async def flight_data_message(self, event):
        flight_data = event['flight_data']
        await self.send(text_data=json.dumps({
            'flight_data': flight_data
        }))
