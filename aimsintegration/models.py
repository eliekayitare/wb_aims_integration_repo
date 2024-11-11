from django.db import models
from datetime import timedelta, datetime

class AirportData(models.Model):
    iata_code = models.CharField(max_length=10, unique=True)  # The IATA code (e.g., KGL, DXB)
    icao_code = models.CharField(max_length=10, blank=True, null=True)  # The ICAO code (e.g., HRYR, OMDX)
    airport_name = models.CharField(max_length=255, blank=True, null=True)  # The name of the airport
    raw_content = models.TextField(blank=True, null=True)  # Raw content from the file for debugging purposes

    class Meta:
        db_table = 'airport_data'

    def __str__(self):
        return f"{self.iata_code} - {self.icao_code}"

from django.utils.timezone import now
class FlightData(models.Model):
    flight_no = models.CharField(max_length=6, null=False, blank=False)
    tail_no = models.CharField(max_length=10, null=True, blank=True)
    dep_code_iata = models.CharField(max_length=10, null=False, blank=False)  # IATA codes are 3 characters
    dep_code_icao = models.CharField(max_length=10, null=True, blank=True)  # ICAO codes are 4 characters
    arr_code_iata = models.CharField(max_length=10, null=False, blank=False)
    arr_code_icao = models.CharField(max_length=10, null=True, blank=True)
    sd_date_utc = models.DateField(null=False, blank=False)  # Scheduled departure date
    sa_date_utc = models.DateField(null=True, blank=True)  # Scheduled arrival date
    std_utc = models.TimeField(null=True, blank=True)  # Scheduled Time of Departure (HH:MM)
    atd_utc = models.TimeField(null=True, blank=True)  # Actual Time of Departure
    takeoff_utc = models.TimeField(null=True, blank=True)
    touchdown_utc = models.TimeField(null=True, blank=True)
    ata_utc = models.TimeField(null=True, blank=True)  # Actual Time of Arrival
    sta_utc = models.TimeField(null=True, blank=True)  # Scheduled Time of Arrival
    source_type = models.CharField(max_length=20)
    raw_content = models.TextField()
    last_modified = models.DateTimeField(default=now, auto_now=True)  # Automatically updates on save

    class Meta:
        db_table = 'flight_data'

    def __str__(self):
        return f"Flight {self.flight_no} from {self.dep_code_iata} to {self.arr_code_iata}"


 


class AcarsMessage(models.Model):
    flight_no = models.CharField(max_length=6, null=False, blank=False)
    origin_icao = models.CharField(max_length=10, null=False, blank=False)
    origin_iata = models.CharField(max_length=10, null=True, blank=True)
    destination_icao = models.CharField(max_length=10, null=False, blank=False)
    destination_iata = models.CharField(max_length=10, null=True, blank=True)
    atd_utc = models.TimeField(null=True, blank=True)
    takeoff_utc = models.TimeField(null=True, blank=True)
    touchdown_utc = models.TimeField(null=True, blank=True)
    ata_utc = models.TimeField(null=True, blank=True)
    raw_message = models.TextField()  # For storing the full message for reference

    class Meta:
        db_table = 'acars_message'

    def __str__(self):
        return f"ACARS {self.flight_no} - {self.origin_icao} to {self.destination_icao}"

