#ACARS Project models

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
    


#CARGO WEBSITE MODELS

class CargoFlightData(models.Model):
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

    class Meta:
        db_table = 'cargo_flight_data'

    def __str__(self):
        return f"Flight {self.flight_no} from {self.dep_code_iata} to {self.arr_code_iata}"




# CPAT Project models 

from django.db import models

class CompletionRecord(models.Model):
    employee_id = models.CharField(max_length=100, null=True, blank=True)  # ID of the user
    employee_email = models.EmailField(max_length=255, null=True, blank=True)  # Email of the user
    course_code = models.CharField(max_length=50, null=True, blank=True)  # Code of the course
    completion_date = models.DateField(null=True, blank=True)  # Date of course completion
    score = models.FloatField(null=True, blank=True)  # Score achieved in the course
    time_in_seconds = models.PositiveIntegerField(null=True, blank=True)  # Time spent on the course
    start_date = models.DateField(null=True, blank=True)  # Start date of the course
    end_date = models.DateField(null=True, blank=True)  # End date of the course
    created_at = models.DateTimeField(auto_now_add=True)  # Timestamp for when the record is added
    updated_at = models.DateTimeField(auto_now=True)  # Timestamp for when the record is updated

    def __str__(self):
        return f"{self.employee_id} - {self.course_code}"

    class Meta:
        db_table = 'completion_record'
        unique_together = ('employee_id', 'course_code', 'completion_date')
        indexes = [
            models.Index(fields=['employee_id', 'course_code', 'completion_date']),
        ]



#FDM Project models
class FdmFlightData(models.Model):
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
    flight_type= models.CharField(max_length=20)
    etd_utc = models.TimeField(null=True, blank=True)  # Estimated Time of Departure
    eta_utc = models.TimeField(null=True, blank=True)  # Estimated Time of Arrival
    source_type = models.CharField(max_length=20)
    raw_content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)  # Timestamp for when the record is added
    updated_at = models.DateTimeField(auto_now=True)  # Timestamp for when the record is updated

    class Meta:
        db_table = 'fdm_flight_data'

    def __str__(self):
        return f"Flight {self.flight_no} from {self.dep_code_iata} to {self.arr_code_iata}"
    



class CrewMember(models.Model):
    ROLE_CHOICES = [
        ('CP', 'Captain'),
        ('FO', 'First Officer'),
        ('FP', 'Purser'),
        ('SA', 'Senior Attendant'),
        ('FA', 'Flight Attendant'),
        ('FE', 'Flight Engineer'),
        ('AC', 'Air Crew'),
    ]

    flight_no = models.CharField(max_length=6)
    sd_date_utc = models.DateField()
    origin = models.CharField(max_length=10)
    destination = models.CharField(max_length=10)
    crew_id = models.CharField(max_length=10)  # <-- remove `unique=True` here
    name = models.CharField(max_length=100)
    role = models.CharField(max_length=2, choices=ROLE_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'crew_member'
        # Keep the combined uniqueness:
        unique_together = ('flight_no', 'sd_date_utc', 'origin', 'destination', 'crew_id')
        indexes = [
            models.Index(fields=['flight_no', 'crew_id']),
        ]



# Tableau Project models

class TableauData(models.Model):
    operation_day = models.DateField(null=False, blank=False)
    departure_station = models.CharField(max_length=100, null=False, blank=False)
    flight_no = models.CharField(max_length=100, null=False, blank=False)
    flight_leg_code = models.CharField(max_length=100, null=False, blank=False)
    cancelled_deleted = models.BooleanField(default=0)
    arrival_station = models.CharField(max_length=100, null=False, blank=False)
    aircraft_reg_id = models.CharField(max_length=100, null=True, blank=True)
    aircraft_type_index = models.CharField(max_length=100, null=True, blank=True)
    aircraft_category = models.CharField(max_length=100, null=True, blank=True)
    flight_service_type = models.CharField(max_length=100, null=True, blank=True)
    std = models.TimeField(null=True, blank=True)
    sta = models.TimeField(null=True, blank=True)
    original_operation_day = models.DateField(null=True, blank=True)
    original_std = models.TimeField(null=True, blank=True)
    original_sta = models.TimeField(null=True, blank=True)
    departure_delay_time = models.IntegerField(null=True, blank=True)
    delay_code_kind = models.CharField(max_length=100, null=True, blank=True)
    # Removed delay_number field
    # Removed seat_type_config field
    aircraft_config = models.CharField(max_length=100, null=True, blank=True)
    atd = models.TimeField(null=True, blank=True)
    takeoff = models.TimeField(null=True, blank=True)
    touchdown = models.TimeField(null=True, blank=True)
    ata = models.TimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'tableau_data'
        indexes = [
            models.Index(fields=['updated_at']),
        ]


# Crew Allowance Project models
from django.db import models
from django.utils import timezone
from decimal import Decimal


class Crew(models.Model):
    """
    Basic info about a crew member.
    """
    crew_id = models.CharField(max_length=50, unique=True)
    first_name = models.CharField(max_length=100)
    last_name = models.CharField(max_length=100)
    position = models.CharField(max_length=50, blank=True, null=True)  # CP, FO, CC, etc.

    class Meta:
        db_table = 'crew_info'

    def __str__(self):
        return f"{self.crew_id} - {self.first_name} {self.last_name}"


class Zone(models.Model):
    """
    Each Zone can contain one or many Airports.
    Each Zone has a unique name and an hourly rate.
    """
    name = models.CharField(max_length=100, unique=True)
    hourly_rate = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal('0.00'))

    class Meta:
        db_table = 'zone_info'

    def __str__(self):
        return f"{self.name} (Rate: {self.hourly_rate})"


class Airport(models.Model):
    """
    Each Airport belongs to one Zone (so a Zone can have multiple Airports).
    """
    iata_code = models.CharField(max_length=10, unique=True)
    zone = models.ForeignKey(
        Zone,
        on_delete=models.CASCADE,
        related_name='airports',
        null=True,
        blank=True
    )

    class Meta:
        db_table = 'airport_destinations'

    def __str__(self):
        return self.iata_code


class Duty(models.Model):
    """
    Represents a single row from the uploaded file (or any flight info).
    References Crew, departure/arrival Airports.
    """
    duty_date = models.DateField()
    crew = models.ForeignKey(Crew, on_delete=models.CASCADE)
    flight_number = models.CharField(max_length=20, blank=True)
    departure_airport = models.ForeignKey(
        Airport,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='duty_departures'
    )
    arrival_airport = models.ForeignKey(
        Airport,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='duty_arrivals'
    )
    layover_time_minutes = models.PositiveIntegerField(default=0)
    tail_number = models.CharField(max_length=20, blank=True)

    class Meta:
        db_table = 'duty_info'

    def __str__(self):
        return f"Duty {self.id}: {self.crew} on {self.duty_date}"


class Invoice(models.Model):
    """
    One invoice per Crew per Month.
    """
    crew = models.ForeignKey(Crew, on_delete=models.CASCADE)
    # We'll store the first day of the month in 'month' to represent that invoice period.
    month = models.DateField()
    total_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'crew_invoice'
        unique_together = ('crew', 'month')

    def __str__(self):
        month_str = self.month.strftime('%B %Y')
        return f"Invoice for {self.crew.crew_id} - {month_str}"

    def recalculate_total(self):
        total = sum(item.allowance_amount for item in self.invoiceitem_set.all())
        self.total_amount = total
        self.save()


class InvoiceItem(models.Model):
    """
    Each InvoiceItem corresponds to a single Duty for that month.
    """
    invoice = models.ForeignKey(Invoice, on_delete=models.CASCADE)
    duty = models.ForeignKey(Duty, on_delete=models.CASCADE)
    allowance_amount = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal('0.00'))

    class Meta:
        db_table = 'invoice_item'
        unique_together = ('invoice', 'duty')

    def __str__(self):
        return f"InvoiceItem {self.id} (Invoice {self.invoice.id})"





# =======================================================================

# DREAMMILES PROJECT MODELS

# =======================================================================

# Add to your existing models.py
import uuid
class DreammilesCampaign(models.Model):
    """Tracks email campaigns to Dreammiles members"""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    subject = models.CharField(max_length=255)
    email_body = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    status = models.CharField(
        max_length=20,
        choices=[
            ('importing', 'Importing Data'),
            ('processing', 'Sending Emails'),
            ('completed', 'Completed'),
            ('failed', 'Failed')
        ],
        default='importing',
        db_index=True
    )
    csv_processed = models.BooleanField(default=False)
    total_recipients = models.IntegerField(default=0)
    emails_sent = models.IntegerField(default=0)
    emails_failed = models.IntegerField(default=0)
    current_batch = models.IntegerField(default=0)
    last_sent_at = models.DateTimeField(null=True, blank=True)
    
    def __str__(self):
        return f"{self.name} ({self.created_at.strftime('%Y-%m-%d')})"
    
    @property
    def progress_percentage(self):
        """Calculate percentage of emails processed"""
        if self.total_recipients == 0:
            return 0
        return round(((self.emails_sent + self.emails_failed) / self.total_recipients) * 100, 2)

class DreamilesEmailRecord(models.Model):
    """Tracks individual email sends"""
    id = models.BigAutoField(primary_key=True)
    campaign = models.ForeignKey(DreammilesCampaign, on_delete=models.CASCADE, related_name='email_records')
    member_id = models.CharField(max_length=50, db_index=True)
    email = models.EmailField(db_index=True)
    tier = models.CharField(max_length=50, blank=True, null=True)
    first_name = models.CharField(max_length=255, blank=True, null=True)
    batch_number = models.IntegerField(default=0, db_index=True)
    sent_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(
        max_length=20,
        choices=[
            ('pending', 'Pending'),
            ('processing', 'Processing'), 
            ('sent', 'Sent'),
            ('failed', 'Failed')
        ],
        default='pending',
        db_index=True
    )
    error_message = models.TextField(blank=True, null=True)
    retry_count = models.SmallIntegerField(default=0)
    
    class Meta:
        unique_together = ['campaign', 'email']
        indexes = [
            models.Index(fields=['campaign', 'status', 'batch_number']),
        ]





#===========================================================================

# QATAR APIS models

#===========================================================================
class QatarCrewDetail(models.Model):
    crew_id = models.CharField(max_length=11, primary_key=True)
    passport_number = models.CharField(max_length=20, null=True, blank=True)
    surname = models.CharField(max_length=50, null=True, blank=True)
    firstname = models.CharField(max_length=50, null=True, blank=True)
    middlename = models.CharField(max_length=50, null=True, blank=True)
    nationality = models.CharField(max_length=20, null=True, blank=True)  # Full text like "RWANDAN"
    passport_issue_date = models.DateField(null=True, blank=True)  # From Job 1008
    place_of_issue = models.CharField(max_length=16, null=True, blank=True)  # From Job 1008
    birth_place_cc = models.CharField(max_length=3, null=True, blank=True)  # From Job 1008
    
    # Fields that will come from Job 97:
    birth_date = models.DateField(null=True, blank=True)  # Will come from Job 97
    sex = models.CharField(max_length=1, choices=[('M','M'),('F','F')], null=True, blank=True)  # Will come from Job 97
    passport_expiry = models.DateField(null=True, blank=True)  # Will come from Job 97

    class Meta:
        db_table = 'qatar_crew_detail'

    def __str__(self):
        return f"{self.crew_id} {self.surname}, {self.firstname}"


class QatarFlightCrewAssignment(models.Model):
    crew_id = models.CharField(max_length=11)
    flight = models.ForeignKey(FlightData, on_delete=models.CASCADE, related_name='assignments')
    tail_no = models.CharField(max_length=10, null=True, blank=True)
    dep_date_utc = models.DateField()
    arr_date_utc = models.DateField(null=True, blank=True)
    std_utc = models.TimeField(null=True, blank=True)
    sta_utc = models.TimeField(null=True, blank=True)
    birth_date = models.DateField(null=True, blank=True)  # From Job 97
    sex = models.CharField(max_length=1, choices=[('M','M'),('F','F')], null=True, blank=True)  # From Job 97
    passport_expiry = models.DateField(null=True, blank=True)  # From Job 97

    class Meta:
        db_table = 'qatar_flight_crew_assignment'
        unique_together = ('crew_id', 'flight')

    def __str__(self):
        return f"{self.crew_id} on {self.flight}"