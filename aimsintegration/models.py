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
    # actual_block_time_mvt = models.TimeField(null=True, blank=True) 
    # flight_time_mvt = models.TimeField(null=True, blank=True)
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
    nationality_code = models.CharField(max_length=3, null=True, blank=True)  # Will come from Job 97
    
    class Meta:
        db_table = 'qatar_crew_detail'

    def __str__(self):
        return f"{self.crew_id} {self.surname}, {self.firstname}"


class QatarFlightDetails(models.Model):
    STATUS_CHOICES = [
        ('GENERATED', 'Generated'),
        ('SENT', 'Sent'),
        ('PROCESSED', 'Processed'),
        ('RECEIVED', 'Received'),
    ]
    
    DIRECTION_CHOICES = [
        ('I', 'Inbound'),   # DOH -> KGL
        ('O', 'Outbound'),  # KGL -> DOH
    ]
    
    crew_id = models.CharField(max_length=11)
    flight = models.ForeignKey(FlightData, on_delete=models.CASCADE, related_name='assignments')
    tail_no = models.CharField(max_length=10, null=True, blank=True)
    dep_date_utc = models.DateField()
    arr_date_utc = models.DateField(null=True, blank=True)
    std_utc = models.TimeField(null=True, blank=True)
    sta_utc = models.TimeField(null=True, blank=True)
    
    # APIS status tracking
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='GENERATED', db_index=True)
    status_updated_at = models.DateTimeField(null=True, blank=True)
    apis_filename = models.CharField(max_length=255, null=True, blank=True)
    direction = models.CharField(max_length=1, choices=DIRECTION_CHOICES, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'qatar_flight_details'
        unique_together = ('crew_id', 'flight')
        indexes = [
            models.Index(fields=['status', 'dep_date_utc']),
            models.Index(fields=['direction', 'dep_date_utc']),
        ]

    def __str__(self):
        return f"{self.crew_id} on {self.flight}"
    





# ============================================================================
# JEPPESSEN PROJECT GENERAL DECLARATION (GD) - Job 97 Integration  - Models
# ============================================================================

class JEPPESSENGDCrewDetail(models.Model):
    """
    Crew personal details from General Declaration RTF.
    Independent storage - all data parsed from GD RTF files.
    """
    crew_id = models.CharField(max_length=11, primary_key=True)
    
    # Name
    surname = models.CharField(max_length=50, null=True, blank=True)
    firstname = models.CharField(max_length=50, null=True, blank=True)
    full_name = models.CharField(max_length=100, null=True, blank=True)
    
    # Passport details
    passport_number = models.CharField(max_length=20, null=True, blank=True)
    passport_expiry = models.DateField(null=True, blank=True)
    
    # Personal details
    birth_date = models.DateField(null=True, blank=True)
    sex = models.CharField(max_length=1, choices=[('M','M'),('F','F')], null=True, blank=True)
    nationality_code = models.CharField(max_length=3, null=True, blank=True)
    
    # Email from ERP
    email = models.CharField(max_length=100, null=True, blank=True, db_index=True)
    
    # Metadata
    last_seen_flight_date = models.DateField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'jeppessen_gd_crew_detail'
    
    def __str__(self):
        return f"{self.crew_id} - {self.full_name or self.surname or 'Unknown'}"


# class JEPPESSENGDFlight(models.Model):
#     """
#     General Declaration flight records from Job 97 RTF files.
#     Format: 212/BGF DLA/11112025 (Flight/Origin Dest/Date)
#     Links to actual FlightData when possible.
#     STD and STA come from FlightData when linked.
#     """
    
#     # Link to actual flight (may be null if not found in FlightData)
#     flight = models.ForeignKey(
#         FlightData,
#         on_delete=models.SET_NULL,
#         related_name='general_declarations',
#         null=True,
#         blank=True
#     )
    
#     # Flight information from GD
#     flight_no = models.CharField(max_length=6, db_index=True)
#     flight_date = models.DateField(db_index=True)
#     origin_iata = models.CharField(max_length=3)
#     origin_icao = models.CharField(max_length=4, null=True, blank=True)
#     destination_iata = models.CharField(max_length=3)
#     destination_icao = models.CharField(max_length=4, null=True, blank=True)
#     tail_no = models.CharField(max_length=10, null=True, blank=True)
    
#     # Schedule from FlightData (when linked)
#     std_utc = models.TimeField(null=True, blank=True)  # From FlightData
#     sta_utc = models.TimeField(null=True, blank=True)  # From FlightData
    
#     # Metadata
#     raw_filename = models.CharField(max_length=255, null=True, blank=True)
#     processed_at = models.DateTimeField(auto_now_add=True)
#     updated_at = models.DateTimeField(auto_now=True)
    
#     class Meta:
#         db_table = 'jeppessen_gd_flight'
#         unique_together = ('flight_no', 'flight_date', 'origin_iata', 'destination_iata')
#         indexes = [
#             models.Index(fields=['flight_no', 'flight_date']),
#             models.Index(fields=['flight_date']),
#         ]
#         ordering = ['-flight_date', 'flight_no']
    
#     def __str__(self):
#         return f"GD Flight {self.flight_no} on {self.flight_date} ({self.origin_iata}->{self.destination_iata})"


# ============================================================================
# JEPPESSEN PROJECT GENERAL DECLARATION (GD) - Job 97 Integration  - Models
# ============================================================================

class JEPPESSENGDFlight(models.Model):
    """
    General Declaration flight records from Job 97 RTF files.
    Format: 212/BGF DLA/11112025 (Flight/Origin Dest/Date)
    Links to actual FlightData when possible.
    STD and STA come from FlightData when linked.
    
    ✨ NEW: Now includes Flitelink API submission tracking
    """
    
    # Link to actual flight (may be null if not found in FlightData)
    flight = models.ForeignKey(
        FlightData,
        on_delete=models.SET_NULL,
        related_name='general_declarations',
        null=True,
        blank=True
    )
    
    # Flight information from GD
    flight_no = models.CharField(max_length=6, db_index=True)
    flight_date = models.DateField(db_index=True)
    origin_iata = models.CharField(max_length=3)
    origin_icao = models.CharField(max_length=4, null=True, blank=True)
    destination_iata = models.CharField(max_length=3)
    destination_icao = models.CharField(max_length=4, null=True, blank=True)
    tail_no = models.CharField(max_length=10, null=True, blank=True)
    
    # Schedule from FlightData (when linked)
    std_utc = models.TimeField(null=True, blank=True)  # From FlightData
    sta_utc = models.TimeField(null=True, blank=True)  # From FlightData
    
    # Metadata
    raw_filename = models.CharField(max_length=255, null=True, blank=True)
    processed_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # ✨ NEW: Flitelink API Integration Fields
    flitelink_request_id = models.UUIDField(null=True, blank=True, db_index=True, unique=True)
    flitelink_status = models.CharField(
        max_length=20,
        choices=[
            ('NOT_SUBMITTED', 'Not Submitted'),
            ('PENDING', 'Pending'),
            ('QUEUED', 'Queued'),
            ('COMPLETED', 'Completed'),
            ('FAILED', 'Failed'),
        ],
        default='NOT_SUBMITTED',
        db_index=True
    )
    flitelink_submitted_at = models.DateTimeField(null=True, blank=True)
    flitelink_completed_at = models.DateTimeField(null=True, blank=True)
    flitelink_last_check = models.DateTimeField(null=True, blank=True)
    flitelink_error_message = models.TextField(null=True, blank=True)
    flitelink_retry_count = models.IntegerField(default=0)
    flitelink_response = models.JSONField(null=True, blank=True)
    
    class Meta:
        db_table = 'jeppessen_gd_flight'
        unique_together = ('flight_no', 'flight_date', 'origin_iata', 'destination_iata')
        indexes = [
            models.Index(fields=['flight_no', 'flight_date']),
            models.Index(fields=['flight_date']),
            models.Index(fields=['flitelink_status']),
            models.Index(fields=['flitelink_request_id']),
        ]
        ordering = ['-flight_date', 'flight_no']
    
    def __str__(self):
        return f"GD Flight {self.flight_no} on {self.flight_date} ({self.origin_iata}->{self.destination_iata})"
    
    # ✨ NEW: Helper methods
    @property
    def flitelink_status_display(self):
        """Human-readable Flitelink status"""
        return self.get_flitelink_status_display()
    
    @property
    def can_submit_to_flitelink(self):
        """Check if flight can be submitted to Flitelink"""
        return (
            self.origin_icao and 
            self.destination_icao and 
            self.std_utc and 
            self.sta_utc and
            self.flitelink_status in ['NOT_SUBMITTED', 'FAILED']
        )
    
    @property
    def crew_count(self):
        """Get total crew count"""
        return self.crew_assignments.count()
    
    @property
    def pic_crew(self):
        """Get PIC crew member"""
        return self.crew_assignments.filter(is_pic=True).first()
    
    @property
    def sic_crew(self):
        """Get SIC crew member"""
        return self.crew_assignments.filter(is_sic=True).first()


#  model for API logging
class FlitelinkAPILog(models.Model):
    """
    Detailed API call logs for Flitelink integration.
    Tracks every API request/response for debugging and monitoring.
    """
    
    REQUEST_TYPE_CHOICES = [
        ('SUBMIT', 'Submit Flight'),
        ('STATUS', 'Check Status'),
    ]
    
    # Link to GD flight
    gd_flight = models.ForeignKey(
        JEPPESSENGDFlight,
        on_delete=models.CASCADE,
        related_name='flitelink_api_logs',
        null=True,
        blank=True
    )
    
    # API call details
    request_type = models.CharField(max_length=20, choices=REQUEST_TYPE_CHOICES)
    request_id = models.UUIDField(db_index=True)
    endpoint = models.CharField(max_length=255)
    http_method = models.CharField(max_length=10)
    
    # Request/Response
    request_payload = models.JSONField(null=True, blank=True)
    response_status_code = models.IntegerField(null=True, blank=True)
    response_data = models.JSONField(null=True, blank=True)
    
    # Timing
    request_time = models.DateTimeField(auto_now_add=True)
    response_time = models.DateTimeField(null=True, blank=True)
    duration_ms = models.IntegerField(null=True, blank=True)
    
    # Error tracking
    success = models.BooleanField(default=True)
    error_message = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'flitelink_api_log'
        ordering = ['-request_time']
        indexes = [
            models.Index(fields=['request_id', '-request_time']),
            models.Index(fields=['gd_flight', '-request_time']),
        ]
    
    def __str__(self):
        return f"{self.request_type} - {self.request_id} - {self.response_status_code or 'No response'}"


class JEPPESSENGDCrew(models.Model):
    """
    Crew assignments from General Declaration (Job 97).
    Tracks PIC and SIC positions.
    Email comes from ERP (MSSQL).
    """
    
    POSITION_CHOICES = [
        ('CP', 'Captain'),
        ('FO', 'First Officer'),
        ('FP', 'Purser'),
        ('SA', 'Senior Attendant'),
        ('FA', 'Flight Attendant'),
        ('FE', 'Flight Engineer'),
        ('MX', 'Maintenance'),
        ('AC', 'Air Crew'),
        ('PIC', 'Pilot in Command'),
        ('SIC', 'Second in Command'),
    ]
    
    # Crew identification
    crew_id = models.CharField(max_length=11, db_index=True)
    
    # Link to GD flight
    gd_flight = models.ForeignKey(
        JEPPESSENGDFlight,
        on_delete=models.CASCADE,
        related_name='crew_assignments'
    )
    
    # Link to actual flight (optional)
    flight = models.ForeignKey(
        FlightData,
        on_delete=models.CASCADE,
        related_name='gd_crew_assignments',
        null=True,
        blank=True
    )
    
    # Position and role
    position = models.CharField(max_length=3, choices=POSITION_CHOICES)
    role = models.CharField(max_length=3, null=True, blank=True)
    is_pic = models.BooleanField(default=False, db_index=True)
    is_sic = models.BooleanField(default=False, db_index=True)
    
    # Email from ERP (MSSQL)
    email = models.CharField(max_length=100, null=True, blank=True, db_index=True)
    
    # Flight details
    flight_no = models.CharField(max_length=6, db_index=True)
    flight_date = models.DateField(db_index=True)
    origin = models.CharField(max_length=4)
    destination = models.CharField(max_length=4)
    tail_no = models.CharField(max_length=10, null=True, blank=True)
    
    # Timestamps
    dep_date_utc = models.DateField(null=True, blank=True)
    arr_date_utc = models.DateField(null=True, blank=True)
    std_utc = models.TimeField(null=True, blank=True)
    sta_utc = models.TimeField(null=True, blank=True)
    
    # Metadata
    processed_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'jeppessen_gd_crew'
        unique_together = ('crew_id', 'gd_flight')
        indexes = [
            models.Index(fields=['crew_id', 'flight_date']),
            models.Index(fields=['flight_date', 'is_pic']),
            models.Index(fields=['flight_date', 'is_sic']),
            models.Index(fields=['position']),
        ]
        ordering = ['-flight_date', 'flight_no', '-is_pic', '-is_sic', 'position']
    
    def __str__(self):
        pic_marker = " [PIC]" if self.is_pic else " [SIC]" if self.is_sic else ""
        return f"{self.position} {self.crew_id}{pic_marker} on Flight {self.flight_no}"


class JEPPESSENGDProcessingLog(models.Model):
    """
    Log of General Declaration processing from Job 97.
    """
    
    STATUS_CHOICES = [
        ('SUCCESS', 'Success'),
        ('PARTIAL', 'Partial Success'),
        ('FAILED', 'Failed'),
    ]
    
    # Email/File details
    email_subject = models.CharField(max_length=255)
    attachment_name = models.CharField(max_length=255, null=True, blank=True)
    gd_identifier = models.CharField(max_length=255, null=True, blank=True)
    
    # Processing stats
    total_crew = models.IntegerField(default=0)
    emails_found = models.IntegerField(default=0)
    emails_not_found = models.IntegerField(default=0)
    pic_identified = models.BooleanField(default=False)
    sic_identified = models.BooleanField(default=False)
    flight_found = models.BooleanField(default=False)
    
    # Status
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='SUCCESS')
    error_message = models.TextField(null=True, blank=True)
    
    # Timestamps
    processed_at = models.DateTimeField(auto_now_add=True)
    processing_duration = models.FloatField(null=True, blank=True)
    
    class Meta:
        db_table = 'jeppessen_gd_processing_log'
        ordering = ['-processed_at']
    
    def __str__(self):
        return f"GD Processing {self.status} at {self.processed_at} ({self.total_crew} crew)"

# =============================================================================================

# Crew Documents Backup/Archiving System

# =============================================================================================
class Backup(models.Model):
    name = models.CharField(max_length=255)
    backup_type = models.CharField(max_length=50, default="weekly")
    backup_date = models.DateField(auto_now_add=True)
    start_time = models.DateTimeField()
    end_time = models.DateTimeField(null=True, blank=True)
    duration_minutes = models.FloatField(null=True, blank=True)
    status = models.CharField(max_length=50, default="running")   # running, success, failed
    message = models.TextField(blank=True)
    backup_size = models.FloatField(null=True, blank=True)
    
    class Meta:
        db_table = 'backup'
        ordering = ['-backup_date']

    def __str__(self):
        return f"{self.name} - {self.backup_date}"

class CrewDocumentsArchive(models.Model):
    #  wb _ number is a number
    wb_number = models.IntegerField(unique=True)
    crew_name = models.CharField(max_length=255)
    date_of_leaving = models.DateField()
    position = models.CharField(max_length=255)
    archive_date = models.DateField()
    archived = models.BooleanField(default=False)
    archive_path = models.CharField(null=True, blank=True, max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'crew_documents_archive'
        ordering = ['-date_of_leaving']

    def __str__(self):
        return f"{self.name} - {self.date_of_leaving}"
