# from django.core.management.base import BaseCommand
# from aimsintegration.tasks import fetch_emails

# class Command(BaseCommand):
#     help = 'Fetch and process emails for flight schedules, airport data, and ACARS messages.'

#     def add_arguments(self, parser):
#         # Add arguments to control the process   
#         parser.add_argument(
#             '--airport', 
#             action='store_true', 
#             help='Process only airport data from emails'
#         )
#         parser.add_argument(
#             '--schedule', 
#             action='store_true', 
#             help='Process only flight schedule data from emails'
#         )
#         parser.add_argument(
#             '--acars', 
#             action='store_true', 
#             help='Process only ACARS messages from emails'
#         )
#         parser.add_argument(
#             '--all', 
#             action='store_true', 
#             help='Process airport data, flight schedules, and ACARS messages in the proper order'
#         )
#         # Add limit argument (optional)
#         parser.add_argument(
#             '--limit',
#             type=int,
#             help='Limit the number of emails to fetch and process. If not provided, processes all emails.'
#         )

#     def handle(self, *args, **options):
#         email_limit = options['limit']

#         # Handle different cases for processing data
#         if options['airport']:
#             self.stdout.write(self.style.NOTICE('Processing only airport data...'))
#             fetch_emails(process_airport=True, process_schedule=False, process_acars=False, email_limit=email_limit)

#         elif options['schedule']:
#             self.stdout.write(self.style.NOTICE('Processing only flight schedule data...'))
#             fetch_emails(process_airport=False, process_schedule=True, process_acars=False, email_limit=email_limit)

#         elif options['acars']:
#             self.stdout.write(self.style.NOTICE('Processing only ACARS messages...'))
#             fetch_emails(process_airport=False, process_schedule=False, process_acars=True, email_limit=email_limit)

#         elif options['all']:
#             self.stdout.write(self.style.NOTICE('Processing all data in the proper order...'))
#             fetch_emails(process_airport=True, process_schedule=True, process_acars=True, email_limit=email_limit)
            
#         else:
#             self.stdout.write(self.style.ERROR('Please specify --airport, --schedule, --acars, or --all to run the command.'))



from django.core.management.base import BaseCommand
from aimsintegration.tasks import fetch_emails
from datetime import datetime

class Command(BaseCommand):
    help = 'Fetch and process emails for flight schedules, airport data, and ACARS messages.'

    def add_arguments(self, parser):
        # Add arguments to control the process   
        parser.add_argument(
            '--airport', 
            action='store_true', 
            help='Process only airport data from emails'
        )
        parser.add_argument(
            '--schedule', 
            action='store_true', 
            help='Process only flight schedule data from emails'
        )
        parser.add_argument(
            '--acars', 
            action='store_true', 
            help='Process only ACARS messages from emails'
        )
        parser.add_argument(
            '--all', 
            action='store_true', 
            help='Process airport data, flight schedules, and ACARS messages in the proper order'
        )
        # Add limit argument (optional)
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit the number of emails to fetch and process. If not provided, processes all emails.'
        )
        # Add date filtering arguments
        parser.add_argument(
            '--date',
            type=str,
            help='Fetch ACARS messages for a specific date (format: YYYY-MM-DD).'
        )
        parser.add_argument(
            '--date-range',
            nargs=2,
            type=str,
            metavar=('START_DATE', 'END_DATE'),
            help='Fetch ACARS messages within a date range (format: YYYY-MM-DD YYYY-MM-DD).'
        )

    def handle(self, *args, **options):
        email_limit = options['limit']
        date = options['date']
        date_range = options['date_range']

        # Parse and validate date arguments
        if date:
            try:
                date = datetime.strptime(date, '%Y-%m-%d').date()
            except ValueError:
                self.stdout.write(self.style.ERROR('Invalid date format. Please use YYYY-MM-DD.'))
                return

        if date_range:
            try:
                start_date = datetime.strptime(date_range[0], '%Y-%m-%d').date()
                end_date = datetime.strptime(date_range[1], '%Y-%m-%d').date()
                if start_date > end_date:
                    self.stdout.write(self.style.ERROR('Start date must be earlier than or equal to end date.'))
                    return
            except ValueError:
                self.stdout.write(self.style.ERROR('Invalid date range format. Please use YYYY-MM-DD YYYY-MM-DD.'))
                return
        else:
            start_date = end_date = None

        # Handle different cases for processing data
        if options['airport']:
            self.stdout.write(self.style.NOTICE('Processing only airport data...'))
            fetch_emails(process_airport=True, process_schedule=False, process_acars=False, email_limit=email_limit)

        elif options['schedule']:
            self.stdout.write(self.style.NOTICE('Processing only flight schedule data...'))
            fetch_emails(process_airport=False, process_schedule=True, process_acars=False, email_limit=email_limit)

        elif options['acars']:
            self.stdout.write(self.style.NOTICE('Processing only ACARS messages...'))
            fetch_emails(
                process_airport=False,
                process_schedule=False,
                process_acars=True,
                email_limit=email_limit,
                acars_date=date,
                acars_start_date=start_date,
                acars_end_date=end_date
            )

        elif options['all']:
            self.stdout.write(self.style.NOTICE('Processing all data in the proper order...'))
            fetch_emails(
                process_airport=True,
                process_schedule=True,
                process_acars=True,
                email_limit=email_limit,
                acars_date=date,
                acars_start_date=start_date,
                acars_end_date=end_date
            )
            
        else:
            self.stdout.write(self.style.ERROR('Please specify --airport, --schedule, --acars, or --all to run the command.'))
