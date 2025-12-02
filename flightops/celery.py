

# flightops/celery.py
from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from django.conf import settings

# Set the default Django settings module for the 'celery' program
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'flightops.settings')

app = Celery('flightops')

# Load task modules from all registered Django app configs
app.config_from_object('django.conf:settings', namespace='CELERY')

# Explicitly specify your Django apps containing Celery tasks
app.autodiscover_tasks(['aimsintegration'])  # Add other app names in this list if needed

# Initial tasks to run once at startup
# @app.on_after_configure.connect
# def setup_initial_tasks(sender, **kwargs):
#     # Run fetch_airport_data first; it will trigger fetch_flight_schedules automatically
#     sender.send_task('aimsintegration.tasks.fetch_airport_data')

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')




