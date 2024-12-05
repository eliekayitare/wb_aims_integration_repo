from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('flights/', views.FlightDataListView.as_view(), name='flights'),
    path('cpat_completion_records/', views.todays_completion_records_view, name='cpat_completion_records'),
    path('fdm_dashboard/', views.fdm_dashboard_view, name='fdm_dashboard'),
    path('get_crew_details/', views.get_crew_details, name='get_crew_details'),
]
