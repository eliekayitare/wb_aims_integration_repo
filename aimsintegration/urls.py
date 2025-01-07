from django.urls import path
from . import views

urlpatterns = [
    #ACARS Project
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('flights/', views.FlightDataListView.as_view(), name='flights'),
    #CPAT Project
    path('cpat_completion_records/', views.todays_completion_records_view, name='cpat_completion_records'),
    #FDM Project
    path('fdm_dashboard/', views.fdm_dashboard_view, name='fdm_dashboard'),
    path('get_crew_details/', views.get_crew_details, name='get_crew_details'),

    # Crew allowance project
    # path('list_zones/', views.list_zones, name='list_zones'),
    # path('add_zone/', views.add_zone, name='add_zone'),
    # path('zones/<int:zone_id>/edit_zone/', views.edit_zone, name='edit_zone'),
    # path('zones/<int:zone_id>/delete_zone/', views.delete_zone, name='delete_zone'),
    # path('zones/<int:zone_id>/add_destination/', views.add_destination, name='add_destination'),
    # path('zones/<int:zone_id>/destinations/<int:destination_id>/delete_destination/', views.delete_destination, name='delete_destination'),
    # path('zones/<int:zone_id>/', views.zone_details, name='zone_details'),
    # path('zones/<int:zone_id>/destinations/<int:destination_id>/edit_destination/', views.edit_destination, name='edit_destination'),
    # path('zones/<int:zone_id>/destinations/batch_add/', views.batch_add_destinations, name='batch_add_destinations'),
    # path('zones_with_destinations/', views.list_zones_with_destinations, name='list_zones_with_destinations'),

]
