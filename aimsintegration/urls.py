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

    
    # Crew Allowance project URLs
     # Upload
    path('c-all/upload/', views.upload_callowance_file, name='callowance_upload'),

    # Main list: by default show previous month's data
    path('c-all/', views.crew_allowance_list, name='crew_allowance_list'),

    # Show details for one crew's duties
    path('c-all/details/<int:crew_id>/<int:year>/<int:month>/',
         views.crew_allowance_details, name='crew_allowance_details'),
        
    path('generate_overall_payslip/', views.generate_overall_payslip, name='generate_overall_payslip'),

    path('layover_setup/', views.layover_setup, name='layover_setup'),
    path('zones/create/', views.create_zone, name='create_zone'),
    # path("zones/<int:zone_id>/get_airports/", views.get_zone_airports, name="get_zone_airports"),
    path("zones/<int:zone_id>/edit/", views.update_zone, name="update_zone"),
    path('zones/<int:zone_id>/delete/', views.delete_zone, name='delete_zone'),
    path('zones/<int:zone_id>/add_airport/', views.add_airport, name='add_airport'),
    path('zones/<int:zone_id>/get_airports/', views.get_zone_airports, name='get_zone_airports'),
    path('airport/<int:airport_id>/update/', views.update_airport, name='update_airport'),
    path('airport/<int:airport_id>/delete/', views.delete_airport, name='delete_airport'),


]
