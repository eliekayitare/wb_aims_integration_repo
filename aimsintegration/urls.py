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
    # path("generate_usd_payslip/", views.generate_usd_payslip, name="generate_usd_payslip"),
    # path("generate_others_payslip/", views.generate_others_payslip, name="generate_others_payslip"),
    path('generate_currency_payslips/', views.generate_currency_payslips, name='generate_currency_payslips'),
    # Add this to your urlpatterns
    path('generate_combined_payslips_email/', views.generate_combined_payslips_email, name='generate_combined_payslips_email'),
    path('currency_payslip_download/', views.currency_payslip_download, name='currency_payslip_download'),
    # New endpoint for generating payslips by bank
     # New URLs for payslips by bank
    path('generate_all_bank_payslips_zip_email/', views.generate_all_bank_payslips_zip_email, name='generate_all_bank_payslips_zip_email'),
    # Add these to your urlpatterns
    path('generate_individual_payslip/<int:crew_id>/<int:year>/<int:month>/', views.generate_individual_payslip, name='generate_individual_payslip'),
    path('email_individual_payslip/<int:crew_id>/<int:year>/<int:month>/', views.email_individual_payslip, name='email_individual_payslip'),
    path('generate_all_individual_payslips_zip_email/', views.generate_all_individual_payslips_zip_email, name='generate_all_individual_payslips_zip_email'),
    path('layover_setup/', views.layover_setup, name='layover_setup'),
    path('zones/create/', views.create_zone, name='create_zone'),
    # path("zones/<int:zone_id>/get_airports/", views.get_zone_airports, name="get_zone_airports"),
    path("zones/<int:zone_id>/edit/", views.update_zone, name="update_zone"),
    path('zones/<int:zone_id>/delete/', views.delete_zone, name='delete_zone'),
    path('zones/<int:zone_id>/add_airport/', views.add_airport, name='add_airport'),
    path('zones/<int:zone_id>/get_airports/', views.get_zone_airports, name='get_zone_airports'),
    path('airport/<int:airport_id>/update/', views.update_airport, name='update_airport'),
    path('airport/<int:airport_id>/delete/', views.delete_airport, name='delete_airport'),

    # Quality Control Export
    path('generate_simple_csv/', views.generate_simple_csv_export, name='generate_simple_csv'),

    #HUBSPOT
    path('flight-disruption-data/', views.TableauDataListView.as_view(), name='flight-disruption-data'),

    #QATAR APIS

    path('qatar_apis_dashboard/', views.qatar_apis_dashboard, name='qatar_apis_dashboard'),
    path('qatar-apis/details/<int:record_id>/', views.qatar_apis_details, name='qatar_apis_details'),

    # # Jeppessen General Declaration
    # path('jeppessen/', views.jeppessen_dashboard, name='jeppessen_dashboard'),
    # path('jeppessen/details/<int:flight_id>/', views.jeppessen_flight_details, name='jeppessen_flight_details'),

    path('jeppessen/', views.jeppessen_dashboard, name='jeppessen_dashboard'),
    path('jeppessen/details/<int:flight_id>/', views.jeppessen_flight_details, name='jeppessen_flight_details'),
    
    # ✨ NEW: Flitelink Actions
    path('jeppessen/flitelink/submit/<int:flight_id>/', views.flitelink_submit_flight, name='flitelink_submit_flight'),
    path('jeppessen/flitelink/retry/<int:flight_id>/', views.flitelink_retry_submission, name='flitelink_retry_submission'),
    path('jeppessen/flitelink/refresh/<int:flight_id>/', views.flitelink_refresh_status, name='flitelink_refresh_status'),
    path('jeppessen/flitelink/bulk-submit/', views.flitelink_bulk_submit, name='flitelink_bulk_submit'),

]
