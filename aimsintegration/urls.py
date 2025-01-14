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

     # Upload
    path('c-all/upload/', views.upload_callowance_file, name='callowance_upload'),

    # Main list: by default show previous month's data
    path('c-all/', views.crew_allowance_list, name='crew_allowance_list'),

    # Generate overall invoice for the month
    path('c-all/generate-invoice/', views.generate_overall_invoice, name='generate_overall_invoice'),

    # Show details for one crew's duties
    path('c-all/details/<int:crew_id>/<int:year>/<int:month>/',
         views.crew_allowance_details, name='crew_allowance_details'),

]
