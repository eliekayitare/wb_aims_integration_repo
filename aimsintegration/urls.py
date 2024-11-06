from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard_view, name='dashboard'),
    path('search-flights/', views.search_flights, name='search_flights'),  # Add this line
]
