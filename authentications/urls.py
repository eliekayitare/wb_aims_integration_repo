from django.urls import path
from .views import login_view, logout_view, systems_list

urlpatterns = [
    path('', login_view, name='login'),
    path('logout/', logout_view, name='logout'),
    path('wb_systems/', systems_list, name='systems'),
]
