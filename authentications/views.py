import requests
from django.contrib.auth import authenticate, login,logout 
from django.shortcuts import render, redirect
from django.db import connections, DatabaseError
from django.contrib import messages
from .models import User, Role
from django.conf import settings  # Import settings to access AD_AUTH_URL
import logging

logger = logging.getLogger(__name__)

from django.contrib import messages


def login_view(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']

        try:
            response = requests.get(
                settings.AD_AUTH_URL,
                params={"key": "Nas!2015", "user": username, "pass": password},
                verify=False,
                timeout=10
            )

            if response.status_code == 200 and response.json() != 0:
                email = f"{username}@rwandair.com"
                try:
                    with connections['mssql'].cursor() as cursor:
                        cursor.execute("""
                            SELECT [First Name], [Last Name], [Job Title], [E-Mail], [Responsibility Center], [Sub Responsibility Center], [Station]
                            FROM [RwandAir].[dbo].[RwandAir$Employee$04be3167-71f9-46b8-93ec-2d5e5e08bf9b]
                            WHERE [Company E-Mail] = %s AND [Status] = 0
                        """, [email])
                        row = cursor.fetchone()

                    if row:
                        first_name, last_name, position, personal_email, department, section, station = row
                        user, created = User.objects.get_or_create(email=email)

                        if not created:
                            user.firstname = first_name
                            user.lastname = last_name
                            user.position = position
                            user.personal_email = personal_email
                            user.department = department
                            user.section = section
                            user.station = station
                            user.save()

                        if user.role.name == 'admin':
                            login(request, user)
                            messages.success(request, "Login successful. Welcome!")
                            return redirect('dashboard')
                        else:
                            messages.error(request, 'Login failed. You must have the admin role to access the system.')
                            return render(request, 'authentications/login.html')

                    else:
                        messages.error(request, 'User not found or inactive in the ERP system.')
                        return render(request, 'authentications/login.html')

                except DatabaseError as e:
                    logger.error(f"Database error: {e}")
                    messages.error(request, 'A database error occurred.')
                    return render(request, 'authentications/login.html')

            else:
                messages.error(request, 'Invalid credentials. Please try again.')
                return render(request, 'authentications/login.html')

        except requests.Timeout:
            logger.error(f"Timeout error during authentication for {username}")
            messages.error(request, 'Request timed out. Please try again.')
            return render(request, 'authentications/login.html')

        except requests.RequestException as e:
            logger.error(f"Request error during authentication for {username}: {str(e)}")
            messages.error(request, 'A network error occurred. Please try again later.')
            return render(request, 'authentications/login.html')

    return render(request, 'authentications/login.html')




def logout_view(request):
    logout(request)
    messages.success(request, "You have been logged out successfully.")
    return redirect('login')
