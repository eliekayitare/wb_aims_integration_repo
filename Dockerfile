# # Pull base image
# FROM python:3.10

# # Set environment variables
# ENV PYTHONDONTWRITEBYTECODE 1
# ENV PYTHONUNBUFFERED 1

# # Install dependencies
# RUN apt-get update && apt-get install -y \
#     gcc \
#     g++ \
#     unixodbc-dev \
#     curl \
#     ca-certificates

# # Install Microsoft ODBC 17
# RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
# RUN apt-get update
# RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# # Set work directory
# WORKDIR /app

# # Update pip and install python dependencies
# RUN python -m pip install --upgrade pip
# COPY requirements.txt /app/
# RUN pip install -r requirements.txt

# # Update the CA certificates
# RUN update-ca-certificates

# # Copy project files
# COPY . /app/

# # Expose the port for the Django app
# EXPOSE 8000


# Pull base image
FROM python:3.10

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    curl \
    ca-certificates \
    nginx   # Install Nginx in the container

# Install Microsoft ODBC 17
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Set work directory
WORKDIR /app

# Update pip and install python dependencies
RUN python -m pip install --upgrade pip
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Update the CA certificates
RUN update-ca-certificates

# Copy project files
COPY . /app/

# Copy SSL certificates into the container
COPY server.crt /etc/nginx/ssl/server.crt
COPY server.key /etc/nginx/ssl/server.key

# Expose the port for the Django app and Nginx (8000 for Django, 80 and 443 for Nginx)
EXPOSE 8000
EXPOSE 80
EXPOSE 443

# Command to run both Nginx and Django
CMD service nginx start && python manage.py runserver 0.0.0.0:8000
