# FROM python:3.9-slim

# WORKDIR /app
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt
# COPY . .
# EXPOSE 8000
# CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]


FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DJANGO_SETTINGS_MODULE=data_collector_service.settings

# Copy application code
COPY . .

# Expose port 8000
EXPOSE 8000

# Run Gunicorn with configuration
CMD ["python","manage.py", "runserver", "0.0.0.0:8000"]
