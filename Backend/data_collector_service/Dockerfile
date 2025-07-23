FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV DJANGO_SETTINGS_MODULE=data_collector_service.settings

COPY . .

EXPOSE 8000

CMD ["sh", "-c", "python manage.py run_kafka_collector & gunicorn data_collector_service.wsgi:application --bind 0.0.0.0:8000"]