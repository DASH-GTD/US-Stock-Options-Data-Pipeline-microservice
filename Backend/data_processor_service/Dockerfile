FROM python:3.9-slim

WORKDIR /app 

COPY requirements.txt . 

RUN pip install --no-cache-dir -r requirements.txt 

ENV DJANGO_SETTINGS_MODULE=data_processor_service.settings

COPY . .

EXPOSE 8001 

CMD ["sh", "-c", "python manage.py run_kafka_processor & gunicorn data_processor_service.wsgi:application --bind 0.0.0.0:8001"]