
from datetime import datetime, timedelta
from django.http import JsonResponse
from collector.utils.logConfig import LogConfig
from collector.kafka.kafkaConfig import create_producer
from collector.handler.DataCollector import fetch_data
# Configure logging
logger = LogConfig()


def fetch_historical_data(request):
    return fetch_data(request, 'historical')

def fetch_daily_data(request):
    return fetch_data(request, 'daily')

def fetch_15min_data(request):
    return fetch_data(request, '15min')


from collector.handler.OptionDataCollector import OptionDataCollector
def fetch_option_data(request):
    """
    Fetches options data for multiple symbols (in batches of 8) from Yahoo Finance.
    """
    today = datetime.now().date()
    start_date = datetime.strptime("2025-06-16", "%Y-%m-%d").date()
    # cutoff = datetime.strptime("2025-08-16", "%Y-%m-%d").date()
    cutoff = today + timedelta(days=65)
    full_result = []

    logger.info(f"today: {today}")
    logger.info(f"cutoff: {cutoff}")
    producer = create_producer()
    full_result = OptionDataCollector(producer, start_date, cutoff, full_result)

    return JsonResponse({
        "status": "success",
        "message": "Options data fetched for all symbols in batches.",
        "data": full_result
    }, status=200)