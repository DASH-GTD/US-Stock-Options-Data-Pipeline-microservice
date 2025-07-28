# from datetime import datetime, timedelta
# from django.http import JsonResponse
# from django.views.decorators.csrf import csrf_exempt
# from collector.utils.logConfig import LogConfig
# from collector.kafka.kafkaConfig import create_producer
# from collector.handler.DataCollector import fetch_data
# from collector.handler.OptionDataCollector import OptionDataCollector

# # Configure logging
# logger = LogConfig()

# @csrf_exempt
# def fetch_historical_data(request):
#     return fetch_data(request, 'historical')

# @csrf_exempt
# def fetch_daily_data(request):
#     return fetch_data(request, 'daily')

# @csrf_exempt
# def fetch_15min_data(request):
#     return fetch_data(request, '15min')

# @csrf_exempt
# def fetch_option_data(request):
#     """
#     Fetches options data for multiple symbols (in batches of 8) from Yahoo Finance.
#     """
#     today = datetime.now().date()
#     start_date = datetime.strptime("2025-06-16", "%Y-%m-%d").date()
#     cutoff = today + timedelta(days=65)
#     full_result = []

#     logger.info(f"today: {today}")
#     logger.info(f"cutoff: {cutoff}")
#     producer = create_producer()
#     full_result = OptionDataCollector(producer, start_date, cutoff, full_result)

#     return JsonResponse({
#         "status": "success",
#         "message": "Options data fetched for all symbols in batches.",
#         "data": full_result
#     }, status=200)

# views.py
import json
from django.http import JsonResponse
from django.conf import settings
from collector.utils.logConfig import LogConfig
from collector.kafka import kafkaConfig
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collector.handler.OptionDataCollector import OptionDataCollector
from django.views.decorators.csrf import csrf_exempt

logger = LogConfig()

def fetch_data(request, trigger_type):
    """
    Endpoint to trigger data fetching by publishing a task to Kafka.
    """
    if trigger_type not in ['daily', '15min', 'historical']:
        logger.error(f"Invalid trigger_type: {trigger_type}")
        return JsonResponse({
            "status": "error",
            "message": f"Invalid trigger_type: {trigger_type}"
        }, status=400)

    logger.info(f"API triggered for fetching {trigger_type} data")
    try:
        producer = kafkaConfig.create_producer()
        if not producer:
            logger.error(f"Failed to create Kafka producer for {trigger_type}")
            return JsonResponse({
                "status": "error",
                "message": f"Failed to create Kafka producer"
            }, status=500)

        trigger_time = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')
        task_message = {
            "trigger_type": trigger_type,
            "trigger_time": trigger_time
        }
        kafka_topic = settings.KAFKA_TOPICS['task_queue']  
        producer.produce(kafka_topic, value=json.dumps(task_message).encode('utf-8'))
        producer.flush(timeout=10)

        logger.info(f"Published {trigger_type} task to Kafka topic {kafka_topic}")
        return JsonResponse({
            "status": "success",
            "message": f"Triggered {trigger_type} processing at {trigger_time}",
        }, status=202)
    except Exception as e:
        logger.error(f"Failed to trigger {trigger_type} task: {str(e)}")
        return JsonResponse({
            "status": "error",
            "message": f"Failed to trigger {trigger_type} processing"
        }, status=500)
        
       
@csrf_exempt 
def fetch_historical_data(request):
    return fetch_data(request, 'historical')

@csrf_exempt
def fetch_daily_data(request):
    return fetch_data(request, 'daily')

@csrf_exempt
def fetch_15min_data(request):
    return fetch_data(request, '15min')
    
@csrf_exempt
def fetch_option_data(request):
    """
    Fetches options data for multiple symbols (in batches of 8) from Yahoo Finance.
    """
    today = datetime.now().date()
    # start_date = datetime.strptime("2025-06-16", "%Y-%m-%d").date()
    cutoff = today + timedelta(days=90)
    full_result = []

    logger.info(f"today: {today}")
    logger.info(f"cutoff: {cutoff}")
    producer = kafkaConfig.create_producer()
    full_result = OptionDataCollector(producer, today, cutoff, full_result)

    return JsonResponse({
        "status": "success",
        "message": "Options data fetched for all symbols in batches.",
        "data": full_result
    }, status=200)