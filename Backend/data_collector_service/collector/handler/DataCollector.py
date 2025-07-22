import json
import time
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
from django.conf import settings
from django.http import JsonResponse
from collector.utils.create_batch import create_batches
from collector.kafka import kafkaConfig
from collector.utils.logConfig import LogConfig
from collector.utils.symbols import SYMBOL_SETS

# Configure logging
logger = LogConfig()

# Configuration
NUM_THREADS = 5
API_KEYS = settings.TWELVE_DATA_API_KEYS  # List of 5 API keys
NUM_SETS = 5 # Total number of sets
SYMBOLS_PER_SET = SYMBOL_SETS  # List of symbols for each set

class ThreadedDataCollector:
    def __init__(self, set_id, symbols, api_key, partition, trigger_type):
        self.set_id = set_id
        self.symbols = symbols
        self.api_key = api_key
        self.partition = partition
        self.trigger_type = trigger_type
        self.URL = "https://api.twelvedata.com/time_series"

    def fetch_data(self, params, symbol, batch_result, producer, kafka_topic):
        """
        Fetches data for a single symbol and produces it to Kafka.
        """
        for attempt in range(3):
            try:
                response = requests.get(self.URL, params=params, timeout=10)
                logger.debug(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Attempt {attempt + 1}, Response Status: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    if data.get('code') == 400 or "values" not in data or not data.get("values"):
                        message = data.get("message", "No data available or invalid symbol/parameters.")
                        logger.info(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: API returned error or no data: {message}")
                        batch_result[symbol] = {"error": message}
                        return False
                    else:
                        batch_result[symbol] = data
                        data_value = json.dumps(data).encode('utf-8')
                        logger.info(f"Publishing to {kafka_topic} from  partition {self.partition}: {data_value}")
                        producer.produce(kafka_topic, value=data_value, partition=self.partition)
                        logger.debug(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Produced data to partition {self.partition}, topic {kafka_topic}.")
                        return True
                else:
                    logger.error(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: HTTP Request failed with status {response.status_code}")
                    batch_result[symbol] = {
                        "error": f"HTTP Failed: {response.status_code}",
                        "details": response.text
                    }
                    if response.status_code == 429:
                        logger.warning(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Rate limit hit. Retrying after delay.")
                        time.sleep(15 * (attempt + 1))
                    else:
                        time.sleep(5)
            except requests.RequestException as e:
                logger.error(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Request exception on attempt {attempt + 1}: {str(e)}")
                batch_result[symbol] = {"error": str(e)}
                time.sleep(5 * (attempt + 1))
            except Exception as e:
                logger.error(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Unexpected error on attempt {attempt + 1}: {str(e)}")
                batch_result[symbol] = {"error": f"Unexpected error: {str(e)}"}
                time.sleep(5)
        logger.error(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: All {attempt + 1} attempts failed to fetch data.")
        return False

    def process(self):
        logger.info(f"Thread {self.set_id}, Trigger {self.trigger_type}: Processing set {self.set_id}, partition {self.partition} for {len(self.symbols)} symbols.")
        
        producer = kafkaConfig.create_producer()
        if not producer:
            logger.error(f"Thread {self.set_id}, Trigger {self.trigger_type}: Failed to create Kafka producer.")
            return []

        batch_results = []
        params_base = {}
        kafka_topic = ""
        sleep_time = 70  # Adjusted based on API rate limits

        if self.trigger_type == 'daily':
            params_base = {
                "interval": "1day",
                "start_date": (datetime.now(ZoneInfo("UTC")).date() - timedelta(days=1)).strftime('%Y-%m-%d'),
                "end_date": datetime.now(ZoneInfo("UTC")).date().strftime('%Y-%m-%d'),
                "outputsize": 1
            }
            kafka_topic = settings.KAFKA_TOPICS['daily']
        elif self.trigger_type == '15min':
            params_base = {
                "interval": "1min",
                "start_date": (datetime.now(ZoneInfo("UTC")) - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S'),
                "end_date": datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S'),
                "outputsize": 15
            }
            kafka_topic = settings.KAFKA_TOPICS['15min']
        elif self.trigger_type == 'historical':
            params_base = {
                "interval": "1day",
                "start_date": (datetime.now(ZoneInfo("UTC")).date() - timedelta(days=7)).strftime('%Y-%m-%d'),
                "end_date": datetime.now(ZoneInfo("UTC")).date().strftime('%Y-%m-%d'),
                "outputsize": 5000
            }
            kafka_topic = settings.KAFKA_TOPICS['historical']
        else:
            logger.error(f"Thread {self.set_id}, Trigger {self.trigger_type}: Unknown trigger_type. Exiting process.")
            return []

        try:
            logger.info(f"Thread {self.set_id}, Trigger {self.trigger_type}: Starting data fetch for {len(self.symbols)} symbols.")
            for batch in create_batches(self.symbols, batch_size=8):
                logger.info(f"Thread {self.set_id}, Trigger {self.trigger_type}: Processing batch: {batch}")
                batch_result = {}
                for symbol in batch:
                    params = params_base.copy()
                    params.update({"symbol": symbol, "apikey": self.api_key})
                    success = self.fetch_data(params, symbol, batch_result, producer, kafka_topic)
                    if not success:
                        logger.warning(f"Thread {self.set_id}, Symbol {symbol}, Trigger {self.trigger_type}: Failed to fetch")
                batch_results.append(batch_result)
                logger.info(f"Thread {self.set_id}, Trigger {self.trigger_type}: Completed batch, waiting {sleep_time} seconds (for API rate limits).")
                time.sleep(sleep_time)
        except Exception as e:
            logger.error(f"Thread {self.set_id}, Trigger {self.trigger_type}: Processing failed: {str(e)}")
        finally:
            producer.flush(timeout=30)
            logger.info(f"Thread {self.set_id}, Trigger {self.trigger_type}: All buffered data flushed to Kafka topic {kafka_topic}.")
        
        return batch_results

logger = LogConfig()


def fetch_data(request, trigger_type):
    """
    Generic endpoint to trigger data fetching for 'daily', '15min', or 'historical' data.
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
        trigger_time = datetime.now(ZoneInfo("UTC")).strftime('%Y-%m-%d %H:%M:%S')

        # Divide sets among threads
        sets_per_thread = NUM_SETS // NUM_THREADS
        remainder_sets = NUM_SETS % NUM_THREADS
        set_assignments = []
        start_set = 0

        for i in range(NUM_THREADS):
            num_sets = sets_per_thread + (1 if i < remainder_sets else 0)
            set_assignments.append(list(range(start_set, start_set + num_sets)))
            start_set += num_sets

        def process_thread(thread_id, set_ids):
            api_key = API_KEYS[thread_id]
            for set_id in set_ids:
                symbols = SYMBOLS_PER_SET[set_id]
                collector = ThreadedDataCollector(set_id, symbols, api_key, set_id, trigger_type)
                collector.process()
        
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = [executor.submit(process_thread, i, set_ids) for i, set_ids in enumerate(set_assignments)]
            for future in futures:
                future.result()  # Wait for all threads to complete

        producer.flush(timeout=30)
        return JsonResponse({
            "status": "success",
            "message": f"Triggered {trigger_type} processing for all sets at {trigger_time}",
        }, status=202)
    except Exception as e:
        logger.error(f"Failed to process {trigger_type} data: {str(e)}")
        return JsonResponse({
            "status": "error",
            "message": f"Failed to trigger {trigger_type} processing"
        }, status=500)