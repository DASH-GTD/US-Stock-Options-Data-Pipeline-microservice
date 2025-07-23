import json
from concurrent.futures import ThreadPoolExecutor
from django.conf import settings
from django.core.management.base import BaseCommand
from collector.utils.logConfig import LogConfig
from collector.utils.symbols import SYMBOL_SETS
from collector.handler.DataCollector import ThreadedDataCollector
from collector.kafka import kafkaConfig

logger = LogConfig()

# Configuration
NUM_THREADS = 5
API_KEYS = settings.TWELVE_DATA_API_KEYS
NUM_SETS = 5
SYMBOLS_PER_SET = SYMBOL_SETS

def process_task(trigger_type):
    """
    Process a task for the given trigger_type.
    """
    logger.info(f"Processing task for trigger_type: {trigger_type}")
    try:
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

        logger.info(f"Completed processing for {trigger_type}")
    except Exception as e:
        logger.error(f"Failed to process {trigger_type} task: {str(e)}")

class Command(BaseCommand):
    help = 'Run Kafka consumer to process tasks from the task_queue topic'

    def handle(self, *args, **options):
        """
        Execute the Kafka consumer to listen for tasks on the task_queue topic.
        """
        consumer = kafkaConfig.create_consumer()
        if not consumer:
            logger.error("Failed to create Kafka consumer")
            self.stderr.write("Failed to create Kafka consumer")
            return

        consumer.subscribe([settings.KAFKA_TOPICS['task_queue']])
        logger.info("Kafka consumer started, listening on task_queue topic")
        self.stdout.write("Kafka consumer started, listening on task_queue topic")

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    self.stderr.write(f"Consumer error: {msg.error()}")
                    continue

                try:
                    task = json.loads(msg.value().decode('utf-8'))
                    trigger_type = task.get('trigger_type')
                    if trigger_type in ['daily', '15min', 'historical']:
                        logger.info(f"Received task: {trigger_type}")
                        self.stdout.write(f"Received task: {trigger_type}")
                        process_task(trigger_type)
                    else:
                        logger.error(f"Invalid trigger_type in task: {trigger_type}")
                        self.stderr.write(f"Invalid trigger_type in task: {trigger_type}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode task message: {str(e)}")
                    self.stderr.write(f"Failed to decode task message: {str(e)}")
        except KeyboardInterrupt:
            logger.info("Shutting down consumer")
            self.stdout.write("Shutting down consumer")
        finally:
            consumer.close()
            self.stdout.write("Kafka consumer closed")