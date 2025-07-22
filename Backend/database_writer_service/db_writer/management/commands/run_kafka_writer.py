import os
import json
import time
import threading
import traceback
from confluent_kafka import Consumer, KafkaError, KafkaException

from django.conf import settings
from django.core.management.base import BaseCommand

from db_writer.kafka import kafkaConfig
from db_writer.handler.InfluxHandler import InfluxHandler
from db_writer.handler.DailySQLHandler import DailySQLHandler
from db_writer.handler.HistoricalSQLHandler import HistoricalSQLHandler
from db_writer.handler.OptionsSQLHandler import OptionsSQLHandler
from db_writer.utils.logConfig import LogConfig

# Setup logging
logger = LogConfig()
influx = InfluxHandler()
daily = DailySQLHandler() 
historical = HistoricalSQLHandler()
options = OptionsSQLHandler()

class Command(BaseCommand):
    help = 'Kafka consumer with robust partition processing'

    def __init__(self):
        super().__init__()
        self.running = True
        self.consumer = None

    def process_messages(self, consumer, topics):
        """Process messages from subscribed topics."""
        max_retries = 3
        retry_delay = 5

        try:
            consumer.subscribe(topics)
            logger.info(f"Subscribed to topics: {topics}")

            while self.running:
                try:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            logger.error(f"Kafka error on {msg.topic()} partition {msg.partition()}: {msg.error()}")
                        continue

                    logger.info(f"Consumed message from {msg.topic()} partition {msg.partition()}")
                    raw_value = msg.value().decode('utf-8')
                    logger.debug(f"Raw message data: {raw_value}")

                    try:
                        data = json.loads(raw_value)
                        logger.info(f"Parsed data from {msg.topic()} partition {msg.partition()}: {data}")

                        # Validate data
                        if not isinstance(data, list) or not data:
                            logger.error(f"Invalid data format: {data}")
                            continue
                           
                        # Process based on topic
                        if os.getenv("RUN_DB_HANDLER") == "True":
                            for attempt in range(max_retries):
                                try:
                                    if msg.topic() == settings.KAFKA_TOPICS['processed-daily']:
                                        daily.write_data(data)
                                    elif msg.topic() == settings.KAFKA_TOPICS['processed-15min']:
                                        influx.write_data(data)
                                    elif msg.topic() == settings.KAFKA_TOPICS['processed-historical']:
                                        historical.write_data(data)
                                    elif msg.topic() == settings.KAFKA_TOPICS['processed-options']:
                                        options.write_data(data)
                                    logger.info(f"Successfully processed data for {msg.topic()}")
                                    break
                                except Exception as e:
                                    logger.error(f"Processing attempt {attempt + 1}/{max_retries} failed for {msg.topic()}: {e}")
                                    if attempt < max_retries - 1:
                                        time.sleep(retry_delay)
                                    else:
                                        logger.error(f"All retries failed for {msg.topic()} partition {msg.partition()}")
                            else:
                                logger.error(f"Set RUN_DB_HANDLER to True to Execute DB Handler")


                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in {msg.topic()} partition {msg.partition()}: {e}")
                    except Exception as e:
                        logger.error(f"Unexpected error processing message: {e}\n{traceback.format_exc()}")

                except KafkaException as e:
                    logger.error(f"Kafka consumer error: {e}\n{traceback.format_exc()}")
                    time.sleep(retry_delay)

        except Exception as e:
            logger.error(f"Consumer thread crashed: {e}\n{traceback.format_exc()}")
        finally:
            logger.info("Closing consumer")
            consumer.close()

    def handle(self, *args, **options):
        topics = [
            settings.KAFKA_TOPICS['processed-daily'],
            settings.KAFKA_TOPICS['processed-15min'],
            settings.KAFKA_TOPICS['processed-options'],
            settings.KAFKA_TOPICS['processed-historical']
        ]
        logger.info(f"Configured Kafka topics: {topics}")

        self.consumer = kafkaConfig.create_consumer()
        consumer_thread = threading.Thread(
            target=self.process_messages,
            args=(self.consumer, topics)
        )
        consumer_thread.daemon = True
        consumer_thread.start()

        logger.info("Kafka processor started. Monitoring for messages...")

        try:
            while self.running:
                if not consumer_thread.is_alive():
                    logger.error("Consumer thread has stopped. Restarting...")
                    self.consumer = kafkaConfig.create_consumer()
                    consumer_thread = threading.Thread(
                        target=self.process_messages,
                        args=(self.consumer, topics)
                    )
                    consumer_thread.daemon = True
                    consumer_thread.start()
                time.sleep(5)  # Check thread health periodically
        except KeyboardInterrupt:
            logger.info("Received shutdown signal. Shutting down Kafka processor...")
            self.running = False
            consumer_thread.join(timeout=10.0)
            self.consumer.close()
            daily.close()
            historical.close()
            options.close()
            logger.info("Kafka processor and database connections shut down successfully")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}\n{traceback.format_exc()}")
            self.running = False
            consumer_thread.join(timeout=10.0)
            self.consumer.close()
            daily.close()
            historical.close()
            options.close()
            logger.info("Kafka processor shut down due to error")