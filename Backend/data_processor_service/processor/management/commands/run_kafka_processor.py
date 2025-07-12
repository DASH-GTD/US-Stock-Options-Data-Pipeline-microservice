import time
import json
import logging
import threading
from confluent_kafka import KafkaError, TopicPartition, KafkaException

from django.conf import settings
from django.core.management.base import BaseCommand


from processor.kafka import kafkaConfig
from processor.handler.DailyDataProcessor import DailyDataProcessor
from processor.handler.RealTimeDataProcessor import RealTimeDataProcessor
from processor.handler.OptionDataProcessor import OptionDataProcessor
from processor.handler.HistoricalDataProcessor import HistoricalDataProcessor
from processor.utils.logConfig import LogConfig
# Setup logging

logger = LogConfig()

class Command(BaseCommand):
    help = 'Kafka consumer with multi-threaded partition processing'

    def __init__(self):
        super().__init__()
        self.running = True  # Flag to control shutdown
        self.partition_threads = []
        self.active_partitions = {}  # Track assigned partitions

    def get_partitions(self, consumer, topic, retries=3, delay=5):
        """Fetch partitions for a topic with retries."""
        for attempt in range(retries):
            try:
                metadata = consumer.list_topics(topic=topic, timeout=10)
                if topic in metadata.topics:
                    partitions = metadata.topics[topic].partitions.keys()
                    logger.info(f"Partitions for topic {topic}: {partitions}")
                    return partitions
                else:
                    logger.warning(f"Topic {topic} not found in metadata")
            except KafkaException as e:
                logger.error(f"Failed to fetch metadata for {topic} (attempt {attempt + 1}/{retries}): {e}")
            except Exception as e:
                logger.error(f"Unexpected error fetching metadata for {topic} (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
        logger.warning(f"No partitions found for {topic} after {retries} attempts")
        return []

    def process_partition(self, topic, partition):
        """Process messages from a specific partition."""
        consumer = kafkaConfig.create_consumer()
        producer = kafkaConfig.create_producer()

        # Assign consumer to the specific partition, TopicPartition(topic, partition) create Kafka object that represent sepecific partition
        consumer.assign([TopicPartition(topic, partition)])
        logger.info(f"Started processing for {topic} partition {partition}")

        while self.running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error on {topic} partition {partition}: {msg.error()}")
                continue

            logger.info(f"Consumed message from {topic} partition {partition}: {msg}")
            raw_value = msg.value().decode('utf-8')

            try:
                data = json.loads(raw_value)
                logger.info(f"Consumed Data from {topic} partition {partition}: {data}")

                # Process based on topic
                if topic == settings.KAFKA_TOPICS['daily']:
                    processed = DailyDataProcessor(data)
                    output_topic = settings.KAFKA_TOPICS['processed-daily'],
                    
                elif topic == settings.KAFKA_TOPICS['15min']:
                    processed = RealTimeDataProcessor(data)
                    output_topic = settings.KAFKA_TOPICS['processed-15min'],
                    
                elif topic == settings.KAFKA_TOPICS['options']:
                    processed = OptionDataProcessor(data)
                    output_topic = settings.KAFKA_TOPICS['processed-options'],
                    
                elif topic == settings.KAFKA_TOPICS['historical']:
                    processed = HistoricalDataProcessor(data)
                    output_topic = settings.KAFKA_TOPICS['processed-historical'],
                    

                data_value = json.dumps(processed).encode('utf-8')
                logger.info(f"Publishing to {output_topic} from {topic} partition {partition}: {data_value}")
                producer.produce(output_topic, value=data_value)
                producer.flush()

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {topic} partition {partition}: {e}")
            except Exception as e:
                logger.error(f"Processing error in {topic} partition {partition}: {e}")

        consumer.close()
        logger.info(f"Stopped processing for {topic} partition {partition}")

    def monitor_partitions(self, consumer, topics, check_interval=30):
        """Periodically check for new partitions and start threads."""
        while self.running:
            for topic in topics:
                partitions = self.get_partitions(consumer, topic)
                for partition in partitions:
                    partition_key = (topic, partition)
                    if partition_key not in self.active_partitions:
                        logger.info(f"Starting thread for {topic} partition {partition}")
                        thread = threading.Thread(
                            target=self.process_partition,
                            args=(topic, partition)
                        )
                        thread.daemon = True
                        self.partition_threads.append(thread)
                        self.active_partitions[partition_key] = thread
                        thread.start()
            time.sleep(check_interval)

    def handle(self, *args, **options):
        topics = [
            settings.KAFKA_TOPICS['daily'],
            settings.KAFKA_TOPICS['15min'],
            settings.KAFKA_TOPICS['options'],
            settings.KAFKA_TOPICS['historical']
        ]
        logger.info(f"Configured Kafka topics: {topics}")

        consumer = kafkaConfig.create_consumer()

        # Starting partition monitoring in a separate thread
        monitor_thread = threading.Thread(
            target=self.monitor_partitions,
            args=(consumer, topics)
        )
        monitor_thread.daemon = True
        monitor_thread.start()

        logger.info("Kafka processor started. Monitoring for partitions and messages...")

        try:
            while self.running:
                # Keep main thread alive to let Kafka consumer threads continue running in the background.
                # When the main thread finishes, the entire program may terminate, even if background threads (daemon threads) are still running.
                time.sleep(1)  
        except KeyboardInterrupt:
            logger.info("Shutting down Kafka processor...")
            self.running = False
            for thread in self.partition_threads:
                thread.join(timeout=5.0)
            consumer.close()