from confluent_kafka import Consumer, Producer
from django.conf import settings
import os 

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'data_processor_group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'PLAINTEXT'
}
PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'PLAINTEXT'
}

def create_consumer():
    """Create and return a Kafka consumer."""
    return Consumer(CONSUMER_CONFIG)

def create_producer():
    """Create and return a Kafka producer."""
    return Producer(PRODUCER_CONFIG)
