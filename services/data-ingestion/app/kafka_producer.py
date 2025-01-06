#!/usr/bin/env python3

import json
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

##################################
# 1. Create Kafka Producers
##################################
KAFKA_BROKER = "kafka:9092"

# Producer for all topics
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

##################################
# 2. Send Function
##################################
def send_to_kafka(data, topic, key=None):
    """
    Sends 'data' to the specified 'topic'.

    :param data: The message payload (dict or any Python object).
    :param topic: The Kafka topic string.
    :param key: The key string (e.g. stock symbol).
    """
    try:
        future = producer.send(topic, key=key, value=data)
        record_metadata = future.get(timeout=30)
        logging.info(
            f"Data sent to topic={topic}, partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}, data={data}"
        )
        return True
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")
        return False
