from kafka import KafkaProducer
import json
import logging
logging.basicConfig(level=logging.INFO)
# Kafka configuration
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "stock_data"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_to_kafka(data):
    try:
        # Send data to Kafka topic
        producer.send(TOPIC_NAME, value=data)
        producer.flush()
        logging.info(f"Data sent to Kafka topic {TOPIC_NAME}: {data}")
        return True
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")
        return False
