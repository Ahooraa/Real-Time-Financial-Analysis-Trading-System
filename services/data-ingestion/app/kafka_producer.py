from kafka import KafkaProducer
import json
import logging
logging.basicConfig(level=logging.INFO)
# Kafka configuration
KAFKA_BROKER = "kafka:9092"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_to_kafka(data, topic):
    try:
        # Send data to Kafka topic
        producer.send(topic, value=data)
        producer.flush()
        logging.info(f"Data sent to Kafka topic {topic}: {data}")
        return True
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")
        return False
