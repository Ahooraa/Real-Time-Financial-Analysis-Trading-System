import time
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)

# Kafka configuration
KAFKA_BROKER = "kafka-broker:9092"
TOPICS = ["btcirt_topic", "usdtirt_topic", "ethirt_topic", "etcirt_topic", "shibirt_topic"]

def consume_messages():
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='stream-processing-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info("Kafka Consumer started, listening on topics: %s", TOPICS)
            for message in consumer:
                logging.info(
                    "Received message: Topic=%s, Partition=%s, Offset=%s, Key=%s, Value=%s",
                    message.topic, message.partition, message.offset, message.key, message.value
                )
        except KafkaError as e:
            logging.error("Error connecting to Kafka: %s", e)
            logging.info("Retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            logging.info("Kafka Consumer stopped by the user.")
            break
        except Exception as e:
            logging.error("Unexpected error: %s", e)
            logging.info("Retrying in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    consume_messages()
