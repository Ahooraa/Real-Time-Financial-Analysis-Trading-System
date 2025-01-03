from kafka import KafkaConsumer
import json

# Kafka configuration
KAFKA_BROKER = "kafka-broker:9092"
TOPICS = ["stock_prices", "order_book", "news_sentiment", "market_data", "economic_indicator"]

# Initialize Kafka consumer
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Subscribed to topics: {TOPICS}")

# Consume messages
for message in consumer:
    print(f"Received message from topic {message.topic}: {message.value}")