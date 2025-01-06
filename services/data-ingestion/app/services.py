from app.kafka_producer import send_to_kafka
import logging
import datetime
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO)

# Mapping of stock symbols to topics
symbol_to_topic = {
    "BTCIRT": "btcirt_topic",
    "USDTIRT": "usdtirt_topic",
    "ETHIRT": "ethirt_topic",
    "ETCIRT": "etcirt_topic",
    "SHIBIRT": "shibirt_topic"
}

def process_data(data):
    # Determine the topic based on the stock symbol
    stock_symbol = data.get("stock_symbol", "")
    topic = symbol_to_topic.get(stock_symbol, "default_topic")

    # Forward to Kafka
    logging.info("received message from the generator: %s", data)
    kafka_result = send_to_kafka(data, topic, key=stock_symbol)
    if not kafka_result:
        return {"error": "Failed to send data to Kafka"}

    return {"status": "success", "message": f"Data processed and forwarded to Kafka topic:{topic}", "data": data}
