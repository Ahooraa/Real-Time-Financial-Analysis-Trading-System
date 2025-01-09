from app.kafka_producer import send_to_kafka
import logging
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.INFO)

def process_data(data):
    try:
        # Determine the topic based on the stock symbol
        stock_symbol = data.get("stock_symbol", "")
        topic = f"{stock_symbol.lower()}_topic" if stock_symbol else "default_topic"

        # Forward to Kafka
        logging.info("received message from the API: %s", data)
        data["topic"]= topic
        kafka_result = send_to_kafka(data, topic, key=stock_symbol)
        if not kafka_result:
            return {"error": "Failed to send data to Kafka"}

        return {"status": "success", "message": f"Data processed and forwarded to Kafka topic:{topic}", "data": data}

    except Exception as e:
        logging.error("Exception occurred while processing data: %s", str(e))
        return {"error": "Exception occurred while processing data", "details": str(e)}
