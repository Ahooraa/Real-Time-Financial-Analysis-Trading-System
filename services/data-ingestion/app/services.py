from app.kafka_producer import send_to_kafka
import logging

logging.basicConfig(level=logging.INFO)

def process_data(data):
    # Determine the topic based on the data type
    if "data_type" in data:
        data_type = data["data_type"]
        if data_type == "order_book":
            topic = "order_book"
        elif data_type == "news_sentiment":
            topic = "news_sentiment"
        elif data_type == "market_data":
            topic = "market_data"
        elif data_type == "economic_indicator":
            topic = "economic_indicator"
        else:
            return {"error": "Unknown data type"}
    else:
        topic = "stock_prices"

    # Forward to Kafka
    logging.info("received message from the generator: %s", data)
    kafka_result = send_to_kafka(data, topic)
    if not kafka_result:
        return {"error": "Failed to send data to Kafka"}

    return {"status": "success", "message": f"Data processed and forwarded to Kafka topic:{topic}", "data": data}
