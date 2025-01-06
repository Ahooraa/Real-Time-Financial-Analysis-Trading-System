from app.kafka_producer import send_to_kafka
import logging
import datetime
from zoneinfo import ZoneInfo

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
    stock_symbol = data.get("stock_symbol", "")
    dt_local = datetime.datetime.fromtimestamp(data.get("timestamp", 0), tz=ZoneInfo("Asia/Tehran"))
    data["local_time"] = dt_local.strftime('%Y-%m-%d %H:%M:%S')

    kafka_result = send_to_kafka(data, topic, key=stock_symbol)
    if not kafka_result:
        return {"error": "Failed to send data to Kafka"}

    return {"status": "success", "message": f"Data processed and forwarded to Kafka topic:{topic}", "data": data}
