from app.kafka_producer import send_to_kafka
import logging
logging.basicConfig(level=logging.INFO)

def process_data(data):
    # Validate required fields
    # required_fields = ["stock_symbol", "opening_price", "closing_price", "timestamp"]
    # for field in required_fields:
    #     if field not in data:
    #         return {"error": f"Missing field: {field}"}

    # Ensure numerical fields are valid
    # try:
    #     if(["average_price"] in data):
    #         data["average_price"] = (float(data["opening_price"]) + float(data["closing_price"])) / 2
    # except ValueError:
    #     return {"error": "Invalid numerical values for opening or closing price"}

    # Forward to Kafka
    logging.info("received message from the generator: ", data)
    kafka_result = send_to_kafka(data)
    if not kafka_result:
        return {"error": "Failed to send data to Kafka"}

    return {"status": "success", "message": "Data processed and forwarded to Kafka", "data": data}
