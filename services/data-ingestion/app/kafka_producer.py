from kafka import KafkaProducer
import json
import logging
import mmh3  # MurmurHash3 for consistent hashing

logging.basicConfig(level=logging.INFO)

# Kafka configuration
KAFKA_BROKER = "kafka:9092"

############################
# 1. Partition Map
############################
# For the "stock_data" topic, you want each known symbol to have a dedicated partition.
# Example mapping:
#   TSLA -> partition 1
#   GOOGL -> partition 2
#   MSFT -> partition 3
#   AMZN -> partition 4
#   ...
# If the symbol is not in this map, we do standard hashing.

partition_map = {
    "AMZN": 0,
    "TSLA": 1,
    "AAPL": 2,
    "GOOGL": 3,
    "MSFT": 4,
    # etc...
}

############################
# 2. Custom Partitioner
############################
def custom_partitioner(key_bytes, all_partitions, available_partitions):
    """
    If key_bytes corresponds to a known stock symbol, we force a specific partition.
    Otherwise, we do hash-based partitioning for consistency.
    """
    # Safety check: if no key is provided, do a fallback
    if not key_bytes:
        # Just default to partition 0 or do a random/hashing fallback
        logging.info("No key provided. Using partition 0.")
        return 0

    stock_symbol = key_bytes.decode('utf-8')  # Convert bytes to string
    if stock_symbol in partition_map:
        forced_partition = partition_map[stock_symbol]
        logging.info(f"[Partitioner] Forcing symbol={stock_symbol} to partition={forced_partition}")
        return forced_partition

    # Otherwise, do normal hash-based partitioning
    key_hash = mmh3.hash(key_bytes)
    partition = key_hash % len(all_partitions)
    logging.info(f"[Partitioner] symbol={stock_symbol} hashed to partition={partition}")
    return partition

############################
# 3. Kafka Producer
############################
# We set the 'partitioner' argument to use our custom function
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    partitioner=custom_partitioner
)

############################
# 4. Send Function
############################
def send_to_kafka(data, topic, key=None):
    """
    Sends data to the given topic. 
    - 'key' is your stock symbol (or another string).
    - If 'key' matches one in partition_map, we force its partition.
    """
    try:
        # The key controls the partition assignment
        future = producer.send(topic, key=key, value=data)
        record_metadata = future.get(timeout=10)
        logging.info(
            f"Data sent to topic={topic}, partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}, data={data}"
        )
        producer.flush()
        return True
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")
        return False
