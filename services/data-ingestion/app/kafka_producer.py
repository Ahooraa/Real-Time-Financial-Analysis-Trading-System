#!/usr/bin/env python3

import json
import logging
import mmh3
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

##################################
# 1. Partition Maps and Classes
##################################
partition_map = {
    "AMZN": 0,
    "TSLA": 1,
    "AAPL": 2,
    "GOOGL": 3,
    "MSFT": 4,
    # ...
}

class StockPricesPartitioner:
    """
    Custom partitioner for the 'stock_prices' topic. Forces known stock symbols
    to a specific partition. Otherwise, fallback to hash-based partitioning.
    """
    def __call__(self, key_bytes, all_partitions, available_partitions):
        if not key_bytes:
            logging.info("[StockPricesPartitioner] No key -> partition=0 by default")
            return 0

        symbol = key_bytes.decode('utf-8')
        if symbol in partition_map:
            forced_partition = partition_map[symbol]
            logging.info(f"[StockPricesPartitioner] Forcing symbol={symbol} to partition={forced_partition}")
            return forced_partition

        # fallback: hash-based
        key_hash = mmh3.hash(key_bytes)
        partition = key_hash % len(all_partitions)
        logging.info(f"[StockPricesPartitioner] Hashing symbol={symbol} -> partition={partition}")
        return partition


class SinglePartitionPartitioner:
    """
    Simple partitioner that always returns partition=0
    (or you could do random, or just use default partitioner).
    """
    def __call__(self, key_bytes, all_partitions, available_partitions):
        logging.info("[SinglePartitionPartitioner] Using partition=0")
        return 0

##################################
# 2. Create Two Producers
##################################
KAFKA_BROKER = "kafka:9092"

# Producer for 'stock_prices' (multi-partition, one partition per known symbol)
stock_prices_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    partitioner=StockPricesPartitioner()
)

# Producer for all other topics (always partition=0, or you can use default)
other_producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    partitioner=SinglePartitionPartitioner()
)

##################################
# 3. Send Function
##################################
def send_to_kafka(data, topic, key=None):
    """
    Sends 'data' to the specified 'topic' using either:
    - 'stock_prices_producer' if topic == "stock_prices"
    - 'other_producer' otherwise

    :param data: The message payload (dict or any Python object).
    :param topic: The Kafka topic string, e.g. "stock_prices"
    :param key: The key string (e.g. stock symbol).
    """
    try:
        if topic == "stock_prices":
            future = stock_prices_producer.send(topic, key=key, value=data)
        else:
            future = other_producer.send(topic, key=key, value=data)

        record_metadata = future.get(timeout=30)
        logging.info(
            f"Data sent to topic={topic}, partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}, data={data}"
        )
        return True
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")
        return False
