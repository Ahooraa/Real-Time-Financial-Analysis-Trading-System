import os
import json
import time
import psycopg2

from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError

# Kafka configs
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'my_topic')


# QuestDB configs
QUESTDB_HOST = os.getenv('QUESTDB_HOST', 'localhost')
QUESTDB_PORT = int(os.getenv('QUESTDB_PORT', '8812'))
QUESTDB_DB = os.getenv('QUESTDB_DB', 'qdb')
QUESTDB_USER = os.getenv('QUESTDB_USER', 'admin')
QUESTDB_PASSWORD = os.getenv('QUESTDB_PASSWORD', 'quest')


def create_kafka_consumer():
    """
    Create and return a KafkaConsumer using kafka-python.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,  # we can pass multiple topics if needed
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )
    return consumer


def create_db_connection():
    """
    Create and return a connection to QuestDB via psycopg2.
    """
    conn = psycopg2.connect(
        dbname=QUESTDB_DB,
        user=QUESTDB_USER,
        password=QUESTDB_PASSWORD,
        host=QUESTDB_HOST,
        port=QUESTDB_PORT
    )
    return conn


def create_table_if_not_exists(conn):
    """
    Create the stock_data table in QuestDB if it doesn't exist.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_data (
        stock_symbol SYMBOL,
        topic STRING,
        local_time TIMESTAMP,
        open DOUBLE,
        close DOUBLE,
        high DOUBLE,
        low DOUBLE,
        volume LONG,
        SMA_5 DOUBLE,
        EMA_5 DOUBLE,
        RSI_7 DOUBLE
    )
    TIMESTAMP(local_time)
    PARTITION BY DAY;
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()


def insert_stock_data(conn, data):
    """
    Insert a record into the stock_data table in QuestDB.
    """
    query = """
    INSERT INTO stock_data 
        (stock_symbol, topic, local_time, open, close, high, low, volume, SMA_5, EMA_5, RSI_7)
    VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        cur.execute(query, (
            data['stock_symbol'],
            data['topic'],
            data['local_time'],
            data['open'],
            data['close'],
            data['high'],
            data['low'],
            data['volume'],
            data['SMA_5'],
            data['EMA_5'],
            data['RSI_7']
        ))


def main():
    consumer = create_kafka_consumer()
    conn = create_db_connection()
    create_table_if_not_exists(conn)

    print("Consumer started. Waiting for messages...")

    try:
        for msg in consumer:
            # msg is a kafka.ConsumerRecord object
            try:
                data_str = msg.value.decode('utf-8')
                data_json = json.loads(data_str)

                insert_stock_data(conn, data_json)
                conn.commit()

                # Manually commit offset after successful DB insert
                consumer.commit()

            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing JSON or missing fields: {e}")

    except KeyboardInterrupt:
        print("Stopping consumer...")

    except KafkaError as e:
        print(f"Kafka error: {e}")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
