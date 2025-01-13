from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

import pandas as pd
import json
from kafka import KafkaProducer

# -------------------------------------------------------------------
# 1) Global DataFrame to store and accumulate all rows
# -------------------------------------------------------------------
global_data = pd.DataFrame(columns=[
    "stock_symbol", "local_time",
    "open", "high", "low", "close", "volume"
])

# -------------------------------------------------------------------
# 2) Define function to compute rolling indicators in Pandas
# -------------------------------------------------------------------
def compute_indicators_for_group(group_df: pd.DataFrame):
    """
    Computes SMA(5), EMA(10), RSI(10) for each symbol's rows (row-based).
    """
    gdf = group_df.copy()

    # -- SMA(5) --
    gdf["SMA_5"] = gdf["close"].rolling(window=5).mean()

    # -- EMA(10) --
    gdf["EMA_10"] = gdf["close"].ewm(span=10, adjust=False).mean()

    # -- RSI(10) --
    gdf["delta"] = gdf["close"].diff()
    gdf["gain"] = gdf["delta"].clip(lower=0)
    gdf["loss"] = -gdf["delta"].clip(upper=0)
    gdf["avg_gain_10"] = gdf["gain"].rolling(window=10).mean()
    gdf["avg_loss_10"] = gdf["loss"].rolling(window=10).mean()

    # Avoid division by zero
    gdf["rs"] = gdf["avg_gain_10"] / gdf["avg_loss_10"].replace({0: None})
    gdf["RSI_10"] = 100 - (100 / (1 + gdf["rs"]))

    return gdf

# -------------------------------------------------------------------
# 3) foreachBatch function
# -------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    print(f"\n=== Processing Micro-Batch: {batch_id} ===")

    # Disable Arrow conversions to avoid 'iteritems' error in .toPandas()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    # 1) Convert local_time to string to avoid timestamp issues
    batch_df = batch_df.selectExpr(
        "stock_symbol",
        "CAST(local_time AS STRING) AS local_time_str",
        "open", "high", "low", "close", "volume"
    )

    # 2) Convert to Pandas
    pdf = batch_df.toPandas()
    if pdf.empty:
        print("No new data in this batch.")
        return

    # 3) Convert local_time_str -> actual datetime
    pdf["local_time"] = pd.to_datetime(pdf["local_time_str"])
    pdf.drop(columns=["local_time_str"], inplace=True)

    # 4) Accumulate data into global_data
    global global_data
    global_data = pd.concat([global_data, pdf], ignore_index=True)

    # 5) Sort + compute rolling indicators
    global_data.sort_values(["stock_symbol", "local_time"], inplace=True)
    updated_pdf = global_data.groupby("stock_symbol", group_keys=False).apply(compute_indicators_for_group)
    updated_pdf.reset_index(drop=True, inplace=True)

    # 6) Group by (stock_symbol, local_time), keep only the last row per group
    #    so that for each minute (local_time), we only send 1 row per symbol
    latest_symbol_time = updated_pdf.groupby(
        ["stock_symbol", "local_time"], group_keys=False
    ).tail(1)

    # 7) Convert these final rows to JSON
    records = latest_symbol_time.to_dict(orient="records")

    # 8) Produce them to Kafka using kafka-python
    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )

    count = 0
    for row in records:
        producer.send(output_topic, row)
        count += 1

    producer.flush()
    producer.close()

    print(f"Sent {count} messages to Kafka topic: {output_topic}")

# -------------------------------------------------------------------
# 4) Spark Setup
# -------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("OneRowPerSymbolMinute") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Kafka config
kafka_broker = "kafka-broker:9092"
input_topics = "btcirt_topic,usdtirt_topic,ethirt_topic,etcirt_topic,shibirt_topic"
output_topic = "output_topic"

# Define schema for incoming JSON
schema = StructType([
    StructField("stock_symbol", StringType(), True),
    StructField("local_time", TimestampType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True)
])

# (A) Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", input_topics) \
    .option("startingOffsets", "earliest") \
    .load()

# (B) Extract columns from 'value' JSON
value_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# (C) foreachBatch => process_batch
query = value_df.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
