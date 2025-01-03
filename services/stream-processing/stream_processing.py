from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TradingIndicators") \
    .getOrCreate()

# Define schema for incoming data
schema = StructType([
    StructField("stock_symbol", StringType(), True),
    StructField("opening_price", FloatType(), True),
    StructField("closing_price", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", LongType(), True)
])

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock_prices") \
    .load()

# Convert value column to string and parse JSON
stock_data_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, schema) as data") \
    .select("data.*")

# Calculate Moving Average
moving_avg_df = stock_data_df \
    .groupBy("stock_symbol") \
    .agg(avg("closing_price").alias("moving_average"))

# Calculate Exponential Moving Average (EMA)
ema_df = stock_data_df \
    .withColumn("ema", expr("exp(avg(log(closing_price))) OVER (PARTITION BY stock_symbol ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"))

# Calculate Relative Strength Index (RSI)
def calculate_rsi(df):
    window_spec = Window.partitionBy("stock_symbol").orderBy("timestamp").rowsBetween(-14, 0)
    df = df.withColumn("change", col("closing_price") - lag("closing_price", 1).over(window_spec))
    df = df.withColumn("gain", when(col("change") > 0, col("change")).otherwise(0))
    df = df.withColumn("loss", when(col("change") < 0, -col("change")).otherwise(0))
    df = df.withColumn("avg_gain", avg("gain").over(window_spec))
    df = df.withColumn("avg_loss", avg("loss").over(window_spec))
    df = df.withColumn("rs", col("avg_gain") / col("avg_loss"))
    df = df.withColumn("rsi", 100 - (100 / (1 + col("rs"))))
    return df

rsi_df = calculate_rsi(stock_data_df)

# Write the results back to Kafka
query = moving_avg_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "processed_stock_data") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()