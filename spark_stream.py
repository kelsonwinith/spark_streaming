from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as spark_sum, 
    count, lag, unix_timestamp, current_timestamp,
    round as spark_round
)
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType
import time
from datetime import datetime

# Create Spark Session with Kafka package
spark = SparkSession.builder \
    .appName("StockVelocityAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Set log level to ERROR to reduce noise
spark.sparkContext.setLogLevel("ERROR")

# Benchmark metrics
start_time = time.time()
total_records = 0
window_count = 0

def process_batch(df, epoch_id):
    global total_records, window_count
    batch_count = df.count()
    total_records += batch_count
    window_count += 1
    
    current_time = time.time()
    elapsed = current_time - start_time
    records_per_second = total_records / elapsed
    
    print(f"\n=== Benchmark Metrics at {datetime.now()} ===")
    print(f"Total Records Processed: {total_records}")
    print(f"Average Records/Second: {records_per_second:.2f}")
    print(f"Windows Processed: {window_count}")
    print(f"Elapsed Time: {elapsed:.2f} seconds")
    print("=====================================\n")

# Schema for the incoming stock data
schema = StructType() \
    .add("Datetime", StringType()) \
    .add("Open", FloatType()) \
    .add("High", FloatType()) \
    .add("Low", FloatType()) \
    .add("Close", FloatType()) \
    .add("Volume", IntegerType()) \
    .add("Dividends", FloatType()) \
    .add("Stock Splits", IntegerType()) \
    .add("symbol", StringType())

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "market-data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and prepare the data
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("Datetime").cast(TimestampType()))

# Calculate velocity metrics over sliding windows
velocity_metrics = parsed_df \
    .withWatermark("event_time", "60 seconds") \
    .groupBy(
        window(col("event_time"), "30 seconds", "5 seconds"),
        col("symbol")
    ).agg(
        # Trading Volume Velocity (volume per second)
        (spark_sum("Volume") / 30).alias("volume_velocity"),
        
        # Price Change Velocity (price change per second)
        ((spark_round(avg("Close"), 2) - spark_round(avg("Open"), 2)) / 30).alias("price_velocity"),
        
        # Trading Frequency (trades per second based on volume changes)
        (count("*") / 30).alias("trading_frequency"),
        
        # Price Volatility (difference between high and low)
        (avg("High") - avg("Low")).alias("price_range"),
        
        # Additional metrics
        avg("Close").alias("avg_price"),
        spark_sum("Volume").alias("total_volume"),
        count("*").alias("updates_count")
    )

# Write query to console with complete output mode and benchmarking
query = velocity_metrics.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .foreachBatch(process_batch) \
    .start()

print("Starting velocity analysis with benchmarking...")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping streaming query")
    final_time = time.time()
    elapsed = final_time - start_time
    print(f"\nFinal Statistics:")
    print(f"Total Runtime: {elapsed:.2f} seconds")
    print(f"Total Records: {total_records}")
    print(f"Average Processing Rate: {total_records/elapsed:.2f} records/second")
finally:
    query.stop()
    spark.stop()