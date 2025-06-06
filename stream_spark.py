from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, count, sum as spark_sum
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType, FloatType, StructField
from datetime import datetime

# Create Spark Session
spark = SparkSession.builder \
    .appName("HashtagCounter") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.streaming.kafka.maxRatePerPartition", "100000") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/opt/spark/logs") \
    .config("spark.history.fs.logDirectory", "/opt/spark/logs") \
    .getOrCreate()

# Set log level to ERROR to reduce noise
spark.sparkContext.setLogLevel("ERROR")

# Schema for the incoming tweet data
schema = StructType([
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("username", StringType()),
    StructField("created_at", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("likes", IntegerType()),
    StructField("retweets", IntegerType()),
    StructField("total_likes", IntegerType()),
    StructField("total_retweets", IntegerType()),
    StructField("viral_score", FloatType()),
    StructField("streaming_timestamp", StringType()),
    StructField("batch_id", StringType())
])

try:
    # Read from Kafka
    tweets_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "tweet-stream") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 100000) \
        .option("kafka.max.partition.fetch.bytes", "10485760") \
        .option("kafka.fetch.max.bytes", "52428800") \
        .load()

    # Parse JSON and extract hashtags
    parsed_df = tweets_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.hashtags")

    # Count hashtags
    hashtag_counts = parsed_df \
        .select(explode("hashtags").alias("hashtag")) \
        .groupBy("hashtag") \
        .agg(count("*").alias("count"))

    # Custom output formatting
    def foreach_batch_function(df, epoch_id):
        if df.isEmpty():
            # Handle empty DataFrame
            current_time = datetime.now().replace(microsecond=0)
            # Round to nearest 5 seconds
            seconds = (current_time.second // 5) * 5
            timestamp = current_time.replace(second=seconds)
            
            print("\n" + "="*50)
            print(f"HASHTAG COUNTS AT {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*50)
            print("No new hashtags in this window")
            return

        # Process all data ordered by count
        print("\n" + "="*50)
        print(f"HASHTAG COUNTS AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*50)
        
        # Show hashtag counts
        counts_df = df.orderBy(col("count").desc())
        counts_df.show(n=999999, truncate=False)

    # Output the results
    query = hashtag_counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime="5 seconds") \
        .start()

    print("Starting to consume messages...")
    print("Will print counts every 5 seconds at :00, :05, :10, :15, etc.")
    
    query.awaitTermination()

except Exception as e:
    print(f"Error: {str(e)}")
    raise e