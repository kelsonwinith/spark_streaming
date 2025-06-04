from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, sum
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType
import yfinance as yf
from datetime import datetime
import pandas as pd
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("StockStreamingAnalysis") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def get_stock_data(symbol, interval='1m'):
    """Fetch stock data using yfinance"""
    try:
        stock = yf.Ticker(symbol)
        # Get 1 minute data for the last hour
        data = stock.history(period='1d', interval=interval)
        data = data.reset_index()

        result = []
        for _, row in data.iterrows():
            if pd.notna(row['Datetime']) and all(pd.notna([row['Open'], row['High'], row['Low'], row['Close'], row['Volume']])):
                result.append((
                    row['Datetime'].to_pydatetime(),  # Ensure it's a Python datetime
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    float(row['Volume'])
                ))
        return result
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return []

def main():
    # Create Spark session
    spark = create_spark_session()

    # Define schema for stock data
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])

    symbols = ['AAPL', 'MSFT', 'GOOGL']

    try:
        while True:
            all_data = []
            for symbol in symbols:
                stock_data = get_stock_data(symbol)
                all_data.extend(stock_data)

            # Validate data format
            valid_data = [
                row for row in all_data
                if isinstance(row[0], datetime) and all(isinstance(x, (int, float)) for x in row[1:])
            ]

            if valid_data:
                df = spark.createDataFrame(valid_data, schema)
                df.createOrReplaceTempView("stock_data")

                windowed_stats = df \
                    .groupBy(window("timestamp", "5 minutes", "1 minute")) \
                    .agg(
                        avg("close").alias("avg_price"),
                        sum("volume").alias("total_volume")
                    )

                logger.info("\n=== Stock Statistics for Last 5 Minutes (Sliding Every 1 Minute) ===")
                windowed_stats.orderBy("window").show(truncate=False)
            else:
                logger.warning("No valid stock data available this cycle.")

            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("Stopping the streaming application...")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
