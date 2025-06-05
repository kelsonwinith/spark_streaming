import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "market-data"
SYMBOLS = [f"SYM{i:03}" for i in range(100)]  # Simulate 100 stock symbols


def connect_kafka(max_retries=10, delay=5):
    """Try to connect to Kafka with retries."""
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("✅ Connected to Kafka")
            return producer
        except NoBrokersAvailable:
            print(f"⚠️ Kafka not available (attempt {attempt + 1}/{max_retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("❌ Could not connect to Kafka after retries.")


def generate_fake_data():
    """Simulate a fake stock tick."""
    symbol = random.choice(SYMBOLS)
    base_price = random.uniform(100, 500)
    open_price = round(base_price, 2)
    high = round(open_price * random.uniform(1.00, 1.05), 2)
    low = round(open_price * random.uniform(0.95, 1.00), 2)
    close = round(random.uniform(low, high), 2)
    volume = random.randint(1000, 50000)

    return {
        "Datetime": datetime.utcnow().isoformat(),
        "Open": open_price,
        "High": high,
        "Low": low,
        "Close": close,
        "Volume": volume,
        "Dividends": 0.0,
        "Stock Splits": 0,
        "symbol": symbol
    }


def main():
    producer = connect_kafka()

    print("🚀 Starting high-throughput fake stock price stream to Kafka...")

    try:
        while True:
            data = generate_fake_data()
            producer.send(TOPIC, data)
            time.sleep(0.01)  # 100 messages per second (adjust as needed)

    except KeyboardInterrupt:
        print("🛑 Producer stopped by user.")
    finally:
        producer.flush()
        producer.close()
        print("✅ Producer shut down cleanly.")


if __name__ == "__main__":
    main()