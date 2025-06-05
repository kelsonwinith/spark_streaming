import os
import json
import time
import signal
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "market-data"
SYMBOLS = [f"SYM{i:03}" for i in range(500)]  # Generate 500 stock symbols

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True

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

def generate_fake_data(symbol):
    """Simulate a fake stock tick for a given symbol."""
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

def print_final_stats(messages_sent, start_time):
    """Print final statistics."""
    elapsed = time.time() - start_time
    rate = messages_sent / elapsed if elapsed > 0 else 0
    print("\n" + "="*50)
    print("Final Statistics:")
    print(f"Total Messages Sent: {messages_sent:,}")
    print(f"Total Runtime: {elapsed:.2f} seconds")
    print(f"Average Rate: {rate:.2f} messages/second")
    print("="*50 + "\n")

def main():
    killer = GracefulKiller()
    producer = connect_kafka()
    messages_sent = 0
    start_time = time.time()

    print("🚀 Starting high-throughput fake stock price stream to Kafka...")

    try:
        while not killer.kill_now:
            batch_start = time.time()
            
            # Generate and send data for all symbols
            for symbol in SYMBOLS:
                if killer.kill_now:
                    break
                data = generate_fake_data(symbol)
                producer.send(TOPIC, data)
            
            producer.flush()  # Ensure all messages are sent
            messages_sent += len(SYMBOLS)
                        
            # Wait until next second if needed
            processing_time = time.time() - batch_start
            if processing_time < 1.0 and not killer.kill_now:
                time.sleep(1.0 - processing_time)

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    finally:
        print("\n🛑 Producer stopping...")
        producer.flush()
        producer.close()
        print_final_stats(messages_sent, start_time)
        print("✅ Producer shut down cleanly.")

if __name__ == "__main__":
    main()