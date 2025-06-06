import json
import time
from kafka import KafkaConsumer
from collections import Counter
from datetime import datetime, timedelta

class HashtagCounter:
    def __init__(self):
        self.hashtag_counts = Counter()
        self.next_print_time = self.get_next_print_time()
        
    def get_next_print_time(self):
        """Get the next 5-second aligned timestamp"""
        now = datetime.now()
        # Round to previous 5 seconds
        seconds = (now.second // 5) * 5
        base = now.replace(second=seconds, microsecond=0)
        # Add 5 seconds to get next print time
        return base + timedelta(seconds=5)
        
    def process_messages(self, messages):
        # Process batch of messages
        for message in messages:
            tweet = message
            # Update hashtag counts
            if tweet['hashtags']:
                self.hashtag_counts.update(tweet['hashtags'])
        
        # Check if it's time to print
        now = datetime.now()
        if now >= self.next_print_time:
            print("\n" + "="*50)
            print(f"HASHTAG COUNTS AT {self.next_print_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*50)
            
            # Sort hashtags by count and print
            for hashtag, count in sorted(self.hashtag_counts.items(), key=lambda x: (-x[1], x[0])):
                print(f"#{hashtag}: {count}")
            
            # Calculate next print time
            while self.next_print_time <= now:
                self.next_print_time += timedelta(seconds=5)

def main():
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        'tweet-stream',
        bootstrap_servers=['kafka:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='baseline_consumer_group',
        fetch_max_bytes=52428800,  # 50MB max fetch size
        max_partition_fetch_bytes=10485760  # 10MB per partition
    )
    
    # Initialize counter
    counter = HashtagCounter()
    
    print("Starting to consume messages...")
    print("Will print counts every 5 seconds at :00, :05, :10, :15, etc.")
    
    # Buffer for batch processing
    message_buffer = []
    batch_size = 5000  # Increased batch size
    
    try:
        for message in consumer:
            message_buffer.append(message.value)
            
            # Process when batch is full or it's time to print
            now = datetime.now()
            if len(message_buffer) >= batch_size or now >= counter.next_print_time:
                counter.process_messages(message_buffer)
                message_buffer = []
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main() 