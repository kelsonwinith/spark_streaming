import json
import time
import signal
import sys
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import threading
from tqdm import tqdm

class TweetProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], initial_batch_size=1000):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            batch_size=10485760,  # 10MB batches
            linger_ms=500,  # Wait up to 500ms for batches to fill
            compression_type='gzip',  # Use compression for better throughput
            buffer_memory=268435456,  # 256MB buffer memory
            max_request_size=10485760,  # 10MB max request size
            request_timeout_ms=60000,  # 60 seconds timeout
            max_block_ms=60000  # 60 seconds max blocking
        )
        self.is_running = False
        self.total_messages = 0
        self.start_time = None
        self.lock = threading.Lock()
        self.initial_batch_size = initial_batch_size
        self.current_batch_size = initial_batch_size
        self.max_batch_size = 50000  # Maximum batch size
        self.growth_interval = 30  # Seconds between batch size increases
        self.growth_factor = 1.5  # Multiply batch size by this factor
        self.last_growth_time = None
        self.last_progress_time = None
        self.progress_interval = 5  # Print progress every 5 seconds
        
        # Load tweets from file
        print("Loading tweets from file...")
        with open('fake_tweets.json', 'r') as f:
            self.tweets = json.load(f)
        print(f"Loaded {len(self.tweets):,} tweets")
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("\nReceived shutdown signal. Closing gracefully...")
        self.stop()

    def increment_messages(self, count=1):
        with self.lock:
            self.total_messages += count

    def get_next_print_time(self):
        """Get the next 5-second aligned timestamp"""
        now = datetime.now()
        seconds = (now.second // 5) * 5
        base = now.replace(second=seconds, microsecond=0)
        return base + timedelta(seconds=5)

    def print_progress(self, total_tweets_to_send):
        """Print progress at 5-second intervals"""
        current_time = datetime.now()
        if (self.last_progress_time is None or 
            current_time >= self.last_progress_time):
            
            runtime = time.time() - self.start_time
            rate = self.total_messages / runtime if runtime > 0 else 0
            remaining = total_tweets_to_send - self.total_messages
            percent_complete = (self.total_messages / total_tweets_to_send) * 100
            
            print("\n" + "="*50)
            print(f"PROGRESS UPDATE AT {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*50)
            print(f"Messages Sent: {self.total_messages:,}")
            print(f"Messages Remaining: {remaining:,}")
            print(f"Progress: {percent_complete:.1f}%")
            print(f"Current Rate: {rate:.0f} messages/second")
            print(f"Current Batch Size: {self.current_batch_size:,}")
            
            # Calculate next print time
            while self.last_progress_time is None or current_time >= self.last_progress_time:
                if self.last_progress_time is None:
                    self.last_progress_time = self.get_next_print_time()
                else:
                    self.last_progress_time += timedelta(seconds=5)

    def update_batch_size(self):
        """Increase batch size over time"""
        current_time = time.time()
        if self.last_growth_time is None:
            self.last_growth_time = current_time
            return

        if current_time - self.last_growth_time >= self.growth_interval:
            old_batch_size = self.current_batch_size
            self.current_batch_size = min(
                int(self.current_batch_size * self.growth_factor),
                self.max_batch_size
            )
            self.last_growth_time = current_time
            
            if old_batch_size != self.current_batch_size:
                print(f"\nIncreasing batch size from {old_batch_size:,} to {self.current_batch_size:,}")

    def prepare_batch(self, start_idx, batch_size, num_tweets):
        """Prepare a batch of tweets with timestamps"""
        batch = []
        current_time = datetime.now()
        
        for i in range(batch_size):
            idx = (start_idx + i) % num_tweets
            tweet = self.tweets[idx].copy()
            
            # Add streaming metadata
            tweet['streaming_timestamp'] = current_time.isoformat()
            tweet['batch_id'] = f"{current_time.timestamp()}-{i}"
            
            # Add engagement metrics
            tweet['total_likes'] = tweet['likes'] + (i % 100)
            tweet['total_retweets'] = tweet['retweets'] + (i % 50)
            tweet['viral_score'] = (tweet['total_likes'] + tweet['total_retweets']) / 1000
            
            batch.append(tweet)
            
        return batch

    def send_batch(self, batch):
        """Send a batch of tweets to Kafka"""
        try:
            # Send all messages in the batch
            futures = []
            for tweet in batch:
                future = self.producer.send('tweet-stream', tweet)
                futures.append(future)
            
            # Wait for all messages to be sent
            for future in futures:
                future.get(timeout=60)
            
            self.increment_messages(len(batch))
            return True
            
        except KafkaError as e:
            print(f"Error sending batch: {str(e)}")
            return False

    def print_stats(self):
        if self.start_time:
            runtime = time.time() - self.start_time
            rate = self.total_messages / runtime if runtime > 0 else 0
            
            print("\n==================================================")
            print("Final Statistics:")
            print(f"Total Messages Sent: {self.total_messages:,}")
            print(f"Total Runtime: {runtime:.2f} seconds")
            print(f"Average Rate: {rate:.2f} messages/second")
            print(f"Final Batch Size: {self.current_batch_size:,}")
            print("==================================================")

    def stop(self):
        self.is_running = False
        if hasattr(self, 'producer'):
            self.producer.flush()
            self.producer.close()
        self.print_stats()

    def start(self):
        """Start streaming with increasing batch sizes"""
        self.is_running = True
        self.start_time = time.time()
        self.last_growth_time = self.start_time
        tweet_index = 0
        num_tweets = len(self.tweets)
        total_tweets_to_send = num_tweets

        print(f"Starting tweet stream with initial batch size of {self.initial_batch_size:,}...")
        print(f"Total tweets to send: {total_tweets_to_send:,}")
        print(f"Batch size will increase by {(self.growth_factor-1)*100:.0f}% every {self.growth_interval} seconds")
        print(f"Maximum batch size: {self.max_batch_size:,}")
        print(f"Progress updates every {self.progress_interval} seconds")
        
        try:
            while self.is_running and self.total_messages < total_tweets_to_send:
                # Update batch size based on time
                self.update_batch_size()
                
                # Print progress at intervals
                self.print_progress(total_tweets_to_send)
                
                remaining = total_tweets_to_send - self.total_messages
                current_batch_size = min(self.current_batch_size, remaining)
                
                if current_batch_size <= 0:
                    break
                
                # Prepare and send batch
                batch = self.prepare_batch(tweet_index, current_batch_size, num_tweets)
                if self.send_batch(batch):
                    tweet_index = (tweet_index + current_batch_size) % num_tweets
                    
            print("\nFinished sending all tweets!")
            self.stop()
                    
        except Exception as e:
            print(f"Error in producer: {str(e)}")
            self.stop()

def main():
    # Create and start the producer
    producer = TweetProducer(
        bootstrap_servers=['kafka:9092'],
        initial_batch_size=1000  # Start with smaller batches
    )
    
    try:
        producer.start()
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt. Shutting down...")
    finally:
        producer.stop()

if __name__ == "__main__":
    main()