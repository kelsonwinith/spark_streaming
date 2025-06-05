import json
import time
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
from kafka import KafkaConsumer
import numpy as np
from typing import Dict, List
import os

class BaselineProcessor:
    def __init__(self, window_size: int = 30, slide_interval: int = 5):
        """
        Initialize the baseline processor
        :param window_size: Window size in seconds
        :param slide_interval: Slide interval in seconds
        """
        self.window_size = window_size
        self.slide_interval = slide_interval
        self.data_buffer: Dict[str, List[dict]] = defaultdict(list)
        self.start_time = time.time()
        self.total_records = 0
        self.window_count = 0
        
        # Connect to Kafka
        self.consumer = KafkaConsumer(
            'market-data',
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092'),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='baseline_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def process_window(self, current_time: float) -> None:
        """Process data in the current window and calculate metrics"""
        window_start = current_time - self.window_size
        
        # Convert buffer to DataFrame for each symbol
        for symbol, data in self.data_buffer.items():
            # Filter data for current window
            window_data = [
                d for d in data 
                if window_start <= d['timestamp'] <= current_time
            ]
            
            if not window_data:
                continue

            df = pd.DataFrame(window_data)
            
            # Calculate velocity metrics
            metrics = {
                'symbol': symbol,
                'window_end': datetime.fromtimestamp(current_time),
                'volume_velocity': df['Volume'].sum() / self.window_size,
                'price_velocity': (df['Close'].mean() - df['Open'].mean()) / self.window_size,
                'trading_frequency': len(df) / self.window_size,
                'price_range': df['High'].max() - df['Low'].min(),
                'avg_price': df['Close'].mean(),
                'total_volume': df['Volume'].sum(),
                'updates_count': len(df)
            }
            
            self.print_metrics(metrics)

        # Clean up old data
        cleanup_threshold = current_time - self.window_size
        for symbol in self.data_buffer:
            self.data_buffer[symbol] = [
                d for d in self.data_buffer[symbol]
                if d['timestamp'] > cleanup_threshold
            ]

    def print_metrics(self, metrics: dict) -> None:
        """Print metrics for the current window"""
        print(f"\n=== Metrics for {metrics['symbol']} at {metrics['window_end']} ===")
        for key, value in metrics.items():
            if key not in ['symbol', 'window_end']:
                print(f"{key}: {value:.2f}")

    def print_benchmark(self) -> None:
        """Print benchmark metrics"""
        current_time = time.time()
        elapsed = current_time - self.start_time
        records_per_second = self.total_records / elapsed if elapsed > 0 else 0
        
        print(f"\n=== Benchmark Metrics at {datetime.now()} ===")
        print(f"Total Records Processed: {self.total_records}")
        print(f"Average Records/Second: {records_per_second:.2f}")
        print(f"Windows Processed: {self.window_count}")
        print(f"Elapsed Time: {elapsed:.2f} seconds")
        print("=====================================\n")

    def run(self):
        """Main processing loop"""
        print("Starting baseline velocity analysis...")
        last_window_time = time.time()

        try:
            for message in self.consumer:
                current_time = time.time()
                data = message.value
                data['timestamp'] = current_time
                
                # Add to buffer
                symbol = data['symbol']
                self.data_buffer[symbol].append(data)
                self.total_records += 1

                # Check if it's time for a new window
                if current_time - last_window_time >= self.slide_interval:
                    self.process_window(current_time)
                    self.window_count += 1
                    self.print_benchmark()
                    last_window_time = current_time

        except KeyboardInterrupt:
            print("\nStopping baseline processor")
            final_time = time.time()
            elapsed = final_time - self.start_time
            print(f"\nFinal Statistics:")
            print(f"Total Runtime: {elapsed:.2f} seconds")
            print(f"Total Records: {self.total_records}")
            print(f"Average Processing Rate: {self.total_records/elapsed:.2f} records/second")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    processor = BaselineProcessor(window_size=30, slide_interval=5)
    processor.run() 