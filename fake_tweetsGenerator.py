import json
import random
import os
from faker import Faker
from tqdm import tqdm
from collections import Counter
from tabulate import tabulate

fake = Faker()

# Broad, simple hashtag topics
hashtag_pool = [
    "sports", "tech", "food", "travel", "music",
    "movies", "news", "fashion", "health", "finance"
]

hashtag_counter = Counter()

def generate_fake_tweet(tweet_id):
    hashtags = random.sample(hashtag_pool, k=random.randint(1, 4))  # 1–4 hashtags
    for tag in hashtags:
        hashtag_counter[tag] += 1
    return {
        "id": f"{tweet_id:06}",
        "text": f"{fake.sentence()} " + " ".join(f"#{tag}" for tag in hashtags),
        "username": fake.user_name(),
        "created_at": fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
        "hashtags": hashtags,
        "likes": random.randint(0, 500),
        "retweets": random.randint(0, 100)
    }

# Number of tweets
num_tweets = 10_000_000
filename = "fake_tweets.json"

# Generate tweets with progress bar
tweets = [generate_fake_tweet(i) for i in tqdm(range(1, num_tweets + 1), desc="Generating Tweets")]

# Save to file
with open(filename, "w") as f:
    json.dump(tweets, f, indent=2)

# File size in MB
file_size_mb = os.path.getsize(filename) / (1024 * 1024)

# Print file summary
print(f"\n{num_tweets} fake tweets saved to {filename}")
print(f"File size: {file_size_mb:.2f} MB")

# Format and print hashtag usage as a table
table_data = [[f"#{tag}", count] for tag, count in hashtag_counter.most_common()]
print("\nHashtag usage summary:\n")
print(tabulate(table_data, headers=["Hashtag", "Count"], tablefmt="pretty"))
