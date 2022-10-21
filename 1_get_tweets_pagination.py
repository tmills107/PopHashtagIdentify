from datetime import datetime
import pandas as pd
import tweepy
import os

UNIQUE_USER_IDS = True

if False:
    HASHTAG = '#blacktwitter'
    now = datetime.now()
    date_string = now.strftime("%Y_%m_%d_%H_%M")
    HOUR = now.strftime("%H")
else:
    import argparse
    parser = argparse.ArgumentParser(description="Run my script")
    parser.add_argument("hashtag", type=str)
    parser.add_argument("timestamp", type=str)
    parser.add_argument("hour", type=str)
    HASHTAG = parser.parse_args().hashtag
    date_string = parser.parse_args().timestamp
    HOUR = parser.parse_args().hour

file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"

bearer_token = os.environ.get('BEARER_TOKEN')
assert bearer_token != None, "Remember to set API credentials as environment variables first!"

QUERY = HASHTAG + ' -is:retweet -is:quote'

client = tweepy.Client(wait_on_rate_limit = True, 
                    bearer_token = bearer_token)

tweets = []
tweets_df = []

for tweet in tweepy.Paginator(client.search_recent_tweets, 
                    query=QUERY,
                    tweet_fields=['created_at', 'entities', 'public_metrics'],
                    media_fields=['url'],
                    user_fields=['description'],
                    expansions=['attachments.media_keys', 'author_id'],
                    max_results = 100).flatten(limit=500):
    tweets.append(tweet)

# Go through each tweet from each user and oragnize the data into coloumns for export. 
for item in tweets: #removed ['data']
    if "entities" not in item:
        continue
    if "hashtags" in item["entities"]:
        hashtags = [d["tag"].lower() for d in item["entities"]["hashtags"]]
    else:
        hashtags = []

    tweet_data = {
            'tweet_id': item['id'],
            'author_id': item['author_id'],
            'text': item['text'],
            'hashtags': hashtags,
            'created_at': item['created_at'],
            'mined_at': datetime.now(),
            'like_count': item['public_metrics']['like_count'],
            'quote_count': item['public_metrics']['quote_count'],
            'reply_count': item['public_metrics']['reply_count'],
            'retweet_count': item['public_metrics']['retweet_count']
    }     
    tweets_df.append(tweet_data)

user_tweets_dfs = pd.DataFrame(tweets_df)

user_tweets_dfs.to_csv(file_name_prefix + 'tweets.csv', index=False)