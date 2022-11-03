import os
from datetime import datetime
import pandas as pd
import tweepy

# BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAAB2AZAEAAAAAKTgvleV%2BvXsTWFRxYtlLhYgh9%2Bg%3DRCnTpTCOYD4hHMUuEIXA2mdOstUkbZhxSdL9Ox4znWuuXY7Z9v"

HASHTAG = "blm"
date_string = "2022_10_25_22_00"
HOUR = 10

STARTDATE = "2022-10-25T08:00:00-04:00"
ENDDATE = "2022-10-25T22:00:00-04:00"

file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"

bearer_token = os.environ.get('BEARER_TOKEN')

client = tweepy.Client(wait_on_rate_limit = True, bearer_token = bearer_token, return_type=dict)

tweets_df = []

QUERY = '#' + HASHTAG + ' -is:retweet -is:quote lang:en' 

client = tweepy.Client(wait_on_rate_limit = True, 
                            bearer_token = bearer_token)

# for hashtag in HASHTAG:
#     count_query = '#' + hashtag + ' -is:retweet -is:quote lang:en'

#     hashtag_count = client.get_recent_tweets_count(query=count_query,
#         granularity = 'day',
#         end_time = ENDDATE,
#         start_time = STARTDATE,
#         )
#     hashtag_total_count_object = hashtag_count
#     print(f"Hashtag: {hashtag} Number of Unique Tweets over the past day: {hashtag_total_count_object}")

my_paginator = tweepy.Paginator(client.search_recent_tweets, 
    query=QUERY,
    tweet_fields=['created_at', 'entities', 'public_metrics'],
    media_fields=['url'],
    user_fields=['description'],
    expansions=['attachments.media_keys', 'author_id'],
    end_time = ENDDATE,
    max_results = 100).flatten(limit=5000)

tweets_df = [] 

for item in my_paginator:
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

user_tweets_dfs.to_csv(file_name_prefix + 'final_tweets.csv')
