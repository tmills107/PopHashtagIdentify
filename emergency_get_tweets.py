import os
from datetime import datetime
import pandas as pd
import tweepy

# BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAAB2AZAEAAAAAKTgvleV%2BvXsTWFRxYtlLhYgh9%2Bg%3DRCnTpTCOYD4hHMUuEIXA2mdOstUkbZhxSdL9Ox4znWuuXY7Z9v"

HASHTAG = "lgbtq"
date_string = "2022_11_07_22_00"
HOUR = 10

STARTDATE = "2022-11-07T08:00:00-04:00"
ENDDATE = "2022-11-07T22:00:00-04:00"

file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"

bearer_token = os.environ.get('BEARER_TOKEN')

client = tweepy.Client(wait_on_rate_limit = True, bearer_token = bearer_token, return_type=dict)

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
    user_fields=['description', 'public_metrics'],
    expansions=['author_id'],
    end_time = ENDDATE,
    max_results = 10,
    limit = 2)

tweets_df = []
for page in my_paginator:
    for tweet in page:
        users = {u["id"]: u for u in page.includes['users']} 
        for tweet in page.data:
            if users[tweet.author_id]:
                user = users[tweet.author_id]
                follower_count = user.public_metrics['followers_count']
            tweet['data']['follower_count'] = int(follower_count)
    tweets_df.append(page)

tweets = []
for page in tweets_df:
    for item in page:
        tweets.append(item)

tweets = pd.DataFrame(tweets)
print(tweets)

tweets_data = []
for item in tweets_df:
    if "entities" not in item:
        continue
    if "hashtags" in item["entities"]:
        hashtags = [d["tag"].lower() for d in item["entities"]["hashtags"]]
    else:
        hashtags = []

    tweet_data = {
        'tweet_id': item['id'],
        'author_id': item['author_id'],
        'follower_count': item['follower_count'],
        'text': item['text'],
        'hashtags': hashtags,
        'created_at': item['created_at'],
        'mined_at': datetime.now(),
        'like_count': item['public_metrics']['like_count'],
        'quote_count': item['public_metrics']['quote_count'],
        'reply_count': item['public_metrics']['reply_count'],
        'retweet_count': item['public_metrics']['retweet_count']
    }     
    tweets_data.append(tweet_data)

user_tweets_df = pd.DataFrame(tweets_data)
print(user_tweets_df)

user_tweets_df.to_csv(file_name_prefix + 'final_tweets.csv')
