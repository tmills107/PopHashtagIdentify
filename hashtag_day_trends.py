import tweepy
import collections
import os
import pandas as pd
from datetime import datetime

bearer_token = os.environ.get('BEARER_TOKEN')

client = tweepy.Client(bearer_token = bearer_token,
    return_type=dict)

hashtag_list = ["trump", "biden", "maga", "gop", "republicans"]

hashtag_count=[]

STARTTIME = '2022-10-25T08:00:00-04:00'
ENDTIME = '2022-10-25T22:00:00-04:00'

for hashtag in hashtag_list:
    count = client.get_recent_tweets_count(query= "#" + hashtag,
        granularity = 'hour',
        start_time = STARTTIME,
        end_time = ENDTIME)
    
    tweet_count_by_hour = []
    hour = 8
    item_time_count = 00
    for item in count['data']:
        tweet_count = count['data'][item_time_count]['tweet_count']
        tweet_count_by_hour.append((hour,tweet_count))
        hour = hour+1 
        item_time_count= item_time_count + 1
    
    total_tweet_count = count['meta']['total_tweet_count']

    hashtag_count.append((hashtag, tweet_count_by_hour, total_tweet_count))

sorted_hashtag_count = sorted(hashtag_count, key = lambda kv:kv[2], reverse=True)

df_all_twitter = pd.DataFrame(sorted_hashtag_count, columns=["hashtag", "Hourly" , "Total"])
df_all_twitter.sort_values("Total", inplace=True, ascending=False)
df_all_twitter.to_csv('./data/trump_all_twitter_hashtag_count_top.csv', index=False)