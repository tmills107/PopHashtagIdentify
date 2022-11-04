import tweepy
import os
import pandas as pd
from datetime import datetime
from datetime import timedelta

bearer_token = os.environ.get('BEARER_TOKEN')

client = tweepy.Client(bearer_token = bearer_token,
    return_type=dict)

#################################################################
hashtag_list = ["takeoff", "blacktwitter", "kanyewest", "blacklivesmatter", "halloween", "gay", "lgbtq", "lgbt", "pride", "voteblue", "vote", "bluewave", "bluecrew", "votebluetoprotectyourrights", "trump", "maga", "elonmusk", "gop", "biden"]
time_year, time_month, time_day = (2022, 11, 1)
#################################################################

def query_hashtag_counts(_hashtag, _start_time, _end_time):
        hashtag_count_hourly = []
        count = client.get_recent_tweets_count(query= "#" + _hashtag,
            granularity = 'hour',
            start_time=_start_time,
            end_time=_end_time)
        
        for item in count['data']:
            __start_time = datetime.strftime(datetime.strptime(item['start'], '%Y-%m-%dT%H:%M:%S.%f%z'), '%Y_%m_%d_%H')
            __end_time = datetime.strftime(datetime.strptime(item['end'], '%Y-%m-%dT%H:%M:%S.%f%z'), '%Y_%m_%d_%H')
            __count = item['tweet_count']
            hashtag_count_hourly.append([hashtag, __start_time, __end_time, __count])
        df = pd.DataFrame(hashtag_count_hourly, columns = ["hashtag", "start time", "end time", "count"])
        return df


datetime_day = datetime(time_year, time_month, time_day) # First moment of day to check
ENDTIME = datetime_day + timedelta(hours=22)
STARTTIME = datetime_day + timedelta(hours=8)

all_df =[]

for hashtag in hashtag_list:
    df = query_hashtag_counts(hashtag, STARTTIME, ENDTIME)
    all_df.append(df)

all_df = pd.concat(all_df)

all_df.to_csv('hashtag_daily_count.csv', index=False)
