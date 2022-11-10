import os
import pandas as pd
from datetime import datetime
from datetime import timedelta
from utils import (
    get_tweets_pagination,
    get_user_tweets,
    top_hashtags,
    all_twitter_top_counts,
    hashtag_analysis,
    make_pruned_hashtag_list
)

PYTHON_EXEC = "/usr/local/bin/python3"

def main(hashtag, end_time, top_number, start_time = None):
    df_ids = get_tweets_pagination(hashtag, end_time, write_to_file=True, start_time=start_time, limit=100, user_tweet_limit=10)
    df_user_tweets = get_user_tweets(df_ids, hashtag, end_time, write_to_file=True, start_time=start_time)
    hashtags_list = make_pruned_hashtag_list(df_user_tweets, top_number)
    df_population, df_sample = top_hashtags(hashtags_list, hashtag, end_time, write_to_file=True, start_time=start_time)
    return (df_population, df_sample)

def main_final(hashtag, end_time, limit=5000):
    return get_tweets_pagination(hashtag, end_time=end_time, write_to_file=True, start_time=None, limit=limit, user_tweet_limit=10)

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))
if DEBUG:
    print("Running script in DEBUG mode")
else:
    print("Running script in FULL mode")

#################################################################
HASHTAG_LIST = ["lgbtq"]
HASHTAG = "lgbtq"
FINAL_RUN = False
TOP_NUMBER = 50
PLOT_NUMBER = 5
time_year, time_month, time_day = (2022, 11, 8)
#hours_to_check = list(range(8, 23, 4)) # 23 means 10pm will run
hours_to_check = [8,9]
#################################################################

list_df_population = []
list_df_sample = []
all_hashtag_list = []
df_sample_list = []
for h in hours_to_check:
    datetime_day = datetime(time_year, time_month, time_day) # First moment of day to check
    end_time = datetime_day + timedelta(hours=h)
    start_time = end_time + timedelta(hours=-1)
    
    df_ids = get_tweets_pagination(HASHTAG, end_time, write_to_file=True, start_time=start_time, limit=100, user_tweet_limit=10)
    df_user_tweets = get_user_tweets(df_ids, HASHTAG, end_time, write_to_file=True, start_time=start_time)
    hashtags_list = make_pruned_hashtag_list(df_user_tweets, TOP_NUMBER)
    all_hashtag_list.extend(hashtags_list)

    _df = pd.DataFrame([
        {"hashtag": h, "counts": c, "input_start_time": start_time, "input_end_time": end_time}
        for (h,c) in 
        make_pruned_hashtag_list(df_user_tweets, 10000000000, return_counts=True)
        ])
    df_sample_list.append(_df)

df_sample = pd.concat(df_sample_list)

all_hashtag_list = list(set(all_hashtag_list))

START_TIME = datetime_day + timedelta(hours=8)
END_TIME = datetime_day + timedelta(hours=22)

counts = all_twitter_top_counts(all_hashtag_list, START_TIME, END_TIME)[0:PLOT_NUMBER] # [("asdf", 275), ...]

filtered_sample = df_sample.loc[df_sample['hashtag'].isin([h for (h,_,_) in counts])]

# hashtag_analysis(filtered_sample, "debug", HASHTAG, 0, 0)

_columns = ["hashtag", "counts", "input_start_time", "input_end_time"]
population_count = [] #pd.DataFrame(columns=_columns)
for _hashtag in counts:
    hashtag_counts = [] #{'hashtag': [], 'counts': [], "input_start_time": [], "input_end_time": []} #pd.DataFrame(columns=_columns)
    hashtag = _hashtag[0]
    for count in _hashtag[2]:
        tweet_count = count['tweet_count']
        _end_time = count['end']
        _start_time = count['start']
        population_count.append([hashtag, tweet_count, _start_time, _end_time])
population_df = pd.DataFrame(population_count, columns=_columns)

hashtag_analysis(population_df, "debug_population", HASHTAG, 0, 0)