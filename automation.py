import os
import pandas as pd
import pause
from datetime import datetime
from datetime import timedelta
from utils import (
    get_tweets_pagination,
    get_user_tweets,
    top_hashtags,
    hashtag_analysis
)

PYTHON_EXEC = "/usr/local/bin/python3"

def main(hashtag, end_time, top_number, start_time = None):
    df_ids = get_tweets_pagination(hashtag, end_time, write_to_file=True, start_time=start_time, limit=100)
    df_user_tweets = get_user_tweets(df_ids, hashtag, end_time, write_to_file=True, start_time=start_time)
    df_population, df_sample = top_hashtags(df_user_tweets, hashtag, end_time, top_number, write_to_file=True, start_time=start_time)
    return (df_population, df_sample)

def main_final(hashtag, end_time, limit=5000):
    return get_tweets_pagination(hashtag, end_time=end_time, write_to_file=True, start_time=None, limit=limit)

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))
if DEBUG:
    print("Running script in DEBUG mode")
else:
    print("Running script in FULL mode")

#################################################################
HASHTAG_LIST = ["blacklivesmatter"]
FINAL_RUN = True
TOP_NUMBER = 5
time_year, time_month, time_day = (2022, 11, 1)
hours_to_check = list(range(8, 23)) # 23 means 10pm will run
#hours_to_check = [8,9,10,11,12,13,14]
#################################################################

if not FINAL_RUN:
    for HASHTAG in HASHTAG_LIST:
        list_df_population = []
        list_df_sample = []
        for h in hours_to_check:
            datetime_day = datetime(time_year, time_month, time_day) # First moment of day to check
            end_time = datetime_day + timedelta(hours=h)
            #start_time = end_time + timedelta(hours=-1)
            (df_population, df_sample) = main(HASHTAG, end_time, top_number=TOP_NUMBER, start_time=None)
            list_df_population.append(df_population)
            list_df_sample.append(df_sample)
        df_final_population = pd.concat(list_df_population)
        df_final_sample = pd.concat(list_df_sample)
        # print("\n\n")
        # print("Population:")
        # print(df_final_population)
        # print("\n\n")
        # print("Sample:")
        # print(df_final_sample)
        hashtag_analysis(df_final_population, f"output/{time_month}_{time_day}_{HASHTAG}_population", HASHTAG, time_month, time_day)
        hashtag_analysis(df_final_sample, f"output/{time_month}_{time_day}_{HASHTAG}_sample", HASHTAG, time_month, time_day)
else:
    for HASHTAG in HASHTAG_LIST:
        datetime_day = datetime(time_year, time_month, time_day)
        end_time = datetime_day + timedelta(hours = 12+10)
        main_final(HASHTAG, end_time, limit=5000)