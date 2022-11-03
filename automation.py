import os
import pause
from datetime import datetime
from datetime import timedelta
from utils import (
    get_tweets_pagination,
    get_user_tweets,
    top_hashtags
)

PYTHON_EXEC = "/usr/local/bin/python3"

def main(hashtag, end_time, top_number, start_time = None):
    df_ids = get_tweets_pagination(hashtag, end_time, write_to_file=True, start_time=start_time, limit=500)
    df_user_tweets = get_user_tweets(df_ids, hashtag, end_time, write_to_file=True, start_time=start_time)
    df_all_twitter, df_sample = top_hashtags(df_user_tweets, hashtag, end_time, top_number, write_to_file=True, start_time=start_time)

def main_final(hashtag, end_time, limit=5000):
    return get_tweets_pagination(hashtag, end_time=end_time, write_to_file=True, start_time=None, limit=limit)

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))
if DEBUG:
    print("Running script in DEBUG mode")
else:
    print("Running script in FULL mode")

#################################################################
HASHTAG_LIST = ["blacklivesmatter"]
FINAL_RUN = False
TOP_NUMBER = 5
time_year, time_month, time_day = (2022, 10, 31)
hours_to_check = [9, 11] # 23 means 10pm will run
#hours_to_check = list(range(8, 23)) # 23 means 10pm will run
#################################################################

if not FINAL_RUN:
    for h in hours_to_check:
        for HASHTAG in HASHTAG_LIST:
            datetime_day = datetime(time_year, time_month, time_day) # First moment of day to check
            end_time = datetime_day + timedelta(hours=h)
            #start_time = end_time + timedelta(hours=-1)
            main(HASHTAG, end_time, top_number=TOP_NUMBER, start_time=None)
else:
    for HASHTAG in HASHTAG_LIST:
        datetime_day = datetime(time_year, time_month, time_day)
        end_time = datetime_day + timedelta(hours = 12+10)
        main_final(HASHTAG, end_time, limit=5000)