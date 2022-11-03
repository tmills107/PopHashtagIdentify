import os
import pause
from datetime import datetime
from datetime import timedelta
from utils import (
    get_tweets_pagination,
    get_user_tweets,
    top_hashtags,
    get_final_tweets_pagination
)

PYTHON_EXEC = "/usr/local/bin/python3"

now = datetime.now()

def main(hashtag, end_time, top_number, method="b", start_time = None):
    if method not in ["a", "b"]:
        raise ValueError("invalid method")
    
    df_ids = get_tweets_pagination(hashtag, end_time, write_to_file=True, start_time=start_time)
    df_user_tweets = get_user_tweets(df_ids, hashtag, end_time, write_to_file=True, start_time=start_time)
    df_all_twitter, df_sample = top_hashtags(df_user_tweets, hashtag, end_time, top_number, write_to_file=True, start_time=start_time)

    if method == "a":
        tweets_df = get_final_tweets_pagination(df_all_twitter, hashtag, end_time, write_to_file=True, override_hashtag=None, start_time=start_time)

now = datetime.now()
year = int(now.strftime("%Y"))
month = int(now.strftime("%m"))
day = int(now.strftime("%d"))

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))
if DEBUG:
    print("Running script in DEBUG mode")
else:
    print("Running script in FULL mode")

#################################################################
HASHTAG_LIST = ["blacklivesmatter"]
METHOD = "a"
TOP_NUMBER = 5
time_year, time_month, time_day = (2022, 10, 31)
hours_to_check = [9, 11] # 23 means 10pm will run
#hours_to_check = list(range(8, 23)) # 23 means 10pm will run
#################################################################

#print(f"Starting run with method {METHOD}, will check at hours={hours_to_check}")
#for h in hours_to_check:
#    if not DEBUG:
#        pause.until(datetime(year, month, day, h))
#    for HASHTAG in HASHTAG_LIST:
#        print(f"Running at h={h} with {HASHTAG}")
#        main(HASHTAG, now, top_number=TOP_NUMBER, method=METHOD)

for h in hours_to_check:
    for HASHTAG in HASHTAG_LIST:
        datetime_day = datetime(time_year, time_month, time_day) # First moment of day to check
        end_time = datetime_day + timedelta(hours=h)
        #start_time = end_time + timedelta(hours=-1)
        main(HASHTAG, end_time, top_number=TOP_NUMBER, method=METHOD, start_time=None)