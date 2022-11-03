import os
import pause
from datetime import datetime
from utils import (
    get_tweets_pagination,
    get_user_tweets,
    top_hashtags,
    get_final_tweets_pagination
)

now = datetime.now()
TIMESTAMP = now.strftime("%Y_%m_%d_%H_%M")

def standard(hashtag, timestamp, hour, top_number, sample_limit):
    get_tweets_pagination(hashtag, timestamp, hour, sample_limit, write_to_file=True)
    get_user_tweets(hashtag, timestamp, hour, write_to_file=True)
    top_hashtags(hashtag, timestamp, hour, top_number, write_to_file=True)

def option_finale(hashtag, timestamp, hour, final_limit):
    get_final_tweets_pagination(hashtag, timestamp, hour, final_limit, write_to_file=True)

now = datetime.now()
year = int(now.strftime("%Y"))
month = int(now.strftime("%m"))
day = int(now.strftime("%d"))

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))
if DEBUG:
    print("Running script in DEBUG mode")
else:
    print("Running script in FULL mode")

################################################################################################################
method = "a" # "a" means standard, "b" means running option for all day 
HASHTAG_LIST = ["blacklivesmatter"]
TOP_NUMBER = 5
SAMPLE_LIMIT = 500
FINAL_LIMIT = 5000
################################################################################################################

if method == "a":
    hours_to_check = [1]
if method == "b":
    hours_to_check = list(range(8, 23)) # 23 means 10pm will run

print(f"Starting to run with option {method}, will check at hours={hours_to_check}")

if method == "a":
    for h in hours_to_check:
        if not DEBUG:
                pause.until(datetime(year, month, day, h))
        for HASHTAG in HASHTAG_LIST:
                print(f"Running at h={h} with {HASHTAG}")
                standard(HASHTAG, TIMESTAMP, h, top_number=TOP_NUMBER, limit=SAMPLE_LIMIT)

if method=="b":
    for h in hours_to_check:
            if not DEBUG:
                    pause.until(datetime(year, month, day, h))
            for HASHTAG in HASHTAG_LIST:
                    print(f"Running at h={h} with {HASHTAG}")
                    standard(HASHTAG, TIMESTAMP, h, top_number=TOP_NUMBER, limit=SAMPLE_LIMIT)

    pause.until(datetime(year, month, day, 11))
    option_finale(FINAL_HASHTAG, TIMESTAMP, h, FINAL_LIMIT)




