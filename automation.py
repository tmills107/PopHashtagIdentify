import os
import pause
from datetime import datetime

PYTHON_EXEC = "/usr/local/bin/python3"

now = datetime.now()
DATE_STRING = now.strftime("%Y_%m_%d_%H_%M")

def main(hashtag, date_string, hour, top_number=20, method="b"):
    if method not in ["a", "b"]:
        raise ValueError("invalid method")
    
    os.system(f"{PYTHON_EXEC} 1_get_tweets_pagination.py {hashtag} {date_string} {hour}")
    os.system(f"{PYTHON_EXEC} 2_get_user_tweets.py {hashtag} {date_string} {hour}")
    os.system(f"{PYTHON_EXEC} 3_top_hashtags.py {hashtag} {date_string} {hour} {top_number}")

    if method == "a":
        os.system(f"{PYTHON_EXEC} 4_get_final_tweets_pagination.py {hashtag} {date_string} {hour}")

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
HASHTAG_LIST = ["blacktwitter"]
METHOD = "b"
TOP_NUMBER = 20
#hours_to_check = [1,2] # 23 means 10pm will run
hours_to_check = list(range(8, 23)) # 23 means 10pm will run
#hours_to_check = [12+8]
#################################################################

print(f"Starting run with method {METHOD}, will check at hours={hours_to_check}")

for h in hours_to_check:
    if not DEBUG:
        pause.until(datetime(year, month, day, h))
    for HASHTAG in HASHTAG_LIST:
        print(f"Running at h={h} with {HASHTAG}")
        main(HASHTAG, DATE_STRING, h, top_number=TOP_NUMBER, method=METHOD)