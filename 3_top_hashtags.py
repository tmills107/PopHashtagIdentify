import tweepy
import collections
import os
import pandas as pd
from datetime import datetime

UNIQUE_USER_HASHTAGS_COUNT = True
HASHTAG_COUNT_LIMIT = 15

if False:
    HASHTAG      = '#blacktwitter'
    now = datetime.now()
    date_string = '2022_10_14_09_19' #now.strftime("%Y_%m_%d_%H_%M")
    HOUR = '09' #now.strftime("%H")
    top_number = 5
else:
    import argparse
    parser = argparse.ArgumentParser(description="Run my script")
    parser.add_argument("hashtag", type=str)
    parser.add_argument("timestamp", type=str)
    parser.add_argument("hour", type=str)
    parser.add_argument("top_number", type=int)
    HASHTAG = "#" + parser.parse_args().hashtag
    date_string = parser.parse_args().timestamp
    HOUR = parser.parse_args().hour
    top_number = parser.parse_args().top_number

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))

if DEBUG:
  top_number = 1

if DEBUG:
  file_name_prefix = f"./data/debug_{HASHTAG}_{date_string}_{HOUR}_"
else:
  file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"


# Get authentication information from the shell environment.
bearer_token = os.environ.get('BEARER_TOKEN')

# If environment variable isn't defined, a reminder pops up.
assert bearer_token != None, "Remember to set API credentials as environment variables first!"

# Create .Client() object that will let us access the full archive.
client = tweepy.Client(bearer_token = bearer_token,
                       return_type=dict)

# Opens the csv file created in the last script of the identified user tweets. 
hashtags_df = pd.read_csv(file_name_prefix + 'identified_user_tweets.csv', index_col=False)

# Removes the quotations created when python reads the hashtag items as strings. 
hashtags_df["hashtags"] = hashtags_df["hashtags"].map(eval)

hashtags_list = []

for author_id, group in hashtags_df.groupby("author_id"):
  _filtered_hashtags = list(group["hashtags"])
  _filtered_hashtags = filter(lambda ht_l: len(ht_l) < HASHTAG_COUNT_LIMIT, _filtered_hashtags)

  if UNIQUE_USER_HASHTAGS_COUNT:
    _hash_list = list(set([item for sublist in _filtered_hashtags for item in sublist]))
  else:
    _hash_list = [item for sublist in _filtered_hashtags for item in sublist]
  
  hashtags_list.extend(_hash_list)

# Finds the most common used hashtags (case sensitive) from the list.
counts = collections.Counter(hashtags_list).most_common(top_number)

if len(counts) == 0:
  raise ValueError("No hashtags found")

final_hashtagcount = []

# Queries Twitter to find the count of the tweets for each of the 5 hashtags identified over the past 7 days. 
for hashtag, c in counts:
  hashtagcount = client.get_recent_tweets_count(query=hashtag,
                                                granularity = 'day')
  total_tweet_count = hashtagcount['meta']['total_tweet_count']
  final_hashtagcount.append((hashtag, total_tweet_count))

#Sorts the Hashtags (and counts) in decending order of count
sorted_hashtag_count_final = sorted(final_hashtagcount, key = lambda kv:kv[1], reverse=True)


df_sample = pd.DataFrame(counts, columns=["hashtag", "counts"])
df_sample.sort_values("counts", inplace=True, ascending=False)
df_sample.to_csv(file_name_prefix + 'sample_hashtag_count_top.csv', index=False)

df_all_twitter = pd.DataFrame(sorted_hashtag_count_final, columns=["hashtag", "counts"])
df_all_twitter.sort_values("counts", inplace=True, ascending=False)
df_all_twitter.to_csv(file_name_prefix + 'all_twitter_hashtag_count_top.csv', index=False)