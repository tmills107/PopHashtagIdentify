from datetime import datetime
import pandas as pd
import tweepy
import os

UNIQUE_USER_IDS = True

if False:
    HASHTAG      = '#blacktwitter'
    now = datetime.now()
    date_string = '2022_10_14_09_19' #now.strftime("%Y_%m_%d_%H_%M")
    HOUR = '09' #now.strftime("%H")
else:
    import argparse
    parser = argparse.ArgumentParser(description="Run my script")
    parser.add_argument("hashtag", type=str)
    parser.add_argument("timestamp", type=str)
    parser.add_argument("hour", type=str)
    HASHTAG = parser.parse_args().hashtag
    date_string = parser.parse_args().timestamp
    HOUR = parser.parse_args().hour

file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"


# Get authentication information from the shell environment.
bearer_token = os.environ.get('BEARER_TOKEN')


# If environment variable isn't defined, a reminder pops up.
assert bearer_token != None, "Remember to set API credentials as environment variables first!"

# Create .Client() object that will let us access the full archive.
client = tweepy.Client(wait_on_rate_limit = True, bearer_token = bearer_token,
                       return_type=dict)

# Read tweets.csv for IDs and convert it into a list of IDs
ids = pd.read_csv(file_name_prefix + 'tweets.csv', usecols=['author_id'], index_col=False)
ids_df = pd.DataFrame(ids)
id_list = ids_df['author_id'].values.tolist()

if UNIQUE_USER_IDS:
  id_list = list(set(id_list))

tweets_df = []

for curr_user in id_list:
  # For each user, you retrieve the most recent 10 tweets that they posted.
  usertweets = client.get_users_tweets(id = curr_user,
                                      exclude=['retweets','replies'],
                                      tweet_fields=['created_at','entities','public_metrics',],                                      
                                      media_fields=['url'],
                                      user_fields=['description'],
                                      expansions=['attachments.media_keys','author_id'],
                                      max_results = 10) 

  # Potentially some users accounts will either be deleted or suspended so this bypasses those users/tweets with no data. 
  if "data" not in usertweets:
    continue

  # Go through each tweet from each user and oragnize the data into coloumns for export. 
  for item in usertweets['data']: 
      
      # If a tweet has no "entities" data it is bypassed
      if "entities" not in item:
        continue
      # Creates a variable hashtags and is filled with the "tag" data in each case. If there are no hashtags it is left empty. This is done because tweepy could not do this on its own. 
      if "hashtags" in item["entities"]:
        hashtags = [d["tag"].lower() for d in item["entities"]["hashtags"]]
      else:
        hashtags = []
      
      tweet_data = {
                  'tweet_id': item['id'],
                  'author_id': item['author_id'],
                  'text': item['text'],
                  'hashtags': hashtags,
                  'created_at': item['created_at'],
                  'mined_at': datetime.now(),
                  'like_count': item['public_metrics']['like_count'],
                  'quote_count': item['public_metrics']['quote_count'],
                  'reply_count': item['public_metrics']['reply_count'],
                  'retweet_count': item['public_metrics']['retweet_count']
        }     
      tweets_df.append(tweet_data)

tweets_dfs = pd.DataFrame(tweets_df)

# Save dfs to data/.
tweets_dfs.to_csv(file_name_prefix + 'identified_user_tweets.csv', index=False)
