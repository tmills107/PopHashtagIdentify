import os
import pause
import collections
from datetime import datetime
import pandas as pd
import tweepy

API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT"))
API_RETRY_WAIT_MINS = int(os.getenv("API_RETRY_WAIT_MINS"))
DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))
UNIQUE_USER_IDS = True
UNIQUE_USER_HASHTAGS_COUNT = True
HASHTAG_COUNT_LIMIT = 15

########################################################
########################################################
########################################################
########################################################

def make_timestring(d: datetime):
  return d.strftime("%Y_%m_%d_%H_%M")

def retry_query(func):
    def wrapper():
        current_retries = 0
        retry_success = False
        while (current_retries < API_RETRY_COUNT) and (not retry_success):
            try:
                res = func()
                retry_success = True
                return res
            except Exception as e:
                print("Failed, trying again")
                print(e)
                current_retries += 1
                if DEBUG:
                    pause.seconds(API_RETRY_WAIT_MINS)
                else:
                    pause.minutes(API_RETRY_WAIT_MINS)

        if not retry_success:
            raise ValueError("Could not make query")
    return wrapper

########################################################
########################################################
########################################################
########################################################

def get_tweets_pagination(hashtag: str, timestamp: datetime, hour: str, write_to_file:bool = True):

    if DEBUG:
        max_results = 10
        limit = 10
    else:
        max_results = 100
        limit = 500

    HASHTAG = "#" + hashtag
    date_string = make_timestring(timestamp)
    HOUR = hour

    if DEBUG:
        file_name_prefix = f"./data/debug_{HASHTAG}_{date_string}_{HOUR}_"
    else:
        file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"

    bearer_token = os.environ.get('BEARER_TOKEN')
    assert bearer_token != None, "Remember to set API credentials as environment variables first!"

    QUERY = HASHTAG + ' -is:retweet -is:quote lang:en' 

    @retry_query
    def make_query_1():
        tweets = []
        client = tweepy.Client(wait_on_rate_limit = True, 
                            bearer_token = bearer_token)

        my_paginator = tweepy.Paginator(client.search_recent_tweets, 
            query=QUERY,
            tweet_fields=['created_at', 'entities', 'public_metrics'],
            media_fields=['url'],
            user_fields=['description'],
            expansions=['attachments.media_keys', 'author_id'],
            max_results = max_results).flatten(limit=limit)

        for tweet in my_paginator:
            tweets.append(tweet)
        return tweets

    tweets = make_query_1()

    tweets_df = []
    # Go through each tweet from each user and oragnize the data into coloumns for export. 
    for item in tweets: #removed ['data']
        if "entities" not in item:
            continue
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

    user_tweets_dfs = pd.DataFrame(tweets_df)

    if write_to_file:
        user_tweets_dfs.to_csv(file_name_prefix + 'tweets.csv', index=False)

    return user_tweets_dfs


########################################################
########################################################
########################################################
########################################################



def get_user_tweets(hashtag:str, timestamp:datetime, hour:str, write_to_file:bool = True):
  if DEBUG:
      max_results = 10
  else:
      max_results = 10

  HASHTAG = "#" + hashtag
  date_string = make_timestring(timestamp)
  HOUR = hour

  if DEBUG:
    file_name_prefix = f"./data/debug_{HASHTAG}_{date_string}_{HOUR}_"
  else:
    file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"


  # Get authentication information from the shell environment.
  bearer_token = os.environ.get('BEARER_TOKEN')


  # If environment variable isn't defined, a reminder pops up.
  assert bearer_token != None, "Remember to set API credentials as environment variables first!"

  # Create .Client() object that will let us access the full archive.

  @retry_query
  def make_client():
    client = tweepy.Client(wait_on_rate_limit = True, bearer_token = bearer_token, return_type=dict)
    return client

  client = make_client()

  # Read tweets.csv for IDs and convert it into a list of IDs
  ids = pd.read_csv(file_name_prefix + 'tweets.csv', usecols=['author_id'], index_col=False)
  ids_df = pd.DataFrame(ids)
  id_list = ids_df['author_id'].values.tolist()

  if UNIQUE_USER_IDS:
    id_list = list(set(id_list))

  tweets_df = []

  for curr_user in id_list:
    # For each user, you retrieve the most recent 10 tweets that they posted.
    @retry_query
    def query_user_tweets():
      usertweets = client.get_users_tweets(id = curr_user,
        exclude=['retweets','replies'],
        tweet_fields=['created_at','entities','public_metrics',],                                      
        media_fields=['url'],
        user_fields=['description'],
        expansions=['attachments.media_keys','author_id'],
        max_results = max_results) 
      return usertweets
    usertweets = query_user_tweets()

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
  if write_to_file:
    tweets_dfs.to_csv(file_name_prefix + 'identified_user_tweets.csv', index=False)
  
  return tweets_dfs

########################################################
########################################################
########################################################
########################################################

def top_hashtags(hashtag:str, timestamp:datetime, hour:str, top_number:int, write_to_file:bool = True):
  HASHTAG = "#" + hashtag
  date_string = make_timestring(timestamp)
  HOUR = hour
  top_number = top_number

  in_year, in_month, in_day, in_hour, in_minute = date_string.split("_")
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
  @retry_query
  def make_client():
    client = tweepy.Client(bearer_token = bearer_token,
      return_type=dict)
    return client
  client = make_client()

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
    @retry_query
    def query_hashtag_counts():
      start_time=f"{in_year}-{in_month}-{in_day}T{int(in_hour)-1}:00:00-04:00",
      end_time=f"{in_year}-{in_month}-{in_day}T{in_hour}:00:00-04:00"
      hashtagcount = client.get_recent_tweets_count(query= "#" + hashtag,
        granularity = 'hour',
        start_time=start_time,
        end_time=end_time)
      return hashtagcount
    hashtagcount = query_hashtag_counts()
    total_tweet_count = hashtagcount['meta']['total_tweet_count']
    final_hashtagcount.append((hashtag, total_tweet_count))

  #Sorts the Hashtags (and counts) in decending order of count
  sorted_hashtag_count_final = sorted(final_hashtagcount, key = lambda kv:kv[1], reverse=True)


  df_sample = pd.DataFrame(counts, columns=["hashtag", "counts"])
  df_sample.sort_values("counts", inplace=True, ascending=False)
  if write_to_file:
    df_sample.to_csv(file_name_prefix + 'sample_hashtag_count_top.csv', index=False)

  df_all_twitter = pd.DataFrame(sorted_hashtag_count_final, columns=["hashtag", "counts"])
  df_all_twitter.sort_values("counts", inplace=True, ascending=False)
  if write_to_file:
    df_all_twitter.to_csv(file_name_prefix + 'all_twitter_hashtag_count_top.csv', index=False)
  
  return {"all_twitter": df_all_twitter, "sample": df_sample}

########################################################
########################################################
########################################################
########################################################

def get_final_tweets_pagination(hashtag:str, timestamp:datetime, hour:str, write_to_file:bool = True, override_hashtag=None):

    date_string = make_timestring(timestamp)
    HOUR = hour

    INITIAL_HASHTAG = "#" + hashtag

    if DEBUG:
        file_name_prefix = f"./data/debug_{INITIAL_HASHTAG}_{date_string}_{HOUR}_"
    else:
        file_name_prefix = f"./data/{INITIAL_HASHTAG}_{date_string}_{HOUR}_"

    df = pd.read_csv(file_name_prefix + 'all_twitter_hashtag_count_top.csv')

    top_num_hashtags = list(df.itertuples(index=False, name=None))

    if override_hashtag != None:
      HASHTAG = override_hashtag
    else:
      HASHTAG, _ = sorted(top_num_hashtags, key = lambda kv:kv[1], reverse=True)[0]

    if DEBUG:
        output_file_name_prefix = f"./data/debug_{HASHTAG}_{date_string}_"
    else:
        output_file_name_prefix = f"./data/{HASHTAG}_{date_string}_"
    
    ENDDATE = datetime.now()
 
    get_individual_final_tweet(HASHTAG, output_file_name_prefix, ENDDATE, write_to_file=write_to_file)

def get_individual_final_tweet(HASHTAG, output_file_name_prefix, ENDDATE, write_to_file:bool=True):
    if DEBUG:
        max_results = 10
        limit = 10
    else:
        max_results = 100
        limit = 5000

    QUERY = "#" + HASHTAG + " -is:retweet -is:quote lang:en"

    bearer_token = os.environ.get('BEARER_TOKEN')

    @retry_query
    def make_client():
        client = tweepy.Client(wait_on_rate_limit = True, 
                        bearer_token = bearer_token)
        return client
    client = make_client()

    @retry_query
    def query_tweets():
        tweets = []

        my_paginator = tweepy.Paginator(client.search_recent_tweets, 
            query=QUERY,
            tweet_fields=['created_at', 'entities', 'public_metrics'],
            media_fields=['url'],
            user_fields=['description'],
            expansions=['attachments.media_keys', 'author_id'],
            end_time = ENDDATE,
            max_results = max_results).flatten(limit=limit)

        for tweet in my_paginator:
            tweets.append(tweet)
        return tweets
    tweets = query_tweets()

    tweet_data_list = []

    for item in tweets:
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
                'hashtags': hashtags, #[tag['tag'] for tag in item['entities']['hashtags']],
                'created_at': item['created_at'],
                'mined_at': datetime.now(),
                'like_count': item['public_metrics']['like_count'],
                'quote_count': item['public_metrics']['quote_count'],
                'reply_count': item['public_metrics']['reply_count'],
                'retweet_count': item['public_metrics']['retweet_count'],
            }

        tweet_data_list.append(tweet_data)

    tweets_df = pd.DataFrame(tweet_data_list)

    if write_to_file:
        tweets_df.to_csv(output_file_name_prefix + 'finaltweets.csv')
    
    return tweets_df

########################################################
########################################################
########################################################
########################################################
