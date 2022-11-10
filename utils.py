import os
from copy import copy
import matplotlib.pyplot as plt
import seaborn as sns
import pause
import collections
from datetime import datetime
from datetime import timedelta
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

def fill_empty_counts(df:pd.DataFrame):
  _df = copy(df)

  all_start_times = list(df["input_start_time"].unique())

  g_list = []
  for (h,g) in _df.groupby("hashtag"):
    for t in all_start_times:
      if t not in g["input_start_time"]:
        g = pd.concat([g, pd.DataFrame([{"hashtag": h, "input_start_time": t, "counts": 0, "input_end_time": t}])], ignore_index=True)
        print("I filled a time!")
    g_list.append(g)
  _df = pd.concat(g_list)

  return _df

def hashtag_analysis(df_input:pd.DataFrame, output_prefix, hashtag, month, day):
  # Make Plots
  plt.clf()
  df = df_input # .set_index('input_end_time', inplace=False)
  #df_input = fill_empty_counts(df_input)
  sns.lineplot(data=df, hue="hashtag", x="input_start_time", y="counts", legend=False)
  ax = sns.scatterplot(data=df_input, hue="hashtag", x="input_start_time", y="counts", marker="o")
  sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
  plt.title(f"#{hashtag} Daily Trend {month}/{day}")
  plt.xlabel("Time")
  plt.ylabel("Counts")
  fig = plt.gcf()
  fig.savefig(f"{output_prefix}_plot.png", bbox_inches = 'tight')
  plt.clf()

  ## Aggregate Data
  hourly_average = df_input.groupby("hashtag").mean()
  hourly_average.sort_values('counts', ascending=False).to_csv(f"{output_prefix}_hourly_average.csv")
  daily_total = df_input.groupby("hashtag").sum()
  daily_total.sort_values('counts', ascending=False).to_csv(f"{output_prefix}_daily_total.csv")

########################################################
########################################################
########################################################
########################################################

def get_tweets_pagination(hashtag: str, end_time:datetime, write_to_file:bool = True, start_time = None, limit = None, user_tweet_limit=None):

    if DEBUG:
        limit = 200
        print(f"Hashtag: {hashtag}")

    HASHTAG = hashtag
    date_string = make_timestring(end_time)
    HOUR = end_time.hour

    if DEBUG:
        file_name_prefix = f"./data/debug_{HASHTAG}_{date_string}_{HOUR}_"
    else:
        file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"

    bearer_token = os.environ.get('BEARER_TOKEN')
    assert bearer_token != None, "Remember to set API credentials as environment variables first!"

    QUERY = "#" + HASHTAG + ' -is:retweet -is:quote lang:en' 

    @retry_query
    def make_query_1():
        tweets = []
        followers_count_dict = {}
        client = tweepy.Client(wait_on_rate_limit = True, 
                            bearer_token = bearer_token)

        page_limit = int(limit/100)
        if page_limit == 0:
          raise ValueError("Limit too low")
        my_paginator = tweepy.Paginator(client.search_recent_tweets, 
            query=QUERY,
            tweet_fields=['created_at', 'entities', 'public_metrics'],
            media_fields=['url'],
            start_time=start_time,
            end_time=end_time,
            user_fields=['description', "public_metrics"],
            expansions=['attachments.media_keys', 'author_id'],
            max_results = 100,
            limit = page_limit)
        
        for page in my_paginator:
          for u in page.includes["users"]:
            followers_count_dict[u.id] = u.public_metrics["followers_count"]
          for tweet in page.data:
            tweet["data"]["follower_count"] = followers_count_dict[tweet["author_id"]]
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
                'follower_count': item['data']['follower_count'],
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

    if user_tweet_limit != None:
      groups = []
      for (_,group) in user_tweets_dfs.groupby("author_id"):
        group.sort_values("created_at", inplace=True, ascending=False)
        group = group.head(user_tweet_limit)
        groups.append(group)
      user_tweets_dfs = pd.concat(groups)

    if write_to_file:
        user_tweets_dfs.to_csv(file_name_prefix + f"{limit}_" + 'tweets.csv', index=False)

    return user_tweets_dfs


########################################################
########################################################
########################################################
########################################################



def get_user_tweets(input_df, hashtag:str, end_time:datetime, write_to_file:bool = True, start_time = None):
  if DEBUG:
      max_results = 10
  else:
      max_results = 10

  HASHTAG = "#" + hashtag
  date_string = make_timestring(end_time)
  HOUR = end_time.hour

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
  id_list = input_df['author_id'].values.tolist()

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
        start_time=start_time,
        end_time=end_time,
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

def make_pruned_hashtag_list(user_tweets, top_number, return_counts = False):
  hashtags_list = []

  for author_id, group in user_tweets.groupby("author_id"):
    _filtered_hashtags = list(group["hashtags"])
    _filtered_hashtags = filter(lambda ht_l: len(ht_l) < HASHTAG_COUNT_LIMIT, _filtered_hashtags)

    if UNIQUE_USER_HASHTAGS_COUNT:
      _hash_list = list(set([item for sublist in _filtered_hashtags for item in sublist]))
    else:
      _hash_list = [item for sublist in _filtered_hashtags for item in sublist]

    hashtags_list.extend(_hash_list)
  
  if DEBUG:
    top_number = 5

  # Finds the most common used hashtags (case sensitive) from the list.
  counts = collections.Counter(hashtags_list).most_common(top_number)

  if len(counts) == 0:
    raise ValueError("No hashtags found")

  if return_counts:
    return counts

  return [h for (h,_) in counts]

def all_twitter_top_counts(hashtags, start_time, end_time):
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

  final_hashtagcount = []

  # Queries Twitter to find the count of the tweets for each of the 5 hashtags identified over the past 7 days. 
  for _hashtag in hashtags:
    @retry_query
    def query_hashtag_counts():
      hashtagcount = client.get_recent_tweets_count(query= "#" + _hashtag,
        granularity = 'hour',
        start_time=start_time,
        end_time=end_time)
      return hashtagcount
    hashtagcount = query_hashtag_counts()
    total_tweet_count = hashtagcount['meta']['total_tweet_count']
    final_hashtagcount.append((_hashtag, total_tweet_count, hashtagcount['data']))

  #Sorts the Hashtags (and counts) in decending order of count
  sorted_hashtag_count_final = sorted(final_hashtagcount, key = lambda kv:kv[1], reverse=True)

  return sorted_hashtag_count_final

def top_hashtags(hashtags_list, hashtag:str, end_time:datetime, write_to_file:bool = True, start_time = None):
  if start_time == None:
    start_time = end_time - timedelta(hours=1)

  HASHTAG = "#" + hashtag
  date_string = make_timestring(end_time)
  HOUR = end_time.hour

  if DEBUG:
    file_name_prefix = f"./data/debug_{HASHTAG}_{date_string}_{HOUR}_"
  else:
    file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"


  # Opens the csv file created in the last script of the identified user tweets. 

  # Removes the quotations created when python reads the hashtag items as strings. 
  #hashtags_df["hashtags"] = hashtags_df["hashtags"].map(eval)

  sorted_hashtag_count_final = all_twitter_top_counts(hashtags_list, start_time, end_time)

  #df_sample = pd.DataFrame(counts, columns=["hashtag", "counts"])
  #df_sample.sort_values("counts", inplace=True, ascending=False)
  #if write_to_file:
  #  df_sample.to_csv(file_name_prefix + 'sample_hashtag_count_top.csv', index=False)

  df_all_twitter = pd.DataFrame(sorted_hashtag_count_final, columns=["hashtag", "counts"])
  df_all_twitter.sort_values("counts", inplace=True, ascending=False)
  if write_to_file:
    df_all_twitter.to_csv(file_name_prefix + 'all_twitter_hashtag_count_top.csv', index=False)
  
  df_all_twitter["input_hashtag"] = hashtag
  df_all_twitter["input_start_time"] = start_time
  df_all_twitter["input_end_time"] = end_time

  #df_sample["input_hashtag"] = hashtag
  #df_sample["input_start_time"] = start_time
  #df_sample["input_end_time"] = end_time
  
  #return (df_all_twitter, df_sample)
  return (df_all_twitter, None)