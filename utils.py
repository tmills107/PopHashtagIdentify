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

########################################################
########################################################
## Standard Inputs 
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

## MAKE TIMESTRING
## Makes timestring (for naming purposed) from a Datetime object
def make_timestring(d: datetime):
  return d.strftime("%Y_%m_%d_%H_%M")

########################################################
########################################################
########################################################
########################################################

## RETRY QUERY
## Query wrapper so that if the query fails (disconnected from API) it will retry the query 
## for an inputed number of tries (API_RETRY_COUNT) seperated by an inputed number of minutes (API_RETRY_WAIT_MINUTES) 
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

## FILL EMPTY COLUMN FROM HASHTAG ANALYSIS
## Inputs: DataFrame (columns are hashtag, counts, start_time, and end_time) 
## Outputs: DataFrame (columns are hashtag, counts, start_time, and end_time) with empty observations filled with Zero (mainly for sample)
def fill_empty_counts(df:pd.DataFrame):
  _df = copy(df)

  all_start_times = list(_df["input_start_time"].unique())

  g_list = []
  for (h,g) in _df.groupby("hashtag"):
    for t in all_start_times:
      if t not in list(g["input_start_time"]):
        g = pd.concat([g, pd.DataFrame([{"hashtag": h, "input_start_time": t, "counts": 0, "input_end_time": t}])], ignore_index=True)
    g_list.append(g)
  _df = pd.concat(g_list)

  return _df

########################################################
########################################################
########################################################
########################################################

## HASHTAG ANALYSIS 
## Inputs: DataFrame (columns are hashtag, counts, start_time, and end_time)
## Outputs: Line plot of hashtag count over time (Saved as png) 
##          & Aggregate Data (Hourly Average and Daily Total) for each hashtag) (Saved as CSVs)
def hashtag_analysis(df_input:pd.DataFrame, hashtag, year, month, day, sample_or_population):
  df = fill_empty_counts(df_input)
  
  if DEBUG:
        file_name_prefix = f"./data/debug_{hashtag}_{sample_or_population}_{month}_{day}"
  else:
        file_name_prefix = f"./data/{hashtag}_{sample_or_population}_{month}_{day}"

  df.to_csv(file_name_prefix + "_count_data.csv")

  ## Aggregate Data

  ## Hourly Average
  hourly_average = df.groupby("hashtag").mean()
  hourly_average.sort_values('counts', ascending=False).to_csv(f"{file_name_prefix}_hourly_average.csv")
  
  ## Daily Total
  daily_total = df.groupby("hashtag").sum()
  daily_total.sort_values('counts', ascending=False).to_csv(f"{file_name_prefix}_daily_total.csv")
  
  if sample_or_population == 'population':
    ## Weekly Total Time Setting
    week_start_day = datetime(year, month, day)
    week_start_time = week_start_day + timedelta(days=-6)
    week_end_time = datetime(year, month, day)
    hashtags = list(set(df['hashtag']))

    ## Weekly Total 
    weekly_total = weekly_counts(hashtags, week_start_time, week_end_time)
    weekly_total.sort_values('weekly_total', ascending=False).to_csv(f"{file_name_prefix}_weekly_total.csv")
  
  ## Make Plots
  #plt.clf()

  x_ticks = list(df["input_start_time"])
  x_tick_labels = list(pd.DatetimeIndex(list(df["input_start_time"])).hour)

  ax = sns.lineplot(data=df, hue="hashtag", x="input_start_time", y="counts", legend=True, markers="o")
  ax.set_xticks(x_ticks)
  ax.set_xticklabels(x_tick_labels)

  sns.move_legend(ax, "upper left", bbox_to_anchor=(1, 1))
  plt.title(f"#{hashtag} Daily Trend {month}/{day}")
  plt.xlabel("Time")
  plt.ylabel("Counts")
  fig = plt.gcf()
  fig.savefig(f"{file_name_prefix}_plot.png", bbox_inches = 'tight')
  plt.clf()

########################################################
########################################################
########################################################
########################################################

## GET TWEETS (PAGINATION)
## Inputs: Hashtag, Time to Query = End Time (Collected in time reverse order), User Tweet Limit: Number of Tweets Collected Per User (limiting bots)
## Outputs: DataFrame of tweets (Written to file too)
def get_tweets_pagination(hashtag: str, end_time:datetime, write_to_file:bool = True, start_time = None, limit = None, user_tweet_limit=None):

    if DEBUG:
        limit = 100

    HASHTAG = hashtag
    date_string = make_timestring(end_time)
    HOUR = end_time.hour

    if DEBUG:
        file_name_prefix = f"./data/debug_{HASHTAG}_{date_string}_{HOUR}_"
    else:
        file_name_prefix = f"./data/{HASHTAG}_{date_string}_{HOUR}_"

    ## Get authentication information from the shell environment.
    bearer_token = os.environ.get('BEARER_TOKEN')
    
    ## If environment variable isn't defined, a reminder pops up.
    assert bearer_token != None, "Remember to set API credentials as environment variables first!"

    ## Sets the query as "#" + term inputed, exludes retweets and quote tweets, find tweets only in English
    QUERY = f"#{HASHTAG} -is:retweet -is:quote -is:nullcast lang:en" 

    @retry_query
    def make_query_1():
        ##Creates Tweets DataFrame 
        tweets = []
        
        ##Creates dictionary to place tweet authors followers count 
        followers_count_dict = {}
        username_dict = {}
        
        ## Create .Client() object that will let us access the full archive.
        client = tweepy.Client(wait_on_rate_limit = True, 
                            bearer_token = bearer_token)

        ## Sets the number of pages the paginator will go through 
        ## (Calculated by dividing total number of tweets desired by 100 (max number of results per page))
        page_limit = int(limit/100)
        if page_limit == 0:
          raise ValueError("Limit too low")
        
        ## Builds Query for tweets including desired tweet/user/media fields and expansions
        my_paginator = tweepy.Paginator(client.search_recent_tweets, 
            query=QUERY,
            tweet_fields=['created_at', 'entities', 'public_metrics'],
            media_fields=['url'],
            start_time=start_time,
            end_time=end_time,
            user_fields=['username', 'description', 'public_metrics'],
            expansions=['attachments.media_keys', 'author_id'],
            max_results = 100,
            limit = page_limit)
        
        ## Add Followers Count and Username to main data object
        for page in my_paginator:
          for u in page.includes["users"]:
            followers_count_dict[u.id] = u.public_metrics["followers_count"]
            username_dict[u.id] = u["username"]
          for tweet in page.data:
            tweet["data"]["follower_count"] = followers_count_dict[tweet["author_id"]]
            tweet["data"]["username"] = username_dict[tweet["author_id"]]
            tweets.append(tweet)

        return tweets

    ## Making Query
    tweets = make_query_1()

    tweets_df = []
    # Go through each tweet from each user and oragnize the data into coloumns for export. 
    for item in tweets:
        if "entities" not in item:
            continue
        if "hashtags" in item["entities"]:
            hashtags = [d["tag"].lower() for d in item["entities"]["hashtags"]]
        else:
            hashtags = []

        tweet_data = {
                'tweet_id': item['id'],
                'author_id': item['author_id'],
                'username': item['data']['username'],
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

    ## If data includes more than [user_tweet_limit tweets] per a single user it takes only the most recent [user_tweet_limit] from that user
    if user_tweet_limit != None:
      groups = []
      for (_,group) in user_tweets_dfs.groupby("author_id"):
        group.sort_values("created_at", inplace=True, ascending=False)
        group = group.head(user_tweet_limit)
        groups.append(group)
      user_tweets_dfs = pd.concat(groups)

    ## Writed to File 
    if write_to_file:
        user_tweets_dfs.to_csv(file_name_prefix + 'tweets' + f"_{limit}" + '.csv', index=False)

    ## Returns DataFrame of Tweets
    return user_tweets_dfs

########################################################
########################################################
########################################################
########################################################

## GET USER TWEETS
## Inputs: DataFrame of tweets
## Outputs: DataFrame of user's tweets (Writes to file)
def get_user_tweets(input_df, hashtag:str, end_time:datetime, write_to_file:bool = True, start_time = None):
  if DEBUG:
      max_results = 10
  else:
      max_results = 10

  date_string = make_timestring(end_time)
  HOUR = end_time.hour

  if DEBUG:
    file_name_prefix = f"./data/debug_{hashtag}_{date_string}_{HOUR}_"
  else:
    file_name_prefix = f"./data/{hashtag}_{date_string}_{HOUR}_"

  ## Get authentication information from the shell environment.
  bearer_token = os.environ.get('BEARER_TOKEN')

  ## If environment variable isn't defined, a reminder pops up.
  assert bearer_token != None, "Remember to set API credentials as environment variables first!"

  ## Create .Client() object that will let us access the full archive.
  @retry_query
  def make_client():
    client = tweepy.Client(wait_on_rate_limit = True, bearer_token = bearer_token, return_type=dict)
    return client

  client = make_client()

  ## Read tweets df for IDs and convert it into a list of IDs
  id_list = input_df['author_id'].values.tolist()

  ## Limits Repeats of Users in ID List (so each unique user only queried once)
  if UNIQUE_USER_IDS:
    id_list = list(set(id_list))

  tweets_df = []

  for curr_user in id_list:
    ## For each user, you retrieve the most recent 10 tweets that they posted.
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

## MAKING PRUNED HASHTAG LIST FROM INPUTED DATAFRAME OF TWEETS
## Inputs: Tweet DataFrame & Number of Hashtags to List (top_number)
## Outputs: List of Hashtags 
def make_pruned_hashtag_list(user_tweets, top_number, return_counts = True):
  
  hashtags_list = []

  ## Grouping Tweets by Author
  for author_id, group in user_tweets.groupby("author_id"):
    
    ## Limit to only include tweets with fewer than HASHTAG_COUNT_LIMIT 
    _filtered_hashtags = list(group["hashtags"])
    _filtered_hashtags = filter(lambda ht_l: len(ht_l) < HASHTAG_COUNT_LIMIT, _filtered_hashtags)

    ## Limits to only a list of unique hashtags per user (only counts hashtags once per user)
    if UNIQUE_USER_HASHTAGS_COUNT:
      _hash_list = list(set([item for sublist in _filtered_hashtags for item in sublist]))
    else:
      _hash_list = [item for sublist in _filtered_hashtags for item in sublist]

    hashtags_list.extend(_hash_list)
  
  if DEBUG:
    top_number = 10

  ## Finds the most common used hashtags from the list.
  counts = collections.Counter(hashtags_list).most_common(top_number)
  counts = sorted(counts, key = lambda x: x[1], reverse=True)

  if len(counts) == 0:
    raise ValueError("No hashtags found")

  if return_counts:
    return counts
  else:
    return [h for (h,_) in counts]

########################################################
########################################################
########################################################
########################################################

## QUERY TWITTER FOR THE MOST USED HASHTAG FROM INPUTED LIST 
## Inputs: List of hashtags, starting time, ending time
## Outputs: Hashtag, Total Count, Hourly Count
def all_twitter_top_counts(hashtags, start_time, end_time):
  # Get authentication information from the shell environment.
  bearer_token = os.environ.get('BEARER_TOKEN')

  # If environment variable isn't defined, a reminder pops up.
  assert bearer_token != None, "Remember to set API credentials as environment variables first!"

  # Create .Client() object that will let us access the full archive. Sent through Query Retry
  @retry_query
  def make_client():
    client = tweepy.Client(bearer_token = bearer_token,
      return_type=dict)
    return client
  client = make_client()

  final_hashtagcount = []

  # Queries Twitter to get the count (hourly and total) over the time period specified. 
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

########################################################
########################################################
########################################################
########################################################

## QUERY TWITTER FOR WEEKLY TOTAL HASHTAG COUNT 
## Inputs:
## Outputs:
def weekly_counts(hashtags, start_time, end_time):
  
  # Get authentication information from the shell environment.
  bearer_token = os.environ.get('BEARER_TOKEN')

  # If environment variable isn't defined, a reminder pops up.
  assert bearer_token != None, "Remember to set API credentials as environment variables first!"

  # Create .Client() object that will let us access the full archive. Sent through Query Retry
  @retry_query
  def make_client():
    client = tweepy.Client(bearer_token = bearer_token,
      return_type=dict)
    return client
  client = make_client()
  
  weekly_totals = []

  for hashtag in hashtags:
    count = client.get_all_tweets_count(query= "#" + hashtag,
            granularity = 'day',
            start_time= start_time,
            end_time=end_time)
    
    hashtag_total = count['meta']['total_tweet_count']
    weekly_totals.append([hashtag, hashtag_total])
    weekly_total_df = pd.DataFrame(weekly_totals, columns = ['hashtag', 'weekly_total'])

  return weekly_total_df

########################################################
########################################################
########################################################
########################################################

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

########################################################
########################################################
########################################################
########################################################