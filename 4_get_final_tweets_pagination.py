from datetime import datetime, timedelta
import os
import pandas as pd
import tweepy

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))

if DEBUG:
    max_results = 1
    limit = 10
else:
    max_results = 100
    limit = 1000

if False:
    INITIAL_HASHTAG = '#blacktwitter'
    HASHTAG      = '#blm'
    now = datetime.now()
    date_string = now.strftime("%Y_%m_%d_%H_%M")
else:
    import argparse
    parser = argparse.ArgumentParser(description="Run my script")
    parser.add_argument("hashtag", type=str)
    parser.add_argument("timestamp", type=str)
    parser.add_argument("hour", type=str)
    date_string = parser.parse_args().timestamp
    HOUR = parser.parse_args().hour

    INITIAL_HASHTAG = parser.parse_args().hashtag

    if DEBUG:
        file_name_prefix = f"./data/debug_{INITIAL_HASHTAG}_{date_string}_{HOUR}_"
    else:
        file_name_prefix = f"./data/{INITIAL_HASHTAG}_{date_string}_{HOUR}_"

    df = pd.read_csv(file_name_prefix + 'all_twitter_hashtag_count_top.csv')
    
    top_num_hashtags = list(df.itertuples(index=False, name=None))

    _, HASHTAG, counts = sorted(top_num_hashtags, key = lambda kv:kv[2], reverse=True)[0]

if DEBUG:
    output_file_name_prefix = f"./data/debug_{INITIAL_HASHTAG}_{HASHTAG}_{date_string}_"
else:
    output_file_name_prefix = f"./data/{INITIAL_HASHTAG}_{HASHTAG}_{date_string}_"

QUERY        = HASHTAG + ' -is:retweet -is:quote'

ENDDATE      = datetime.now()
STARTDATE    = ENDDATE - timedelta(days=6)

bearer_token = os.environ.get('BEARER_TOKEN')

client = tweepy.Client(wait_on_rate_limit = True, 
                    bearer_token = bearer_token)

tweets = []

for tweet in tweepy.Paginator(client.search_recent_tweets, 
                    query=QUERY,
                    tweet_fields=['created_at', 'entities', 'public_metrics'],
                    media_fields=['url'],
                    user_fields=['description'],
                    expansions=['attachments.media_keys', 'author_id'],
                    start_time = STARTDATE,
                    end_time = ENDDATE,
                    max_results = max_results).flatten(limit=limit):
    tweets.append(tweet)

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

tweets_df.to_csv(output_file_name_prefix + 'finaltweets.csv')