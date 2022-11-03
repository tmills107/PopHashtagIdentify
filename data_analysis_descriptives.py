import pandas as pd
import os
import re
import numpy as np

file_names = os.listdir("data/final_tweets")

all_df = []


for file in file_names:
    df = pd.read_csv(f"/Users/thomasmiller/Documents/ThesisProject/Protocol - Automation/data/final_tweets/{file}")
    
    df = df.iloc[:,1:]

    df['reply_count'] = df['reply_count'].astype(int)
    df['like_count'] = df['like_count'].astype(int)
    df['retweet_count'] = df['retweet_count'].astype(int)

    hashtag = re.match(r'^[^_]+(?=_)',file)
    hashtag = hashtag.group(0)

    df['starting_hashtag'] = hashtag #.group(0)
 
    df['reply_binary'] = np.nan
    df['like_binary'] = np.nan
    df['retweet_binary'] = np.nan

    for tweet in df:
        df['like_binary'] = [int(bool(i)) for i in df['like_count']]
        df['retweet_binary'] = [int(bool(i)) for i in df['retweet_count']]
        df['reply_binary'] = [int(bool(i)) for i in df['reply_count']]

    # num_tweets=len(df)
    # print(f"Number of Tweets = {num_tweets}")

    # # Number of Unique Users
    # ids = df["author_id"]
    # ids = list(set(ids))
    # id_count = len(ids)
    # print(f"Number of Unique Users = {id_count}")

    # # Tweet to User Ratio
    # tweet_user_ratio = num_tweets/id_count
    # print(f"Tweet to User Ratio = {round(tweet_user_ratio, 2)}")

    # # Tweets with Retweets
    # tweet_retweet_df = df[df["retweet_count"]>=1]
    # retweet_count = len(tweet_retweet_df)
    # retweet_percentage = (retweet_count/num_tweets)*100
    # print(f"Tweets with Retweets = {retweet_count} ({round(retweet_percentage, 2)}%)")

    # # Tweets with Likes
    # tweet_like_df = df[df["like_count"]>=1]
    # like_count = len(tweet_like_df)
    # like_percentage = (like_count/num_tweets)*100
    # print(f"Tweets with Likes = {like_count} ({round(like_percentage, 2)}%)")

    # # Tweets with Replies
    # tweet_replies_df = df[df["reply_count"]>=1]
    # replies_count = len(tweet_replies_df)
    # replies_percentage = (replies_count/num_tweets)*100
    # print(f"Tweets with Replies = {replies_count} ({round(replies_percentage,2)}%)")

    all_df.append(df)

all_df = pd.concat(all_df)
all_df = pd.get_dummies(all_df, columns = ['starting_hashtag'], prefix = "starting_hashtag")
all_df = all_df.iloc[:,1:]

# all_df.to_csv("./data/2022_10_26_all_tweets_extended.csv")

# all_num_tweets=len(all_df)
# print(f"Total Number of Tweets = {all_num_tweets}")

# ids_all = all_df["author_id"]
# ids_all = list(set(ids_all))
# id_all_count = len(ids_all)
# print(f"Number of Unique Users for all Tweets in all Hashtag= {id_all_count}")

# all_tweet_user_ratio = all_num_tweets/id_all_count
# print(f"Tweet to User Ratio = {round(all_tweet_user_ratio, 2)}")

# all_tweet_retweet_df = all_df[all_df["retweet_count"]>=1]
# all_retweet_count = len(all_tweet_retweet_df)
# all_retweet_percentage = (all_retweet_count/all_num_tweets)*100
# print(f"All Tweets with Retweets = {all_retweet_count} ({round(all_retweet_percentage, 2)}%)")

# all_tweet_like_df = all_df[all_df["like_count"]>=1]
# all_like_count = len(all_tweet_like_df)
# all_like_percentage = (all_like_count/all_num_tweets)*100
# print(f"All Tweets with Likes = {all_like_count} ({round(all_like_percentage, 2)}%)")

# all_tweet_replies_df = all_df[all_df["reply_count"]>=1]
# all_replies_count = len(all_tweet_replies_df)
# all_replies_percentage = (all_replies_count/all_num_tweets)*100
# print(f"All Tweets with Replies = {all_replies_count} ({round(all_replies_percentage,2)}%)")