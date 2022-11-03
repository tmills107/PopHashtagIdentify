import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("data/2022_10_26_all_tweets.csv", parse_dates=["created_at"])
df = df.iloc[:,1:]

df["day"] = df["created_at"].dt.day
df["hour"] = df["created_at"].dt.hour

for key, grp in df.groupby(['starting_hashtag']):
    tweet_df_30min = df.groupby(pd.Grouper(key='created_at', freq='30Min', convention='start')).size()
    tweet_df_30min.plot(figsize=(18,6))
    plt.ylabel('30 Minute Tweet Count')
    plt.title('Hashtag Tweet Freq. Count')
    plt.grid(True)
