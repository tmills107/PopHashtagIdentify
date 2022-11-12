### Automation File for Querying Twitter within the Past Week for Particular Hashtags 
### Author: Thomas Miller (in collaboration with Samantha Barron, Ph.D.) 
### Written for Master's Thesis Project "Queer Digital Community: An Analysis of Twitter Counterpublics"
### Fall 2022 / Spring 2023

import os
import pandas as pd
from datetime import datetime
from datetime import timedelta
from utils import (
    get_tweets_pagination,
    get_user_tweets,
    all_twitter_top_counts,
    hashtag_analysis,
    make_pruned_hashtag_list
)

PYTHON_EXEC = "/usr/local/bin/python3"

DEBUG = bool(int(os.getenv("DEBUG_SCRIPT")))
if DEBUG:
    print("Running script in DEBUG mode")
else:
    print("Running script in FULL mode")


## Main Function that Repeats Steps 1-3 in Time Window Provided. 
def main_cycle(HASHTAG_LIST, hours_to_check, time_year, time_month, time_day, PLOT_NUMBER, TOP_NUMBER):
    for HASHTAG in HASHTAG_LIST:
        
        all_hashtag_list = []
        df_sample_list = []
        
        for h in hours_to_check:
            ## Setting Hour Window to Query
            datetime_day = datetime(time_year, time_month, time_day) # First moment of day to check
            end_time = datetime_day + timedelta(hours=h)
            start_time = end_time + timedelta(hours=-1)
            
            ## Step 1 (inital query of 100) | Output: df of 100 Tweet
            df_ids = get_tweets_pagination(HASHTAG, end_time, write_to_file=True, start_time=None, limit=100, user_tweet_limit=50)
            
            ## Step 2 (getting user ids and last 10 tweets per user) | Output: df of user tweets
            df_user_tweets = get_user_tweets(df_ids, HASHTAG, end_time, write_to_file=True, start_time=None)
            
            ## Step 3 (getting hashtags used in user tweets) | Output: TOP_NUMBER list of hashtags w/ counts 
            hashtags_list = make_pruned_hashtag_list(df_user_tweets, TOP_NUMBER, return_counts=True)
            all_hashtag_list.extend(hashtags_list)

            ## Adding to df of hourly hashtags (limited to TOP_NUMBER) and counts 
            _df = pd.DataFrame([
                {"hashtag": h, "counts": c, "input_start_time": start_time, "input_end_time": end_time}
                for (h,c) in 
                make_pruned_hashtag_list(df_user_tweets, TOP_NUMBER, return_counts=True)
                ])
            df_sample_list.append(_df)

        ## Concatinating 
        df_sample = pd.concat(df_sample_list)

        ## Finds Top 5 Hashtags Used Through All Samples Combined
        d = {x: 0 for x, _ in all_hashtag_list}
        for name, num in all_hashtag_list:
            d[name] += num
        all_hashtag_list = list(map(tuple, d.items()))[0:PLOT_NUMBER]
        top_sample_list = list(zip(*all_hashtag_list))[0]

        ## Sets Count Collection Time to 8am-10pm for the Day of Query 
        START_TIME = datetime_day + timedelta(hours=8)
        END_TIME = datetime_day + timedelta(hours=22)

        ## Filtering Sample Hashtags (and counts) by the Top PLOT_NUMBER Hashtags 
        filtered_sample = df_sample.loc[df_sample['hashtag'].isin([h for (h,_) in all_hashtag_list])]

        ## Making Sample Chart and Figures
        sample_or_population = "sample"
        hashtag_analysis(filtered_sample, HASHTAG, time_year, time_month, time_day, sample_or_population)

        ## Counts Hashtag Use in All of Twitter (Returns PLOT_NUMBER many)
        counts = all_twitter_top_counts(top_sample_list, START_TIME, END_TIME)[0:PLOT_NUMBER] 

        ## Prepping Population Count Data for Analysis  
        _columns = ["hashtag", "counts", "input_start_time", "input_end_time"]
        population_count = [] 
        for _hashtag in counts:
            hashtag = _hashtag[0]
            for count in _hashtag[2]:
                tweet_count = count['tweet_count']
                _end_time = count['end']
                _start_time = count['start']
                population_count.append([hashtag, tweet_count, _start_time, _end_time])
        population_df = pd.DataFrame(population_count, columns=_columns)

        ## Making Population Chart and Figures
        sample_or_population = "population"
        hashtag_analysis(population_df, HASHTAG, time_year, time_month, time_day, sample_or_population)

## Final Function to Pull 5000 Tweets (limit) from inputed Hashtag
def final(hashtag, end_time, limit=5000):
    return get_tweets_pagination(hashtag, end_time=end_time, write_to_file=True, start_time=None, limit=limit, user_tweet_limit=50)

## INPUTS ##
#################################################################
#################################################################
HASHTAG_LIST = ["blacktwitter", "lgbtq", "bluewave", "trump"] ## Input without # (added in querying functions)
FINAL_RUN = False
TOP_NUMBER = 10                                         ## Number of Hashtags Identified in User Tweets
PLOT_NUMBER = 5                                         ## Number of Hashtags to be Plotted From Population Top Hashtag
time_year, time_month, time_day = (2022, 11, 11)         ## Year, Month, and Day to be Queried (must be within the past week) 
hours_to_check = list(range(8, 23))                     ## Time of Day to be Queried (8 = 8am, 23 means 10pm will run), include ", 4" after to skip hours by 4                    
#################################################################
#################################################################

if FINAL_RUN is False:
    main_cycle(HASHTAG_LIST, hours_to_check, time_year, time_month, time_day, PLOT_NUMBER, TOP_NUMBER)

if FINAL_RUN is True:
    datetime_day = datetime(time_year, time_month, time_day)
    end_time = datetime_day + timedelta(hours=22)
    for hashtag in HASHTAG_LIST:
        final(hashtag, end_time, limit=5000)