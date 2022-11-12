#!/bin/bash
conda activate twitter_hashtag_analysis
rm data/debug_*.csv
rm data/debug_*.png
export $(cat .env | xargs)
python data_analysis_descriptives.py