#!/bin/bash
conda activate twitter_hashtag_analysis
rm data/debug_*.csv
rm data/debug_*.png
export $(cat .env | xargs)
#python -i automation.py # Run in interactive mode
python automation.py # Run in regular mode