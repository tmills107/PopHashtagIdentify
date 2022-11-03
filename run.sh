#!/bin/bash
rm data/debug_*.csv
export $(cat .env | xargs)
#/usr/local/bin/python3 -i automation.py # Run in interactive mode
/usr/local/bin/python3 automation.py # Run in regular mode