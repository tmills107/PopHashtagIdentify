#!/bin/bash
rm data/debug_*.csv
export $(cat .env | xargs)
/usr/local/bin/python3 automation.py