#!/bin/bash
export $(cat .env | xargs)
/usr/local/bin/python3 automation.py