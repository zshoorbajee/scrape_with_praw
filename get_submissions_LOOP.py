import pandas as pd
import numpy as np
import json
import datetime as dt
import os
import sys
import praw
from prawcore.exceptions import TooManyRequests
import re
import string
import time

src_path = os.path.abspath('./src/')
sys.path.append(src_path)
from functions import get_reddit_posts, get_posts_data, get_next_filename

##################################################

print('\n')
print('#'*50)
print(f'Script started at {dt.datetime.now().strftime("%H:%M:%S")}')
print('#'*50)
print('\n')

start_time = time.time()
def time_check(start=None):
    if start:
        t = time.time() - start
    else:
        t = time.time() - start_time
    print(f'Time check: {t//60:.0f} minutes and {t%60:.0f} seconds')

#########################################
##### CHANGE INFO BELOW ACCORDINGLY #####
#########################################

## Name of subreddit you are scraping
sub_names = [
    'washingtondc',
    'nova',
    'bikedc'
    ] 

## Manner of sorting submissions
## Supported: new, top, hot
sort_by = 'new'

## Time filter
## Supported: all, year, month, week, day, hour
## Leave uncommented, but this is only used if setting sort_by equal to 'top' or 'controversial'.
time_filter = 'all'

output_dir = './data'

## Location of API client ID and secret
with open('../../../.secret/reddit_api.json', 'r') as f:
    client = json.load(f)

client_id = client['api_key']
client_secret = client['secret']

## Set up API 
reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent="ZS"
)

#########################################
##### CHANGE INFO ABOVE ACCORDINGLY #####
#########################################

for sub_name in sub_names:

    ### Getting submissions  ###
    df, time_run = get_reddit_posts(reddit, sub_name=sub_name, sort_by=sort_by, time_filter=time_filter, limit=1000)
    print(time_run)
    print('\n')


    ### Extracting data from submissions ###
    df = get_posts_data(df, max_attempts=10, sleep=30)
    print('\n')

    ### Display data info ###
    print(f"Newest submission: {df['datetime'].max()}")
    print(f"Oldest submission: {df['datetime'].min()}")
    print(f'DataFrame shape: {df.shape}')
    print('\n')

    ### Create appropriate filepath and name ###
    if sort_by in ['new', 'hot', 'rising']:
        sort_type = sort_by
    else:
        sort_type = f'{sort_by}_{time_filter}'

    filename = f'{time_run.strftime("%Y%m%d")}_{sub_name}_{sort_type}_reddit_data.pkl'

    filename = get_next_filename(filename, output_dir)

    filepath = os.path.join(output_dir, filename)

    ### Save file ###
    df.to_pickle(filepath)

    print(f'Saved {filepath}')
    print('\n')

    time_check()
    print('\n')

print('#'*50)
print(f"Script ended at {dt.datetime.now().strftime('%H:%M:%S')}")
print('#'*50)
print('\n')