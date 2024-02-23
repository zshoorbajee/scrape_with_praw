import pandas as pd
import numpy as np
import json
import datetime as dt
import os
import praw
from prawcore.exceptions import TooManyRequests
import re
import string
import time

#########################################
##### CHANGE INFO BELOW ACCORDINGLY #####
#########################################

## Name of subreddit you are combining
sub_name = 'washingtondc' 

## Manner of sorting submissions
## Supported: new
sort_by = 'new'

## Location of files to combine
file_dir = './data'
output_dir = './data/combined'

## Name and path of the combined DataFrame
name = f'combined_{sort_by}_submissions_{sub_name}_reddit.pkl'
output_path = os.path.join(output_dir, name)

## Whether to delete already merged files
delete_combined = False

## Creates a "combined" directory if it doesn't already exists
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

## Grab all filenames matching desired subreddit and sort/filter
filenames = os.listdir(file_dir)
filenames = [x for x in filenames if ((sort_by in x) & (sub_name in x))]
filenames = sorted(filenames)[::-1]

# Start a combined dataframe
if os.path.exists(output_path):
    df = pd.read_pickle(output_path)
    print(f'The file {output_path} already exists. Combining newer files with this one.', '\n')
else:
    df = pd.read_pickle(os.path.join(file_dir, filenames[0]))
    print(f'The file {output_path} does not exist. Starting a new combined file to save to this path.', '\n')

# Concat all dataframes, sort with newest at the top and drop duplicates (keeping newest)
for file in filenames:
    filepath = os.path.join(file_dir, file)
    _df = pd.read_pickle(filepath)
    df = pd.concat([df, _df])
    print(f'Added {file} to combined file.')
    if delete_combined:
        os.remove(filepath)
        print(f'Deleted {file}', '\n')
    else:
        print('\n')
df.sort_values(by=['datetime', 'submission_age'], ascending=False, inplace=True)
df.drop_duplicates(subset=['id'], keep='first', inplace=True)
df.to_pickle(output_path)