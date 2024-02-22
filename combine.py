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

file_dir = './data'
output_dir = './data/combined'
name = f'combined_{sort_by}_submissions_{sub_name}_reddit.pkl'
path = os.path.join(output_dir, name)

## Grab all filenames matching desired subreddit and sort/filter
filenames = os.listdir(file_dir)
filenames = [x for x in filenames if ((sort_by in x) & (sub_name in x))]
filenames = sorted(filenames)[::-1]

# Start a combined dataframe
df = pd.read_pickle(os.path.join(file_dir, filenames[0]))

# Save if only one dataframe
# Otherwise, concat all dataframes, sort with newest at the top and drop duplicates (keeping newest)
if len(filenames) < 2:
    df.to_pickle(path)
else:
    for file in filenames[1:]:
        _df = pd.read_pickle(os.path.join(file_dir, file))
        df = pd.concat([df, _df])
    df.sort_values(by=['datetime', 'submission_age'], ascending=False, inplace=True)
    df.drop_duplicates(subset=['id'], keep='first', inplace=True)
    df.to_pickle(path)