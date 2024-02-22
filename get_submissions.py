import pandas as pd
import numpy as np
import datetime as dt
import os
import praw
import re
import string
import time

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
sub_name = 'irs' 

## Manner of sorting submissions
## Supported: new, top, hot
sort_by = 'new'

## Time filter
## Supported: all, year, month, week, day, hour
## Leave uncommented, but this is only used if setting sort_by equal to 'top' or 'controversial'.
time_filter = 'all'

output_dir = './data'

## Location of API client ID and secret
with open('../../../.secret/reddit/ZSDSFI_client_id.txt') as f:
    client_id = f.read()

with open('../../../.secret/reddit/ZSDSFI_client_secret.txt') as f:
    client_secret = f.read()

## Set up API 
reddit = praw.Reddit(
    client_id=client_id,
    client_secret=client_secret,
    user_agent="ZS"
)

#########################################
##### CHANGE INFO ABOVE ACCORDINGLY #####
#########################################

def get_reddit_posts(
    reddit_praw: praw.reddit.Reddit,
    sub_name: str,
    sort_by: str,
    time_filter: str = None,
    limit: int = 1000,
    return_time = True
    ):
    """Returns a DataFrame consisting of PRAW submission objects. 
    These objects contain data about body, comments, and other aspects of a Reddit post.
    The function needs to take in a PRAW Reddit object and a subreddit name, and a 
    sorting option (new, hot, rising, top, or controversial).

    Optionally takes in a time filter if sorting by top or controversial.
    This  determines which time period of top posts to retrieve (hour, day, week, month, year, all).

    By default, returns a tuple of the DataFrame and the time the function was run. 
    This may be updated, but it's important to not when a set of submissions was retrieved,
    since subreddits are getting new submissions constantly and Reddit only keeps 1000 of them
    on the website at any given time. 
    The return_time parameter can be set to False. 
    """
    time_run = dt.datetime.now()

    posts = []

    subreddit = reddit_praw.subreddit(sub_name)

    if sort_by == 'new':
        submissions = subreddit.new(limit=limit)
    elif sort_by == 'hot':
        submissions = subreddit.hot(limit=limit)
    elif sort_by == 'rising':
        submissions = subreddit.rising(limit=limit)
    elif sort_by == 'top':
        submissions = subreddit.top(limit=limit, time_filter=time_filter)
    elif sort_by == 'controversial':
        submissions = subreddit.controversial(limit=limit, time_filter=time_filter)
    else:
        print(f'Function currently does not support sorting by {sort_by}.')
        return
    
    for submission in submissions:
        posts.append([submission])
    
    _df = pd.DataFrame(posts)
    _df.columns = ['submission']
    # _df['date_retrieved'] = time_run.strftime("%Y-%m-%d")
    _df['date_retrieved'] = time_run
    _df['subreddit'] = sub_name

    if time_filter:
        if time_filter == 'all':
            tf_str = ' (from all time) '
        else:
            tf_str = f' (in the past {time_filter}) '
    else:
        tf_str = ' '

    print(f'Collected {len(posts)} {sort_by.upper()} submissions{tf_str}from r/{sub_name} as of {time_run.strftime("%Y-%m-%d at %H:%M")}.')
    print('\n')

    if return_time:
        return (_df, time_run)
    else:
        return _df
    
def get_posts_data(
        submission_df, 
        max_attempts=10,
        sleep=30
        ):
    """Returns a DataFrame displaying information about a set of submissions.
    Takes in a DataFrame output by get_reddit_posts() containing a 'submission',
    which should consist of PRAW submission objects.

    If drop_24 is True, drops any submissions less than 24 hours old, which may
    not have a sufficient number of comments/votes for analysis.
    """
    _df = submission_df.copy()
    _df['title'] = _df['submission'].apply(lambda x: x.title)
    _df['created_utc'] = _df['submission'].apply(lambda x: x.created_utc)
    _df['datetime'] = _df['created_utc'].apply(lambda x: dt.datetime.fromtimestamp(x))
    _df['submission_age'] = _df['date_retrieved'] - _df['datetime']
    _df['id'] = _df['submission'].apply(lambda x: x.id)
    _df['url'] = _df['submission'].apply(lambda x: x.url)
    _df['is_self'] = _df['submission'].apply(lambda x: x.is_self)
    _df['selftext'] = _df['submission'].apply(lambda x: x.selftext)
    _df['post_hint'] = _df['submission'].apply(lambda x: x.post_hint if 'post_hint' in vars(x) else None)
    _df['score'] = _df['submission'].apply(lambda x: x.score)
    _df['upvote_ratio'] = _df['submission'].apply(lambda x: x.upvote_ratio)
    _df['num_comments'] = _df['submission'].apply(lambda x: x.num_comments)

    # There is occasionally an HTTP 429 error with the next section, but simply running it again usually works.
    # The code repeats until it doesn't throw an error or reaches max_attempts.
    
    attempt = 1
    try_again = True
    while ((attempt <= max_attempts) and (try_again == True)):
        try:
            _df['comments'] = _df['submission'].apply(lambda x: x.comments)
            try_again = False
            print(f'Successfully extracted comments on attempt {attempt}.')
        except:
            print(f'Attempt {attempt} of extracting comments failed. Max attempts = {max_attempts}.')
            attempt += 1
            try_again = True
            if attempt > max_attempts:
                raise Exception('*** Max attempts of extracting comments reached ***')
            else:
                time.sleep(sleep)
    return _df

### Getting submissions  ###
df, time_run = get_reddit_posts(reddit, sub_name=sub_name, sort_by=sort_by, time_filter=time_filter, limit=1000)
print(time_run)
print('\n')


### Extracting data from submissions ###
# This section repeats until there is no HTTP 429 error or it reaches max_attempts.

attempt = 1
max_attempts = 10
try_again = True
sleep = 30
while ((attempt <= max_attempts) and (try_again)):
    try:
        df = get_posts_data(df)
        try_again = False
        print(f'Successfully extracted submission data on attempt {attempt}.')
    except:
        print(f'Attempt {attempt} of extracting submission data failed. Max attempts = {max_attempts}.')
        attempt += 1
        try_again = True
        if attempt > max_attempts:
            raise Exception('*** Max attempts of get_posts_data() reached ***')
        else:
            time.sleep(sleep)
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

filename = f'{sub_name}_{sort_type}_reddit_data_{time_run.strftime("%m_%d_%Y")}.pkl'

filepath = os.path.join(output_dir + filename)

### Save file ###
df.to_pickle(filepath)

print(f'Saved {filepath}')
print('\n')

time_check()
print('\n')

print('\n')
print('#'*50)
print(f"Script ended at {dt.datetime.now().strftime('%H:%M:%S')}")
print('#'*50)
print('\n')