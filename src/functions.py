import os
import praw
from prawcore.exceptions import TooManyRequests
import time
import datetime as dt
import pandas as pd

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
    """
    
    mapper = {
        'title': lambda x: x.title,
        'created_utc': lambda x: x.created_utc,
        'id': lambda x: x.id,
        'url': lambda x: x.url,
        'is_self': lambda x: x.is_self,
        'selftext': lambda x: x.selftext,
        'post_hint': lambda x: x.post_hint if 'post_hint' in vars(x) else None,
        'score': lambda x: x.score,
        'upvote_ratio': lambda x: x.upvote_ratio,
        'num_comments': lambda x: x.num_comments,
        'comments': lambda x: x.comments
    }

    _df = submission_df.copy()  
    # There is occasionally an HTTP 429 error with the next section, but simply running each extraction again usually works.
    # The code repeats until it doesn't throw an error or reaches max_attempts.
    for column, function, in mapper.items():
        attempt = 1
        try_again = True
        while ((attempt <= max_attempts) and (try_again == True)):
            try:
                _df[column] = _df['submission'].apply(function)
                try_again = False
                print(f'Successfully extracted {column} on attempt {attempt}.')
            except TooManyRequests:
                print(f'Attempt {attempt} of extracting {column} failed. Max attempts = {max_attempts}.')
                attempt += 1
                try_again = True
                if attempt > max_attempts:
                    raise Exception('*** Max attempts of extracting comments reached ***')
                else:
                    time.sleep(sleep)
    _df['datetime'] = _df['created_utc'].apply(lambda x: dt.datetime.fromtimestamp(x))
    _df['submission_age'] = _df['date_retrieved'] - _df['datetime']

    # Reorder DF
    _df = _df[[
        'submission', 'id', 'subreddit', 'title', 'url', 'selftext', 'is_self', 'post_hint', 'score', 'upvote_ratio', 'num_comments', 'comments',
        'created_utc', 'datetime', 'date_retrieved', 'submission_age'
        ]]

    return _df

def get_next_filename(filename, output_dir):
    """
    Checks if the filename exists in the output directory.
    If versions of this file exist, returns a version with 
    a numerical suffix like "_1" or "_2" to differentiate. 
    """
    base, ext = os.path.splitext(filename)
    path = os.path.join(output_dir, filename)
    i = 1
    if not os.path.exists(path):
        return(filename)
    else:
        while True:
            new_filename = f"{base}_{i}{ext}"
            new_path = os.path.join(output_dir, new_filename)
            if not os.path.exists(new_path):
                return new_filename
            i += 1