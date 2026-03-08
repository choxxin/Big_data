# Reddit ETL - Extract, Transform, Load functions for Reddit data
# This file handles connecting to Reddit API and processing post data

import sys

import numpy as np
import pandas as pd
import praw
from praw import Reddit

from utils.constants import POST_FIELDS


def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    """
    Connect to Reddit API using credentials
    Returns: Reddit instance for making API calls
    """
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("connected to reddit!")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)


def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    """
    Extract top posts from a subreddit
    Args:
        reddit_instance: Connected Reddit API instance
        subreddit: Name of subreddit (e.g., 'dataengineering')
        time_filter: Time period ('day', 'week', 'month', 'year')
        limit: Number of posts to extract (None = all)
    Returns: List of post dictionaries
    """
    subreddit = reddit_instance.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)

    post_lists = []

    # Extract only the fields we need from each post
    for post in posts:
        post_dict = vars(post)
        post = {key: post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)

    return post_lists


def transform_data(post_df: pd.DataFrame):
    """
    Clean and format the Reddit data
    - Convert timestamps to datetime
    - Standardize boolean fields
    - Ensure correct data types
    """
    # Convert Unix timestamp to readable datetime
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    
    # Standardize boolean fields
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    
    # Handle edited field (can be True/False or timestamp)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    
    # Ensure numeric fields are integers
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df


def load_data_to_csv(data: pd.DataFrame, path: str):
    """
    Save DataFrame to CSV file
    """
    data.to_csv(path, index=False)
