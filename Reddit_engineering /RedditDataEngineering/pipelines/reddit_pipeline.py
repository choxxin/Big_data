# Reddit Pipeline - Orchestrates the Reddit data extraction process
# This is called by the Airflow DAG

import pandas as pd

from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH


def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    """
    Complete pipeline to extract Reddit data and save to CSV
    
    Steps:
    1. Connect to Reddit API
    2. Extract posts from subreddit
    3. Transform/clean the data
    4. Save to CSV file
    
    Args:
        file_name: Name for output CSV file
        subreddit: Subreddit to extract from (e.g., 'dataengineering')
        time_filter: Time period for top posts ('day', 'week', etc.)
        limit: Max number of posts to extract
        
    Returns: Path to saved CSV file
    """
    # Step 1: Connect to Reddit
    instance = connect_reddit(CLIENT_ID, SECRET, 'Airscholar Agent')
    
    # Step 2: Extract posts
    posts = extract_posts(instance, subreddit, time_filter, limit)
    post_df = pd.DataFrame(posts)
    
    # Step 3: Transform/clean data
    post_df = transform_data(post_df)
    
    # Step 4: Save to CSV
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path
