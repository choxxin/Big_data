# Reddit DAG - Main Airflow workflow definition
# THIS IS THE ENTRY POINT - Airflow runs this file daily
# Defines the pipeline: Extract from Reddit → Upload to S3

import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Add parent directory to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

# DAG configuration
default_args = {
    'owner': 'Yusuf Ganiyu',
    'start_date': datetime(2023, 10, 22)
}

# Add today's date to filename (e.g., reddit_20240305)
file_postfix = datetime.now().strftime("%Y%m%d")

# Create the DAG (workflow)
dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Run once per day
    catchup=False,  # Don't run for past dates
    tags=['reddit', 'etl', 'pipeline']
)

# Task 1: Extract data from Reddit and save to CSV
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',  # Which subreddit to scrape
        'time_filter': 'day',  # Get top posts from today
        'limit': 100  # Get 100 posts
    },
    dag=dag
)

# Task 2: Upload CSV file to AWS S3
upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipeline,
    dag=dag
)

# Define task order: extract runs first, then upload_s3
extract >> upload_s3
