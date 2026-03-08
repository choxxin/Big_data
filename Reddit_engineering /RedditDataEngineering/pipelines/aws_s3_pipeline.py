# AWS S3 Pipeline - Uploads CSV file to S3
# This is called by the Airflow DAG after Reddit extraction

from etls.aws_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_BUCKET_NAME


def upload_s3_pipeline(ti):
    """
    Upload the CSV file to AWS S3
    
    Steps:
    1. Get file path from previous task (reddit_extraction)
    2. Connect to S3
    3. Create bucket if needed
    4. Upload file to S3
    
    Args:
        ti: Airflow task instance (used to get data from previous task)
    """
    # Get the CSV file path from the previous task
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')

    # Connect to S3
    s3 = connect_to_s3()
    
    # Ensure bucket exists
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
    
    # Upload file (extract filename from path)
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])