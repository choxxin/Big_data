# AWS ETL - Functions to interact with AWS S3
# Handles connecting to S3 and uploading files

import s3fs
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def connect_to_s3():
    """
    Connect to AWS S3 using credentials from config
    Returns: S3FileSystem object for file operations
    """
    try:
        s3 = s3fs.S3FileSystem(anon=False,
                               key= AWS_ACCESS_KEY_ID,
                               secret=AWS_ACCESS_KEY)
        return s3
    except Exception as e:
        print(e)

def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
    """
    Check if S3 bucket exists, create if it doesn't
    Args:
        s3: Connected S3 instance
        bucket: Name of the S3 bucket
    """
    try:
        if not s3.exists(bucket):
            s3.mkdir(bucket)
            print("Bucket created")
        else :
            print("Bucket already exists")
    except Exception as e:
        print(e)


def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket:str, s3_file_name: str):
    """
    Upload a file to S3 bucket in the 'raw' folder
    Args:
        s3: Connected S3 instance
        file_path: Local path to file
        bucket: S3 bucket name
        s3_file_name: Name to save file as in S3
    """
    try:
        s3.put(file_path, bucket+'/raw/'+ s3_file_name)
        print('File uploaded to s3')
    except FileNotFoundError:
        print('The file was not found')