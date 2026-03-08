"""
AWS Lambda Function to trigger Glue job when CSV uploaded to S3
Copy this code to AWS Lambda Console
"""

import boto3
import json

def lambda_handler(event, context):
    """
    Triggered when file uploaded to s3://ansh-reddit-raw
    Starts Glue job: Reddit_transformation
    """
    
    # Initialize Glue client
    glue = boto3.client('glue', region_name='us-east-1')
    
    # Get file details from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    file_path = f"s3://{bucket}/{key}"
    
    print(f"File uploaded: {file_path}")
    
    # Only process CSV files
    if not key.endswith('.csv'):
        print("Not a CSV file, skipping")
        return {
            'statusCode': 200,
            'body': json.dumps('Skipped - not a CSV file')
        }
    
    # Start Glue job
    try:
        response = glue.start_job_run(
            JobName='Reddit_transformation',  # Your Glue job name
            Arguments={
                '--input_bucket': bucket,
                '--input_file': key,
                '--input_path': file_path
            }
        )
        
        job_run_id = response['JobRunId']
        print(f"Glue job started successfully: {job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Glue job triggered successfully',
                'job_run_id': job_run_id,
                'file': file_path
            })
        }
        
    except Exception as e:
        error_msg = f"Error starting Glue job: {str(e)}"
        print(error_msg)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'file': file_path
            })
        }
