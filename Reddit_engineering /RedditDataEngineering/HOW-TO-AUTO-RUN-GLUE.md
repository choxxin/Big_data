# How to Auto-Run Glue Job

Three methods to automatically run your Glue job when new files are uploaded.

---

## Method 1: Add to Airflow DAG (Easiest)

Glue runs automatically after Airflow uploads the file.

### Step 1: Install AWS provider

```bash
pip install apache-airflow-providers-amazon
```

### Step 2: Update reddit_dag.py

Add this code:

```python
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Add after upload_s3 task
run_glue = GlueJobOperator(
    task_id='trigger_glue_transformation',
    job_name='reddit_etl_job',  # Your Glue job name in AWS
    region_name='us-east-1',
    aws_conn_id='aws_default',
    dag=dag
)

# Update task chain
extract >> upload_s3 >> run_glue
```

### Step 3: Configure AWS connection in Airflow

Go to Airflow UI → Admin → Connections → Add:
- Connection ID: `aws_default`
- Connection Type: `Amazon Web Services`
- AWS Access Key ID: Your key
- AWS Secret Access Key: Your secret
- Region: `us-east-1`

### Result:
Every day Airflow will: Extract Reddit data → Upload to S3 → Trigger Glue job

---

## Method 2: S3 Event + Lambda (Fully Automatic)

Glue runs immediately when ANY file is uploaded to S3.

### Step 1: Create Lambda function

Go to AWS Lambda Console → Create function:
- Name: `trigger-glue-on-s3-upload`
- Runtime: Python 3.9
- Create function

### Step 2: Add Lambda code

```python
import boto3
import json

def lambda_handler(event, context):
    glue = boto3.client('glue')
    
    # Get file info from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    print(f"New file uploaded: s3://{bucket}/{key}")
    
    # Only process CSV files
    if key.endswith('.csv'):
        try:
            response = glue.start_job_run(
                JobName='reddit_etl_job',
                Arguments={
                    '--input_file': f's3://{bucket}/{key}'
                }
            )
            print(f"Started Glue job: {response['JobRunId']}")
            return {
                'statusCode': 200,
                'body': json.dumps('Glue job started successfully')
            }
        except Exception as e:
            print(f"Error: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error: {str(e)}')
            }
    else:
        print("Not a CSV file, skipping")
        return {
            'statusCode': 200,
            'body': json.dumps('Not a CSV file')
        }
```

### Step 3: Add S3 trigger to Lambda

In Lambda function page:
1. Click "Add trigger"
2. Select "S3"
3. Bucket: `ansh-reddit-raw`
4. Event type: "All object create events" or "PUT"
5. Suffix: `.csv` (optional - only trigger for CSV files)
6. Click "Add"

### Step 4: Give Lambda permissions

Go to Lambda → Configuration → Permissions → Click role name

Add this policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:GetJobRun"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
```

### Result:
Anytime a CSV is uploaded to S3, Lambda triggers Glue automatically.

---

## Method 3: AWS Glue Trigger (Scheduled)

Glue runs on a schedule (e.g., daily at 2 AM).

### Step 1: Create Glue Trigger

Go to AWS Glue Console → Triggers → Add trigger:
- Name: `daily-reddit-etl`
- Trigger type: "Schedule"
- Frequency: "Daily"
- Start time: "02:00 AM"
- Actions: Select your Glue job `reddit_etl_job`

### Step 2: Enable trigger

Click "Enable" on the trigger

### Result:
Glue runs every day at 2 AM, processes all files in S3.

---

## Comparison

| Method | When it runs | Best for |
|--------|-------------|----------|
| Method 1 (Airflow) | After Airflow uploads file | Integrated pipeline |
| Method 2 (Lambda) | Immediately when file uploaded | Real-time processing |
| Method 3 (Schedule) | Daily at specific time | Batch processing |

---

## Recommended Approach

**Use Method 1 (Airflow)** because:
- Everything in one pipeline
- Easy to monitor
- Runs in correct order
- No extra Lambda setup

**Use Method 2 (Lambda)** if:
- Files uploaded from multiple sources
- Need real-time processing
- Want independent trigger

**Use Method 3 (Schedule)** if:
- Simple batch processing
- Don't need Airflow integration
- Process all files at once

---

## Testing

After setup, test by:

1. Upload a CSV to S3:
```bash
aws s3 cp reddit_20231022.csv s3://ansh-reddit-raw/
```

2. Check if Glue job started:
```bash
aws glue get-job-runs --job-name reddit_etl_job
```

3. Monitor in AWS Glue Console → Jobs → reddit_etl_job → Run history
