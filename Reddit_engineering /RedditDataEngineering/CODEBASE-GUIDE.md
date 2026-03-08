# Codebase Guide - Reddit Data Engineering Pipeline

## Execution Flow (What Runs First)

### 1. Setup Phase (Run Once)
```
config.conf → Docker setup → Airflow initialization
```

### 2. Runtime Phase (Daily Automatic)
```
reddit_dag.py → reddit_pipeline.py → aws_s3_pipeline.py
```

---

## File Execution Order

### Step 1: Configuration (First Time Setup)
**File:** `config/config.conf`
- Contains all credentials and settings
- Must be created from `config.conf.example`
- Loaded by all other files

### Step 2: Docker Setup
**Files:** `Dockerfile` → `docker-compose.yml`
- Dockerfile builds the container image
- docker-compose.yml starts all services (Postgres, Redis, Airflow)

### Step 3: Airflow DAG (Scheduler Triggers This Daily)
**File:** `dags/reddit_dag.py`
- Entry point for the pipeline
- Runs automatically every day
- Defines 2 tasks: extract from Reddit → upload to S3

### Step 4: Reddit Extraction
**Flow:** `reddit_dag.py` → `pipelines/reddit_pipeline.py` → `etls/reddit_etl.py`
- Connects to Reddit API
- Extracts posts from subreddit
- Transforms data (clean, format)
- Saves to CSV file

### Step 5: AWS Upload
**Flow:** `reddit_dag.py` → `pipelines/aws_s3_pipeline.py` → `etls/aws_etl.py`
- Connects to AWS S3
- Creates bucket if needed
- Uploads CSV file to S3

---

## File Purposes

### Configuration Files
- `config/config.conf` - All credentials and settings
- `airflow.env` - Airflow environment variables
- `requirements.txt` - Python packages to install
- `.gitignore` - Files to exclude from git

### Docker Files
- `Dockerfile` - Recipe to build Airflow container
- `docker-compose.yml` - Runs multiple containers together

### Core Pipeline Files
- `dags/reddit_dag.py` - Orchestrates the entire pipeline
- `pipelines/reddit_pipeline.py` - Reddit extraction workflow
- `pipelines/aws_s3_pipeline.py` - AWS upload workflow

### ETL Logic Files
- `etls/reddit_etl.py` - Reddit API connection and data processing
- `etls/aws_etl.py` - AWS S3 connection and upload

### Utility Files
- `utils/constants.py` - Loads config and defines constants

### Data Folders
- `data/input/` - Input files (if any)
- `data/output/` - CSV files saved here before S3 upload

---

## Quick Start Order

1. Copy `config/config.conf.example` to `config/config.conf` and add credentials
2. Run `docker-compose up -d` to start all services
3. Open `localhost:8080` in browser (Airflow UI)
4. The DAG runs automatically daily, or trigger manually from UI

---

## Data Flow Summary

```
Reddit API 
  ↓
reddit_etl.py (extract & transform)
  ↓
CSV file in data/output/
  ↓
aws_etl.py (upload)
  ↓
AWS S3 Bucket
  ↓
AWS Glue (catalog data)
  ↓
AWS Athena (query data)
  ↓
AWS Redshift (analytics)
```

---

## Data Transformation Example

### What data looks like BEFORE transformation (raw from Reddit):

```python
{
    'id': 'abc123',
    'title': 'Best ETL tools',
    'score': 1250,
    'num_comments': 87,
    'author': <Redditor object>,           # Object, not string
    'created_utc': 1709636400,             # Unix timestamp (hard to read)
    'url': 'https://reddit.com/...',
    'over_18': True,
    'edited': 1709640000,                  # Could be timestamp OR False
    'spoiler': False,
    'stickied': False
}
```

### What data looks like AFTER transformation (cleaned):

```python
{
    'id': 'abc123',
    'title': 'Best ETL tools',
    'score': 1250,
    'num_comments': 87,
    'author': 'data_guru',                 # Now a string
    'created_utc': '2024-03-05 10:30:00',  # Readable datetime
    'url': 'https://reddit.com/...',
    'over_18': True,
    'edited': True,                        # Standardized to True/False
    'spoiler': False,
    'stickied': False
}
```

### Transformations applied:

1. Convert Unix timestamp to readable datetime: `1709636400` → `2024-03-05 10:30:00`
2. Convert author object to string: `<Redditor object>` → `"data_guru"`
3. Standardize 'edited' field to True/False (Reddit returns timestamp if edited)
4. Ensure correct data types (integers, strings, booleans)

### Final CSV output:

```csv
id,title,score,num_comments,author,created_utc,url,over_18,edited,spoiler,stickied
abc123,Best ETL tools,1250,87,data_guru,2024-03-05 10:30:00,https://reddit.com/...,True,False,False,False
def456,Help with Airflow,45,12,newbie_dev,2024-03-05 09:15:00,https://reddit.com/...,False,True,False,False
```

This cleaned CSV is uploaded to S3 for analysis by AWS Glue, Athena, and Redshift.
