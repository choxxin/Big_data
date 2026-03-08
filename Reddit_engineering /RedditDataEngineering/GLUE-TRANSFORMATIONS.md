# AWS Glue Transformations Explained

## What This Script Does

Takes your raw Reddit CSV from S3 and adds meaningful analytics fields for business intelligence.

---

## Transformations Applied

### 1. Popularity Category
Categorizes posts by engagement level:
- **Viral**: score >= 1000
- **Popular**: score >= 100
- **Normal**: score >= 10
- **Low engagement**: score < 10

**Use case:** Identify trending content

---

### 2. Engagement Rate
Calculates comments per upvote ratio:
```
engagement_rate = num_comments / score
```

**Use case:** Find posts that spark discussion vs just upvotes

---

### 3. Date Components
Breaks down timestamp into:
- `post_date` - Full date
- `post_year` - Year (2024)
- `post_month` - Month (1-12)
- `post_day` - Day of month (1-31)
- `post_hour` - Hour of day (0-23)
- `post_day_of_week` - Day (1=Sunday, 7=Saturday)

**Use case:** Analyze posting patterns (best time to post, weekly trends)

---

### 4. Content Type
Categorizes posts by URL:
- **Image**: i.redd.it, imgur
- **Video**: v.redd.it, youtube
- **Text post**: reddit.com/r/
- **External link**: other URLs

**Use case:** See which content types perform best

---

### 5. Special Post Flags
Boolean flags for filtering:
- `is_highly_commented` - More than 50 comments
- `is_controversial` - Low score but high comments
- `is_viral` - Score over 1000

**Use case:** Quick filtering in analytics dashboards

---

### 6. Author Status
Identifies system vs real users:
- **System**: [deleted], [removed], AutoModerator
- **Active**: Real users

**Use case:** Filter out deleted accounts from analytics

---

### 7. Title Analysis
Analyzes title length:
- `title_length` - Character count
- `title_length_category`:
  - Short: < 50 chars
  - Medium: 50-100 chars
  - Long: > 100 chars

**Use case:** Does title length affect engagement?

---

### 8. Processing Metadata
Adds tracking fields:
- `processed_timestamp` - When Glue processed this
- `data_source` - Always "reddit_api"

**Use case:** Data lineage and debugging

---

## Output Format

### Changed from CSV to Parquet
- **Parquet** is columnar format (faster queries)
- **Snappy compression** (smaller file size)
- **Partitioned by year/month** (faster filtering)

### File Structure in S3:
```
s3://ansh-reddit-processed/
├── post_year=2024/
│   ├── post_month=1/
│   │   └── data.parquet
│   ├── post_month=2/
│   │   └── data.parquet
│   └── post_month=3/
│       └── data.parquet
```

---

## Example: Before vs After

### Before (Raw CSV):
```csv
id,title,score,num_comments,author,created_utc,url,over_18,edited,spoiler,stickied
abc123,Best ETL tools,1250,87,data_guru,2024-03-05 10:30:00,https://reddit.com/...,False,False,False,False
```

### After (Processed Parquet):
```csv
id,title,title_length,title_length_category,score,num_comments,author,author_status,created_utc,post_date,post_year,post_month,post_day,post_hour,post_day_of_week,url,content_type,over_18,edited,spoiler,stickied,popularity_category,engagement_rate,is_highly_commented,is_controversial,is_viral,processed_timestamp,data_source
abc123,Best ETL tools,14,short,1250,87,data_guru,active,2024-03-05 10:30:00,2024-03-05,2024,3,5,10,3,https://reddit.com/...,text_post,False,False,False,False,viral,0.0696,True,False,True,2024-03-05 11:00:00,reddit_api
```

---

## How to Use in AWS Glue

1. Go to AWS Glue Console
2. Click "ETL Jobs" → "Script Editor"
3. Copy the entire `glue_transformation_script.py` content
4. Paste into the editor
5. Update bucket names if needed:
   - `s3://ansh-reddit-raw` (input)
   - `s3://ansh-reddit-processed` (output)
6. Save and run the job

---

## Query Examples in Athena

After Glue processes the data, query it with Athena:

```sql
-- Find viral posts
SELECT title, score, num_comments, engagement_rate
FROM reddit_processed
WHERE is_viral = true
ORDER BY score DESC;

-- Best time to post
SELECT post_hour, AVG(score) as avg_score
FROM reddit_processed
GROUP BY post_hour
ORDER BY avg_score DESC;

-- Content type performance
SELECT content_type, 
       COUNT(*) as post_count,
       AVG(score) as avg_score,
       AVG(num_comments) as avg_comments
FROM reddit_processed
GROUP BY content_type;

-- Monthly trends
SELECT post_year, post_month, 
       COUNT(*) as total_posts,
       AVG(score) as avg_score
FROM reddit_processed
GROUP BY post_year, post_month
ORDER BY post_year, post_month;
```

---

## Benefits of These Transformations

1. **Analytics-ready**: No need to calculate in queries
2. **Faster queries**: Pre-computed fields
3. **Better insights**: Categorization makes patterns visible
4. **Partitioned data**: Queries only scan relevant months
5. **Compressed format**: Lower storage costs


<!-- ffff -->