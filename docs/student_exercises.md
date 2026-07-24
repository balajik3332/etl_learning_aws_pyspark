# Student Exercises — Hands-On Challenges

These five exercises extend what you built in the course. Each one is self-contained — you can do them in any order after completing the main tasks.

---

## Exercise 1 — Add a Region Filter to Simple ETL

**Difficulty**: Beginner  
**Estimated time**: 30–45 minutes

**Task**: Modify `glue_simple_etl.py` and `emr_simple_etl.py` to accept an optional `--region_filter` parameter. When provided, only process rows where `region == region_filter`.

**Requirements:**
- Parameter is optional — if not provided, process all regions (existing behaviour)
- Add a log line showing how many rows were filtered out
- Test with `--region_filter east` and confirm only east-region rows appear in output

**Hints:**
- Glue: add `region_filter` to `getResolvedOptions` with a default of `None`
- EMR: add `sys.argv[3]` with a check `if len(sys.argv) > 3`
- PySpark filter: `df.filter(col("region") == region_filter)` when filter is set

**Stretch goal**: Accept a comma-separated list of regions (`--region_filter east,west`).

---

## Exercise 2 — Store CDC Checkpoint in DynamoDB

**Difficulty**: Intermediate  
**Estimated time**: 1–2 hours

**Task**: Modify `emr_cdc_job.py` to store the checkpoint in a DynamoDB table instead of an S3 JSON file.

**Why this matters**: DynamoDB checkpoints are more robust than S3 files — they support conditional writes (preventing two jobs from updating the checkpoint simultaneously) and are easier to query.

**Requirements:**
- Create a DynamoDB table `etl-cdc-checkpoints` with partition key `job_name` (string)
- On startup, read `last_processed_timestamp` from DynamoDB using `job_name = "emr-cdc"`
- After a successful run, update the item with the new timestamp using a conditional write
- If the table/item doesn't exist, treat as first run (process all data)

**Hints:**
```python
import boto3
dynamo = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamo.Table("etl-cdc-checkpoints")

# Read
response = table.get_item(Key={"job_name": "emr-cdc"})
last_ts = response.get("Item", {}).get("last_processed_timestamp", "1970-01-01T00:00:00Z")

# Write (conditional — only update if timestamp has advanced)
table.put_item(Item={"job_name": "emr-cdc", "last_processed_timestamp": new_ts})
```

**Stretch goal**: Add `rows_processed` and `last_run_at` fields to the DynamoDB item.

---

## Exercise 3 — Add a Data Quality Report to the Multi-Step Pipeline

**Difficulty**: Intermediate  
**Estimated time**: 1–2 hours

**Task**: Add a Step 0 (before Raw) that runs a data quality check on the incoming file and writes a quality report to S3 before any ETL runs.

**Requirements:**
- New script: `glue_step0_quality_check.py` (or `emr_step0_quality_check.py`)
- The script should compute and output a JSON report containing:
  - Total row count
  - Null count per column
  - Min/max/mean for numeric columns
  - Count of duplicate `order_id` values
  - Pass/fail result: PASS if null rate < 5% on critical columns, FAIL otherwise
- If the quality check FAILs, the workflow should stop (raise an exception)
- Report saved to: `s3://etl-course-yourname-output/quality-reports/YYYY-MM-DD/report.json`

**Hints:**
- Use `df.describe()` for numeric stats
- Use `df.filter(col("order_id").isNull()).count()` for null counts
- Use `df.groupBy("order_id").count().filter(col("count") > 1).count()` for duplicates

**Stretch goal**: Send the quality report as a formatted message to your SNS topic.

---

## Exercise 4 — SQS as Event Source Instead of S3

**Difficulty**: Intermediate  
**Estimated time**: 1–2 hours

**Task**: Create a new Lambda trigger that starts an ETL job when a message is published to an SQS queue — instead of being triggered by an S3 file upload.

**Why this matters**: SQS-triggered ETL is common when an upstream service (a web app, another pipeline, an external system) needs to signal "new data is ready" without directly uploading to S3.

**Requirements:**
- Create a new SQS queue: `etl-job-trigger-queue`
- Create a new Lambda function: `etl-sqs-trigger`
- Lambda reads a message like `{"source_path": "s3://...", "job_type": "simple-etl", "track": "glue"}`
- Lambda starts the appropriate Glue or EMR job with the provided parameters
- Configure the queue as an event source for the Lambda
- Test by manually sending a message to the queue:

```bash
aws sqs send-message \
  --queue-url <QUEUE_URL> \
  --message-body '{"source_path":"s3://etl-course-yourname-landing/sales/","job_type":"simple-etl","track":"glue"}'
```

**Stretch goal**: Add message validation — if the message is missing required fields, move it to a DLQ instead of trying to start a job.

---

## Exercise 5 — Run the Full Pipeline on a Different Public Dataset

**Difficulty**: Advanced  
**Estimated time**: 2–3 hours

**Task**: Choose a different public dataset, adapt the pipeline to its schema, and run the full end-to-end flow (Simple ETL → CDC → Multi-step Pipeline).

**Suggested dataset**: NOAA Global Surface Summary of Day (GSOD) — daily weather readings from thousands of weather stations worldwide.

**Public S3 location**: `s3://noaa-gsod-pds/` (public, no credentials needed)

**Schema highlights:**
- `STATION` — station ID
- `DATE` — observation date (YYYY-MM-DD)
- `LATITUDE`, `LONGITUDE`
- `TEMP` — mean temperature (°F), `9999.9` means missing
- `PRCP` — precipitation amount
- `WDSP` — mean wind speed

**Requirements:**
1. Write `public-datasets/fetch_noaa_weather.py` to download one year of data for a few stations
2. Handle the `9999.9` sentinel value for missing data (replace with null)
3. Adapt the multi-step pipeline's clean stage to handle weather-specific quality issues
4. Aggregate stage should produce: average temperature by month and station
5. Run both Glue and EMR versions

**Hints:**
```bash
# List available files
aws s3 ls s3://noaa-gsod-pds/2023/ --no-sign-request | head -20

# Download a specific year
aws s3 cp s3://noaa-gsod-pds/2023/ /tmp/noaa-2023/ \
  --recursive --no-sign-request --exclude "*" --include "72*.csv"
```

**Stretch goal**: Compare average NYC area temperature in January 2023 vs January 2022. Are winters getting warmer?

---

## Submission Checklist

For each exercise you complete, verify:

- [ ] Code runs without errors on your local PySpark session (`pytest` passes)
- [ ] Code runs successfully as a Glue job or EMR job on AWS
- [ ] Output files exist in S3 with correct schema
- [ ] No AWS resources left running after the exercise
