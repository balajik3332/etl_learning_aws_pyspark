# Task 4 — Lambda Trigger Functions

## What Are We Building?

Three Lambda functions that implement all the ways an ETL job can be started:

1. **S3 Event Trigger** — When a new file lands in S3, Lambda automatically starts the ETL job
2. **Scheduled Trigger** — Lambda runs on a cron schedule (covered in depth in Task 5)
3. **Notifier** — Lambda reads job status messages from SQS and logs structured summaries

This demonstrates the event-driven architecture pattern used in real data pipelines.

---

## Concepts Explained

### What is AWS Lambda?
Lambda lets you run Python code without managing any server. You upload a function (a `.py` file), and AWS runs it whenever a trigger fires. You pay only for the milliseconds your code runs. Lambda has a maximum runtime of 15 minutes per invocation.

### What are the three trigger types?

| Type | Who fires the trigger | Use case |
|------|----------------------|----------|
| Event-driven | S3 fires it automatically when a file is uploaded | Process data as soon as it arrives |
| Scheduled | CloudWatch fires it on a cron schedule | Daily batch jobs at midnight |
| On-demand | You fire it manually from Console or CLI | Testing, ad-hoc reruns |

### How does S3 → Lambda → Glue/EMR work?

```
New file uploaded to S3 landing bucket
           │
           ▼
   S3 Event Notification
           │
           ▼
  Lambda: s3_event_trigger
           │
    ┌──────┴──────┐
    ▼             ▼
glue.start_job_run  emr_serverless.start_job_run
    │             │
    ▼             ▼
  Glue Job     EMR Job
    │             │
    └──────┬──────┘
           ▼
   SNS: publish job status
```

Which track (Glue or EMR) the Lambda uses is controlled by an **environment variable** — no code change needed to switch.

---

## Prerequisites

- [ ] Task 1 complete (IAM roles exist)
- [ ] Task 2 complete (data files exist in landing bucket)
- [ ] Task 3A or 3B complete (at least one ETL job registered)

---

## Step 1 — Deploy the Lambda Functions

```bash
python infrastructure/deploy_lambdas.py --bucket etl-course-yourname-output
```

This script:
1. Zips each `handler.py` file into a deployment package
2. Uploads the zip to S3
3. Creates the Lambda function with the correct IAM role and environment variables
4. Configures S3 event notifications to invoke the `s3_event_trigger` Lambda

**Expected output:**
```
[OK] Deployed: etl-s3-event-trigger  (role: BatchETL-LambdaTriggerRole)
[OK] Deployed: etl-scheduled-trigger (role: BatchETL-LambdaTriggerRole)
[OK] Deployed: etl-notifier          (role: BatchETL-LambdaNotifierRole)
[OK] S3 event notification configured: etl-course-yourname-landing → etl-s3-event-trigger
[OK] SQS event source mapped: etl-job-status-queue → etl-notifier
```

---

## Step 2 — Configure S3 Event Notification (Console)

If you prefer to configure manually:

1. Go to **S3** → landing bucket → **Properties** tab
2. Scroll to **Event notifications** → **Create event notification**
3. Settings:
   - **Event name**: `new-data-arrived`
   - **Event types**: ✅ `s3:ObjectCreated:*` (all PUT/POST/COPY events)
   - **Destination**: Lambda → select `etl-s3-event-trigger`
4. Click **Save changes**

---

## Step 3 — Test the Event-Driven Trigger

Generate and upload a small batch of data:

```bash
python data-generator/generate_sales.py --rows 100
python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing
```

Within a few seconds, the Lambda will fire automatically.

**Verify in CloudWatch Logs:**
1. Go to **CloudWatch** → **Log groups** → `/aws/lambda/etl-s3-event-trigger`
2. Click the most recent log stream
3. You should see a structured JSON log entry like:
```json
{
  "event": "job_started",
  "track": "glue",
  "job_name": "glue-simple-etl",
  "source_path": "s3://etl-course-yourname-landing/sales/sales_20240115.csv",
  "timestamp": "2024-01-15T10:00:00Z"
}
```

**Verify in Glue or EMR Console:**
- Glue: **AWS Glue** → **Jobs** → `glue-simple-etl` → should show a new run starting
- EMR: **EMR** → **Serverless** → your application → **Job runs** → new run visible

---

## Step 4 — Test On-Demand Trigger

You can invoke any Lambda function manually without uploading data:

```bash
aws lambda invoke \
  --function-name etl-s3-event-trigger \
  --payload '{"track":"glue","job_type":"simple-etl","source_path":"s3://etl-course-yourname-landing/sales/"}' \
  --cli-binary-format raw-in-base64-out \
  response.json

cat response.json
```

Expected response:
```json
{"statusCode": 200, "body": "Job started: glue-simple-etl"}
```

---

## The Lambda Code — Line by Line

### lambda-functions/s3_event_trigger/handler.py

```python
import os
import json
import boto3
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Triggered by S3 ObjectCreated events OR direct invocation.
    Starts either a Glue job or EMR Serverless job based on TRACK env var.
    """
    # 1. Read which track to use from environment variables
    #    Set TRACK='glue' or TRACK='emr' when deploying the Lambda
    track = os.environ.get("TRACK", "glue")

    # 2. Extract the S3 path from the event payload
    #    S3 events look like: event['Records'][0]['s3']['bucket']['name']
    if "Records" in event:
        record = event["Records"][0]["s3"]
        bucket = record["bucket"]["name"]
        key = record["object"]["key"]
        source_path = f"s3://{bucket}/{key}"
        job_type = _infer_job_type(key)  # e.g. 'sales/' → 'simple-etl'
    else:
        # Direct invocation with explicit parameters
        source_path = event.get("source_path", "")
        job_type = event.get("job_type", "simple-etl")

    output_path = os.environ.get(
        "OUTPUT_PATH",
        f"s3://etl-course-output/{track}/{job_type}/"
    )

    # 3. Start the appropriate job
    if track == "glue":
        job_run_id = _start_glue_job(job_type, source_path, output_path)
    else:
        job_run_id = _start_emr_job(job_type, source_path, output_path)

    # 4. Log structured JSON to CloudWatch
    log_entry = {
        "event": "job_started",
        "track": track,
        "job_type": job_type,
        "job_run_id": job_run_id,
        "source_path": source_path,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(log_entry))

    return {"statusCode": 200, "body": f"Job started: {job_run_id}"}
```

### lambda-functions/notifier/handler.py

```python
def lambda_handler(event, context):
    """
    Triggered by SQS messages (which came from SNS).
    Parses each job status message and logs a structured summary.
    """
    for record in event["Records"]:
        # SQS body is the SNS notification JSON string
        sns_message = json.loads(record["body"])
        job_status = json.loads(sns_message["Message"])

        logger.info(json.dumps({
            "event": "job_status_received",
            "job_name":         job_status.get("job_name"),
            "track":            job_status.get("track"),
            "status":           job_status.get("status"),
            "duration_seconds": job_status.get("duration_seconds"),
            "rows_processed":   job_status.get("rows_processed"),
        }))
```

---

## Step 5 — Verify the Notifier Lambda

After a job completes, it publishes to SNS → SNS puts a message on SQS → SQS triggers the Notifier Lambda.

Check the notifier logs:
1. **CloudWatch** → **Log groups** → `/aws/lambda/etl-notifier`
2. You should see a structured log line like:
```json
{
  "event": "job_status_received",
  "job_name": "glue-simple-etl",
  "track": "glue",
  "status": "SUCCEEDED",
  "duration_seconds": 312,
  "rows_processed": 1000
}
```

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| Lambda not triggered by S3 upload | Event notification not configured | Run `deploy_lambdas.py` again or check S3 → Properties → Event notifications |
| `AccessDeniedException: glue:StartJobRun` | Lambda role missing Glue permission | Check `BatchETL-LambdaTriggerRole` has inline policy for Glue |
| Lambda timeout (15 min limit) | Job submission itself shouldn't take long | Lambda only *starts* the job, doesn't wait for it |
| `Function not found` | Wrong function name in CLI invoke | Check exact name: `etl-s3-event-trigger` |
| Notifier Lambda not firing | SQS event source mapping missing | Re-run `deploy_lambdas.py` to create the mapping |
| `JSONDecodeError` in notifier | SNS wraps message in outer JSON | Access `json.loads(record['body'])['Message']` |

---

## Cost Impact

- Lambda: First 1 million requests/month are free. Each ETL run costs < 1 request.
- S3 event notifications: Free
- **Estimated cost for this task: $0.00**

---

## Next Step

→ `docs/05-scheduled-trigger-guide.md` — Configure a CloudWatch cron rule to run your ETL job automatically every day.
