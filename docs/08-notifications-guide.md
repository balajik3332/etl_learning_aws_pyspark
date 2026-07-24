# Task 8 — SNS/SQS Notifications

## What Are We Building?

A complete notification pipeline so you know immediately when any ETL job succeeds or fails — without manually checking the AWS Console.

```
ETL Job finishes
      │
      ▼
SNS: etl-job-notifications
      │
   ┌──┴──────────────────┐
   ▼                      ▼
SQS: etl-job-status-queue   Your Email
      │
      ▼
Lambda: etl-notifier
      │
      ▼
CloudWatch Logs (structured status)
```

---

## Concepts Explained

### Why Do ETL Jobs Need Notifications?
In production, ETL jobs run overnight. You need to know if they succeeded or failed without manually checking. Email gives human awareness; SQS gives programmatic access so downstream systems can react automatically.

### What is "Fan-out"?
One SNS message goes to multiple destinations simultaneously. Like sending one email to a group — everyone gets it at the same time. Here: your email inbox AND the SQS queue both receive every notification.

### What is a Dead Letter Queue (DLQ)?
If a message in SQS fails to be processed after several attempts, it moves to the DLQ instead of being lost forever. This is a safety net for debugging — malformed or unexpected messages land in the DLQ where you can inspect them.

---

## Prerequisites

- [ ] Task 1 complete (SNS topic and SQS queue exist, email subscription confirmed)
- [ ] At least one ETL job from Tasks 3A/3B complete

---

## The Notification Message Format

Every ETL job (Glue and EMR) publishes this JSON payload to the SNS topic when it finishes:

```json
{
  "job_name": "glue-simple-etl",
  "track": "glue",
  "status": "SUCCEEDED",
  "start_time": "2024-01-15T10:00:00Z",
  "end_time": "2024-01-15T10:08:00Z",
  "duration_seconds": 480,
  "rows_processed": 10000,
  "output_path": "s3://etl-course-output/glue/sales/2024-01-15/"
}
```

---

## Step 1 — Verify Your Email Subscription is Confirmed

```bash
aws sns list-subscriptions-by-topic \
  --topic-arn $(aws sns list-topics --query "Topics[?contains(TopicArn,'etl-job-notifications')].TopicArn" --output text) \
  --query "Subscriptions[*].{Protocol:Protocol,Endpoint:Endpoint,Status:SubscriptionArn}"
```

Your email should show a full ARN (not `PendingConfirmation`). If still pending, check your spam folder.

---

## Step 2 — Run a Job and Check for the Email

Run the simple ETL job:
```bash
aws glue start-job-run \
  --job-name glue-simple-etl \
  --arguments '{"--source_path":"s3://etl-course-yourname-landing/sales/","--output_path":"s3://etl-course-yourname-output/glue/sales/"}'
```

Within a few minutes of the job completing, you should receive an email with subject:
`AWS Notification — ETL Job Status: SUCCEEDED`

---

## Step 3 — Check the SQS Queue

```bash
# Poll for messages (won't delete them — just reads)
aws sqs receive-message \
  --queue-url $(aws sqs get-queue-url --queue-name etl-job-status-queue --query QueueUrl --output text) \
  --max-number-of-messages 5 \
  --query "Messages[*].Body"
```

You should see the JSON notification payload wrapped in an SNS envelope.

Or in the Console:
1. **SQS** → `etl-job-status-queue` → **Send and receive messages**
2. Click **Poll for messages**

---

## Step 4 — Check the Notifier Lambda Logs

The notifier Lambda automatically processes SQS messages and logs a structured summary:

1. **CloudWatch** → **Log groups** → `/aws/lambda/etl-notifier`
2. Click the most recent log stream
3. Look for entries like:

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

## Step 5 — Trigger a Failure Notification

Deploy a Glue job with a deliberate error to test failure alerts:

```bash
# Register a broken version of the job temporarily
aws glue create-job \
  --name glue-test-failure \
  --role BatchETL-GlueServiceRole \
  --command '{"Name":"glueetl","ScriptLocation":"s3://etl-course-yourname-output/scripts/nonexistent.py"}'

aws glue start-job-run --job-name glue-test-failure
```

Within a few minutes you should receive a failure email. Clean up:
```bash
aws glue delete-job --job-name glue-test-failure
```

---

## How Each Job Publishes Its Status

Add this pattern to the end of every ETL script (Glue and EMR):

```python
import boto3, json
from datetime import datetime, timezone

def publish_job_status(job_name, track, status, start_time, rows_processed, output_path):
    """Called at the end of every ETL job to notify SNS."""
    sns = boto3.client("sns", region_name="us-east-1")
    topic_arn = os.environ.get("SNS_TOPIC_ARN", "")

    if not topic_arn:
        return  # Skip if not configured (local testing)

    end_time = datetime.now(timezone.utc)
    payload = {
        "job_name": job_name,
        "track": track,
        "status": status,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_seconds": int((end_time - start_time).total_seconds()),
        "rows_processed": rows_processed,
        "output_path": output_path,
    }
    sns.publish(
        TopicArn=topic_arn,
        Subject=f"ETL Job Status: {status}",
        Message=json.dumps(payload, indent=2),
    )
```

In Glue scripts, wrap the job in try/except:
```python
try:
    # ... ETL logic ...
    publish_job_status(job_name, "glue", "SUCCEEDED", start_time, row_count, output_path)
    job.commit()
except Exception as e:
    publish_job_status(job_name, "glue", "FAILED", start_time, 0, "")
    raise
```

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| No email received after job completes | Subscription not confirmed | Re-confirm email, check spam |
| SQS queue empty after job completes | SNS→SQS subscription missing | Re-run `sns_sqs_setup.py` |
| Notifier Lambda not firing | SQS event source mapping missing | Re-run `deploy_lambdas.py` |
| DLQ growing | Malformed messages | Inspect DLQ messages: check JSON schema |
| `AuthorizationError` on SNS publish | Job IAM role missing `sns:Publish` | Add `AmazonSNSFullAccess` to the job's role |

---

## Cost Impact

- SNS: first 1M publishes/month free
- SQS: first 1M requests/month free
- Lambda notifier: first 1M invocations/month free
- **Estimated cost for this task: $0.00**

---

## Next Step

→ `docs/09-public-dataset-guide.md` — Apply all your ETL patterns to real-world NYC Taxi data.
