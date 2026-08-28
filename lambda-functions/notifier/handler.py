"""
lambda-functions/notifier/handler.py
--------------------------------------
Lambda function that processes ETL job status messages from SQS.

Sits at the end of the SNS → SQS → Lambda fan-out chain. Each time a Glue
or EMR job finishes, it publishes a status message to the SNS topic. SNS
delivers that message to the SQS queue, which triggers this Lambda.

NOTIFICATION FLOW:
  ETL Job (Glue or EMR)
       │ publishes JSON status
       ▼
  SNS: etl-job-notifications
       │
  ┌────┴───────────────────┐
  ▼                         ▼
SQS: etl-job-status-queue  Your Email (subscription)
  │
  ▼
Lambda: etl-notifier  ← (this function)
  │
  ▼
CloudWatch Logs (structured status summary)

HOW TO DEPLOY:
  python infrastructure/deploy_lambdas.py --bucket <your-output-bucket>

  The deploy script also creates the SQS event source mapping that connects
  the queue to this Lambda.

ENVIRONMENT VARIABLES:
  SNS_TOPIC_ARN    — (optional) SNS topic for forwarding alerts on parse errors
  DLQ_ALERT_EMAIL  — informational only; DLQ config is on the SQS queue itself

SQS MESSAGE SHAPE:
  SQS receives SNS notifications, so each SQS message body is an SNS envelope:
  {
    "Type": "Notification",
    "TopicArn": "arn:aws:sns:...",
    "Subject": "ETL Job Status: SUCCEEDED",
    "Message": "{\"job_name\": \"glue-simple-etl\", \"status\": \"SUCCEEDED\", ...}"
  }
  The actual job status payload is the JSON string inside "Message".

EXPECTED JOB STATUS PAYLOAD (inside SNS "Message"):
  {
    "job_name":         "glue-simple-etl",
    "track":            "glue",
    "status":           "SUCCEEDED",
    "start_time":       "2026-07-22T10:00:00Z",
    "end_time":         "2026-07-22T10:08:00Z",
    "duration_seconds": 480,
    "rows_processed":   10000,
    "output_path":      "s3://etl-course-output/glue/sales/"
  }
"""

import json
import logging
import os
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _parse_sqs_record(record: dict) -> dict:
    """
    Extract the job status dict from a single SQS record.

    SQS delivers SNS notifications wrapped in an outer envelope, so we need
    two levels of JSON parsing:
      1. record["body"]      → SNS envelope (Type, TopicArn, Message, ...)
      2. sns_envelope["Message"] → actual job status JSON string
    """
    # First parse: unwrap the SQS body to get the SNS envelope
    sns_envelope = json.loads(record["body"])

    # Second parse: unwrap the SNS Message field to get the job status dict
    job_status = json.loads(sns_envelope["Message"])

    return job_status


def _log_status_summary(job_status: dict) -> None:
    """
    Log a structured status summary to CloudWatch Logs.

    Using json.dumps keeps the log entry machine-parseable so CloudWatch
    Logs Insights can query it with filter @message like '{ $.status = "FAILED" }'.
    """
    summary = {
        "event":            "job_status_received",
        "job_name":         job_status.get("job_name"),
        "track":            job_status.get("track"),
        "status":           job_status.get("status"),
        "stage":            job_status.get("stage"),           # present in pipeline jobs
        "duration_seconds": job_status.get("duration_seconds"),
        "rows_processed":   job_status.get("rows_processed"),
        "output_path":      job_status.get("output_path"),
        "received_at":      datetime.now(timezone.utc).isoformat(),
    }
    # Remove None values to keep logs clean
    summary = {k: v for k, v in summary.items() if v is not None}

    if job_status.get("status") == "FAILED":
        logger.error(json.dumps(summary))
    else:
        logger.info(json.dumps(summary))


def _maybe_alert_on_failure(job_status: dict) -> None:
    """
    If the job failed, publish a high-priority SNS alert (if topic is configured).

    In a production setup you would wire a PagerDuty or Slack integration to
    a separate SNS topic for failures. This function demonstrates the pattern.
    """
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    if not sns_topic_arn:
        return
    if job_status.get("status") != "FAILED":
        return

    sns = boto3.client("sns", region_name="us-east-1")
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject=f"[ALERT] ETL Job FAILED: {job_status.get('job_name', 'unknown')}",
        Message=json.dumps({
            "alert_type":  "etl_job_failure",
            "job_name":    job_status.get("job_name"),
            "track":       job_status.get("track"),
            "output_path": job_status.get("output_path"),
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }),
    )


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point called by AWS Lambda when SQS delivers messages.

    Lambda automatically batches up to 10 SQS records per invocation
    (configurable via the event source mapping batch size). We process
    each record independently so a single bad message does not block
    the rest of the batch.
    """
    records = event.get("Records", [])
    logger.info(f"[INFO] Processing {len(records)} SQS record(s).")

    processed = 0
    errors = []

    for i, record in enumerate(records):
        try:
            job_status = _parse_sqs_record(record)
            _log_status_summary(job_status)
            _maybe_alert_on_failure(job_status)
            processed += 1

        except json.JSONDecodeError as e:
            # Malformed message — log and continue; bad message goes to DLQ
            # after max receive count is reached on the SQS queue.
            error_msg = f"Record {i}: JSON parse error — {e}"
            logger.error(error_msg)
            errors.append(error_msg)

        except Exception as e:
            error_msg = f"Record {i}: Unexpected error — {e}"
            logger.error(error_msg)
            errors.append(error_msg)

    logger.info(json.dumps({
        "event": "batch_complete",
        "total_records": len(records),
        "processed": processed,
        "errors": len(errors),
    }))

    # Returning normally (no exception) tells SQS the batch was processed
    # successfully and deletes the messages from the queue.
    # If we raise an exception, SQS retries the entire batch.
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed,
            "errors": len(errors),
        }),
    }
