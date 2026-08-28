"""
lambda-functions/s3_event_trigger/handler.py
----------------------------------------------
Lambda function triggered by S3 ObjectCreated events (or direct invocation).

Automatically starts an ETL job whenever a new file lands in the S3 landing
bucket. Supports both Glue and EMR Serverless via the TRACK environment
variable — no code change needed to switch between them.

TRIGGER SOURCES:
  1. S3 Event Notification — fires automatically on s3:ObjectCreated:* events
  2. Direct invocation     — CLI / Console / another Lambda passes a JSON payload

HOW TO DEPLOY:
  python infrastructure/deploy_lambdas.py --bucket <your-output-bucket>

  Or manually:
    zip handler.zip handler.py
    aws lambda create-function \\
        --function-name etl-s3-event-trigger \\
        --runtime python3.12 \\
        --handler handler.lambda_handler \\
        --role arn:aws:iam::<account>:role/BatchETL-LambdaTriggerRole \\
        --zip-file fileb://handler.zip \\
        --environment Variables='{
            "TRACK":"glue",
            "OUTPUT_PATH":"s3://<output-bucket>/glue/sales/",
            "GLUE_JOB_PREFIX":"glue",
            "EMR_APP_ID":"<app-id>",
            "EMR_ROLE_ARN":"arn:aws:iam::<account>:role/BatchETL-EMRServerlessRole",
            "SCRIPTS_BASE":"s3://<output-bucket>/scripts",
            "SNS_TOPIC_ARN":"arn:aws:sns:us-east-1:<account>:etl-job-notifications"
        }'

ENVIRONMENT VARIABLES:
  TRACK           — "glue" (default) or "emr"
  OUTPUT_PATH     — S3 URI where ETL output is written
  GLUE_JOB_PREFIX — prefix used to build Glue job names (default: "glue")
  EMR_APP_ID      — EMR Serverless application ID (required when TRACK=emr)
  EMR_ROLE_ARN    — IAM role ARN passed to EMR job runs (required when TRACK=emr)
  SCRIPTS_BASE    — S3 base URI where EMR PySpark scripts are stored
  SNS_TOPIC_ARN   — SNS topic ARN for job-started notifications (optional)

S3 EVENT PAYLOAD SHAPE (sent by AWS automatically):
  {
    "Records": [{
      "s3": {
        "bucket": {"name": "<bucket-name>"},
        "object": {"key": "<object-key>"}
      }
    }]
  }

DIRECT INVOCATION PAYLOAD:
  {
    "track":       "glue",           # optional override
    "job_type":    "simple-etl",     # optional override
    "source_path": "s3://..."        # required when invoked directly
  }
"""

import os
import json
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Job-type inference ────────────────────────────────────────────────────────
# Maps S3 key prefixes to the corresponding job type name.
# Extend this dict if you add new data sources.
PREFIX_TO_JOB_TYPE = {
    "sales/":        "simple-etl",
    "cdc/":          "cdc",
    "transactions/": "simple-etl",
}


def _infer_job_type(s3_key: str) -> str:
    """
    Derive the job type from the S3 object key prefix.
    Falls back to 'simple-etl' if no prefix matches.

    Examples:
      'sales/sales_20260722.csv'  → 'simple-etl'
      'cdc/changes_20260722.csv'  → 'cdc'
    """
    for prefix, job_type in PREFIX_TO_JOB_TYPE.items():
        if s3_key.startswith(prefix):
            return job_type
    return "simple-etl"


# ── Glue helpers ──────────────────────────────────────────────────────────────

def _start_glue_job(job_type: str, source_path: str, output_path: str) -> str:
    """
    Start a Glue job run and return the JobRunId.

    The Glue job name is built as '<GLUE_JOB_PREFIX>-<job_type>', e.g.
    'glue-simple-etl' or 'glue-cdc'. The prefix is read from the
    GLUE_JOB_PREFIX environment variable (default: 'glue').
    """
    prefix = os.environ.get("GLUE_JOB_PREFIX", "glue")
    job_name = f"{prefix}-{job_type}"

    glue = boto3.client("glue", region_name="us-east-1")
    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--source_path": source_path,
            "--output_path": output_path,
        },
    )
    run_id = response["JobRunId"]
    logger.info(json.dumps({
        "event": "glue_job_started",
        "job_name": job_name,
        "job_run_id": run_id,
        "source_path": source_path,
    }))
    return run_id


# ── EMR helpers ───────────────────────────────────────────────────────────────

# Maps job_type to the corresponding EMR script filename (without .py).
JOB_TYPE_TO_EMR_SCRIPT = {
    "simple-etl": "emr_simple_etl",
    "cdc":        "emr_cdc_job",
}


def _start_emr_job(job_type: str, source_path: str, output_path: str) -> str:
    """
    Submit a job run to EMR Serverless and return the jobRunId.

    The script is fetched from SCRIPTS_BASE/<script_name>.py in S3.
    EMR_APP_ID and EMR_ROLE_ARN must be set as environment variables.
    """
    app_id = os.environ["EMR_APP_ID"]
    role_arn = os.environ["EMR_ROLE_ARN"]
    scripts_base = os.environ.get("SCRIPTS_BASE", "s3://etl-course-output/scripts")

    script_name = JOB_TYPE_TO_EMR_SCRIPT.get(job_type, "emr_simple_etl")
    entry_point = f"{scripts_base}/{script_name}.py"

    emr = boto3.client("emr-serverless", region_name="us-east-1")
    response = emr.start_job_run(
        applicationId=app_id,
        executionRoleArn=role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": [source_path, output_path],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g"
                ),
            }
        },
    )
    run_id = response["jobRunId"]
    logger.info(json.dumps({
        "event": "emr_job_started",
        "entry_point": entry_point,
        "job_run_id": run_id,
        "source_path": source_path,
    }))
    return run_id


# ── Handler ───────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Entry point called by AWS Lambda.

    Supports two invocation modes:
      1. S3 event  — event contains 'Records' with S3 bucket/key info
      2. Direct    — event contains 'source_path' and optional overrides
    """
    track = os.environ.get("TRACK", "glue")
    default_output = os.environ.get("OUTPUT_PATH", "")
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")

    # ── Parse source path and job type ───────────────────────────────────────
    if "Records" in event:
        # Triggered by S3 event notification
        record = event["Records"][0]["s3"]
        bucket = record["bucket"]["name"]
        key = record["object"]["key"]
        source_path = f"s3://{bucket}/{key}"
        job_type = _infer_job_type(key)
    else:
        # Direct invocation — caller provides explicit parameters
        source_path = event.get("source_path", "")
        job_type = event.get("job_type", "simple-etl")
        # Allow the caller to override the track for this invocation
        track = event.get("track", track)

    if not source_path:
        logger.error("No source_path could be determined from the event payload.")
        return {"statusCode": 400, "body": "Missing source_path"}

    output_path = default_output or f"s3://etl-course-output/{track}/{job_type}/"

    # ── Start the appropriate job ─────────────────────────────────────────────
    try:
        if track == "glue":
            job_run_id = _start_glue_job(job_type, source_path, output_path)
        else:
            job_run_id = _start_emr_job(job_type, source_path, output_path)
    except Exception as e:
        logger.error(json.dumps({"event": "job_start_failed", "error": str(e)}))
        return {"statusCode": 500, "body": f"Failed to start job: {e}"}

    # ── Log structured entry to CloudWatch ───────────────────────────────────
    log_entry = {
        "event": "job_started",
        "track": track,
        "job_type": job_type,
        "job_run_id": job_run_id,
        "source_path": source_path,
        "output_path": output_path,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(json.dumps(log_entry))

    # ── Optionally publish a start notification to SNS ────────────────────────
    if sns_topic_arn:
        sns = boto3.client("sns", region_name="us-east-1")
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject=f"ETL Job Started: {track}-{job_type}",
            Message=json.dumps(log_entry),
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Job started: {job_run_id}",
            "track": track,
            "job_type": job_type,
        }),
    }
