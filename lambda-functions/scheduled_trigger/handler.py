"""
lambda-functions/scheduled_trigger/handler.py
-----------------------------------------------
Lambda function invoked by CloudWatch Events (EventBridge) on a cron schedule.

Starts an ETL job automatically at a configured time — equivalent to a
cloud-based cron job. One Lambda function handles both Glue and EMR tracks;
the CloudWatch rule's input payload controls which track fires.

SCHEDULE SETUP (via cloudwatch_setup.py):
  python infrastructure/cloudwatch_setup.py --add-schedules

  This creates two rules:
    etl-glue-daily-schedule  cron(0 0 * * ? *)  → {"track": "glue"}
    etl-emr-daily-schedule   cron(0 1 * * ? *)  → {"track": "emr"}

  Always disable rules after testing to avoid unexpected costs:
    aws events disable-rule --name etl-glue-daily-schedule
    aws events disable-rule --name etl-emr-daily-schedule

ENVIRONMENT VARIABLES:
  TRACK           — "glue" (default) or "emr"
  JOB_TYPE        — job type to run (default: "simple-etl")
  SOURCE_PATH     — S3 URI for the source data
  OUTPUT_PATH     — S3 URI for the ETL output
  GLUE_JOB_PREFIX — prefix for Glue job names (default: "glue")
  EMR_APP_ID      — EMR Serverless application ID (required when TRACK=emr)
  EMR_ROLE_ARN    — IAM role ARN for EMR job runs (required when TRACK=emr)
  SCRIPTS_BASE    — S3 base URI where EMR scripts are stored
  SNS_TOPIC_ARN   — SNS topic ARN for notifications (optional)

CLOUDWATCH EVENT PAYLOAD (overrides env vars per-rule):
  {"track": "glue"}
  {"track": "emr", "job_type": "cdc"}

TESTING (temporarily accelerate the schedule to every 5 minutes):
  1. CloudWatch → Rules → etl-glue-daily-schedule → Edit
  2. Change cron expression to: rate(5 minutes)
  3. Wait 5 minutes, check CloudWatch Logs
  4. Immediately change back and disable the rule
"""

import os
import json
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _start_glue_job(job_name: str, source_path: str, output_path: str) -> str:
    """Start a named Glue job run and return the JobRunId."""
    glue = boto3.client("glue", region_name="us-east-1")
    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--source_path": source_path,
            "--output_path": output_path,
        },
    )
    return response["JobRunId"]


def _start_emr_job(job_type: str, source_path: str, output_path: str) -> str:
    """Submit a job run to EMR Serverless and return the jobRunId."""
    app_id = os.environ["EMR_APP_ID"]
    role_arn = os.environ["EMR_ROLE_ARN"]
    scripts_base = os.environ.get("SCRIPTS_BASE", "s3://etl-course-output/scripts")

    script_map = {
        "simple-etl": "emr_simple_etl",
        "cdc":        "emr_cdc_job",
    }
    script_name = script_map.get(job_type, "emr_simple_etl")

    emr = boto3.client("emr-serverless", region_name="us-east-1")
    response = emr.start_job_run(
        applicationId=app_id,
        executionRoleArn=role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"{scripts_base}/{script_name}.py",
                "entryPointArguments": [source_path, output_path],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g"
                ),
            }
        },
    )
    return response["jobRunId"]


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point called by CloudWatch Events on a schedule.

    Configuration is read first from environment variables (set at deploy time),
    then overridden by any values present in the CloudWatch event payload.
    This allows a single Lambda to serve multiple schedule rules — each rule
    passes its own {"track": "glue"} or {"track": "emr"} input.
    """
    # ── 1. Read defaults from environment variables ───────────────────────────
    track = os.environ.get("TRACK", "glue")
    job_type = os.environ.get("JOB_TYPE", "simple-etl")
    source_path = os.environ.get("SOURCE_PATH", "")
    output_path = os.environ.get("OUTPUT_PATH", "")
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")

    # ── 2. Allow the CloudWatch rule payload to override env vars ─────────────
    # This is the key design: one Lambda, multiple rules. Each CloudWatch rule
    # passes a static JSON input that selectively overrides specific settings.
    track = event.get("track", track)
    job_type = event.get("job_type", job_type)
    source_path = event.get("source_path", source_path)
    output_path = event.get("output_path", output_path)

    fired_at = datetime.now(timezone.utc).isoformat()

    logger.info(json.dumps({
        "event": "scheduled_trigger_fired",
        "track": track,
        "job_type": job_type,
        "source_path": source_path,
        "scheduled_at": fired_at,
    }))

    if not source_path:
        msg = "SOURCE_PATH not configured — set it as an env var or in the rule payload."
        logger.error(msg)
        return {"statusCode": 400, "body": msg}

    # ── 3. Start the appropriate job ──────────────────────────────────────────
    try:
        if track == "glue":
            prefix = os.environ.get("GLUE_JOB_PREFIX", "glue")
            glue_job_name = f"{prefix}-{job_type}"
            job_run_id = _start_glue_job(glue_job_name, source_path, output_path)
        else:
            job_run_id = _start_emr_job(job_type, source_path, output_path)
    except Exception as e:
        logger.error(json.dumps({"event": "job_start_failed", "error": str(e)}))
        return {"statusCode": 500, "body": f"Failed to start job: {e}"}

    log_entry = {
        "event": "job_started_by_schedule",
        "track": track,
        "job_type": job_type,
        "job_run_id": job_run_id,
        "source_path": source_path,
        "output_path": output_path,
        "scheduled_at": fired_at,
    }
    logger.info(json.dumps(log_entry))

    # ── 4. Publish start notification to SNS (optional) ───────────────────────
    if sns_topic_arn:
        boto3.client("sns", region_name="us-east-1").publish(
            TopicArn=sns_topic_arn,
            Subject=f"Scheduled ETL Job Started: {track}-{job_type}",
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
