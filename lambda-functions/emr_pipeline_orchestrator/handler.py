"""
lambda-functions/emr_pipeline_orchestrator/handler.py
-------------------------------------------------------
Lambda function that orchestrates the 4-stage EMR Serverless pipeline.

Acts as a simple state machine: submits each stage to EMR Serverless,
polls until completion, then submits the next stage. Publishes an SNS
alert if any stage fails and aborts the pipeline immediately.

PIPELINE STAGES (in order):
  Stage 1 — emr_step1_raw       : schema validation + raw Parquet write
  Stage 2 — emr_step2_clean     : cast, filter nulls, trim whitespace
  Stage 3 — emr_step3_enrich    : add order_date, revenue, price_tier
  Stage 4 — emr_step4_aggregate : KPI aggregation by region/date/tier

HOW TO TRIGGER:
  aws lambda invoke \\
      --function-name etl-emr-pipeline-orchestrator \\
      --payload '{
          "source_path":  "s3://<landing>/sales/",
          "base_output":  "s3://<processed>/emr-pipeline/",
          "final_output": "s3://<output>/emr-pipeline/aggregate/"
      }' \\
      --cli-binary-format raw-in-base64-out \\
      response.json

HOW TO DEPLOY:
  python infrastructure/deploy_lambdas.py \\
      --deploy-orchestrator \\
      --bucket <your-output-bucket> \\
      --app-name etl-course-spark

ENVIRONMENT VARIABLES:
  EMR_APP_ID       — EMR Serverless application ID (required)
  EMR_ROLE_ARN     — IAM role ARN for EMR job execution (required)
  SCRIPTS_BASE     — S3 base URI where EMR scripts are stored
                     e.g. s3://<output-bucket>/scripts
  SNS_TOPIC_ARN    — SNS topic ARN for success/failure notifications

INVOCATION PAYLOAD:
  source_path   — S3 URI to the raw CSV input files
  base_output   — S3 URI prefix for intermediate stage outputs
  final_output  — S3 URI for the final aggregate output (Stage 4)

  Intermediate paths are derived automatically:
    Stage 1 output → <base_output>raw/
    Stage 2 output → <base_output>clean/
    Stage 3 output → <base_output>enriched/
    Stage 4 output → <final_output>

LAMBDA TIMEOUT NOTE:
  Lambda has a 15-minute maximum runtime. With small datasets (< 100k rows)
  all four stages typically complete in under 10 minutes. For larger datasets
  consider AWS Step Functions as the orchestrator instead.

MONITORING:
  CloudWatch → Log groups → /aws/lambda/etl-emr-pipeline-orchestrator
  Each stage submission and every status poll emits a structured JSON log line.
"""

import os
import json
import time
import logging
import boto3
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# How often to poll for a stage's completion (seconds)
POLL_INTERVAL_SECONDS = 30

# Maximum time to wait for a single stage before declaring a timeout (seconds)
MAX_WAIT_SECONDS = 600  # 10 minutes per stage

# EMR job states that indicate a terminal outcome
TERMINAL_FAILURE_STATES = {"FAILED", "CANCELLED", "CANCELLING"}
TERMINAL_SUCCESS_STATES = {"SUCCESS"}


def _submit_stage(emr_client, app_id: str, role_arn: str,
                  scripts_base: str, stage: dict) -> str:
    """
    Submit one pipeline stage to EMR Serverless.

    Parameters
    ----------
    stage : dict with keys:
        name   — script filename without .py extension
        input  — S3 URI for this stage's source data
        output — S3 URI for this stage's output data

    Returns the jobRunId string.
    """
    entry_point = f"{scripts_base}/{stage['name']}.py"
    response = emr_client.start_job_run(
        applicationId=app_id,
        executionRoleArn=role_arn,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": entry_point,
                "entryPointArguments": [stage["input"], stage["output"]],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g"
                ),
            }
        },
    )
    return response["jobRunId"]


def _poll_until_done(emr_client, app_id: str, run_id: str,
                     stage_num: int, stage_name: str) -> str:
    """
    Poll EMR Serverless every POLL_INTERVAL_SECONDS until the job reaches
    a terminal state or MAX_WAIT_SECONDS elapses.

    Returns the final status string ('SUCCESS', 'FAILED', etc.)
    Raises TimeoutError if the job does not finish within MAX_WAIT_SECONDS.
    """
    elapsed = 0
    while elapsed < MAX_WAIT_SECONDS:
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

        response = emr_client.get_job_run(applicationId=app_id, jobRunId=run_id)
        status = response["jobRun"]["state"]

        logger.info(json.dumps({
            "event":      "stage_poll",
            "stage":      stage_num,
            "stage_name": stage_name,
            "status":     status,
            "elapsed_s":  elapsed,
            "run_id":     run_id,
        }))

        if status in TERMINAL_SUCCESS_STATES or status in TERMINAL_FAILURE_STATES:
            return status

    raise TimeoutError(
        f"Stage {stage_num} ({stage_name}) timed out after {MAX_WAIT_SECONDS}s. "
        f"run_id={run_id}"
    )


def lambda_handler(event: dict, context) -> dict:
    """
    Entry point called by AWS Lambda.

    Orchestrates the four EMR pipeline stages sequentially, failing fast
    if any stage is unsuccessful.
    """
    # ── 1. Read configuration ─────────────────────────────────────────────────
    app_id = os.environ["EMR_APP_ID"]
    role_arn = os.environ["EMR_ROLE_ARN"]
    scripts_base = os.environ.get(
        "SCRIPTS_BASE", "s3://etl-course-output/scripts"
    )
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")

    source_path = event["source_path"]
    base_output = event["base_output"].rstrip("/") + "/"
    final_output = event["final_output"]

    pipeline_start = datetime.now(timezone.utc)

    # ── 2. Define the four stages ─────────────────────────────────────────────
    # Each stage reads from the previous stage's output. The base_output
    # prefix is shared for intermediate results; final_output is the
    # caller-specified destination for the aggregate (Stage 4) results.
    stages = [
        {
            "name":   "emr_step1_raw",
            "input":  source_path,
            "output": f"{base_output}raw/",
        },
        {
            "name":   "emr_step2_clean",
            "input":  f"{base_output}raw/",
            "output": f"{base_output}clean/",
        },
        {
            "name":   "emr_step3_enrich",
            "input":  f"{base_output}clean/",
            "output": f"{base_output}enriched/",
        },
        {
            "name":   "emr_step4_aggregate",
            "input":  f"{base_output}enriched/",
            "output": final_output,
        },
    ]

    emr = boto3.client("emr-serverless", region_name="us-east-1")
    sns = boto3.client("sns", region_name="us-east-1") if sns_topic_arn else None

    logger.info(json.dumps({
        "event":        "pipeline_start",
        "source_path":  source_path,
        "base_output":  base_output,
        "final_output": final_output,
        "total_stages": len(stages),
    }))

    # ── 3. Run stages sequentially ────────────────────────────────────────────
    for i, stage in enumerate(stages, start=1):
        stage_name = stage["name"]

        logger.info(json.dumps({
            "event":      "stage_start",
            "stage":      i,
            "stage_name": stage_name,
            "input":      stage["input"],
            "output":     stage["output"],
        }))

        # Submit the EMR job for this stage
        try:
            run_id = _submit_stage(emr, app_id, role_arn, scripts_base, stage)
        except Exception as e:
            err = f"Stage {i} ({stage_name}) submission failed: {e}"
            logger.error(err)
            if sns:
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Subject=f"ETL Pipeline FAILED at stage {i} (submission)",
                    Message=json.dumps({
                        "pipeline":     "emr-multi-step",
                        "failed_stage": stage_name,
                        "error":        str(e),
                        "timestamp":    datetime.now(timezone.utc).isoformat(),
                    }),
                )
            return {"statusCode": 500, "error": err}

        logger.info(json.dumps({
            "event":      "stage_submitted",
            "stage":      i,
            "stage_name": stage_name,
            "run_id":     run_id,
        }))

        # Poll until done
        try:
            final_status = _poll_until_done(emr, app_id, run_id, i, stage_name)
        except TimeoutError as e:
            err = str(e)
            logger.error(err)
            if sns:
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Subject=f"ETL Pipeline TIMEOUT at stage {i}",
                    Message=json.dumps({
                        "pipeline":     "emr-multi-step",
                        "failed_stage": stage_name,
                        "error":        err,
                        "timestamp":    datetime.now(timezone.utc).isoformat(),
                    }),
                )
            return {"statusCode": 504, "error": err}

        # Abort on failure
        if final_status in TERMINAL_FAILURE_STATES:
            err = f"Stage {i} ({stage_name}) ended with status: {final_status}"
            logger.error(err)
            if sns:
                sns.publish(
                    TopicArn=sns_topic_arn,
                    Subject=f"ETL Pipeline FAILED at stage {i}",
                    Message=json.dumps({
                        "pipeline":     "emr-multi-step",
                        "failed_stage": stage_name,
                        "status":       final_status,
                        "run_id":       run_id,
                        "timestamp":    datetime.now(timezone.utc).isoformat(),
                    }),
                )
            return {"statusCode": 500, "error": err}

        logger.info(json.dumps({
            "event":      "stage_complete",
            "stage":      i,
            "stage_name": stage_name,
            "status":     final_status,
        }))

    # ── 4. All stages succeeded ───────────────────────────────────────────────
    duration = int((datetime.now(timezone.utc) - pipeline_start).total_seconds())

    logger.info(json.dumps({
        "event":            "pipeline_complete",
        "status":           "SUCCESS",
        "total_stages":     len(stages),
        "duration_seconds": duration,
        "final_output":     final_output,
    }))

    if sns:
        sns.publish(
            TopicArn=sns_topic_arn,
            Subject="ETL Pipeline SUCCEEDED",
            Message=json.dumps({
                "pipeline":         "emr-multi-step",
                "status":           "SUCCESS",
                "duration_seconds": duration,
                "final_output":     final_output,
                "timestamp":        datetime.now(timezone.utc).isoformat(),
            }),
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":          "All 4 stages completed successfully.",
            "duration_seconds": duration,
            "final_output":     final_output,
        }),
    }
