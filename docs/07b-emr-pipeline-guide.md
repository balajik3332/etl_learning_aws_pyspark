# Task 7B — Multi-Step Pipeline with EMR Serverless + Lambda Orchestrator

## What Are We Building?

The same four-stage pipeline (Raw → Clean → Enrich → Aggregate) as Task 7A, but running on EMR Serverless and orchestrated by a Lambda function instead of a Glue Workflow.

This teaches you how pipeline orchestration works at a code level — no visual editor, just Python logic.

---

## Concepts Explained

### Why is EMR Pipeline Orchestration Different from Glue?
EMR Serverless has no built-in workflow engine. There is no "visual workflow editor" or "conditional trigger." Instead, we write a Lambda function that:
1. Submits Stage 1 to EMR
2. Polls for completion (checks job status every 30 seconds)
3. If Stage 1 succeeded → submits Stage 2
4. Repeats through all four stages
5. If any stage fails → stops and publishes an SNS failure alert

### Lambda as an Orchestrator
Lambda is the control plane; EMR jobs are the compute plane. Lambda is cheap (milliseconds of billing per poll) while EMR jobs do the heavy lifting. The Lambda effectively acts as a simple state machine.

### Glue Workflow vs Lambda Orchestrator — Side by Side

| | Glue Workflow | Lambda Orchestrator |
|--|--------------|-------------------|
| Configuration | Visual, click-based | Code in `handler.py` |
| Visibility | Graph in Glue Console | CloudWatch Logs |
| State management | AWS manages it | You manage it (in-memory) |
| Maximum pipeline duration | No limit | 15 min Lambda timeout |
| Flexibility | Fixed trigger types | Any logic you can write |
| Cost | Free (Glue Workflows have no extra charge) | Pennies (Lambda is nearly free) |

> **Lambda timeout note**: Lambda has a 15-minute maximum runtime. If your pipeline runs for longer, you need Step Functions or a different orchestration approach. For this course's small datasets the pipeline runs in well under 15 minutes.

---

## Prerequisites

- [ ] Task 7A complete — understand the stage logic before running it on EMR
- [ ] EMR Serverless application `etl-course-spark` exists

---

## Step 1 — Upload All Four EMR Scripts

```bash
for script in emr-jobs/multi-step/*.py; do
  aws s3 cp $script s3://etl-course-yourname-output/scripts/
done
```

---

## Step 2 — Deploy the Lambda Orchestrator

```bash
python infrastructure/deploy_lambdas.py --deploy-orchestrator \
  --bucket etl-course-yourname-output \
  --app-name etl-course-spark
```

---

## Step 3 — Trigger the Pipeline

```bash
aws lambda invoke \
  --function-name etl-emr-pipeline-orchestrator \
  --payload '{
    "source_path": "s3://etl-course-yourname-landing/sales/",
    "base_output": "s3://etl-course-yourname-processed/emr-pipeline/",
    "final_output": "s3://etl-course-yourname-output/emr-pipeline/aggregate/"
  }' \
  --cli-binary-format raw-in-base64-out \
  response.json

cat response.json
```

---

## Step 4 — Monitor the Pipeline

The Lambda logs every job submission and status poll to CloudWatch:

1. Go to **CloudWatch** → **Log groups** → `/aws/lambda/etl-emr-pipeline-orchestrator`
2. Click the most recent log stream

You'll see output like:
```
[INFO] Starting stage 1/4: emr_step1_raw
[INFO] Stage 1 job submitted. Run ID: jr_abc123
[INFO] Stage 1 status: PENDING (elapsed: 30s)
[INFO] Stage 1 status: RUNNING (elapsed: 60s)
[INFO] Stage 1 status: RUNNING (elapsed: 90s)
[INFO] Stage 1 SUCCEEDED in 95s. Starting stage 2/4: emr_step2_clean
[INFO] Stage 2 job submitted. Run ID: jr_def456
...
[INFO] All 4 stages completed successfully.
```

---

## The Orchestrator Lambda — Logic Walkthrough

File: `lambda-functions/emr_pipeline_orchestrator/handler.py`

```python
import os, json, time, boto3, logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

POLL_INTERVAL_SECONDS = 30
MAX_WAIT_SECONDS = 600  # 10 minutes per stage

def lambda_handler(event, context):
    emr = boto3.client("emr-serverless", region_name="us-east-1")
    sns = boto3.client("sns", region_name="us-east-1")

    app_id   = os.environ["EMR_APP_ID"]
    role_arn = os.environ["EMR_ROLE_ARN"]
    sns_arn  = os.environ["SNS_TOPIC_ARN"]

    source     = event["source_path"]
    base_out   = event["base_output"]
    final_out  = event["final_output"]
    scripts_base = os.environ.get("SCRIPTS_BASE", "s3://etl-course-output/scripts")

    # Define the four stages in order
    stages = [
        {"name": "emr_step1_raw",       "input": source,                "output": f"{base_out}raw/"},
        {"name": "emr_step2_clean",      "input": f"{base_out}raw/",     "output": f"{base_out}clean/"},
        {"name": "emr_step3_enrich",     "input": f"{base_out}clean/",   "output": f"{base_out}enriched/"},
        {"name": "emr_step4_aggregate",  "input": f"{base_out}enriched/","output": final_out},
    ]

    for i, stage in enumerate(stages, 1):
        logger.info(json.dumps({"event": "stage_start", "stage": i, "name": stage["name"]}))

        # Submit the EMR job
        response = emr.start_job_run(
            applicationId=app_id,
            executionRoleArn=role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"{scripts_base}/{stage['name']}.py",
                    "entryPointArguments": [stage["input"], stage["output"]],
                }
            },
        )
        run_id = response["jobRunId"]

        # Poll until the job finishes (or timeout)
        elapsed = 0
        while elapsed < MAX_WAIT_SECONDS:
            time.sleep(POLL_INTERVAL_SECONDS)
            elapsed += POLL_INTERVAL_SECONDS

            status_resp = emr.get_job_run(applicationId=app_id, jobRunId=run_id)
            status = status_resp["jobRun"]["state"]
            logger.info(json.dumps({"event": "stage_poll", "stage": i, "status": status, "elapsed": elapsed}))

            if status == "SUCCESS":
                logger.info(json.dumps({"event": "stage_complete", "stage": i, "name": stage["name"]}))
                break
            elif status in ("FAILED", "CANCELLED", "CANCELLING"):
                # Stage failed — publish alert and abort
                sns.publish(
                    TopicArn=sns_arn,
                    Subject=f"ETL Pipeline FAILED at stage {i}",
                    Message=json.dumps({
                        "pipeline": "emr-multi-step",
                        "failed_stage": stage["name"],
                        "status": status,
                        "run_id": run_id,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }),
                )
                return {"statusCode": 500, "error": f"Stage {i} failed: {status}"}
        else:
            return {"statusCode": 504, "error": f"Stage {i} timed out after {MAX_WAIT_SECONDS}s"}

    # All stages succeeded
    sns.publish(
        TopicArn=sns_arn,
        Subject="ETL Pipeline SUCCEEDED",
        Message=json.dumps({"pipeline": "emr-multi-step", "status": "SUCCESS"}),
    )
    return {"statusCode": 200, "message": "All 4 stages completed successfully"}
```

---

## Verify All Four Outputs Exist

```bash
for stage in raw clean enriched; do
  echo "--- $stage ---"
  aws s3 ls s3://etl-course-yourname-processed/emr-pipeline/$stage/ --recursive | head -5
done

echo "--- aggregate ---"
aws s3 ls s3://etl-course-yourname-output/emr-pipeline/aggregate/ --recursive
```

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| Lambda times out after 15 min | Pipeline taking too long | Use smaller test data; increase Lambda timeout to max (15 min) |
| Stage 2 not starting | Stage 1 never reached SUCCESS | Check EMR job run logs in EMR Console |
| `AccessDeniedException` in EMR job | EMR role missing S3 write | Verify `BatchETL-EMRServerlessRole` |
| `KeyError: EMR_APP_ID` | Lambda env vars not set | Re-deploy with `deploy_lambdas.py --deploy-orchestrator` |
| `APPLICATION_NOT_FOUND` | App name/ID wrong | Check `aws emr-serverless list-applications` |

---

## Cost Impact

- Four EMR jobs × ~5 min × ~2 vCPU = ~$0.10 per full pipeline run
- Lambda orchestrator: effectively $0.00 (mostly sleeping between polls)
- **Estimated cost for this task: $0.10–$0.20** (including test runs)

---

## Next Step

→ `docs/08-notifications-guide.md` — Wire up SNS email alerts and SQS messages for all job completions and failures.
