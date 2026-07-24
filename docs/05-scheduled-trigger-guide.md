# Task 5 — Scheduled Trigger with CloudWatch Events

## What Are We Building?

A CloudWatch Event rule that runs your ETL job automatically on a schedule — like a cloud-based cron job. We'll set up two rules:
- One that triggers the Glue job at midnight UTC every day
- One that triggers the EMR job at 1 AM UTC

You'll also test the schedule, then immediately disable it to avoid unwanted costs.

---

## Concepts Explained

### What is Amazon CloudWatch Events (EventBridge)?
CloudWatch Events (now officially called Amazon EventBridge, but the API is the same) is a scheduler and event router. You create a **rule** that says "fire this target at this time" — similar to a Unix cron job but managed by AWS.

### What is a cron expression?
A cron expression is a compact string that describes a schedule. AWS cron format has six fields:

```
cron(minutes  hours  day-of-month  month  day-of-week  year)
```

Examples:

| Expression | Meaning |
|-----------|---------|
| `cron(0 0 * * ? *)` | Every day at midnight UTC |
| `cron(0 1 * * ? *)` | Every day at 1 AM UTC |
| `cron(0 8 ? * MON-FRI *)` | Weekdays at 8 AM UTC |
| `rate(5 minutes)` | Every 5 minutes (useful for testing) |
| `rate(1 day)` | Once per day |

The `?` in day-of-week means "don't care" — AWS requires either day-of-month or day-of-week to be `?`.

---

## Prerequisites

- [ ] Task 4 complete (Lambda functions deployed)
- [ ] `etl-scheduled-trigger` Lambda function exists

---

## Step 1 — Create the Schedule Rules

```bash
python infrastructure/cloudwatch_setup.py --add-schedules
```

This creates:
- Rule `etl-glue-daily-schedule` → `cron(0 0 * * ? *)` → invokes `etl-scheduled-trigger` with `{"track": "glue"}`
- Rule `etl-emr-daily-schedule` → `cron(0 1 * * ? *)` → invokes `etl-scheduled-trigger` with `{"track": "emr"}`
- Grants CloudWatch permission to invoke the Lambda (`lambda:AddPermission`)

**Expected output:**
```
[OK] Rule created: etl-glue-daily-schedule  (cron: 0 0 * * ? *)
[OK] Rule created: etl-emr-daily-schedule   (cron: 0 1 * * ? *)
[OK] Lambda permission added for events.amazonaws.com
```

---

## Step 2 — Test by Temporarily Accelerating the Schedule

Rather than waiting until midnight, temporarily change the schedule to every 5 minutes:

**Console path:**
1. Go to **CloudWatch** → **Rules** (left sidebar) → click `etl-glue-daily-schedule`
2. Click **Edit**
3. Change **Schedule expression** from `cron(0 0 * * ? *)` to `rate(5 minutes)`
4. Click **Update rule**
5. Wait 5 minutes

**Check CloudWatch Logs:**
- Go to `/aws/lambda/etl-scheduled-trigger`
- You should see a new log stream appear every ~5 minutes
- Each entry confirms the Lambda ran and started the ETL job

---

## Step 3 — Disable the Schedule (Do This Immediately After Testing)

> **Cost Warning**: A Glue job running daily costs approximately $1.50/month. Always disable schedules when you are not actively learning.

```bash
aws events disable-rule --name etl-glue-daily-schedule
aws events disable-rule --name etl-emr-daily-schedule
```

Or in the Console:
1. CloudWatch → Rules → click the rule name
2. Click **Disable** button

To re-enable when you want it running again:
```bash
aws events enable-rule --name etl-glue-daily-schedule
```

---

## Step 4 — Verify the Setup

Check rules exist and their current state:
```bash
aws events list-rules --name-prefix etl
```

Expected output:
```json
{
  "Rules": [
    {
      "Name": "etl-glue-daily-schedule",
      "ScheduleExpression": "cron(0 0 * * ? *)",
      "State": "DISABLED"
    },
    {
      "Name": "etl-emr-daily-schedule",
      "ScheduleExpression": "cron(0 1 * * ? *)",
      "State": "DISABLED"
    }
  ]
}
```

State should be `DISABLED` after you disable them.

---

## The Lambda Handler — Scheduled Trigger

File: `lambda-functions/scheduled_trigger/handler.py`

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
    Invoked by CloudWatch Events on a cron schedule.
    Environment variables control which job to run.
    """
    # 1. Read config from environment variables set at deploy time
    track    = os.environ.get("TRACK", "glue")       # "glue" or "emr"
    job_type = os.environ.get("JOB_TYPE", "simple-etl")
    source   = os.environ.get("SOURCE_PATH", "")
    output   = os.environ.get("OUTPUT_PATH", "")

    # 2. Allow the CloudWatch event payload to override env vars
    #    This lets one Lambda function serve multiple rules
    track    = event.get("track", track)
    job_type = event.get("job_type", job_type)

    logger.info(json.dumps({
        "event": "scheduled_trigger_fired",
        "track": track,
        "job_type": job_type,
        "scheduled_at": datetime.now(timezone.utc).isoformat(),
    }))

    # 3. Start the appropriate job (same logic as s3_event_trigger)
    if track == "glue":
        glue = boto3.client("glue", region_name="us-east-1")
        response = glue.start_job_run(
            JobName=f"glue-{job_type}",
            Arguments={"--source_path": source, "--output_path": output},
        )
        return {"statusCode": 200, "jobRunId": response["JobRunId"]}
    else:
        # EMR job submission omitted for brevity — see s3_event_trigger for full impl
        return {"statusCode": 200, "message": "EMR job submitted"}
```

**Key design point**: The Lambda reads `track` from both the environment variable (`TRACK`) and the event payload. This means you can use one Lambda function for both tracks — the CloudWatch rule passes `{"track": "glue"}` or `{"track": "emr"}` in the input.

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| Rule created but Lambda never fires | Lambda permission not added | Run `aws lambda add-permission` or re-run `cloudwatch_setup.py --add-schedules` |
| `AccessDenied` in Lambda logs | Lambda role missing Glue/EMR permission | Check `BatchETL-LambdaTriggerRole` |
| Rule shows ENABLED but jobs don't start | Lambda environment variables not set | Check Lambda → Configuration → Environment variables |
| Unexpected charges | Schedule was left ENABLED | Always disable after testing — `aws events disable-rule --name <name>` |
| `ScheduleExpression is not valid` | Wrong cron syntax | Test expressions at [crontab.guru](https://crontab.guru) and add `cron()` wrapper |

---

## Cost Impact

| Scenario | Estimated Monthly Cost |
|----------|----------------------|
| Rules exist but DISABLED | $0.00 |
| Rule ENABLED, Glue job runs daily | ~$1.50 |
| Rule ENABLED, EMR job runs daily | ~$2.00 |
| Rule runs every 5 minutes (testing only!) | ~$40+ — disable immediately |

**Rule**: Always leave schedules in `DISABLED` state when you finish a session.

---

## Next Step

→ `docs/06a-glue-cdc-guide.md` — Learn Change Data Capture with AWS Glue job bookmarks to process only new data each run.
