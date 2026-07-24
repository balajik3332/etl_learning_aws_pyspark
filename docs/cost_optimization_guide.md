# Cost Optimization Guide — Stay Under $10/Month

## Target Budget

This course is designed to cost **$3.50–$5.00/month** with normal learning usage, with a hard ceiling of **$10/month**.

---

## 1. Billing Alarms (Set These First — Task 1)

Two CloudWatch billing alarms protect you:

| Alarm | Threshold | Action |
|-------|----------|--------|
| `etl-billing-warning-5usd` | $5 spent | Email via SNS |
| `etl-billing-critical-9usd` | $9 spent | Email via SNS |

If you receive the $5 alarm, pause and review your Cost Explorer before running more jobs.  
If you receive the $9 alarm, stop all running jobs immediately.

Verify alarms exist:
```bash
aws cloudwatch describe-alarms \
  --alarm-names etl-billing-warning-5usd etl-billing-critical-9usd \
  --query "MetricAlarms[*].{Name:AlarmName,State:StateValue,Threshold:Threshold}"
```

---

## 2. The Biggest Cost Risks

### Risk 1 — Leaving EMR Serverless Application Running
An EMR Serverless application in `STARTED` state with pre-initialized capacity costs money even when idle.

**Rule**: Never enable pre-initialized capacity. Leave it at 0. The application will auto-start when you submit a job (takes ~1–2 min for the first run).

Check application state:
```bash
aws emr-serverless list-applications \
  --query "applications[*].{name:name,state:state}"
```

If any application shows `STARTED`, stop it:
```bash
aws emr-serverless stop-application --application-id <APP_ID>
```

### Risk 2 — CloudWatch Schedule Rules Left ENABLED
A daily Glue job costs ~$1.50/month. Leave two schedules enabled for a month = $3.

**Rule**: Always disable schedule rules after testing. Re-enable them only when actively learning.

```bash
# Disable all course schedule rules
aws events disable-rule --name etl-glue-daily-schedule
aws events disable-rule --name etl-emr-daily-schedule
```

Check rule states:
```bash
aws events list-rules --name-prefix etl \
  --query "Rules[*].{Name:Name,State:State}"
```

All should show `State: DISABLED` when you're not actively using them.

### Risk 3 — Large S3 Data Accumulating
Each ETL run writes Parquet files to S3. Over many runs this accumulates.

**Rule**: The landing bucket has a 7-day lifecycle rule (auto-deletes). Manually clean processed and output buckets after major tasks.

```bash
# Delete all processed data (keeps bucket, removes objects)
aws s3 rm s3://etl-course-yourname-processed/ --recursive

# Delete all output data
aws s3 rm s3://etl-course-yourname-output/ --recursive --exclude "scripts/*"
```

### Risk 4 — Running Too Many Test Runs
Each Glue job run costs ~$0.07. 50 test runs = $3.50.

**Rule**: Test transformation logic locally with a small Pandas/PySpark DataFrame before submitting to Glue or EMR. Only submit to AWS when you're fairly confident the code is correct.

---

## 3. Cost Per Service — Monthly Estimate

| Service | Free Tier | Normal Learning Usage | Estimated Cost |
|---------|----------|-----------------------|---------------|
| S3 | 5 GB, 20k GET, 2k PUT | < 1 GB, < 5k requests | $0.00 |
| Lambda | 1M requests, 400k GB-s | < 10k requests | $0.00 |
| SNS | 1M publishes | < 1k publishes | $0.00 |
| SQS | 1M requests | < 1k requests | $0.00 |
| CloudWatch | 10 metrics, 5 GB logs | < limits | $0.00 |
| IAM | Always free | — | $0.00 |
| **AWS Glue** | No free tier | ~8 jobs × 10 min × 2 DPU | **~$1.50** |
| **EMR Serverless** | No free tier | ~8 jobs × 10 min × 4 vCPU | **~$1.80** |
| **Total** | | | **~$3.30** |

---

## 4. Compute Sizing Rules

### AWS Glue
- Use **G.1X** worker type (4 vCPU, 16 GB RAM) — smallest available
- Use **2 workers** minimum
- Never use G.2X or G.4X for learning exercises
- Set `MaxConcurrentRuns=1` to prevent accidental parallel runs

### EMR Serverless
- Use default application settings (no pre-initialized capacity)
- Spark submit params: `--conf spark.executor.cores=2 --conf spark.executor.memory=4g`
- Never set `--conf spark.executor.instances` higher than needed for your data size

### Lambda
- 128 MB memory is sufficient for trigger functions
- 512 MB for the pipeline orchestrator (it runs longer)
- Timeout: 300s for triggers, 900s for orchestrator

---

## 5. Daily Check Routine

While actively learning, spend 2 minutes each day:

```bash
# 1. Check current month spend
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics UnblendedCost \
  --query "ResultsByTime[0].Total.UnblendedCost.Amount"

# 2. Confirm no schedule rules enabled
aws events list-rules --name-prefix etl \
  --query "Rules[?State=='ENABLED'].Name"

# 3. Confirm no EMR apps running
aws emr-serverless list-applications \
  --query "applications[?state=='STARTED'].name"
```

All three commands should return empty/zero values when you're not actively running jobs.

---

## 6. End-of-Session Checklist

After each learning session, before closing your laptop:

- [ ] No Glue jobs currently running: **AWS Glue → Jobs → no RUNNING state**
- [ ] No EMR jobs running: **EMR → Serverless → Job runs → no RUNNING state**
- [ ] Schedule rules disabled: `aws events list-rules --name-prefix etl`
- [ ] EMR application stopped (if you started it manually)

---

## 7. End-of-Course Cleanup

Run the full cleanup when done with the course:

```bash
python infrastructure/cleanup.py --prefix etl-course-yourname --confirm
```

This deletes everything. After running it, your AWS bill for course resources drops to $0.

See `docs/10-comparison-and-cleanup.md` for the complete cleanup procedure.
