# Task 10 — Comparison Guide, Cost Review, and Cleanup

## What This Guide Covers

1. Full Glue vs EMR Serverless comparison with real numbers from this course
2. How to view your actual AWS costs
3. CloudWatch dashboard walkthrough
4. **IMPORTANT**: How to delete every resource created in this course

---

## Part 1 — Glue vs EMR Serverless: Full Comparison

### Feature Comparison

| Feature | AWS Glue | EMR Serverless |
|---------|----------|---------------|
| **API** | DynamicFrame + Spark DataFrame | Native Spark DataFrame only |
| **Incremental processing** | Job bookmarks (built-in, zero code) | Custom S3 checkpoint (~20 lines) |
| **Workflow orchestration** | Glue Workflows (visual, click-based) | Lambda state machine (code-based) |
| **Data Catalog integration** | Automatic | Manual |
| **Job editor** | Visual drag-and-drop + script | Script only |
| **Script startup overhead** | `GlueContext`, `Job.init()`, `job.commit()` | `SparkSession.builder` only |
| **Special imports** | `from awsglue.transforms import *` | None |
| **Debugging** | Glue Console + CloudWatch | EMR Console + CloudWatch |
| **Cold start time** | ~1–2 minutes | ~1–2 minutes |

### Code Complexity Comparison

| Task | Glue lines | EMR lines | Winner |
|------|-----------|----------|--------|
| Simple ETL | ~55 | ~40 | EMR (simpler) |
| CDC | ~80 | ~100 | Glue (bookmarks = free) |
| Multi-step pipeline | ~60/job + Console config | ~60/job + Lambda orchestrator | Tie |
| Infrastructure setup | boto3 `create_job` | boto3 `start_job_run` | Tie |

### Cost Comparison (actual from this course)

| Pattern | Glue cost | EMR cost |
|---------|----------|---------|
| Simple ETL (10 min, 2 workers) | ~$0.07 | ~$0.04 |
| CDC (10 min, 2 workers) | ~$0.07 | ~$0.04 |
| Multi-step (4×10 min, 2 workers) | ~$0.28 | ~$0.16 |
| Total course estimate | ~$2–3 | ~$1–2 |

> Note: EMR is slightly cheaper for identical workloads. Glue costs more but saves development time on incremental processing and orchestration.

### When to Choose Each Service

**Choose AWS Glue when:**
- You need incremental processing and don't want to write checkpoint logic
- Your team uses the Glue Data Catalog for schema management
- You want a visual workflow editor for non-developers to understand the pipeline
- You're building a managed ETL pipeline quickly with minimal code

**Choose EMR Serverless when:**
- You want pure Spark without AWS-specific libraries (more portable code)
- You need maximum flexibility in how you track state
- Your team is experienced with Spark and prefers code over visual tools
- You're building complex pipelines that don't fit Glue's patterns
- Cost optimisation is critical (EMR is ~30–40% cheaper at scale)

### Career Context

| | AWS Glue | EMR Serverless |
|--|----------|---------------|
| Job listings mentioning it | Very common (especially at AWS shops) | Common (especially at companies using Hadoop/Spark) |
| Enterprise adoption | High — easy for non-Spark teams | High — preferred by data engineering specialists |
| Startup adoption | Medium | Medium-High |
| Learning curve | Lower | Higher |

---

## Part 2 — View Your Actual AWS Costs

1. Go to **AWS Console** → **Billing** → **Cost Explorer**
2. Click **Explore costs**
3. Set date range to this month
4. Group by **Service** to see Glue vs EMR breakdown
5. For detailed Glue costs: filter by **Service = AWS Glue**, group by **Usage type**

You can also check from the CLI:
```bash
aws ce get-cost-and-usage \
  --time-period Start=$(date -d "1 month ago" +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics "BlendedCost" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --query "ResultsByTime[0].Groups[?Keys[0]!='Tax'].{Service:Keys[0],Cost:Metrics.BlendedCost.Amount}" \
  --output table
```

---

## Part 3 — CloudWatch Dashboard

A CloudWatch dashboard was created in Task 1 with these widgets:

- **Glue job duration** — time per job run (helps identify slow jobs)
- **EMR job duration** — time per EMR job run
- **Lambda invocations** — how many times triggers fired
- **Estimated charge** — billing metric showing accumulated spend

View it:
1. **CloudWatch** → **Dashboards** → `etl-course-dashboard`

Or from the CLI:
```bash
aws cloudwatch list-dashboards --query "DashboardEntries[*].DashboardName"
```

---

## Part 4 — IMPORTANT: Delete All Resources

> **Do not skip this step.** Leaving resources running will continue to accrue charges even when you're not actively using them.

### Option A — Run the Cleanup Script (Recommended)

First, do a dry run to see exactly what will be deleted:
```bash
python infrastructure/cleanup.py \
  --prefix etl-course-yourname \
  --dry-run
```

Review the output — it lists every resource that will be deleted. When you're ready:
```bash
python infrastructure/cleanup.py \
  --prefix etl-course-yourname \
  --confirm
```

**What the script deletes:**
- S3 buckets and all objects inside them (landing, processed, output)
- IAM roles: all four `BatchETL-*` roles
- SNS topic: `etl-job-notifications` and all subscriptions
- SQS queues: `etl-job-status-queue` and `etl-job-status-dlq`
- Lambda functions: all four `etl-*` functions
- Glue jobs: `glue-simple-etl`, `glue-cdc`, all multi-step jobs
- Glue workflow: `etl-multi-step-workflow`
- EMR Serverless application: `etl-course-spark`
- CloudWatch rules: `etl-*-schedule` rules
- CloudWatch alarms: `etl-billing-*`
- CloudWatch log groups: all `/aws/lambda/etl-*` groups
- CloudWatch dashboard: `etl-course-dashboard`

### Option B — Manual Deletion (Console)

If the script fails for any reason, delete resources manually in this order (order matters due to dependencies):

1. **Lambda** → delete all four `etl-*` functions
2. **Glue** → Jobs → delete all `glue-*` jobs
3. **Glue** → Workflows → delete `etl-multi-step-workflow`
4. **EMR** → Serverless → stop and delete `etl-course-spark` application
5. **CloudWatch** → Rules → disable and delete `etl-*-schedule` rules
6. **CloudWatch** → Alarms → delete `etl-billing-warning-5usd` and `etl-billing-critical-9usd`
7. **SNS** → Topics → delete `etl-job-notifications`
8. **SQS** → Queues → delete `etl-job-status-queue` and `etl-job-status-dlq`
9. **S3** → empty each bucket first → then delete the bucket
   - Must empty before deleting: select all objects → Delete
   - Then delete the bucket itself
10. **IAM** → Roles → delete all four `BatchETL-*` roles

### Verify Everything is Gone

```bash
# Check for remaining Glue jobs
aws glue list-jobs --query "JobNames[?contains(@,'glue-')]"

# Check for remaining EMR apps
aws emr-serverless list-applications --query "applications[*].{name:name,state:state}"

# Check for remaining S3 buckets
aws s3 ls | grep etl-course

# Check for remaining Lambda functions
aws lambda list-functions --query "Functions[?contains(FunctionName,'etl-')].FunctionName"
```

All four commands should return empty results.

---

## Final Checklist

- [ ] All ETL patterns implemented and tested (3A, 3B, 6A, 6B, 7A, 7B)
- [ ] All trigger types tested (event-driven, scheduled, on-demand)
- [ ] Notifications working (email + SQS + CloudWatch Logs)
- [ ] Real data (NYC Taxi) processed end-to-end
- [ ] Cost stayed under $10 for the full course
- [ ] **All AWS resources deleted** — `cleanup.py --confirm` run successfully
- [ ] AWS Console shows no remaining course resources

---

## What You've Learned

By completing this project you can now:

1. Write PySpark ETL jobs for both AWS Glue and EMR Serverless
2. Implement three trigger patterns: event-driven, scheduled, on-demand
3. Build incremental CDC pipelines with SCD Type 2 history
4. Chain multi-stage pipelines with dependency management
5. Wire up SNS/SQS notifications for job status visibility
6. Explain when to choose Glue vs EMR for a given use case
7. Estimate AWS costs for batch ETL workloads
8. Manage AWS infrastructure with Python boto3 scripts

These skills map directly to data engineer job requirements at companies using AWS.

---

## Student Exercises

See `docs/student_exercises.md` for five hands-on challenges that extend what you've built.

See `docs/glue_vs_emr_comparison.md` for the extended reference comparison.

See `docs/cost_optimization_guide.md` for strategies to stay under $10/month in future projects.
