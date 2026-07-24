# Task 7A — Multi-Step Pipeline with AWS Glue Workflow

## What Are We Building?

A four-stage Glue Workflow that chains four jobs together:

```
Raw → Clean → Enrich → Aggregate
```

Each stage reads from the previous stage's S3 output. If any stage fails, the workflow stops and sends an SNS alert. You never re-run successful stages unnecessarily.

---

## Concepts Explained

### What is a Multi-Step Pipeline?
Real ETL systems rarely do everything in one job. Breaking work into stages gives you:
- **Testability** — test each stage independently
- **Debuggability** — when something fails you know exactly which stage and why
- **Reusability** — the Clean stage can be reused by other pipelines
- **Partial reruns** — if Stage 3 fails, re-run from Stage 3, not Stage 1

### What is a Glue Workflow?
A Glue Workflow is a visual orchestrator built into the Glue service. You connect jobs together and define dependency rules: "Run Job B only if Job A succeeded." AWS manages the execution order, monitors each job, and stops the chain on failure.

You can see the entire pipeline in a visual graph in the Glue Console — no code needed to understand the structure.

### What Each Stage Does

| Stage | Input | What it does | Output |
|-------|-------|-------------|--------|
| Step 1: Raw | CSV from landing | Load, schema check, write as-is | Raw Parquet |
| Step 2: Clean | Raw Parquet | Remove nulls, fix types, standardise formats | Clean Parquet |
| Step 3: Enrich | Clean Parquet | Add calculated columns, join reference data | Enriched Parquet |
| Step 4: Aggregate | Enriched Parquet | Group by dimensions, compute KPIs | Summary Parquet |

---

## Prerequisites

- [ ] Task 3A complete (understand basic Glue job structure)
- [ ] Task 2 complete (sales data in landing bucket)
- [ ] Task 8 partially done (SNS topic exists for failure alerts)

---

## Step 1 — Upload All Four Scripts to S3

```bash
for script in glue-jobs/multi-step/*.py; do
  aws s3 cp $script s3://etl-course-yourname-output/scripts/
done
```

---

## Step 2 — Create the Glue Workflow

### Console path:
1. Go to **AWS Glue** → **Workflows** → **Create workflow**
2. **Name**: `etl-multi-step-workflow`
3. Click **Add trigger** to create the start trigger:
   - Trigger type: **On demand** (you'll start it manually)
   - Add job: `glue-step1-raw`
4. Click the `glue-step1-raw` node → **Add trigger**:
   - Trigger type: **Conditional** → **Job succeeded**
   - Watched job: `glue-step1-raw`
   - Add job: `glue-step2-clean`
5. Repeat for each downstream stage:
   - `glue-step2-clean` succeeded → trigger `glue-step3-enrich`
   - `glue-step3-enrich` succeeded → trigger `glue-step4-aggregate`

### Or via script:
```bash
python infrastructure/register_glue_jobs.py --create-workflow
```

---

## Step 3 — Run the Workflow

```bash
aws glue start-workflow-run --name etl-multi-step-workflow
```

Or in the Console:
1. Glue → **Workflows** → `etl-multi-step-workflow`
2. Click **Run** (top right)

---

## Step 4 — Monitor the Workflow

In the Glue Console:
1. Workflows → click `etl-multi-step-workflow`
2. Click the **Run history** tab → click the active run
3. You'll see a live visual graph:
   - Blue node = running
   - Green node = succeeded
   - Red node = failed (workflow stops here)
4. Click any node to see that job's CloudWatch logs

---

## Step 5 — Verify the Staged Outputs

```bash
# Raw output
aws s3 ls s3://etl-course-yourname-processed/pipeline/raw/ --recursive

# Clean output
aws s3 ls s3://etl-course-yourname-processed/pipeline/clean/ --recursive

# Enriched output
aws s3 ls s3://etl-course-yourname-processed/pipeline/enriched/ --recursive

# Final aggregate
aws s3 ls s3://etl-course-yourname-output/pipeline/aggregate/ --recursive
```

All four folders should contain Parquet files.

---

## What the Four Scripts Do

### glue_step1_raw.py — Raw Stage
- Read CSV from landing bucket
- Validate schema: assert required columns exist (`order_id`, `product_name`, `quantity`, `unit_price`, `region`)
- If schema check fails → raise exception (workflow stops, SNS alert fires)
- Write raw Parquet with no transformations applied

### glue_step2_clean.py — Clean Stage
- Read raw Parquet
- Remove rows where `order_id` is null
- Cast `unit_price` → double, `quantity` → int
- Remove rows where `quantity <= 0` or `unit_price <= 0`
- Trim whitespace from `region` and `product_name`
- Write clean Parquet

### glue_step3_enrich.py — Enrich Stage
- Read clean Parquet
- Add `order_date` = `to_date(order_timestamp)`
- Add `revenue` = `quantity * unit_price`
- Add `price_tier`: `when(unit_price < 20, "budget").when(unit_price < 100, "mid").otherwise("premium")`
- Write enriched Parquet

### glue_step4_aggregate.py — Aggregate Stage
- Read enriched Parquet
- Compute KPIs grouped by `region` and `order_date`:
  - `total_orders` = count
  - `total_revenue` = sum of revenue
  - `avg_order_value` = avg of revenue
  - `top_category` = most frequent product category
- Write summary Parquet (small file — one row per region/date combination)

---

## Testing Failure Handling

Inject a bad file to see the workflow stop gracefully:

```bash
# Create a CSV with missing required columns
echo "wrong_col,another_col" > /tmp/bad_data.csv
echo "x,y" >> /tmp/bad_data.csv

aws s3 cp /tmp/bad_data.csv \
  s3://etl-course-yourname-landing/sales/bad_data.csv
```

Start the workflow with this bad data as input. Step 1 (Raw) will fail on schema validation → workflow stops → SNS failure alert sent to your email.

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| Workflow stuck in RUNNING | One job waiting for a trigger | Check trigger conditions in workflow graph |
| Step 2 fails with `path does not exist` | Step 1 output path mismatch | Ensure each job's output path matches the next job's input path |
| Workflow starts Step 2 even when Step 1 failed | Trigger condition wrong | Trigger should be "Job succeeded", not "Job completed" |
| `RESOURCE_NOT_FOUND` on start | Workflow not registered | Run `register_glue_jobs.py --create-workflow` |
| SNS alert not received | SNS topic or job failure hook missing | Verify each script's `finally` block publishes to SNS |

---

## Cost Impact

- Four jobs running ~5 minutes each with 2 G.1X workers ≈ $0.28 per full pipeline run
- **Estimated cost for this task: $0.30–$0.50** (including a few test runs)

---

## Next Step

→ `docs/07b-emr-pipeline-guide.md` — Implement the same 4-stage pipeline on EMR Serverless, orchestrated by a Lambda function instead of a Glue Workflow.
