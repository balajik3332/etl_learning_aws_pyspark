# Task 3A — AWS Glue Simple ETL

## What Are We Building?

In this task you write and run your first AWS Glue ETL job. The job reads the sales CSV data you generated in Task 2 from S3, applies a series of transformations (removing bad rows, fixing data types, computing new columns), and writes clean Parquet output back to S3 — partitioned by region so queries are fast.

By the end of this task you will have:
- A working PySpark script running on a managed Spark cluster in the cloud
- A Glue job visible in the AWS Console
- Clean Parquet output sitting in your S3 output bucket
- A clear understanding of what DynamicFrames are and why they exist

---

## Concepts Explained

### What is AWS Glue?

AWS Glue is a fully managed ETL service. You write a PySpark script, upload it to S3, tell Glue about it, and AWS runs it for you on a managed Spark cluster — you do not have to provision, configure, or shut down any servers.

Key things Glue provides:
- **Managed compute** — AWS starts Spark workers when your job runs and shuts them down when it finishes. You only pay for the time the job runs (billed per second, rounded to the minute).
- **Glue Data Catalog** — a central metadata store where your ETL job can register the output table schema. Other services like Athena, Redshift Spectrum, and Lake Formation can then query your data without you manually describing the schema each time.
- **Job Bookmarks** — a built-in incremental processing mechanism. Glue remembers exactly which files it already processed. The next run only processes new files. We use this in Task 6A (CDC).
- **Visual job editor** — a drag-and-drop interface for building ETL pipelines without code. For learning we use the script editor so you can see exactly what happens.

### What is a DynamicFrame?

A DynamicFrame is Glue's version of a Spark DataFrame. The key difference is how they handle messy or inconsistent data:

| Feature | Spark DataFrame | Glue DynamicFrame |
|---------|-----------------|-------------------|
| Schema enforcement | Strict — wrong types cause errors | Flexible — mismatched types become `ChoiceType` |
| Null handling | Requires explicit handling | Tolerates missing columns automatically |
| API | Full PySpark DataFrame API | Subset of operations; use `.toDF()` to get full API |
| When to use | After cleaning data | Initial read of raw/messy data |

In this job we use DynamicFrame for the **read and write** steps (where Glue's managed I/O is convenient) and convert to a regular DataFrame for the **transformation** step (where we need the full PySpark API).

### What is the Glue Data Catalog?

The Glue Data Catalog is like a card catalogue for your data lake. It stores:
- **Database and table definitions** — column names, types, partition keys
- **Table locations** — which S3 path holds this table's data
- **Format metadata** — Parquet, CSV, JSON, etc.

When your ETL job writes output, it can register the result in the Catalog. Then you can run SQL queries on the data using Amazon Athena without writing any schema declaration — Athena looks it up in the Catalog automatically.

---

## Prerequisites

Before starting this task, confirm the following are complete:

- [ ] Task 0: `aws configure` done, `aws sts get-caller-identity` returns your account ID
- [ ] Task 1: IAM role `BatchETL-GlueServiceRole` created
- [ ] Task 1: S3 buckets created (`*-landing`, `*-processed`, `*-output`)
- [ ] Task 2: Sales CSV data uploaded to `s3://<your-landing-bucket>/sales/`
- [ ] Python dependencies installed: `pip install -r requirements.txt`

---

## Step 1: Upload the Script to S3

The Glue script runs on AWS infrastructure, not your laptop. Before you can run it you must upload it to an S3 location that Glue can read.

```bash
aws s3 cp glue-jobs/simple-etl/glue_simple_etl.py \
    s3://<your-output-bucket>/scripts/glue_simple_etl.py
```

Replace `<your-output-bucket>` with your actual bucket name (e.g., `etl-course-yourname-output`).

**Verify the upload:**
```bash
aws s3 ls s3://<your-output-bucket>/scripts/
```

Expected output:
```
2026-05-19 10:23:45       4821 glue_simple_etl.py
```

---

## Step 2: Register the Glue Job

You can register the job using the AWS Console (good for understanding the settings) or the Python script (good for repeatability).

### Option A: AWS Console

1. Open the AWS Console and navigate to **AWS Glue**.
2. In the left sidebar click **ETL Jobs**, then click **Create job**.
3. Choose **Script editor** and click **Create**.
4. In the editor, paste the contents of `glue-jobs/simple-etl/glue_simple_etl.py`. Then click **Script details** (top tab).
5. Fill in the following settings:

   | Setting | Value |
   |---------|-------|
   | Name | `glue-simple-etl` |
   | IAM Role | `BatchETL-GlueServiceRole` |
   | Glue version | `Glue 4.0` |
   | Language | Python 3 |
   | Worker type | `G.1X` |
   | Number of workers | `2` |
   | Max concurrency | `1` |

6. Scroll to **Job parameters** and add:

   | Key | Value |
   |-----|-------|
   | `--source_path` | `s3://<your-landing-bucket>/sales/` |
   | `--output_path` | `s3://<your-output-bucket>/glue/sales/` |

7. Click **Save** in the top-right corner.

### Option B: Python Script

```bash
python infrastructure/register_glue_jobs.py --script-bucket <your-output-bucket>
```

Expected output:
```
[INFO] Resolving IAM role: BatchETL-GlueServiceRole
[INFO] Role ARN: arn:aws:iam::123456789012:role/BatchETL-GlueServiceRole
[INFO] Script location: s3://etl-course-yourname-output/scripts/glue_simple_etl.py
[INFO] No existing job found — creating new job 'glue-simple-etl'.

[SUCCESS] Glue job created successfully!
  Job name : glue-simple-etl
  Role ARN : arn:aws:iam::123456789012:role/BatchETL-GlueServiceRole
  Script   : s3://etl-course-yourname-output/scripts/glue_simple_etl.py
  Workers  : 2 x G.1X
```

> **Note:** After running the script, update the `--source_path` and `--output_path`
> job parameters in the Glue Console to point to your actual bucket names.

---

## Step 3: Run the Job

### Option A: AWS Console

1. Go to **AWS Glue → ETL Jobs**.
2. Click on `glue-simple-etl`.
3. Click the **Run** button (top right).
4. In the pop-up, confirm the job parameters look correct, then click **Run job**.

The job will show status **Running** for 2–5 minutes, then **Succeeded**.

### Option B: AWS CLI

```bash
aws glue start-job-run \
  --job-name glue-simple-etl \
  --arguments '{
    "--source_path": "s3://<your-landing-bucket>/sales/",
    "--output_path": "s3://<your-output-bucket>/glue/sales/"
  }' \
  --region us-east-1
```

The CLI prints a `JobRunId`. Save it — you'll use it to check the run status.

Expected output:
```json
{
    "JobRunId": "jr_abc123def456"
}
```

Check the run status:
```bash
aws glue get-job-run \
  --job-name glue-simple-etl \
  --run-id jr_abc123def456 \
  --region us-east-1
```

Look for `"JobRunState": "SUCCEEDED"` in the response.

---

## Step 4: Monitor the Job

### AWS Glue Console

1. Go to **Glue → ETL Jobs → glue-simple-etl**.
2. Click the **Runs** tab.
3. Click on the latest run ID to see details: start time, duration, worker metrics.

The run detail page shows:
- **Status**: RUNNING → SUCCEEDED (or FAILED with an error message)
- **Duration**: typically 3–6 minutes for a small dataset
- **Log group**: a link to CloudWatch Logs

### CloudWatch Logs

Glue writes all `print()` output and Python tracebacks to CloudWatch Logs.

1. Go to **CloudWatch → Log groups**.
2. Find `/aws-glue/jobs/output`.
3. Click the log stream matching your run ID.
4. You should see lines like:
   ```
   [INFO] Job 'glue-simple-etl' started.
   [INFO] Source path : s3://etl-course-yourname-landing/sales/
   [INFO] Records read from source: 1000
   [INFO] Records after null order_id filter: 998
   [INFO] Transformations complete. Final record count: 998
   [INFO] Output written to: s3://etl-course-yourname-output/glue/sales/
   [INFO] Job committed successfully.
   ```

If the job failed, the error traceback appears here. This is the first place to look when debugging.

---

## Step 5: Verify the Output

After the job succeeds, check that Parquet files exist in your output bucket partitioned by region.

```bash
aws s3 ls s3://<your-output-bucket>/glue/sales/ --recursive
```

Expected output (one folder per region value):
```
2026-05-19 10:31:22      45123 glue/sales/region=ap-southeast/part-00000.snappy.parquet
2026-05-19 10:31:23      52841 glue/sales/region=ca-central/part-00000.snappy.parquet
2026-05-19 10:31:21      89302 glue/sales/region=eu-west/part-00000.snappy.parquet
2026-05-19 10:31:20     102455 glue/sales/region=us-east/part-00000.snappy.parquet
2026-05-19 10:31:24      71098 glue/sales/region=us-west/part-00000.snappy.parquet
```

**What does partitioning mean?** The output is split into sub-folders, one per region value. If you later run a query like `WHERE region = 'us-east'`, Spark and Athena will only read the `region=us-east/` folder and skip the rest — much faster and cheaper than reading the entire dataset.

**Spot-check a Parquet file locally:**
```bash
# Download one partition
aws s3 cp "s3://<your-output-bucket>/glue/sales/region=us-east/part-00000.snappy.parquet" /tmp/check.parquet

# Read it with Python
python -c "
import pandas as pd
df = pd.read_parquet('/tmp/check.parquet')
print(df.dtypes)
print(df.head())
"
```

You should see:
- `order_id` — object (string)
- `unit_price` — float64
- `quantity` — int32 or int64
- `order_date` — datetime64 or object (date string in Parquet)
- `revenue` — float64

---

## Understanding the Script (line-by-line walkthrough)

```python
from awsglue.utils import getResolvedOptions
```
This imports Glue's argument parser. It reads the `--source_path` and `--output_path` values that were passed to the job at runtime (either from the Console job parameters or from `start-job-run --arguments`).

---

```python
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "output_path"])
```
Reads three arguments from `sys.argv`: the mandatory `JOB_NAME` (always injected by Glue) and our two custom parameters. After this line, `args["source_path"]` holds the S3 path string.

---

```python
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
```
Initialises the Glue runtime. Think of this as "starting the engine":
- `SparkContext` — low-level Spark entry point (manages cluster resources)
- `GlueContext` — wraps `SparkContext` and adds Glue-specific I/O methods
- `spark_session` — the standard DataFrame API (needed for transformations)
- `Job.init()` — registers this run with Glue so bookmarks and state are tracked

---

```python
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["source_path"]]},
    format="csv",
    format_options={"withHeader": "True"}
)
```
Reads the CSV from S3 into a DynamicFrame. `withHeader: True` tells Glue that the first row is column names, not data. Glue reads all CSV files under the given S3 prefix automatically.

---

```python
df = datasource.toDF()
```
Converts the DynamicFrame to a regular Spark DataFrame. From this point on we use the standard PySpark API. All transformation steps below (`filter`, `withColumn`, `cast`) are standard PySpark — nothing Glue-specific.

---

```python
df = df.filter(col("order_id").isNotNull())
```
Drops rows where `order_id` is null. An order without an ID cannot be tracked, joined, or deduplicated — it is useless noise. `isNotNull()` is `True` for non-null values, so this keeps only valid rows.

---

```python
df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
df = df.withColumn("quantity",   col("quantity").cast(IntegerType()))
```
CSV stores everything as text. `"40.10"` is a string, not a number. These two lines convert the string columns to proper numeric types so we can do arithmetic. `cast()` returns `null` for values that cannot be converted (e.g. `"N/A"`).

---

```python
df = df.withColumn("order_date", to_date(col("order_timestamp")))
```
`order_timestamp` looks like `"2026-05-19T22:45:06Z"`. `to_date()` strips the time portion and returns just the date part: `2026-05-19`. This is useful for daily aggregations — grouping by `order_date` is much simpler than grouping by a full timestamp.

---

```python
df = df.withColumn("revenue", col("quantity") * col("unit_price"))
```
Computes `revenue = quantity × unit_price`. We recompute this rather than trusting the `total_price` column from the source CSV because this pipeline should own its own calculations. If the source was wrong, our value will be correct.

---

```python
output_dyf = DynamicFrame.fromDF(df, glueContext, "output")
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={"path": args["output_path"], "partitionKeys": ["region"]},
    format="parquet"
)
```
Converts the DataFrame back to a DynamicFrame and writes it as Parquet. `partitionKeys=["region"]` creates one sub-folder per region value — the Hive-style partitioning format (`region=us-east/`) that Spark, Athena, and Glue all understand natively.

---

```python
job.commit()
```
Tells Glue this run completed successfully. Always the last line of a Glue script. If your script raises an exception before reaching this line, Glue marks the run as FAILED.

---

## Comparing the Two Approaches (DynamicFrame vs DataFrame)

This script mixes both Glue and standard Spark types. Here is when to use each:

| Task | Use | Why |
|------|-----|-----|
| Read from S3 with auto-discovery | DynamicFrame | Handles schema inconsistencies without errors |
| Read from Glue Catalog tables | DynamicFrame | Direct catalog integration |
| Filter, cast, derive columns | Spark DataFrame | Full PySpark API available |
| Joins, window functions, aggregations | Spark DataFrame | Rich SQL-like API |
| Write partitioned Parquet to S3 | DynamicFrame | `partitionKeys` parameter is clean and concise |
| Write to Glue Catalog table | DynamicFrame | Automatic Catalog registration |

The pattern used here — DynamicFrame for I/O, DataFrame for transforms — is the recommended approach for Glue ETL scripts.

---

## Verification Checklist

Work through this list before moving to Task 3B:

- [ ] `glue_simple_etl.py` uploaded to S3 at `s3://<bucket>/scripts/`
- [ ] Glue job `glue-simple-etl` visible in Glue Console → ETL Jobs
- [ ] Job run status shows **SUCCEEDED**
- [ ] CloudWatch Logs show `[INFO] Job committed successfully.`
- [ ] S3 output contains folders named `region=<value>/` under your output path
- [ ] Spot-check: `unit_price` is float, `quantity` is int, `revenue` equals `quantity × unit_price`
- [ ] Unit tests pass locally: `pytest tests/test_glue_transformations.py -v`

---

## Common Errors and Fixes

| Error Message | Likely Cause | Fix |
|---------------|--------------|-----|
| `AccessDeniedException: ... glue:CreateJob` | IAM user lacks Glue permissions | Attach `AWSGlueConsoleFullAccess` to your IAM user |
| `EntityNotFoundException: Role ... not found` | `BatchETL-GlueServiceRole` not created | Run `python infrastructure/iam_roles.py` first |
| `EntityNotFoundException: script ... not found` | Script not uploaded to S3 | Run the `aws s3 cp` command in Step 1 |
| `NoSuchBucket` | Bucket name typo in `--source_path` or `--output_path` | Check bucket names with `aws s3 ls` |
| `FAILED: com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied` | Glue role lacks S3 permissions on this bucket | Verify `BatchETL-GlueServiceRole` policy includes your bucket ARN |
| `pyspark.sql.utils.AnalysisException: Path does not exist` | No CSV files in source path | Upload data first: `python data-generator/upload_to_s3.py` |
| `TypeError: cannot unpack non-sequence NoneType` | `getResolvedOptions` can't find a parameter | Check that job parameters are set in Glue Console or `--arguments` |
| Job stuck in RUNNING for > 15 min | Memory issue on small dataset | This shouldn't happen with 1000-row CSV; check CloudWatch for OOM errors |
| `ModuleNotFoundError: No module named 'awsglue'` | Running the script locally | This script only runs on Glue. Use the unit tests for local validation. |

---

## Cost Impact

This task creates the following cost:

| Resource | Cost |
|----------|------|
| Glue job run (2 × G.1X, ~5 minutes) | ~$0.07 per run |
| S3 PUT requests (Parquet write) | < $0.01 |
| S3 storage (small Parquet output) | < $0.01/month |
| **Total for this task** | **~$0.08** |

Glue charges per DPU-hour. G.1X workers are 1 DPU each. 2 workers for 5 minutes = 2 × (5/60) DPU-hours × $0.44/DPU-hour ≈ $0.07.

**To minimise cost:**
- Only run the job when actively learning — not on a schedule (Task 5 covers scheduling)
- Use small datasets (1000 rows is enough to see the transforms work)
- If you accidentally start a job, cancel it in Glue Console → Jobs → Action → Stop run

---

## Next Step

With your first Glue job working, the next task is Task 3B where you implement the **exact same transformations using native PySpark on EMR Serverless** — no DynamicFrames, no Glue libraries. This lets you compare the two approaches side by side and understand the tradeoffs.

→ Continue to **[Task 3B — EMR Serverless Simple ETL](03b-emr-simple-etl-guide.md)**
