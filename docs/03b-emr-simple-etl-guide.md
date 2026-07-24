# Task 3B — Simple ETL with EMR Serverless

## What Are We Building?

We're implementing the exact same ETL logic from Task 3A — read CSV sales data, clean and transform it, write Parquet output — but this time using **native PySpark on EMR Serverless** instead of AWS Glue.

Running both versions back-to-back lets you directly compare the developer experience, code complexity, and cost between the two services.

---

## Concepts Explained

### What is EMR Serverless?
EMR (Elastic MapReduce) Serverless is AWS's managed Apache Spark service. Unlike Glue, you write pure PySpark with no special AWS libraries. AWS starts a Spark cluster for your job, runs it, and shuts everything down automatically when done. You pay only for the time your job runs (billed per vCPU-second and GB-second).

### EMR Serverless vs AWS Glue — Key Differences So Far

| | AWS Glue | EMR Serverless |
|--|----------|----------------|
| API | `GlueContext` + `DynamicFrame` | `SparkSession` + `DataFrame` |
| Job editor | Visual drag-and-drop | Code only |
| Data Catalog | Built-in integration | Manual schema management |
| Incremental processing | Job bookmarks (built-in) | Custom S3 checkpoint (you write it) |
| Debugging | Glue Console + CloudWatch | EMR Console + CloudWatch |
| Script language | Python or Scala | Python, Scala, or R |

### What is an EMR Serverless Application?
An Application is a persistent resource that holds your pre-configured Spark environment. You create it once and submit multiple job runs to it. When idle, it costs nothing. Think of it as a Spark cluster that exists on paper until you actually need it.

---

## Prerequisites

- [ ] Completed Task 1 (S3 buckets and IAM roles exist)
- [ ] Completed Task 2 (sales CSV files are in your landing bucket)
- [ ] Completed Task 3A (understand what the ETL does before running it with EMR)

---

## Step 1 — Create an EMR Serverless Application

### Console path:
1. Go to **Amazon EMR** → **Serverless** in the left sidebar
2. Click **Create application**
3. Settings:
   - **Name**: `etl-course-spark`
   - **Release**: `emr-6.15.0`
   - **Type**: Spark
   - **Pre-initialized capacity**: Leave disabled (saves cost)
   - **Maximum capacity**: Leave defaults
4. Click **Create application**

The application starts in `CREATED` state. It will auto-start when you submit a job.

### Or via script:
```bash
python infrastructure/register_glue_jobs.py --create-emr-app
```

---

## Step 2 — Upload the PySpark Script to S3

```bash
aws s3 cp emr-jobs/simple-etl/emr_simple_etl.py \
  s3://etl-course-yourname-output/scripts/emr_simple_etl.py
```

---

## Step 3 — Submit the Job

```bash
python infrastructure/register_glue_jobs.py \
  --submit-emr-job \
  --app-name etl-course-spark \
  --script s3://etl-course-yourname-output/scripts/emr_simple_etl.py \
  --source s3://etl-course-yourname-landing/sales/ \
  --output s3://etl-course-yourname-output/emr/sales/
```

Or submit directly via AWS CLI:
```bash
# Get your application ID first
APP_ID=$(aws emr-serverless list-applications \
  --query "applications[?name=='etl-course-spark'].id" \
  --output text)

ROLE_ARN=$(aws iam get-role \
  --role-name BatchETL-EMRServerlessRole \
  --query "Role.Arn" --output text)

aws emr-serverless start-job-run \
  --application-id $APP_ID \
  --execution-role-arn $ROLE_ARN \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://etl-course-yourname-output/scripts/emr_simple_etl.py",
      "entryPointArguments": [
        "s3://etl-course-yourname-landing/sales/",
        "s3://etl-course-yourname-output/emr/sales/"
      ],
      "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
    }
  }'
```

---

## Step 4 — Monitor the Job

**EMR Console:**
1. Go to **EMR** → **Serverless** → click your application
2. Click the **Job runs** tab
3. Click your job run to see details: status, duration, logs

**CloudWatch Logs:**
- Log group: `/aws/emr-serverless/{application-id}/spark`
- The driver log (`stdout`) shows your `print()` statements
- The driver log (`stderr`) shows any errors

**Job states:**
- `PENDING` → waiting for compute to start
- `RUNNING` → actively executing
- `SUCCESS` → completed with no errors
- `FAILED` → check the driver stderr log

---

## Step 5 — Verify the Output

```bash
aws s3 ls s3://etl-course-yourname-output/emr/sales/ --recursive
```

You should see Parquet files partitioned by region:
```
emr/sales/region=east/part-00000-xxx.parquet
emr/sales/region=west/part-00000-xxx.parquet
emr/sales/region=north/part-00000-xxx.parquet
```

---

## The PySpark Script — Line by Line

File: `emr-jobs/simple-etl/emr_simple_etl.py`

```python
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

def main():
    # 1. Create a SparkSession — the entry point to all Spark functionality
    spark = SparkSession.builder \
        .appName("SimpleETL") \
        .getOrCreate()

    # 2. Read source path and output path from command-line arguments
    #    sys.argv[1] = source_path, sys.argv[2] = output_path
    source_path = sys.argv[1]
    output_path = sys.argv[2]

    # 3. Read CSV file — header=true tells Spark the first row has column names
    df = spark.read.option("header", "true").csv(source_path)

    # 4. Filter: drop rows where order_id is null (bad data)
    df = df.filter(col("order_id").isNotNull())

    # 5. Cast columns to correct types (CSV reads everything as string)
    df = df.withColumn("unit_price", col("unit_price").cast("double")) \
           .withColumn("quantity", col("quantity").cast("int"))

    # 6. Add a date-only column extracted from the timestamp
    df = df.withColumn("order_date", to_date(col("order_timestamp")))

    # 7. Compute revenue = quantity × unit_price
    df = df.withColumn("revenue", col("quantity") * col("unit_price"))

    # 8. Write output as Parquet, partitioned by region
    #    mode("overwrite") replaces any existing output
    df.write.mode("overwrite").partitionBy("region").parquet(output_path)

    print(f"[OK] ETL complete. Rows written: {df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
```

**Key differences from the Glue version:**
- No `GlueContext` or `DynamicFrame` imports — pure PySpark
- No `job.init()` or `job.commit()` calls
- Arguments come from `sys.argv` instead of `getResolvedOptions`
- `SparkSession.builder` instead of `SparkContext` + `GlueContext`

---

## Step 6 — Compare with Glue Output

Run both versions on the same input, then compare:

```bash
# Count rows in Glue output
aws s3 ls s3://etl-course-yourname-output/glue/sales/ --recursive | wc -l

# Count rows in EMR output
aws s3 ls s3://etl-course-yourname-output/emr/sales/ --recursive | wc -l
```

Both should produce the same number of Parquet files and equivalent data.

**Side-by-side comparison:**

| Metric | AWS Glue | EMR Serverless |
|--------|----------|----------------|
| Script length | ~55 lines | ~40 lines |
| Special AWS imports | Yes (awsglue.*) | No |
| Job setup overhead | `GlueContext`, `Job.init()` | `SparkSession.builder` only |
| Runtime (small dataset) | ~2-3 min | ~2-3 min |
| Cost per run (2 workers) | ~$0.07 | ~$0.04 |
| Debugging | Glue Console | EMR Console + CloudWatch |

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `Application not found` | App name wrong or wrong region | Check `aws emr-serverless list-applications` |
| `AccessDeniedException` | EMR role missing S3 permission | Verify `BatchETL-EMRServerlessRole` has S3 access |
| Job stays in `PENDING` | Application cold starting | Wait 1-2 minutes for first run |
| `java.lang.ClassNotFoundException` | Wrong Spark release for your script | Use `emr-6.15.0` or later |
| Empty output folder | Source path wrong or no data | Check `--source` path matches your S3 structure |
| Script path not found | Script not uploaded to S3 | Re-run the `aws s3 cp` command |

---

## Cost Impact

- EMR Serverless: ~$0.052 per vCPU-hour + $0.0057 per GB-hour
- A 10-minute job with 2 vCPUs and 4 GB RAM: approximately **$0.03**
- Application is free when idle (no pre-initialized capacity)

**Estimated cost for this task: $0.03–$0.10 depending on how many runs you do**

---

## Next Step

→ `docs/04-lambda-triggers-guide.md` — Build Lambda functions that automatically start your Glue or EMR job when new data arrives in S3.
