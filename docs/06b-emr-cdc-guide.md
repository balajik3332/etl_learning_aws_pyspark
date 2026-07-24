# Task 6B — CDC Pattern with EMR Serverless (S3 Checkpoint)

## What Are We Building?

The same CDC + SCD Type 2 pattern from Task 6A, but implemented using EMR Serverless with a **custom S3 checkpoint file** instead of Glue job bookmarks.

This teaches you how incremental processing works at a code level — no magic, just a JSON file in S3 that tracks where you left off.

---

## Concepts Explained

### Why Doesn't EMR Have Job Bookmarks?
EMR Serverless gives you a plain Apache Spark environment. There is no built-in "remember where you left off" feature — that's a Glue-specific capability.

Instead, we implement our own checkpoint: a small JSON file stored in S3 that records the last-processed timestamp. On each run the script:
1. Reads the checkpoint file to find `last_processed_timestamp`
2. Filters source data: only rows where `updated_at > last_processed_timestamp`
3. Processes the filtered rows
4. Updates the checkpoint with the new max timestamp

### Tradeoff: Glue Bookmarks vs S3 Checkpoint

| | Glue Job Bookmarks | EMR S3 Checkpoint |
|--|-------------------|-------------------|
| Code required | Zero (it's automatic) | ~20 extra lines |
| Flexibility | Tracks file position only | You control what's tracked |
| Transparency | Black box | You can read/edit the JSON |
| Portability | Glue-only | Works anywhere (EMR, local, etc.) |
| Reset | Click button in Console | Delete or edit the JSON file |

### When Would You Choose EMR Checkpoint Over Glue Bookmarks?
- When you need to track multiple dimensions (not just file position)
- When you need the checkpoint value for other processes (e.g., trigger a downstream job only if data is newer than X)
- When your team doesn't use Glue at all
- When you need the checkpoint to be human-readable and editable

---

## Prerequisites

- [ ] Task 6A complete — run the Glue CDC scenario first to understand the SCD logic
- [ ] EMR Serverless application `etl-course-spark` exists

---

## The Checkpoint File

The checkpoint is a simple JSON file stored in S3:

```json
{
  "last_processed_timestamp": "2024-01-15T10:00:00Z",
  "last_run_at": "2024-01-15T10:08:00Z",
  "rows_processed": 1000
}
```

On the **first run**, the file doesn't exist yet. The script handles this by setting `last_processed_timestamp` to a far-past date (`1970-01-01T00:00:00Z`) so all records are processed.

---

## The CDC Scenario — Step by Step

### Step 1 — Upload and Run the First Pass

```bash
# Generate full load data (same as 6A)
python data-generator/generate_cdc_data.py --rows 1000 --mode full
python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing --prefix cdc/

# Upload EMR script
aws s3 cp emr-jobs/cdc/emr_cdc_job.py \
  s3://etl-course-yourname-output/scripts/emr_cdc_job.py

# Submit job (first run — no checkpoint exists yet)
python infrastructure/register_glue_jobs.py \
  --submit-emr-job \
  --script s3://etl-course-yourname-output/scripts/emr_cdc_job.py \
  --app-name etl-course-spark \
  --source s3://etl-course-yourname-landing/cdc/ \
  --output s3://etl-course-yourname-processed/emr-cdc/ \
  --checkpoint s3://etl-course-yourname-processed/checkpoints/cdc_checkpoint.json
```

**Expected result:** 1000 rows processed. Checkpoint file created:
```json
{"last_processed_timestamp": "2024-01-15T09:59:00Z", "rows_processed": 1000}
```

Check the checkpoint was created:
```bash
aws s3 cp s3://etl-course-yourname-processed/checkpoints/cdc_checkpoint.json - | python -m json.tool
```

### Step 2 — Generate Incremental Data

```bash
python data-generator/generate_cdc_data.py --rows 50 --mode incremental
python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing --prefix cdc/
```

### Step 3 — Run the Second Pass

```bash
python infrastructure/register_glue_jobs.py \
  --submit-emr-job \
  --script s3://etl-course-yourname-output/scripts/emr_cdc_job.py \
  --app-name etl-course-spark \
  --source s3://etl-course-yourname-landing/cdc/ \
  --output s3://etl-course-yourname-processed/emr-cdc/ \
  --checkpoint s3://etl-course-yourname-processed/checkpoints/cdc_checkpoint.json
```

**Expected result:** Only 50 rows processed (those with `updated_at` newer than the checkpoint). Checkpoint advances to new max timestamp.

---

## The Script — Checkpoint Logic Walkthrough

File: `emr-jobs/cdc/emr_cdc_job.py`

```python
import sys, json, boto3
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max

def read_checkpoint(s3_client, bucket, key):
    """Read the last-processed timestamp from S3. Returns epoch start if not found."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read())
        return data["last_processed_timestamp"]
    except s3_client.exceptions.NoSuchKey:
        # First run — process everything
        return "1970-01-01T00:00:00Z"

def write_checkpoint(s3_client, bucket, key, timestamp, rows_processed):
    """Save the new checkpoint back to S3 after a successful run."""
    data = {
        "last_processed_timestamp": timestamp,
        "last_run_at": datetime.now(timezone.utc).isoformat(),
        "rows_processed": rows_processed,
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
    )

def main():
    spark = SparkSession.builder.appName("EMR-CDC").getOrCreate()
    s3 = boto3.client("s3", region_name="us-east-1")

    source_path     = sys.argv[1]
    output_path     = sys.argv[2]
    checkpoint_path = sys.argv[3]  # e.g. s3://bucket/checkpoints/cdc.json

    # Parse bucket and key from checkpoint_path
    parts = checkpoint_path.replace("s3://", "").split("/", 1)
    chk_bucket, chk_key = parts[0], parts[1]

    # 1. Read checkpoint — where did we stop last time?
    last_ts = read_checkpoint(s3, chk_bucket, chk_key)
    print(f"[INFO] Last processed timestamp: {last_ts}")

    # 2. Read all source data
    df_all = spark.read.parquet(source_path)  # or .csv() depending on format

    # 3. Filter: only rows newer than the checkpoint
    df_new = df_all.filter(col("updated_at") > last_ts)
    print(f"[INFO] New records to process: {df_new.count()}")

    if df_new.count() == 0:
        print("[INFO] No new data. Exiting.")
        spark.stop()
        return

    # 4. Apply SCD Type 2 logic (same as Glue version)
    updated_history = apply_scd_type2(spark, df_new, output_path)

    # 5. Write updated history table
    updated_history.write.mode("overwrite").parquet(output_path)

    # 6. Advance the checkpoint to the max updated_at seen in this batch
    new_ts = df_new.agg(spark_max("updated_at")).collect()[0][0]
    write_checkpoint(s3, chk_bucket, chk_key, new_ts, df_new.count())
    print(f"[INFO] Checkpoint advanced to: {new_ts}")

    spark.stop()
```

---

## Compare Outputs: Glue vs EMR

After running both Task 6A and 6B on the same input data, compare the SCD history tables:

```bash
# Count current records in Glue output
aws s3 ls s3://etl-course-yourname-processed/cdc/ --recursive | wc -l

# Count current records in EMR output
aws s3 ls s3://etl-course-yourname-processed/emr-cdc/ --recursive | wc -l
```

Both should have identical row counts. The SCD logic is the same — only the incremental tracking mechanism differs.

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| All 1000 rows processed on second run | Checkpoint not saved or wrong path | Verify checkpoint file exists: `aws s3 ls s3://.../checkpoints/` |
| `NoSuchKey` error on checkpoint read | First run, expected | Script handles this — processes all data |
| Wrong rows filtered | `updated_at` column format mismatch | Ensure both checkpoint and data use ISO 8601 strings |
| Checkpoint advances but wrong timestamp | `spark_max` aggregation on wrong column | Confirm column name is exactly `updated_at` |
| SCD history table grows unbounded | Old versions not being retained correctly | This is expected — use a periodic compaction job to archive old versions |

---

## Cost Impact

Same as 6A: ~$0.03–$0.07 per run. Second run is cheaper because less data is processed.

---

## Next Step

→ `docs/07a-glue-pipeline-guide.md` — Build a 4-stage Glue Workflow that chains Raw → Clean → Enrich → Aggregate jobs with automatic failure handling.
