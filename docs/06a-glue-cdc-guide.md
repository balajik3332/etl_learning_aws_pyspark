# Task 6A — CDC Pattern with AWS Glue Job Bookmarks

## What Are We Building?

A Glue ETL job that processes only *new* data on each run (incremental processing), combined with SCD Type 2 history tracking so we never lose old versions of records.

This is **Change Data Capture (CDC)** — one of the most important patterns in real data engineering.

---

## Concepts Explained

### What is CDC (Change Data Capture)?
In production, datasets grow continuously. If you reprocess all 50 million rows every night just because 500 rows changed, you're wasting time and money. CDC means you only process the records that are new or changed since the last run.

Real databases emit a stream of change events: INSERT (new record), UPDATE (existing record changed), DELETE (record removed). Your ETL job consumes these events and applies them to the target table.

### What is SCD Type 2?
SCD stands for Slowly Changing Dimension. Type 2 is the most common history-tracking pattern. Instead of overwriting a record when it changes, you:
1. Close the old version (set `valid_to` date = today, `is_current` = False)
2. Insert a new version (set `valid_from` = today, `is_current` = True)

This lets you answer time-travel questions: "What was this customer's email address in January?"

Example — a user changes their email:

| user_id | email | valid_from | valid_to | is_current |
|---------|-------|-----------|---------|-----------|
| 101 | old@example.com | 2024-01-01 | 2024-06-15 | False |
| 101 | new@example.com | 2024-06-15 | 9999-12-31 | True |

### What are Glue Job Bookmarks?
A Glue Job Bookmark is like a bookmark in a book — Glue remembers exactly where it stopped reading last time. On the next run it picks up from that point automatically. **You don't write any bookmark code** — you just enable the feature and Glue handles it.

Internally, Glue tracks which S3 objects it has already processed based on their ETags and timestamps. New files added after the last run are processed; already-processed files are skipped.

---

## Prerequisites

- [ ] Task 1 complete (S3 buckets and Glue role exist)
- [ ] Task 2 complete (`generate_cdc_data.py` available)
- [ ] Task 3A complete (understand the basic Glue job structure)

---

## The CDC Scenario — Step by Step

### Step 1 — Generate Full Load Data

```bash
python data-generator/generate_cdc_data.py --rows 1000 --mode full
python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing --prefix cdc/
```

This generates 1000 rows all with `operation=INSERT` (a full initial load) and uploads to `s3://.../cdc/run_001/`.

### Step 2 — Run the Glue CDC Job (First Run)

Upload the script and register the job:
```bash
aws s3 cp glue-jobs/cdc/glue_cdc_job.py \
  s3://etl-course-yourname-output/scripts/glue_cdc_job.py

python infrastructure/register_glue_jobs.py --register-cdc
```

Start the job:
```bash
aws glue start-job-run \
  --job-name glue-cdc \
  --arguments '{"--source_path":"s3://etl-course-yourname-landing/cdc/","--output_path":"s3://etl-course-yourname-processed/cdc/"}'
```

**Expected result:** 1000 rows processed, all inserted into the SCD history table with `is_current=True`.

### Step 3 — Generate Incremental Data

```bash
python data-generator/generate_cdc_data.py --rows 50 --mode incremental
python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing --prefix cdc/
```

This generates 50 rows with mixed operations: ~10 INSERTs, ~30 UPDATEs, ~10 DELETEs.

### Step 4 — Run the Glue CDC Job (Second Run)

```bash
aws glue start-job-run \
  --job-name glue-cdc \
  --arguments '{"--source_path":"s3://etl-course-yourname-landing/cdc/","--output_path":"s3://etl-course-yourname-processed/cdc/"}'
```

**Expected result:** Only 50 rows processed (the bookmark skipped the 1000 rows from run 1). The SCD table now shows version history for the updated records.

### Step 5 — Verify the Bookmark is Working

In the Glue Console:
1. Go to **AWS Glue** → **Jobs** → `glue-cdc`
2. Click the **Run history** tab
3. Both runs should show `SUCCEEDED`
4. Check the **Data bytes scanned** metric — Run 2 should show far less data scanned than Run 1

---

## The SCD Logic — Script Walkthrough

File: `glue-jobs/cdc/glue_cdc_job.py`

```python
# Key SCD Type 2 logic — explained step by step

# 1. Read incoming CDC events (only new ones, thanks to job bookmark)
new_events = datasource.toDF()

# 2. Read the existing SCD history table from S3 (if it exists)
existing = spark.read.parquet(output_path) if history_exists else empty_df

# 3. Handle INSERTs — new records that don't exist in history
inserts = new_events.filter(col("operation") == "INSERT") \
    .withColumn("valid_from", col("updated_at")) \
    .withColumn("valid_to", lit("9999-12-31")) \
    .withColumn("is_current", lit(True)) \
    .withColumn("is_deleted", lit(False))

# 4. Handle UPDATEs — close old version, create new version
#    Step 4a: Close the old version of updated records
old_records_to_close = existing \
    .join(updates.select("order_id"), "order_id", "inner") \
    .withColumn("valid_to", col("updated_at")) \
    .withColumn("is_current", lit(False))

#    Step 4b: Insert the new version
new_versions = updates \
    .withColumn("valid_from", col("updated_at")) \
    .withColumn("valid_to", lit("9999-12-31")) \
    .withColumn("is_current", lit(True))

# 5. Handle DELETEs — mark as deleted without removing
deleted_records = existing \
    .join(deletes.select("order_id"), "order_id", "inner") \
    .withColumn("is_current", lit(False)) \
    .withColumn("is_deleted", lit(True))

# 6. Union all pieces into the updated history table
updated_history = unchanged \
    .union(old_records_to_close) \
    .union(new_versions) \
    .union(inserts) \
    .union(deleted_records)

# 7. Write back to S3
updated_history.write.mode("overwrite").parquet(output_path)
```

---

## Enabling Job Bookmarks

When registering the job, set:
```python
glue.create_job(
    Name="glue-cdc",
    DefaultArguments={
        "--job-bookmark-option": "job-bookmark-enable",  # ← this is the key line
        ...
    }
)
```

Or in the Console:
1. Glue → Jobs → `glue-cdc` → Edit
2. Scroll to **Advanced properties**
3. Set **Job bookmark** to **Enable**

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| Second run processes same rows again | Bookmark not enabled | Verify `--job-bookmark-option job-bookmark-enable` in job defaults |
| `AnalysisException: path does not exist` | History table not yet created | Script should handle first-run case (no existing history) |
| SCD shows duplicate `is_current=True` for same key | Bug in join logic | Add deduplication step after union |
| All records marked as DELETE | Wrong `operation` column values | Check CDC data generator: should be `INSERT`/`UPDATE`/`DELETE` in uppercase |
| Bookmark resets unexpectedly | Job was reset manually | Avoid using "Reset job bookmark" in Console unless you want a full reload |

---

## Cost Impact

- Same as simple ETL: ~$0.07 per run with 2 G.1X workers
- Second run processes less data → typically faster → slightly cheaper
- **Estimated cost for running both scenarios: ~$0.10–$0.15**

---

## Next Step

→ `docs/06b-emr-cdc-guide.md` — Implement the same CDC pattern using PySpark on EMR Serverless with a custom S3 checkpoint file instead of Glue bookmarks.
