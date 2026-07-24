"""
glue_cdc_job.py — AWS Glue CDC job with job bookmarks + SCD Type 2

Reads incremental CDC data from S3 (only new files since last run,
tracked automatically by Glue job bookmarks), applies SCD Type 2 logic
to maintain a full history table, and writes the result back to S3.

Job arguments (set in Glue job definition):
    --source_path   S3 path to incoming CDC CSV files
    --output_path   S3 path for the SCD history Parquet table
    --JOB_NAME      Injected by Glue runtime

Enable job bookmark in the Glue job definition:
    --job-bookmark-option job-bookmark-enable
"""

import sys
import json
import os
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max as spark_max

# ---------------------------------------------------------------------------
# Glue bootstrap
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_path", "output_path"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = args["source_path"]
OUTPUT_PATH = args["output_path"]
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")

start_time = datetime.now(timezone.utc)
job_name = args["JOB_NAME"]


# ---------------------------------------------------------------------------
# Helper — publish job status to SNS
# ---------------------------------------------------------------------------
def publish_status(status: str, rows_processed: int) -> None:
    if not SNS_TOPIC_ARN:
        return
    sns = boto3.client("sns", region_name="us-east-1")
    end_time = datetime.now(timezone.utc)
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"ETL Job Status: {status}",
        Message=json.dumps({
            "job_name": job_name,
            "track": "glue",
            "status": status,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": int((end_time - start_time).total_seconds()),
            "rows_processed": rows_processed,
            "output_path": OUTPUT_PATH,
        }),
    )


# ---------------------------------------------------------------------------
# Helper — apply SCD Type 2 logic
# ---------------------------------------------------------------------------
def apply_scd_type2(new_events: DataFrame, existing: DataFrame) -> DataFrame:
    """
    Merge incoming CDC events into the existing SCD history table.

    Rules:
      INSERT  → add new row with is_current=True, valid_to='9999-12-31'
      UPDATE  → close old version (is_current=False), insert new version
      DELETE  → mark matching current row as is_deleted=True, is_current=False
    """
    inserts = new_events.filter(col("operation") == "INSERT") \
        .withColumn("valid_from", col("updated_at")) \
        .withColumn("valid_to", lit("9999-12-31")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("is_deleted", lit(False))

    updates = new_events.filter(col("operation") == "UPDATE")
    deletes = new_events.filter(col("operation") == "DELETE")

    # Rows in existing history that are NOT affected by any update or delete
    updated_ids = updates.select("order_id")
    deleted_ids = deletes.select("order_id")
    all_changed_ids = updated_ids.union(deleted_ids).distinct()

    unchanged = existing.join(all_changed_ids, "order_id", "left_anti")

    # Close old versions for updated records
    old_closed = existing \
        .join(updated_ids, "order_id", "inner") \
        .filter(col("is_current") == True) \
        .withColumn("valid_to", updates.select("updated_at")
                    .join(existing.select("order_id"), "order_id", "inner")
                    .select("updated_at").limit(1).collect()[0][0]
                    if False else col("valid_to")) \
        .withColumn("is_current", lit(False))

    # Simpler approach: broadcast update timestamps for the close operation
    update_ts_map = {
        row["order_id"]: row["updated_at"]
        for row in updates.select("order_id", "updated_at").collect()
    }
    update_ts_broadcast = sc.broadcast(update_ts_map)

    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    @udf(StringType())
    def get_close_ts(order_id):
        return update_ts_broadcast.value.get(order_id, "9999-12-31")

    old_closed = existing \
        .join(updated_ids, "order_id", "inner") \
        .filter(col("is_current") == True) \
        .withColumn("valid_to", get_close_ts(col("order_id"))) \
        .withColumn("is_current", lit(False))

    # New versions for updated records
    new_versions = updates \
        .withColumn("valid_from", col("updated_at")) \
        .withColumn("valid_to", lit("9999-12-31")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("is_deleted", lit(False))

    # Mark deleted records
    deleted_closed = existing \
        .join(deleted_ids, "order_id", "inner") \
        .filter(col("is_current") == True) \
        .withColumn("is_current", lit(False)) \
        .withColumn("is_deleted", lit(True))

    # Union all parts — ensure column alignment
    history_cols = [
        "order_id", "product_name", "category", "quantity", "unit_price",
        "total_price", "customer_id", "order_timestamp", "region",
        "operation", "updated_at", "valid_from", "valid_to",
        "is_current", "is_deleted",
    ]

    def align(df: DataFrame) -> DataFrame:
        for c in history_cols:
            if c not in df.columns:
                df = df.withColumn(c, lit(None))
        return df.select(history_cols)

    return (
        align(unchanged)
        .union(align(old_closed))
        .union(align(new_versions))
        .union(align(inserts))
        .union(align(deleted_closed))
    )


# ---------------------------------------------------------------------------
# Main ETL logic
# ---------------------------------------------------------------------------
rows_processed = 0

try:
    # 1. Read new CDC events — job bookmark ensures only new files are read
    print(f"[INFO] Reading CDC data from: {SOURCE_PATH}")
    new_events_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [SOURCE_PATH], "recurse": True},
        format="csv",
        format_options={"withHeader": True},
    )
    new_events = new_events_dyf.toDF()
    rows_processed = new_events.count()
    print(f"[INFO] New CDC rows to process: {rows_processed}")

    if rows_processed == 0:
        print("[INFO] No new data since last run. Exiting.")
        publish_status("SUCCEEDED", 0)
        job.commit()
        sys.exit(0)

    # 2. Try reading existing SCD history table
    try:
        existing = spark.read.parquet(OUTPUT_PATH)
        print(f"[INFO] Existing history rows: {existing.count()}")
    except Exception:
        print("[INFO] No existing history table found — this is the first run.")
        # Build an empty DataFrame with the expected schema
        existing = spark.createDataFrame([], new_events.schema) \
            .withColumn("valid_from", lit(None)) \
            .withColumn("valid_to", lit(None)) \
            .withColumn("is_current", lit(None).cast("boolean")) \
            .withColumn("is_deleted", lit(None).cast("boolean"))

    # 3. Apply SCD Type 2
    updated_history = apply_scd_type2(new_events, existing)

    # 4. Write updated history table
    updated_history.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[INFO] SCD history written to: {OUTPUT_PATH}")
    print(f"[INFO] Total history rows: {updated_history.count()}")

    publish_status("SUCCEEDED", rows_processed)
    job.commit()

except Exception as e:
    print(f"[ERROR] Job failed: {e}")
    publish_status("FAILED", rows_processed)
    raise
