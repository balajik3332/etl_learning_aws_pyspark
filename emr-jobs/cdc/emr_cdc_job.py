"""
emr-jobs/cdc/emr_cdc_job.py
-----------------------------
Pure PySpark CDC job for EMR Serverless with S3 checkpoint + SCD Type 2.

Implements the same Change Data Capture + Slowly Changing Dimension Type 2
logic as the Glue version (glue-jobs/cdc/glue_cdc_job.py), but replaces
Glue job bookmarks with a lightweight JSON checkpoint file stored in S3.

INCREMENTAL PROCESSING STRATEGY:
  Glue job bookmarks track file positions automatically (black box).
  This script instead maintains a JSON checkpoint in S3:
    {
      "last_processed_timestamp": "2026-05-19T22:45:06Z",
      "last_run_at": "2026-05-19T23:00:00Z",
      "rows_processed": 1000
    }
  On each run the script reads this file, filters source rows where
  updated_at > last_processed_timestamp, processes only that subset,
  then updates the checkpoint with the new max updated_at value.

HOW TO RUN:
  1. Upload this script:
       aws s3 cp emr-jobs/cdc/emr_cdc_job.py \\
           s3://<your-bucket>/scripts/emr_cdc_job.py

  2. Submit to EMR Serverless:
       aws emr-serverless start-job-run \\
           --application-id $APP_ID \\
           --execution-role-arn $ROLE_ARN \\
           --job-driver '{
             "sparkSubmit": {
               "entryPoint": "s3://<your-bucket>/scripts/emr_cdc_job.py",
               "entryPointArguments": [
                 "s3://<landing>/cdc/",
                 "s3://<processed>/emr-cdc/",
                 "s3://<processed>/checkpoints/cdc_checkpoint.json"
               ]
             }
           }'

ARGUMENTS (positional):
  sys.argv[1]  source_path       — S3 URI to incoming CDC CSV files
  sys.argv[2]  output_path       — S3 URI for SCD history Parquet table
  sys.argv[3]  checkpoint_path   — S3 URI to checkpoint JSON file

SCD TYPE 2 RULES:
  INSERT  → new row with is_current=True,  valid_to='9999-12-31'
  UPDATE  → close old version (is_current=False), insert new version
  DELETE  → mark current row is_deleted=True, is_current=False

KEY DIFFERENCES FROM GLUE VERSION:
  - No GlueContext / awsglue.* — pure PySpark
  - No job bookmarks — manual S3 checkpoint tracks last processed timestamp
  - Checkpoint is human-readable and editable (plain JSON)
  - To reset: delete or edit the checkpoint file in S3
"""

import sys
import os
import json
import boto3
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, max as spark_max


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Split 's3://bucket/key' into ('bucket', 'key')."""
    path = s3_uri.replace("s3://", "")
    bucket, _, key = path.partition("/")
    return bucket, key


def read_checkpoint(s3_client, checkpoint_path: str) -> str:
    """
    Read last_processed_timestamp from the S3 checkpoint file.
    Returns '1970-01-01T00:00:00Z' on first run (file does not exist yet),
    which causes all source records to pass the timestamp filter.
    """
    bucket, key = _parse_s3_uri(checkpoint_path)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        ts = data["last_processed_timestamp"]
        print(f"[INFO] Checkpoint read. Last processed timestamp: {ts}")
        return ts
    except s3_client.exceptions.NoSuchKey:
        print("[INFO] No checkpoint found — this is the first run. Processing all records.")
        return "1970-01-01T00:00:00Z"


def write_checkpoint(s3_client, checkpoint_path: str,
                     new_timestamp: str, rows_processed: int) -> None:
    """
    Write an updated checkpoint back to S3 after a successful run.
    Overwrites the previous file entirely.
    """
    bucket, key = _parse_s3_uri(checkpoint_path)
    data = {
        "last_processed_timestamp": new_timestamp,
        "last_run_at": datetime.now(timezone.utc).isoformat(),
        "rows_processed": rows_processed,
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[INFO] Checkpoint updated → last_processed_timestamp: {new_timestamp}")


# ── SCD Type 2 logic ──────────────────────────────────────────────────────────

def apply_scd_type2(new_events: DataFrame, existing: DataFrame) -> DataFrame:
    """
    Merge incoming CDC events into the existing SCD history table.

    Parameters
    ----------
    new_events : DataFrame
        Filtered rows from this run's source data (INSERT / UPDATE / DELETE).
    existing : DataFrame
        The current state of the full SCD history table (may be empty on
        the first run).

    Returns
    -------
    DataFrame
        Updated history table with all versions of every record.
    """

    # ── Partition by operation type ───────────────────────────────────────────
    inserts = new_events.filter(col("operation") == "INSERT")
    updates = new_events.filter(col("operation") == "UPDATE")
    deletes = new_events.filter(col("operation") == "DELETE")

    updated_ids = updates.select("order_id")
    deleted_ids = deletes.select("order_id")
    all_changed_ids = updated_ids.union(deleted_ids).distinct()

    # ── Records not touched by this batch ─────────────────────────────────────
    unchanged = existing.join(all_changed_ids, "order_id", "left_anti")

    # ── INSERTs: brand new records ────────────────────────────────────────────
    new_inserts = inserts \
        .withColumn("valid_from", col("updated_at")) \
        .withColumn("valid_to", lit("9999-12-31")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("is_deleted", lit(False))

    # ── UPDATEs: close old version, open new version ──────────────────────────
    # Broadcast the update-timestamp map so the UDF can look it up per row
    # without a shuffle join.
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    update_ts_map: dict = {
        row["order_id"]: row["updated_at"]
        for row in updates.select("order_id", "updated_at").collect()
    }
    update_ts_bc = new_events.sparkSession.sparkContext.broadcast(update_ts_map)

    @udf(StringType())
    def lookup_close_ts(order_id: str) -> str:
        return update_ts_bc.value.get(order_id, "9999-12-31")

    # Close existing current rows for updated order_ids
    old_closed = existing \
        .join(updated_ids, "order_id", "inner") \
        .filter(col("is_current") == True) \
        .withColumn("valid_to", lookup_close_ts(col("order_id"))) \
        .withColumn("is_current", lit(False))

    # New version rows from the UPDATE events
    new_versions = updates \
        .withColumn("valid_from", col("updated_at")) \
        .withColumn("valid_to", lit("9999-12-31")) \
        .withColumn("is_current", lit(True)) \
        .withColumn("is_deleted", lit(False))

    # ── DELETEs: mark current row as deleted ──────────────────────────────────
    deleted_closed = existing \
        .join(deleted_ids, "order_id", "inner") \
        .filter(col("is_current") == True) \
        .withColumn("is_current", lit(False)) \
        .withColumn("is_deleted", lit(True))

    # ── Union all parts with aligned columns ──────────────────────────────────
    history_cols = [
        "order_id", "product_name", "category", "quantity", "unit_price",
        "total_price", "customer_id", "order_timestamp", "region",
        "operation", "updated_at", "valid_from", "valid_to",
        "is_current", "is_deleted",
    ]

    def align(df: DataFrame) -> DataFrame:
        """Add any missing columns as null so every part has identical schema."""
        for c in history_cols:
            if c not in df.columns:
                df = df.withColumn(c, lit(None))
        return df.select(history_cols)

    return (
        align(unchanged)
        .union(align(old_closed))
        .union(align(new_versions))
        .union(align(new_inserts))
        .union(align(deleted_closed))
    )


# ── SNS helper ────────────────────────────────────────────────────────────────

def publish_status(sns_topic_arn: str, job_name: str, status: str,
                   start_time: datetime, rows: int, output_path: str) -> None:
    """Publish job completion status to SNS (no-op if topic ARN not set)."""
    if not sns_topic_arn:
        return
    sns = boto3.client("sns", region_name="us-east-1")
    end_time = datetime.now(timezone.utc)
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject=f"ETL Job Status: {status}",
        Message=json.dumps({
            "job_name": job_name,
            "track": "emr",
            "status": status,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": int((end_time - start_time).total_seconds()),
            "rows_processed": rows,
            "output_path": output_path,
        }),
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 4:
        print("[ERROR] Usage: emr_cdc_job.py <source_path> <output_path> <checkpoint_path>")
        sys.exit(1)

    source_path = sys.argv[1]
    output_path = sys.argv[2]
    checkpoint_path = sys.argv[3]
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    job_name = "emr-cdc-job"
    start_time = datetime.now(timezone.utc)

    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    s3 = boto3.client("s3", region_name="us-east-1")
    rows_processed = 0

    print(f"[INFO] Job '{job_name}' started.")
    print(f"[INFO] Source path     : {source_path}")
    print(f"[INFO] Output path     : {output_path}")
    print(f"[INFO] Checkpoint path : {checkpoint_path}")

    try:
        # ── 1. Read checkpoint ────────────────────────────────────────────────
        last_ts = read_checkpoint(s3, checkpoint_path)

        # ── 2. Read all source CDC data ───────────────────────────────────────
        df_all = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(source_path)

        print(f"[INFO] Total source rows: {df_all.count()}")

        # ── 3. Filter to only rows newer than the checkpoint ──────────────────
        # String comparison works for ISO 8601 timestamps because they sort
        # lexicographically in chronological order.
        df_new = df_all.filter(col("updated_at") > last_ts)
        rows_processed = df_new.count()
        print(f"[INFO] New rows to process (updated_at > {last_ts}): {rows_processed}")

        if rows_processed == 0:
            print("[INFO] No new data since last run. Exiting.")
            publish_status(sns_topic_arn, job_name, "SUCCEEDED",
                           start_time, 0, output_path)
            spark.stop()
            return

        # ── 4. Load existing SCD history table (or start empty) ───────────────
        try:
            existing = spark.read.parquet(output_path)
            print(f"[INFO] Existing history rows: {existing.count()}")
        except Exception:
            print("[INFO] No existing history table — building from scratch.")
            # Create an empty DataFrame with the expected CDC + SCD columns
            from pyspark.sql.types import StructType, StructField, StringType, BooleanType
            schema = StructType([
                StructField("order_id", StringType()),
                StructField("product_name", StringType()),
                StructField("category", StringType()),
                StructField("quantity", StringType()),
                StructField("unit_price", StringType()),
                StructField("total_price", StringType()),
                StructField("customer_id", StringType()),
                StructField("order_timestamp", StringType()),
                StructField("region", StringType()),
                StructField("operation", StringType()),
                StructField("updated_at", StringType()),
                StructField("valid_from", StringType()),
                StructField("valid_to", StringType()),
                StructField("is_current", BooleanType()),
                StructField("is_deleted", BooleanType()),
            ])
            existing = spark.createDataFrame([], schema)

        # ── 5. Apply SCD Type 2 ───────────────────────────────────────────────
        updated_history = apply_scd_type2(df_new, existing)

        # ── 6. Persist updated history table ─────────────────────────────────
        updated_history.write.mode("overwrite").parquet(output_path)
        total_rows = updated_history.count()
        print(f"[INFO] SCD history written to: {output_path}")
        print(f"[INFO] Total history rows after merge: {total_rows}")

        # ── 7. Advance the checkpoint ─────────────────────────────────────────
        # Use the maximum updated_at from this batch — not wall-clock time —
        # so the checkpoint is tied to the data, not when the job ran.
        new_ts = df_new.agg(spark_max("updated_at")).collect()[0][0]
        write_checkpoint(s3, checkpoint_path, new_ts, rows_processed)

        publish_status(sns_topic_arn, job_name, "SUCCEEDED",
                       start_time, rows_processed, output_path)

    except Exception as e:
        print(f"[ERROR] Job failed: {e}")
        publish_status(sns_topic_arn, job_name, "FAILED",
                       start_time, rows_processed, output_path)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
