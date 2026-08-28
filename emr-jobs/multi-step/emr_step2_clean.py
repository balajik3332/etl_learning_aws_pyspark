"""
emr-jobs/multi-step/emr_step2_clean.py
----------------------------------------
Stage 2 of 4 — Data cleaning and quality enforcement.

Reads raw Parquet from Stage 1, applies cleaning rules, and writes
clean Parquet ready for enrichment in Stage 3.

This is the EMR Serverless equivalent of glue-jobs/multi-step/glue_step2_clean.py.

CLEANING RULES APPLIED:
  - Cast unit_price to double, quantity to int
  - Drop rows where order_id is null
  - Drop rows where quantity <= 0 or unit_price <= 0
  - Trim whitespace from region and product_name

ARGUMENTS (positional):
  sys.argv[1]  source_path  — S3 URI to Stage 1 raw Parquet output
  sys.argv[2]  output_path  — S3 URI for clean Parquet output

PIPELINE FLOW:
  emr_step1_raw  →  emr_step2_clean  →  emr_step3_enrich  →  emr_step4_aggregate
"""

import sys
import os
import json
import boto3
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


def publish_status(sns_topic_arn: str, job_name: str, status: str,
                   start_time: datetime, rows_in: int, rows_out: int,
                   output_path: str) -> None:
    """Publish stage completion status to SNS (no-op if topic ARN not set)."""
    if not sns_topic_arn:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=sns_topic_arn,
        Subject=f"ETL Pipeline Stage 2 (Clean): {status}",
        Message=json.dumps({
            "job_name": job_name,
            "track": "emr",
            "stage": "clean",
            "status": status,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_dropped": rows_in - rows_out,
            "output_path": output_path,
            "duration_seconds": int(
                (datetime.now(timezone.utc) - start_time).total_seconds()
            ),
        }),
    )


def main() -> None:
    if len(sys.argv) < 3:
        print("[ERROR] Usage: emr_step2_clean.py <source_path> <output_path>")
        sys.exit(1)

    source_path = sys.argv[1]
    output_path = sys.argv[2]
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    job_name = "emr-step2-clean"
    start_time = datetime.now(timezone.utc)

    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    print(f"[INFO] Stage 2 (Clean) started.")
    print(f"[INFO] Source path : {source_path}")
    print(f"[INFO] Output path : {output_path}")

    rows_in = 0
    rows_out = 0

    try:
        # ── 1. Read raw Parquet from Stage 1 ──────────────────────────────────
        print(f"[INFO] Reading raw data from: {source_path}")
        df = spark.read.parquet(source_path)
        rows_in = df.count()
        print(f"[INFO] Input rows: {rows_in}")

        # ── 2. Cast numeric columns ───────────────────────────────────────────
        # Stage 1 preserved all values as strings. Cast here so the filter
        # comparisons below work correctly (string "0" != integer 0 in Spark).
        df = df.withColumn("unit_price", col("unit_price").cast("double")) \
               .withColumn("quantity", col("quantity").cast("int"))

        # ── 3. Drop rows with null order_id ───────────────────────────────────
        # Orders without a primary key cannot be tracked or joined downstream.
        df = df.filter(col("order_id").isNotNull())

        # ── 4. Drop rows with invalid numeric values ──────────────────────────
        # A quantity or unit_price of zero or less makes no business sense and
        # would corrupt revenue calculations in Stage 3.
        df = df.filter(col("quantity").isNotNull() & (col("quantity") > 0))
        df = df.filter(col("unit_price").isNotNull() & (col("unit_price") > 0))

        # ── 5. Trim whitespace from string columns ────────────────────────────
        # Leading/trailing spaces in region or product_name would cause groupBy
        # in Stage 4 to treat "us-east " and "us-east" as different values.
        df = df.withColumn("region", trim(col("region"))) \
               .withColumn("product_name", trim(col("product_name")))

        rows_out = df.count()
        print(f"[INFO] Output rows: {rows_out}  (dropped {rows_in - rows_out} bad rows)")

        # ── 6. Write clean Parquet ────────────────────────────────────────────
        df.write.mode("overwrite").parquet(output_path)
        print(f"[INFO] Clean Parquet written to: {output_path}")

        publish_status(sns_topic_arn, job_name, "SUCCEEDED",
                       start_time, rows_in, rows_out, output_path)

    except Exception as e:
        print(f"[ERROR] Stage 2 failed: {e}")
        publish_status(sns_topic_arn, job_name, "FAILED",
                       start_time, rows_in, 0, output_path)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
