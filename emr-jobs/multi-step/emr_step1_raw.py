"""
emr-jobs/multi-step/emr_step1_raw.py
--------------------------------------
Stage 1 of 4 — Raw ingestion and schema validation.

Reads source CSV from S3, validates that all required columns are present,
and writes the data as-is to Parquet without any transformations. Raises an
exception on schema failure so the Lambda orchestrator stops the pipeline
and fires an SNS failure alert.

This is the EMR Serverless equivalent of glue-jobs/multi-step/glue_step1_raw.py.
The transformation logic is identical; only the runtime scaffolding differs
(SparkSession instead of GlueContext, sys.argv instead of getResolvedOptions).

ARGUMENTS (positional):
  sys.argv[1]  source_path  — S3 URI to input CSV files
  sys.argv[2]  output_path  — S3 URI for raw Parquet output

PIPELINE FLOW:
  emr_step1_raw  →  emr_step2_clean  →  emr_step3_enrich  →  emr_step4_aggregate
"""

import sys
import os
import json
import boto3
from datetime import datetime, timezone

from pyspark.sql import SparkSession


REQUIRED_COLUMNS = {
    "order_id", "product_name", "category",
    "quantity", "unit_price", "order_timestamp", "region",
}


def publish_status(sns_topic_arn: str, job_name: str, stage: str,
                   status: str, start_time: datetime, rows: int,
                   output_path: str) -> None:
    """Publish stage completion status to SNS (no-op if topic ARN not set)."""
    if not sns_topic_arn:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=sns_topic_arn,
        Subject=f"ETL Pipeline Stage 1 (Raw): {status}",
        Message=json.dumps({
            "job_name": job_name,
            "track": "emr",
            "stage": stage,
            "status": status,
            "rows_processed": rows,
            "output_path": output_path,
            "duration_seconds": int(
                (datetime.now(timezone.utc) - start_time).total_seconds()
            ),
        }),
    )


def main() -> None:
    if len(sys.argv) < 3:
        print("[ERROR] Usage: emr_step1_raw.py <source_path> <output_path>")
        sys.exit(1)

    source_path = sys.argv[1]
    output_path = sys.argv[2]
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    job_name = "emr-step1-raw"
    start_time = datetime.now(timezone.utc)

    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    print(f"[INFO] Stage 1 (Raw) started.")
    print(f"[INFO] Source path : {source_path}")
    print(f"[INFO] Output path : {output_path}")

    row_count = 0

    try:
        # ── 1. Read source CSV ────────────────────────────────────────────────
        # inferSchema=false keeps every column as string — we preserve the raw
        # data exactly as received; type casting happens in Stage 2.
        print(f"[INFO] Reading source data from: {source_path}")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(source_path)

        row_count = df.count()
        print(f"[INFO] Rows read: {row_count}")

        # ── 2. Schema validation — fail fast if required columns are missing ──
        # We check for required columns before writing so a bad file never
        # produces a partial output that later stages might silently consume.
        actual_cols = set(df.columns)
        missing = REQUIRED_COLUMNS - actual_cols
        if missing:
            raise ValueError(
                f"Schema validation FAILED. Missing required columns: {sorted(missing)}. "
                f"Found columns: {sorted(actual_cols)}"
            )
        print(f"[INFO] Schema validation PASSED. Columns found: {sorted(actual_cols)}")

        # ── 3. Write raw Parquet — no transformations ─────────────────────────
        # Preserve the original data exactly. Downstream stages do the cleaning.
        df.write.mode("overwrite").parquet(output_path)
        print(f"[INFO] Raw Parquet written to: {output_path}")

        publish_status(sns_topic_arn, job_name, "raw", "SUCCEEDED",
                       start_time, row_count, output_path)

    except Exception as e:
        print(f"[ERROR] Stage 1 failed: {e}")
        publish_status(sns_topic_arn, job_name, "raw", "FAILED",
                       start_time, 0, output_path)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
