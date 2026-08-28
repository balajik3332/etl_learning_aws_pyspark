"""
emr-jobs/multi-step/emr_step3_enrich.py
-----------------------------------------
Stage 3 of 4 — Enrichment with calculated columns.

Reads clean Parquet from Stage 2, adds derived columns, and writes
enriched Parquet partitioned by region, ready for aggregation in Stage 4.

This is the EMR Serverless equivalent of glue-jobs/multi-step/glue_step3_enrich.py.

ENRICHMENTS APPLIED:
  - order_date  : date portion extracted from order_timestamp
  - revenue     : quantity * unit_price
  - price_tier  : 'budget' (< $20) / 'mid' ($20–$99) / 'premium' (>= $100)

ARGUMENTS (positional):
  sys.argv[1]  source_path  — S3 URI to Stage 2 clean Parquet output
  sys.argv[2]  output_path  — S3 URI for enriched Parquet output

PIPELINE FLOW:
  emr_step1_raw  →  emr_step2_clean  →  emr_step3_enrich  →  emr_step4_aggregate
"""

import sys
import os
import json
import boto3
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when


def publish_status(sns_topic_arn: str, job_name: str, status: str,
                   start_time: datetime, rows: int, output_path: str) -> None:
    """Publish stage completion status to SNS (no-op if topic ARN not set)."""
    if not sns_topic_arn:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=sns_topic_arn,
        Subject=f"ETL Pipeline Stage 3 (Enrich): {status}",
        Message=json.dumps({
            "job_name": job_name,
            "track": "emr",
            "stage": "enrich",
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
        print("[ERROR] Usage: emr_step3_enrich.py <source_path> <output_path>")
        sys.exit(1)

    source_path = sys.argv[1]
    output_path = sys.argv[2]
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    job_name = "emr-step3-enrich"
    start_time = datetime.now(timezone.utc)

    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    print(f"[INFO] Stage 3 (Enrich) started.")
    print(f"[INFO] Source path : {source_path}")
    print(f"[INFO] Output path : {output_path}")

    row_count = 0

    try:
        # ── 1. Read clean Parquet from Stage 2 ────────────────────────────────
        print(f"[INFO] Reading clean data from: {source_path}")
        df = spark.read.parquet(source_path)
        row_count = df.count()
        print(f"[INFO] Input rows: {row_count}")

        # ── 2. Add order_date ─────────────────────────────────────────────────
        # to_date() strips the time portion from an ISO 8601 timestamp string,
        # yielding a DateType column. Useful for day-level grouping in Stage 4.
        df = df.withColumn("order_date", to_date(col("order_timestamp")))

        # ── 3. Add revenue ────────────────────────────────────────────────────
        # Recompute from the cleaned, cast values — not from the source
        # total_price which may have been incorrect in the raw data.
        df = df.withColumn("revenue", col("quantity") * col("unit_price"))

        # ── 4. Add price_tier ─────────────────────────────────────────────────
        # Bucketing unit_price into tiers lets Stage 4 aggregate KPIs by
        # market segment without the full cardinality of the price column.
        df = df.withColumn(
            "price_tier",
            when(col("unit_price") < 20.0, "budget")
            .when(col("unit_price") < 100.0, "mid")
            .otherwise("premium"),
        )

        # ── 5. Write enriched Parquet, partitioned by region ──────────────────
        # Partitioning here carries forward to Stage 4: Spark can skip entire
        # partitions when filtering by region.
        df.write.mode("overwrite").partitionBy("region").parquet(output_path)
        print(f"[INFO] Enriched Parquet written to: {output_path}")
        print("[INFO] Sample of enriched data:")
        df.show(5, truncate=False)

        publish_status(sns_topic_arn, job_name, "SUCCEEDED",
                       start_time, row_count, output_path)

    except Exception as e:
        print(f"[ERROR] Stage 3 failed: {e}")
        publish_status(sns_topic_arn, job_name, "FAILED",
                       start_time, 0, output_path)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
