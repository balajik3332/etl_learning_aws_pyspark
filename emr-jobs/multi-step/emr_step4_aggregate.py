"""
emr-jobs/multi-step/emr_step4_aggregate.py
--------------------------------------------
Stage 4 of 4 — Aggregation to business KPIs.

Reads enriched Parquet from Stage 3 and produces a summary table with
key performance indicators grouped by region, order_date, and price_tier.

This is the EMR Serverless equivalent of glue-jobs/multi-step/glue_step4_aggregate.py.

AGGREGATIONS:
  - total_orders     : count of order_id rows per group
  - total_revenue    : sum of revenue, rounded to 2 decimal places
  - avg_order_value  : mean revenue per order, rounded to 2 decimal places
  - avg_unit_price   : mean unit_price, rounded to 2 decimal places

ARGUMENTS (positional):
  sys.argv[1]  source_path  — S3 URI to Stage 3 enriched Parquet output
  sys.argv[2]  output_path  — S3 URI for aggregate Parquet output

PIPELINE FLOW:
  emr_step1_raw  →  emr_step2_clean  →  emr_step3_enrich  →  emr_step4_aggregate
"""

import sys
import os
import json
import boto3
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round
)


def publish_status(sns_topic_arn: str, job_name: str, status: str,
                   start_time: datetime, input_rows: int, output_rows: int,
                   output_path: str) -> None:
    """Publish stage completion status to SNS (no-op if topic ARN not set)."""
    if not sns_topic_arn:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=sns_topic_arn,
        Subject=f"ETL Pipeline Stage 4 (Aggregate): {status}",
        Message=json.dumps({
            "job_name": job_name,
            "track": "emr",
            "stage": "aggregate",
            "status": status,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "output_path": output_path,
            "duration_seconds": int(
                (datetime.now(timezone.utc) - start_time).total_seconds()
            ),
        }),
    )


def main() -> None:
    if len(sys.argv) < 3:
        print("[ERROR] Usage: emr_step4_aggregate.py <source_path> <output_path>")
        sys.exit(1)

    source_path = sys.argv[1]
    output_path = sys.argv[2]
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    job_name = "emr-step4-aggregate"
    start_time = datetime.now(timezone.utc)

    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    print(f"[INFO] Stage 4 (Aggregate) started.")
    print(f"[INFO] Source path : {source_path}")
    print(f"[INFO] Output path : {output_path}")

    input_rows = 0
    output_rows = 0

    try:
        # ── 1. Read enriched Parquet from Stage 3 ─────────────────────────────
        print(f"[INFO] Reading enriched data from: {source_path}")
        df = spark.read.parquet(source_path)
        input_rows = df.count()
        print(f"[INFO] Input rows: {input_rows}")

        # ── 2. Aggregate KPIs ─────────────────────────────────────────────────
        # Group by the three business dimensions: region (geography),
        # order_date (time), and price_tier (market segment).
        # This produces one summary row per unique combination of these three.
        summary = df.groupBy("region", "order_date", "price_tier").agg(
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
            spark_round(avg("revenue"), 2).alias("avg_order_value"),
            spark_round(avg("unit_price"), 2).alias("avg_unit_price"),
        )

        output_rows = summary.count()
        print(f"[INFO] Aggregate output rows: {output_rows}")
        print("[INFO] Sample of aggregate output:")
        summary.show(10, truncate=False)

        # ── 3. Write aggregate Parquet, partitioned by region ─────────────────
        # Final output is small (one row per region/date/tier group) so a single
        # partition per region is sufficient. Downstream BI tools can read this
        # directly with Athena or Redshift Spectrum.
        summary.write.mode("overwrite").partitionBy("region").parquet(output_path)
        print(f"[INFO] Aggregate Parquet written to: {output_path}")

        publish_status(sns_topic_arn, job_name, "SUCCEEDED",
                       start_time, input_rows, output_rows, output_path)

    except Exception as e:
        print(f"[ERROR] Stage 4 failed: {e}")
        publish_status(sns_topic_arn, job_name, "FAILED",
                       start_time, input_rows, 0, output_path)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
