"""
glue_step3_enrich.py — Stage 3: Enrichment with calculated columns

Reads clean Parquet from Stage 2, adds derived columns, and writes
enriched Parquet ready for aggregation.

Enrichments applied:
  - order_date: date portion of order_timestamp
  - revenue:    quantity * unit_price
  - price_tier: 'budget' / 'mid' / 'premium' based on unit_price

Job arguments:
    --source_path    S3 path to Stage 2 clean Parquet output
    --output_path    S3 path for enriched Parquet output
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
from pyspark.sql.functions import col, to_date, when

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "output_path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = args["source_path"]
OUTPUT_PATH = args["output_path"]
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
start_time = datetime.now(timezone.utc)


def publish_status(status: str, rows: int) -> None:
    if not SNS_TOPIC_ARN:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"ETL Pipeline Stage 3 (Enrich): {status}",
        Message=json.dumps({
            "job_name": args["JOB_NAME"], "stage": "enrich",
            "status": status, "rows_processed": rows,
            "output_path": OUTPUT_PATH,
        }),
    )


try:
    print(f"[INFO] Reading clean data from: {SOURCE_PATH}")
    df = spark.read.parquet(SOURCE_PATH)
    row_count = df.count()
    print(f"[INFO] Input rows: {row_count}")

    # Add order_date: extract date from ISO timestamp string
    df = df.withColumn("order_date", to_date(col("order_timestamp")))

    # Add revenue
    df = df.withColumn("revenue", col("quantity") * col("unit_price"))

    # Add price_tier based on unit_price bands
    df = df.withColumn(
        "price_tier",
        when(col("unit_price") < 20.0, "budget")
        .when(col("unit_price") < 100.0, "mid")
        .otherwise("premium"),
    )

    df.write.mode("overwrite").partitionBy("region").parquet(OUTPUT_PATH)
    print(f"[INFO] Enriched Parquet written to: {OUTPUT_PATH}")

    publish_status("SUCCEEDED", row_count)
    job.commit()

except Exception as e:
    print(f"[ERROR] Stage 3 failed: {e}")
    publish_status("FAILED", 0)
    raise
