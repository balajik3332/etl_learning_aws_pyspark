"""
glue_step2_clean.py — Stage 2: Data cleaning and quality enforcement

Reads raw Parquet from Stage 1, applies cleaning rules,
and writes clean Parquet ready for enrichment.

Cleaning rules applied:
  - Drop rows where order_id is null
  - Cast unit_price to double, quantity to int
  - Drop rows where quantity <= 0 or unit_price <= 0
  - Trim whitespace from region and product_name

Job arguments:
    --source_path    S3 path to Stage 1 raw Parquet output
    --output_path    S3 path for clean Parquet output
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
from pyspark.sql.functions import col, trim

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


def publish_status(status: str, rows_in: int, rows_out: int) -> None:
    if not SNS_TOPIC_ARN:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"ETL Pipeline Stage 2 (Clean): {status}",
        Message=json.dumps({
            "job_name": args["JOB_NAME"], "stage": "clean",
            "status": status, "rows_in": rows_in, "rows_out": rows_out,
            "rows_dropped": rows_in - rows_out,
            "output_path": OUTPUT_PATH,
        }),
    )


try:
    print(f"[INFO] Reading raw data from: {SOURCE_PATH}")
    df = spark.read.parquet(SOURCE_PATH)
    rows_in = df.count()
    print(f"[INFO] Input rows: {rows_in}")

    # Cast numeric columns (CSV reads everything as string)
    df = df.withColumn("unit_price", col("unit_price").cast("double")) \
           .withColumn("quantity", col("quantity").cast("int"))

    # Drop null order_id
    df = df.filter(col("order_id").isNotNull())

    # Drop invalid numeric values
    df = df.filter(col("quantity").isNotNull() & (col("quantity") > 0))
    df = df.filter(col("unit_price").isNotNull() & (col("unit_price") > 0))

    # Trim string columns
    df = df.withColumn("region", trim(col("region"))) \
           .withColumn("product_name", trim(col("product_name")))

    rows_out = df.count()
    print(f"[INFO] Output rows: {rows_out}  (dropped {rows_in - rows_out} bad rows)")

    df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[INFO] Clean Parquet written to: {OUTPUT_PATH}")

    publish_status("SUCCEEDED", rows_in, rows_out)
    job.commit()

except Exception as e:
    print(f"[ERROR] Stage 2 failed: {e}")
    publish_status("FAILED", 0, 0)
    raise
