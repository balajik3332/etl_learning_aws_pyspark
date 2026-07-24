"""
glue_step4_aggregate.py — Stage 4: Aggregation to business KPIs

Reads enriched Parquet from Stage 3 and produces a summary table
with key performance indicators grouped by region and order_date.

Aggregations:
  - total_orders:     count of distinct order_ids
  - total_revenue:    sum of revenue
  - avg_order_value:  mean revenue per order
  - avg_unit_price:   mean unit_price

Job arguments:
    --source_path    S3 path to Stage 3 enriched Parquet output
    --output_path    S3 path for aggregate Parquet output
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
from pyspark.sql.functions import col, count, sum as spark_sum, avg, round as spark_round

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


def publish_status(status: str, input_rows: int, output_rows: int) -> None:
    if not SNS_TOPIC_ARN:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"ETL Pipeline Stage 4 (Aggregate): {status}",
        Message=json.dumps({
            "job_name": args["JOB_NAME"], "stage": "aggregate",
            "status": status, "input_rows": input_rows, "output_rows": output_rows,
            "output_path": OUTPUT_PATH,
            "duration_seconds": int((datetime.now(timezone.utc) - start_time).total_seconds()),
        }),
    )


try:
    print(f"[INFO] Reading enriched data from: {SOURCE_PATH}")
    df = spark.read.parquet(SOURCE_PATH)
    input_rows = df.count()
    print(f"[INFO] Input rows: {input_rows}")

    # Aggregate KPIs by region and order_date
    summary = df.groupBy("region", "order_date", "price_tier").agg(
        count("order_id").alias("total_orders"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("revenue"), 2).alias("avg_order_value"),
        spark_round(avg("unit_price"), 2).alias("avg_unit_price"),
    )

    output_rows = summary.count()
    print(f"[INFO] Aggregate output rows: {output_rows}")

    summary.write.mode("overwrite").partitionBy("region").parquet(OUTPUT_PATH)
    print(f"[INFO] Aggregate Parquet written to: {OUTPUT_PATH}")

    publish_status("SUCCEEDED", input_rows, output_rows)
    job.commit()

except Exception as e:
    print(f"[ERROR] Stage 4 failed: {e}")
    publish_status("FAILED", 0, 0)
    raise
