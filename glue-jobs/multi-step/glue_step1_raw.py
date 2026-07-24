"""
glue_step1_raw.py — Stage 1: Raw ingestion and schema validation

Reads source CSV from S3, validates that all required columns are present,
and writes the data as-is to Parquet. Raises an exception on schema failure
so the Glue Workflow stops and triggers an SNS alert.

Job arguments:
    --source_path    S3 path to input CSV files
    --output_path    S3 path for raw Parquet output
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

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------
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

REQUIRED_COLUMNS = {
    "order_id", "product_name", "category",
    "quantity", "unit_price", "order_timestamp", "region",
}


def publish_status(status: str, rows: int) -> None:
    if not SNS_TOPIC_ARN:
        return
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f"ETL Pipeline Stage 1 (Raw): {status}",
        Message=json.dumps({
            "job_name": args["JOB_NAME"], "stage": "raw",
            "status": status, "rows_processed": rows,
            "output_path": OUTPUT_PATH,
            "duration_seconds": int((datetime.now(timezone.utc) - start_time).total_seconds()),
        }),
    )


try:
    # 1. Read source CSV
    print(f"[INFO] Reading source data from: {SOURCE_PATH}")
    dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [SOURCE_PATH], "recurse": True},
        format="csv",
        format_options={"withHeader": True},
    )
    df = dyf.toDF()
    row_count = df.count()
    print(f"[INFO] Rows read: {row_count}")

    # 2. Schema validation — fail fast if required columns are missing
    actual_cols = set(df.columns)
    missing = REQUIRED_COLUMNS - actual_cols
    if missing:
        raise ValueError(
            f"Schema validation FAILED. Missing required columns: {missing}. "
            f"Found: {actual_cols}"
        )
    print(f"[INFO] Schema validation PASSED. Columns: {sorted(actual_cols)}")

    # 3. Write raw Parquet — no transformations, preserve original data
    df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"[INFO] Raw Parquet written to: {OUTPUT_PATH}")

    publish_status("SUCCEEDED", row_count)
    job.commit()

except Exception as e:
    print(f"[ERROR] Stage 1 failed: {e}")
    publish_status("FAILED", 0)
    raise
