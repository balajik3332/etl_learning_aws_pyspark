"""
emr-jobs/simple-etl/emr_simple_etl.py
---------------------------------------
Pure PySpark ETL script for EMR Serverless — Simple ETL job.

Implements the same transformation logic as the Glue version
(glue-jobs/simple-etl/glue_simple_etl.py) but without any AWS Glue
libraries. Uses only a standard SparkSession, making this script
portable: it runs identically on EMR Serverless, a local Spark
installation, or any other Spark cluster.

HOW TO RUN:
  1. Upload this script to S3:
       aws s3 cp emr-jobs/simple-etl/emr_simple_etl.py \\
           s3://<your-bucket>/scripts/emr_simple_etl.py

  2. Submit to EMR Serverless via CLI:
       APP_ID=$(aws emr-serverless list-applications \\
           --query "applications[?name=='etl-course-spark'].id" \\
           --output text)
       ROLE_ARN=$(aws iam get-role \\
           --role-name BatchETL-EMRServerlessRole \\
           --query "Role.Arn" --output text)

       aws emr-serverless start-job-run \\
           --application-id $APP_ID \\
           --execution-role-arn $ROLE_ARN \\
           --job-driver '{
             "sparkSubmit": {
               "entryPoint": "s3://<your-bucket>/scripts/emr_simple_etl.py",
               "entryPointArguments": [
                 "s3://<landing-bucket>/sales/",
                 "s3://<output-bucket>/emr/sales/"
               ],
               "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g"
             }
           }'

ARGUMENTS (positional, via sys.argv):
  sys.argv[1]  source_path  — S3 URI to input CSV files (prefix or file)
  sys.argv[2]  output_path  — S3 URI where Parquet output is written

INPUT SCHEMA (sales CSV):
  order_id        — UUID string, unique per order
  product_name    — free-text product name
  category        — e.g. Food, Electronics, Sports
  quantity        — number of units ordered (integer stored as string in CSV)
  unit_price      — price per unit (decimal stored as string in CSV)
  total_price     — pre-computed total (ignored; we recompute as "revenue")
  customer_id     — UUID string
  order_timestamp — ISO-8601 datetime string e.g. "2026-05-19T22:45:06Z"
  region          — e.g. us-east, eu-west, ap-southeast

OUTPUT SCHEMA (Parquet, partitioned by region):
  All input columns except total_price, plus:
  unit_price  — DoubleType  (cast from string)
  quantity    — IntegerType (cast from string)
  order_date  — DateType    (extracted from order_timestamp)
  revenue     — DoubleType  (quantity * unit_price)

KEY DIFFERENCES FROM GLUE VERSION:
  - No GlueContext, DynamicFrame, or awsglue.* imports
  - SparkSession.builder instead of SparkContext + GlueContext
  - Arguments via sys.argv instead of getResolvedOptions
  - No job.init() / job.commit() lifecycle calls
  - Reads CSV directly with spark.read instead of create_dynamic_frame
"""

import sys
import os
import json
import boto3
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, IntegerType


def publish_status(sns_topic_arn: str, job_name: str, status: str,
                   start_time: datetime, rows: int, output_path: str) -> None:
    """Publish job completion status to SNS (skipped if topic ARN not set)."""
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


def main() -> None:
    # ── Step 1: Parse arguments ───────────────────────────────────────────────
    # EMR scripts receive arguments as plain positional values in sys.argv.
    # argv[0] is the script name itself; user arguments start at index 1.
    if len(sys.argv) < 3:
        print("[ERROR] Usage: emr_simple_etl.py <source_path> <output_path>")
        sys.exit(1)

    source_path = sys.argv[1]
    output_path = sys.argv[2]
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN", "")
    job_name = "emr-simple-etl"
    start_time = datetime.now(timezone.utc)

    # ── Step 2: Create SparkSession ───────────────────────────────────────────
    # SparkSession is the single entry point for all Spark functionality.
    # On EMR Serverless, AWS manages the underlying SparkContext; we just
    # call builder.getOrCreate() and the runtime wires everything up.
    # appName appears in the EMR Console and CloudWatch logs.
    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    print(f"[INFO] Job '{job_name}' started.")
    print(f"[INFO] Source path : {source_path}")
    print(f"[INFO] Output path : {output_path}")

    rows_written = 0

    try:
        # ── Step 3: Extract — read CSV ────────────────────────────────────────
        # spark.read.option("header", "true") tells Spark the first CSV row
        # contains column names. inferSchema=false (default) reads every
        # column as a string — we handle casting ourselves below.
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .csv(source_path)

        print(f"[INFO] Records read from source: {df.count()}")

        # ── Step 4: Transform ─────────────────────────────────────────────────

        # Drop rows where order_id is null — unusable without a primary key
        df = df.filter(col("order_id").isNotNull())
        print(f"[INFO] Records after null order_id filter: {df.count()}")

        # Cast unit_price to Double so arithmetic operations work correctly
        df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))

        # Cast quantity to Integer for the same reason
        df = df.withColumn("quantity", col("quantity").cast(IntegerType()))

        # Extract the date portion from the ISO timestamp column
        df = df.withColumn("order_date", to_date(col("order_timestamp")))

        # Compute revenue = quantity × unit_price
        # We own this calculation rather than trusting the source total_price
        df = df.withColumn("revenue", col("quantity") * col("unit_price"))

        rows_written = df.count()
        print(f"[INFO] Transformations complete. Final record count: {rows_written}")
        print("[INFO] Sample of transformed data:")
        df.show(5, truncate=False)
        df.printSchema()

        # ── Step 5: Load — write partitioned Parquet ─────────────────────────
        # mode("overwrite") replaces any existing output at the path.
        # partitionBy("region") writes one subfolder per region value:
        #   s3://<output_path>/region=us-east/part-00000.parquet
        # Partitioning by region means downstream queries that filter
        # on region skip irrelevant partitions entirely.
        df.write \
            .mode("overwrite") \
            .partitionBy("region") \
            .parquet(output_path)

        print(f"[INFO] Output written to: {output_path}")
        publish_status(sns_topic_arn, job_name, "SUCCEEDED",
                       start_time, rows_written, output_path)

    except Exception as e:
        print(f"[ERROR] Job failed: {e}")
        publish_status(sns_topic_arn, job_name, "FAILED",
                       start_time, 0, output_path)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
