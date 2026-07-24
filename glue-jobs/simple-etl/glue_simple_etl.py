"""
glue-jobs/simple-etl/glue_simple_etl.py
----------------------------------------
AWS Glue PySpark ETL script for the Simple ETL job.

This script runs on AWS Glue's managed Spark environment. It reads raw
sales CSV data from S3, applies a series of transformations to clean and
enrich the data, then writes the result as Parquet partitioned by region.

HOW TO RUN:
  1. Upload this file to S3:
       aws s3 cp glue-jobs/simple-etl/glue_simple_etl.py \\
           s3://<your-bucket>/scripts/glue_simple_etl.py
  2. Register the job:
       python infrastructure/register_glue_jobs.py --script-bucket <your-bucket>
  3. Start the job from the AWS Console (Glue → Jobs → Run) or via CLI:
       aws glue start-job-run --job-name glue-simple-etl \\
         --arguments '{"--source_path":"s3://.../sales/","--output_path":"s3://.../output/"}'

INPUT SCHEMA (sales CSV):
  order_id        — UUID string, unique per order
  product_name    — free-text product name
  category        — e.g. Food, Electronics, Sports
  quantity        — number of units ordered (integer stored as string in CSV)
  unit_price      — price per unit (decimal stored as string in CSV)
  total_price     — pre-computed total (we recompute this as "revenue")
  customer_id     — UUID string
  order_timestamp — ISO-8601 datetime string e.g. "2026-05-19T22:45:06Z"
  region          — e.g. us-east, eu-west, ap-southeast

OUTPUT SCHEMA (Parquet, partitioned by region):
  All input columns except total_price, plus:
  unit_price      — DoubleType  (cast from string)
  quantity        — IntegerType (cast from string)
  order_date      — DateType    (extracted from order_timestamp)
  revenue         — DoubleType  (quantity * unit_price)
"""

import sys

# ── Glue-specific imports ────────────────────────────────────────────────────
# These libraries are provided automatically on the Glue runtime. They are
# NOT available locally — that is why the unit tests use a plain SparkSession.
from awsglue.transforms import *                    # noqa: F401,F403 — wildcard import expected by Glue
from awsglue.utils import getResolvedOptions        # reads CLI arguments passed to the job
from awsglue.context import GlueContext             # Glue's wrapper around SparkContext
from awsglue.job import Job                         # tracks job state; commit() marks success
from awsglue.dynamicframe import DynamicFrame       # Glue's flexible DataFrame alternative
from pyspark.context import SparkContext

# ── Standard PySpark imports ─────────────────────────────────────────────────
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType, IntegerType


# ── Step 1: Read job arguments ───────────────────────────────────────────────
# getResolvedOptions parses the command-line arguments that Glue passes to the
# script. JOB_NAME is always required (Glue injects it). source_path and
# output_path are custom parameters we defined when registering the job.
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_path", "output_path"]
)

# ── Step 2: Initialize Glue context ─────────────────────────────────────────
# SparkContext is the low-level Spark entry point.
# GlueContext wraps it and adds Glue-specific methods (DynamicFrame I/O, etc.).
# spark_session gives us the standard DataFrame API on top of GlueContext.
# Job tracks the job lifecycle; calling job.init() registers this run with Glue.
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"[INFO] Job '{args['JOB_NAME']}' started.")
print(f"[INFO] Source path : {args['source_path']}")
print(f"[INFO] Output path : {args['output_path']}")


# ── Step 3: Extract — read CSV as a DynamicFrame ─────────────────────────────
# DynamicFrame is Glue's version of a Spark DataFrame. It tolerates messy data:
# missing columns, inconsistent types, extra whitespace — it handles all of
# that without throwing errors. We use it here for the initial read, then
# convert to a regular DataFrame for the transformation step.
#
# from_options parameters:
#   connection_type  — "s3" tells Glue to read from Amazon S3
#   connection_options — paths: a list of S3 URIs to read (can be a prefix)
#   format           — "csv" means comma-separated values
#   format_options   — withHeader:True means the first row contains column names
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["source_path"]]},
    format="csv",
    format_options={"withHeader": "True"}
)

print(f"[INFO] Records read from source: {datasource.count()}")


# ── Step 4: Transform — convert to DataFrame for full PySpark API ─────────────
# toDF() converts the DynamicFrame to a Spark DataFrame. Once we have a
# DataFrame we can use the complete PySpark API: filter(), withColumn(),
# cast(), window functions, joins, etc. DynamicFrames do not support all
# of these operations directly.
df = datasource.toDF()

# ── Transformation 1: Drop rows where order_id is null ───────────────────────
# An order with no order_id is useless — we cannot track, deduplicate, or join
# on it. Any such row is considered corrupt and is removed before further
# processing. isNotNull() returns True for non-null values.
df = df.filter(col("order_id").isNotNull())
print(f"[INFO] Records after null order_id filter: {df.count()}")

# ── Transformation 2: Cast unit_price to DoubleType ──────────────────────────
# CSV files store everything as strings. "40.10" is text, not a number.
# We cast unit_price to Double (64-bit floating point) so we can do arithmetic.
# Rows where the cast fails (e.g., the value is "N/A") will become null.
df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))

# ── Transformation 3: Cast quantity to IntegerType ───────────────────────────
# Same reason as above — "14" is a string in CSV. We cast it to Integer so
# we can multiply it with unit_price to compute revenue.
df = df.withColumn("quantity", col("quantity").cast(IntegerType()))

# ── Transformation 4: Extract order_date from order_timestamp ─────────────────
# order_timestamp looks like "2026-05-19T22:45:06Z". We strip the time portion
# using to_date() and store just the date (2026-05-19) in a new column.
# This makes it easy to group sales by day in later aggregation steps.
df = df.withColumn("order_date", to_date(col("order_timestamp")))

# ── Transformation 5: Compute revenue column ─────────────────────────────────
# The source CSV already has total_price but we recompute it ourselves so
# this pipeline owns the calculation logic and is not dependent on upstream
# values being correct. revenue = quantity × unit_price.
df = df.withColumn("revenue", col("quantity") * col("unit_price"))

print(f"[INFO] Transformations complete. Final record count: {df.count()}")
print("[INFO] Sample of transformed data:")
df.show(5, truncate=False)
df.printSchema()


# ── Step 5: Load — convert back to DynamicFrame and write Parquet ────────────
# fromDF() converts our Spark DataFrame back to a DynamicFrame so we can use
# Glue's write_dynamic_frame API which handles partitioned Parquet writes and
# optional Glue Catalog registration cleanly.
#
# partitionKeys=["region"] tells Glue to write one sub-folder per unique value
# of the region column. The S3 output will look like:
#   s3://<output_path>/region=us-east/part-00000.parquet
#   s3://<output_path>/region=eu-west/part-00000.parquet
# Partitioning by region means downstream queries that filter by region will
# only read the relevant partition — much faster and cheaper at query time.
output_dyf = DynamicFrame.fromDF(df, glueContext, "output")

glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": args["output_path"],
        "partitionKeys": ["region"]
    },
    format="parquet"
)

print(f"[INFO] Output written to: {args['output_path']}")


# ── Step 6: Commit the job ────────────────────────────────────────────────────
# job.commit() tells Glue that this run finished successfully. Glue uses this
# signal to advance job bookmarks (used in CDC jobs) and to mark the run as
# SUCCEEDED in the Glue Console. Always call this at the end of your script.
job.commit()
print("[INFO] Job committed successfully.")
