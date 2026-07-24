"""
infrastructure/register_glue_jobs.py
--------------------------------------
boto3 script to register the Glue Simple ETL job in your AWS account.

This script does exactly what you would do in the AWS Console under
Glue → Jobs → Create job, but automated so you can recreate the job
quickly if you ever delete it or need to update its settings.

USAGE:
    python infrastructure/register_glue_jobs.py --script-bucket <bucket-name>

    Where <bucket-name> is the S3 bucket where you uploaded the script.
    For example:
        aws s3 cp glue-jobs/simple-etl/glue_simple_etl.py \\
            s3://etl-course-yourname-output/scripts/glue_simple_etl.py

        python infrastructure/register_glue_jobs.py \\
            --script-bucket etl-course-yourname-output

PREREQUISITES:
    - Task 1 infrastructure must be created (IAM role BatchETL-GlueServiceRole)
    - Script uploaded to S3 (see Step 1 in docs/03a-glue-simple-etl-guide.md)
    - AWS credentials configured: run `aws configure` if not done yet

WHAT THIS SCRIPT CREATES:
    - A Glue job named "glue-simple-etl"
    - Configured with 2 workers of type G.1X (the smallest Glue worker)
    - Maximum concurrency of 1 (only one run at a time)
    - Default arguments for source_path and output_path (update these before running)
"""

import argparse
import json
import sys

import boto3
from botocore.exceptions import ClientError

# ── Constants ────────────────────────────────────────────────────────────────

JOB_NAME = "glue-simple-etl"
GLUE_ROLE_NAME = "BatchETL-GlueServiceRole"
REGION = "us-east-1"

# The script S3 key — relative to the bucket root
SCRIPT_S3_KEY = "scripts/glue_simple_etl.py"

# Worker configuration — G.1X is the smallest Glue worker type.
# It has 4 vCPU and 16 GB RAM. 2 workers = 1 driver + 1 executor.
# This is the minimum that makes sense for a Glue job; it keeps cost low.
WORKER_TYPE = "G.1X"
NUMBER_OF_WORKERS = 2

# Max concurrent runs — set to 1 so the same job cannot run twice at the
# same time (which would cause output corruption if writing to the same path).
MAX_CONCURRENT_RUNS = 1


# ── Helper: build the script S3 path ────────────────────────────────────────

def build_script_location(script_bucket: str) -> str:
    """Return the full S3 URI for the uploaded Glue script."""
    return f"s3://{script_bucket}/{SCRIPT_S3_KEY}"


# ── Helper: get the IAM role ARN ─────────────────────────────────────────────

def get_role_arn(iam_client, role_name: str) -> str:
    """
    Look up an IAM role by name and return its ARN.
    Raises a RuntimeError with a helpful message if the role does not exist.
    """
    try:
        response = iam_client.get_role(RoleName=role_name)
        return response["Role"]["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            raise RuntimeError(
                f"IAM role '{role_name}' not found in account. "
                "Make sure you have run infrastructure/iam_roles.py first."
            ) from e
        raise


# ── Main: create or update the Glue job ──────────────────────────────────────

def register_glue_job(script_bucket: str) -> dict:
    """
    Create the Glue simple-etl job. If the job already exists it is deleted
    and recreated with the latest settings so this script is idempotent.

    Returns:
        dict with keys 'job_name' and 'role_arn' for confirmation logging.
    """
    glue_client = boto3.client("glue", region_name=REGION)
    iam_client = boto3.client("iam", region_name=REGION)

    # ── Resolve IAM role ARN ──────────────────────────────────────────────
    print(f"[INFO] Resolving IAM role: {GLUE_ROLE_NAME}")
    role_arn = get_role_arn(iam_client, GLUE_ROLE_NAME)
    print(f"[INFO] Role ARN: {role_arn}")

    # ── Build script S3 location ──────────────────────────────────────────
    script_location = build_script_location(script_bucket)
    print(f"[INFO] Script location: {script_location}")

    # ── Delete existing job if present (idempotent recreate) ─────────────
    # Glue's create_job API raises AlreadyExistsException if the job name
    # is taken, so we delete first and then create fresh.
    try:
        glue_client.delete_job(JobName=JOB_NAME)
        print(f"[INFO] Existing job '{JOB_NAME}' deleted (will be recreated).")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"[INFO] No existing job found — creating new job '{JOB_NAME}'.")
        else:
            raise

    # ── Create the Glue job ───────────────────────────────────────────────
    # Command block:
    #   Name        — must be "glueetl" for a Spark (PySpark) job
    #   ScriptLocation — S3 path to your Python script
    #   PythonVersion — "3" for Python 3 (required for modern PySpark features)
    #
    # DefaultArguments:
    #   --job-bookmark-option  — disabled for simple ETL (enabled in CDC jobs)
    #   --source_path          — placeholder; override at job-run time
    #   --output_path          — placeholder; override at job-run time
    #   --enable-metrics       — enables Glue job metrics in CloudWatch
    response = glue_client.create_job(
        Name=JOB_NAME,
        Description=(
            "Simple ETL job that reads sales CSV from S3, "
            "applies type casting and enrichment transforms, "
            "then writes Parquet output partitioned by region."
        ),
        Role=role_arn,
        Command={
            "Name": "glueetl",
            "ScriptLocation": script_location,
            "PythonVersion": "3",
        },
        DefaultArguments={
            "--job-bookmark-option": "job-bookmark-disable",
            "--source_path": "s3://REPLACE-WITH-YOUR-LANDING-BUCKET/sales/",
            "--output_path": "s3://REPLACE-WITH-YOUR-OUTPUT-BUCKET/glue/sales/",
            "--enable-metrics": "",
            "--TempDir": f"s3://{script_bucket}/tmp/",
        },
        GlueVersion="4.0",
        WorkerType=WORKER_TYPE,
        NumberOfWorkers=NUMBER_OF_WORKERS,
        MaxConcurrentRuns=MAX_CONCURRENT_RUNS,
        Timeout=60,  # minutes — job will be killed if it runs longer than this
        Tags={
            "Project": "batch-etl-teaching",
            "Task": "3A",
        },
    )

    job_name = response["Name"]
    print(f"\n[SUCCESS] Glue job created successfully!")
    print(f"  Job name : {job_name}")
    print(f"  Role ARN : {role_arn}")
    print(f"  Script   : {script_location}")
    print(f"  Workers  : {NUMBER_OF_WORKERS} x {WORKER_TYPE}")
    print(f"\nNext steps:")
    print(f"  1. Update --source_path and --output_path arguments in the Glue Console")
    print(f"     (Glue → Jobs → {job_name} → Job details → Job parameters)")
    print(f"  2. Run the job:")
    print(f"     aws glue start-job-run --job-name {job_name} \\")
    print(f'       --arguments \'{{"--source_path":"s3://YOUR-BUCKET/sales/","--output_path":"s3://YOUR-BUCKET/output/"}}\'')

    return {"job_name": job_name, "role_arn": role_arn}


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Register the glue-simple-etl Glue job in AWS."
    )
    parser.add_argument(
        "--script-bucket",
        required=True,
        help=(
            "Name of the S3 bucket where glue_simple_etl.py was uploaded. "
            "Example: etl-course-yourname-output"
        ),
    )
    args = parser.parse_args()

    try:
        register_glue_job(script_bucket=args.script_bucket)
    except RuntimeError as e:
        print(f"\n[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    except ClientError as e:
        print(f"\n[AWS ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
