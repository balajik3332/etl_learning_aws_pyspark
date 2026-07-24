"""
upload_to_s3.py — Upload generated data files to an S3 landing bucket.

Folder mapping:
    sales_*.csv          → s3://{bucket}/sales/
    users_*.json         → s3://{bucket}/users/
    transactions_*.parquet → s3://{bucket}/transactions/
    cdc_*.csv            → s3://{bucket}/cdc/

Usage:
    python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing
    python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing --output-dir data-generator/output
"""

import argparse
import fnmatch
import os

import boto3


def get_s3_prefix(filename: str) -> str | None:
    """
    Determine the S3 prefix folder for a given filename.

    Returns the prefix string (e.g. 'sales/') or None if the file
    does not match any known pattern and should be skipped.
    """
    if fnmatch.fnmatch(filename, "sales_*.csv"):
        return "sales/"
    if fnmatch.fnmatch(filename, "users_*.json"):
        return "users/"
    if fnmatch.fnmatch(filename, "transactions_*.parquet"):
        return "transactions/"
    if fnmatch.fnmatch(filename, "cdc_*.csv"):
        return "cdc/"
    return None


def upload_files(bucket: str, output_dir: str) -> list[str]:
    """
    Upload all matching files from output_dir to the given S3 bucket.

    Args:
        bucket: Name of the S3 bucket (landing bucket).
        output_dir: Local directory containing generated files.

    Returns:
        List of S3 URIs that were uploaded.
    """
    s3 = boto3.client("s3")
    uploaded = []

    if not os.path.isdir(output_dir):
        print(f"Output directory not found: {output_dir}")
        return uploaded

    for filename in sorted(os.listdir(output_dir)):
        prefix = get_s3_prefix(filename)
        if prefix is None:
            continue  # skip files that don't match any pattern

        local_path = os.path.join(output_dir, filename)
        s3_key = f"{prefix}{filename}"
        s3_uri = f"s3://{bucket}/{s3_key}"

        s3.upload_file(local_path, bucket, s3_key)
        print(f"Uploaded → {s3_uri}")
        uploaded.append(s3_uri)

    if not uploaded:
        print("No matching files found to upload.")

    return uploaded


def main():
    parser = argparse.ArgumentParser(description="Upload generated data files to S3.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name (landing bucket)")
    parser.add_argument(
        "--output-dir",
        default="data-generator/output",
        help="Local directory containing generated files (default: data-generator/output)",
    )
    args = parser.parse_args()

    uploaded = upload_files(args.bucket, args.output_dir)
    print(f"\nDone. Uploaded {len(uploaded)} file(s) to s3://{args.bucket}/")


if __name__ == "__main__":
    main()
