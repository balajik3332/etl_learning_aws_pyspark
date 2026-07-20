"""
s3_setup.py — Create the three S3 buckets needed for the ETL teaching project.

Buckets created:
  {prefix}-landing    Raw input data; lifecycle rule expires objects after 7 days
  {prefix}-processed  Intermediate data between pipeline stages
  {prefix}-output     Final Parquet output; versioning enabled

Usage:
    python infrastructure/s3_setup.py --prefix etl-course
"""

import argparse
import json
import boto3
from botocore.exceptions import ClientError


REGION = "us-east-1"


def create_bucket(s3_client, bucket_name: str) -> bool:
    """Create an S3 bucket in us-east-1 and block all public access.

    Returns True if the bucket was created or already exists,
    False if an unexpected error occurred.
    """
    try:
        # us-east-1 does not accept a LocationConstraint — other regions do
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"  [OK] Created bucket: {bucket_name}")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            print(f"  [SKIP] Bucket already exists: {bucket_name}")
        else:
            print(f"  [ERROR] Could not create bucket {bucket_name}: {exc}")
            return False

    # Block all public access regardless of whether the bucket is new
    s3_client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    print(f"  [OK] Public access blocked on: {bucket_name}")
    return True


def add_lifecycle_policy(s3_client, bucket_name: str) -> None:
    """Add a lifecycle policy that expires all objects after 7 days.

    This keeps the landing bucket lean and prevents storage costs from
    accumulating as raw input files pile up.
    """
    lifecycle_config = {
        "Rules": [
            {
                "ID": "expire-landing-objects-7days",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "Expiration": {"Days": 7},
            }
        ]
    }
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle_config,
    )
    print(f"  [OK] Lifecycle policy (expire after 7 days) applied to: {bucket_name}")


def enable_versioning(s3_client, bucket_name: str) -> None:
    """Enable versioning on the given S3 bucket.

    Versioning on the output bucket protects final Parquet files from
    accidental overwrites or deletions.
    """
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"},
    )
    print(f"  [OK] Versioning enabled on: {bucket_name}")


def assert_buckets_exist(s3_client, bucket_names: list) -> None:
    """Verify all expected buckets appear in list_buckets and print PASS/FAIL."""
    print("\n--- Verification ---")
    response = s3_client.list_buckets()
    existing = {b["Name"] for b in response.get("Buckets", [])}
    all_pass = True
    for name in bucket_names:
        if name in existing:
            print(f"  PASS  bucket exists: {name}")
        else:
            print(f"  FAIL  bucket NOT found: {name}")
            all_pass = False
    if all_pass:
        print("\nAll bucket assertions PASSED.")
    else:
        print("\nSome bucket assertions FAILED — review output above.")


def main() -> None:
    """Parse CLI arguments, create buckets, and run assertions."""
    parser = argparse.ArgumentParser(
        description="Create ETL course S3 buckets with lifecycle and versioning."
    )
    parser.add_argument(
        "--prefix",
        default="etl-course",
        help="Prefix for bucket names (default: etl-course)",
    )
    args = parser.parse_args()

    prefix = args.prefix
    landing_bucket = f"{prefix}-landing"
    processed_bucket = f"{prefix}-processed"
    output_bucket = f"{prefix}-output"
    all_buckets = [landing_bucket, processed_bucket, output_bucket]

    s3 = boto3.client("s3", region_name=REGION)

    print(f"\nCreating S3 buckets with prefix '{prefix}' in {REGION}...\n")

    # Landing bucket — lifecycle rule to expire after 7 days
    print(f"[1/3] Landing bucket: {landing_bucket}")
    if create_bucket(s3, landing_bucket):
        add_lifecycle_policy(s3, landing_bucket)

    # Processed bucket — no special policy needed
    print(f"\n[2/3] Processed bucket: {processed_bucket}")
    create_bucket(s3, processed_bucket)

    # Output bucket — versioning enabled to protect final output
    print(f"\n[3/3] Output bucket: {output_bucket}")
    if create_bucket(s3, output_bucket):
        enable_versioning(s3, output_bucket)

    # Post-creation assertions
    assert_buckets_exist(s3, all_buckets)


if __name__ == "__main__":
    main()
