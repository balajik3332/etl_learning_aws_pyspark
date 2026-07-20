"""
iam_roles.py — Create the four IAM service roles needed by the ETL teaching project.

Roles created:
  BatchETL-GlueServiceRole      Assumed by AWS Glue jobs
  BatchETL-EMRServerlessRole    Assumed by EMR Serverless job runs
  BatchETL-LambdaTriggerRole    Assumed by the trigger Lambda functions
  BatchETL-LambdaNotifierRole   Assumed by the SQS notifier Lambda function

Usage:
    python infrastructure/iam_roles.py
"""

import json
import boto3
from botocore.exceptions import ClientError


REGION = "us-east-1"

# S3 resource pattern covering all ETL course buckets regardless of prefix used
ETL_S3_RESOURCE = "arn:aws:s3:::etl-course-*"

# ---------------------------------------------------------------------------
# Trust policy helpers
# ---------------------------------------------------------------------------

def _trust_policy(service_principal: str) -> str:
    """Return a JSON string for a single-service trust policy."""
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": service_principal},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )


# ---------------------------------------------------------------------------
# Inline policy documents for each role
# ---------------------------------------------------------------------------

GLUE_INLINE_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadWrite",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                "Resource": [ETL_S3_RESOURCE, f"{ETL_S3_RESOURCE}/*"],
            },
            {
                "Sid": "GlueCatalogCRUD",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "glue:BatchCreatePartition",
                ],
                "Resource": "*",
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }
)

EMR_INLINE_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadWrite",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],
                "Resource": [ETL_S3_RESOURCE, f"{ETL_S3_RESOURCE}/*"],
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }
)

LAMBDA_TRIGGER_INLINE_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "StartETLJobs",
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun",
                    "emr-serverless:StartJobRun",
                ],
                "Resource": "*",
            },
            {
                "Sid": "S3GetObject",
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [ETL_S3_RESOURCE, f"{ETL_S3_RESOURCE}/*"],
            },
            {
                "Sid": "SNSPublish",
                "Effect": "Allow",
                "Action": ["sns:Publish"],
                "Resource": "*",
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }
)

LAMBDA_NOTIFIER_INLINE_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "SQSConsume",
                "Effect": "Allow",
                "Action": [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                ],
                "Resource": "*",
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
        ],
    }
)

# ---------------------------------------------------------------------------
# Role definitions — (role_name, service_principal, inline_policy_name, policy_doc)
# ---------------------------------------------------------------------------

ROLES = [
    (
        "BatchETL-GlueServiceRole",
        "glue.amazonaws.com",
        "BatchETL-GlueInlinePolicy",
        GLUE_INLINE_POLICY,
    ),
    (
        "BatchETL-EMRServerlessRole",
        "emr-serverless.amazonaws.com",
        "BatchETL-EMRInlinePolicy",
        EMR_INLINE_POLICY,
    ),
    (
        "BatchETL-LambdaTriggerRole",
        "lambda.amazonaws.com",
        "BatchETL-LambdaTriggerInlinePolicy",
        LAMBDA_TRIGGER_INLINE_POLICY,
    ),
    (
        "BatchETL-LambdaNotifierRole",
        "lambda.amazonaws.com",
        "BatchETL-LambdaNotifierInlinePolicy",
        LAMBDA_NOTIFIER_INLINE_POLICY,
    ),
]


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def create_role(iam_client, role_name: str, service_principal: str) -> str:
    """Create an IAM role with a trust policy for the given AWS service.

    Returns the role ARN.  If the role already exists, returns its existing ARN.
    """
    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=_trust_policy(service_principal),
            Description=f"ETL teaching project role for {service_principal}",
        )
        arn = response["Role"]["Arn"]
        print(f"  [OK] Created role: {role_name}")
        print(f"       ARN: {arn}")
        return arn
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "EntityAlreadyExists":
            existing = iam_client.get_role(RoleName=role_name)
            arn = existing["Role"]["Arn"]
            print(f"  [SKIP] Role already exists: {role_name}")
            print(f"         ARN: {arn}")
            return arn
        raise


def attach_inline_policy(
    iam_client,
    role_name: str,
    policy_name: str,
    policy_document: str,
) -> None:
    """Attach (or update) an inline policy on an IAM role."""
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=policy_document,
    )
    print(f"  [OK] Inline policy attached: {policy_name} -> {role_name}")


def assert_roles_exist(iam_client, role_names: list) -> None:
    """Verify each role can be retrieved with get_role and print PASS/FAIL."""
    print("\n--- Verification ---")
    all_pass = True
    for name in role_names:
        try:
            iam_client.get_role(RoleName=name)
            print(f"  PASS  role exists: {name}")
        except ClientError:
            print(f"  FAIL  role NOT found: {name}")
            all_pass = False
    if all_pass:
        print("\nAll IAM role assertions PASSED.")
    else:
        print("\nSome IAM role assertions FAILED — review output above.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Create all four ETL course IAM roles and verify them."""
    iam = boto3.client("iam", region_name=REGION)

    print("\nCreating IAM roles for the ETL teaching project...\n")

    for idx, (role_name, service_principal, policy_name, policy_doc) in enumerate(
        ROLES, start=1
    ):
        print(f"[{idx}/{len(ROLES)}] {role_name}")
        create_role(iam, role_name, service_principal)
        attach_inline_policy(iam, role_name, policy_name, policy_doc)
        print()

    assert_roles_exist(iam, [r[0] for r in ROLES])


if __name__ == "__main__":
    main()
