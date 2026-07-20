"""
cloudwatch_setup.py — Create CloudWatch billing alarms and Lambda log groups.

Resources created:
  Billing alarms:
    etl-billing-warning   Triggers when estimated charge exceeds $5.00
    etl-billing-critical  Triggers when estimated charge exceeds $9.00
  Log groups (7-day retention):
    /aws/lambda/etl-s3-event-trigger
    /aws/lambda/etl-scheduled-trigger
    /aws/lambda/etl-notifier

  Optional (--add-schedules flag):
    CloudWatch Event Rules for scheduled Lambda invocations (Task 5 extension)

Usage:
    python infrastructure/cloudwatch_setup.py
    python infrastructure/cloudwatch_setup.py --add-schedules
"""

import argparse
import boto3
from botocore.exceptions import ClientError


REGION = "us-east-1"
SNS_TOPIC_NAME = "etl-job-notifications"

# Billing alarm definitions: (name, description, threshold_usd)
BILLING_ALARMS = [
    (
        "etl-billing-warning",
        "ETL course billing warning: estimated charge exceeds $5",
        5.0,
    ),
    (
        "etl-billing-critical",
        "ETL course billing critical: estimated charge exceeds $9",
        9.0,
    ),
]

# Lambda log groups to pre-create with 7-day retention
LAMBDA_LOG_GROUPS = [
    "/aws/lambda/etl-s3-event-trigger",
    "/aws/lambda/etl-scheduled-trigger",
    "/aws/lambda/etl-notifier",
]

# CloudWatch Event schedule rules (only created when --add-schedules is set)
SCHEDULE_RULES = [
    {
        "name": "etl-daily-glue-schedule",
        "description": "Daily trigger for Glue simple-ETL job (midnight UTC)",
        "schedule": "cron(0 0 * * ? *)",
        "input": '{"job_type": "simple-etl", "track": "glue"}',
    },
    {
        "name": "etl-daily-emr-schedule",
        "description": "Daily trigger for EMR simple-ETL job (01:00 UTC)",
        "schedule": "cron(0 1 * * ? *)",
        "input": '{"job_type": "simple-etl", "track": "emr"}',
    },
]


def get_sns_topic_arn(sns_client) -> str:
    """Look up the ARN of the etl-job-notifications SNS topic.

    Billing alarms need the topic ARN so they can send alerts.
    Returns an empty string if the topic has not been created yet;
    the alarms will still be created but without an action.
    """
    paginator = sns_client.get_paginator("list_topics")
    for page in paginator.paginate():
        for topic in page.get("Topics", []):
            if topic["TopicArn"].endswith(f":{SNS_TOPIC_NAME}"):
                return topic["TopicArn"]
    print(
        f"  [WARN] SNS topic '{SNS_TOPIC_NAME}' not found. "
        "Run sns_sqs_setup.py first to enable alarm notifications."
    )
    return ""


def create_billing_alarm(
    cw_client,
    alarm_name: str,
    description: str,
    threshold: float,
    sns_topic_arn: str,
) -> None:
    """Create (or update) a CloudWatch billing alarm for estimated charges.

    The EstimatedCharges metric is only available in us-east-1.
    If the SNS topic ARN is empty the alarm is created without an action.
    """
    alarm_actions = [sns_topic_arn] if sns_topic_arn else []

    cw_client.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=description,
        ActionsEnabled=True,
        AlarmActions=alarm_actions,
        MetricName="EstimatedCharges",
        Namespace="AWS/Billing",
        Statistic="Maximum",
        Dimensions=[{"Name": "Currency", "Value": "USD"}],
        Period=86400,          # 24 hours — billing metrics update daily
        EvaluationPeriods=1,
        Threshold=threshold,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
    )
    print(f"  [OK] Billing alarm created/updated: {alarm_name} (threshold: ${threshold:.2f})")


def create_log_group(logs_client, log_group_name: str, retention_days: int = 7) -> None:
    """Create a CloudWatch log group with the given retention policy.

    If the log group already exists the function skips creation but still
    applies (or updates) the retention policy.
    """
    try:
        logs_client.create_log_group(logGroupName=log_group_name)
        print(f"  [OK] Log group created: {log_group_name}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"  [SKIP] Log group already exists: {log_group_name}")
        else:
            raise

    logs_client.put_retention_policy(
        logGroupName=log_group_name,
        retentionInDays=retention_days,
    )
    print(f"       Retention set to {retention_days} days")


def create_schedule_rules(events_client, cw_client) -> None:
    """Create CloudWatch Event (EventBridge) rules to trigger the scheduled Lambda.

    This is only called when the --add-schedules flag is passed (Task 5).
    """
    print("\n[Extra] Creating CloudWatch schedule rules...")
    for rule in SCHEDULE_RULES:
        events_client.put_rule(
            Name=rule["name"],
            ScheduleExpression=rule["schedule"],
            State="DISABLED",   # Start disabled so students enable them deliberately
            Description=rule["description"],
        )
        print(f"  [OK] Schedule rule created (DISABLED): {rule['name']}")
        print(f"       Schedule: {rule['schedule']}")
        print(f"       Tip: Enable with: aws events enable-rule --name {rule['name']}")


def assert_alarms_exist(cw_client, alarm_names: list) -> None:
    """Verify each billing alarm can be retrieved via describe_alarms."""
    print("\n--- Verification ---")
    response = cw_client.describe_alarms(AlarmNames=alarm_names)
    existing = {a["AlarmName"] for a in response.get("MetricAlarms", [])}
    all_pass = True
    for name in alarm_names:
        if name in existing:
            print(f"  PASS  alarm exists: {name}")
        else:
            print(f"  FAIL  alarm NOT found: {name}")
            all_pass = False
    if all_pass:
        print("\nAll CloudWatch alarm assertions PASSED.")
    else:
        print("\nSome CloudWatch alarm assertions FAILED — review output above.")


def main() -> None:
    """Parse CLI arguments, create CloudWatch resources, and verify."""
    parser = argparse.ArgumentParser(
        description="Create CloudWatch billing alarms and Lambda log groups."
    )
    parser.add_argument(
        "--add-schedules",
        action="store_true",
        help="Also create CloudWatch Event schedule rules (Task 5 extension)",
    )
    args = parser.parse_args()

    cw = boto3.client("cloudwatch", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)

    print("\nSetting up CloudWatch billing alarms and log groups...\n")

    # Resolve SNS topic ARN for alarm actions
    sns_topic_arn = get_sns_topic_arn(sns)

    # Create billing alarms
    print("[1/2] Creating billing alarms")
    for alarm_name, description, threshold in BILLING_ALARMS:
        create_billing_alarm(cw, alarm_name, description, threshold, sns_topic_arn)

    # Create Lambda log groups with 7-day retention
    print(f"\n[2/2] Creating Lambda log groups (7-day retention)")
    for log_group in LAMBDA_LOG_GROUPS:
        create_log_group(logs, log_group, retention_days=7)

    # Optionally create schedule rules (Task 5 extension)
    if args.add_schedules:
        events = boto3.client("events", region_name=REGION)
        create_schedule_rules(events, cw)

    # Post-creation assertions for alarms
    assert_alarms_exist(cw, [a[0] for a in BILLING_ALARMS])


if __name__ == "__main__":
    main()
