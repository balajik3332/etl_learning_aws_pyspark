"""
sns_sqs_setup.py — Create the SNS topic and SQS queue for ETL job notifications.

Resources created:
  SNS topic : etl-job-notifications
  SQS queue : etl-job-status-queue
  Subscription: SQS queue subscribed to the SNS topic
  Optional: email subscription when --email is provided

Usage:
    python infrastructure/sns_sqs_setup.py
    python infrastructure/sns_sqs_setup.py --email you@example.com
"""

import argparse
import json
import boto3
from botocore.exceptions import ClientError


REGION = "us-east-1"
SNS_TOPIC_NAME = "etl-job-notifications"
SQS_QUEUE_NAME = "etl-job-status-queue"


def get_or_create_sns_topic(sns_client) -> str:
    """Create (or get if already existing) the SNS topic.

    SNS create_topic is idempotent — it returns the existing ARN if the
    topic already exists, so no special error handling is needed.

    Returns the topic ARN.
    """
    response = sns_client.create_topic(Name=SNS_TOPIC_NAME)
    topic_arn = response["TopicArn"]
    print(f"  [OK] SNS topic ready: {SNS_TOPIC_NAME}")
    print(f"       ARN: {topic_arn}")
    return topic_arn


def get_or_create_sqs_queue(sqs_client) -> tuple:
    """Create (or get if already existing) the SQS standard queue.

    Returns a tuple of (queue_url, queue_arn).
    """
    try:
        response = sqs_client.create_queue(
            QueueName=SQS_QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": "60",
                "MessageRetentionPeriod": "86400",  # 1 day
            },
        )
        queue_url = response["QueueUrl"]
        print(f"  [OK] SQS queue ready: {SQS_QUEUE_NAME}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "QueueAlreadyExists":
            queue_url = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
            print(f"  [SKIP] SQS queue already exists: {SQS_QUEUE_NAME}")
        else:
            raise

    print(f"       URL: {queue_url}")

    # Retrieve the queue ARN (needed to build the SQS access policy for SNS)
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]
    return queue_url, queue_arn


def allow_sns_to_send_to_sqs(sqs_client, queue_url: str, queue_arn: str, topic_arn: str) -> None:
    """Set an SQS access policy that allows the SNS topic to send messages.

    Without this policy SNS cannot deliver messages to the SQS queue even
    after the subscription is confirmed.
    """
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowSNSPublishToSQS",
                "Effect": "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnEquals": {"aws:SourceArn": topic_arn}
                },
            }
        ],
    }
    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(policy)},
    )
    print(f"  [OK] SQS access policy updated to allow SNS delivery")


def subscribe_sqs_to_sns(sns_client, topic_arn: str, queue_arn: str) -> str:
    """Subscribe the SQS queue to the SNS topic.

    Returns the subscription ARN.
    """
    response = sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="sqs",
        Endpoint=queue_arn,
        Attributes={"RawMessageDelivery": "false"},
    )
    subscription_arn = response["SubscriptionArn"]
    print(f"  [OK] SQS queue subscribed to SNS topic")
    print(f"       Subscription ARN: {subscription_arn}")
    return subscription_arn


def subscribe_email(sns_client, topic_arn: str, email: str) -> None:
    """Add an email subscription to the SNS topic.

    The subscriber will receive a confirmation email from AWS — they must
    click the link before notifications are delivered.
    """
    sns_client.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=email,
    )
    print(f"  [OK] Email subscription requested for: {email}")
    print(f"       Check your inbox and confirm the subscription link from AWS.")


def assert_resources_exist(
    sns_client, sqs_client, topic_arn: str, queue_url: str
) -> None:
    """Verify topic, queue, and SQS subscription exist and print PASS/FAIL."""
    print("\n--- Verification ---")
    all_pass = True

    # Check SNS topic exists
    try:
        sns_client.get_topic_attributes(TopicArn=topic_arn)
        print(f"  PASS  SNS topic exists: {SNS_TOPIC_NAME}")
    except ClientError:
        print(f"  FAIL  SNS topic NOT found: {SNS_TOPIC_NAME}")
        all_pass = False

    # Check SQS queue exists
    try:
        sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        print(f"  PASS  SQS queue exists: {SQS_QUEUE_NAME}")
    except ClientError:
        print(f"  FAIL  SQS queue NOT found: {SQS_QUEUE_NAME}")
        all_pass = False

    # Check at least one SQS subscription to the topic
    subscriptions = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
    sqs_subs = [
        s for s in subscriptions.get("Subscriptions", []) if s["Protocol"] == "sqs"
    ]
    if sqs_subs:
        print(f"  PASS  SQS subscription exists on topic")
    else:
        print(f"  FAIL  No SQS subscription found on topic")
        all_pass = False

    if all_pass:
        print("\nAll SNS/SQS assertions PASSED.")
    else:
        print("\nSome SNS/SQS assertions FAILED — review output above.")


def main() -> None:
    """Parse CLI arguments, create SNS/SQS resources, and verify."""
    parser = argparse.ArgumentParser(
        description="Create SNS topic and SQS queue for ETL job notifications."
    )
    parser.add_argument(
        "--email",
        default=None,
        help="Email address to subscribe to the SNS topic (optional)",
    )
    args = parser.parse_args()

    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)

    print("\nSetting up SNS topic and SQS queue...\n")

    print("[1/4] Creating SNS topic")
    topic_arn = get_or_create_sns_topic(sns)

    print("\n[2/4] Creating SQS queue")
    queue_url, queue_arn = get_or_create_sqs_queue(sqs)

    print("\n[3/4] Configuring SQS access policy for SNS delivery")
    allow_sns_to_send_to_sqs(sqs, queue_url, queue_arn, topic_arn)
    subscribe_sqs_to_sns(sns, topic_arn, queue_arn)

    if args.email:
        print("\n[4/4] Adding email subscription")
        subscribe_email(sns, topic_arn, args.email)
    else:
        print("\n[4/4] No --email provided, skipping email subscription")

    assert_resources_exist(sns, sqs, topic_arn, queue_url)


if __name__ == "__main__":
    main()
