"""
tests/test_infrastructure.py — Unit tests for the four infrastructure scripts.

All AWS calls are mocked with moto so no real AWS credentials are needed and
no actual resources are created during the test run.

Run with:
    pytest tests/test_infrastructure.py -v
"""

import json
import sys
import os
import pytest

# Make sure the project root is on the path so we can import the scripts
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import boto3
from moto import mock_aws


# ---------------------------------------------------------------------------
# Helper — boto3 client factory inside a moto context
# ---------------------------------------------------------------------------

REGION = "us-east-1"


def _s3():
    return boto3.client("s3", region_name=REGION)


def _iam():
    return boto3.client("iam", region_name=REGION)


def _sns():
    return boto3.client("sns", region_name=REGION)


def _sqs():
    return boto3.client("sqs", region_name=REGION)


def _cloudwatch():
    return boto3.client("cloudwatch", region_name=REGION)


def _logs():
    return boto3.client("logs", region_name=REGION)


# ===========================================================================
# s3_setup tests
# ===========================================================================

class TestS3Setup:
    """Tests for infrastructure/s3_setup.py"""

    @mock_aws
    def test_creates_three_buckets(self):
        """Running the setup functions must produce exactly three buckets."""
        from infrastructure.s3_setup import create_bucket, add_lifecycle_policy, enable_versioning

        s3 = _s3()
        prefix = "test-etl"
        buckets = [f"{prefix}-landing", f"{prefix}-processed", f"{prefix}-output"]

        for b in buckets:
            create_bucket(s3, b)

        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
        for b in buckets:
            assert b in existing, f"Expected bucket {b} to exist"

    @mock_aws
    def test_landing_bucket_has_lifecycle_policy(self):
        """The landing bucket must have a lifecycle rule expiring objects after 7 days."""
        from infrastructure.s3_setup import create_bucket, add_lifecycle_policy

        s3 = _s3()
        bucket_name = "test-etl-landing"
        create_bucket(s3, bucket_name)
        add_lifecycle_policy(s3, bucket_name)

        lc = s3.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        rules = lc["Rules"]
        assert len(rules) >= 1, "Expected at least one lifecycle rule"
        expiration_days = rules[0]["Expiration"]["Days"]
        assert expiration_days == 7, f"Expected 7-day expiration, got {expiration_days}"

    @mock_aws
    def test_public_access_blocked(self):
        """All buckets must have public access blocked after creation."""
        from infrastructure.s3_setup import create_bucket

        s3 = _s3()
        bucket_name = "test-etl-processed"
        create_bucket(s3, bucket_name)

        config = s3.get_public_access_block(Bucket=bucket_name)
        block = config["PublicAccessBlockConfiguration"]
        assert block["BlockPublicAcls"] is True
        assert block["IgnorePublicAcls"] is True
        assert block["BlockPublicPolicy"] is True
        assert block["RestrictPublicBuckets"] is True

    @mock_aws
    def test_output_bucket_has_versioning(self):
        """The output bucket must have versioning enabled."""
        from infrastructure.s3_setup import create_bucket, enable_versioning

        s3 = _s3()
        bucket_name = "test-etl-output"
        create_bucket(s3, bucket_name)
        enable_versioning(s3, bucket_name)

        versioning = s3.get_bucket_versioning(Bucket=bucket_name)
        assert versioning.get("Status") == "Enabled", "Versioning should be Enabled"

    @mock_aws
    def test_bucket_already_exists_is_handled_gracefully(self):
        """Creating a bucket that already exists must not raise an exception."""
        from infrastructure.s3_setup import create_bucket

        s3 = _s3()
        bucket_name = "test-etl-landing"
        # First creation — should succeed
        result_first = create_bucket(s3, bucket_name)
        assert result_first is True

        # Second creation — should return True (gracefully skipped)
        result_second = create_bucket(s3, bucket_name)
        assert result_second is True


# ===========================================================================
# iam_roles tests
# ===========================================================================

class TestIAMRoles:
    """Tests for infrastructure/iam_roles.py"""

    @mock_aws
    def test_creates_four_roles(self):
        """All four IAM roles must be created."""
        from infrastructure.iam_roles import ROLES, create_role, attach_inline_policy

        iam = _iam()
        for role_name, service_principal, policy_name, policy_doc in ROLES:
            create_role(iam, role_name, service_principal)

        for role_name, *_ in ROLES:
            response = iam.get_role(RoleName=role_name)
            assert response["Role"]["RoleName"] == role_name

    @mock_aws
    def test_glue_role_trusts_glue_service(self):
        """BatchETL-GlueServiceRole must have glue.amazonaws.com as trusted entity."""
        from infrastructure.iam_roles import create_role

        iam = _iam()
        create_role(iam, "BatchETL-GlueServiceRole", "glue.amazonaws.com")

        role = iam.get_role(RoleName="BatchETL-GlueServiceRole")
        trust = role["Role"]["AssumeRolePolicyDocument"]
        principals = [
            stmt["Principal"]["Service"]
            for stmt in trust["Statement"]
        ]
        assert "glue.amazonaws.com" in principals

    @mock_aws
    def test_emr_role_trusts_emr_serverless(self):
        """BatchETL-EMRServerlessRole must trust emr-serverless.amazonaws.com."""
        from infrastructure.iam_roles import create_role

        iam = _iam()
        create_role(iam, "BatchETL-EMRServerlessRole", "emr-serverless.amazonaws.com")

        role = iam.get_role(RoleName="BatchETL-EMRServerlessRole")
        trust = role["Role"]["AssumeRolePolicyDocument"]
        principals = [
            stmt["Principal"]["Service"]
            for stmt in trust["Statement"]
        ]
        assert "emr-serverless.amazonaws.com" in principals

    @mock_aws
    def test_lambda_trigger_role_trusts_lambda(self):
        """BatchETL-LambdaTriggerRole must trust lambda.amazonaws.com."""
        from infrastructure.iam_roles import create_role

        iam = _iam()
        create_role(iam, "BatchETL-LambdaTriggerRole", "lambda.amazonaws.com")

        role = iam.get_role(RoleName="BatchETL-LambdaTriggerRole")
        trust = role["Role"]["AssumeRolePolicyDocument"]
        principals = [
            stmt["Principal"]["Service"]
            for stmt in trust["Statement"]
        ]
        assert "lambda.amazonaws.com" in principals

    @mock_aws
    def test_lambda_notifier_role_trusts_lambda(self):
        """BatchETL-LambdaNotifierRole must trust lambda.amazonaws.com."""
        from infrastructure.iam_roles import create_role

        iam = _iam()
        create_role(iam, "BatchETL-LambdaNotifierRole", "lambda.amazonaws.com")

        role = iam.get_role(RoleName="BatchETL-LambdaNotifierRole")
        trust = role["Role"]["AssumeRolePolicyDocument"]
        principals = [
            stmt["Principal"]["Service"]
            for stmt in trust["Statement"]
        ]
        assert "lambda.amazonaws.com" in principals

    @mock_aws
    def test_entity_already_exists_is_handled_gracefully(self):
        """Creating a role that already exists must return the existing ARN without error."""
        from infrastructure.iam_roles import create_role

        iam = _iam()
        arn_first = create_role(iam, "BatchETL-GlueServiceRole", "glue.amazonaws.com")
        arn_second = create_role(iam, "BatchETL-GlueServiceRole", "glue.amazonaws.com")

        # Both calls should return the same ARN
        assert arn_first == arn_second


# ===========================================================================
# sns_sqs_setup tests
# ===========================================================================

class TestSNSSQSSetup:
    """Tests for infrastructure/sns_sqs_setup.py"""

    @mock_aws
    def test_creates_sns_topic(self):
        """An SNS topic named etl-job-notifications must be created."""
        from infrastructure.sns_sqs_setup import get_or_create_sns_topic

        sns = _sns()
        topic_arn = get_or_create_sns_topic(sns)

        assert "etl-job-notifications" in topic_arn

        # Verify via get_topic_attributes
        attrs = sns.get_topic_attributes(TopicArn=topic_arn)
        assert attrs["Attributes"]["TopicArn"] == topic_arn

    @mock_aws
    def test_creates_sqs_queue(self):
        """An SQS queue named etl-job-status-queue must be created."""
        from infrastructure.sns_sqs_setup import get_or_create_sqs_queue

        sqs = _sqs()
        queue_url, queue_arn = get_or_create_sqs_queue(sqs)

        assert "etl-job-status-queue" in queue_url
        assert queue_arn.endswith("etl-job-status-queue")

    @mock_aws
    def test_sqs_subscribed_to_sns(self):
        """The SQS queue must be subscribed to the SNS topic."""
        from infrastructure.sns_sqs_setup import (
            get_or_create_sns_topic,
            get_or_create_sqs_queue,
            allow_sns_to_send_to_sqs,
            subscribe_sqs_to_sns,
        )

        sns = _sns()
        sqs = _sqs()

        topic_arn = get_or_create_sns_topic(sns)
        queue_url, queue_arn = get_or_create_sqs_queue(sqs)
        allow_sns_to_send_to_sqs(sqs, queue_url, queue_arn, topic_arn)
        sub_arn = subscribe_sqs_to_sns(sns, topic_arn, queue_arn)

        # Verify the subscription appears in the list
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        protocols = [s["Protocol"] for s in subs]
        assert "sqs" in protocols, "Expected an SQS subscription on the topic"

    @mock_aws
    def test_topic_idempotent(self):
        """Calling get_or_create_sns_topic twice must return the same ARN."""
        from infrastructure.sns_sqs_setup import get_or_create_sns_topic

        sns = _sns()
        arn1 = get_or_create_sns_topic(sns)
        arn2 = get_or_create_sns_topic(sns)
        assert arn1 == arn2


# ===========================================================================
# cloudwatch_setup tests
# ===========================================================================

class TestCloudWatchSetup:
    """Tests for infrastructure/cloudwatch_setup.py"""

    @mock_aws
    def test_creates_two_billing_alarms(self):
        """Both billing alarms (warning and critical) must be created."""
        from infrastructure.cloudwatch_setup import create_billing_alarm, BILLING_ALARMS

        cw = _cloudwatch()

        for alarm_name, description, threshold in BILLING_ALARMS:
            create_billing_alarm(cw, alarm_name, description, threshold, "")

        alarm_names = [a[0] for a in BILLING_ALARMS]
        response = cw.describe_alarms(AlarmNames=alarm_names)
        existing = {a["AlarmName"] for a in response["MetricAlarms"]}

        for name in alarm_names:
            assert name in existing, f"Expected alarm {name} to exist"

    @mock_aws
    def test_warning_alarm_threshold_is_5_dollars(self):
        """etl-billing-warning must have a threshold of $5.00."""
        from infrastructure.cloudwatch_setup import create_billing_alarm

        cw = _cloudwatch()
        create_billing_alarm(cw, "etl-billing-warning", "warning", 5.0, "")

        response = cw.describe_alarms(AlarmNames=["etl-billing-warning"])
        alarm = response["MetricAlarms"][0]
        assert alarm["Threshold"] == 5.0

    @mock_aws
    def test_critical_alarm_threshold_is_9_dollars(self):
        """etl-billing-critical must have a threshold of $9.00."""
        from infrastructure.cloudwatch_setup import create_billing_alarm

        cw = _cloudwatch()
        create_billing_alarm(cw, "etl-billing-critical", "critical", 9.0, "")

        response = cw.describe_alarms(AlarmNames=["etl-billing-critical"])
        alarm = response["MetricAlarms"][0]
        assert alarm["Threshold"] == 9.0

    @mock_aws
    def test_creates_lambda_log_groups(self):
        """All three Lambda log groups must be created with 7-day retention."""
        from infrastructure.cloudwatch_setup import create_log_group, LAMBDA_LOG_GROUPS

        logs = _logs()

        for lg in LAMBDA_LOG_GROUPS:
            create_log_group(logs, lg, retention_days=7)

        response = logs.describe_log_groups()
        existing = {g["logGroupName"] for g in response["logGroups"]}

        for lg in LAMBDA_LOG_GROUPS:
            assert lg in existing, f"Expected log group {lg} to exist"

    @mock_aws
    def test_log_group_retention_is_7_days(self):
        """Log groups must have a 7-day retention policy."""
        from infrastructure.cloudwatch_setup import create_log_group

        logs = _logs()
        log_group = "/aws/lambda/etl-s3-event-trigger"
        create_log_group(logs, log_group, retention_days=7)

        response = logs.describe_log_groups(logGroupNamePrefix=log_group)
        group = response["logGroups"][0]
        assert group.get("retentionInDays") == 7
