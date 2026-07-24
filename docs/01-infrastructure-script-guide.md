# Task 1 — Infrastructure Script Guide (Python boto3)

## What Are We Building?

In `docs/01-infrastructure-manual-setup.md` you created all AWS resources by clicking in the Console. This guide shows you how to do the exact same thing with Python scripts using the `boto3` library — automating every click into a few lines of code.

This is how infrastructure is managed in real data engineering teams: code that creates, configures, and destroys cloud resources reproducibly.

---

## Concepts Explained

### What is boto3?
`boto3` is the official AWS SDK for Python. Every action you can perform in the AWS Console has a corresponding Python function in boto3. For example:
- Console: S3 → Create bucket
- boto3: `s3_client.create_bucket(Bucket='my-bucket', ...)`

You've already installed it via `pip install -r requirements.txt`.

### Why automate infrastructure?
- **Reproducibility**: Anyone on your team can recreate the exact same environment by running one script
- **Speed**: Four scripts take ~30 seconds vs 20+ minutes of clicking
- **Fewer errors**: No typos in bucket names, no missed checkbox
- **Teardown**: Cleanup scripts can delete everything just as easily

---

## Prerequisites

- [ ] Completed `docs/00-prerequisites-and-setup.md`
- [ ] AWS CLI configured (`aws configure` done)
- [ ] `pip install -r requirements.txt` run successfully

---

## Option B: Script Setup

Run the four scripts in order. Each script prints exactly what it created.

### Step 1 — Create S3 Buckets

```bash
python infrastructure/s3_setup.py --prefix etl-course-yourname
```

Replace `yourname` with your name or initials. This becomes the bucket name prefix.

**What it does:**
- Creates `etl-course-yourname-landing` with a 7-day lifecycle expiry rule
- Creates `etl-course-yourname-processed`
- Creates `etl-course-yourname-output` with versioning enabled
- Blocks all public access on every bucket

**Expected output:**
```
[OK] Created bucket: etl-course-yourname-landing
[OK] Lifecycle rule added: expire after 7 days
[OK] Created bucket: etl-course-yourname-processed
[OK] Created bucket: etl-course-yourname-output
[OK] Versioning enabled on output bucket
```

---

### Step 2 — Create IAM Roles

```bash
python infrastructure/iam_roles.py
```

**What it does:**
- Creates `BatchETL-GlueServiceRole` — Glue service role with S3 and CloudWatch access
- Creates `BatchETL-EMRServerlessRole` — EMR role with S3 and CloudWatch access
- Creates `BatchETL-LambdaTriggerRole` — Lambda role to start Glue/EMR jobs
- Creates `BatchETL-LambdaNotifierRole` — Lambda role to read from SQS

**Expected output:**
```
[OK] Role created: BatchETL-GlueServiceRole
     ARN: arn:aws:iam::123456789012:role/BatchETL-GlueServiceRole
[OK] Role created: BatchETL-EMRServerlessRole
     ARN: arn:aws:iam::123456789012:role/BatchETL-EMRServerlessRole
[OK] Role created: BatchETL-LambdaTriggerRole
     ARN: arn:aws:iam::123456789012:role/BatchETL-LambdaTriggerRole
[OK] Role created: BatchETL-LambdaNotifierRole
     ARN: arn:aws:iam::123456789012:role/BatchETL-LambdaNotifierRole
```

---

### Step 3 — Create SNS Topic and SQS Queue

```bash
python infrastructure/sns_sqs_setup.py --email your@email.com
```

**What it does:**
- Creates SNS topic `etl-job-notifications`
- Creates SQS queue `etl-job-status-queue`
- Creates SQS dead-letter queue `etl-job-status-dlq`
- Subscribes the queue to the SNS topic
- Adds your email as a subscriber (you'll get a confirmation email)

**Expected output:**
```
[OK] SNS Topic created: etl-job-notifications
     ARN: arn:aws:sns:us-east-1:123456789012:etl-job-notifications
[OK] SQS Queue created: etl-job-status-queue
     URL: https://sqs.us-east-1.amazonaws.com/123456789012/etl-job-status-queue
[OK] DLQ created: etl-job-status-dlq
[OK] SQS subscribed to SNS topic
[OK] Email subscription added for your@email.com — check inbox to confirm
```

> Don't forget to click the confirmation link in your email before running later tasks.

---

### Step 4 — Create CloudWatch Alarms and Log Groups

```bash
python infrastructure/cloudwatch_setup.py
```

**What it does:**
- Creates billing alarm at $5 → sends alert to SNS topic
- Creates billing alarm at $9 → sends alert to SNS topic
- Creates CloudWatch log groups for all Lambda functions (7-day retention)

**Expected output:**
```
[OK] Billing alarm created: etl-billing-warning-5usd
[OK] Billing alarm created: etl-billing-critical-9usd
[OK] Log group created: /aws/lambda/etl-s3-event-trigger (retention: 7 days)
[OK] Log group created: /aws/lambda/etl-scheduled-trigger (retention: 7 days)
[OK] Log group created: /aws/lambda/etl-notifier (retention: 7 days)
```

---

## Script Walkthrough — What Each Script Does

### infrastructure/s3_setup.py

| Function | What It Does |
|----------|-------------|
| `create_bucket(prefix)` | Creates one S3 bucket with public access blocked |
| `add_lifecycle_rule(bucket)` | Adds 7-day expiry on the landing bucket |
| `enable_versioning(bucket)` | Enables versioning on the output bucket |
| `main()` | Parses `--prefix` argument and calls the above |

### infrastructure/iam_roles.py

| Function | What It Does |
|----------|-------------|
| `create_role(name, trust_policy, policies)` | Creates a role with the given trust and permission policies |
| `get_glue_trust_policy()` | Returns the JSON trust policy for `glue.amazonaws.com` |
| `get_emr_trust_policy()` | Returns the JSON trust policy for `emr-serverless.amazonaws.com` |
| `get_lambda_trust_policy()` | Returns the JSON trust policy for `lambda.amazonaws.com` |
| `main()` | Creates all four roles |

### infrastructure/sns_sqs_setup.py

| Function | What It Does |
|----------|-------------|
| `create_sns_topic(name)` | Creates an SNS topic, returns its ARN |
| `create_sqs_queue(name, dlq_arn)` | Creates a standard SQS queue with optional DLQ |
| `subscribe_sqs_to_sns(topic_arn, queue_arn)` | Wires the queue to the topic |
| `add_email_subscription(topic_arn, email)` | Adds an email subscriber |
| `main()` | Parses `--email`, calls all of the above |

### infrastructure/cloudwatch_setup.py

| Function | What It Does |
|----------|-------------|
| `create_billing_alarm(name, threshold, sns_arn)` | Creates a billing alarm at the given $ threshold |
| `create_log_group(name, retention_days)` | Creates a CloudWatch log group |
| `main()` | Creates alarms and log groups |

---

## Verify in AWS Console

After running all four scripts, confirm in the Console:

1. **S3** → should see three buckets with your prefix
2. **IAM** → Roles → search `BatchETL` → should see four roles
3. **SNS** → Topics → `etl-job-notifications` → Subscriptions tab → SQS and Email listed
4. **SQS** → `etl-job-status-queue` exists
5. **CloudWatch** → Alarms → two billing alarms in OK state

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `BucketAlreadyExists` | Another account owns that bucket name | Change your `--prefix` to something more unique |
| `EntityAlreadyExists` on IAM | Role already created (from manual setup) | Script skips duplicates — this is safe |
| `AuthorizationError` | IAM user missing permissions | Check `IAMFullAccess` policy is attached to your user |
| `InvalidClientTokenId` | Wrong AWS credentials | Re-run `aws configure` and check your access key |
| SNS email subscription stays Pending | Email not confirmed | Check spam folder, click the AWS confirmation link |
| `RegionDisabledException` (billing alarms) | Billing metrics only exist in us-east-1 | Ensure `AWS_DEFAULT_REGION=us-east-1` or use `--region us-east-1` |

---

## Cost Impact

Same as the manual setup — all resources here are free or within free tier limits.

**Estimated cost: $0.00**

---

## Next Step

→ `docs/02-data-generator-guide.md` — Generate synthetic CSV, JSON, and Parquet test data and upload it to your new S3 landing bucket.
