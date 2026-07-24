# Task 1 — Infrastructure Manual Setup (AWS Console)

## What Are We Building?

Before writing a single ETL job, we need to create the AWS resources that the entire project depends on:

- **3 S3 buckets** — store raw input data, intermediate results, and final output
- **4 IAM roles** — give AWS services permission to talk to each other
- **1 SNS topic** — send job notifications (success/failure)
- **1 SQS queue** — receive and buffer those notifications
- **2 CloudWatch billing alarms** — warn you before the bill gets out of hand

This guide walks you through creating every resource by hand in the AWS Console. This is intentional — clicking through the Console once helps you understand *what each resource is* before we automate it with Python scripts in `docs/01-infrastructure-script-guide.md`.

---

## Concepts Explained

### What is Amazon S3?
S3 (Simple Storage Service) is cloud file storage. Think of it as a folder in the cloud that any AWS service can read from or write to. The top-level container is called a **bucket**. Files inside a bucket are called **objects**.

We use three separate buckets to keep data organised by stage:
- **Landing** — raw files arrive here (CSV, JSON)
- **Processed** — intermediate Parquet files after cleaning
- **Output** — final results and ETL scripts

### What is IAM?
IAM (Identity and Access Management) controls who can do what in your AWS account. A **Role** is like a job badge — when an AWS service (like Glue) wears the badge, it gets permission to do certain things (like read from S3). Roles are safer than hardcoding credentials.

### What is Amazon SNS?
SNS (Simple Notification Service) is a pub/sub messaging service. When an ETL job finishes, it publishes a message to an SNS **topic**. Everyone subscribed to that topic (your email, an SQS queue) receives the message simultaneously — like a group text.

### What is Amazon SQS?
SQS (Simple Queue Service) is a message queue. It holds messages from SNS until something reads them. Unlike email, messages in SQS don't disappear after they're read — they stay for up to 14 days and can be reprocessed. It's useful for building robust, decoupled systems.

### What is CloudWatch?
CloudWatch is AWS's monitoring service. We use it for:
- Collecting logs from Lambda, Glue, and EMR
- Setting billing alarms so you know when costs are rising

---

## Prerequisites

- [ ] Completed `docs/00-prerequisites-and-setup.md`
- [ ] Logged into AWS Console as your IAM user (not root)
- [ ] Region set to **us-east-1 (N. Virginia)**

---

## Option A: Manual Setup (AWS Console)

### Part 1 — Create S3 Buckets

We need three buckets. Bucket names must be globally unique across all AWS accounts, so add your initials or a short random suffix.

**Bucket 1: Landing**

1. Go to **S3** in the AWS Console → click **Create bucket**
2. **Bucket name**: `etl-course-landing-<your-initials>` (e.g., `etl-course-landing-jd`)
3. **AWS Region**: US East (N. Virginia) `us-east-1`
4. **Block Public Access**: Leave all four checkboxes **ON** (default)
5. **Versioning**: Leave disabled
6. Click **Create bucket**
7. Open the bucket → go to **Management** tab → click **Create lifecycle rule**
   - Rule name: `expire-after-7-days`
   - Apply to all objects in the bucket: checked
   - Expiration: ✅ Expire current versions after **7 days**
   - Click **Create rule**

**Bucket 2: Processed**

1. Click **Create bucket**
2. **Bucket name**: `etl-course-processed-<your-initials>`
3. Region: `us-east-1`, Block public access: ON
4. Click **Create bucket** — no lifecycle rule needed

**Bucket 3: Output**

1. Click **Create bucket**
2. **Bucket name**: `etl-course-output-<your-initials>`
3. Region: `us-east-1`, Block public access: ON
4. **Versioning**: Enable (keeps previous versions of output files)
5. Click **Create bucket**

---

### Part 2 — Create IAM Roles

We need four roles. Each is created the same way — only the trusted service and permissions differ.

#### Role 1: BatchETL-GlueServiceRole

1. Go to **IAM** → **Roles** → **Create role**
2. **Trusted entity**: AWS service → select **Glue**
3. Click **Next**
4. Search for and add these policies:
   - `AmazonS3FullAccess` *(we'll tighten this in production — fine for learning)*
   - `AWSGlueServiceRole`
   - `CloudWatchLogsFullAccess`
5. Click **Next**
6. **Role name**: `BatchETL-GlueServiceRole`
7. Click **Create role**

#### Role 2: BatchETL-EMRServerlessRole

1. **IAM** → **Roles** → **Create role**
2. **Trusted entity**: AWS service → select **EMR** → choose **EMR Serverless**
3. Add policies: `AmazonS3FullAccess`, `CloudWatchLogsFullAccess`
4. **Role name**: `BatchETL-EMRServerlessRole`
5. Create role

#### Role 3: BatchETL-LambdaTriggerRole

1. **IAM** → **Roles** → **Create role**
2. **Trusted entity**: AWS service → **Lambda**
3. Add policies: `AWSLambdaBasicExecutionRole`, `AmazonS3ReadOnlyAccess`, `AmazonSNSFullAccess`
4. After creating the role, open it → **Add permissions** → **Create inline policy**
   - Service: **Glue** → Actions: `StartJobRun` → Resources: All
   - Add a second statement: Service: **EMR Serverless** → Actions: `StartJobRun` → Resources: All
5. **Role name**: `BatchETL-LambdaTriggerRole`

#### Role 4: BatchETL-LambdaNotifierRole

1. **IAM** → **Roles** → **Create role**
2. **Trusted entity**: AWS service → **Lambda**
3. Add policies: `AWSLambdaBasicExecutionRole`, `AmazonSQSFullAccess`
4. **Role name**: `BatchETL-LambdaNotifierRole`

---

### Part 3 — Create SNS Topic

1. Go to **Amazon SNS** → **Topics** → **Create topic**
2. **Type**: Standard
3. **Name**: `etl-job-notifications`
4. Click **Create topic**
5. On the topic page, click **Create subscription**:
   - **Protocol**: Email
   - **Endpoint**: your email address
   - Click **Create subscription**
6. Check your email inbox for a confirmation email from AWS → click **Confirm subscription**

---

### Part 4 — Create SQS Queue

1. Go to **Amazon SQS** → **Create queue**
2. **Type**: Standard
3. **Name**: `etl-job-status-queue`
4. Scroll down to **Dead-letter queue** → enable it:
   - Create a new DLQ first (repeat these steps with name `etl-job-status-dlq`, no DLQ on it)
   - Then come back and set the DLQ ARN and **Maximum receives**: 3
5. Click **Create queue**

**Subscribe the queue to the SNS topic:**

1. Open the `etl-job-status-queue`
2. Click **Subscribe to Amazon SNS topic**
3. Select the `etl-job-notifications` topic
4. Click **Save**

---

### Part 5 — Create CloudWatch Billing Alarms

> **Important**: Billing alarms must be created in the `us-east-1` region (they only work there, regardless of where your other resources are).

**Alarm 1 — Warning at $5**

1. Go to **CloudWatch** → **Alarms** → **Create alarm**
2. Click **Select metric** → **Billing** → **Total Estimated Charge** → select `USD` → **Select metric**
3. Conditions:
   - **Threshold type**: Static
   - **Whenever EstimatedCharges is**: Greater than **5**
4. Notification: Send to the `etl-job-notifications` SNS topic
5. **Alarm name**: `etl-billing-warning-5usd`
6. Create alarm

**Alarm 2 — Critical at $9**

1. Repeat the steps above with threshold **9**
2. **Alarm name**: `etl-billing-critical-9usd`

---

## Verification Steps

After completing all steps, confirm every resource exists:

| Resource | How to Verify |
|----------|--------------|
| S3 buckets (3) | S3 Console → bucket list shows all three |
| IAM roles (4) | IAM → Roles → search "BatchETL" → 4 results |
| SNS topic | SNS → Topics → `etl-job-notifications` visible |
| SNS email subscription | Status shows **Confirmed** (not Pending) |
| SQS queue | SQS → Queues → `etl-job-status-queue` visible |
| SQS subscribed to SNS | SNS → Topics → `etl-job-notifications` → Subscriptions tab |
| Billing alarms (2) | CloudWatch → Alarms → both alarms show OK state |

---

## What Did We Just Build?

```
                    ┌──────────────────────────────────────┐
                    │           S3 Storage Layer            │
                    │  landing  │  processed  │   output    │
                    └──────────────────────────────────────┘

  BatchETL-GlueServiceRole     BatchETL-EMRServerlessRole
  BatchETL-LambdaTriggerRole   BatchETL-LambdaNotifierRole

         SNS: etl-job-notifications
                    │
              ┌─────┴──────┐
              ▼            ▼
     Email (you)    SQS: etl-job-status-queue

    CloudWatch Billing Alarms: $5 warning, $9 critical
```

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| Bucket name already exists | Names are globally unique | Add more characters to your suffix |
| Can't create billing alarm | Wrong region | Switch to us-east-1 in console |
| SNS subscription stays Pending | Email not confirmed | Check spam folder, click confirmation link |
| IAM: "Not authorized to assume role" | Trust relationship wrong | Check trusted entity matches the AWS service |
| SQS subscription fails | SNS topic policy blocks SQS | SNS adds SQS policy automatically — retry |

---

## Cost Impact

All resources created in this task are free or near-free:
- S3: First 5 GB/month and 20,000 GET requests are free
- IAM roles: Always free
- SNS: First 1 million publishes/month are free
- SQS: First 1 million requests/month are free
- CloudWatch billing alarms: First 10 alarms are free

**Estimated cost for this task: $0.00**

---

## Next Step

→ `docs/01-infrastructure-script-guide.md` — Learn how to automate everything you just did manually using Python boto3 scripts.
