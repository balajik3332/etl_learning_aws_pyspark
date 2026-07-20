# Tasks - Batch Process ETL Teaching Project

## Overview

This task list follows agile best practices with test-driven development. Each task produces a working, demonstrable increment. Tasks build on each other with no orphaned code. The dual-track (Glue + EMR) approach is introduced progressively.

**Every task produces two outputs:**
1. Working code or configuration
2. A student-facing instruction markdown file in `docs/` explaining what was built, why it matters, how to run it manually (AWS Console) and via script, and how to verify it worked

**Total Tasks:** 11 (Task 0 through Task 10)
**Estimated Duration:** 2-3 weeks (part-time)

---

## Task 0: Prerequisites, Environment Setup and Student Orientation

**Objective:** Ensure every student has a working local environment, a configured AWS account, and understands the course cost rules before writing a single line of code.

**What this task produces:**
- `docs/00-prerequisites-and-setup.md` — the very first file students read
- `requirements.txt` — all Python dependencies pinned with exact versions

**Implementation Guidance:**

Create `requirements.txt` in the project root with all pinned dependencies:

```
boto3==1.34.0
faker==20.1.0
pandas==2.1.4
pyarrow==14.0.2
pyspark==3.4.2
pytest==7.4.4
moto==4.2.14
```

Create `docs/00-prerequisites-and-setup.md` covering:

**Section 1 — What is this course?**
- Plain English explanation of ETL (Extract, Transform, Load)
- Why batch processing matters in industry
- What students will build and learn
- Overview of AWS services used and one-line description of each

**Section 2 — Install local tools (step by step)**
- Python 3.9+: download link, how to verify with `python --version`
- pip: how to verify with `pip --version`, how to upgrade
- AWS CLI v2: download link for Windows/Mac/Linux, verify with `aws --version`
- Git: download link, verify with `git --version`
- VS Code: download link, recommended extensions (Python, AWS Toolkit)

**Section 3 — Set up your AWS account**
- How to create a free-tier AWS account (with cost warning)
- Why NOT to use the root account — create an IAM user instead
- Step-by-step: create IAM user in AWS Console with screenshots guidance
  - Go to IAM → Users → Create user
  - User name: `etl-course-student`
  - Enable console access (optional) and programmatic access
  - Attach policies: list each policy name and what it allows
  - Download access keys — store securely, never commit to Git
- Run `aws configure` — show exactly what to type
- Verify with `aws sts get-caller-identity` — show expected output

**Section 4 — Clone project and install Python packages**
```bash
git clone <repo-url>
cd batch-etl-teaching-project
pip install -r requirements.txt
```
- Explain what each package is for in plain English

**Section 5 — Cost rules (IMPORTANT — read before anything else)**
- Always set billing alarms (done in Task 1)
- Never leave EMR applications in STARTED state when not using them
- Always disable CloudWatch schedule rules after demos
- Delete test data from S3 regularly
- Check AWS Cost Explorer daily while learning
- Hard stop: if bill approaches $8, stop all jobs and review

**Section 6 — AWS region to use**
- Always use `us-east-1` (N. Virginia) — explain why (all services available)
- How to set default region in AWS Console and CLI

**Section 7 — Troubleshooting common setup issues**

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `aws: command not found` | CLI not installed or not in PATH | Reinstall AWS CLI, restart terminal |
| `Unable to locate credentials` | `aws configure` not run | Run `aws configure` |
| `An error occurred (InvalidClientTokenId)` | Wrong access key | Re-check key from IAM console |
| `python: command not found` | Python not in PATH | Add Python to PATH, restart terminal |
| `pip install` fails with permissions error | Need admin rights | Use `pip install --user -r requirements.txt` |

**Test Requirements:**
- Student can run `aws sts get-caller-identity` and see their account ID
- Student can run `python -c "import boto3; print(boto3.__version__)"` without errors
- Student can run `python -c "import pyspark; print(pyspark.__version__)"` without errors

**Demo:** Walk through the full `docs/00-prerequisites-and-setup.md` guide. Show a clean terminal with all tools verified. Show AWS Console logged in as the IAM user (not root).

---

## Task 1: Infrastructure Setup

**Objective:** Create all AWS resources required for the project — first by hand using the AWS Console so students understand what each resource is, then using Python boto3 scripts so students can automate the same steps.

**What this task produces:**
- `infrastructure/s3_setup.py`
- `infrastructure/iam_roles.py`
- `infrastructure/sns_sqs_setup.py`
- `infrastructure/cloudwatch_setup.py`
- `docs/01-infrastructure-manual-setup.md` — step-by-step AWS Console guide
- `docs/01-infrastructure-script-guide.md` — how to run the scripts

**Instruction File: `docs/01-infrastructure-manual-setup.md`**

This file teaches students what each resource is and how to create it by clicking in the AWS Console. Sections:

- **What are we building?** — Diagram showing the three S3 buckets, four IAM roles, SNS topic, SQS queue, and billing alarms. Explain why each one is needed.
- **What is Amazon S3?** — Plain English: S3 is like a folder in the cloud. Explain buckets, objects, and why we use three separate buckets (landing = raw input, processed = intermediate, output = final results).
- **Create S3 buckets manually:**
  - Go to S3 Console → Create bucket
  - Name: `etl-course-landing-<your-initials>` (bucket names must be globally unique)
  - Region: us-east-1
  - Block all public access: ON
  - Create lifecycle rule: expire objects after 7 days
  - Repeat for `-processed` and `-output` buckets (no lifecycle rule on these)
- **What is IAM?** — Plain English: IAM controls who and what can access your AWS resources. A Role is like a job badge — a service wears the badge to get permission to do certain things.
- **Create IAM roles manually** (for each of the four roles):
  - Go to IAM → Roles → Create role
  - Trusted entity: AWS service (select Glue / EMR / Lambda)
  - Attach the relevant policies
  - Name the role exactly as shown: `BatchETL-GlueServiceRole` etc.
- **What is SNS?** — Plain English: SNS is a notification service. When a job finishes, it sends a message to everyone who subscribed (like a group text message).
- **Create SNS topic manually:**
  - Go to SNS → Topics → Create topic
  - Type: Standard, Name: `etl-job-notifications`
  - Create email subscription → confirm via email
- **What is SQS?** — Plain English: SQS is a queue — a waiting line for messages. Services put messages in, other services read them at their own pace.
- **Create SQS queue manually:**
  - Go to SQS → Create queue
  - Type: Standard, Name: `etl-job-status-queue`
  - Subscribe queue to the SNS topic
- **Create CloudWatch billing alarms manually:**
  - Go to CloudWatch → Alarms → Create alarm
  - Metric: Billing → Total Estimated Charge
  - Threshold: $5 → action: notify SNS topic
  - Repeat for $9 threshold
- **Verification checklist:** Screenshots of all created resources

**Instruction File: `docs/01-infrastructure-script-guide.md`**

This file explains the Python scripts and how to run them. Sections:

- **What are the scripts doing?** — Explain that the scripts do exactly what students did manually in the Console, but automated with Python using the `boto3` library
- **What is boto3?** — Plain English: boto3 is the Python library for talking to AWS. Every AWS action you can do in the Console, you can do with boto3 in Python.
- **Run the scripts in order:**
  ```bash
  # Step 1: Create S3 buckets
  python infrastructure/s3_setup.py --prefix etl-course-yourname

  # Step 2: Create IAM roles
  python infrastructure/iam_roles.py

  # Step 3: Create SNS topic and SQS queue
  python infrastructure/sns_sqs_setup.py --email your@email.com

  # Step 4: Create CloudWatch billing alarms
  python infrastructure/cloudwatch_setup.py
  ```
- **What each script does** — plain English walkthrough of every function in each script
- **Expected output** — show exactly what the terminal should print on success
- **Verify in AWS Console** — confirm resources created by checking the Console
- **Common errors and fixes** — e.g., `BucketAlreadyExists` (choose a different prefix), `AccessDenied` (check IAM user policies)

**Implementation Guidance for Scripts:**
- `infrastructure/s3_setup.py`:
  - Three S3 buckets: `{prefix}-landing`, `{prefix}-processed`, `{prefix}-output`
  - Block all public access on each bucket
  - Add lifecycle policy on landing bucket: expire objects after 7 days
  - Enable versioning on output bucket
  - Print success message for each resource created
- `infrastructure/iam_roles.py`:
  - `BatchETL-GlueServiceRole`: trust glue.amazonaws.com, permissions for S3 read/write on project buckets, Glue Catalog CRUD, CloudWatch logs
  - `BatchETL-EMRServerlessRole`: trust emr-serverless.amazonaws.com, S3 read/write, CloudWatch logs
  - `BatchETL-LambdaTriggerRole`: trust lambda.amazonaws.com, glue:StartJobRun, emr-serverless:StartJobRun, s3:GetObject, sns:Publish, CloudWatch logs
  - `BatchETL-LambdaNotifierRole`: trust lambda.amazonaws.com, sqs:ReceiveMessage, sqs:DeleteMessage, CloudWatch logs
  - Print each role ARN on creation
- `infrastructure/sns_sqs_setup.py`:
  - SNS topic: `etl-job-notifications`
  - SQS queue: `etl-job-status-queue`
  - Subscribe SQS to SNS topic
  - Add email subscription (parameterized via `--email` argument)
  - Print topic ARN and queue URL
- `infrastructure/cloudwatch_setup.py`:
  - CloudWatch billing alarms at $5 (WARNING) and $9 (CRITICAL) linked to SNS topic
  - Log groups with 7-day retention for Lambda functions
  - Print alarm names on creation

**Test Requirements:**
- After each script runs, assert resources exist using boto3 describe/list calls
- Verify IAM role trust relationships and attached policies
- Verify SQS is subscribed to SNS
- All assertions printed to terminal with PASS/FAIL status

**Demo:** Walk through `docs/01-infrastructure-manual-setup.md` showing each resource in AWS Console. Then run all four scripts and show the same resources created automatically. Compare the two approaches.

---

## Task 2: Synthetic Data Generator

**Objective:** Build Python scripts that generate realistic test datasets and upload them to the S3 landing bucket.

**What this task produces:**
- `data-generator/generate_sales.py`
- `data-generator/generate_users.py`
- `data-generator/generate_transactions.py`
- `data-generator/generate_cdc_data.py`
- `data-generator/upload_to_s3.py`
- `docs/02-data-generator-guide.md`

**Instruction File: `docs/02-data-generator-guide.md`**

- **What are we building?** — Explain why we generate fake data instead of using real data (privacy, repeatability, cost). Describe the three datasets: sales, users, transactions.
- **What is the `faker` library?** — Plain English: faker generates realistic-looking fake names, emails, addresses, dates etc. in Python.
- **What is Parquet format?** — Plain English: Parquet is a compressed, column-based file format used in data engineering. Explain how it compares to CSV (smaller size, faster reads, typed columns).
- **Run the generators:**
  ```bash
  # Generate all datasets with default 1000 rows each
  python data-generator/generate_sales.py --rows 1000
  python data-generator/generate_users.py --rows 1000
  python data-generator/generate_transactions.py --rows 1000

  # Generate CDC (incremental) data
  python data-generator/generate_cdc_data.py --rows 200 --mode full
  python data-generator/generate_cdc_data.py --rows 50 --mode incremental

  # Upload all generated files to S3
  python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing
  ```
- **What gets uploaded where** — show the S3 folder structure that results
- **Verify in S3 Console** — Go to S3 → landing bucket → confirm files exist with correct sizes
- **Common errors and fixes** — e.g., `NoCredentialsError` (run `aws configure`), `NoSuchBucket` (run Task 1 first)

**Implementation Guidance:**
- Create `data-generator/generate_sales.py`:
  - Fields: order_id, product_name, category, quantity, unit_price, total_price, customer_id, order_timestamp, region
  - Format: CSV
  - Configurable row count via `--rows` CLI argument (default 1000)
- Create `data-generator/generate_users.py`:
  - Fields: user_id, first_name, last_name, email, country, signup_date, is_active
  - Format: JSON (array of records)
- Create `data-generator/generate_transactions.py`:
  - Fields: txn_id, user_id, amount, currency, status, txn_date, merchant
  - Format: Parquet (using pandas + pyarrow)
- Create `data-generator/generate_cdc_data.py`:
  - Same schema as sales, but add: operation (INSERT/UPDATE/DELETE), updated_at timestamp
  - `--mode full`: all INSERT records (full load)
  - `--mode incremental`: mix of INSERT (20%), UPDATE (60%), DELETE (20%)
- Create `data-generator/upload_to_s3.py`:
  - Upload all generated files to `s3://{landing-bucket}/sales/`, `/users/`, `/transactions/`
  - Print S3 URI of each uploaded file

**Test Requirements:**
- `tests/test_data_generator.py`:
  - Assert row count matches requested volume
  - Assert all required columns present
  - Assert no null values in primary key columns
  - Assert CDC operations have expected distribution (within 10% tolerance)

**Demo:** Run all generators with `--rows 1000`. Open AWS Console S3 → landing bucket. Show CSV, JSON, and Parquet files uploaded correctly. Open one file to show the data structure.

---

## Task 3A: Simple ETL — AWS Glue Track

**Objective:** Build and run a Glue ETL job that reads CSV sales data, applies transformations, and writes Parquet output.

**What this task produces:**
- `glue-jobs/simple-etl/glue_simple_etl.py`
- `docs/03a-glue-simple-etl-guide.md`

**Instruction File: `docs/03a-glue-simple-etl-guide.md`**

- **What is AWS Glue?** — Plain English: AWS Glue is a managed ETL service. You write a PySpark script, upload it, and AWS runs it for you on a managed Spark cluster — you don't have to set up any servers.
- **What is a DynamicFrame?** — Plain English: A DynamicFrame is Glue's version of a Spark DataFrame. It is more flexible with messy data (handles missing or inconsistent columns automatically). You can convert between them at any point.
- **What is the Glue Data Catalog?** — Plain English: The Glue Catalog is a central database of table definitions. When your ETL job writes output, it can register the schema so other tools know what columns exist.
- **Step 1: Upload script to S3**
  ```bash
  aws s3 cp glue-jobs/simple-etl/glue_simple_etl.py s3://etl-course-yourname-output/scripts/
  ```
- **Step 2: Register the Glue job (script or console)**
  - Console path: Go to Glue → Jobs → Create job → Script editor
  - Script path: `s3://etl-course-yourname-output/scripts/glue_simple_etl.py`
  - IAM Role: `BatchETL-GlueServiceRole`
  - Workers: 2, Worker type: G.1X
  - Job parameters: `--source_path` and `--output_path`
  - Or run via script: `python infrastructure/register_glue_jobs.py`
- **Step 3: Run the job**
  - Console: Glue → Jobs → select job → Run
  - Or via CLI: `aws glue start-job-run --job-name glue-simple-etl`
- **Step 4: Monitor the job**
  - Glue Console → Jobs → Run details tab
  - CloudWatch Logs → log group `/aws-glue/jobs/output`
- **Step 5: Verify output**
  - S3 → output bucket → confirm Parquet files partitioned by region
- **Explain the transformations in the script line by line** — students should understand every line before moving on
- **Common errors and fixes**

**Implementation Guidance:**
- Create `glue-jobs/simple-etl/glue_simple_etl.py`:
  - Use `getResolvedOptions` to accept `source_path` and `output_path` arguments
  - Read CSV using `glueContext.create_dynamic_frame.from_options`
  - Convert DynamicFrame to Spark DataFrame for transformations
  - Transformations: filter nulls on order_id, cast price to double, cast quantity to int, add `order_date` column from timestamp, compute `revenue` = quantity * unit_price
  - Write output as Parquet partitioned by `region`
  - Register output table in Glue Catalog
- Register Glue job via boto3 `glue.create_job` with 2 workers (G.1X), max concurrency 1
- Include job parameters for source/output paths

**Test Requirements:**
- `tests/test_glue_transformations.py`:
  - Use local PySpark session (not Glue context) to unit-test transformation logic
  - Assert output schema matches expected (order_id string, price double, etc.)
  - Assert row count after null filter is correct
  - Assert `revenue` column computed correctly

**Demo:** Upload sample CSV to S3 landing. Start Glue job from AWS Console. Show job completing successfully. Inspect output Parquet files in S3. Explain what each transformation did to the data.

---

## Task 3B: Simple ETL — EMR Serverless Track

**Objective:** Implement the identical ETL logic using native PySpark on EMR Serverless, then compare results and cost with Task 3A.

**What this task produces:**
- `emr-jobs/simple-etl/emr_simple_etl.py`
- `docs/03b-emr-simple-etl-guide.md`

**Instruction File: `docs/03b-emr-simple-etl-guide.md`**

- **What is EMR Serverless?** — Plain English: EMR Serverless is AWS's managed Apache Spark service. Unlike Glue, you write pure PySpark with no special Glue libraries. AWS starts a Spark cluster for your job and shuts it down automatically when done. You only pay for the time your job runs.
- **EMR vs Glue — key differences so far:**
  - Glue uses `GlueContext` and `DynamicFrame`; EMR uses `SparkSession` and `DataFrame`
  - Glue has a visual job editor; EMR is code-only
  - Glue has built-in Data Catalog integration; EMR needs manual schema management
- **Step 1: Create an EMR Serverless Application**
  - Console: Go to EMR → Serverless → Create application
  - Type: Spark, Release: emr-6.15.0, Name: `etl-course-spark`
  - Leave all other settings default to minimize cost
  - Or via script: `python infrastructure/create_emr_app.py`
- **Step 2: Upload the PySpark script to S3**
  ```bash
  aws s3 cp emr-jobs/simple-etl/emr_simple_etl.py s3://etl-course-yourname-output/scripts/
  ```
- **Step 3: Submit the job**
  ```bash
  python infrastructure/submit_emr_job.py \
    --script s3://etl-course-yourname-output/scripts/emr_simple_etl.py \
    --source s3://etl-course-yourname-landing/sales/ \
    --output s3://etl-course-yourname-output/emr/sales/
  ```
- **Step 4: Monitor the job**
  - EMR Console → Serverless → Applications → Job runs tab
  - CloudWatch Logs for Spark driver output
- **Step 5: Compare with Glue output**
  - Open both output folders in S3
  - Confirm same row count and schema
  - Note the runtime and estimated cost difference
- **Explain the PySpark script line by line**
- **Common errors and fixes**

**Implementation Guidance:**
- Create `emr-jobs/simple-etl/emr_simple_etl.py`:
  - Use `SparkSession.builder.appName("SimpleETL").getOrCreate()`
  - Read CSV with `spark.read.option("header", "true").csv(source_path)`
  - Apply same transformations as Task 3A (filter nulls, cast types, compute revenue)
  - Write Parquet partitioned by `region`
  - Accept `source_path` and `output_path` as sys.argv arguments
- Create EMR Serverless Application via boto3 (type: SPARK, release: emr-6.x)
- Upload script to S3
- Submit job via `emr_serverless.start_job_run` with spark-submit entry point
- After both jobs run, compare: output row count, schema, runtime, cost

**Test Requirements:**
- `tests/test_emr_transformations.py`:
  - Same unit tests as 3A (local PySpark session)
  - Assert output is equivalent to Glue output (same row count, same schema)

**Demo:** Run EMR job on same input data. Show identical Parquet output. Display side-by-side comparison table of Glue vs EMR: runtime, cost, lines of code, difficulty rating.

---

## Task 4: Lambda Trigger Functions

**Objective:** Build Lambda functions implementing all three trigger patterns (event-driven, scheduled, on-demand), supporting both Glue and EMR tracks.

**What this task produces:**
- `lambda-functions/s3_event_trigger/handler.py`
- `lambda-functions/scheduled_trigger/handler.py`
- `lambda-functions/notifier/handler.py`
- `docs/04-lambda-triggers-guide.md`

**Instruction File: `docs/04-lambda-triggers-guide.md`**

- **What is AWS Lambda?** — Plain English: Lambda lets you run Python code without managing any server. You upload a function, and AWS runs it whenever something triggers it. You only pay for the milliseconds your code runs.
- **What are the three trigger types?**
  - Event-driven: S3 calls Lambda automatically when a new file is uploaded
  - Scheduled: CloudWatch calls Lambda on a cron schedule (covered in Task 5)
  - On-demand: You call Lambda manually from the Console or CLI
- **How does the S3 → Lambda → Glue/EMR flow work?** — Diagram showing the chain of events
- **Step 1: Package the Lambda functions**
  ```bash
  python infrastructure/deploy_lambdas.py
  ```
  Explain what this script does: zips each handler.py, uploads to S3, creates the Lambda function with boto3
- **Step 2: Configure S3 event notification**
  - Console path: S3 → landing bucket → Properties → Event notifications → Create
  - Event type: PUT (ObjectCreated)
  - Destination: Lambda → select `etl-s3-event-trigger`
  - Or via script: already done by `deploy_lambdas.py`
- **Step 3: Test event-driven trigger**
  ```bash
  python data-generator/generate_sales.py --rows 100
  python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing
  ```
  - Go to CloudWatch → Log groups → `/aws/lambda/etl-s3-event-trigger`
  - Confirm Lambda was invoked and started the Glue or EMR job
- **Step 4: Test on-demand trigger**
  ```bash
  aws lambda invoke \
    --function-name etl-s3-event-trigger \
    --payload '{"track":"glue","job_type":"simple-etl"}' \
    response.json
  ```
- **Explain the handler.py code line by line**
- **Common errors and fixes** — e.g., Lambda permissions, timeout issues

**Implementation Guidance:**
- Create `lambda-functions/s3_event_trigger/handler.py`:
  - Receives S3 ObjectCreated event
  - Parses bucket name and object key from event
  - Determines job type from S3 prefix (e.g., `/landing/sales/` → `simple-etl`)
  - Reads `TRACK` environment variable (`glue` or `emr`)
  - Calls `glue.start_job_run` or `emr_serverless.start_job_run` accordingly
  - Publishes trigger event to SNS
  - Logs structured JSON to CloudWatch
- Create `lambda-functions/scheduled_trigger/handler.py`:
  - Reads `TRACK`, `JOB_TYPE`, `SOURCE_PATH`, `OUTPUT_PATH` from env vars
  - Starts appropriate job
- Create `lambda-functions/notifier/handler.py`:
  - Reads messages from SQS event source
  - Parses job status message (job_name, status, duration)
  - Logs structured JSON summary
- Deploy all Lambdas via boto3 `lambda.create_function` with zip packages
- Configure S3 bucket notification to invoke `s3_event_trigger` Lambda

**Test Requirements:**
- Unit tests using `moto` library to mock AWS calls
- Assert `glue.start_job_run` called with correct parameters when TRACK=glue
- Assert `emr_serverless.start_job_run` called correctly when TRACK=emr
- Assert S3 prefix correctly maps to job type

**Demo:** Upload a CSV to S3 landing bucket. Watch Lambda auto-trigger in CloudWatch Logs. Confirm Glue or EMR job starts in the respective console. Show the structured JSON log entry.

---

## Task 5: Scheduled Trigger with CloudWatch Events

**Objective:** Configure CloudWatch Event rules to run ETL jobs on a schedule, demonstrating the scheduled trigger pattern.

**What this task produces:**
- CloudWatch Event rules (via `infrastructure/cloudwatch_setup.py` update)
- `docs/05-scheduled-trigger-guide.md`

**Instruction File: `docs/05-scheduled-trigger-guide.md`**

- **What is Amazon CloudWatch Events?** — Plain English: CloudWatch Events is like a cron job in the cloud. You tell it "run this Lambda every day at midnight" and AWS handles the rest. It is now called Amazon EventBridge but still works the same way.
- **What is a cron expression?** — Explain cron syntax with examples:
  - `cron(0 0 * * ? *)` = every day at midnight UTC
  - `rate(5 minutes)` = every 5 minutes (useful for demos)
  - `rate(1 day)` = every day
- **Step 1: Create schedule rules**
  ```bash
  python infrastructure/cloudwatch_setup.py --add-schedules
  ```
  Explain what the script creates: two rules (one for Glue, one for EMR)
- **Step 2: Test by temporarily setting to every 5 minutes**
  - Console: CloudWatch → Rules → `etl-daily-schedule` → Edit → Change rate to `rate(5 minutes)`
  - Wait 5 minutes, check CloudWatch Logs for Lambda invocation
- **Step 3: Disable schedules to stop costs**
  ```bash
  aws events disable-rule --name etl-daily-schedule
  aws events disable-rule --name etl-emr-schedule
  ```
  Emphasise: always disable when not actively learning
- **Cost warning box** — Running a Glue job daily = ~$1.50/month. Always disable when not needed.
- **Common errors and fixes**

**Implementation Guidance:**
- In `infrastructure/cloudwatch_setup.py`, add:
  - CloudWatch Event Rule: `etl-daily-schedule` with cron `cron(0 0 * * ? *)` (midnight UTC)
  - Target: `scheduled_trigger` Lambda with input `{"job_type": "simple-etl", "track": "glue"}`
  - A second rule for EMR track at `cron(0 1 * * ? *)` (1 AM UTC)
- Add Lambda permission allowing `events.amazonaws.com` to invoke it

**Test Requirements:**
- Verify CloudWatch rule created and in ENABLED state
- Verify Lambda has resource-based policy allowing `events.amazonaws.com` to invoke it
- Test Lambda handler directly with a mock CloudWatch event payload

**Demo:** Change schedule to `rate(5 minutes)`, wait for trigger, show job runs automatically in CloudWatch Logs. Then immediately disable the rule and explain why disabling matters for cost.

---

## Task 6A: CDC Pattern — AWS Glue Track

**Objective:** Implement Change Data Capture using Glue job bookmarks for incremental processing and SCD Type 2 history tracking.

**What this task produces:**
- `glue-jobs/cdc/glue_cdc_job.py`
- `docs/06a-glue-cdc-guide.md`

**Instruction File: `docs/06a-glue-cdc-guide.md`**

- **What is CDC (Change Data Capture)?** — Plain English: CDC means you don't reprocess all your data every time. You only process the records that changed since the last run. This is critical in real ETL systems where datasets are millions of rows.
- **What is SCD Type 2?** — Plain English: SCD Type 2 keeps a history of changes. Instead of overwriting an old record, you close it (set an end date) and add a new version. This lets you answer questions like "what was this customer's address in January?"
- **What are Glue Job Bookmarks?** — Plain English: A bookmark is like a bookmark in a book — Glue remembers exactly where it stopped reading last time. Next run, it starts from that position automatically. Students don't write any bookmark logic — Glue handles it.
- **How to enable job bookmarks** — show the exact Glue Console toggle and the script parameter
- **Run the CDC scenario step by step:**
  - Step 1: Generate full-load data (1000 rows, all INSERT)
  - Step 2: Run Glue CDC job → confirm 1000 rows processed
  - Step 3: Generate incremental data (50 rows, mix of INSERT/UPDATE/DELETE)
  - Step 4: Run Glue CDC job again → confirm only 50 rows processed (bookmark working)
  - Step 5: Show the SCD history table — rows with different effective dates
- **Explain the SCD logic in the script line by line**
- **Common errors and fixes** — e.g., bookmark not advancing (check job bookmark option is enabled)

**Implementation Guidance:**
- Create `glue-jobs/cdc/glue_cdc_job.py`:
  - Enable job bookmarks: `--job-bookmark-option job-bookmark-enable`
  - Read incremental CDC data (bookmarks automatically track position)
  - SCD Type 2 logic using Spark DataFrame operations
  - Write updated SCD history table back to S3
  - Log count of inserts, updates, deletes processed

**Test Requirements:**
- Unit test SCD logic with a small in-memory PySpark DataFrame
- Assert after INSERT: new row with `is_current=True` exists
- Assert after UPDATE: old row has `is_current=False`, new row has `is_current=True`
- Assert after DELETE: row has `is_deleted=True`
- Run job twice: second run should process 0 new records (bookmark test)

**Demo:** Run the full CDC scenario showing two job runs. Show only 50 rows processed second time. Display the SCD history table with version history visible.

---

## Task 6B: CDC Pattern — EMR Serverless Track

**Objective:** Implement the same CDC pattern using PySpark with a custom S3-based checkpoint file, and compare the approach with Glue job bookmarks.

**What this task produces:**
- `emr-jobs/cdc/emr_cdc_job.py`
- `docs/06b-emr-cdc-guide.md`

**Instruction File: `docs/06b-emr-cdc-guide.md`**

- **Why doesn't EMR have job bookmarks?** — EMR Serverless gives you a plain Spark environment. There is no built-in "remember where you left off" feature. Instead, we implement our own checkpoint: a small JSON file in S3 that stores the last processed timestamp.
- **Tradeoff comparison:**
  - Glue bookmarks: automatic, zero code, less flexible
  - EMR checkpoint: ~20 lines of extra code, but you control exactly what is tracked and can store any metadata
- **Walk through the checkpoint logic in the script line by line**
- **Run the CDC scenario** (same steps as 6A but with EMR job)
- **Compare outputs** — confirm both approaches produce identical SCD history tables
- **When would you choose EMR checkpoint over Glue bookmarks?**
  - When you need to track multiple dimensions (not just file position)
  - When you need the checkpoint to trigger other processes
  - When your team doesn't use Glue at all
- **Common errors and fixes**

**Implementation Guidance:**
- Create `emr-jobs/cdc/emr_cdc_job.py`:
  - On startup, read checkpoint file from `s3://{bucket}/checkpoints/cdc_checkpoint.json`
  - Checkpoint contains `{"last_processed_timestamp": "2024-01-15T10:00:00Z"}`
  - Filter source data: `WHERE updated_at > last_processed_timestamp`
  - Apply same SCD Type 2 logic as Task 6A using PySpark window functions
  - Write updated history table and new checkpoint

**Test Requirements:**
- Same SCD assertions as Task 6A
- Assert checkpoint file created on first run
- Assert checkpoint timestamp advances after second run
- Assert second run processes only new records

**Demo:** Run same CDC scenario as 6A with EMR. Show identical SCD history output. Present side-by-side comparison: Glue bookmarks vs EMR checkpoint — lines of code, flexibility, use cases.

---

## Task 7A: Multi-Step Pipeline — AWS Glue Track

**Objective:** Build a Glue Workflow with four dependent jobs executing the raw → clean → enrich → aggregate pipeline.

**What this task produces:**
- `glue-jobs/multi-step/glue_step1_raw.py` through `glue_step4_aggregate.py`
- `docs/07a-glue-pipeline-guide.md`

**Instruction File: `docs/07a-glue-pipeline-guide.md`**

- **What is a multi-step pipeline?** — Real ETL systems rarely do everything in one job. Breaking work into stages makes each step testable, debuggable, and reusable. If Step 3 fails, you don't re-run Steps 1 and 2.
- **What is a Glue Workflow?** — Plain English: A Glue Workflow is a visual orchestrator. You connect jobs together and say "run Job 2 only if Job 1 succeeded." AWS handles the dependency and execution order.
- **Explain each stage and its purpose:**
  - Step 1 Raw: Load the file, check the schema is correct, write as-is to Parquet
  - Step 2 Clean: Fix data quality problems (nulls, wrong types, bad formats)
  - Step 3 Enrich: Add extra useful information by joining with reference data
  - Step 4 Aggregate: Summarise data into business KPIs
- **Show the Glue Workflow visual editor** — explain how to read the dependency graph
- **Step-by-step: create the Glue Workflow**
  - Console: Glue → Workflows → Create workflow → Add jobs and triggers
  - Or via script: `python infrastructure/create_glue_workflow.py`
- **How to trigger the workflow:**
  ```bash
  aws glue start-workflow-run --name etl-multi-step-workflow
  ```
- **How to monitor** — Glue Console workflow run history, individual job logs
- **What happens when a step fails?** — Show the workflow stopping, SNS alert arriving
- **Common errors and fixes**

**Implementation Guidance:**
- Four Glue job scripts with the stage logic described above
- Glue Workflow via boto3 with four chained triggers
- SNS notification on workflow failure

**Test Requirements:**
- Unit test each step's transformation logic independently
- Integration test: run full workflow, assert all four S3 outputs exist
- Assert final aggregate totals match sum of input data
- Test failure handling: inject bad data in Step 2, assert workflow stops

**Demo:** Trigger Glue Workflow from console. Watch all four jobs execute sequentially in the Glue Workflow visual editor. Show intermediate S3 outputs at each stage. Show the SNS failure alert by injecting a bad row.

---

## Task 7B: Multi-Step Pipeline — EMR Serverless Track

**Objective:** Orchestrate the same four-stage pipeline using EMR Serverless jobs coordinated by a Lambda orchestrator function.

**What this task produces:**
- `emr-jobs/multi-step/emr_step1_raw.py` through `emr_step4_aggregate.py`
- `lambda-functions/emr_pipeline_orchestrator/handler.py`
- `docs/07b-emr-pipeline-guide.md`

**Instruction File: `docs/07b-emr-pipeline-guide.md`**

- **Why is EMR pipeline orchestration different from Glue?** — EMR Serverless has no built-in workflow engine. We use a Lambda function that submits one EMR job at a time, waits for it to finish, then submits the next. This teaches students how pipeline orchestration works at a code level.
- **Walk through the Lambda orchestrator logic step by step** — explain the polling loop, status check, and failure handling
- **Compare with Glue Workflow:**
  - Glue: visual, click-based, AWS manages the state
  - EMR + Lambda: code-based, you manage the state, more transparent
- **How to trigger the pipeline:**
  ```bash
  aws lambda invoke \
    --function-name etl-emr-pipeline-orchestrator \
    --payload '{}' \
    response.json
  cat response.json
  ```
- **How to monitor** — Lambda CloudWatch Logs showing each job submission and status poll
- **Cost note** — Lambda running for up to 15 minutes; explain Lambda timeout and how to work around it for longer pipelines
- **Common errors and fixes**

**Implementation Guidance:**
- Four EMR PySpark scripts with identical stage logic as Task 7A
- Lambda orchestrator: submit → poll → submit next, stop on failure, publish to SNS

**Test Requirements:**
- Unit test orchestrator logic with mocked EMR responses
- Assert Step 2 is NOT started if Step 1 fails
- Integration test: run full pipeline, verify all four S3 outputs exist and match Task 7A outputs

**Demo:** Trigger Lambda orchestrator. Watch four EMR jobs run sequentially in EMR Console. Compare side-by-side with Glue Workflow visual: orchestration code vs visual editor, runtime, cost.

---

## Task 8: SNS/SQS Notifications Integration

**Objective:** Add job completion and failure notifications to all Glue and EMR jobs, and wire up the full SNS → SQS → Lambda notification pipeline.

**What this task produces:**
- Updated Glue and EMR job scripts with SNS publish calls
- Updated `lambda-functions/notifier/handler.py`
- `docs/08-notifications-guide.md`

**Instruction File: `docs/08-notifications-guide.md`**

- **Why do ETL jobs need notifications?** — In production, ETL jobs run overnight. You need to know if they succeeded or failed without manually checking. SNS + SQS is the standard AWS pattern for this.
- **How the notification chain works:**
  ```
  ETL Job finishes → publishes to SNS → SNS fans out to:
    → SQS queue (for programmatic processing)
    → Your email (for human awareness)
  SQS → Lambda Notifier → CloudWatch Logs (structured status)
  ```
- **What is "fan-out"?** — Plain English: one SNS message goes to multiple destinations simultaneously. Like sending one email to a group.
- **What is a Dead Letter Queue (DLQ)?** — Plain English: if a message can't be processed after several attempts, it moves to the DLQ instead of being lost. Good for debugging.
- **How to verify notifications:**
  - Run a Glue job → check email inbox for completion notification
  - Go to SQS Console → `etl-job-status-queue` → Poll for messages → see the status JSON
  - Go to CloudWatch → `/aws/lambda/etl-notifier` → see the structured log
- **How to trigger a failure notification intentionally** — deploy a Glue job with a deliberate syntax error, show the failure alert
- **Common errors and fixes**

**Implementation Guidance:**
- Update all Glue and EMR scripts to publish to SNS on completion
- Status payload: `{job_name, track, status, start_time, end_time, rows_processed, output_path}`
- Add SQS dead-letter queue for unprocessable messages
- Notifier Lambda: parse SQS message body, log structured status summary

**Test Requirements:**
- Unit test SNS publish payload schema validation
- Integration test: run a job, assert SQS message received within 60 seconds
- Test DLQ: send malformed message, assert it lands in DLQ

**Demo:** Run a Glue simple-ETL job. Show SNS email notification received. Show SQS message in AWS Console. Show structured log from notifier Lambda in CloudWatch. Then trigger a failure and show the failure alert.

---

## Task 9: Public Dataset Integration

**Objective:** Fetch a real-world public dataset and apply all ETL patterns to it, demonstrating that the pipeline works with real data.

**What this task produces:**
- `lambda-functions/public_data_fetcher/handler.py`
- `public-datasets/fetch_nyc_taxi.py`
- `docs/09-public-dataset-guide.md`

**Instruction File: `docs/09-public-dataset-guide.md`**

- **Why use real data?** — Synthetic data is clean and predictable. Real data has nulls, outliers, wrong types, inconsistent formats. This is what students will encounter in real jobs.
- **What is the NYC Taxi dataset?** — Plain English: NYC publishes monthly records of every taxi trip in New York City. It includes pickup/dropoff times, distance, fares, and tips. It is free and widely used for data engineering learning.
- **Step 1: Fetch the dataset**
  ```bash
  python public-datasets/fetch_nyc_taxi.py --month 2023-01 --sample 50000
  ```
  Explain what this does: downloads ~50k rows from the public S3 bucket, uploads to your landing bucket
- **Step 2: Explore the data first**
  - Download a few rows locally and open in a spreadsheet or Python
  - Notice: some fares are negative (data quality issue), some trips have 0 distance, passenger_count can be null
- **Step 3: Run the Simple ETL on real data**
  - Same Glue and EMR jobs from Task 3A/3B — what happens? Do they handle the data issues?
  - Document any errors or unexpected results
- **Step 4: Run the Multi-Step Pipeline on real data**
  - Step 2 (clean) should catch the negative fares and 0-distance trips
  - Step 3 (enrich) should add `time_of_day_bucket` (morning/afternoon/evening/night)
  - Step 4 (aggregate) should produce: average fare by hour of day
- **Show the final analytical result** — a table of average NYC taxi fares by hour
- **What did you learn from real data that synthetic data didn't teach you?**
- **Common errors and fixes**

**Implementation Guidance:**
- Lambda fetcher: download NYC Taxi CSV from public URL, sample 50k rows, upload to S3
- Apply all ETL patterns from Tasks 3-7 to the real dataset
- Document schema differences and data quality issues in the guide

**Test Requirements:**
- Assert public data lands in S3 with correct file size (> 0 bytes)
- Assert ETL output schema matches expected columns
- Assert aggregate totals are within reasonable range
- Assert no job failures on real data

**Demo:** Run full end-to-end pipeline on NYC Taxi data with both Glue and EMR. Show final aggregate table: average trip fare by hour of day. Discuss the data quality issues found and how they were handled.

---

## Task 10: Comparison Guide, Cost Dashboard and Student Exercises

**Objective:** Consolidate all learnings into reference documentation, a cost dashboard, and hands-on exercises. Critically: clean up all AWS resources so students don't accrue costs after the course.

**What this task produces:**
- `docs/10-comparison-and-cleanup.md`
- `docs/glue_vs_emr_comparison.md`
- `docs/cost_optimization_guide.md`
- `docs/student_exercises.md`
- CloudWatch dashboard (via boto3)
- `infrastructure/cleanup.py` — deletes all course resources

**Instruction File: `docs/10-comparison-and-cleanup.md`**

- **Full Glue vs EMR Serverless comparison** — tables covering:
  - When to use each service (decision framework with real scenarios)
  - Feature comparison (bookmarks, catalog, DynamicFrames, job submission, monitoring)
  - Code complexity (show same transformation in both, count lines)
  - Cost comparison using actual numbers from Tasks 3-9
  - Career context: which is more common in enterprise vs startup? which appears more on job listings?
- **How to view your actual costs:**
  - Go to AWS Console → Billing → Cost Explorer
  - Filter by service (Glue, EMR) to see per-service breakdown
  - Compare actual vs estimated costs from Task 1 setup
- **CloudWatch dashboard walkthrough:**
  - What each metric widget shows
  - How to read job duration and cost trends
- **IMPORTANT: Resource cleanup steps** — explain that leaving resources running costs money, and every resource created in this course must be deleted:
  ```bash
  # Delete all course resources (run this after completing the course)
  python infrastructure/cleanup.py --prefix etl-course-yourname --confirm
  ```
  Also provide manual Console steps for each resource type as a backup
- **Student exercises** — five take-home challenges:
  - Exercise 1: Modify Simple ETL to add a new transformation (filter by region)
  - Exercise 2: Change CDC checkpoint to store in DynamoDB instead of S3
  - Exercise 3: Add a data quality report to the multi-step pipeline
  - Exercise 4: Add a new trigger using SQS as the event source instead of S3
  - Exercise 5: Run the full pipeline on a different public dataset (suggest NOAA weather data)

**Implementation Guidance:**
- Create `docs/glue_vs_emr_comparison.md` with full comparison matrix
- Create `docs/cost_optimization_guide.md` with checklist, billing alarm guide, cost estimation template
- Create CloudWatch dashboard via boto3 with job duration, Lambda invocations, estimated charge widgets
- Create `infrastructure/cleanup.py` to delete: S3 buckets and objects, IAM roles, SNS topic, SQS queues, Lambda functions, Glue jobs and workflows, EMR Serverless application, CloudWatch rules and alarms

**Test Requirements:**
- Verify CloudWatch dashboard created and displays metrics from at least 3 job runs
- Run `cleanup.py --dry-run` to show what would be deleted before actually deleting

**Demo:** Walk through the Glue vs EMR comparison guide with actual numbers from the course. Show CloudWatch cost dashboard. Run `cleanup.py --dry-run` to show the cleanup plan, then run it for real. Confirm AWS Console shows no remaining course resources.

---

## Completion Checklist

### Setup and Infrastructure
- [x] Task 0: Local tools installed, AWS account configured, `aws sts get-caller-identity` works
- [-] Task 0: `docs/00-prerequisites-and-setup.md` created
- [~] Task 1: Infrastructure created (S3, IAM, SNS, SQS, CloudWatch billing alarms)
- [~] Task 1: `docs/01-infrastructure-manual-setup.md` created
- [~] Task 1: `docs/01-infrastructure-script-guide.md` created

### Data and Simple ETL
- [~] Task 2: Synthetic data generator producing CSV, JSON, Parquet, CDC data
- [~] Task 2: `docs/02-data-generator-guide.md` created
- [~] Task 3A: Glue Simple ETL job running and producing Parquet output
- [~] Task 3A: `docs/03a-glue-simple-etl-guide.md` created
- [~] Task 3B: EMR Serverless Simple ETL job running with equivalent output
- [~] Task 3B: `docs/03b-emr-simple-etl-guide.md` created

### Triggers
- [~] Task 4: Lambda triggers (event-driven, on-demand) deployed and tested
- [~] Task 4: `docs/04-lambda-triggers-guide.md` created
- [~] Task 5: CloudWatch scheduled trigger firing and starting jobs
- [~] Task 5: `docs/05-scheduled-trigger-guide.md` created

### CDC
- [~] Task 6A: Glue CDC with job bookmarks tracking incremental data
- [~] Task 6A: `docs/06a-glue-cdc-guide.md` created
- [~] Task 6B: EMR CDC with S3 checkpoint tracking incremental data
- [~] Task 6B: `docs/06b-emr-cdc-guide.md` created

### Multi-Step Pipeline
- [~] Task 7A: Glue Workflow executing 4-stage pipeline sequentially
- [~] Task 7A: `docs/07a-glue-pipeline-guide.md` created
- [~] Task 7B: Lambda-orchestrated EMR 4-stage pipeline executing sequentially
- [~] Task 7B: `docs/07b-emr-pipeline-guide.md` created

### Notifications, Real Data, and Wrap-Up
- [~] Task 8: SNS email + SQS messages received on all job completions/failures
- [~] Task 8: `docs/08-notifications-guide.md` created
- [~] Task 9: Public dataset (NYC Taxi) processed end-to-end with both tracks
- [~] Task 9: `docs/09-public-dataset-guide.md` created
- [~] Task 10: Comparison guide, cost dashboard, and student exercises complete
- [~] Task 10: `docs/10-comparison-and-cleanup.md` created
- [~] Task 10: `infrastructure/cleanup.py` run — all course AWS resources deleted
- [~] Total monthly AWS cost verified under $10
