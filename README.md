# Batch ETL Teaching Project — AWS Glue & EMR Serverless

A hands-on learning project for intermediate Python developers to build real batch ETL pipelines using **AWS Glue** and **AWS EMR Serverless** with PySpark. Every pattern is implemented twice — once with Glue and once with EMR — so you can directly compare the two approaches.

**Budget:** Designed to stay under **$10/month** on AWS.  
**Duration:** 2–3 weeks, part-time.

---

## What You Will Build

| # | Task | What Gets Built |
|---|------|----------------|
| 0 | Prerequisites & Setup | Local tools, AWS account, IAM user, billing alarms |
| 1 | Infrastructure | S3 buckets, IAM roles, SNS, SQS, CloudWatch alarms |
| 2 | Synthetic Data Generator | CSV / JSON / Parquet test data + CDC data |
| 3A | Simple ETL — Glue | DynamicFrame ETL job → Parquet output |
| 3B | Simple ETL — EMR | PySpark DataFrame ETL → Parquet output |
| 4 | Lambda Triggers | Event-driven (S3), scheduled, and on-demand triggers |
| 5 | Scheduled Trigger | CloudWatch cron rule → Lambda → ETL job |
| 6A | CDC — Glue | Job bookmarks + SCD Type 2 history |
| 6B | CDC — EMR | S3 checkpoint file + SCD Type 2 history |
| 7A | Multi-Step Pipeline — Glue | 4-stage Glue Workflow (Raw → Clean → Enrich → Aggregate) |
| 7B | Multi-Step Pipeline — EMR | Lambda-orchestrated 4-stage EMR pipeline |
| 8 | SNS/SQS Notifications | Job completion/failure alerts to email + SQS |
| 9 | Public Dataset | Full pipeline on real NYC Taxi data |
| 10 | Comparison & Cleanup | Glue vs EMR analysis, cost review, resource teardown |

---

## Project Structure

```
batch-etl-teaching-project/
├── infrastructure/          # boto3 scripts to create all AWS resources
│   ├── s3_setup.py
│   ├── iam_roles.py
│   ├── sns_sqs_setup.py
│   └── cloudwatch_setup.py
├── data-generator/          # Synthetic data generators (CSV, JSON, Parquet, CDC)
├── glue-jobs/               # AWS Glue PySpark scripts
│   ├── simple-etl/
│   ├── cdc/
│   └── multi-step/
├── emr-jobs/                # EMR Serverless PySpark scripts
│   ├── simple-etl/
│   ├── cdc/
│   └── multi-step/
├── lambda-functions/        # Lambda trigger and notifier functions
│   ├── s3_event_trigger/
│   ├── scheduled_trigger/
│   ├── notifier/
│   └── public_data_fetcher/
├── public-datasets/         # Scripts to fetch real-world datasets
├── tests/                   # Unit and integration tests
├── docs/                    # Student instruction guides (read these in order)
└── requirements.txt         # Pinned Python dependencies
```

---

## Quick Start

### 1. Prerequisites

- Python 3.9+
- AWS CLI v2 ([install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html))
- Git
- An AWS account (free tier works — see cost notes below)

> **Read `docs/00-prerequisites-and-setup.md` first.** It walks you through every installation step and AWS account configuration.

### 2. Clone and install

```bash
git clone https://github.com/balajik3332/etl_learning_aws_pyspark.git
cd etl_learning_aws_pyspark
pip install -r requirements.txt
```

### 3. Configure AWS

```bash
aws configure
# Enter your Access Key ID, Secret Access Key, region (us-east-1), output format (json)

# Verify it works
aws sts get-caller-identity
```

### 4. Follow the task guides in order

All student-facing instruction files are in `docs/`. Start with `docs/00-prerequisites-and-setup.md` and follow the numbered guides sequentially.

---

## AWS Services Used

| Service | Purpose |
|---------|---------|
| Amazon S3 | Landing, processed, and output data storage |
| AWS Glue | Managed ETL — Track A jobs and workflows |
| AWS EMR Serverless | Managed Spark — Track B jobs |
| AWS Lambda | Trigger functions and pipeline orchestrator |
| Amazon SNS | Job completion/failure notifications (fan-out) |
| Amazon SQS | Decoupled job status queue |
| Amazon CloudWatch | Logs, metrics, cron schedules, billing alarms |
| AWS IAM | Least-privilege roles for every service |

All resources are created in **us-east-1 (N. Virginia)**.

---

## Cost Guidance

Estimated monthly cost with normal learning usage: **$3.50–$5.00**.

Hard rules to stay under $10:
- Set billing alarms at $5 and $9 (done in Task 1).
- Disable CloudWatch schedule rules when not actively learning.
- Never leave an EMR Serverless application in STARTED state.
- Delete test data from S3 regularly.
- Run `python infrastructure/cleanup.py` after completing the course.

See `docs/cost_optimization_guide.md` for the full checklist.

---

## ETL Patterns Covered

- **Simple ETL** — Extract CSV/JSON, transform (cast types, filter nulls, compute columns), load Parquet
- **Change Data Capture (CDC)** — Incremental processing with SCD Type 2 history
  - Glue: job bookmarks (built-in, zero extra code)
  - EMR: custom S3 checkpoint file
- **Multi-Step Pipeline** — Four dependent stages with failure handling
  - Glue: Glue Workflows with job triggers
  - EMR: Lambda-based step orchestrator

## Trigger Patterns Covered

- **Event-driven** — S3 file upload → Lambda → ETL job
- **Scheduled** — CloudWatch cron rule → Lambda → ETL job
- **On-demand** — Manual start via AWS Console or CLI

---

## Glue vs EMR Serverless — Quick Summary

| | AWS Glue | EMR Serverless |
|--|----------|----------------|
| API | DynamicFrame + Spark DataFrame | Native Spark DataFrame |
| Incremental processing | Job bookmarks (built-in) | Custom S3 checkpoint |
| Orchestration | Glue Workflows (visual) | Lambda state machine (code) |
| Data Catalog | Built-in integration | Manual schema management |
| Best for | Managed ETL, quick setup | More control, pure Spark |

See `docs/glue_vs_emr_comparison.md` for the full comparison with actual cost numbers.

---

## Running Tests

```bash
# Unit tests only (no AWS credentials needed — uses moto mocks)
pytest tests/ -v

# Specific test file
pytest tests/test_data_generator.py -v
```

---

## Spec and Design Documents

This project was built using [Kiro](https://kiro.dev) spec-driven development. The full requirements, design, and task breakdown are in `.kiro/specs/batch-etl-teaching-project/`.

---

## License

MIT
