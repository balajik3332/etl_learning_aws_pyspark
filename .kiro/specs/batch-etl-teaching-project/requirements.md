# Requirements - Batch Process ETL Teaching Project

## 1. Project Overview

### 1.1 Purpose
Build a cost-effective, hands-on ETL (Extract, Transform, Load) teaching project that demonstrates batch processing patterns using both AWS Glue and AWS EMR Serverless. This project enables intermediate Python students to gain practical experience with distributed data processing while staying within a minimal AWS budget.

### 1.2 Target Audience
- **Skill Level**: Intermediate Python developers
- **Prior Knowledge**: Python programming, basic data manipulation with pandas
- **Learning Goal**: Distributed data processing with PySpark on AWS
- **Student Interest**: Some students prefer AWS Glue (managed ETL), others prefer EMR Serverless (pure Spark)
- **AWS Experience**: Little or no prior AWS experience — all setup steps must be explained clearly

### 1.3 Budget Constraint
- **Maximum Monthly Cost**: Under $10/month
- **AWS Tier**: Free tier where possible
- **Cost Optimization**: Primary design consideration

---

## 2. Prerequisites and Student Onboarding Requirements

Before starting any task, students must have the following in place. Every prerequisite is explained in the `docs/00-prerequisites-and-setup.md` instruction file.

### 2.1 Local Machine Requirements

| Requirement | Minimum Version | Why It Is Needed |
|-------------|----------------|-----------------|
| Python | 3.9 or higher | Running all project scripts locally |
| pip | Latest | Installing Python packages |
| AWS CLI | Version 2.x | Running AWS commands from terminal |
| Git | Any recent version | Cloning the project repository |
| Text editor or IDE | VS Code recommended | Editing Python scripts |
| Terminal / Command Prompt | Any | Running scripts and commands |

### 2.2 Python Package Prerequisites

All packages are listed in `requirements.txt`. Students install them with one command:

```
pip install -r requirements.txt
```

| Package | Version | Purpose |
|---------|---------|---------|
| boto3 | >=1.34 | AWS SDK — creates and manages all AWS resources from Python |
| faker | >=20.0 | Generates realistic fake data for testing |
| pandas | >=2.0 | Data manipulation and reading/writing files |
| pyarrow | >=14.0 | Reading and writing Parquet format files |
| pyspark | 3.4.x | Local Spark for testing and running EMR-style jobs |
| pytest | >=7.0 | Running automated tests |
| moto | >=4.0 | Mocking AWS services in unit tests (no real AWS calls) |

### 2.3 AWS Account Prerequisites

Students must complete the following AWS account setup before running any scripts:

1. **Create a free-tier AWS account** at https://aws.amazon.com (requires credit card, no charge if free tier limits respected)
2. **Create an IAM User** with programmatic access (do NOT use root account)
3. **Attach the following AWS managed policies** to the IAM user for this course:
   - `AmazonS3FullAccess`
   - `AWSGlueConsoleFullAccess`
   - `AmazonEMRFullAccessPolicy_v2`
   - `AWSLambda_FullAccess`
   - `AmazonSNSFullAccess`
   - `AmazonSQSFullAccess`
   - `CloudWatchFullAccess`
   - `IAMFullAccess` (needed to create service roles)
4. **Generate Access Key ID and Secret Access Key** for the IAM user
5. **Configure AWS CLI** on local machine:
   ```
   aws configure
   AWS Access Key ID: <your-access-key>
   AWS Secret Access Key: <your-secret-key>
   Default region name: us-east-1
   Default output format: json
   ```
6. **Verify CLI is working**:
   ```
   aws sts get-caller-identity
   ```
   This should print your account ID and user ARN.

### 2.4 AWS Region Requirement

Use **us-east-1 (N. Virginia)** for all resources. This region supports all required services:
- AWS Glue
- EMR Serverless
- Lambda, S3, SNS, SQS, CloudWatch, IAM

### 2.5 Cost Awareness Prerequisites

Students must read `docs/cost-optimization-guide.md` before starting. Key rules:
- Never leave EMR Serverless applications running when not in use
- Never leave Glue Dev Endpoints running (not used in this course but good practice)
- Always disable CloudWatch scheduled rules after demos
- Set up billing alarms in Task 1 — do NOT skip this step

### 2.6 Knowledge Prerequisites

Students should be comfortable with the following before starting:

| Topic | Level Required | Where to Learn If Not Familiar |
|-------|---------------|-------------------------------|
| Python functions, loops, dictionaries | Comfortable | Python.org tutorial |
| Reading/writing CSV files in Python | Comfortable | pandas documentation |
| Basic SQL (SELECT, WHERE, GROUP BY) | Familiar | W3Schools SQL tutorial |
| Command line / terminal basics | Familiar | Any beginner terminal guide |
| What is cloud computing | Aware | AWS Cloud Practitioner Essentials (free) |
| What is ETL | Aware | Explained in `docs/00-prerequisites-and-setup.md` |

---

## 3. Learning Objectives

### 2.1 Core Competencies
By completing this project, students will be able to:

1. **Understand ETL Fundamentals**
   - Explain Extract, Transform, Load concepts
   - Identify when to use batch vs streaming processing
   - Recognize common ETL patterns in industry

2. **Work with PySpark**
   - Write PySpark transformations using DataFrame API
   - Understand Glue DynamicFrames vs native Spark DataFrames
   - Perform filtering, aggregations, joins, and window functions

3. **Compare AWS Services**
   - Explain differences between AWS Glue and EMR Serverless
   - Choose appropriate service based on use case, cost, and complexity
   - Understand managed vs self-managed Spark environments

4. **Implement Orchestration Patterns**
   - Build event-driven triggers (S3 → Lambda → ETL Job)
   - Configure scheduled jobs (CloudWatch cron)
   - Trigger jobs on-demand (manual/API)

5. **Apply AWS Best Practices**
   - Implement IAM least-privilege security
   - Use SNS/SQS for decoupled notifications
   - Monitor costs with CloudWatch billing alerts
   - Apply S3 lifecycle policies for cost control

6. **Handle Real-World ETL Scenarios**
   - Implement Change Data Capture (CDC) patterns
   - Build multi-step pipelines with data quality checks
   - Process both synthetic and public datasets

---

## 4. Functional Requirements

### 3.1 ETL Patterns (Must Implement All Three)

#### 3.1.1 Simple ETL
- **Description**: Basic extract-transform-load workflow
- **Input**: CSV or JSON files in S3 landing bucket
- **Transformations**:
  - Schema validation and type casting
  - Null value handling (filtering or imputation)
  - Column renaming and selection
  - Basic aggregations (sum, count, average)
- **Output**: Parquet format in S3 output bucket
- **Implementation**: Both Glue and EMR Serverless versions

#### 3.1.2 Change Data Capture (CDC)
- **Description**: Incremental processing of only new/changed records
- **Input**: Timestamped records indicating insert/update/delete operations
- **Transformations**:
  - Identify changed records since last run
  - Implement Slowly Changing Dimension (SCD) Type 2 logic
  - Maintain historical versions of records
  - Handle merge operations (inserts, updates, deletes)
- **Output**: SCD history table in Parquet format
- **Implementation**: 
  - Glue: Use job bookmarks for tracking progress
  - EMR: Custom checkpoint file in S3

#### 3.1.3 Multi-Step Pipeline
- **Description**: Complex pipeline with multiple dependent stages
- **Stages**:
  1. **Raw**: Load data from landing, minimal validation
  2. **Clean**: Remove nulls, fix data quality issues, standardize formats
  3. **Enrich**: Join with reference data, add calculated columns
  4. **Aggregate**: Create summary tables for analytics
- **Data Flow**: Each stage reads from previous stage's S3 output
- **Error Handling**: Pipeline stops on failure at any stage
- **Implementation**:
  - Glue: Use Glue Workflows with job dependencies
  - EMR: Lambda-based orchestration or Step Functions

### 3.2 Trigger Mechanisms (Must Implement All Three)

#### 3.2.1 On-Demand (Manual Trigger)
- Students can start jobs manually via AWS Console or CLI
- Useful for testing and debugging
- No recurring costs

#### 3.2.2 Scheduled (CloudWatch Events)
- Jobs run on a cron schedule (e.g., daily at midnight)
- CloudWatch Event Rule triggers Lambda → starts Glue/EMR job
- Schedule can be enabled/disabled to control costs

#### 3.2.3 Event-Driven (S3 Event Notification)
- S3 bucket sends notification on file upload
- Lambda receives S3 event and starts appropriate ETL job
- Demonstrates real-time response to data arrival

### 3.3 Data Sources

#### 3.3.1 Synthetic Data Generator
- **Purpose**: Generate reproducible test data for learning
- **Data Types**:
  - Sales transactions (CSV): order_id, product, quantity, price, timestamp
  - Users (JSON): user_id, name, email, signup_date, country
  - Transactions (Parquet): txn_id, user_id, amount, status, date
- **Features**:
  - Configurable volume (100 rows to 100k rows)
  - Realistic data using `faker` library
  - Support for multiple formats (CSV, JSON, Parquet)
  - CDC-style incremental data with insert/update/delete flags

#### 3.3.2 Public Datasets
- **Purpose**: Apply ETL patterns to real-world data
- **Examples**: NYC Taxi Trip Data, NOAA Weather Data, COVID-19 datasets
- **Integration**: Lambda function fetches data from public URL to S3
- **Learning Value**: Handle schema variations, data quality issues, larger volumes

### 3.4 Notifications and Monitoring

#### 3.4.1 SNS (Simple Notification Service)
- All ETL jobs publish completion/failure status to SNS topic
- SNS sends email alerts to students
- SNS fans out to SQS for downstream processing

#### 3.4.2 SQS (Simple Queue Service)
- Receives job status messages from SNS
- Lambda notifier processes messages and logs structured status
- Demonstrates decoupled architecture

#### 3.4.3 CloudWatch
- **Logs**: All Lambda and ETL job logs centralized
- **Metrics**: Job duration, data volume processed
- **Billing Alerts**: Alarms at $5 and $9 thresholds

### 3.5 AWS Service Comparison

#### 3.5.1 Dual-Track Implementation
- Every ETL pattern must be implemented twice:
  - **Track A**: AWS Glue (managed ETL)
  - **Track B**: EMR Serverless (managed Spark)
- Students run both versions and compare results

#### 3.5.2 Comparison Dimensions
- **Cost**: Actual spend for equivalent workloads
- **Performance**: Job duration, throughput
- **Complexity**: Lines of code, ease of debugging
- **Use Cases**: When to choose Glue vs EMR

---

## 5. Non-Functional Requirements

### 4.1 Cost Optimization

#### 4.1.1 Compute Sizing
- **AWS Glue**: Use minimum 2 workers with G.1X DPU (4 vCPU, 16 GB RAM each)
- **EMR Serverless**: Use minimal pre-initialized capacity, scale to zero when idle
- **Lambda**: Use smallest memory allocation that meets performance needs (128-512 MB)

#### 4.1.2 Storage Management
- **S3 Lifecycle Policies**: Auto-delete data in landing bucket after 7 days
- **S3 Storage Class**: Use Standard for active data, transition to Intelligent-Tiering if needed
- **Data Volume**: Keep test datasets under 1 GB total

#### 4.1.3 Job Execution
- **Short Jobs**: Design jobs to complete in under 10 minutes
- **Manual Cleanup**: Students should delete EMR applications and Glue Dev Endpoints after use
- **Scheduled Jobs**: Disable schedules when not actively learning

#### 4.1.4 Monitoring
- Set up CloudWatch billing alarms at:
  - **Warning**: $5 spent
  - **Critical**: $9 spent
- Daily cost review recommended

### 4.2 Security

#### 4.2.1 IAM Least-Privilege
- Separate IAM roles for each service:
  - **GlueServiceRole**: S3 read/write, Glue Catalog access, CloudWatch logs
  - **EMRServerlessRole**: S3 read/write, CloudWatch logs
  - **LambdaExecutionRole**: Start Glue/EMR jobs, S3 read, SNS publish, CloudWatch logs
- No wildcard (*) permissions
- Resource-based policies where possible

#### 4.2.2 Data Security
- S3 buckets: Block public access by default
- Encryption: Use S3 default encryption (SSE-S3)
- No sensitive data: Use synthetic data only, no PII

### 4.3 Reliability

#### 4.3.1 Error Handling
- All Lambda functions: try/catch with structured error logging
- ETL jobs: validate input schema before processing
- Pipeline failure: stop execution, send failure notification

#### 4.3.2 Idempotency
- Jobs can be re-run safely without duplicating data
- Use partition overwrite mode where appropriate

### 4.4 Observability

#### 4.4.1 Logging
- Structured logs (JSON format) for all Lambda functions
- CloudWatch log groups with 7-day retention
- Glue/EMR job logs captured automatically

#### 4.4.2 Metrics
- Job success/failure count
- Job duration (p50, p95, p99)
- Data volume processed (rows, bytes)
- Cost per job

### 4.5 Maintainability

#### 4.5.1 Code Quality
- Python: Follow PEP 8 style guide
- PySpark: Use DataFrame API (preferred over RDD)
- Comments: Explain business logic, not obvious syntax
- Modularity: Reusable functions, avoid code duplication

#### 4.5.2 Version Control
- All code in Git repository
- Meaningful commit messages
- Separate branches for Glue vs EMR implementations (optional)

### 4.6 Scalability (Learning Context)

#### 4.6.1 Data Volume
- Designed for learning, not production scale
- Test with small datasets (1k-100k rows)
- Demonstrate how to scale up (add workers/vCPUs)

#### 4.6.2 Concurrency
- Show how to run multiple jobs in parallel
- Explain Glue/EMR concurrency limits

---

## 6. Success Criteria

### 5.1 Technical Success
- [ ] All 3 ETL patterns implemented and tested with both Glue and EMR
- [ ] All 3 trigger mechanisms working (on-demand, scheduled, event-driven)
- [ ] Synthetic data generator produces valid test data
- [ ] Public dataset successfully processed end-to-end
- [ ] SNS/SQS notifications delivered on job completion/failure
- [ ] CloudWatch billing alerts configured and tested
- [ ] All IAM roles follow least-privilege principle
- [ ] Total monthly AWS cost stays under $10

### 5.2 Educational Success
- [ ] Students can explain when to use Glue vs EMR Serverless
- [ ] Students can write PySpark transformations independently
- [ ] Students understand CDC and multi-step pipeline patterns
- [ ] Students can troubleshoot failed jobs using CloudWatch logs
- [ ] Students can estimate AWS costs for ETL workloads

### 6.3 Deliverables

Every task in this project must produce **two outputs**:
1. The working code or configuration for that task
2. A student-facing instruction markdown file explaining what was built, why, and how to run it

**Instruction files per task:**

| File | Covers |
|------|--------|
| `docs/00-prerequisites-and-setup.md` | Python install, AWS account, CLI setup, cost rules |
| `docs/01-infrastructure-manual-setup.md` | Step-by-step AWS Console guide to create all resources manually |
| `docs/01-infrastructure-script-guide.md` | How to run infrastructure Python scripts, what each does |
| `docs/02-data-generator-guide.md` | How to run data generators, what data is produced |
| `docs/03a-glue-simple-etl-guide.md` | What Glue is, how to register and run the Simple ETL job |
| `docs/03b-emr-simple-etl-guide.md` | What EMR Serverless is, how to submit and run the PySpark job |
| `docs/04-lambda-triggers-guide.md` | How Lambda triggers work, how to deploy and test all three types |
| `docs/05-scheduled-trigger-guide.md` | How CloudWatch Events work, how to set and disable schedules |
| `docs/06a-glue-cdc-guide.md` | What CDC is, how Glue job bookmarks work, how to run CDC job |
| `docs/06b-emr-cdc-guide.md` | How S3 checkpoint-based CDC works, how to run and compare |
| `docs/07a-glue-pipeline-guide.md` | How Glue Workflows work, how to trigger and monitor the pipeline |
| `docs/07b-emr-pipeline-guide.md` | How Lambda-orchestrated EMR pipeline works |
| `docs/08-notifications-guide.md` | How SNS and SQS work, how to verify notifications |
| `docs/09-public-dataset-guide.md` | How to fetch and process public data end-to-end |
| `docs/10-comparison-and-cleanup.md` | Glue vs EMR summary, cost review, how to delete all resources |

**Other existing deliverables:**
- [ ] Complete project codebase with all scripts and configurations
- [ ] `README.md` with project overview and quick start
- [ ] `docs/glue_vs_emr_comparison.md`
- [ ] `docs/cost_optimization_guide.md`
- [ ] `docs/student_exercises.md`
- [ ] Test suite: unit tests for data generator, integration tests for ETL jobs

---

## 7. Out of Scope

The following are explicitly **not included** in this project:

- **Streaming ETL**: Only batch processing (no Kinesis, Kafka)
- **Data Warehouse**: No Redshift, Athena, or BI tool integration
- **Data Lake Architecture**: No Glue Crawlers, Lake Formation
- **Advanced Orchestration**: No Airflow, Step Functions (basic Lambda orchestration only)
- **CI/CD**: No automated deployment pipelines
- **Production Readiness**: This is a teaching project, not production-grade
- **Real-Time Dashboards**: No Grafana, QuickSight
- **Data Governance**: No data catalog, lineage tracking (beyond basic Glue Catalog)
- **Machine Learning**: No SageMaker, ML pipelines

---

## 8. Assumptions and Constraints

### 8.1 Assumptions
- Students have AWS accounts with free tier access
- Students have basic knowledge of Python and SQL
- Students can access AWS Console and CLI
- Students have local development environment (Python 3.9+, boto3)
- Students will follow the instruction guides in order — skipping steps may cause failures

### 8.2 Constraints
- **Budget**: Hard limit of $10/month per student
- **Time**: Project designed for 2-3 weeks of part-time learning
- **AWS Region**: Use regions with Glue and EMR Serverless availability (e.g., us-east-1, us-west-2)
- **Data Privacy**: No real customer data, synthetic only
- **Network**: No VPC configuration, use default VPC

---

## 9. Dependencies

### 8.1 AWS Services (Required)
- Amazon S3
- AWS Lambda
- AWS Glue
- AWS EMR Serverless
- Amazon SNS
- Amazon SQS
- AWS IAM
- Amazon CloudWatch

### 8.2 Python Libraries
- `boto3` (AWS SDK)
- `pyspark` (Spark processing)
- `faker` (synthetic data generation)
- `pandas` (data manipulation)
- `pytest` (testing)

### 8.3 AWS CLI
- Version 2.x for job submission and resource management

---

## 10. Risks and Mitigations

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Cost overrun beyond $10/month | High | Medium | CloudWatch billing alerts, daily cost review, auto-delete policies |
| EMR Serverless not available in student's region | Medium | Low | Provide region list in documentation, fallback to Glue-only |
| Students lack AWS permissions | Medium | Medium | Provide IAM policy templates for students to request from admin |
| Job failures due to insufficient memory | Low | Medium | Document minimum resource requirements, provide troubleshooting guide |
| Free tier limits exhausted | Medium | Low | Monitor usage, rotate test accounts if needed |

---

## 11. Future Enhancements (Post-Course)

- Add Glue DataBrew for visual data preparation
- Integrate Athena for ad-hoc querying
- Add Step Functions for complex orchestration
- Implement data quality checks with Deequ
- Add Delta Lake for ACID transactions
- Integrate with QuickSight for visualization
- Add Glue Crawlers for schema discovery
- Implement data lineage tracking
