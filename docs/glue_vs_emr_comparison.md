# Glue vs EMR Serverless — Full Comparison Reference

## 1. Service Overview

| | AWS Glue | EMR Serverless |
|--|----------|---------------|
| **Type** | Managed ETL service | Managed Spark runtime |
| **Abstraction level** | High (opinionated, managed) | Medium (pure Spark, self-managed logic) |
| **First available** | 2017 | 2022 |
| **Pricing unit** | DPU-hour (Data Processing Unit) | vCPU-hour + GB-hour |
| **Minimum billable unit** | 1 DPU for 10 seconds | 1 vCPU-second |

---

## 2. API and Code Comparison

### Simple ETL — same transformation, two APIs

**AWS Glue:**
```python
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_path]},
    format="csv",
    format_options={"withHeader": True}
)
df = datasource.toDF()
# ... transformations ...
job.commit()
```

**EMR Serverless:**
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL").getOrCreate()
df = spark.read.option("header", "true").csv(source_path)
# ... same transformations ...
spark.stop()
```

EMR code is shorter and uses no AWS-specific libraries — it's portable to any Spark environment.

---

## 3. Feature Matrix

| Feature | AWS Glue | EMR Serverless | Notes |
|---------|----------|---------------|-------|
| DynamicFrame (schema-flexible) | ✅ | ❌ | Glue-only feature |
| Native DataFrame API | ✅ | ✅ | Both support full PySpark |
| Job bookmarks (incremental) | ✅ built-in | ❌ manual | Glue advantage |
| Glue Data Catalog | ✅ built-in | ⚠️ manual | EMR can read Catalog but doesn't auto-register |
| Visual workflow editor | ✅ Glue Workflows | ❌ | Glue advantage |
| Lambda orchestration | ⚠️ possible | ✅ natural | EMR is designed for this |
| Script language | Python, Scala | Python, Scala, R | EMR supports R |
| Local testing | ⚠️ hard (awsglue not on PyPI) | ✅ easy (standard PySpark) | EMR advantage |
| Streaming | ✅ Glue Streaming | ❌ | Out of scope for this course |
| Schema evolution | ✅ DynamicFrame handles it | ⚠️ manual | Glue advantage |

---

## 4. Cost Comparison

### Pricing (us-east-1, as of 2024)

| | AWS Glue | EMR Serverless |
|--|----------|---------------|
| Compute | $0.44 / DPU-hour | $0.052 / vCPU-hour |
| Memory | Included in DPU | $0.0057 / GB-hour |
| G.1X worker = 4 vCPU + 16 GB | ~$0.44/hr | ~$0.30/hr |
| **Effective savings** | baseline | ~32% cheaper |

### Real numbers from this course

| Task | Glue | EMR | Savings |
|------|------|-----|---------|
| Simple ETL (10 min, 2 G.1X) | $0.07 | $0.05 | 29% |
| CDC (10 min, 2 G.1X) | $0.07 | $0.05 | 29% |
| Multi-step (4×10 min, 2 G.1X) | $0.29 | $0.20 | 31% |
| Full course (estimated) | $2.50 | $1.70 | 32% |

At scale (1,000 job runs/month), EMR savings become significant. For small teams running a few jobs a day, the cost difference is negligible.

---

## 5. When to Choose Each

### Choose AWS Glue when:
- You need **job bookmarks** for incremental processing and don't want to write checkpoint code
- Your team uses the **Glue Data Catalog** for centralised schema management
- Non-engineers need to understand the pipeline via the **visual Workflow editor**
- You want **schema-flexible** data loading (DynamicFrames handle evolving/messy schemas gracefully)
- You're an AWS-first shop and want tighter native integration

### Choose EMR Serverless when:
- You write **pure PySpark** and want code that runs identically locally, on-prem, and on AWS
- You need **full flexibility** in how state is tracked (custom checkpoints, DynamoDB, Redis, etc.)
- Your team is experienced with Spark and doesn't need managed scaffolding
- **Cost optimisation** matters at scale (30–40% cheaper)
- You're working with languages other than Python/Scala (R support)
- You want **easier local unit testing** (no `awsglue` mock needed)

---

## 6. Code Portability

| Aspect | AWS Glue | EMR Serverless |
|--------|----------|---------------|
| Runs locally without AWS | ❌ Requires mocked GlueContext | ✅ Standard PySpark |
| Runs on Databricks | ❌ awsglue not available | ✅ Identical code |
| Runs on Azure HDInsight | ❌ | ✅ |
| Runs on on-prem Spark | ❌ | ✅ |

If portability matters (e.g., multi-cloud or migrating away from AWS), EMR-style pure PySpark is the safer bet.

---

## 7. Career and Industry Context

Both services appear regularly in data engineering job listings. Some patterns:

- **AWS-heavy companies** (media, retail, financial services on AWS): Glue is very common
- **Data platform teams**: EMR Serverless or EMR on EC2 preferred for flexibility
- **Startups**: Either, depending on team background
- **Enterprise IT**: Glue (managed, less ops overhead)

Knowing both is a genuine differentiator. Most job descriptions list one but are happy with candidates who know the other.
