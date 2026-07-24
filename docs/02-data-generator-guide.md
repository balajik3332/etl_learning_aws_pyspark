# Task 2 — Synthetic Data Generator

## What Are We Building?

In this task we build Python scripts that create realistic fake datasets and upload them to the S3 landing bucket. Every ETL job in the course reads from these files, so we need them before we can run any processing.

We generate four datasets:

| Dataset | Format | What It Represents |
|---------|--------|--------------------|
| Sales | CSV | Customer orders with products, quantities, prices, and timestamps |
| Users | JSON | Registered user accounts with countries and activity status |
| Transactions | Parquet | Financial transactions with amounts, currencies, and merchant names |
| CDC | CSV | Change Data Capture records simulating inserts, updates, and deletes |

## Concepts Explained

### What is the `faker` library?

`faker` is a Python library that generates realistic-looking fake data: names, emails, addresses, company names, dates, and much more. We use it instead of real customer data to protect privacy and make the course data repeatable and shareable.

```python
from faker import Faker
fake = Faker()
print(fake.name())     # → "Sarah Mitchell"
print(fake.email())    # → "john.doe@example.com"
print(fake.company())  # → "Reynolds and Sons"
```

### What is Parquet format?

Parquet is a column-oriented file format widely used in data engineering. Compared to CSV:

- **Smaller size**: Parquet compresses much better than CSV, often 5–10x smaller
- **Typed columns**: Every column has a declared data type (string, int, float, date) so readers don't have to guess
- **Faster reads**: Analytics tools can read only the columns they need, skipping the rest
- **Industry standard**: Glue, Spark, Athena, and most data tools read Parquet natively

We use `pandas` + `pyarrow` to write Parquet files:

```python
import pandas as pd
df = pd.DataFrame(records)
df.to_parquet("output.parquet", engine="pyarrow", index=False)
```

### What is CDC (Change Data Capture)?

CDC is the practice of capturing every change that happens to a dataset — inserts of new records, updates to existing ones, and deletes. Real production databases emit CDC streams. Our generator simulates this with an `operation` column (INSERT/UPDATE/DELETE) and an `updated_at` timestamp.

Two modes:
- **Full load** (`--mode full`): all records are `operation=INSERT`, representing the initial snapshot of a table
- **Incremental** (`--mode incremental`): a mix of 20% INSERT, 60% UPDATE, 20% DELETE, representing ongoing changes

## Prerequisites

- Task 1 complete — S3 landing bucket exists
- Python packages installed: `pip install -r requirements.txt`

## Run the Generators

```bash
# Generate sales CSV (1000 rows)
python data-generator/generate_sales.py --rows 1000

# Generate users JSON (1000 rows)
python data-generator/generate_users.py --rows 1000

# Generate transactions Parquet (1000 rows)
python data-generator/generate_transactions.py --rows 1000

# Generate CDC full-load data (200 rows, all INSERT)
python data-generator/generate_cdc_data.py --rows 200 --mode full

# Generate CDC incremental data (50 rows, mixed operations)
python data-generator/generate_cdc_data.py --rows 50 --mode incremental

# Upload all generated files to S3
python data-generator/upload_to_s3.py --bucket etl-course-yourname-landing
```

All files are saved to `data-generator/output/` with a timestamp in the filename (e.g., `sales_20240115_120000.csv`). The directory is created automatically if it doesn't exist.

## What Gets Uploaded Where

The upload script maps each file to a prefix in S3 based on its filename pattern:

```
data-generator/output/sales_20240115_120000.csv
    → s3://etl-course-yourname-landing/sales/sales_20240115_120000.csv

data-generator/output/users_20240115_120001.json
    → s3://etl-course-yourname-landing/users/users_20240115_120001.json

data-generator/output/transactions_20240115_120002.parquet
    → s3://etl-course-yourname-landing/transactions/transactions_20240115_120002.parquet

data-generator/output/cdc_full_20240115_120003.csv
    → s3://etl-course-yourname-landing/cdc/cdc_full_20240115_120003.csv
```

## Sales Schema

| Column | Type | Example |
|--------|------|---------|
| `order_id` | UUID string | `3f7a2b1c-...` |
| `product_name` | string | `Wireless Headphones` |
| `category` | string | `Electronics` |
| `quantity` | int (1–20) | `3` |
| `unit_price` | float (1.00–500.00) | `49.99` |
| `total_price` | float | `149.97` |
| `customer_id` | UUID string | `a1b2c3d4-...` |
| `order_timestamp` | ISO 8601 string | `2024-01-15T10:30:00Z` |
| `region` | string | `us-east` |

## Users Schema

| Column | Type | Example |
|--------|------|---------|
| `user_id` | UUID string | `9f8e7d6c-...` |
| `first_name` | string | `Sarah` |
| `last_name` | string | `Mitchell` |
| `email` | string | `sarah.mitchell@example.com` |
| `country` | string | `United States` |
| `signup_date` | ISO date string | `2023-03-22` |
| `is_active` | boolean | `true` |

80% of users are active (`is_active = true`).

## Transactions Schema

| Column | Type | Example |
|--------|------|---------|
| `txn_id` | UUID string | `c4d5e6f7-...` |
| `user_id` | UUID string | `a1b2c3d4-...` |
| `amount` | float (5.00–2000.00) | `249.50` |
| `currency` | string | `USD` |
| `status` | string | `completed` |
| `txn_date` | ISO date string | `2024-01-10` |
| `merchant` | string | `Reynolds and Sons` |

Status distribution: 70% completed, 20% pending, 10% failed.

## CDC Schema

All sales columns plus:

| Column | Type | Example |
|--------|------|---------|
| `operation` | string | `INSERT` |
| `updated_at` | ISO 8601 string | `2024-01-15T11:00:00Z` |

## Verify in S3 Console

1. Go to **AWS Console → S3**
2. Click your landing bucket (`etl-course-yourname-landing`)
3. You should see four folders: `sales/`, `users/`, `transactions/`, `cdc/`
4. Open each folder and confirm files exist with non-zero sizes
5. Click a CSV file → **Download** → open in a spreadsheet to inspect the data

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `ModuleNotFoundError: No module named 'faker'` | Packages not installed | Run `pip install -r requirements.txt` |
| `NoCredentialsError` | AWS CLI not configured | Run `aws configure` |
| `NoSuchBucket` | Landing bucket doesn't exist | Complete Task 1 first |
| `ParquetFile read error` | Mismatched pyarrow versions | Run `pip install pyarrow==14.0.2` |
| Files not appearing in S3 | Upload script ran on empty output dir | Run the generators first, then upload |

## Cost Impact

These scripts run entirely locally and write files to your machine. The only AWS cost is the S3 PUT requests when uploading (~5 requests = $0.00 at free tier rates) and a few KB of storage. Effectively **$0.00**.

## Next Step

→ Read `docs/03a-glue-simple-etl-guide.md` to register and run your first AWS Glue ETL job on this data.
