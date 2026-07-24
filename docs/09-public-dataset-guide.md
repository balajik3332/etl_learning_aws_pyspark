# Task 9 — Public Dataset Integration (NYC Taxi Data)

## What Are We Building?

We're applying all the ETL patterns built in Tasks 3–8 to a real-world dataset: **NYC Taxi Trip Records**. This is the most important task in the course because real data is messy in ways synthetic data never is.

---

## Concepts Explained

### Why Use Real Data?
Synthetic data is clean and predictable. Real data has:
- Negative values where they shouldn't be (negative fares, negative distances)
- Nulls in critical columns
- Outliers (a taxi trip with 100 passengers, a fare of $10,000)
- Inconsistent formats across months
- Schema changes between dataset versions

These are exactly the problems you'll encounter on the job.

### What is the NYC Taxi Dataset?
New York City publishes monthly records of every yellow and green taxi trip — pickup/dropoff times, distance, number of passengers, fare amount, tip amount, and payment type. The data goes back to 2009. It is free, widely used for data engineering learning, and available in Parquet format from a public S3 bucket.

**Schema (key columns):**

| Column | Type | Notes |
|--------|------|-------|
| `tpep_pickup_datetime` | timestamp | Trip start time |
| `tpep_dropoff_datetime` | timestamp | Trip end time |
| `passenger_count` | float | Can be null! |
| `trip_distance` | float | Some trips = 0.0 miles |
| `fare_amount` | float | Some values are negative (refunds/errors) |
| `tip_amount` | float | Always >= 0 |
| `total_amount` | float | Fare + tip + tolls + surcharges |
| `payment_type` | int | 1=Credit, 2=Cash, 3=No charge, 4=Dispute |
| `PULocationID` | int | Pickup zone (1-265) |
| `DOLocationID` | int | Dropoff zone (1-265) |

---

## Prerequisites

- [ ] Tasks 3A/3B complete (ETL jobs working on synthetic data)
- [ ] Tasks 7A/7B complete (multi-step pipeline working)
- [ ] S3 buckets exist

---

## Step 1 — Fetch the Dataset

```bash
python public-datasets/fetch_nyc_taxi.py \
  --month 2023-01 \
  --sample 50000 \
  --bucket etl-course-yourname-landing
```

This script:
1. Downloads the January 2023 Parquet file from the public NYC TLC S3 bucket
2. Samples 50,000 rows (the full file has ~3M rows — too large and too costly for learning)
3. Uploads the sample to your landing bucket at `s3://.../nyc-taxi/2023-01/sample.parquet`

**Expected output:**
```
[INFO] Downloading: s3://nyc-tlc/trip data/yellow_tripdata_2023-01.parquet
[INFO] Full file: 3,066,766 rows
[INFO] Sampled: 50,000 rows
[INFO] Uploaded to: s3://etl-course-yourname-landing/nyc-taxi/2023-01/sample.parquet
```

---

## Step 2 — Explore the Data First

Before running ETL, always explore:

```python
import pandas as pd

df = pd.read_parquet("nyc_taxi_sample.parquet")
print(df.shape)           # (50000, 19)
print(df.dtypes)          # column types
print(df.describe())      # min/max/mean for numeric columns
print(df.isnull().sum())  # null counts per column
```

**What you'll discover:**
- `passenger_count` has ~6,000 nulls
- `fare_amount` has some negative values (about 200 rows)
- `trip_distance` = 0.0 for about 500 rows
- `total_amount` can be as high as $4,000 (outliers)

---

## Step 3 — Run the Simple ETL on Real Data

```bash
aws glue start-job-run \
  --job-name glue-simple-etl \
  --arguments '{
    "--source_path": "s3://etl-course-yourname-landing/nyc-taxi/2023-01/",
    "--output_path": "s3://etl-course-yourname-output/nyc-taxi/glue/simple-etl/"
  }'
```

What happens? The simple ETL job from Task 3A was designed for the sales schema. Running it on taxi data will likely produce:
- Schema mismatch errors (different column names)
- Type casting failures (some columns behave differently)

**Learning moment**: this is expected and intentional. Real-world ETL requires schema-aware code.

---

## Step 4 — Run the Multi-Step Pipeline on Real Data

This is where the pipeline earns its complexity. Run the Glue Workflow with the taxi data as input:

```bash
aws glue start-workflow-run --name etl-multi-step-workflow
```

With the NYC taxi data, each stage handles real problems:

**Stage 2 (Clean):**
- Drops rows where `fare_amount < 0` (refunds and errors)
- Drops rows where `trip_distance == 0`
- Imputes `passenger_count` nulls with 1
- Filters out extreme outliers: `total_amount > 500`

**Stage 3 (Enrich):**
- Extracts `pickup_hour` from `tpep_pickup_datetime`
- Adds `time_of_day_bucket`:
  - `morning` = 6–10 AM
  - `afternoon` = 10 AM–4 PM
  - `evening` = 4–9 PM
  - `night` = 9 PM–6 AM
- Adds `trip_duration_minutes` = dropoff - pickup (in minutes)

**Stage 4 (Aggregate):**
- Groups by `pickup_hour` and `time_of_day_bucket`
- Computes:
  - `avg_fare` = average fare amount
  - `avg_distance` = average trip distance
  - `total_trips` = count of trips
  - `avg_tip_pct` = average tip as % of fare

---

## Step 5 — View the Final Analytical Result

```bash
# Download and view the aggregate output
aws s3 cp \
  s3://etl-course-yourname-output/nyc-taxi/aggregate/ \
  /tmp/taxi-aggregate/ --recursive

python -c "
import pandas as pd, glob
files = glob.glob('/tmp/taxi-aggregate/*.parquet')
df = pd.concat([pd.read_parquet(f) for f in files])
print(df.sort_values('pickup_hour').to_string())
"
```

**Expected result — average NYC taxi fare by hour:**

| pickup_hour | time_of_day_bucket | avg_fare | avg_distance | total_trips |
|------------|-------------------|---------|-------------|------------|
| 0 | night | $14.20 | 2.8 mi | 1,842 |
| 7 | morning | $18.50 | 3.2 mi | 3,210 |
| 12 | afternoon | $16.40 | 2.6 mi | 2,980 |
| 18 | evening | $21.30 | 3.8 mi | 4,120 |

---

## What Does Real Data Teach You That Synthetic Data Doesn't?

| Lesson | Synthetic Data | Real Data |
|--------|---------------|-----------|
| Schema consistency | Always perfect | Changes between file versions |
| Null handling | Controlled, predictable | Unpredictable patterns |
| Outliers | None | Many (e.g., $10,000 fares) |
| Data volume | You control it | You have to sample it |
| Business meaning | None | Requires domain knowledge |
| Error recovery | Not needed | Essential |

---

## Common Errors and Fixes

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `NoSuchKey` on download | Month not available yet | Use `2023-01` through `2023-12` |
| ETL job fails on schema | Column names different from sales schema | Taxi data uses `tpep_pickup_datetime`, not `order_timestamp` |
| Aggregate is empty | Clean stage filtered all rows | Review filter thresholds — check `fare_amount` range |
| `OutOfMemoryError` in Spark | Too many rows loaded | Reduce sample size: `--sample 10000` |
| Download very slow | Full Parquet file is ~100MB | Use `--sample` flag to limit rows after download |

---

## Cost Impact

- One pipeline run on 50k rows ≈ $0.15–$0.25
- Data download from public S3 bucket: free (same region)
- **Estimated cost for this task: $0.20–$0.40** for a few runs

---

## Next Step

→ `docs/10-comparison-and-cleanup.md` — Review what you built, compare Glue vs EMR with real numbers, then delete all AWS resources to stop billing.
