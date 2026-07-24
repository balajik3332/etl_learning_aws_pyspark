"""
tests/test_glue_transformations.py
------------------------------------
Unit tests for the transformation logic in glue-jobs/simple-etl/glue_simple_etl.py.

These tests use a LOCAL PySpark session (no Glue context, no AWS credentials
needed). The transformation logic is extracted into pure Spark functions and
applied to small in-memory DataFrames so tests run quickly on any machine.

Why local PySpark?
  The actual Glue job uses awsglue libraries that are only available on the
  Glue runtime. We can't run those locally. However, the transformation logic
  (filtering, casting, column derivation) is pure PySpark and works identically
  whether it runs inside Glue or locally.

HOW TO RUN:
    pytest tests/test_glue_transformations.py -v

REQUIREMENTS:
    pip install pyspark==3.4.2 pytest==7.4.4
"""

import pytest

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    DateType,
    StringType,
    StructField,
    StructType,
)


# ── Shared SparkSession fixture ───────────────────────────────────────────────
# scope="session" means one SparkSession is created for the entire test run
# and reused across all test classes. Starting Spark is expensive (~5 seconds);
# reusing it makes the test suite much faster.

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Create a local SparkSession for unit testing.

    local[1] means 'run Spark locally with 1 thread' — single-threaded,
    no parallelism, no cluster needed. Perfect for unit tests.
    """
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_glue_transformations")
        # Suppress noisy INFO logs during tests; change to INFO if debugging
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


# ── Transformation helpers ────────────────────────────────────────────────────
# These functions mirror the transformation steps in glue_simple_etl.py.
# By isolating each transformation we can test it independently and also
# reuse the functions in the Glue script if desired.

def apply_transformations(df: DataFrame) -> DataFrame:
    """
    Apply all five transformation steps from the Glue ETL script.

    This is the same logic that runs inside the Glue job, written in pure
    PySpark so it can be tested locally without any Glue libraries.

    Steps:
      1. Filter out rows where order_id is null
      2. Cast unit_price to DoubleType
      3. Cast quantity to IntegerType
      4. Add order_date column extracted from order_timestamp using to_date()
      5. Add revenue column = quantity * unit_price
    """
    # Step 1: Drop rows where order_id is null
    df = df.filter(col("order_id").isNotNull())

    # Step 2: Cast unit_price string → double
    df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))

    # Step 3: Cast quantity string → integer
    df = df.withColumn("quantity", col("quantity").cast(IntegerType()))

    # Step 4: Extract date part from timestamp string
    df = df.withColumn("order_date", to_date(col("order_timestamp")))

    # Step 5: Compute revenue
    df = df.withColumn("revenue", col("quantity") * col("unit_price"))

    return df


def make_sales_df(spark: SparkSession, rows: list) -> DataFrame:
    """
    Helper that creates a sales DataFrame with the same schema as the
    CSV produced by data-generator/generate_sales.py.

    All columns start as StringType to match what you would get when reading
    a CSV file — exactly like the Glue job does before any casting.
    """
    schema = StructType([
        StructField("order_id",        StringType(), True),
        StructField("product_name",    StringType(), True),
        StructField("category",        StringType(), True),
        StructField("quantity",        StringType(), True),  # string until cast
        StructField("unit_price",      StringType(), True),  # string until cast
        StructField("total_price",     StringType(), True),
        StructField("customer_id",     StringType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("region",          StringType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)


# ── Test data ─────────────────────────────────────────────────────────────────

VALID_ROW = (
    "abc-123",          # order_id
    "Widget Pro",       # product_name
    "Electronics",      # category
    "5",                # quantity
    "19.99",            # unit_price
    "99.95",            # total_price
    "cust-001",         # customer_id
    "2026-05-19T22:45:06Z",  # order_timestamp
    "us-east",          # region
)

NULL_ORDER_ID_ROW = (
    None,               # order_id — should be filtered out
    "Mystery Item",
    "Food",
    "3",
    "5.00",
    "15.00",
    "cust-002",
    "2026-06-01T10:00:00Z",
    "eu-west",
)

SECOND_VALID_ROW = (
    "def-456",
    "Gadget Mini",
    "Electronics",
    "3",
    "49.50",
    "148.50",
    "cust-003",
    "2026-07-04T08:30:00Z",
    "ap-southeast",
)


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestNullOrderIdFiltered:
    """Rows with a null order_id must be dropped by the filter step."""

    def test_null_order_id_filtered(self, spark):
        """
        Given a DataFrame with one valid row and one row whose order_id is null,
        after applying transformations only the valid row should remain.
        """
        df = make_sales_df(spark, [VALID_ROW, NULL_ORDER_ID_ROW])
        assert df.count() == 2, "Should start with 2 rows"

        result = apply_transformations(df)

        assert result.count() == 1, (
            f"Expected 1 row after filtering null order_ids, got {result.count()}"
        )

    def test_all_null_order_ids_removed(self, spark):
        """All rows with null order_id are dropped, leaving an empty DataFrame."""
        df = make_sales_df(spark, [NULL_ORDER_ID_ROW, NULL_ORDER_ID_ROW])
        result = apply_transformations(df)

        assert result.count() == 0, (
            "All rows had null order_id — result should be empty"
        )

    def test_no_null_order_ids_unchanged(self, spark):
        """A DataFrame with no null order_ids should keep all its rows."""
        df = make_sales_df(spark, [VALID_ROW, SECOND_VALID_ROW])
        result = apply_transformations(df)

        assert result.count() == 2, (
            "No rows had null order_id — count should be unchanged"
        )

    def test_surviving_row_has_correct_order_id(self, spark):
        """The row that survives the null filter must have the expected order_id value."""
        df = make_sales_df(spark, [VALID_ROW, NULL_ORDER_ID_ROW])
        result = apply_transformations(df)

        order_ids = [r["order_id"] for r in result.collect()]
        assert "abc-123" in order_ids, (
            f"Expected 'abc-123' to survive the filter, got: {order_ids}"
        )


class TestPriceCastToDouble:
    """unit_price column must be DoubleType after transformations."""

    def test_price_cast_to_double(self, spark):
        """
        After transformations the unit_price field must be DoubleType,
        not StringType.
        """
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        price_type = dict(result.dtypes)["unit_price"]
        assert price_type == "double", (
            f"Expected unit_price to be 'double', got '{price_type}'"
        )

    def test_price_value_preserved_after_cast(self, spark):
        """The numeric value of unit_price must be preserved after the cast."""
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        row = result.first()
        assert row["unit_price"] == pytest.approx(19.99, rel=1e-6), (
            f"Expected unit_price to be 19.99, got {row['unit_price']}"
        )

    def test_price_schema_field_is_double_type(self, spark):
        """The schema StructField for unit_price must be DoubleType."""
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        field = {f.name: f.dataType for f in result.schema.fields}["unit_price"]
        assert isinstance(field, DoubleType), (
            f"Expected DoubleType schema, got {type(field).__name__}"
        )


class TestQuantityCastToInt:
    """quantity column must be IntegerType after transformations."""

    def test_quantity_cast_to_int(self, spark):
        """
        After transformations the quantity field must be IntegerType,
        not StringType.
        """
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        qty_type = dict(result.dtypes)["quantity"]
        assert qty_type == "int", (
            f"Expected quantity to be 'int', got '{qty_type}'"
        )

    def test_quantity_value_preserved_after_cast(self, spark):
        """The numeric value of quantity must be preserved after the cast."""
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        row = result.first()
        assert row["quantity"] == 5, (
            f"Expected quantity to be 5, got {row['quantity']}"
        )

    def test_quantity_schema_field_is_integer_type(self, spark):
        """The schema StructField for quantity must be IntegerType."""
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        field = {f.name: f.dataType for f in result.schema.fields}["quantity"]
        assert isinstance(field, IntegerType), (
            f"Expected IntegerType schema, got {type(field).__name__}"
        )


class TestRevenueComputedCorrectly:
    """revenue column must equal quantity * unit_price."""

    def test_revenue_computed_correctly(self, spark):
        """
        revenue = quantity * unit_price.
        For VALID_ROW: quantity=5, unit_price=19.99 → revenue=99.95
        """
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        row = result.first()
        expected_revenue = 5 * 19.99  # = 99.95
        assert row["revenue"] == pytest.approx(expected_revenue, rel=1e-6), (
            f"Expected revenue={expected_revenue}, got {row['revenue']}"
        )

    def test_revenue_column_exists(self, spark):
        """The revenue column must be present in the output DataFrame."""
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        assert "revenue" in result.columns, (
            f"Expected 'revenue' column in output. Columns: {result.columns}"
        )

    def test_revenue_is_double_type(self, spark):
        """revenue must be a numeric (double) column."""
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        revenue_type = dict(result.dtypes)["revenue"]
        assert revenue_type == "double", (
            f"Expected revenue type to be 'double', got '{revenue_type}'"
        )

    def test_revenue_for_multiple_rows(self, spark):
        """Revenue computation must be correct for every row in the DataFrame."""
        df = make_sales_df(spark, [VALID_ROW, SECOND_VALID_ROW])
        result = apply_transformations(df)

        rows = {r["order_id"]: r for r in result.collect()}

        # abc-123: 5 * 19.99 = 99.95
        assert rows["abc-123"]["revenue"] == pytest.approx(5 * 19.99, rel=1e-6)
        # def-456: 3 * 49.50 = 148.50
        assert rows["def-456"]["revenue"] == pytest.approx(3 * 49.50, rel=1e-6)


class TestOrderDateExtracted:
    """order_date must be a DateType column extracted from order_timestamp."""

    def test_order_date_extracted(self, spark):
        """
        After transformations the order_date column must exist and be DateType.
        """
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        assert "order_date" in result.columns, (
            "Expected 'order_date' column to exist in output"
        )
        date_type = dict(result.dtypes)["order_date"]
        assert date_type == "date", (
            f"Expected order_date type to be 'date', got '{date_type}'"
        )

    def test_order_date_schema_field_is_date_type(self, spark):
        """The schema StructField for order_date must be DateType."""
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        field = {f.name: f.dataType for f in result.schema.fields}["order_date"]
        assert isinstance(field, DateType), (
            f"Expected DateType schema, got {type(field).__name__}"
        )

    def test_order_date_value_correct(self, spark):
        """
        order_timestamp='2026-05-19T22:45:06Z' → order_date='2026-05-19'.
        The date should strip the time component correctly.
        """
        from datetime import date

        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        row = result.first()
        # PySpark returns datetime.date objects for DateType columns
        assert row["order_date"] == date(2026, 5, 19), (
            f"Expected order_date=2026-05-19, got {row['order_date']}"
        )

    def test_order_date_for_multiple_rows(self, spark):
        """order_date extraction must work correctly for every row."""
        from datetime import date

        df = make_sales_df(spark, [VALID_ROW, SECOND_VALID_ROW])
        result = apply_transformations(df)

        rows = {r["order_id"]: r for r in result.collect()}

        # VALID_ROW timestamp: "2026-05-19T22:45:06Z" → 2026-05-19
        assert rows["abc-123"]["order_date"] == date(2026, 5, 19), (
            f"Expected 2026-05-19, got {rows['abc-123']['order_date']}"
        )
        # SECOND_VALID_ROW timestamp: "2026-07-04T08:30:00Z" → 2026-07-04
        assert rows["def-456"]["order_date"] == date(2026, 7, 4), (
            f"Expected 2026-07-04, got {rows['def-456']['order_date']}"
        )

    def test_order_timestamp_column_still_present(self, spark):
        """
        The original order_timestamp column should still exist in the output
        (we add order_date, we do not replace order_timestamp).
        """
        df = make_sales_df(spark, [VALID_ROW])
        result = apply_transformations(df)

        assert "order_timestamp" in result.columns, (
            "order_timestamp should still be present after adding order_date"
        )
