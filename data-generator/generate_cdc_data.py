"""
generate_cdc_data.py — CDC (Change Data Capture) data generator

Produces sales-schema records tagged with CDC operations (INSERT/UPDATE/DELETE)
and an updated_at timestamp, simulating incremental data arriving from a source system.

Usage:
    # Full load — all INSERTs
    python data-generator/generate_cdc_data.py --rows 1000 --mode full

    # Incremental — mix of INSERT (20%), UPDATE (60%), DELETE (20%)
    python data-generator/generate_cdc_data.py --rows 50 --mode incremental
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker()

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Food", "Beauty"]
REGIONS = ["east", "west", "north", "south", "central"]

# CDC operation distributions
FULL_WEIGHTS = {"INSERT": 1.0, "UPDATE": 0.0, "DELETE": 0.0}
INCREMENTAL_WEIGHTS = {"INSERT": 0.20, "UPDATE": 0.60, "DELETE": 0.20}


def generate_cdc_row(order_id: int, operation: str, base_ts: datetime) -> dict:
    """Generate one CDC record with the given operation type."""
    # updated_at is offset slightly from the base timestamp so each batch
    # has a distinct time window — important for checkpoint-based CDC
    jitter_seconds = random.randint(0, 3600)
    updated_at = base_ts + timedelta(seconds=jitter_seconds)

    quantity = random.randint(1, 20)
    unit_price = round(random.uniform(2.99, 499.99), 2)

    return {
        "order_id": f"ORD-{order_id:07d}",
        "product_name": fake.catch_phrase(),
        "category": random.choice(CATEGORIES),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": round(quantity * unit_price, 2),
        "customer_id": f"CUST-{random.randint(1, 50000):06d}",
        "order_timestamp": base_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": random.choice(REGIONS),
        "operation": operation,
        "updated_at": updated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def generate_cdc_data(num_rows: int, mode: str, output_path: str) -> str:
    """
    Generate CDC records.

    mode='full'        → all INSERTs, timestamps in 2024-01 window
    mode='incremental' → mixed operations, timestamps in 2024-06 window
                         (later than full load, so checkpoint filters work correctly)
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if mode == "full":
        base_ts = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
        weights = FULL_WEIGHTS
        # Full load uses a sequential range of order IDs
        order_id_start = 1
    else:
        # Incremental data comes later in time (important for timestamp-based filtering)
        base_ts = datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
        weights = INCREMENTAL_WEIGHTS
        # Incremental updates target existing order IDs (1–1000) for updates/deletes
        # and uses high IDs for new inserts
        order_id_start = 900  # overlaps with existing for UPDATE/DELETE

    operations = random.choices(
        list(weights.keys()),
        weights=list(weights.values()),
        k=num_rows,
    )

    fieldnames = [
        "order_id", "product_name", "category", "quantity", "unit_price",
        "total_price", "customer_id", "order_timestamp", "region",
        "operation", "updated_at",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i, operation in enumerate(operations, order_id_start):
            writer.writerow(generate_cdc_row(i, operation, base_ts))

    op_counts = {op: operations.count(op) for op in set(operations)}
    print(f"[OK] Generated {num_rows} CDC rows (mode={mode}) → {output_path}")
    print(f"     Operations: {op_counts}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic CDC data")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows (default: 1000)")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="'full' = all INSERTs; 'incremental' = mixed INSERT/UPDATE/DELETE",
    )
    parser.add_argument("--output", type=str, default=None, help="Output file path")
    args = parser.parse_args()

    if args.output:
        output_path = args.output
    else:
        os.makedirs(os.path.join(os.path.dirname(__file__), "output"), exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(
            os.path.dirname(__file__), "output",
            f"cdc_{args.mode}_{timestamp}.csv",
        )

    generate_cdc_data(args.rows, args.mode, output_path)


if __name__ == "__main__":
    main()
