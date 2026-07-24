"""
generate_sales.py — Synthetic sales data generator (CSV format)

Usage:
    python data-generator/generate_sales.py --rows 1000
    python data-generator/generate_sales.py --rows 5000 --output /tmp/sales.csv
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

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data-generator", "output")


def generate_sales_row(order_id: int) -> dict:
    """Generate one sales record with realistic values."""
    category = random.choice(CATEGORIES)
    quantity = random.randint(1, 20)
    unit_price = round(random.uniform(2.99, 499.99), 2)
    total_price = round(quantity * unit_price, 2)
    order_ts = fake.date_time_between(
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
    )
    return {
        "order_id": f"ORD-{order_id:07d}",
        "product_name": fake.catch_phrase(),
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": total_price,
        "customer_id": f"CUST-{random.randint(1, 50000):06d}",
        "order_timestamp": order_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": random.choice(REGIONS),
    }


def generate_sales(num_rows: int, output_path: str) -> str:
    """Generate num_rows sales records and write to a CSV file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        "order_id", "product_name", "category", "quantity",
        "unit_price", "total_price", "customer_id", "order_timestamp", "region",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(1, num_rows + 1):
            writer.writerow(generate_sales_row(i))

    print(f"[OK] Generated {num_rows} sales rows → {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sales CSV data")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate (default: 1000)")
    parser.add_argument("--output", type=str, default=None, help="Output file path")
    args = parser.parse_args()

    if args.output:
        output_path = args.output
    else:
        os.makedirs(os.path.join(os.path.dirname(__file__), "output"), exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(os.path.dirname(__file__), "output", f"sales_{timestamp}.csv")

    generate_sales(args.rows, output_path)


if __name__ == "__main__":
    main()
