"""
generate_transactions.py — Synthetic transactions generator (Parquet format)

Usage:
    python data-generator/generate_transactions.py --rows 1000
    python data-generator/generate_transactions.py --rows 5000 --output /tmp/transactions.parquet
"""

import argparse
import os
import random
from datetime import datetime, timezone

import pandas as pd
from faker import Faker

fake = Faker()

CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD"]
STATUSES = ["completed", "pending", "failed", "refunded"]
STATUS_WEIGHTS = [0.75, 0.10, 0.10, 0.05]

MERCHANTS = [
    "Amazon", "Walmart", "Target", "Best Buy", "Apple Store",
    "Nike", "Starbucks", "McDonald's", "Uber", "Airbnb",
]


def generate_transactions(num_rows: int, output_path: str) -> str:
    """Generate num_rows transaction records and write to Parquet."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    records = []
    for i in range(1, num_rows + 1):
        txn_date = fake.date_time_between(
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
        )
        records.append({
            "txn_id": f"TXN-{i:08d}",
            "user_id": f"USR-{random.randint(1, 50000):07d}",
            "amount": round(random.uniform(0.99, 9999.99), 2),
            "currency": random.choice(CURRENCIES),
            "status": random.choices(STATUSES, STATUS_WEIGHTS)[0],
            "txn_date": txn_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "merchant": random.choice(MERCHANTS),
        })

    df = pd.DataFrame(records)
    df.to_parquet(output_path, index=False, engine="pyarrow")

    print(f"[OK] Generated {num_rows} transaction rows → {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transactions Parquet data")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate (default: 1000)")
    parser.add_argument("--output", type=str, default=None, help="Output file path")
    args = parser.parse_args()

    if args.output:
        output_path = args.output
    else:
        os.makedirs(os.path.join(os.path.dirname(__file__), "output"), exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(os.path.dirname(__file__), "output", f"transactions_{timestamp}.parquet")

    generate_transactions(args.rows, output_path)


if __name__ == "__main__":
    main()
