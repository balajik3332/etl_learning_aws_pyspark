"""
generate_users.py — Synthetic user data generator (JSON format)

Usage:
    python data-generator/generate_users.py --rows 1000
    python data-generator/generate_users.py --rows 500 --output /tmp/users.json
"""

import argparse
import json
import os
import random
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

COUNTRIES = ["US", "UK", "CA", "AU", "DE", "FR", "IN", "BR", "JP", "MX"]


def generate_user_row(user_id: int) -> dict:
    """Generate one user record."""
    signup = fake.date_time_between(
        start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
    )
    return {
        "user_id": f"USR-{user_id:07d}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "country": random.choice(COUNTRIES),
        "signup_date": signup.strftime("%Y-%m-%d"),
        "is_active": random.random() > 0.1,  # 90% active
    }


def generate_users(num_rows: int, output_path: str) -> str:
    """Generate num_rows user records and write to a JSON file (array of objects)."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    records = [generate_user_row(i) for i in range(1, num_rows + 1)]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"[OK] Generated {num_rows} user rows → {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic users JSON data")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate (default: 1000)")
    parser.add_argument("--output", type=str, default=None, help="Output file path")
    args = parser.parse_args()

    if args.output:
        output_path = args.output
    else:
        os.makedirs(os.path.join(os.path.dirname(__file__), "output"), exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(os.path.dirname(__file__), "output", f"users_{timestamp}.json")

    generate_users(args.rows, output_path)


if __name__ == "__main__":
    main()
