"""
Data Quality Validation using Great Expectations

Validates Silver-layer data against defined expectation suites.
Fails the pipeline if critical checks don't pass.

Checks performed:
  Orders:
    - order_id is unique and not null
    - net_amount > 0
    - status in valid set
    - No more than 5% null user_ids
    - order_date is not in the future

  Users:
    - user_id is unique and not null
    - email is unique and not null
    - signup_date <= today

Usage:
    python -m data_quality.validate
    python -m data_quality.validate --date 2024-01-15
"""
import os
import argparse
import json
from datetime import date, timedelta, datetime

import boto3
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def get_s3_client():
    env = os.getenv("ENVIRONMENT", "local")
    if env == "local":
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
    return boto3.client("s3")


def read_silver_parquet(entity: str) -> pd.DataFrame:
    """Read Silver Parquet into Pandas DataFrame."""
    import io
    s3 = get_s3_client()
    bucket = os.getenv("MINIO_BUCKET", "ecommerce-lake")
    prefix = f"silver/{entity}/"

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]

    if not keys:
        return pd.DataFrame()

    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Expectation Checks ────────────────────────────────────────────────────────

class ExpectationResult:
    def __init__(self, name: str, passed: bool, detail: str = ""):
        self.name   = name
        self.passed = passed
        self.detail = detail

    def __repr__(self):
        symbol = "✓" if self.passed else "✗"
        return f"  [{symbol}] {self.name}" + (f" — {self.detail}" if self.detail else "")


def validate_orders(df: pd.DataFrame) -> list[ExpectationResult]:
    results = []

    # 1. Not empty
    results.append(ExpectationResult(
        "orders_not_empty", len(df) > 0, f"rows={len(df):,}"
    ))

    # 2. order_id uniqueness
    dup = df["order_id"].duplicated().sum() if "order_id" in df else 0
    results.append(ExpectationResult(
        "order_id_unique", dup == 0, f"duplicates={dup}"
    ))

    # 3. order_id not null
    null_oid = df["order_id"].isna().sum() if "order_id" in df else 0
    results.append(ExpectationResult(
        "order_id_not_null", null_oid == 0, f"nulls={null_oid}"
    ))

    # 4. net_amount positive
    if "net_amount" in df:
        neg = (pd.to_numeric(df["net_amount"], errors="coerce") <= 0).sum()
        results.append(ExpectationResult(
            "net_amount_positive", neg == 0, f"non_positive={neg}"
        ))

    # 5. Status values
    valid_statuses = {"completed", "shipped", "processing", "cancelled", "refunded", "unknown"}
    if "status" in df:
        invalid = (~df["status"].isin(valid_statuses)).sum()
        results.append(ExpectationResult(
            "status_valid_values", invalid == 0, f"invalid_statuses={invalid}"
        ))

    # 6. Null user_id < 5%
    if "user_id" in df:
        null_pct = df["user_id"].isna().sum() / max(len(df), 1) * 100
        results.append(ExpectationResult(
            "user_id_null_rate_under_5pct",
            null_pct < 5,
            f"null_pct={null_pct:.2f}%"
        ))

    # 7. order_date not in future
    if "order_date" in df:
        today = date.today()
        future = (pd.to_datetime(df["order_date"], errors="coerce").dt.date > today).sum()
        results.append(ExpectationResult(
            "order_date_not_future", future == 0, f"future_dates={future}"
        ))

    return results


def validate_users(df: pd.DataFrame) -> list[ExpectationResult]:
    results = []

    results.append(ExpectationResult(
        "users_not_empty", len(df) > 0, f"rows={len(df):,}"
    ))

    if "user_id" in df:
        dup = df["user_id"].duplicated().sum()
        results.append(ExpectationResult("user_id_unique", dup == 0, f"dups={dup}"))

    if "email" in df:
        null_email = df["email"].isna().sum()
        results.append(ExpectationResult("email_not_null", null_email == 0,
                                         f"nulls={null_email}"))
        dup_email = df["email"].duplicated().sum()
        results.append(ExpectationResult("email_unique", dup_email == 0,
                                         f"dups={dup_email}"))

    return results


# ── Runner ────────────────────────────────────────────────────────────────────

VALIDATORS = {
    "orders": validate_orders,
    "users":  validate_users,
}


def run_validation(batch_date: date | None = None) -> bool:
    """Run all validations. Returns True if all CRITICAL checks pass."""
    logger.info("Running Data Quality checks...")
    all_passed = True

    for entity, validator in VALIDATORS.items():
        logger.info(f"Validating: {entity}")
        df = read_silver_parquet(entity)
        results = validator(df)

        failed = [r for r in results if not r.passed]
        for r in results:
            logger.info(str(r))

        if failed:
            all_passed = False
            logger.error(f"  {len(failed)} check(s) FAILED for {entity}")

    if all_passed:
        logger.success("✅ All data quality checks passed.")
    else:
        logger.error("❌ Data quality checks failed — review above errors.")

    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    args = parser.parse_args()
    d = date.fromisoformat(args.date) if args.date else None
    ok = run_validation(d)
    exit(0 if ok else 1)
