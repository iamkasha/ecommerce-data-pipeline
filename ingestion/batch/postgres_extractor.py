"""
Batch Extractor: PostgreSQL (RDS) → S3 Bronze Layer

Extracts incremental data from source tables and writes
date-partitioned JSON/Parquet files to the Bronze S3 layer.

Supports full-load and incremental (watermark-based) extraction.

Usage:
    python -m ingestion.batch.postgres_extractor
    python -m ingestion.batch.postgres_extractor --table orders --date 2024-01-15
"""
import os
import argparse
from datetime import date, timedelta
from typing import Iterator

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "ecommerce"),
    "user":     os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
}

S3_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
S3_BUCKET     = os.getenv("MINIO_BUCKET", "ecommerce-lake")

CHUNK_SIZE = 10_000  # rows per chunk to avoid OOM


# ── S3 Client ─────────────────────────────────────────────────────────────────
def get_s3_client():
    """Returns boto3 S3 client. Points to MinIO locally, real S3 in production."""
    env = os.getenv("ENVIRONMENT", "local")
    if env == "local":
        return boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        )
    return boto3.client("s3")  # Uses IAM role in production


# ── Extraction Queries ────────────────────────────────────────────────────────
INCREMENTAL_QUERIES = {
    "orders": """
        SELECT o.*, u.country AS user_country, u.city AS user_city
        FROM   orders o
        JOIN   users  u ON u.user_id = o.user_id
        WHERE  o.created_at::date = %(batch_date)s
        ORDER  BY o.created_at
    """,
    "order_items": """
        SELECT oi.*, o.created_at::date AS order_date
        FROM   order_items oi
        JOIN   orders o ON o.order_id = oi.order_id
        WHERE  o.created_at::date = %(batch_date)s
    """,
    "users": """
        SELECT * FROM users
        WHERE  DATE(updated_at) = %(batch_date)s
           OR  DATE(created_at) = %(batch_date)s
    """,
    "products": """
        SELECT * FROM products
        WHERE  DATE(updated_at) = %(batch_date)s
           OR  DATE(created_at) = %(batch_date)s
    """,
}

FULL_LOAD_QUERIES = {
    "users":    "SELECT * FROM users",
    "products": "SELECT * FROM products",
}


def chunked_query(conn, sql: str, params: dict) -> Iterator[pd.DataFrame]:
    """Execute a query and yield results in chunks as DataFrames."""
    with conn.cursor(name="batch_cursor", cursor_factory=RealDictCursor) as cur:
        cur.itersize = CHUNK_SIZE
        cur.execute(sql, params)
        chunk = []
        for row in cur:
            chunk.append(dict(row))
            if len(chunk) >= CHUNK_SIZE:
                yield pd.DataFrame(chunk)
                chunk = []
        if chunk:
            yield pd.DataFrame(chunk)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def upload_to_s3(s3, df: pd.DataFrame, s3_key: str, fmt: str = "json"):
    """Upload a DataFrame to S3 as JSON or Parquet."""
    if fmt == "json":
        body = df.to_json(orient="records", lines=True, date_format="iso").encode()
        content_type = "application/json"
    else:
        import io
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        body = buf.getvalue()
        content_type = "application/octet-stream"

    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=body, ContentType=content_type)
    logger.debug(f"  Uploaded {len(df)} rows → s3://{S3_BUCKET}/{s3_key}")


def extract_table(table: str, batch_date: date, full_load: bool = False):
    """
    Extract one table for a given date and upload to Bronze S3.
    Partitioned as: bronze/{table}/year=YYYY/month=MM/day=DD/data.json
    """
    s3 = get_s3_client()
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        query  = FULL_LOAD_QUERIES.get(table) if full_load else INCREMENTAL_QUERIES.get(table)
        params = {} if full_load else {"batch_date": batch_date}

        if not query:
            raise ValueError(f"No query defined for table: {table}")

        s3_prefix = (
            f"bronze/{table}/year={batch_date.year}/"
            f"month={batch_date.month:02d}/day={batch_date.day:02d}"
        )

        total_rows = 0
        part = 0
        for df in chunked_query(conn, query, params):
            # Serialise datetime columns
            for col in df.select_dtypes(include=["datetime64", "datetimetz"]).columns:
                df[col] = df[col].astype(str)

            s3_key = f"{s3_prefix}/part-{part:04d}.json"
            upload_to_s3(s3, df, s3_key)
            total_rows += len(df)
            part += 1

        logger.info(f"✓ {table}: extracted {total_rows} rows → s3://{S3_BUCKET}/{s3_prefix}/")
        return total_rows

    finally:
        conn.close()


def run_batch(batch_date: date, tables: list[str] | None = None):
    """Run extraction for all tables (or a subset) for the given date."""
    tables = tables or list(INCREMENTAL_QUERIES.keys())
    logger.info(f"Starting batch extraction for {batch_date}")
    summary = {}
    for table in tables:
        try:
            rows = extract_table(table, batch_date)
            summary[table] = rows
        except Exception as e:
            logger.error(f"  ✗ Failed to extract {table}: {e}")
            summary[table] = -1

    logger.info("Extraction summary:")
    for t, n in summary.items():
        status = "✓" if n >= 0 else "✗"
        logger.info(f"  {status} {t:<20} {n:>8} rows")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract source data to Bronze S3")
    parser.add_argument("--date",  default=str(date.today() - timedelta(days=1)),
                        help="Batch date (YYYY-MM-DD). Default: yesterday.")
    parser.add_argument("--table", default=None, help="Extract one specific table only")
    parser.add_argument("--full-load", action="store_true", help="Full load instead of incremental")
    args = parser.parse_args()

    batch_date = date.fromisoformat(args.date)
    tables = [args.table] if args.table else None
    run_batch(batch_date, tables)
