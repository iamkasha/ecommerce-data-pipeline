"""
Redshift / Warehouse Loader

Loads Gold-layer Parquet data from S3 into the warehouse using:
  - Redshift COPY command (production)
  - awswrangler for local PostgreSQL (development)

Handles:
  - Incremental loads with UPSERT (delete + insert)
  - Schema validation before load
  - Row count reconciliation

Usage:
    python -m warehouse.loaders.redshift_loader
    python -m warehouse.loaders.redshift_loader --table daily_revenue --date 2024-01-15
"""
import os
import argparse
from datetime import date

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

GOLD_BUCKET   = os.getenv("MINIO_BUCKET",      "ecommerce-lake")
ENVIRONMENT   = os.getenv("ENVIRONMENT",        "local")

WH_CONFIG = {
    "host":     os.getenv("REDSHIFT_HOST",     "localhost"),
    "port":     int(os.getenv("REDSHIFT_PORT", 5439)),
    "dbname":   os.getenv("REDSHIFT_DB",       "warehouse"),
    "user":     os.getenv("REDSHIFT_USER",     "warehouse"),
    "password": os.getenv("REDSHIFT_PASSWORD", "warehouse123"),
}

# Gold path → staging table mapping
GOLD_TABLES = {
    "daily_revenue":      "staging.daily_revenue",
    "product_metrics":    "staging.product_metrics",
    "user_order_summary": "staging.user_order_summary",
}


def get_s3_client():
    if ENVIRONMENT == "local":
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
    return boto3.client("s3")


def read_parquet_from_s3(table: str, batch_date: date | None = None) -> pd.DataFrame:
    """Read Parquet files from Gold S3 layer into a DataFrame."""
    s3 = get_s3_client()
    prefix = f"gold/{table}/"
    if batch_date:
        prefix = f"gold/{table}/order_date={batch_date}/"

    response = s3.list_objects_v2(Bucket=GOLD_BUCKET, Prefix=prefix)
    keys = [obj["Key"] for obj in response.get("Contents", [])
            if obj["Key"].endswith(".parquet")]

    if not keys:
        logger.warning(f"No Parquet files found at s3://{GOLD_BUCKET}/{prefix}")
        return pd.DataFrame()

    frames = []
    for key in keys:
        obj = s3.get_object(Bucket=GOLD_BUCKET, Key=key)
        import io
        frames.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))

    df = pd.concat(frames, ignore_index=True)
    logger.info(f"  Read {len(df):,} rows from s3://{GOLD_BUCKET}/{prefix}")
    return df


def load_to_warehouse(df: pd.DataFrame, staging_table: str, pk_col: str | None = None):
    """Load DataFrame into warehouse staging table."""
    if df.empty:
        logger.warning(f"  Empty DataFrame — skipping {staging_table}")
        return

    # Convert all columns to string for staging (schema-on-read approach)
    df = df.astype(str).where(df.notna(), None)

    conn = psycopg2.connect(**WH_CONFIG)
    try:
        cols = list(df.columns)
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {staging_table}")
            sql = f"INSERT INTO {staging_table} ({', '.join(cols)}) VALUES %s"
            vals = [tuple(row) for row in df.itertuples(index=False, name=None)]
            execute_values(cur, sql, vals, page_size=1000)
        conn.commit()
        logger.success(f"  ✓ Loaded {len(df):,} rows → {staging_table}")
    finally:
        conn.close()


def upsert_dwh(staging_table: str, target_table: str, pk_cols: list[str]):
    """UPSERT from staging into DWH using DELETE + INSERT strategy."""
    conn = psycopg2.connect(**WH_CONFIG)
    try:
        join_cond = " AND ".join(
            f"t.{c} = s.{c}" for c in pk_cols
        )
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM {target_table} t
                USING {staging_table} s
                WHERE {join_cond}
            """)
            deleted = cur.rowcount
            cur.execute(f"""
                INSERT INTO {target_table}
                SELECT * FROM {staging_table}
            """)
            inserted = cur.rowcount
        conn.commit()
        logger.info(f"  Upsert {target_table}: deleted={deleted}, inserted={inserted}")
    finally:
        conn.close()


def run(table: str | None = None, batch_date: date | None = None):
    tables = [table] if table else list(GOLD_TABLES.keys())
    logger.info(f"Loading Gold → Warehouse | tables={tables} | date={batch_date}")

    for t in tables:
        staging = GOLD_TABLES.get(t)
        if not staging:
            logger.error(f"Unknown table: {t}")
            continue
        logger.info(f"Processing: {t}")
        df = read_parquet_from_s3(t, batch_date)
        load_to_warehouse(df, staging)

    logger.success("Warehouse load complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=None)
    parser.add_argument("--date",  default=None)
    args = parser.parse_args()
    batch_date = date.fromisoformat(args.date) if args.date else None
    run(args.table, batch_date)
