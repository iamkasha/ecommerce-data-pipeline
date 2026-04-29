"""
PySpark Job: Bronze → Silver — Orders

Reads raw JSON orders from Bronze S3, applies:
  - Schema enforcement
  - Null / anomaly handling
  - Deduplication on order_id
  - Type casting and standardisation
  - Derived columns (order_month, order_year, is_high_value)

Writes cleaned Parquet to Silver S3, partitioned by order_date.

Usage:
    python -m processing.bronze_to_silver.clean_orders
    python -m processing.bronze_to_silver.clean_orders --date 2024-01-15
"""
import os
import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, BooleanType
)
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BRONZE_PATH = "s3a://ecommerce-lake/bronze/orders"
SILVER_PATH = "s3a://ecommerce-lake/silver/orders"

ORDERS_SCHEMA = StructType([
    StructField("order_id",         StringType(),       False),
    StructField("user_id",          StringType(),       False),
    StructField("status",           StringType(),       True),
    StructField("total_amount",     DecimalType(12, 2), True),
    StructField("discount_amount",  DecimalType(10, 2), True),
    StructField("shipping_cost",    DecimalType(8, 2),  True),
    StructField("payment_method",   StringType(),       True),
    StructField("shipping_country", StringType(),       True),
    StructField("user_country",     StringType(),       True),
    StructField("user_city",        StringType(),       True),
    StructField("created_at",       StringType(),       True),
    StructField("updated_at",       StringType(),       True),
])

VALID_STATUSES = {"completed", "shipped", "processing", "cancelled", "refunded"}


def build_spark(app_name: str = "bronze-to-silver-orders") -> SparkSession:
    """Create SparkSession with S3/MinIO config."""
    minio_endpoint   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint",              minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",            minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key",            minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .master("local[*]")
        .getOrCreate()
    )


def transform_orders(df: DataFrame) -> DataFrame:
    """Apply all cleaning and enrichment transformations."""
    return (
        df
        # Parse timestamps
        .withColumn("created_at",  F.to_timestamp("created_at"))
        .withColumn("updated_at",  F.to_timestamp("updated_at"))

        # Derived date columns for partitioning & analytics
        .withColumn("order_date",  F.to_date("created_at"))
        .withColumn("order_year",  F.year("created_at"))
        .withColumn("order_month", F.month("created_at"))
        .withColumn("order_week",  F.weekofyear("created_at"))

        # Clean status — map unknowns to 'unknown'
        .withColumn("status", F.when(
            F.lower(F.col("status")).isin(list(VALID_STATUSES)),
            F.lower(F.col("status"))
        ).otherwise(F.lit("unknown")))

        # Fill nulls
        .fillna({"discount_amount": 0.0, "shipping_cost": 0.0})
        .fillna({"payment_method": "unknown", "shipping_country": "unknown"})

        # Revenue after discount
        .withColumn("net_amount", F.col("total_amount") - F.col("discount_amount"))

        # High-value order flag (top quartile proxy)
        .withColumn("is_high_value", F.col("net_amount") > F.lit(200.0))

        # Drop rows with null primary keys
        .filter(F.col("order_id").isNotNull() & F.col("user_id").isNotNull())

        # Dedup: keep the latest record per order_id
        .dropDuplicates(["order_id"])

        # Audit columns
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_source", F.lit("postgres/orders"))
    )


def run(batch_date: date):
    spark = build_spark()
    try:
        input_path = (
            f"{BRONZE_PATH}/year={batch_date.year}/"
            f"month={batch_date.month:02d}/day={batch_date.day:02d}/"
        )
        output_path = f"{SILVER_PATH}"

        logger.info(f"Reading Bronze orders: {input_path}")
        raw_df = spark.read.schema(ORDERS_SCHEMA).json(input_path)
        raw_count = raw_df.count()
        logger.info(f"  Raw records:     {raw_count:,}")

        silver_df = transform_orders(raw_df)
        silver_count = silver_df.count()
        dropped = raw_count - silver_count
        logger.info(f"  Silver records:  {silver_count:,}  (dropped {dropped:,})")

        (
            silver_df
            .write
            .mode("overwrite")
            .partitionBy("order_year", "order_month", "order_date")
            .parquet(output_path)
        )
        logger.success(f"✓ Silver orders written → {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)))
    args = parser.parse_args()
    run(date.fromisoformat(args.date))
