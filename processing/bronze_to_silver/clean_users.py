"""
PySpark Job: Bronze → Silver — Users & Products

Cleans and standardises user and product records from Bronze.
"""
import os
import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def build_spark(app_name: str) -> SparkSession:
    minio_endpoint   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint",          minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",        minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key",        minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .master("local[*]")
        .getOrCreate()
    )


def clean_users(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("created_at",    F.to_timestamp("created_at"))
        .withColumn("signup_date",   F.to_date("signup_date"))
        .withColumn("email",         F.lower(F.trim(F.col("email"))))
        .withColumn("country",       F.upper(F.trim(F.col("country"))))
        .withColumn("full_name",     F.initcap(F.trim(F.col("full_name"))))
        .withColumn("days_since_signup",
                    F.datediff(F.current_date(), F.col("signup_date")))
        .withColumn("cohort_month",
                    F.date_format(F.col("signup_date"), "yyyy-MM"))
        .filter(F.col("user_id").isNotNull() & F.col("email").isNotNull())
        .dropDuplicates(["user_id"])
        .withColumn("_processed_at", F.current_timestamp())
    )


def clean_products(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("created_at",  F.to_timestamp("created_at"))
        .withColumn("price",       F.col("price").cast("decimal(10,2)"))
        .withColumn("cost",        F.col("cost").cast("decimal(10,2)"))
        .withColumn("margin_pct",
                    F.round(
                        (F.col("price") - F.col("cost")) / F.col("price") * 100, 2
                    ))
        .withColumn("category",    F.trim(F.col("category")))
        .withColumn("subcategory", F.trim(F.col("subcategory")))
        .filter(F.col("product_id").isNotNull() & (F.col("price") > 0))
        .dropDuplicates(["product_id"])
        .withColumn("_processed_at", F.current_timestamp())
    )


def run_users(batch_date: date):
    spark = build_spark("bronze-to-silver-users")
    try:
        in_path  = (f"s3a://ecommerce-lake/bronze/users/"
                    f"year={batch_date.year}/month={batch_date.month:02d}/day={batch_date.day:02d}/")
        out_path = "s3a://ecommerce-lake/silver/users"
        df = clean_users(spark.read.json(in_path))
        df.write.mode("overwrite").parquet(out_path)
        logger.success(f"✓ Silver users ({df.count():,} rows) → {out_path}")
    finally:
        spark.stop()


def run_products(batch_date: date):
    spark = build_spark("bronze-to-silver-products")
    try:
        in_path  = (f"s3a://ecommerce-lake/bronze/products/"
                    f"year={batch_date.year}/month={batch_date.month:02d}/day={batch_date.day:02d}/")
        out_path = "s3a://ecommerce-lake/silver/products"
        df = clean_products(spark.read.json(in_path))
        df.write.mode("overwrite").parquet(out_path)
        logger.success(f"✓ Silver products ({df.count():,} rows) → {out_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--entity", choices=["users", "products", "all"], default="all")
    args = parser.parse_args()
    d = date.fromisoformat(args.date)
    if args.entity in ("users", "all"):
        run_users(d)
    if args.entity in ("products", "all"):
        run_products(d)
