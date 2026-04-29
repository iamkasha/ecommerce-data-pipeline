"""
PySpark Job: Silver → Gold — Daily Business Aggregates

Reads cleaned Silver data and produces Gold-layer aggregates:
  1. daily_revenue        — Revenue KPIs per day + country
  2. product_metrics      — Sales + revenue per product per day
  3. user_order_summary   — Per-user order history (for CLV calculations)
  4. funnel_metrics       — Conversion funnel from events

All outputs written as Parquet to Gold S3, consumed by dbt & Redshift.

Usage:
    python -m processing.silver_to_gold.aggregate_daily
"""
import os
import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

SILVER = "s3a://ecommerce-lake/silver"
GOLD   = "s3a://ecommerce-lake/gold"


def build_spark() -> SparkSession:
    minio_endpoint   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName("silver-to-gold-aggregates")
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


def build_daily_revenue(orders: DataFrame) -> DataFrame:
    """Revenue KPIs aggregated by order_date and country."""
    completed = orders.filter(F.col("status") == "completed")
    return (
        completed
        .groupBy("order_date", "shipping_country")
        .agg(
            F.count("order_id")                        .alias("total_orders"),
            F.sum("net_amount")                        .alias("gross_revenue"),
            F.sum("discount_amount")                   .alias("total_discounts"),
            F.sum("shipping_cost")                     .alias("total_shipping"),
            F.avg("net_amount")                        .alias("avg_order_value"),
            F.countDistinct("user_id")                 .alias("unique_customers"),
            F.sum(F.when(F.col("is_high_value"), 1)
                  .otherwise(0))                       .alias("high_value_orders"),
        )
        .withColumn("revenue_per_customer",
                    F.col("gross_revenue") / F.col("unique_customers"))
        .withColumn("_processed_at", F.current_timestamp())
    )


def build_product_metrics(orders: DataFrame, items: DataFrame,
                          products: DataFrame) -> DataFrame:
    """Per-product sales metrics per day."""
    enriched = (
        items
        .join(orders.select("order_id", "order_date", "status", "user_id"),
              on="order_id", how="inner")
        .filter(F.col("status") == "completed")
        .join(products.select("product_id", "name", "category", "subcategory", "cost"),
              on="product_id", how="left")
    )
    return (
        enriched
        .groupBy("order_date", "product_id", "name", "category", "subcategory")
        .agg(
            F.sum("quantity")                          .alias("units_sold"),
            F.sum("total_price")                       .alias("revenue"),
            F.countDistinct("order_id")                .alias("order_count"),
            F.countDistinct("user_id")                 .alias("unique_buyers"),
            F.avg("unit_price")                        .alias("avg_selling_price"),
        )
        .withColumn("gross_profit",
                    F.col("revenue") - (F.col("units_sold") * F.col("cost")))
        .withColumn("_processed_at", F.current_timestamp())
    )


def build_user_order_summary(orders: DataFrame) -> DataFrame:
    """Per-user order history — feeds CLV and cohort analysis in dbt."""
    window = Window.partitionBy("user_id").orderBy("order_date")
    return (
        orders.filter(F.col("status") == "completed")
        .groupBy("user_id")
        .agg(
            F.count("order_id")                        .alias("total_orders"),
            F.sum("net_amount")                        .alias("total_revenue"),
            F.avg("net_amount")                        .alias("avg_order_value"),
            F.min("order_date")                        .alias("first_order_date"),
            F.max("order_date")                        .alias("last_order_date"),
            F.countDistinct("payment_method")          .alias("payment_methods_used"),
        )
        .withColumn("days_as_customer",
                    F.datediff(F.col("last_order_date"), F.col("first_order_date")))
        .withColumn("is_repeat_customer", F.col("total_orders") > 1)
        .withColumn("_processed_at", F.current_timestamp())
    )


def run():
    spark  = build_spark()
    try:
        logger.info("Loading Silver data...")
        orders   = spark.read.parquet(f"{SILVER}/orders")
        items    = spark.read.parquet(f"{SILVER}/order_items") \
                       if _path_exists(spark, f"{SILVER}/order_items") else None
        products = spark.read.parquet(f"{SILVER}/products")

        logger.info("Building Gold: daily_revenue...")
        rev_df = build_daily_revenue(orders)
        rev_df.write.mode("overwrite").partitionBy("order_date")\
              .parquet(f"{GOLD}/daily_revenue")
        logger.success(f"✓ daily_revenue ({rev_df.count():,} rows)")

        if items:
            logger.info("Building Gold: product_metrics...")
            prod_df = build_product_metrics(orders, items, products)
            prod_df.write.mode("overwrite").partitionBy("order_date")\
                   .parquet(f"{GOLD}/product_metrics")
            logger.success(f"✓ product_metrics ({prod_df.count():,} rows)")

        logger.info("Building Gold: user_order_summary...")
        user_df = build_user_order_summary(orders)
        user_df.write.mode("overwrite").parquet(f"{GOLD}/user_order_summary")
        logger.success(f"✓ user_order_summary ({user_df.count():,} rows)")

    finally:
        spark.stop()


def _path_exists(spark: SparkSession, path: str) -> bool:
    try:
        spark.read.parquet(path).limit(1)
        return True
    except Exception:
        return False


if __name__ == "__main__":
    run()
