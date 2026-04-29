"""
Analytics Report Generator

Queries the warehouse analytics schema and prints
a summary report to stdout (or exports to CSV/JSON).

Covers:
  - Revenue summary (last 30 days)
  - Top 10 products by revenue
  - Customer segment breakdown
  - Country revenue distribution
  - Week-over-week growth

Usage:
    python -m analytics.reports
    python -m analytics.reports --output csv --file report.csv
"""
import os
from datetime import date
from textwrap import dedent

import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

WH_CONFIG = {
    "host":     os.getenv("REDSHIFT_HOST",     "localhost"),
    "port":     int(os.getenv("REDSHIFT_PORT", 5439)),
    "dbname":   os.getenv("REDSHIFT_DB",       "warehouse"),
    "user":     os.getenv("REDSHIFT_USER",     "warehouse"),
    "password": os.getenv("REDSHIFT_PASSWORD", "warehouse123"),
}


def query(sql: str) -> pd.DataFrame:
    conn = psycopg2.connect(**WH_CONFIG)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


QUERIES = {
    "revenue_30d": dedent("""
        SELECT
            SUM(gross_revenue)          AS total_revenue,
            SUM(total_orders)           AS total_orders,
            ROUND(AVG(avg_order_value)::NUMERIC, 2) AS avg_order_value,
            SUM(unique_customers)       AS total_customers,
            SUM(high_value_orders)      AS high_value_orders
        FROM analytics.revenue_daily
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
    """),

    "daily_trend": dedent("""
        SELECT
            date,
            SUM(total_orders)       AS orders,
            ROUND(SUM(gross_revenue)::NUMERIC, 2) AS revenue,
            SUM(unique_customers)   AS customers
        FROM analytics.revenue_daily
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 1
    """),

    "top_countries": dedent("""
        SELECT
            country,
            SUM(total_orders)   AS orders,
            ROUND(SUM(gross_revenue)::NUMERIC, 2) AS revenue
        FROM analytics.revenue_daily
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """),

    "customer_segments": dedent("""
        SELECT
            customer_segment,
            COUNT(*)                                AS customers,
            ROUND(AVG(total_revenue)::NUMERIC, 2)  AS avg_revenue,
            ROUND(AVG(total_orders)::NUMERIC, 1)   AS avg_orders
        FROM analytics.customer_ltv
        GROUP BY 1
        ORDER BY 2 DESC
    """),

    "wow_growth": dedent("""
        WITH weekly AS (
            SELECT
                year, week,
                week_start,
                SUM(gross_revenue) AS revenue,
                SUM(total_orders)  AS orders
            FROM analytics.revenue_weekly
            GROUP BY 1,2,3
            ORDER BY 1 DESC, 2 DESC
            LIMIT 8
        )
        SELECT
            week_start,
            revenue,
            orders,
            ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY week_start))
                  / NULLIF(LAG(revenue) OVER (ORDER BY week_start), 0), 2) AS revenue_growth_pct
        FROM weekly
        ORDER BY week_start DESC
    """),
}


def print_report():
    separator = "═" * 60
    print(f"\n{separator}")
    print("  ECOMMERCE PIPELINE — ANALYTICS REPORT")
    print(f"  Generated: {date.today()}")
    print(separator)

    # Revenue Summary
    try:
        df = query(QUERIES["revenue_30d"])
        r  = df.iloc[0]
        print("\n📊 REVENUE SUMMARY (Last 30 Days)")
        print(f"  Total Revenue:      ${r['total_revenue']:>12,.2f}")
        print(f"  Total Orders:       {int(r['total_orders']):>12,}")
        print(f"  Avg Order Value:    ${r['avg_order_value']:>12,.2f}")
        print(f"  Unique Customers:   {int(r['total_customers']):>12,}")
        print(f"  High-Value Orders:  {int(r['high_value_orders']):>12,}")
    except Exception as e:
        print(f"  (No data yet — run the pipeline first: {e})")

    # Top Countries
    try:
        df = query(QUERIES["top_countries"])
        print("\n🌍 TOP 10 COUNTRIES BY REVENUE")
        print(f"  {'Country':<15} {'Orders':>8} {'Revenue':>14}")
        print(f"  {'-'*15} {'-'*8} {'-'*14}")
        for _, row in df.iterrows():
            print(f"  {row['country']:<15} {int(row['orders']):>8,} ${float(row['revenue']):>13,.2f}")
    except Exception as e:
        print(f"  (No data: {e})")

    # Customer Segments
    try:
        df = query(QUERIES["customer_segments"])
        print("\n👤 CUSTOMER SEGMENTS")
        print(f"  {'Segment':<15} {'Customers':>10} {'Avg Revenue':>13} {'Avg Orders':>11}")
        print(f"  {'-'*15} {'-'*10} {'-'*13} {'-'*11}")
        for _, row in df.iterrows():
            print(f"  {row['customer_segment']:<15} {int(row['customers']):>10,} "
                  f"${float(row['avg_revenue']):>12,.2f} {float(row['avg_orders']):>11.1f}")
    except Exception as e:
        print(f"  (No data: {e})")

    # WoW Growth
    try:
        df = query(QUERIES["wow_growth"])
        print("\n📈 WEEK-OVER-WEEK REVENUE GROWTH")
        print(f"  {'Week Start':<14} {'Revenue':>14} {'Orders':>8} {'Growth%':>9}")
        print(f"  {'-'*14} {'-'*14} {'-'*8} {'-'*9}")
        for _, row in df.iterrows():
            growth = f"{row['revenue_growth_pct']:+.1f}%" if pd.notna(row['revenue_growth_pct']) else "  —"
            print(f"  {str(row['week_start']):<14} ${float(row['revenue']):>13,.2f} "
                  f"{int(row['orders']):>8,} {growth:>9}")
    except Exception as e:
        print(f"  (No data: {e})")

    print(f"\n{separator}\n")


if __name__ == "__main__":
    print_report()
