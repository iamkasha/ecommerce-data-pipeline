"""
Seed the source PostgreSQL database with realistic fake e-commerce data.
Uses Faker to generate users, products, orders and order items.

Usage:
    python scripts/seed_postgres.py
    python scripts/seed_postgres.py --users 5000 --products 500 --orders 20000
"""
import os
import sys
import uuid
import random
import argparse
from datetime import datetime, timedelta, date

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv
from loguru import logger

load_dotenv()
fake = Faker()
Faker.seed(42)
random.seed(42)

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "ecommerce"),
    "user":     os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
}

CATEGORIES = {
    "Electronics":   ["Smartphones", "Laptops", "Tablets", "Accessories"],
    "Clothing":      ["Men", "Women", "Kids", "Sports"],
    "Home & Garden": ["Furniture", "Kitchen", "Garden", "Decor"],
    "Books":         ["Fiction", "Non-Fiction", "Technology", "Business"],
    "Sports":        ["Fitness", "Outdoor", "Team Sports", "Water Sports"],
}

STATUSES    = ["completed", "completed", "completed", "shipped", "processing", "cancelled"]
PAYMENTS    = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]
COUNTRIES   = ["US", "UK", "DE", "FR", "CA", "AU", "IN", "JP", "BR", "MX"]


def generate_users(n: int) -> list[dict]:
    logger.info(f"Generating {n} users...")
    users = []
    for _ in range(n):
        signup = fake.date_between(start_date=date(2020, 1, 1), end_date=date(2024, 1, 1))
        users.append({
            "user_id":    str(uuid.uuid4()),
            "email":      fake.unique.email(),
            "full_name":  fake.name(),
            "country":    random.choice(COUNTRIES),
            "city":       fake.city(),
            "signup_date": signup,
            "is_active":  random.random() > 0.05,
        })
    return users


def generate_products(n: int) -> list[dict]:
    logger.info(f"Generating {n} products...")
    products = []
    for _ in range(n):
        cat = random.choice(list(CATEGORIES.keys()))
        subcat = random.choice(CATEGORIES[cat])
        price = round(random.uniform(5.0, 999.99), 2)
        products.append({
            "product_id":  str(uuid.uuid4()),
            "sku":         f"SKU-{fake.unique.bothify('??####').upper()}",
            "name":        f"{fake.word().capitalize()} {subcat} {fake.word().capitalize()}",
            "category":    cat,
            "subcategory": subcat,
            "price":       price,
            "cost":        round(price * random.uniform(0.3, 0.7), 2),
            "stock_qty":   random.randint(0, 1000),
            "is_active":   random.random() > 0.1,
        })
    return products


def generate_orders(n: int, users: list, products: list) -> tuple[list, list]:
    logger.info(f"Generating {n} orders...")
    orders, items = [], []
    for _ in range(n):
        user      = random.choice(users)
        order_id  = str(uuid.uuid4())
        created   = fake.date_time_between(start_date="-2y", end_date="now")
        n_items   = random.randint(1, 5)
        chosen    = random.sample(products, min(n_items, len(products)))
        total     = 0.0
        for p in chosen:
            qty   = random.randint(1, 4)
            price = float(p["price"])
            total += qty * price
            items.append({
                "item_id":    str(uuid.uuid4()),
                "order_id":   order_id,
                "product_id": p["product_id"],
                "quantity":   qty,
                "unit_price": price,
            })
        orders.append({
            "order_id":        order_id,
            "user_id":         user["user_id"],
            "status":          random.choice(STATUSES),
            "total_amount":    round(total, 2),
            "discount_amount": round(total * random.uniform(0, 0.15), 2),
            "shipping_cost":   round(random.uniform(0, 25), 2),
            "payment_method":  random.choice(PAYMENTS),
            "shipping_country": user["country"],
            "created_at":      created,
        })
    return orders, items


def insert_batch(conn, table: str, rows: list[dict], batch_size: int = 1000):
    if not rows:
        return
    cols = list(rows[0].keys())
    sql  = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s ON CONFLICT DO NOTHING"
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            vals  = [tuple(r[c] for c in cols) for r in batch]
            execute_values(cur, sql, vals)
    conn.commit()
    logger.info(f"  ✓ Inserted {len(rows)} rows into {table}")


def main(n_users: int, n_products: int, n_orders: int):
    logger.info("Connecting to source PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)

    users    = generate_users(n_users)
    products = generate_products(n_products)
    orders, items = generate_orders(n_orders, users, products)

    logger.info("Inserting data...")
    insert_batch(conn, "users",       users)
    insert_batch(conn, "products",    products)
    insert_batch(conn, "orders",      orders)
    insert_batch(conn, "order_items", items)

    conn.close()
    logger.success(f"Seed complete — {n_users} users, {n_products} products, "
                   f"{n_orders} orders, {len(items)} order items")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--users",    type=int, default=2000)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders",   type=int, default=10000)
    args = parser.parse_args()
    main(args.users, args.products, args.orders)
