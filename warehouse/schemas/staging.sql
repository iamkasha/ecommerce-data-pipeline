-- ─────────────────────────────────────────────────────────────────────────────
-- Staging Schema — raw COPY from S3 Gold layer into Redshift/PostgreSQL
-- All columns VARCHAR to accept any raw data; type-cast in dwh schema
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.daily_revenue;
CREATE TABLE staging.daily_revenue (
    order_date          VARCHAR(20),
    shipping_country    VARCHAR(100),
    total_orders        VARCHAR(20),
    gross_revenue       VARCHAR(30),
    total_discounts     VARCHAR(30),
    total_shipping      VARCHAR(30),
    avg_order_value     VARCHAR(30),
    unique_customers    VARCHAR(20),
    high_value_orders   VARCHAR(20),
    revenue_per_customer VARCHAR(30),
    _processed_at       VARCHAR(50)
);

DROP TABLE IF EXISTS staging.product_metrics;
CREATE TABLE staging.product_metrics (
    order_date          VARCHAR(20),
    product_id          VARCHAR(100),
    name                VARCHAR(255),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    units_sold          VARCHAR(20),
    revenue             VARCHAR(30),
    order_count         VARCHAR(20),
    unique_buyers       VARCHAR(20),
    avg_selling_price   VARCHAR(30),
    gross_profit        VARCHAR(30),
    _processed_at       VARCHAR(50)
);

DROP TABLE IF EXISTS staging.user_order_summary;
CREATE TABLE staging.user_order_summary (
    user_id             VARCHAR(100),
    total_orders        VARCHAR(20),
    total_revenue       VARCHAR(30),
    avg_order_value     VARCHAR(30),
    first_order_date    VARCHAR(20),
    last_order_date     VARCHAR(20),
    payment_methods_used VARCHAR(10),
    days_as_customer    VARCHAR(10),
    is_repeat_customer  VARCHAR(10),
    _processed_at       VARCHAR(50)
);

DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders (
    order_id            VARCHAR(100),
    user_id             VARCHAR(100),
    status              VARCHAR(50),
    total_amount        VARCHAR(30),
    discount_amount     VARCHAR(30),
    shipping_cost       VARCHAR(30),
    net_amount          VARCHAR(30),
    payment_method      VARCHAR(50),
    shipping_country    VARCHAR(100),
    order_date          VARCHAR(20),
    order_year          VARCHAR(10),
    order_month         VARCHAR(10),
    is_high_value       VARCHAR(10),
    created_at          VARCHAR(50),
    _processed_at       VARCHAR(50)
);

DROP TABLE IF EXISTS staging.users;
CREATE TABLE staging.users (
    user_id             VARCHAR(100),
    email               VARCHAR(255),
    full_name           VARCHAR(255),
    country             VARCHAR(100),
    city                VARCHAR(100),
    signup_date         VARCHAR(20),
    cohort_month        VARCHAR(10),
    days_since_signup   VARCHAR(10),
    is_active           VARCHAR(10),
    _processed_at       VARCHAR(50)
);

DROP TABLE IF EXISTS staging.products;
CREATE TABLE staging.products (
    product_id          VARCHAR(100),
    sku                 VARCHAR(50),
    name                VARCHAR(255),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    price               VARCHAR(20),
    cost                VARCHAR(20),
    margin_pct          VARCHAR(10),
    stock_qty           VARCHAR(10),
    is_active           VARCHAR(10),
    _processed_at       VARCHAR(50)
);
