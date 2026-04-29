-- ─────────────────────────────────────────────────────────────────────────────
-- DWH Schema — Star Schema (Fact + Dimensions)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS dwh;

-- ── Dimension: Date ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INTEGER PRIMARY KEY,    -- YYYYMMDD
    full_date       DATE    NOT NULL,
    year            SMALLINT,
    quarter         SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(10),
    week            SMALLINT,
    day_of_month    SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
);

-- Populate dim_date for 2020–2026
INSERT INTO dwh.dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER                 AS date_key,
    d                                                AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT                AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT                AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT                AS month,
    TO_CHAR(d, 'Month')                              AS month_name,
    EXTRACT(WEEK    FROM d)::SMALLINT                AS week,
    EXTRACT(DAY     FROM d)::SMALLINT                AS day_of_month,
    EXTRACT(DOW     FROM d)::SMALLINT                AS day_of_week,
    TO_CHAR(d, 'Day')                                AS day_name,
    EXTRACT(DOW     FROM d) IN (0, 6)                AS is_weekend
FROM generate_series('2020-01-01'::DATE, '2026-12-31'::DATE, '1 day') d
ON CONFLICT (date_key) DO NOTHING;

-- ── Dimension: Users ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dwh.dim_users (
    user_key            BIGSERIAL   PRIMARY KEY,
    user_id             VARCHAR(100) UNIQUE NOT NULL,
    email               VARCHAR(255),
    full_name           VARCHAR(255),
    country             VARCHAR(100),
    city                VARCHAR(100),
    signup_date         DATE,
    cohort_month        VARCHAR(10),
    days_since_signup   INTEGER,
    is_active           BOOLEAN,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ── Dimension: Products ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dwh.dim_products (
    product_key     BIGSERIAL   PRIMARY KEY,
    product_id      VARCHAR(100) UNIQUE NOT NULL,
    sku             VARCHAR(50),
    name            VARCHAR(255),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    price           NUMERIC(10, 2),
    cost            NUMERIC(10, 2),
    margin_pct      NUMERIC(6, 2),
    is_active       BOOLEAN,
    _loaded_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Fact: Orders ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_key           BIGSERIAL   PRIMARY KEY,
    order_id            VARCHAR(100) UNIQUE NOT NULL,
    user_key            BIGINT      REFERENCES dwh.dim_users(user_key),
    date_key            INTEGER     REFERENCES dwh.dim_date(date_key),
    status              VARCHAR(50),
    total_amount        NUMERIC(12, 2),
    discount_amount     NUMERIC(10, 2),
    shipping_cost       NUMERIC(8,  2),
    net_amount          NUMERIC(12, 2),
    payment_method      VARCHAR(50),
    shipping_country    VARCHAR(100),
    is_high_value       BOOLEAN,
    created_at          TIMESTAMPTZ,
    _loaded_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_date   ON dwh.fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_user   ON dwh.fact_orders(user_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_status ON dwh.fact_orders(status);
