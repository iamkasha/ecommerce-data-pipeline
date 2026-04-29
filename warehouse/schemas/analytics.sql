-- ─────────────────────────────────────────────────────────────────────────────
-- Analytics Schema — Business-ready views & materialised tables
-- These are the final outputs consumed by Grafana, BI tools, and reports
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS analytics;

-- ── Daily Revenue Summary ────────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.revenue_daily AS
SELECT
    dd.full_date                                        AS date,
    dd.year,
    dd.month,
    dd.month_name,
    dd.quarter,
    dd.is_weekend,
    fo.shipping_country                                 AS country,
    COUNT(fo.order_id)                                  AS total_orders,
    SUM(fo.net_amount)                                  AS gross_revenue,
    SUM(fo.discount_amount)                             AS total_discounts,
    AVG(fo.net_amount)                                  AS avg_order_value,
    COUNT(DISTINCT fo.user_key)                         AS unique_customers,
    SUM(CASE WHEN fo.is_high_value THEN 1 ELSE 0 END)  AS high_value_orders
FROM   dwh.fact_orders fo
JOIN   dwh.dim_date    dd ON dd.date_key = fo.date_key
WHERE  fo.status = 'completed'
GROUP BY 1,2,3,4,5,6,7;

-- ── Weekly Revenue Trend ─────────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.revenue_weekly AS
SELECT
    dd.year,
    dd.week,
    MIN(dd.full_date)                                   AS week_start,
    COUNT(fo.order_id)                                  AS total_orders,
    SUM(fo.net_amount)                                  AS gross_revenue,
    AVG(fo.net_amount)                                  AS avg_order_value,
    COUNT(DISTINCT fo.user_key)                         AS unique_customers
FROM   dwh.fact_orders fo
JOIN   dwh.dim_date    dd ON dd.date_key = fo.date_key
WHERE  fo.status = 'completed'
GROUP BY 1,2;

-- ── Customer Lifetime Value (CLV) ────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.customer_ltv AS
WITH order_stats AS (
    SELECT
        fo.user_key,
        COUNT(fo.order_id)          AS total_orders,
        SUM(fo.net_amount)          AS total_revenue,
        AVG(fo.net_amount)          AS avg_order_value,
        MIN(dd.full_date)           AS first_order_date,
        MAX(dd.full_date)           AS last_order_date,
        MAX(dd.full_date) - MIN(dd.full_date) AS days_active
    FROM   dwh.fact_orders fo
    JOIN   dwh.dim_date    dd ON dd.date_key = fo.date_key
    WHERE  fo.status = 'completed'
    GROUP  BY fo.user_key
)
SELECT
    du.user_id,
    du.email,
    du.country,
    du.cohort_month,
    os.total_orders,
    ROUND(os.total_revenue::NUMERIC, 2)         AS total_revenue,
    ROUND(os.avg_order_value::NUMERIC, 2)       AS avg_order_value,
    os.first_order_date,
    os.last_order_date,
    os.days_active,
    CASE
        WHEN os.total_revenue > 1000 THEN 'VIP'
        WHEN os.total_revenue > 500  THEN 'High'
        WHEN os.total_revenue > 100  THEN 'Medium'
        ELSE 'Low'
    END                                         AS customer_segment,
    os.total_orders > 1                         AS is_repeat_customer
FROM   order_stats os
JOIN   dwh.dim_users du ON du.user_key = os.user_key;

-- ── Monthly Cohort Retention ─────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.cohort_retention AS
WITH cohort_orders AS (
    SELECT
        du.user_id,
        du.cohort_month,
        TO_CHAR(dd.full_date, 'YYYY-MM')                        AS order_month,
        (EXTRACT(YEAR  FROM dd.full_date) * 12 +
         EXTRACT(MONTH FROM dd.full_date)) -
        (SPLIT_PART(du.cohort_month, '-', 1)::INT * 12 +
         SPLIT_PART(du.cohort_month, '-', 2)::INT)              AS months_since_signup
    FROM   dwh.fact_orders fo
    JOIN   dwh.dim_date    dd ON dd.date_key = fo.date_key
    JOIN   dwh.dim_users   du ON du.user_key = fo.user_key
    WHERE  fo.status = 'completed'
)
SELECT
    cohort_month,
    months_since_signup,
    COUNT(DISTINCT user_id)                     AS active_users
FROM cohort_orders
GROUP BY 1, 2
ORDER BY 1, 2;

-- ── Top Products ─────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.top_products AS
SELECT
    dp.product_id,
    dp.name,
    dp.category,
    dp.subcategory,
    dp.price,
    dp.margin_pct,
    COUNT(DISTINCT fo.order_id)                 AS total_orders,
    COUNT(DISTINCT fo.user_key)                 AS unique_buyers,
    SUM(fo.net_amount)                          AS total_revenue
FROM   dwh.fact_orders fo
JOIN   dwh.dim_products dp ON dp.product_key = fo.date_key  -- joined via order_items in dbt
WHERE  fo.status = 'completed'
GROUP BY 1,2,3,4,5,6
ORDER BY total_revenue DESC;

-- ── Payment Method Mix ───────────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.payment_breakdown AS
SELECT
    payment_method,
    COUNT(order_id)             AS order_count,
    SUM(net_amount)             AS total_revenue,
    ROUND(100.0 * COUNT(order_id) /
          SUM(COUNT(order_id)) OVER(), 2) AS pct_of_orders
FROM   dwh.fact_orders
WHERE  status = 'completed'
GROUP  BY 1
ORDER  BY 2 DESC;
