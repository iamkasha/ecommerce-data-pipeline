-- Fact: Orders
-- Joins staging orders with user and date dimensions
{{ config(materialized='table', sort='order_date', dist='user_id') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
users AS (
    SELECT * FROM {{ ref('stg_users') }}
)

SELECT
    o.order_id,
    o.user_id,
    u.country                                       AS user_country,
    u.cohort_month,
    o.order_date,
    o.order_year,
    o.order_month,
    TO_CHAR(o.order_date, 'YYYY-"W"IW')            AS order_week,
    o.status,
    o.total_amount,
    o.discount_amount,
    o.shipping_cost,
    o.net_amount,
    o.payment_method,
    o.shipping_country,
    o.is_high_value,
    -- Discount rate
    CASE WHEN o.total_amount > 0
         THEN ROUND(o.discount_amount / o.total_amount * 100, 2)
         ELSE 0 END                                AS discount_rate_pct,
    o.created_at
FROM orders o
LEFT JOIN users u ON u.user_id = o.user_id
