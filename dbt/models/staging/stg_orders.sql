-- Staging: Orders
-- 1:1 cast from staging schema → typed columns
-- No business logic here — just types + renames
{{ config(materialized='view') }}

SELECT
    order_id::VARCHAR(100)          AS order_id,
    user_id::VARCHAR(100)           AS user_id,
    status::VARCHAR(50)             AS status,
    total_amount::NUMERIC(12,2)     AS total_amount,
    discount_amount::NUMERIC(10,2)  AS discount_amount,
    shipping_cost::NUMERIC(8,2)     AS shipping_cost,
    net_amount::NUMERIC(12,2)       AS net_amount,
    payment_method::VARCHAR(50)     AS payment_method,
    shipping_country::VARCHAR(100)  AS shipping_country,
    order_date::DATE                AS order_date,
    order_year::SMALLINT            AS order_year,
    order_month::SMALLINT           AS order_month,
    is_high_value::BOOLEAN          AS is_high_value,
    created_at::TIMESTAMPTZ         AS created_at
FROM {{ source('staging', 'orders') }}
WHERE order_id IS NOT NULL
  AND user_id  IS NOT NULL
