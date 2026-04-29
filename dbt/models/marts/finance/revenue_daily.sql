-- Daily Revenue Mart
-- Pre-aggregated for fast BI queries
{{ config(materialized='table') }}

SELECT
    order_date,
    order_year,
    order_month,
    shipping_country                            AS country,
    COUNT(order_id)                             AS total_orders,
    COUNT(DISTINCT user_id)                     AS unique_customers,
    SUM(net_amount)                             AS gross_revenue,
    SUM(discount_amount)                        AS total_discounts,
    SUM(shipping_cost)                          AS total_shipping,
    ROUND(AVG(net_amount)::NUMERIC, 2)          AS avg_order_value,
    ROUND(MAX(net_amount)::NUMERIC, 2)          AS max_order_value,
    SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END) AS high_value_orders,
    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(order_id), 0), 2
    )                                           AS cancellation_rate_pct
FROM {{ ref('fct_orders') }}
GROUP BY 1,2,3,4
