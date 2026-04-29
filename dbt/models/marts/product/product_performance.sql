-- Product Performance Mart
{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
order_revenue AS (
    -- Revenue attributed per product via product_metrics staging
    SELECT
        product_id,
        SUM(units_sold::INTEGER)                AS total_units_sold,
        SUM(revenue::NUMERIC)                   AS total_revenue,
        SUM(order_count::INTEGER)               AS total_orders,
        SUM(unique_buyers::INTEGER)             AS total_unique_buyers,
        ROUND(AVG(avg_selling_price::NUMERIC), 2) AS avg_selling_price,
        SUM(gross_profit::NUMERIC)              AS total_gross_profit
    FROM {{ source('staging', 'product_metrics') }}
    GROUP BY 1
)

SELECT
    p.product_id,
    p.sku,
    p.name,
    p.category,
    p.subcategory,
    p.price                                     AS list_price,
    p.cost,
    p.margin_pct,
    p.stock_qty,
    p.is_active,
    COALESCE(r.total_units_sold, 0)             AS total_units_sold,
    COALESCE(r.total_revenue, 0)                AS total_revenue,
    COALESCE(r.total_orders, 0)                 AS total_orders,
    COALESCE(r.total_unique_buyers, 0)          AS total_unique_buyers,
    r.avg_selling_price,
    COALESCE(r.total_gross_profit, 0)           AS total_gross_profit,
    CASE
        WHEN COALESCE(r.total_revenue, 0) = 0 THEN 'No Sales'
        WHEN r.total_revenue > 10000 THEN 'Top Seller'
        WHEN r.total_revenue > 1000  THEN 'Good Seller'
        ELSE 'Slow Mover'
    END                                         AS performance_tier
FROM products p
LEFT JOIN order_revenue r ON r.product_id = p.product_id
