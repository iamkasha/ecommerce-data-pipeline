{{ config(materialized='view') }}

SELECT
    product_id::VARCHAR(100)    AS product_id,
    sku::VARCHAR(50)            AS sku,
    name::VARCHAR(255)          AS name,
    category::VARCHAR(100)      AS category,
    subcategory::VARCHAR(100)   AS subcategory,
    price::NUMERIC(10,2)        AS price,
    cost::NUMERIC(10,2)         AS cost,
    margin_pct::NUMERIC(6,2)    AS margin_pct,
    stock_qty::INTEGER          AS stock_qty,
    is_active::BOOLEAN          AS is_active
FROM {{ source('staging', 'products') }}
WHERE product_id IS NOT NULL
  AND price > 0
