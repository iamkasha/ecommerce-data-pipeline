-- Customer Lifetime Value Model
{{ config(materialized='table') }}

WITH order_agg AS (
    SELECT
        user_id,
        COUNT(order_id)                         AS total_orders,
        SUM(net_amount)                         AS total_revenue,
        ROUND(AVG(net_amount)::NUMERIC, 2)      AS avg_order_value,
        MIN(order_date)                         AS first_order_date,
        MAX(order_date)                         AS last_order_date,
        MAX(order_date) - MIN(order_date)       AS days_active,
        COUNT(DISTINCT payment_method)          AS payment_methods_used
    FROM {{ ref('fct_orders') }}
    WHERE status = 'completed'
    GROUP BY 1
),
users AS (
    SELECT * FROM {{ ref('stg_users') }}
)

SELECT
    u.user_id,
    u.email,
    u.full_name,
    u.country,
    u.cohort_month,
    u.signup_date,
    u.days_since_signup,
    oa.total_orders,
    ROUND(oa.total_revenue::NUMERIC, 2)         AS total_revenue,
    oa.avg_order_value,
    oa.first_order_date,
    oa.last_order_date,
    oa.days_active,
    oa.payment_methods_used,
    oa.total_orders > 1                         AS is_repeat_customer,
    CASE
        WHEN oa.total_revenue > 1000 THEN 'VIP'
        WHEN oa.total_revenue > 500  THEN 'High Value'
        WHEN oa.total_revenue > 100  THEN 'Mid Value'
        ELSE 'Low Value'
    END                                         AS customer_segment,
    -- Simple predicted CLV: avg_order_value × expected annual orders
    ROUND((oa.avg_order_value *
           GREATEST(oa.total_orders / NULLIF(oa.days_active / 365.0, 0), 1)
          )::NUMERIC, 2)                        AS predicted_annual_clv
FROM users u
LEFT JOIN order_agg oa ON oa.user_id = u.user_id
