{{ config(materialized='view') }}

SELECT
    user_id::VARCHAR(100)       AS user_id,
    email::VARCHAR(255)         AS email,
    full_name::VARCHAR(255)     AS full_name,
    country::VARCHAR(100)       AS country,
    city::VARCHAR(100)          AS city,
    signup_date::DATE           AS signup_date,
    cohort_month::VARCHAR(10)   AS cohort_month,
    days_since_signup::INTEGER  AS days_since_signup,
    is_active::BOOLEAN          AS is_active
FROM {{ source('staging', 'users') }}
WHERE user_id IS NOT NULL
  AND email   IS NOT NULL
