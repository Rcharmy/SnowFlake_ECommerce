-- models/staging/stg_customers.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_CUSTOMERS') }}
),
deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC) AS row_num
    FROM source
)
SELECT
    customer_id,
    LOWER(TRIM(email))                             AS email,
    INITCAP(TRIM(first_name))                      AS first_name,
    INITCAP(TRIM(last_name))                       AS last_name,
    INITCAP(TRIM(first_name)) || ' ' ||
        INITCAP(TRIM(last_name))                   AS full_name,
    UPPER(TRIM(region))                            AS region,
    INITCAP(TRIM(segment))                         AS segment,
    signup_date::DATE                              AS signup_date,
    COALESCE(is_active, FALSE)                     AS is_active,
    _loaded_at
FROM deduplicated
WHERE row_num = 1
  AND customer_id IS NOT NULL
  AND email IS NOT NULL
