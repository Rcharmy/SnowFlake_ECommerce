-- models/staging/stg_orders.sql

-- Staging layer: type-cast, deduplicate, and filter RAW_ORDERS.
-- Materialised as a VIEW — no storage cost, always fresh.


WITH source AS (
    SELECT * FROM {{ source('raw', 'RAW_ORDERS') }}
),

deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY _loaded_at DESC
           ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        order_date::DATE                         AS order_date,
        EXTRACT(YEAR  FROM order_date) * 10000
            + EXTRACT(MONTH FROM order_date) * 100
            + EXTRACT(DAY   FROM order_date)     AS date_key,
        quantity::INTEGER                        AS quantity,
        unit_price::FLOAT                        AS unit_price,
        COALESCE(discount_pct, 0)::FLOAT         AS discount_pct,
        revenue::FLOAT                           AS revenue,
        UPPER(TRIM(order_status))                AS order_status,
        UPPER(TRIM(shipping_region))             AS shipping_region,
        LOWER(TRIM(payment_method))              AS payment_method,
        _loaded_at
    FROM deduplicated
    WHERE row_num = 1
      AND order_id    IS NOT NULL
      AND customer_id IS NOT NULL
      AND product_id  IS NOT NULL
      AND revenue > 0
      AND quantity > 0
)

SELECT * FROM cleaned
