
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT customer_id, customer_key
    FROM {{ ref('dim_customers') }}
    WHERE _is_current = TRUE
),

products AS (
    SELECT product_id, product_key, unit_price AS cost_price
    FROM {{ ref('dim_products') }}
    WHERE _is_current = TRUE
),

geography AS (
    SELECT region, geography_key
    FROM {{ source('analytics', 'DIM_GEOGRAPHY') }}
),

joined AS (
    SELECT
        o.order_id,
        c.customer_key,
        p.product_key,
        o.date_key,
        g.geography_key,
        o.quantity,
        o.unit_price,
        o.discount_pct,
        o.revenue,
        -- Gross margin: revenue minus estimated COGS (40% of unit price)
        ROUND(o.revenue - (p.cost_price * 0.40 * o.quantity), 2)  AS gross_margin,
        o.order_status,
        o.payment_method,
        o._loaded_at
    FROM orders o
    LEFT JOIN customers  c ON o.customer_id     = c.customer_id
    LEFT JOIN products   p ON o.product_id      = p.product_id
    LEFT JOIN geography  g ON o.shipping_region = g.region
)

SELECT * FROM joined
