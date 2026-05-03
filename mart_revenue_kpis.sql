

WITH fact AS (
    SELECT * FROM {{ ref('fact_orders') }}
    WHERE order_status NOT IN ('RETURNED', 'CANCELLED')
),

dim_date AS (
    SELECT date_key, year, month, month_name, quarter
    FROM {{ source('analytics', 'DIM_DATE') }}
),

monthly AS (
    SELECT
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        f.geography_key,
        COUNT(DISTINCT f.order_id)                  AS total_orders,
        COUNT(DISTINCT f.customer_key)              AS unique_customers,
        SUM(f.revenue)                              AS total_revenue,
        SUM(f.gross_margin)                         AS total_gross_margin,
        ROUND(SUM(f.revenue) /
              NULLIF(COUNT(DISTINCT f.order_id), 0), 2)  AS avg_order_value,
        ROUND(SUM(f.gross_margin) /
              NULLIF(SUM(f.revenue), 0) * 100, 2)        AS gross_margin_pct,
        SUM(f.quantity)                             AS total_units_sold
    FROM fact f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM monthly
ORDER BY year, month
