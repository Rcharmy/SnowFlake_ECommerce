-- =============================================================
-- snowflake_setup.sql
-- =============================================================
-- Run this script once as ACCOUNTADMIN to set up:
--   1. Database + schema structure
--   2. Virtual warehouses (cost-optimised sizing)
--   3. RBAC roles and grants (data governance)
--   4. RAW staging tables (landed by Glue / Snowpipe)
--   5. Star schema: FACT_ORDERS + 4 dimension tables
--   6. Snowpipe for auto-ingest from S3
-- =============================================================

USE ROLE ACCOUNTADMIN;


-- ─────────────────────────────────────────────
-- 1. DATABASE & SCHEMA STRUCTURE
-- ─────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DW
    COMMENT = 'E-commerce analytics data warehouse — Charmy Raj capstone project';

-- RAW: landing zone for Glue / Snowpipe loads (source-faithful, no transforms)
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.RAW
    COMMENT = 'Source-faithful raw landing zone. No business logic here.';

-- STAGING: cleaned, typed, deduplicated (dbt staging models)
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.STAGING
    COMMENT = 'Cleaned and typed intermediary layer. Output of dbt staging models.';

-- ANALYTICS: star schema — consumption layer for BI tools
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DW.ANALYTICS
    COMMENT = 'Star schema fact and dimension tables. Source of truth for Power BI.';


-- ─────────────────────────────────────────────
-- 2. VIRTUAL WAREHOUSES (cost-optimised)
-- ─────────────────────────────────────────────
-- Separate warehouses isolate compute costs per workload type.

-- ETL warehouse: used by Glue + Snowpipe + dbt runs
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE   = 'X-SMALL'       -- sufficient for 10K-row loads; scale up for production
    AUTO_SUSPEND     = 60              -- suspend after 60s idle (cost control)
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'ETL and transformation workloads';

-- Reporting warehouse: used by Power BI / analysts (separate to avoid contention)
CREATE WAREHOUSE IF NOT EXISTS REPORTING_WH
    WAREHOUSE_SIZE   = 'X-SMALL'
    AUTO_SUSPEND     = 120
    AUTO_RESUME      = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'BI reporting and ad-hoc analyst queries';


-- ─────────────────────────────────────────────
-- 3. RBAC ROLES & GRANTS  (data governance)
-- ─────────────────────────────────────────────
-- Role hierarchy:
--   ACCOUNTADMIN
--     └── DW_ADMIN       (full DW access — engineering leads)
--           ├── LOADER_ROLE    (write to RAW only — Glue service account)
--           ├── TRANSFORMER    (read RAW, write STAGING/ANALYTICS — dbt)
--           └── ANALYST_ROLE   (read ANALYTICS only — Power BI / analysts)

CREATE ROLE IF NOT EXISTS DW_ADMIN;
CREATE ROLE IF NOT EXISTS LOADER_ROLE;
CREATE ROLE IF NOT EXISTS TRANSFORMER;
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;

-- Role hierarchy grants
GRANT ROLE LOADER_ROLE  TO ROLE DW_ADMIN;
GRANT ROLE TRANSFORMER  TO ROLE DW_ADMIN;
GRANT ROLE ANALYST_ROLE TO ROLE DW_ADMIN;
GRANT ROLE DW_ADMIN     TO ROLE ACCOUNTADMIN;

-- Database-level grants
GRANT USAGE ON DATABASE ECOMMERCE_DW TO ROLE DW_ADMIN;
GRANT USAGE ON DATABASE ECOMMERCE_DW TO ROLE LOADER_ROLE;
GRANT USAGE ON DATABASE ECOMMERCE_DW TO ROLE TRANSFORMER;
GRANT USAGE ON DATABASE ECOMMERCE_DW TO ROLE ANALYST_ROLE;

-- Schema-level grants
GRANT USAGE ON SCHEMA ECOMMERCE_DW.RAW       TO ROLE LOADER_ROLE;
GRANT USAGE ON SCHEMA ECOMMERCE_DW.RAW       TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ECOMMERCE_DW.STAGING   TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ECOMMERCE_DW.ANALYTICS TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ECOMMERCE_DW.ANALYTICS TO ROLE ANALYST_ROLE;

-- Table-level grants
GRANT INSERT, SELECT ON ALL TABLES IN SCHEMA ECOMMERCE_DW.RAW       TO ROLE LOADER_ROLE;
GRANT SELECT         ON ALL TABLES IN SCHEMA ECOMMERCE_DW.RAW       TO ROLE TRANSFORMER;
GRANT ALL            ON ALL TABLES IN SCHEMA ECOMMERCE_DW.STAGING   TO ROLE TRANSFORMER;
GRANT ALL            ON ALL TABLES IN SCHEMA ECOMMERCE_DW.ANALYTICS TO ROLE TRANSFORMER;
GRANT SELECT         ON ALL TABLES IN SCHEMA ECOMMERCE_DW.ANALYTICS TO ROLE ANALYST_ROLE;

-- Future grants — auto-apply to new tables
GRANT INSERT, SELECT ON FUTURE TABLES IN SCHEMA ECOMMERCE_DW.RAW       TO ROLE LOADER_ROLE;
GRANT SELECT         ON FUTURE TABLES IN SCHEMA ECOMMERCE_DW.RAW       TO ROLE TRANSFORMER;
GRANT ALL            ON FUTURE TABLES IN SCHEMA ECOMMERCE_DW.STAGING   TO ROLE TRANSFORMER;
GRANT ALL            ON FUTURE TABLES IN SCHEMA ECOMMERCE_DW.ANALYTICS TO ROLE TRANSFORMER;
GRANT SELECT         ON FUTURE TABLES IN SCHEMA ECOMMERCE_DW.ANALYTICS TO ROLE ANALYST_ROLE;

-- Warehouse grants
GRANT USAGE ON WAREHOUSE COMPUTE_WH   TO ROLE LOADER_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH   TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE REPORTING_WH TO ROLE ANALYST_ROLE;

-- Service users (create manually in Snowflake console, then assign roles)
-- CREATE USER glue_loader   DEFAULT_ROLE = LOADER_ROLE   DEFAULT_WAREHOUSE = COMPUTE_WH;
-- CREATE USER dbt_transform  DEFAULT_ROLE = TRANSFORMER   DEFAULT_WAREHOUSE = COMPUTE_WH;
-- CREATE USER powerbi_reader DEFAULT_ROLE = ANALYST_ROLE  DEFAULT_WAREHOUSE = REPORTING_WH;
-- GRANT ROLE LOADER_ROLE  TO USER glue_loader;
-- GRANT ROLE TRANSFORMER  TO USER dbt_transform;
-- GRANT ROLE ANALYST_ROLE TO USER powerbi_reader;


-- ─────────────────────────────────────────────
-- 4. RAW STAGING TABLES
-- ─────────────────────────────────────────────
USE ROLE LOADER_ROLE;
USE SCHEMA ECOMMERCE_DW.RAW;
USE WAREHOUSE COMPUTE_WH;

CREATE TABLE IF NOT EXISTS RAW_ORDERS (
    order_id        VARCHAR(20)   NOT NULL,
    customer_id     VARCHAR(20),
    product_id      VARCHAR(20),
    order_date      TIMESTAMP_NTZ,
    quantity        INTEGER,
    unit_price      FLOAT,
    discount_pct    FLOAT,
    revenue         FLOAT,
    order_status    VARCHAR(20),
    shipping_region VARCHAR(50),
    payment_method  VARCHAR(30),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_PRODUCTS (
    product_id    VARCHAR(20)  NOT NULL,
    product_name  VARCHAR(200),
    category      VARCHAR(50),
    sub_category  VARCHAR(100),
    unit_price    FLOAT,
    price_tier    VARCHAR(20),
    in_stock      BOOLEAN,
    supplier_id   VARCHAR(20),
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    customer_id  VARCHAR(20)  NOT NULL,
    email        VARCHAR(200),
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    region       VARCHAR(50),
    segment      VARCHAR(20),
    signup_date  TIMESTAMP_NTZ,
    is_active    BOOLEAN,
    _loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_WEB_EVENTS (
    event_id    VARCHAR(50)  NOT NULL,
    customer_id VARCHAR(20),
    product_id  VARCHAR(20),
    event_type  VARCHAR(30),
    event_ts    TIMESTAMP_NTZ,
    session_id  VARCHAR(20),
    device      VARCHAR(20),
    page_url    VARCHAR(500),
    _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ─────────────────────────────────────────────
-- 5. STAR SCHEMA  (ANALYTICS layer)
-- ─────────────────────────────────────────────
USE ROLE TRANSFORMER;
USE SCHEMA ECOMMERCE_DW.ANALYTICS;

-- DIM_DATE: pre-populated date spine
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key        INTEGER       NOT NULL PRIMARY KEY,   -- YYYYMMDD integer key
    full_date       DATE          NOT NULL,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(10),
    week_of_year    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(10),
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
);

-- Populate DIM_DATE for 2023–2025 using Snowflake's generator
INSERT INTO DIM_DATE (date_key, full_date, year, quarter, month, month_name,
                      week_of_year, day_of_week, day_name, is_weekend)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))        AS date_key,
    d                                         AS full_date,
    YEAR(d)                                   AS year,
    QUARTER(d)                                AS quarter,
    MONTH(d)                                  AS month,
    TO_CHAR(d, 'MON')                         AS month_name,
    WEEKOFYEAR(d)                             AS week_of_year,
    DAYOFWEEK(d)                              AS day_of_week,
    DAYNAME(d)                                AS day_name,
    DAYOFWEEK(d) IN (0, 6)                    AS is_weekend
FROM (
    SELECT DATEADD(day, SEQ4(), '2023-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 1096))  -- 3 years
)
WHERE d <= '2025-12-31';

-- DIM_CUSTOMERS
CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS (
    customer_key  INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,
    customer_id   VARCHAR(20)   NOT NULL UNIQUE,
    email         VARCHAR(200),
    full_name     VARCHAR(200),
    region        VARCHAR(50),
    segment       VARCHAR(20),
    signup_date   DATE,
    is_active     BOOLEAN,
    _valid_from   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _valid_to     TIMESTAMP_NTZ,
    _is_current   BOOLEAN       DEFAULT TRUE
);

-- DIM_PRODUCTS
CREATE TABLE IF NOT EXISTS DIM_PRODUCTS (
    product_key   INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,
    product_id    VARCHAR(20)   NOT NULL UNIQUE,
    product_name  VARCHAR(200),
    category      VARCHAR(50),
    sub_category  VARCHAR(100),
    unit_price    FLOAT,
    price_tier    VARCHAR(20),
    in_stock      BOOLEAN,
    supplier_id   VARCHAR(20),
    _valid_from   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _valid_to     TIMESTAMP_NTZ,
    _is_current   BOOLEAN       DEFAULT TRUE
);

-- DIM_GEOGRAPHY
CREATE TABLE IF NOT EXISTS DIM_GEOGRAPHY (
    geography_key   INTEGER     NOT NULL AUTOINCREMENT PRIMARY KEY,
    region          VARCHAR(50) NOT NULL UNIQUE,
    region_group    VARCHAR(50),
    timezone        VARCHAR(50)
);

INSERT INTO DIM_GEOGRAPHY (region, region_group, timezone) VALUES
    ('North America', 'Americas',     'America/New_York'),
    ('Latin America', 'Americas',     'America/Sao_Paulo'),
    ('Europe',        'EMEA',         'Europe/London'),
    ('Asia Pacific',  'APAC',         'Asia/Singapore');

-- FACT_ORDERS  (central fact table — grain: one row per order line)
CREATE TABLE IF NOT EXISTS FACT_ORDERS (
    order_key        INTEGER       NOT NULL AUTOINCREMENT PRIMARY KEY,
    order_id         VARCHAR(20)   NOT NULL,
    customer_key     INTEGER       REFERENCES DIM_CUSTOMERS(customer_key),
    product_key      INTEGER       REFERENCES DIM_PRODUCTS(product_key),
    date_key         INTEGER       REFERENCES DIM_DATE(date_key),
    geography_key    INTEGER       REFERENCES DIM_GEOGRAPHY(geography_key),
    -- measures
    quantity         INTEGER,
    unit_price       FLOAT,
    discount_pct     FLOAT,
    revenue          FLOAT,
    gross_margin     FLOAT,        -- computed by dbt: revenue - (unit_price * 0.4 * quantity)
    -- degenerate dimensions
    order_status     VARCHAR(20),
    payment_method   VARCHAR(30),
    -- audit
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ─────────────────────────────────────────────
-- 6. SNOWPIPE  (auto-ingest from S3)
-- ─────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

-- External stage pointing to your S3 bucket
-- Replace <YOUR_S3_BUCKET> and <YOUR_IAM_ROLE_ARN> before running
CREATE STAGE IF NOT EXISTS ECOMMERCE_DW.RAW.S3_STAGE
    URL            = 's3://<YOUR_S3_BUCKET>/raw/ecommerce/'
    CREDENTIALS    = (AWS_ROLE = '<YOUR_IAM_ROLE_ARN>')
    FILE_FORMAT    = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    COMMENT        = 'S3 landing zone for raw e-commerce files';

-- Snowpipe for orders (auto-triggered by S3 event notifications)
CREATE PIPE IF NOT EXISTS ECOMMERCE_DW.RAW.PIPE_ORDERS
    AUTO_INGEST = TRUE
    COMMENT     = 'Continuously ingests new orders.csv files from S3 stage'
AS
COPY INTO ECOMMERCE_DW.RAW.RAW_ORDERS (
    order_id, customer_id, product_id, order_date, quantity,
    unit_price, discount_pct, revenue, order_status, shipping_region, payment_method
)
FROM @ECOMMERCE_DW.RAW.S3_STAGE/orders/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';

-- After creating the pipe, run:
--   SHOW PIPES LIKE 'PIPE_ORDERS';
-- Copy the SQS ARN from the notification_channel column and add it as an
-- S3 event notification on your bucket (All ObjectCreate events, prefix: raw/ecommerce/orders/).

-- ── VERIFICATION QUERIES ──────────────────────────────────────
-- Run these after loading data to verify row counts and schema integrity.

-- SELECT 'RAW_ORDERS'    AS tbl, COUNT(*) AS n FROM ECOMMERCE_DW.RAW.RAW_ORDERS
-- UNION ALL
-- SELECT 'RAW_PRODUCTS',          COUNT(*)    FROM ECOMMERCE_DW.RAW.RAW_PRODUCTS
-- UNION ALL
-- SELECT 'RAW_CUSTOMERS',         COUNT(*)    FROM ECOMMERCE_DW.RAW.RAW_CUSTOMERS
-- UNION ALL
-- SELECT 'RAW_WEB_EVENTS',        COUNT(*)    FROM ECOMMERCE_DW.RAW.RAW_WEB_EVENTS;
