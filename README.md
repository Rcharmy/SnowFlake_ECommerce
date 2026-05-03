# E-commerce Sales Analytics Pipeline on Snowflake

**Author:** Charmy Raj  
**Stack:** Python · AWS S3 · AWS Glue · Snowflake · dbt · Power BI  
**Purpose:** End-to-end data engineering portfolio project — demonstrates Snowflake data warehouse design, ELT pipeline construction, RBAC governance, and dbt transformation modelling.

---

## Project Overview

This project builds a production-style analytics pipeline for an e-commerce business, taking raw transactional data from four source systems and delivering clean, governed KPI dashboards to business stakeholders.

The pipeline answers three business questions:
1. What is our monthly revenue trend, and which regions are growing?
2. Which product categories drive the highest gross margin?
3. Which customer segments are at risk of churning?

---

## Architecture

```
[Sources]           [Ingest]              [Warehouse]              [Serve]
orders.csv    ──→                         ┌─────────────────┐
products.csv  ──→  AWS S3 → AWS Glue  →  │   Snowflake DW  │ → dbt models → Power BI
customers.json──→             +           │  (star schema)  │
web_events    ──→          Snowpipe  ──→  └─────────────────┘
```

### Layer-by-layer

| Layer | Technology | Purpose |
|---|---|---|
| Source | CSV / JSON files | Raw transactional data from 4 operational systems |
| Staging (S3) | AWS S3 | Immutable raw landing zone before any transformation |
| ELT | AWS Glue (PySpark) + Snowpipe | Extract from S3, type-cast, load into Snowflake RAW schema |
| Warehouse | Snowflake | Star schema fact + 4 dimension tables in ANALYTICS schema |
| Transform | dbt (SQL models) | Business logic, KPI computation, gross margin calculation |
| Serve | Power BI | Executive dashboards — revenue, AOV, churn, margin by segment |

---

## Snowflake Architecture Decisions

### Why Snowflake over Redshift or BigQuery?

| Criterion | Snowflake | Redshift | BigQuery |
|---|---|---|---|
| Compute/storage separation | ✓ Native | ✗ Coupled | ✓ Native |
| Multi-cluster concurrency | ✓ | Limited | ✓ |
| Auto-suspend / auto-resume | ✓ Granular | ✗ | N/A (serverless) |
| Snowpipe (event-driven ingest) | ✓ Built-in | ✗ | Pub/Sub required |
| RBAC granularity | ✓ Role hierarchy | Basic | IAM-based |

**Decision:** Snowflake's native separation of compute and storage lets us run separate warehouses for ELT and reporting without paying for idle compute. Snowpipe's S3 auto-ingest eliminates the need for a scheduled Glue crawler. RBAC role hierarchy maps cleanly to our three access tiers.

### Star Schema Design

Central fact table: **FACT_ORDERS** (grain: one row per order)

Dimensions:
- **DIM_DATE** — pre-populated date spine with year/quarter/month/week attributes
- **DIM_CUSTOMERS** — SCD Type 2 (\_valid\_from / \_valid\_to / \_is\_current) to track segment changes
- **DIM_PRODUCTS** — SCD Type 2 with price tier history
- **DIM_GEOGRAPHY** — region-to-timezone mapping

**Why SCD Type 2 on customers and products?**  
A customer's segment changes over time (Regular → VIP → At-Risk). If we overwrite the dimension, we lose the ability to answer "what was this customer's segment when they placed this order?" SCD Type 2 preserves that history.

### RBAC Design

```
ACCOUNTADMIN
  └── DW_ADMIN
        ├── LOADER_ROLE     → WRITE to RAW only  (Glue service account)
        ├── TRANSFORMER     → READ RAW, WRITE STAGING + ANALYTICS  (dbt)
        └── ANALYST_ROLE    → READ ANALYTICS only  (Power BI / analysts)
```

Each role has the minimum permissions needed. FUTURE GRANTS ensure new tables automatically inherit the correct access without manual re-grants.

---

## dbt Transformation Models

```
models/
├── staging/
│   ├── stg_orders.sql       ← deduplicate, type-cast, filter nulls
│   └── stg_customers.sql    ← normalise names, validate emails
└── marts/
    ├── fact_orders.sql       ← join all dims, compute gross_margin
    └── mart_revenue_kpis.sql ← monthly rollup for Power BI
```

### Gross Margin Calculation
```sql
gross_margin = revenue - (unit_price * 0.40 * quantity)
-- 40% COGS assumption — replace with actuals from finance team
```

### Why dbt for transformations?
- SQL-native — no Python overhead for set-based transformations
- Version-controlled models with built-in testing framework
- Lineage graph auto-generated (`dbt docs generate`)
- `post-hook` automatically grants SELECT to ANALYST_ROLE on every new mart table

---

## How to Run This Project

### Prerequisites
- Snowflake free trial account (sign up at snowflake.com)
- AWS account with S3 bucket and IAM role
- Python 3.9+, dbt-snowflake installed (`pip install dbt-snowflake`)

### Step 1: Generate sample data
```bash
cd data/
python generate_sample_data.py
# Outputs: orders.csv, products.csv, customers.json, web_events.json
```

### Step 2: Upload to S3
```bash
aws s3 cp orders.csv       s3://YOUR_BUCKET/raw/ecommerce/orders/
aws s3 cp products.csv     s3://YOUR_BUCKET/raw/ecommerce/products/
aws s3 cp customers.json   s3://YOUR_BUCKET/raw/ecommerce/customers/
aws s3 cp web_events.json  s3://YOUR_BUCKET/raw/ecommerce/events/
```

### Step 3: Run Snowflake setup
```sql
-- In Snowflake Worksheets, run:
snowflake/snowflake_setup.sql
-- This creates databases, schemas, RBAC roles, RAW tables, star schema, and Snowpipe.
```

### Step 4: Run Glue ELT job
```bash
# Deploy glue/glue_etl_job.py to AWS Glue as a PySpark job.
# Set job parameters: S3_BUCKET, SF_ACCOUNT, SF_DATABASE, SF_WAREHOUSE, SF_ROLE, SF_USER, SF_PASSWORD
# Then trigger the job manually or on a schedule.
```

### Step 5: Run dbt transformations
```bash
cd dbt/
dbt deps
dbt run --select staging.*          # materialise staging views
dbt run --select marts.*            # build ANALYTICS tables
dbt test                            # run data quality tests
dbt docs generate && dbt docs serve # view lineage graph at localhost:8080
```

### Step 6: Connect Power BI
1. In Power BI Desktop: Get Data → Snowflake
2. Server: `your_account.snowflakecomputing.com`
3. Warehouse: `REPORTING_WH`, Database: `ECOMMERCE_DW`, Schema: `ANALYTICS`
4. Use credentials for `powerbi_reader` (ANALYST_ROLE)
5. Import `FACT_ORDERS`, `MART_REVENUE_KPIS`, and dimension tables
6. Build measures: Total Revenue, AOV, Gross Margin %, Orders by Region

---

## Key Interview Talking Points

**"Walk me through your Snowflake architecture decisions."**  
> I separated compute into two warehouses — COMPUTE_WH for ELT and dbt, REPORTING_WH for Power BI. Both auto-suspend after idle time. This means the reporting team's query bursts don't compete with pipeline runs, and we're not paying for idle compute. I used Snowpipe for orders specifically because it's the highest-frequency source — auto-ingest fires on S3 event notifications rather than waiting for a scheduled Glue run.

**"Why a star schema over a flat denormalised table?"**  
> Dimension tables let us answer historical questions. With SCD Type 2 on DIM_CUSTOMERS, I can ask "what segment was this customer in when they placed this order?" — even if they've since moved segments. A flat table would overwrite that. The star schema also makes Power BI measures simpler to write and query performance faster due to Snowflake's pruning on dimension keys.

**"How did you handle data governance?"**  
> Three-tier RBAC: LOADER_ROLE can only write to RAW, TRANSFORMER can read RAW and write to STAGING and ANALYTICS, ANALYST_ROLE can only read ANALYTICS. FUTURE GRANTS mean any new table I create automatically inherits the right permissions — I don't have to re-grant manually. I also added a _loaded_at audit column on every RAW table so we can trace exactly when each row was ingested.

---

## Files

```
snowflake_ecommerce/
├── data/
│   └── generate_sample_data.py      ← generates 4 source files
├── glue/
│   └── glue_etl_job.py              ← AWS Glue PySpark ELT job
├── snowflake/
│   └── snowflake_setup.sql          ← full DDL: databases, RBAC, tables, Snowpipe
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_customers.sql
│   │   └── marts/
│   │       ├── fact_orders.sql
│   │       └── mart_revenue_kpis.sql
│   └── tests/
│       └── test_data_quality.sql
└── README.md
```
