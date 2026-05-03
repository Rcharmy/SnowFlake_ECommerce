"""
glue_etl_job.py
---------------
AWS Glue PySpark ELT job.
Reads raw files from S3, applies type casting + null handling,
then bulk-loads into Snowflake RAW schema via the Snowflake Spark connector.

Deploy this script to AWS Glue as a PySpark job.
Required Glue job parameters (set in AWS Console or via CLI):
  --S3_BUCKET         your-s3-bucket-name
  --S3_PREFIX         raw/ecommerce/
  --SF_ACCOUNT        your_account.snowflakecomputing.com
  --SF_DATABASE       ECOMMERCE_DW
  --SF_WAREHOUSE      COMPUTE_WH
  --SF_ROLE           LOADER_ROLE
  --SF_USER           glue_loader
  --SF_PASSWORD       (use Glue job parameter encryption)

Snowflake connection uses the official Spark connector JAR — add it as
a dependent JAR in your Glue job configuration:
  net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3
  net.snowflake:snowflake-jdbc:3.14.1
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

# ── INIT ─────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "S3_BUCKET", "S3_PREFIX",
    "SF_ACCOUNT", "SF_DATABASE", "SF_WAREHOUSE",
    "SF_ROLE", "SF_USER", "SF_PASSWORD"
])
sc         = SparkContext()
glueContext = GlueContext(sc)
spark      = glueContext.spark_session
job        = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BASE  = f"s3://{args['S3_BUCKET']}/{args['S3_PREFIX']}"
SF_OPTS  = {
    "sfURL":       args["SF_ACCOUNT"],
    "sfDatabase":  args["SF_DATABASE"],
    "sfWarehouse": args["SF_WAREHOUSE"],
    "sfRole":      args["SF_ROLE"],
    "sfUser":      args["SF_USER"],
    "sfPassword":  args["SF_PASSWORD"],
    "sfSchema":    "RAW",
    "application": "glue_etl_job"
}
SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


def write_to_snowflake(df, table_name, mode="append"):
    """Bulk-load a Spark DataFrame into a Snowflake RAW table."""
    df.write \
      .format(SNOWFLAKE_SOURCE) \
      .options(**SF_OPTS) \
      .option("dbtable", table_name) \
      .mode(mode) \
      .save()
    print(f"  → Loaded {df.count():,} rows into RAW.{table_name}")


# ── 1. ORDERS ────────────────────────────────────────────────
print("Processing orders.csv ...")
orders_schema = StructType([
    StructField("order_id",        StringType()),
    StructField("customer_id",     StringType()),
    StructField("product_id",      StringType()),
    StructField("order_date",      StringType()),
    StructField("quantity",        IntegerType()),
    StructField("unit_price",      DoubleType()),
    StructField("discount_pct",    DoubleType()),
    StructField("revenue",         DoubleType()),
    StructField("order_status",    StringType()),
    StructField("shipping_region", StringType()),
    StructField("payment_method",  StringType()),
])
orders_raw = spark.read.csv(f"{S3_BASE}orders.csv", header=True, schema=orders_schema)
orders_clean = (
    orders_raw
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("revenue") > 0)
    .withColumn("order_date", F.to_timestamp("order_date"))
    .withColumn("_loaded_at", F.current_timestamp())
    .dropDuplicates(["order_id"])
)
write_to_snowflake(orders_clean, "RAW_ORDERS")


# ── 2. PRODUCTS ──────────────────────────────────────────────
print("Processing products.csv ...")
products_schema = StructType([
    StructField("product_id",    StringType()),
    StructField("product_name",  StringType()),
    StructField("category",      StringType()),
    StructField("sub_category",  StringType()),
    StructField("unit_price",    DoubleType()),
    StructField("price_tier",    StringType()),
    StructField("in_stock",      StringType()),    # comes as string from CSV
    StructField("supplier_id",   StringType()),
])
products_raw = spark.read.csv(f"{S3_BASE}products.csv", header=True, schema=products_schema)
products_clean = (
    products_raw
    .filter(F.col("product_id").isNotNull())
    .withColumn("in_stock", F.when(F.lower("in_stock") == "true", True).otherwise(False).cast(BooleanType()))
    .withColumn("_loaded_at", F.current_timestamp())
    .dropDuplicates(["product_id"])
)
write_to_snowflake(products_clean, "RAW_PRODUCTS")


# ── 3. CUSTOMERS (JSON) ──────────────────────────────────────
print("Processing customers.json ...")
customers_raw = spark.read.json(f"{S3_BASE}customers.json")
customers_clean = (
    customers_raw
    .filter(F.col("customer_id").isNotNull())
    .withColumn("signup_date", F.to_timestamp("signup_date"))
    .withColumn("_loaded_at", F.current_timestamp())
    .dropDuplicates(["customer_id"])
    .select(
        "customer_id", "email", "first_name", "last_name",
        "region", "segment", "signup_date", "is_active", "_loaded_at"
    )
)
write_to_snowflake(customers_clean, "RAW_CUSTOMERS")


# ── 4. WEB EVENTS (JSON) ─────────────────────────────────────
print("Processing web_events.json ...")
events_raw = spark.read.json(f"{S3_BASE}web_events.json")
events_clean = (
    events_raw
    .filter(F.col("event_id").isNotNull())
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("product_id", F.when(F.col("product_id").isNull(), "UNKNOWN").otherwise(F.col("product_id")))
    .withColumn("_loaded_at", F.current_timestamp())
    .dropDuplicates(["event_id"])
)
write_to_snowflake(events_clean, "RAW_WEB_EVENTS")


# ── DONE ──────────────────────────────────────────────────────
job.commit()
print("Glue ELT job completed successfully.")
