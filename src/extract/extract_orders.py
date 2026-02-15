# Databricks notebook source
# MAGIC %md
# MAGIC # Extract: Orders
# MAGIC Ingests orders from TPC-H source into the bronze layer.
# MAGIC
# MAGIC **Pattern**: Staged temp views → single INSERT OVERWRITE

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("extract_schema", "bronze")
dbutils.widgets.text("source_catalog", "samples")
dbutils.widgets.text("source_schema", "tpch")

catalog = dbutils.widgets.get("catalog")
extract_schema = dbutils.widgets.get("extract_schema")
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")

batch_id = spark.sql("SELECT concat('batch_', date_format(current_timestamp(), 'yyyyMMdd_HHmmss'))").collect()[0][0]

# COMMAND ----------

# Stage 1: Pull raw source with audit columns
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_orders_raw AS
    SELECT
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.orders
""")

# COMMAND ----------

# Stage 2: Quality filter — remove nulls on primary key
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_orders_cleaned AS
    SELECT *
    FROM tv_orders_raw
    WHERE o_orderkey IS NOT NULL
      AND o_orderdate IS NOT NULL
""")

# COMMAND ----------

# Stage 3: Deduplication — keep latest by order key
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_orders_deduped AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY o_orderkey ORDER BY _ingested_at DESC) AS _rn
        FROM tv_orders_cleaned
    )
    WHERE _rn = 1
""")

# COMMAND ----------

# Final write: INSERT OVERWRITE into target table
spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.orders
    SELECT
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment,
        _ingested_at,
        _source_system,
        _batch_id
    FROM tv_orders_deduped
""")

# COMMAND ----------

# Validation
df = spark.sql(f"SELECT count(*) AS row_count FROM {catalog}.{extract_schema}.orders")
df.display()
