# Databricks notebook source
# MAGIC %md
# MAGIC # Extract: Customers
# MAGIC Ingests customer dimension from TPC-H source into the bronze layer.
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
    CREATE OR REPLACE TEMPORARY VIEW tv_customers_raw AS
    SELECT
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.customer
""")

# COMMAND ----------

# Stage 2: Quality filter
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_customers_cleaned AS
    SELECT *
    FROM tv_customers_raw
    WHERE c_custkey IS NOT NULL
      AND c_name IS NOT NULL
""")

# COMMAND ----------

# Stage 3: Deduplication on primary key
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_customers_deduped AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY c_custkey ORDER BY _ingested_at DESC) AS _rn
        FROM tv_customers_cleaned
    )
    WHERE _rn = 1
""")

# COMMAND ----------

# Final write
spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.customers
    SELECT
        c_custkey, c_name, c_address, c_nationkey,
        c_phone, c_acctbal, c_mktsegment, c_comment,
        _ingested_at, _source_system, _batch_id
    FROM tv_customers_deduped
""")

# COMMAND ----------

df = spark.sql(f"SELECT count(*) AS row_count FROM {catalog}.{extract_schema}.customers")
df.display()
