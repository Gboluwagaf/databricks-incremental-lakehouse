# Databricks notebook source
# MAGIC %md
# MAGIC # Extract: Suppliers
# MAGIC Ingests supplier dimension from TPC-H source into the bronze layer.
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
    CREATE OR REPLACE TEMPORARY VIEW tv_suppliers_raw AS
    SELECT
        s_suppkey,
        s_name,
        s_address,
        s_nationkey,
        s_phone,
        s_acctbal,
        s_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.supplier
""")

# COMMAND ----------

# Stage 2: Quality filter
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_suppliers_cleaned AS
    SELECT *
    FROM tv_suppliers_raw
    WHERE s_suppkey IS NOT NULL
      AND s_name IS NOT NULL
""")

# COMMAND ----------

# Stage 3: Deduplication
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_suppliers_deduped AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY s_suppkey ORDER BY _ingested_at DESC) AS _rn
        FROM tv_suppliers_cleaned
    )
    WHERE _rn = 1
""")

# COMMAND ----------

# Final write
spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.suppliers
    SELECT
        s_suppkey, s_name, s_address, s_nationkey,
        s_phone, s_acctbal, s_comment,
        _ingested_at, _source_system, _batch_id
    FROM tv_suppliers_deduped
""")

# COMMAND ----------

df = spark.sql(f"SELECT count(*) AS row_count FROM {catalog}.{extract_schema}.suppliers")
df.display()
