# Databricks notebook source
# MAGIC %md
# MAGIC # Extract: Line Items
# MAGIC Ingests line items (order details) from TPC-H source into the bronze layer.
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
    CREATE OR REPLACE TEMPORARY VIEW tv_lineitem_raw AS
    SELECT
        l_orderkey,
        l_partkey,
        l_suppkey,
        l_linenumber,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_linestatus,
        l_shipdate,
        l_commitdate,
        l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.lineitem
""")

# COMMAND ----------

# Stage 2: Quality filter — valid keys and positive quantities
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_lineitem_cleaned AS
    SELECT *
    FROM tv_lineitem_raw
    WHERE l_orderkey IS NOT NULL
      AND l_linenumber IS NOT NULL
      AND l_quantity > 0
      AND l_extendedprice > 0
""")

# COMMAND ----------

# Stage 3: Deduplication on composite key
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_lineitem_deduped AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY l_orderkey, l_linenumber
                ORDER BY _ingested_at DESC
            ) AS _rn
        FROM tv_lineitem_cleaned
    )
    WHERE _rn = 1
""")

# COMMAND ----------

# Final write
spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.lineitem
    SELECT
        l_orderkey, l_partkey, l_suppkey, l_linenumber,
        l_quantity, l_extendedprice, l_discount, l_tax,
        l_returnflag, l_linestatus,
        l_shipdate, l_commitdate, l_receiptdate,
        l_shipinstruct, l_shipmode, l_comment,
        _ingested_at, _source_system, _batch_id
    FROM tv_lineitem_deduped
""")

# COMMAND ----------

df = spark.sql(f"SELECT count(*) AS row_count FROM {catalog}.{extract_schema}.lineitem")
df.display()
