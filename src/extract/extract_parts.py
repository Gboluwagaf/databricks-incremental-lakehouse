# Databricks notebook source
# MAGIC %md
# MAGIC # Extract: Parts & Part-Supplier
# MAGIC Ingests part catalog and part-supplier bridge from TPC-H source into bronze.
# MAGIC
# MAGIC **Pattern**: Staged temp views → single INSERT OVERWRITE (two tables)

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

# MAGIC %md
# MAGIC ### Parts

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_parts_raw AS
    SELECT
        p_partkey, p_name, p_mfgr, p_brand, p_type,
        p_size, p_container, p_retailprice, p_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.part
""")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_parts_cleaned AS
    SELECT *
    FROM tv_parts_raw
    WHERE p_partkey IS NOT NULL
""")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_parts_deduped AS
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY p_partkey ORDER BY _ingested_at DESC) AS _rn
        FROM tv_parts_cleaned
    )
    WHERE _rn = 1
""")

# COMMAND ----------

spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.parts
    SELECT
        p_partkey, p_name, p_mfgr, p_brand, p_type,
        p_size, p_container, p_retailprice, p_comment,
        _ingested_at, _source_system, _batch_id
    FROM tv_parts_deduped
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part-Supplier Bridge

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_partsupp_raw AS
    SELECT
        ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.partsupp
""")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_partsupp_cleaned AS
    SELECT *
    FROM tv_partsupp_raw
    WHERE ps_partkey IS NOT NULL
      AND ps_suppkey IS NOT NULL
""")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_partsupp_deduped AS
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ps_partkey, ps_suppkey
            ORDER BY _ingested_at DESC
        ) AS _rn
        FROM tv_partsupp_cleaned
    )
    WHERE _rn = 1
""")

# COMMAND ----------

spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.partsupp
    SELECT
        ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment,
        _ingested_at, _source_system, _batch_id
    FROM tv_partsupp_deduped
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT 'parts' AS table_name, count(*) AS row_count FROM {catalog}.{extract_schema}.parts
    UNION ALL
    SELECT 'partsupp', count(*) FROM {catalog}.{extract_schema}.partsupp
""")
df.display()
