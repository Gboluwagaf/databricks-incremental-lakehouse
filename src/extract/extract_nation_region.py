# Databricks notebook source
# MAGIC %md
# MAGIC # Extract: Nation & Region (Reference Data)
# MAGIC Full refresh of small reference/lookup tables from TPC-H source.
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

# MAGIC %md
# MAGIC ### Nation

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_nation_raw AS
    SELECT
        n_nationkey,
        n_name,
        n_regionkey,
        n_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.nation
""")

# COMMAND ----------

spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.nation
    SELECT
        n_nationkey, n_name, n_regionkey, n_comment,
        _ingested_at, _source_system, _batch_id
    FROM tv_nation_raw
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Region

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_region_raw AS
    SELECT
        r_regionkey,
        r_name,
        r_comment,
        current_timestamp()  AS _ingested_at,
        'tpch'               AS _source_system,
        '{batch_id}'         AS _batch_id
    FROM {source_catalog}.{source_schema}.region
""")

# COMMAND ----------

spark.sql(f"""
    INSERT OVERWRITE {catalog}.{extract_schema}.region
    SELECT
        r_regionkey, r_name, r_comment,
        _ingested_at, _source_system, _batch_id
    FROM tv_region_raw
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT 'nation' AS table_name, count(*) AS row_count FROM {catalog}.{extract_schema}.nation
    UNION ALL
    SELECT 'region', count(*) FROM {catalog}.{extract_schema}.region
""")
df.display()
