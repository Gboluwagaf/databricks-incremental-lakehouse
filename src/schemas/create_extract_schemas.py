# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Setup: Bronze (Extract) Layer
# MAGIC Creates the Unity Catalog, bronze schema, and all bronze table DDLs.
# MAGIC Idempotent — safe to run on every pipeline execution.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse", "Target Catalog")
dbutils.widgets.text("extract_schema", "bronze", "Extract Schema")

catalog = dbutils.widgets.get("catalog")
extract_schema = dbutils.widgets.get("extract_schema")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog} COMMENT 'Lakehouse catalog for medallion architecture'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{extract_schema} COMMENT 'Bronze layer: Raw ingested data with audit columns'")

# COMMAND ----------

# orders
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.orders (
        o_orderkey       BIGINT,
        o_custkey        BIGINT,
        o_orderstatus    STRING,
        o_totalprice     DECIMAL(15,2),
        o_orderdate      DATE,
        o_orderpriority  STRING,
        o_clerk          STRING,
        o_shippriority   INT,
        o_comment        STRING,
        _ingested_at     TIMESTAMP,
        _source_system   STRING,
        _batch_id        STRING
    )
    USING DELTA
    COMMENT 'Bronze: orders from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

# customers
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.customers (
        c_custkey      BIGINT,
        c_name         STRING,
        c_address      STRING,
        c_nationkey    BIGINT,
        c_phone        STRING,
        c_acctbal      DECIMAL(15,2),
        c_mktsegment   STRING,
        c_comment      STRING,
        _ingested_at   TIMESTAMP,
        _source_system STRING,
        _batch_id      STRING
    )
    USING DELTA
    COMMENT 'Bronze: customers from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

# lineitem
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.lineitem (
        l_orderkey      BIGINT,
        l_partkey       BIGINT,
        l_suppkey       BIGINT,
        l_linenumber    INT,
        l_quantity      DECIMAL(15,2),
        l_extendedprice DECIMAL(15,2),
        l_discount      DECIMAL(15,2),
        l_tax           DECIMAL(15,2),
        l_returnflag    STRING,
        l_linestatus    STRING,
        l_shipdate      DATE,
        l_commitdate    DATE,
        l_receiptdate   DATE,
        l_shipinstruct  STRING,
        l_shipmode      STRING,
        l_comment       STRING,
        _ingested_at    TIMESTAMP,
        _source_system  STRING,
        _batch_id       STRING
    )
    USING DELTA
    COMMENT 'Bronze: lineitem from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

# suppliers
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.suppliers (
        s_suppkey      BIGINT,
        s_name         STRING,
        s_address      STRING,
        s_nationkey    BIGINT,
        s_phone        STRING,
        s_acctbal      DECIMAL(15,2),
        s_comment      STRING,
        _ingested_at   TIMESTAMP,
        _source_system STRING,
        _batch_id      STRING
    )
    USING DELTA
    COMMENT 'Bronze: suppliers from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

# parts
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.parts (
        p_partkey     BIGINT,
        p_name        STRING,
        p_mfgr        STRING,
        p_brand       STRING,
        p_type        STRING,
        p_size        INT,
        p_container   STRING,
        p_retailprice DECIMAL(15,2),
        p_comment     STRING,
        _ingested_at    TIMESTAMP,
        _source_system  STRING,
        _batch_id       STRING
    )
    USING DELTA
    COMMENT 'Bronze: parts from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

# partsupp
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.partsupp (
        ps_partkey      BIGINT,
        ps_suppkey      BIGINT,
        ps_availqty     INT,
        ps_supplycost   DECIMAL(15,2),
        ps_comment      STRING,
        _ingested_at    TIMESTAMP,
        _source_system  STRING,
        _batch_id       STRING
    )
    USING DELTA
    COMMENT 'Bronze: part-supplier bridge from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

# nation
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.nation (
        n_nationkey  BIGINT,
        n_name       STRING,
        n_regionkey  BIGINT,
        n_comment    STRING,
        _ingested_at    TIMESTAMP,
        _source_system  STRING,
        _batch_id       STRING
    )
    USING DELTA
    COMMENT 'Bronze: nation reference from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

# region
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{extract_schema}.region (
        r_regionkey  BIGINT,
        r_name       STRING,
        r_comment    STRING,
        _ingested_at    TIMESTAMP,
        _source_system  STRING,
        _batch_id       STRING
    )
    USING DELTA
    COMMENT 'Bronze: region reference from TPC-H'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'quality' = 'bronze')
""")

# COMMAND ----------

print(f"[schemas] Bronze layer DDL complete: {catalog}.{extract_schema}")
