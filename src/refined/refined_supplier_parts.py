# Databricks notebook source
# MAGIC %md
# MAGIC # Refined: Supplier Parts Catalog
# MAGIC Enriches supplier data with part catalog, cost metrics, and regional competitiveness.
# MAGIC
# MAGIC **Source**: Bronze suppliers, partsupp, parts, nation, region
# MAGIC **Grain**: One row per supplier-part combination

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("extract_schema", "bronze")
dbutils.widgets.text("refined_schema", "silver")

catalog = dbutils.widgets.get("catalog")
extract_schema = dbutils.widgets.get("extract_schema")
refined_schema = dbutils.widgets.get("refined_schema")

batch_id = spark.sql("SELECT concat('batch_', date_format(current_timestamp(), 'yyyyMMdd_HHmmss'))").collect()[0][0]

# COMMAND ----------

# Stage 1: Join suppliers + partsupp + parts + geography
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_supplier_parts_joined AS
    SELECT
        s.s_suppkey         AS supplier_key,
        s.s_name            AS supplier_name,
        n.n_name            AS supplier_nation,
        r.r_name            AS supplier_region,
        s.s_acctbal         AS supplier_acct_balance,
        p.p_partkey         AS part_key,
        p.p_name            AS part_name,
        p.p_brand           AS part_brand,
        p.p_type            AS part_type,
        p.p_size            AS part_size,
        p.p_retailprice     AS retail_price,
        ps.ps_supplycost    AS supply_cost,
        ps.ps_availqty      AS available_qty
    FROM {catalog}.{extract_schema}.suppliers s
    INNER JOIN {catalog}.{extract_schema}.partsupp ps
        ON s.s_suppkey = ps.ps_suppkey
    INNER JOIN {catalog}.{extract_schema}.parts p
        ON ps.ps_partkey = p.p_partkey
    LEFT JOIN {catalog}.{extract_schema}.nation n
        ON s.s_nationkey = n.n_nationkey
    LEFT JOIN {catalog}.{extract_schema}.region r
        ON n.n_regionkey = r.r_regionkey
""")

# COMMAND ----------

# Stage 2: Calculate cost margin metrics
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_supplier_parts_margin AS
    SELECT
        *,
        ROUND(retail_price - supply_cost, 2)                                AS cost_margin,
        ROUND((retail_price - supply_cost) / NULLIF(retail_price, 0), 4)    AS margin_pct
    FROM tv_supplier_parts_joined
""")

# COMMAND ----------

# Stage 3: Regional competitiveness ranking
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_supplier_parts_final AS
    SELECT
        supplier_key,
        supplier_name,
        supplier_nation,
        supplier_region,
        supplier_acct_balance,
        part_key,
        part_name,
        part_brand,
        part_type,
        part_size,
        retail_price,
        supply_cost,
        available_qty,
        cost_margin,
        margin_pct,
        DENSE_RANK() OVER (
            PARTITION BY supplier_region, part_type
            ORDER BY supply_cost ASC
        )                                                                   AS cost_rank_in_region,
        CASE
            WHEN DENSE_RANK() OVER (
                PARTITION BY supplier_region, part_type
                ORDER BY supply_cost ASC
            ) = 1 THEN TRUE
            ELSE FALSE
        END                                                                 AS is_cheapest_in_region,
        ROUND(
            AVG(supply_cost) OVER (PARTITION BY supplier_region, part_type), 2
        )                                                                   AS avg_region_cost,
        ROUND(
            supply_cost / NULLIF(
                AVG(supply_cost) OVER (PARTITION BY supplier_region, part_type), 0
            ), 4
        )                                                                   AS cost_vs_region_avg,
        current_timestamp()                                                 AS _refined_at,
        '{batch_id}'                                                        AS _batch_id
    FROM tv_supplier_parts_margin
""")

# COMMAND ----------

# Final write
spark.sql(f"""
    INSERT OVERWRITE {catalog}.{refined_schema}.supplier_parts
    SELECT
        supplier_key, supplier_name, supplier_nation, supplier_region,
        supplier_acct_balance, part_key, part_name, part_brand, part_type,
        part_size, retail_price, supply_cost, available_qty,
        cost_margin, margin_pct, cost_rank_in_region, is_cheapest_in_region,
        avg_region_cost, cost_vs_region_avg, _refined_at, _batch_id
    FROM tv_supplier_parts_final
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT supplier_region, count(DISTINCT supplier_key) AS suppliers, count(DISTINCT part_key) AS parts
    FROM {catalog}.{refined_schema}.supplier_parts
    GROUP BY supplier_region
    ORDER BY suppliers DESC
""")
df.display()
