# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC Validates data integrity across all three layers of the medallion architecture.
# MAGIC
# MAGIC **Checks**: Row counts, null validation, referential integrity, business rules, freshness

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("extract_schema", "bronze")
dbutils.widgets.text("refined_schema", "silver")
dbutils.widgets.text("views_schema", "gold")

catalog = dbutils.widgets.get("catalog")
extract_schema = dbutils.widgets.get("extract_schema")
refined_schema = dbutils.widgets.get("refined_schema")
views_schema = dbutils.widgets.get("views_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 1: Row Counts

# COMMAND ----------

df_rows = spark.sql(f"""
    SELECT 'Row Count' AS check_type, table_name, row_count,
           CASE WHEN row_count > 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM (
        SELECT 'bronze.orders'          AS table_name, count(*) AS row_count FROM {catalog}.{extract_schema}.orders
        UNION ALL SELECT 'bronze.customers',    count(*) FROM {catalog}.{extract_schema}.customers
        UNION ALL SELECT 'bronze.lineitem',     count(*) FROM {catalog}.{extract_schema}.lineitem
        UNION ALL SELECT 'bronze.suppliers',    count(*) FROM {catalog}.{extract_schema}.suppliers
        UNION ALL SELECT 'bronze.parts',        count(*) FROM {catalog}.{extract_schema}.parts
        UNION ALL SELECT 'bronze.partsupp',     count(*) FROM {catalog}.{extract_schema}.partsupp
        UNION ALL SELECT 'bronze.nation',       count(*) FROM {catalog}.{extract_schema}.nation
        UNION ALL SELECT 'bronze.region',       count(*) FROM {catalog}.{extract_schema}.region
        UNION ALL SELECT 'silver.order_details',   count(*) FROM {catalog}.{refined_schema}.order_details
        UNION ALL SELECT 'silver.customer_orders', count(*) FROM {catalog}.{refined_schema}.customer_orders
        UNION ALL SELECT 'silver.supplier_parts',  count(*) FROM {catalog}.{refined_schema}.supplier_parts
    )
""")
df_rows.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 2: Null Validation

# COMMAND ----------

df_nulls = spark.sql(f"""
    SELECT 'Null Check' AS check_type, check_name, null_count,
           CASE WHEN null_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM (
        SELECT 'orders.o_orderkey'         AS check_name, count(CASE WHEN o_orderkey IS NULL THEN 1 END) AS null_count FROM {catalog}.{extract_schema}.orders
        UNION ALL SELECT 'customers.c_custkey',    count(CASE WHEN c_custkey IS NULL THEN 1 END)  FROM {catalog}.{extract_schema}.customers
        UNION ALL SELECT 'lineitem.l_orderkey',    count(CASE WHEN l_orderkey IS NULL THEN 1 END) FROM {catalog}.{extract_schema}.lineitem
        UNION ALL SELECT 'order_details.net_revenue', count(CASE WHEN net_revenue IS NULL THEN 1 END) FROM {catalog}.{refined_schema}.order_details
        UNION ALL SELECT 'customer_orders.customer_key', count(CASE WHEN customer_key IS NULL THEN 1 END) FROM {catalog}.{refined_schema}.customer_orders
    )
""")
df_nulls.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 3: Referential Integrity

# COMMAND ----------

df_ref = spark.sql(f"""
    SELECT 'Referential Integrity' AS check_type, check_name, orphan_count,
           CASE WHEN orphan_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM (
        SELECT 'orders -> customers' AS check_name, count(*) AS orphan_count
        FROM {catalog}.{extract_schema}.orders o
        LEFT JOIN {catalog}.{extract_schema}.customers c ON o.o_custkey = c.c_custkey
        WHERE c.c_custkey IS NULL
        UNION ALL
        SELECT 'lineitem -> orders', count(*)
        FROM {catalog}.{extract_schema}.lineitem li
        LEFT JOIN {catalog}.{extract_schema}.orders o ON li.l_orderkey = o.o_orderkey
        WHERE o.o_orderkey IS NULL
        UNION ALL
        SELECT 'order_details -> orders', count(*)
        FROM {catalog}.{refined_schema}.order_details od
        LEFT JOIN {catalog}.{extract_schema}.orders o ON od.order_key = o.o_orderkey
        WHERE o.o_orderkey IS NULL
    )
""")
df_ref.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 4: Business Rules

# COMMAND ----------

df_rules = spark.sql(f"""
    SELECT 'Business Rule' AS check_type, check_name, violation_count,
           CASE WHEN violation_count = 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM (
        SELECT 'No negative net_revenue'     AS check_name, count(CASE WHEN net_revenue < 0 THEN 1 END) AS violation_count FROM {catalog}.{refined_schema}.order_details
        UNION ALL SELECT 'No zero/neg quantity',    count(CASE WHEN quantity <= 0 THEN 1 END) FROM {catalog}.{refined_schema}.order_details
        UNION ALL SELECT 'Discount range 0-1',      count(CASE WHEN discount_pct < 0 OR discount_pct > 1 THEN 1 END) FROM {catalog}.{refined_schema}.order_details
        UNION ALL SELECT 'Tax range 0-1',            count(CASE WHEN tax_pct < 0 OR tax_pct > 1 THEN 1 END) FROM {catalog}.{refined_schema}.order_details
        UNION ALL SELECT 'Fulfillment rate 0-100',   count(CASE WHEN fulfillment_rate < 0 OR fulfillment_rate > 100 THEN 1 END) FROM {catalog}.{refined_schema}.customer_orders
        UNION ALL SELECT 'Segment not null',          count(CASE WHEN customer_segment IS NULL THEN 1 END) FROM {catalog}.{refined_schema}.customer_orders
    )
""")
df_rules.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 5: Freshness

# COMMAND ----------

df_fresh = spark.sql(f"""
    SELECT 'Freshness' AS check_type, table_name, last_refresh, hours_since,
           CASE WHEN hours_since <= 25 THEN 'PASS' ELSE 'STALE' END AS status
    FROM (
        SELECT 'bronze.orders' AS table_name, max(_ingested_at) AS last_refresh,
               round((unix_timestamp(current_timestamp()) - unix_timestamp(max(_ingested_at))) / 3600, 1) AS hours_since
        FROM {catalog}.{extract_schema}.orders
        UNION ALL
        SELECT 'silver.order_details', max(_refined_at),
               round((unix_timestamp(current_timestamp()) - unix_timestamp(max(_refined_at))) / 3600, 1)
        FROM {catalog}.{refined_schema}.order_details
        UNION ALL
        SELECT 'silver.customer_orders', max(_refined_at),
               round((unix_timestamp(current_timestamp()) - unix_timestamp(max(_refined_at))) / 3600, 1)
        FROM {catalog}.{refined_schema}.customer_orders
    )
""")
df_fresh.display()

# COMMAND ----------

print("ALL QUALITY CHECKS COMPLETE")
