# Databricks notebook source
# MAGIC %md
# MAGIC # Refined: Order Details (Denormalized Fact)
# MAGIC Joins orders, line items, and parts into a denormalized order details fact table.
# MAGIC Applies business logic: net revenue, shipping delay analysis, order classification.
# MAGIC
# MAGIC **Source**: Bronze orders, lineitem, parts
# MAGIC **Grain**: One row per order line item

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("extract_schema", "bronze")
dbutils.widgets.text("refined_schema", "silver")

catalog = dbutils.widgets.get("catalog")
extract_schema = dbutils.widgets.get("extract_schema")
refined_schema = dbutils.widgets.get("refined_schema")

batch_id = spark.sql("SELECT concat('batch_', date_format(current_timestamp(), 'yyyyMMdd_HHmmss'))").collect()[0][0]

# COMMAND ----------

# Stage 1: Join bronze orders + lineitem + parts
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_order_details_joined AS
    SELECT
        o.o_orderkey        AS order_key,
        li.l_linenumber     AS line_number,
        o.o_custkey         AS customer_key,
        li.l_partkey        AS part_key,
        li.l_suppkey        AS supplier_key,
        o.o_orderdate       AS order_date,
        o.o_orderstatus     AS order_status,
        o.o_orderpriority   AS order_priority,
        p.p_name            AS part_name,
        p.p_brand           AS part_brand,
        p.p_type            AS part_type,
        li.l_quantity       AS quantity,
        li.l_extendedprice  AS extended_price,
        li.l_discount       AS discount_pct,
        li.l_tax            AS tax_pct,
        li.l_shipdate       AS ship_date,
        li.l_commitdate     AS commit_date,
        li.l_receiptdate    AS receipt_date,
        li.l_shipmode       AS ship_mode,
        li.l_returnflag     AS return_flag
    FROM {catalog}.{extract_schema}.orders o
    INNER JOIN {catalog}.{extract_schema}.lineitem li
        ON o.o_orderkey = li.l_orderkey
    LEFT JOIN {catalog}.{extract_schema}.parts p
        ON li.l_partkey = p.p_partkey
""")

# COMMAND ----------

# Stage 2: Apply business calculations
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_order_details_calculated AS
    SELECT
        order_key,
        line_number,
        customer_key,
        part_key,
        supplier_key,
        order_date,
        order_status,
        order_priority,
        part_name,
        part_brand,
        part_type,
        quantity,
        ROUND(extended_price / NULLIF(quantity, 0), 2)             AS unit_price,
        extended_price,
        discount_pct,
        tax_pct,
        ROUND(extended_price * (1 - discount_pct), 2)             AS net_revenue,
        ROUND(extended_price * (1 - discount_pct) * tax_pct, 2)   AS tax_amount,
        ROUND(extended_price * (1 - discount_pct) * (1 + tax_pct), 2) AS total_charge,
        ship_date,
        commit_date,
        receipt_date,
        ship_mode,
        DATEDIFF(ship_date, order_date)                            AS shipping_delay_days,
        DATEDIFF(receipt_date, ship_date)                          AS delivery_delay_days,
        CASE WHEN ship_date > commit_date THEN TRUE ELSE FALSE END AS is_late_shipment,
        return_flag,
        YEAR(order_date)                                           AS order_year,
        MONTH(order_date)                                          AS order_month,
        QUARTER(order_date)                                        AS order_quarter
    FROM tv_order_details_joined
""")

# COMMAND ----------

# Stage 3: Quality gate — filter out invalid records
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_order_details_final AS
    SELECT
        *,
        current_timestamp() AS _refined_at,
        '{batch_id}'        AS _batch_id
    FROM tv_order_details_calculated
    WHERE quantity > 0
      AND extended_price > 0
      AND net_revenue >= 0
""")

# COMMAND ----------

# Final write
spark.sql(f"""
    INSERT OVERWRITE {catalog}.{refined_schema}.order_details
    SELECT
        order_key, line_number, customer_key, part_key, supplier_key,
        order_date, order_status, order_priority,
        part_name, part_brand, part_type,
        quantity, unit_price, extended_price, discount_pct, tax_pct,
        net_revenue, tax_amount, total_charge,
        ship_date, commit_date, receipt_date, ship_mode,
        shipping_delay_days, delivery_delay_days, is_late_shipment,
        return_flag, order_year, order_month, order_quarter,
        _refined_at, _batch_id
    FROM tv_order_details_final
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT
        count(*)                                      AS total_rows,
        count(CASE WHEN net_revenue < 0 THEN 1 END)  AS negative_revenue,
        min(order_date)                               AS min_date,
        max(order_date)                               AS max_date
    FROM {catalog}.{refined_schema}.order_details
""")
df.display()
