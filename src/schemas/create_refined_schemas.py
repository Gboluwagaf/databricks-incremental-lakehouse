# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Setup: Silver (Refined) Layer
# MAGIC Creates the silver schema and all silver table DDLs.
# MAGIC Idempotent — safe to run on every pipeline execution.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse", "Target Catalog")
dbutils.widgets.text("refined_schema", "silver", "Refined Schema")

catalog = dbutils.widgets.get("catalog")
refined_schema = dbutils.widgets.get("refined_schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{refined_schema} COMMENT 'Silver layer: Cleansed, enriched, and denormalized data'")

# COMMAND ----------

# order_details — denormalized fact
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{refined_schema}.order_details (
        order_key           BIGINT,
        line_number         INT,
        customer_key        BIGINT,
        part_key            BIGINT,
        supplier_key        BIGINT,
        order_date          DATE,
        order_status        STRING,
        order_priority      STRING,
        part_name           STRING,
        part_brand          STRING,
        part_type           STRING,
        quantity            DECIMAL(15,2),
        unit_price          DECIMAL(15,2),
        extended_price      DECIMAL(15,2),
        discount_pct        DECIMAL(15,2),
        tax_pct             DECIMAL(15,2),
        net_revenue         DECIMAL(15,2),
        tax_amount          DECIMAL(15,2),
        total_charge        DECIMAL(15,2),
        ship_date           DATE,
        commit_date         DATE,
        receipt_date        DATE,
        ship_mode           STRING,
        shipping_delay_days INT,
        delivery_delay_days INT,
        is_late_shipment    BOOLEAN,
        return_flag         STRING,
        order_year          INT,
        order_month         INT,
        order_quarter       INT,
        _refined_at         TIMESTAMP,
        _batch_id           STRING
    )
    USING DELTA
    PARTITIONED BY (order_year)
    COMMENT 'Silver: Denormalized order details with business metrics'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'silver')
""")

# COMMAND ----------

# customer_orders — customer profile with RFM
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{refined_schema}.customer_orders (
        customer_key           BIGINT,
        customer_name          STRING,
        market_segment         STRING,
        nation_name            STRING,
        region_name            STRING,
        account_balance        DECIMAL(15,2),
        total_orders           BIGINT,
        total_revenue          DECIMAL(18,2),
        avg_order_value        DECIMAL(15,2),
        first_order_date       DATE,
        last_order_date        DATE,
        days_since_last_order  INT,
        order_frequency_days   DECIMAL(10,2),
        fulfilled_orders       BIGINT,
        open_orders            BIGINT,
        partial_orders         BIGINT,
        fulfillment_rate       DECIMAL(5,2),
        customer_tenure_days   INT,
        rfm_recency_score      INT,
        rfm_frequency_score    INT,
        rfm_monetary_score     INT,
        customer_segment       STRING,
        _refined_at            TIMESTAMP,
        _batch_id              STRING
    )
    USING DELTA
    COMMENT 'Silver: Customer profile with RFM segmentation'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'silver')
""")

# COMMAND ----------

# supplier_parts — supplier catalog with cost metrics
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{refined_schema}.supplier_parts (
        supplier_key          BIGINT,
        supplier_name         STRING,
        supplier_nation       STRING,
        supplier_region       STRING,
        supplier_acct_balance DECIMAL(15,2),
        part_key              BIGINT,
        part_name             STRING,
        part_brand            STRING,
        part_type             STRING,
        part_size             INT,
        retail_price          DECIMAL(15,2),
        supply_cost           DECIMAL(15,2),
        available_qty         INT,
        cost_margin           DECIMAL(15,2),
        margin_pct            DECIMAL(8,4),
        cost_rank_in_region   INT,
        is_cheapest_in_region BOOLEAN,
        avg_region_cost       DECIMAL(15,2),
        cost_vs_region_avg    DECIMAL(8,4),
        _refined_at           TIMESTAMP,
        _batch_id             STRING
    )
    USING DELTA
    COMMENT 'Silver: Supplier-part catalog with cost competitiveness'
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true', 'quality' = 'silver')
""")

# COMMAND ----------

print(f"[schemas] Silver layer DDL complete: {catalog}.{refined_schema}")
