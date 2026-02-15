# Databricks notebook source
# MAGIC %md
# MAGIC # Refined: Customer Orders Profile
# MAGIC Aggregates customer data with order history. Builds RFM scoring and segmentation.
# MAGIC
# MAGIC **Source**: Bronze customers, orders, nation, region
# MAGIC **Grain**: One row per customer

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("extract_schema", "bronze")
dbutils.widgets.text("refined_schema", "silver")

catalog = dbutils.widgets.get("catalog")
extract_schema = dbutils.widgets.get("extract_schema")
refined_schema = dbutils.widgets.get("refined_schema")

batch_id = spark.sql("SELECT concat('batch_', date_format(current_timestamp(), 'yyyyMMdd_HHmmss'))").collect()[0][0]

# COMMAND ----------

# Stage 1: Join customers with geographic info
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_customers_geo AS
    SELECT
        c.c_custkey     AS customer_key,
        c.c_name        AS customer_name,
        c.c_mktsegment  AS market_segment,
        c.c_acctbal     AS account_balance,
        n.n_name        AS nation_name,
        r.r_name        AS region_name
    FROM {catalog}.{extract_schema}.customers c
    LEFT JOIN {catalog}.{extract_schema}.nation n
        ON c.c_nationkey = n.n_nationkey
    LEFT JOIN {catalog}.{extract_schema}.region r
        ON n.n_regionkey = r.r_regionkey
""")

# COMMAND ----------

# Stage 2: Aggregate order metrics per customer
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_customer_order_agg AS
    SELECT
        cg.customer_key,
        cg.customer_name,
        cg.market_segment,
        cg.nation_name,
        cg.region_name,
        cg.account_balance,
        COUNT(o.o_orderkey)                                             AS total_orders,
        COALESCE(SUM(o.o_totalprice), 0)                                AS total_revenue,
        COALESCE(ROUND(AVG(o.o_totalprice), 2), 0)                      AS avg_order_value,
        MIN(o.o_orderdate)                                              AS first_order_date,
        MAX(o.o_orderdate)                                              AS last_order_date,
        DATEDIFF(current_date(), MAX(o.o_orderdate))                    AS days_since_last_order,
        CASE
            WHEN COUNT(o.o_orderkey) > 1
            THEN ROUND(
                DATEDIFF(MAX(o.o_orderdate), MIN(o.o_orderdate))
                / (COUNT(o.o_orderkey) - 1.0), 2
            )
            ELSE NULL
        END                                                             AS order_frequency_days,
        COUNT(CASE WHEN o.o_orderstatus = 'F' THEN 1 END)              AS fulfilled_orders,
        COUNT(CASE WHEN o.o_orderstatus = 'O' THEN 1 END)              AS open_orders,
        COUNT(CASE WHEN o.o_orderstatus = 'P' THEN 1 END)              AS partial_orders,
        CASE
            WHEN COUNT(o.o_orderkey) > 0
            THEN ROUND(
                100.0 * COUNT(CASE WHEN o.o_orderstatus = 'F' THEN 1 END)
                / COUNT(o.o_orderkey), 2
            )
            ELSE 0
        END                                                             AS fulfillment_rate,
        DATEDIFF(MAX(o.o_orderdate), MIN(o.o_orderdate))               AS customer_tenure_days
    FROM tv_customers_geo cg
    LEFT JOIN {catalog}.{extract_schema}.orders o
        ON cg.customer_key = o.o_custkey
    GROUP BY
        cg.customer_key, cg.customer_name, cg.market_segment,
        cg.nation_name, cg.region_name, cg.account_balance
""")

# COMMAND ----------

# Stage 3: RFM quintile scoring (only customers with orders)
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW tv_customer_rfm AS
    SELECT
        *,
        NTILE(5) OVER (ORDER BY days_since_last_order ASC)  AS rfm_recency_score,
        NTILE(5) OVER (ORDER BY total_orders DESC)          AS rfm_frequency_score,
        NTILE(5) OVER (ORDER BY total_revenue DESC)         AS rfm_monetary_score
    FROM tv_customer_order_agg
    WHERE total_orders > 0
""")

# COMMAND ----------

# Stage 4: Derive customer segment from RFM scores
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW tv_customer_orders_final AS
    SELECT
        customer_key,
        customer_name,
        market_segment,
        nation_name,
        region_name,
        account_balance,
        total_orders,
        total_revenue,
        avg_order_value,
        first_order_date,
        last_order_date,
        days_since_last_order,
        order_frequency_days,
        fulfilled_orders,
        open_orders,
        partial_orders,
        fulfillment_rate,
        customer_tenure_days,
        rfm_recency_score,
        rfm_frequency_score,
        rfm_monetary_score,
        CASE
            WHEN rfm_recency_score <= 2 AND rfm_frequency_score <= 2 AND rfm_monetary_score <= 2
                THEN 'Champions'
            WHEN rfm_recency_score <= 2 AND rfm_frequency_score <= 3
                THEN 'Loyal Customers'
            WHEN rfm_recency_score <= 2 AND rfm_monetary_score <= 2
                THEN 'Big Spenders'
            WHEN rfm_recency_score <= 3 AND rfm_frequency_score <= 3
                THEN 'Potential Loyalists'
            WHEN rfm_recency_score >= 4 AND rfm_frequency_score >= 4
                THEN 'At Risk'
            WHEN rfm_recency_score >= 4 AND rfm_frequency_score <= 2
                THEN 'Cannot Lose Them'
            ELSE 'Others'
        END                     AS customer_segment,
        current_timestamp()     AS _refined_at,
        '{batch_id}'            AS _batch_id
    FROM tv_customer_rfm
""")

# COMMAND ----------

# Final write
spark.sql(f"""
    INSERT OVERWRITE {catalog}.{refined_schema}.customer_orders
    SELECT
        customer_key, customer_name, market_segment, nation_name, region_name,
        account_balance, total_orders, total_revenue, avg_order_value,
        first_order_date, last_order_date, days_since_last_order,
        order_frequency_days, fulfilled_orders, open_orders, partial_orders,
        fulfillment_rate, customer_tenure_days,
        rfm_recency_score, rfm_frequency_score, rfm_monetary_score,
        customer_segment, _refined_at, _batch_id
    FROM tv_customer_orders_final
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT customer_segment, count(*) AS cnt, ROUND(avg(total_revenue), 2) AS avg_rev
    FROM {catalog}.{refined_schema}.customer_orders
    GROUP BY customer_segment
    ORDER BY avg_rev DESC
""")
df.display()
