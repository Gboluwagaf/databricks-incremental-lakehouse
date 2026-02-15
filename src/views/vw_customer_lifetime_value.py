# Databricks notebook source
# MAGIC %md
# MAGIC # Gold View: Customer Lifetime Value (CLV)
# MAGIC Comprehensive customer analytics with CLV estimation, cohort analysis,
# MAGIC and behavioral segmentation.
# MAGIC
# MAGIC **Consumers**: Marketing analytics, customer success dashboards

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("refined_schema", "silver")
dbutils.widgets.text("views_schema", "gold")

catalog = dbutils.widgets.get("catalog")
refined_schema = dbutils.widgets.get("refined_schema")
views_schema = dbutils.widgets.get("views_schema")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{views_schema}.vw_customer_lifetime_value
    COMMENT 'Gold: Customer lifetime value with cohort analysis and value tiers'
    AS
    WITH order_detail_metrics AS (
        SELECT
            customer_key,
            COUNT(DISTINCT part_brand)                                          AS distinct_brands_purchased,
            COUNT(DISTINCT ship_mode)                                           AS distinct_ship_modes_used,
            COUNT(DISTINCT part_type)                                           AS distinct_part_types,
            ROUND(AVG(shipping_delay_days), 1)                                  AS avg_shipping_delay,
            ROUND(AVG(discount_pct), 4)                                         AS avg_discount_received,
            SUM(CASE WHEN return_flag = 'R' THEN 1 ELSE 0 END)                 AS returned_lines,
            COUNT(*)                                                            AS total_lines,
            ROUND(100.0 * SUM(CASE WHEN return_flag = 'R' THEN 1 ELSE 0 END) / COUNT(*), 2) AS return_rate_pct,
            ROUND(SUM(net_revenue), 2)                                          AS detailed_total_revenue,
            ROUND(SUM(tax_amount), 2)                                           AS total_tax_paid
        FROM {catalog}.{refined_schema}.order_details
        GROUP BY customer_key
    ),
    cohort_analysis AS (
        SELECT
            customer_key,
            concat(YEAR(first_order_date), '-Q', QUARTER(first_order_date))     AS acquisition_cohort,
            ROUND(
                avg_order_value
                * CASE WHEN order_frequency_days > 0 THEN (365.0 / order_frequency_days) ELSE 1 END
                * 3,
                2
            )                                                                   AS estimated_3yr_clv,
            CASE
                WHEN customer_tenure_days > 0
                THEN ROUND(total_revenue / customer_tenure_days, 2)
                ELSE total_revenue
            END                                                                 AS revenue_per_tenure_day
        FROM {catalog}.{refined_schema}.customer_orders
    )
    SELECT
        co.customer_key,
        co.customer_name,
        co.market_segment,
        co.nation_name,
        co.region_name,
        co.account_balance,
        co.total_orders,
        co.total_revenue,
        co.avg_order_value,
        co.first_order_date,
        co.last_order_date,
        co.days_since_last_order,
        co.order_frequency_days,
        co.fulfillment_rate,
        co.customer_tenure_days,
        co.customer_segment,
        co.rfm_recency_score,
        co.rfm_frequency_score,
        co.rfm_monetary_score,
        odm.distinct_brands_purchased,
        odm.distinct_ship_modes_used,
        odm.distinct_part_types,
        odm.avg_shipping_delay,
        odm.avg_discount_received,
        odm.returned_lines,
        odm.total_lines,
        odm.return_rate_pct,
        odm.total_tax_paid,
        ca.acquisition_cohort,
        ca.estimated_3yr_clv,
        ca.revenue_per_tenure_day,
        PERCENT_RANK() OVER (ORDER BY co.total_revenue)                         AS revenue_percentile,
        PERCENT_RANK() OVER (ORDER BY co.total_orders)                          AS order_frequency_percentile,
        CASE
            WHEN PERCENT_RANK() OVER (ORDER BY co.total_revenue) >= 0.9 THEN 'Platinum'
            WHEN PERCENT_RANK() OVER (ORDER BY co.total_revenue) >= 0.7 THEN 'Gold'
            WHEN PERCENT_RANK() OVER (ORDER BY co.total_revenue) >= 0.4 THEN 'Silver'
            ELSE 'Bronze'
        END                                                                     AS value_tier
    FROM {catalog}.{refined_schema}.customer_orders co
    LEFT JOIN order_detail_metrics odm ON co.customer_key = odm.customer_key
    LEFT JOIN cohort_analysis ca       ON co.customer_key = ca.customer_key
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT value_tier, count(*) AS cnt, ROUND(avg(estimated_3yr_clv), 2) AS avg_clv
    FROM {catalog}.{views_schema}.vw_customer_lifetime_value
    GROUP BY value_tier ORDER BY avg_clv DESC
""")
df.display()
