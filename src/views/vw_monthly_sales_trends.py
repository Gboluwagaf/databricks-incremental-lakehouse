# Databricks notebook source
# MAGIC %md
# MAGIC # Gold View: Monthly Sales Trends
# MAGIC Time series analytics with moving averages, seasonal patterns, and growth rates.
# MAGIC
# MAGIC **Consumers**: Finance dashboards, forecasting models

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("refined_schema", "silver")
dbutils.widgets.text("views_schema", "gold")

catalog = dbutils.widgets.get("catalog")
refined_schema = dbutils.widgets.get("refined_schema")
views_schema = dbutils.widgets.get("views_schema")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{views_schema}.vw_monthly_sales_trends
    COMMENT 'Gold: Monthly sales time series with moving averages and growth metrics'
    AS
    WITH monthly_base AS (
        SELECT
            order_year,
            order_month,
            order_quarter,
            COUNT(DISTINCT order_key)                              AS total_orders,
            COUNT(*)                                               AS total_line_items,
            SUM(quantity)                                           AS total_quantity,
            ROUND(SUM(net_revenue), 2)                             AS total_revenue,
            ROUND(SUM(total_charge), 2)                            AS total_revenue_with_tax,
            ROUND(AVG(net_revenue), 2)                             AS avg_line_revenue,
            ROUND(AVG(discount_pct), 4)                            AS avg_discount_rate,
            COUNT(DISTINCT customer_key)                           AS unique_customers,
            COUNT(DISTINCT supplier_key)                           AS unique_suppliers,
            COUNT(DISTINCT part_key)                               AS unique_products,
            COUNT(CASE WHEN is_late_shipment THEN 1 END)           AS late_shipments,
            COUNT(CASE WHEN return_flag = 'R' THEN 1 END)          AS returns,
            ROUND(AVG(shipping_delay_days), 1)                     AS avg_ship_delay
        FROM {catalog}.{refined_schema}.order_details
        GROUP BY order_year, order_month, order_quarter
    ),
    with_trends AS (
        SELECT
            *,
            LAG(total_revenue) OVER (ORDER BY order_year, order_month)     AS prev_month_revenue,
            ROUND(
                (total_revenue - LAG(total_revenue) OVER (ORDER BY order_year, order_month))
                / NULLIF(LAG(total_revenue) OVER (ORDER BY order_year, order_month), 0) * 100, 2
            )                                                              AS mom_revenue_growth_pct,
            LAG(total_revenue, 12) OVER (ORDER BY order_year, order_month) AS same_month_prev_year_revenue,
            ROUND(
                (total_revenue - LAG(total_revenue, 12) OVER (ORDER BY order_year, order_month))
                / NULLIF(LAG(total_revenue, 12) OVER (ORDER BY order_year, order_month), 0) * 100, 2
            )                                                              AS yoy_revenue_growth_pct,
            ROUND(AVG(total_revenue) OVER (
                ORDER BY order_year, order_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ), 2)                                                          AS revenue_3mo_moving_avg,
            ROUND(AVG(total_revenue) OVER (
                ORDER BY order_year, order_month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
            ), 2)                                                          AS revenue_6mo_moving_avg,
            ROUND(AVG(total_revenue) OVER (
                ORDER BY order_year, order_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ), 2)                                                          AS revenue_12mo_moving_avg,
            SUM(total_revenue) OVER (
                PARTITION BY order_year ORDER BY order_month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )                                                              AS ytd_cumulative_revenue,
            RANK() OVER (PARTITION BY order_year ORDER BY total_revenue DESC) AS revenue_rank_in_year,
            ROUND(total_revenue / NULLIF(total_orders, 0), 2)              AS avg_order_value,
            ROUND(total_revenue / NULLIF(unique_customers, 0), 2)          AS revenue_per_customer
        FROM monthly_base
    )
    SELECT
        *,
        ROUND(total_revenue / NULLIF(revenue_12mo_moving_avg, 0), 4)       AS seasonal_index,
        ROUND(
            mom_revenue_growth_pct - LAG(mom_revenue_growth_pct) OVER (ORDER BY order_year, order_month), 2
        )                                                                  AS growth_acceleration
    FROM with_trends
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT order_year, ROUND(SUM(total_revenue), 2) AS annual_revenue, SUM(total_orders) AS orders
    FROM {catalog}.{views_schema}.vw_monthly_sales_trends
    GROUP BY order_year ORDER BY order_year
""")
df.display()
