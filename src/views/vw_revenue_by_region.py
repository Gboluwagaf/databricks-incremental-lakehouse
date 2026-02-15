# Databricks notebook source
# MAGIC %md
# MAGIC # Gold View: Revenue by Region
# MAGIC Multi-dimensional revenue analysis across regions, nations, and time periods.
# MAGIC
# MAGIC **Consumers**: Executive dashboards, regional performance reports

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("refined_schema", "silver")
dbutils.widgets.text("views_schema", "gold")

catalog = dbutils.widgets.get("catalog")
refined_schema = dbutils.widgets.get("refined_schema")
views_schema = dbutils.widgets.get("views_schema")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{views_schema}.vw_revenue_by_region
    COMMENT 'Gold: Revenue aggregation by region, nation, and time period with YoY growth'
    AS
    WITH region_metrics AS (
        SELECT
            co.region_name,
            co.nation_name,
            co.market_segment,
            od.order_year,
            od.order_quarter,
            od.order_month,
            COUNT(DISTINCT od.order_key)                                       AS order_count,
            SUM(od.quantity)                                                    AS total_quantity,
            ROUND(SUM(od.net_revenue), 2)                                      AS total_revenue,
            ROUND(SUM(od.total_charge), 2)                                     AS total_charge_with_tax,
            ROUND(AVG(od.net_revenue), 2)                                      AS avg_line_revenue,
            ROUND(AVG(od.discount_pct), 4)                                     AS avg_discount_rate,
            COUNT(CASE WHEN od.is_late_shipment THEN 1 END)                    AS late_shipments,
            COUNT(*)                                                           AS total_lines,
            ROUND(100.0 * COUNT(CASE WHEN od.is_late_shipment THEN 1 END) / COUNT(*), 2) AS late_shipment_pct
        FROM {catalog}.{refined_schema}.order_details od
        INNER JOIN {catalog}.{refined_schema}.customer_orders co
            ON od.customer_key = co.customer_key
        GROUP BY
            co.region_name, co.nation_name, co.market_segment,
            od.order_year, od.order_quarter, od.order_month
    )
    SELECT
        region_name,
        nation_name,
        market_segment,
        order_year,
        order_quarter,
        order_month,
        order_count,
        total_quantity,
        total_revenue,
        total_charge_with_tax,
        avg_line_revenue,
        avg_discount_rate,
        late_shipments,
        total_lines,
        late_shipment_pct,
        LAG(total_revenue) OVER (
            PARTITION BY region_name, nation_name, market_segment, order_month
            ORDER BY order_year
        )                                                                      AS prev_year_revenue,
        ROUND(
            (total_revenue - LAG(total_revenue) OVER (
                PARTITION BY region_name, nation_name, market_segment, order_month
                ORDER BY order_year
            )) / NULLIF(LAG(total_revenue) OVER (
                PARTITION BY region_name, nation_name, market_segment, order_month
                ORDER BY order_year
            ), 0) * 100, 2
        )                                                                      AS yoy_revenue_growth_pct,
        ROUND(
            total_revenue / NULLIF(
                SUM(total_revenue) OVER (PARTITION BY region_name, order_year, order_quarter), 0
            ) * 100, 2
        )                                                                      AS revenue_share_in_region_pct
    FROM region_metrics
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT region_name, order_year, ROUND(SUM(total_revenue), 2) AS annual_revenue
    FROM {catalog}.{views_schema}.vw_revenue_by_region
    GROUP BY region_name, order_year
    ORDER BY region_name, order_year
""")
df.display()
