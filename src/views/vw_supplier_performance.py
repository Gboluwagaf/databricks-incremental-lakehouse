# Databricks notebook source
# MAGIC %md
# MAGIC # Gold View: Supplier Performance Scorecard
# MAGIC Composite supplier scoring combining cost competitiveness,
# MAGIC delivery performance, and portfolio breadth.
# MAGIC
# MAGIC **Consumers**: Procurement dashboards, supplier relationship management

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse")
dbutils.widgets.text("refined_schema", "silver")
dbutils.widgets.text("views_schema", "gold")

catalog = dbutils.widgets.get("catalog")
refined_schema = dbutils.widgets.get("refined_schema")
views_schema = dbutils.widgets.get("views_schema")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{views_schema}.vw_supplier_performance
    COMMENT 'Gold: Supplier performance scorecard with composite scoring and tiering'
    AS
    WITH supplier_cost_metrics AS (
        SELECT
            supplier_key,
            supplier_name,
            supplier_nation,
            supplier_region,
            supplier_acct_balance,
            COUNT(DISTINCT part_key)                                              AS parts_in_catalog,
            COUNT(DISTINCT part_type)                                             AS distinct_part_types,
            COUNT(DISTINCT part_brand)                                            AS distinct_brands,
            SUM(available_qty)                                                    AS total_available_qty,
            ROUND(AVG(supply_cost), 2)                                            AS avg_supply_cost,
            ROUND(AVG(margin_pct) * 100, 2)                                       AS avg_margin_pct,
            ROUND(AVG(cost_vs_region_avg), 4)                                     AS avg_cost_vs_region,
            SUM(CASE WHEN is_cheapest_in_region THEN 1 ELSE 0 END)               AS cheapest_count,
            COUNT(*)                                                              AS total_combos,
            ROUND(100.0 * SUM(CASE WHEN is_cheapest_in_region THEN 1 ELSE 0 END) / COUNT(*), 2) AS cheapest_pct
        FROM {catalog}.{refined_schema}.supplier_parts
        GROUP BY supplier_key, supplier_name, supplier_nation, supplier_region, supplier_acct_balance
    ),
    supplier_delivery_metrics AS (
        SELECT
            supplier_key,
            COUNT(DISTINCT order_key)                                             AS orders_fulfilled,
            SUM(quantity)                                                          AS total_qty_shipped,
            ROUND(SUM(net_revenue), 2)                                            AS total_revenue_generated,
            ROUND(AVG(shipping_delay_days), 1)                                    AS avg_ship_delay_days,
            ROUND(AVG(delivery_delay_days), 1)                                    AS avg_delivery_delay_days,
            COUNT(CASE WHEN is_late_shipment THEN 1 END)                          AS late_shipments,
            COUNT(*)                                                              AS total_shipments,
            ROUND(100.0 * COUNT(CASE WHEN is_late_shipment THEN 1 END) / COUNT(*), 2) AS late_shipment_rate,
            ROUND(100.0 * (1 - COUNT(CASE WHEN is_late_shipment THEN 1 END) * 1.0 / COUNT(*)), 2) AS on_time_delivery_rate,
            COUNT(CASE WHEN return_flag = 'R' THEN 1 END)                         AS returned_items,
            ROUND(100.0 * COUNT(CASE WHEN return_flag = 'R' THEN 1 END) / COUNT(*), 2) AS return_rate_pct
        FROM {catalog}.{refined_schema}.order_details
        GROUP BY supplier_key
    ),
    composite AS (
        SELECT
            scm.*,
            sdm.orders_fulfilled,
            sdm.total_qty_shipped,
            sdm.total_revenue_generated,
            sdm.avg_ship_delay_days,
            sdm.avg_delivery_delay_days,
            sdm.late_shipments,
            sdm.total_shipments,
            sdm.late_shipment_rate,
            sdm.on_time_delivery_rate,
            sdm.returned_items,
            sdm.return_rate_pct,
            ROUND(
                COALESCE(sdm.on_time_delivery_rate, 50) * 0.40
                + LEAST(scm.cheapest_pct, 100) * 0.30
                + LEAST(scm.distinct_part_types * 5, 100) * 0.20
                + (100 - COALESCE(sdm.return_rate_pct, 50)) * 0.10,
                2
            )                                                                     AS performance_score
        FROM supplier_cost_metrics scm
        LEFT JOIN supplier_delivery_metrics sdm ON scm.supplier_key = sdm.supplier_key
    )
    SELECT
        *,
        CASE
            WHEN performance_score >= 80 THEN 'Tier 1 - Strategic'
            WHEN performance_score >= 60 THEN 'Tier 2 - Preferred'
            WHEN performance_score >= 40 THEN 'Tier 3 - Approved'
            ELSE 'Tier 4 - Under Review'
        END                                                                       AS supplier_tier,
        RANK() OVER (PARTITION BY supplier_region ORDER BY performance_score DESC) AS rank_in_region,
        RANK() OVER (ORDER BY performance_score DESC)                             AS overall_rank
    FROM composite
""")

# COMMAND ----------

df = spark.sql(f"""
    SELECT supplier_region, supplier_tier, count(*) AS cnt, ROUND(avg(performance_score), 2) AS avg_score
    FROM {catalog}.{views_schema}.vw_supplier_performance
    GROUP BY supplier_region, supplier_tier
    ORDER BY supplier_region, supplier_tier
""")
df.display()
