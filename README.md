# Databricks Incremental Lakehouse

## Overview

This repository contains a production-grade data lakehouse built on **Databricks** with **Unity Catalog**, implementing the **Medallion Architecture** (Bronze → Silver → Gold). It models a complete end-to-end ETL pipeline for a global parts supplier business — ingesting raw transactional data, applying business transformations, and surfacing analytical views ready for BI consumption.

The end goal is to provide business stakeholders with four analytical datasets:

1. **Revenue by Region** — How is revenue distributed across geographies, and how is it trending year-over-year?
2. **Customer Lifetime Value** — Who are our most valuable customers, what cohort do they belong to, and how do we segment them?
3. **Supplier Performance** — Which suppliers deliver on time, offer competitive pricing, and carry the broadest catalog?
4. **Monthly Sales Trends** — What are the seasonal patterns, and where is growth accelerating or decelerating?

These views sit in a **Gold** layer and are designed for direct connection from tools like Power BI, Tableau, or Databricks SQL dashboards.

---

## Data Source

All data originates from the **TPC-H benchmark dataset**, a widely-used industry standard for testing analytical query performance. It is available out of the box in every Databricks workspace at `samples.tpch` — no external credentials, API keys, or file uploads are required.

TPC-H models a global parts supplier business with 8 interrelated tables:

| Source Table | Description | Approx Rows | Role |
|---|---|---|---|
| `customer` | End customers who place orders | 150,000 | Dimension |
| `orders` | Order headers with status and dates | 1,500,000 | Fact |
| `lineitem` | Individual line items within each order (quantities, prices, shipping) | 6,000,000 | Fact |
| `supplier` | Suppliers who provide parts | 10,000 | Dimension |
| `part` | Product/part catalog | 200,000 | Dimension |
| `partsupp` | Which supplier offers which part, at what cost and quantity | 800,000 | Bridge |
| `nation` | 25 nations | 25 | Reference |
| `region` | 5 world regions (Africa, America, Asia, Europe, Middle East) | 5 | Reference |

These tables have foreign key relationships (e.g., `orders.o_custkey` → `customer.c_custkey`, `lineitem.l_suppkey` → `supplier.s_suppkey`) that we exploit in the Silver layer to build denormalized analytical models.

---

## What This Pipeline Does

### Bronze Layer (Extract)

Raw data is ingested from `samples.tpch` into the `bronze` schema with minimal transformation. Each record is stamped with audit columns (`_ingested_at`, `_source_system`, `_batch_id`) for lineage tracking. Data is quality-filtered (null primary keys removed) and deduplicated before writing. The write pattern is `INSERT OVERWRITE` — each run produces a clean, full snapshot of the source.

**Tables produced**: `orders`, `customers`, `lineitem`, `suppliers`, `parts`, `partsupp`, `nation`, `region`

### Silver Layer (Refined)

Bronze tables are joined, enriched with business logic, and written into the `silver` schema as denormalized analytical models.

- **`order_details`** — Joins `orders` + `lineitem` + `parts` into a single fact table at the line-item grain. Calculates net revenue (`extended_price * (1 - discount)`), tax amounts, shipping delay in days, and flags late shipments (shipped after commit date). Partitioned by `order_year`.

- **`customer_orders`** — Aggregates each customer's full order history and enriches with geography (`nation` → `region`). Computes RFM (Recency, Frequency, Monetary) quintile scores using `NTILE(5)` and derives segments: Champions, Loyal Customers, Big Spenders, Potential Loyalists, At Risk, Cannot Lose Them.

- **`supplier_parts`** — Joins `suppliers` + `partsupp` + `parts` + geography to build a supplier-part catalog. Uses `DENSE_RANK` to rank each supplier's cost within their region for each part type. Calculates margin percentages and flags the cheapest supplier per region/part-type combination.

### Gold Layer (Views)

`CREATE OR REPLACE VIEW` statements that read directly from Silver tables. These are not materialized — they always reflect the latest Silver data. Designed for BI tool consumption.

- **`vw_revenue_by_region`** — Revenue aggregated by region, nation, market segment, and month. Includes YoY growth via `LAG()` and revenue share within region via window `SUM`.

- **`vw_customer_lifetime_value`** — Full customer profile combining RFM segments, order detail metrics (return rates, brand diversity, shipping patterns), cohort assignment (year-quarter of first order), projected 3-year CLV, and a value tier (Platinum / Gold / Silver / Bronze) based on `PERCENT_RANK`.

- **`vw_supplier_performance`** — Composite supplier scorecard weighted: 40% on-time delivery rate + 30% cost competitiveness + 20% catalog breadth + 10% return rate. Suppliers are classified into tiers (Strategic, Preferred, Approved, Under Review) and ranked within their region and overall.

- **`vw_monthly_sales_trends`** — Monthly time series with MoM and YoY growth rates, 3/6/12-month moving averages, YTD cumulative revenue, seasonal index (month vs 12-month MA), and growth acceleration (change in MoM growth rate).

---

## Project Structure

```
├── configs/                           # Environment-specific configuration
│   ├── dev.json                       #   dev_lakehouse catalog, DEBUG logging
│   ├── stage.json                     #   stage_lakehouse catalog, INFO logging
│   └── prod.json                      #   prod_lakehouse catalog, WARN logging
│
└── src/
    ├── pipelines/                     # Orchestration notebooks
    │   ├── _context.py                #   Env context loader (%run by pipelines)
    │   ├── run_sales_analytics.py     #   Daily: orders → customers → revenue views
    │   └── run_supplier_analytics.py  #   Weekly: suppliers → parts → scorecard view
    │
    ├── schemas/                       # DDL — creates catalog, schemas, and tables
    │   ├── create_extract_schemas.py  #   Bronze: catalog + schema + 8 table DDLs
    │   ├── create_refined_schemas.py  #   Silver: schema + 3 table DDLs
    │   └── create_views_schemas.py    #   Gold: schema only (views created by view notebooks)
    │
    ├── extract/                       # Bronze layer — source ingestion
    │   ├── extract_orders.py          #   1.5M orders
    │   ├── extract_customers.py       #   150K customers
    │   ├── extract_lineitem.py        #   6M line items
    │   ├── extract_suppliers.py       #   10K suppliers
    │   ├── extract_parts.py           #   200K parts + 800K partsupp
    │   └── extract_nation_region.py   #   25 nations + 5 regions
    │
    ├── refined/                       # Silver layer — business transformations
    │   ├── refined_order_details.py   #   Denormalized fact with revenue calcs
    │   ├── refined_customer_orders.py #   Customer profile with RFM segmentation
    │   └── refined_supplier_parts.py  #   Supplier catalog with cost ranking
    │
    ├── views/                         # Gold layer — analytics views
    │   ├── vw_revenue_by_region.py
    │   ├── vw_customer_lifetime_value.py
    │   ├── vw_supplier_performance.py
    │   └── vw_monthly_sales_trends.py
    │
    └── tests/
        └── data_quality_checks.py     # 5 check categories across all layers
```

---

## Pipeline Design

Each pipeline notebook follows this execution pattern:

```
%run ./_context                          ← lightweight config (sets ENV, CATALOG, etc.)
dbutils.notebook.run("../schemas/...")    ← ensure DDL exists (child job)
dbutils.notebook.run("../extract/...")    ← ETL ingestion notebooks (child jobs)
dbutils.notebook.run("../refined/...")    ← business transformation notebooks (child jobs)
dbutils.notebook.run("../views/...")      ← gold view creation notebooks (child jobs)
dbutils.notebook.run("../tests/...")      ← data quality validation (child job)
```

**Why `dbutils.notebook.run()` for everything except context:**
- Each notebook run creates a **linked child job** visible in the Databricks job UI — when something fails, you click directly into the specific notebook that broke and see its output, rather than scrolling through a single monolithic notebook.
- Each child job has an **isolated runtime** — pre/post execution hooks (like widget initialization and temp view cleanup) behave correctly within their own scope.
- `%run` is reserved **exclusively** for `_context.py` because it needs to inject variables (`ENV`, `CATALOG`, etc.) into the pipeline notebook's namespace.

### ETL Notebook Pattern

Every extract and refined notebook follows a consistent structure:

```python
# 1. Read widget parameters passed by the pipeline
catalog = dbutils.widgets.get("catalog")

# 2. Build staged temporary views (transformations happen here)
spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv_raw AS SELECT ... FROM source")
spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv_cleaned AS SELECT ... WHERE pk IS NOT NULL")
spark.sql("CREATE OR REPLACE TEMPORARY VIEW tv_deduped AS SELECT ... ROW_NUMBER() ...")

# 3. Single INSERT OVERWRITE at the very end (only write operation in the notebook)
spark.sql(f"INSERT OVERWRITE {catalog}.{schema}.table SELECT ... FROM tv_deduped")
```

This pattern keeps all intermediate transformations as in-memory temp views and defers the single physical write until the final cell. If any transformation step fails, nothing is written to the target table.

---

## Environment Configuration

All table references are fully parameterized via Databricks widgets. Environment isolation is achieved through **separate Unity Catalog catalogs** — dev, stage, and prod never share a catalog.

| Environment | Catalog | Log Level | Pipeline Mode | Retry Attempts |
|-------------|---------|-----------|---------------|----------------|
| `dev` | `dev_lakehouse` | DEBUG | triggered | 3 |
| `stage` | `stage_lakehouse` | INFO | triggered | 3 |
| `prod` | `prod_lakehouse` | WARN | continuous | 5 |

The same notebook code runs in all environments. The only difference is the config file loaded by `_context.py`:

```
dev:   dev_lakehouse.bronze.orders
stage: stage_lakehouse.bronze.orders
prod:  prod_lakehouse.bronze.orders
```

---

## Analytics Techniques Demonstrated

| Gold View | SQL Techniques Used |
|-----------|---------------------|
| **Revenue by Region** | 5-table JOINs, `LAG()` for YoY growth, window `SUM` for revenue share, `GROUP BY` multi-dimensional aggregation |
| **Customer Lifetime Value** | `NTILE(5)` for RFM quintiles, `PERCENT_RANK` for value tiers, `CASE` segmentation logic, cohort derivation via `YEAR`/`QUARTER`, CLV projection formula |
| **Supplier Performance** | Weighted composite score formula, `DENSE_RANK` for regional cost ranking, `RANK` for overall positioning, multi-CTE pipeline |
| **Monthly Sales Trends** | `LAG(revenue, 12)` for YoY, `AVG() OVER (ROWS BETWEEN n PRECEDING)` for 3/6/12-month moving averages, cumulative `SUM`, seasonal index calculation, growth acceleration via nested `LAG` |

---

## Data Quality Checks

The `data_quality_checks.py` notebook runs as the final stage of every pipeline and validates:

| Check | What It Validates |
|-------|-------------------|
| **Row Counts** | Every table has > 0 rows after pipeline run |
| **Null Validation** | Primary keys and critical columns are never NULL |
| **Referential Integrity** | Foreign keys in orders → customers, lineitem → orders, lineitem → parts, silver → bronze all resolve (zero orphans) |
| **Business Rules** | No negative revenue, quantities > 0, discount/tax in 0-1 range, customer segments are populated |
| **Freshness** | `_ingested_at` and `_refined_at` timestamps are within 25 hours |

---

## Quick Start

### Prerequisites
- Databricks workspace with **Unity Catalog** enabled
- Permission to create catalogs and schemas
- Access to `samples.tpch` (available by default in all workspaces)
- Cluster running **Databricks Runtime 13.0+**

### Steps

1. **Clone** this repository into your Databricks workspace via **Repos** > **Add Repo**
2. **Open** `src/pipelines/run_sales_analytics.py`
3. **Set** the `env` widget to `dev`
4. **Run All** — the pipeline will create the catalog, schemas, tables, ingest all data, build silver models, create gold views, and run quality checks
5. **Query** the gold views:

```sql
-- Regional revenue with YoY growth
SELECT * FROM dev_lakehouse.gold.vw_revenue_by_region
WHERE order_year = 1998;

-- Top platinum customers by CLV
SELECT customer_name, estimated_3yr_clv, customer_segment
FROM dev_lakehouse.gold.vw_customer_lifetime_value
WHERE value_tier = 'Platinum'
ORDER BY estimated_3yr_clv DESC
LIMIT 20;

-- Strategic tier suppliers
SELECT supplier_name, supplier_region, performance_score, on_time_delivery_rate
FROM dev_lakehouse.gold.vw_supplier_performance
WHERE supplier_tier = 'Tier 1 - Strategic'
ORDER BY performance_score DESC;

-- Monthly trends with moving averages
SELECT order_year, order_month, total_revenue, revenue_3mo_moving_avg, mom_revenue_growth_pct
FROM dev_lakehouse.gold.vw_monthly_sales_trends
ORDER BY order_year, order_month;
```

---

## Scheduling

Configure as Databricks Workflows (Jobs):

| Pipeline | Schedule | Timeout | Description |
|----------|----------|---------|-------------|
| `run_sales_analytics` | Daily 06:00 UTC | 4 hours | Full sales domain: orders, customers, line items, revenue views, CLV, trends |
| `run_supplier_analytics` | Weekly Sun 08:00 UTC | 2 hours | Supplier domain: suppliers, parts, cost catalog, performance scorecard |
