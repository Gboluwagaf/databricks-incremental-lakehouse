# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline: Sales Analytics
# MAGIC Orchestrates the full medallion ETL for the sales analytics domain.
# MAGIC
# MAGIC **Stages**: Schema DDL → Extract (Bronze) → Refined (Silver) → Views (Gold) → Quality Checks
# MAGIC **Schedule**: Daily at 06:00 UTC
# MAGIC
# MAGIC | Parameter | Description | Default |
# MAGIC |-----------|-------------|---------|
# MAGIC | `env`     | Target environment | `dev` |

# COMMAND ----------

# MAGIC %run ./_context

# COMMAND ----------

import time
from datetime import datetime

pipeline_name = "sales_analytics"
pipeline_start = datetime.now()
run_id = f"{pipeline_name}_{pipeline_start.strftime('%Y%m%d_%H%M%S')}"
results = {}

print(f"Pipeline:  {pipeline_name}")
print(f"Run ID:    {run_id}")
print(f"Env:       {ENV}")
print(f"Catalog:   {CATALOG}")
print(f"Started:   {pipeline_start}")

# COMMAND ----------

# Shared arguments passed to every child notebook via dbutils.notebook.run()
COMMON_ARGS = {
    "catalog":        CATALOG,
    "extract_schema": EXTRACT_SCHEMA,
    "refined_schema": REFINED_SCHEMA,
    "views_schema":   VIEWS_SCHEMA,
    "source_catalog": SOURCE_CATALOG,
    "source_schema":  SOURCE_SCHEMA,
}

def run_stage(notebook_path, description, timeout_seconds=3600):
    """Execute a child notebook job with timing and error handling."""
    start = time.time()
    print(f"\n{'─' * 55}")
    print(f"  STAGE: {description}")
    print(f"  Path:  {notebook_path}")
    print(f"{'─' * 55}")
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds, COMMON_ARGS)
        elapsed = round(time.time() - start, 2)
        print(f"  ✓ COMPLETED — {elapsed}s")
        return {"status": "SUCCESS", "elapsed": elapsed}
    except Exception as e:
        elapsed = round(time.time() - start, 2)
        print(f"  ✗ FAILED — {elapsed}s — {str(e)[:200]}")
        return {"status": "FAILED", "elapsed": elapsed, "error": str(e)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Schema DDL

# COMMAND ----------

results["schema_bronze"] = run_stage("../schemas/create_extract_schemas", "Create Bronze Schema + Tables")
results["schema_silver"] = run_stage("../schemas/create_refined_schemas", "Create Silver Schema + Tables")
results["schema_gold"]   = run_stage("../schemas/create_views_schemas",   "Create Gold Schema")

# Fail fast on schema errors
for k, v in results.items():
    if v["status"] == "FAILED":
        raise RuntimeError(f"Schema setup failed at '{k}': {v.get('error')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Extract (Bronze)

# COMMAND ----------

# Reference data first (needed by downstream joins)
results["ext_nation_region"] = run_stage("../extract/extract_nation_region", "Extract Nation & Region")

# Dimensions
results["ext_customers"]     = run_stage("../extract/extract_customers",    "Extract Customers")
results["ext_suppliers"]     = run_stage("../extract/extract_suppliers",    "Extract Suppliers")
results["ext_parts"]         = run_stage("../extract/extract_parts",        "Extract Parts & PartSupp")

# Facts
results["ext_orders"]        = run_stage("../extract/extract_orders",       "Extract Orders")
results["ext_lineitem"]      = run_stage("../extract/extract_lineitem",     "Extract Line Items")

# Gate: critical fact tables must succeed
for critical in ["ext_orders", "ext_lineitem"]:
    if results[critical]["status"] == "FAILED":
        raise RuntimeError(f"Critical extract failed: {critical}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Refined (Silver)

# COMMAND ----------

results["ref_order_details"]    = run_stage("../refined/refined_order_details",    "Refined Order Details")
results["ref_customer_orders"]  = run_stage("../refined/refined_customer_orders",  "Refined Customer Orders (RFM)")

for k, v in results.items():
    if k.startswith("ref_") and v["status"] == "FAILED":
        raise RuntimeError(f"Refined stage failed: {k}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Views (Gold)

# COMMAND ----------

results["vw_revenue"]   = run_stage("../views/vw_revenue_by_region",        "View: Revenue by Region")
results["vw_clv"]       = run_stage("../views/vw_customer_lifetime_value",  "View: Customer Lifetime Value")
results["vw_trends"]    = run_stage("../views/vw_monthly_sales_trends",     "View: Monthly Sales Trends")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Data Quality Checks

# COMMAND ----------

results["quality"] = run_stage("../tests/data_quality_checks", "Data Quality Checks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

pipeline_end = datetime.now()
total_seconds = (pipeline_end - pipeline_start).total_seconds()
failed = {k: v for k, v in results.items() if v["status"] == "FAILED"}

print("\n" + "=" * 65)
print(f"  PIPELINE SUMMARY: {pipeline_name}")
print("=" * 65)
print(f"  Run ID:   {run_id}")
print(f"  Env:      {ENV}")
print(f"  Duration: {round(total_seconds, 2)}s")
print("─" * 65)
print(f"  {'Stage':<35} {'Status':<10} {'Time':<10}")
print("─" * 65)
for stage, res in results.items():
    icon = "OK" if res["status"] == "SUCCESS" else "FAIL"
    print(f"  {stage:<35} {icon:<10} {res['elapsed']}s")
print("─" * 65)
print(f"  Result: {'SUCCESS' if not failed else f'FAILED ({len(failed)} failures)'}")
print("=" * 65)

if failed:
    raise RuntimeError(f"Pipeline finished with failures: {list(failed.keys())}")
