# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline: Supplier Analytics
# MAGIC Orchestrates the ETL pipeline for supplier performance analytics.
# MAGIC
# MAGIC **Stages**: Schema DDL → Extract (Bronze) → Refined (Silver) → Views (Gold) → Quality Checks
# MAGIC **Schedule**: Weekly — Sundays at 08:00 UTC
# MAGIC
# MAGIC | Parameter | Description | Default |
# MAGIC |-----------|-------------|---------|
# MAGIC | `env`     | Target environment | `dev` |

# COMMAND ----------

# MAGIC %run ./_context

# COMMAND ----------

import time
from datetime import datetime

pipeline_name = "supplier_analytics"
pipeline_start = datetime.now()
run_id = f"{pipeline_name}_{pipeline_start.strftime('%Y%m%d_%H%M%S')}"
results = {}

print(f"Pipeline:  {pipeline_name}")
print(f"Run ID:    {run_id}")
print(f"Env:       {ENV}")
print(f"Catalog:   {CATALOG}")
print(f"Started:   {pipeline_start}")

# COMMAND ----------

COMMON_ARGS = {
    "catalog":        CATALOG,
    "extract_schema": EXTRACT_SCHEMA,
    "refined_schema": REFINED_SCHEMA,
    "views_schema":   VIEWS_SCHEMA,
    "source_catalog": SOURCE_CATALOG,
    "source_schema":  SOURCE_SCHEMA,
}

def run_stage(notebook_path, description, timeout_seconds=1800):
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

for k, v in results.items():
    if v["status"] == "FAILED":
        raise RuntimeError(f"Schema setup failed: {k}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Extract (Bronze)

# COMMAND ----------

results["ext_nation_region"] = run_stage("../extract/extract_nation_region", "Extract Nation & Region")
results["ext_suppliers"]     = run_stage("../extract/extract_suppliers",     "Extract Suppliers")
results["ext_parts"]         = run_stage("../extract/extract_parts",         "Extract Parts & PartSupp")

# Also need orders + lineitem for delivery performance in supplier scorecard
results["ext_orders"]        = run_stage("../extract/extract_orders",        "Extract Orders")
results["ext_lineitem"]      = run_stage("../extract/extract_lineitem",      "Extract Line Items")

for critical in ["ext_suppliers", "ext_parts"]:
    if results[critical]["status"] == "FAILED":
        raise RuntimeError(f"Critical extract failed: {critical}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Refined (Silver)

# COMMAND ----------

# order_details needed for delivery metrics in supplier scorecard
results["ref_order_details"]   = run_stage("../refined/refined_order_details",   "Refined Order Details")
results["ref_supplier_parts"]  = run_stage("../refined/refined_supplier_parts",  "Refined Supplier Parts")

for k, v in results.items():
    if k.startswith("ref_") and v["status"] == "FAILED":
        raise RuntimeError(f"Refined stage failed: {k}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Views (Gold)

# COMMAND ----------

results["vw_supplier"] = run_stage("../views/vw_supplier_performance", "View: Supplier Performance")

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
