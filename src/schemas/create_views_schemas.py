# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Setup: Gold (Views) Layer
# MAGIC Creates the gold schema. View DDLs are handled by their respective view notebooks.
# MAGIC Idempotent — safe to run on every pipeline execution.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_lakehouse", "Target Catalog")
dbutils.widgets.text("views_schema", "gold", "Views Schema")

catalog = dbutils.widgets.get("catalog")
views_schema = dbutils.widgets.get("views_schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{views_schema} COMMENT 'Gold layer: Business-facing aggregated views for BI consumption'")

# COMMAND ----------

print(f"[schemas] Gold layer schema complete: {catalog}.{views_schema}")
