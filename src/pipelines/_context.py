# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Context
# MAGIC Lightweight context loader for pipeline notebooks. Sets environment variables
# MAGIC and configuration that downstream `dbutils.notebook.run()` calls receive as arguments.
# MAGIC
# MAGIC **Usage**: `%run ./_context` from any pipeline notebook.

# COMMAND ----------

import json
import os

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
ENV = dbutils.widgets.get("env")

VALID_ENVS = ["dev", "stage", "prod"]
assert ENV in VALID_ENVS, f"Invalid environment '{ENV}'. Must be one of: {VALID_ENVS}"

# COMMAND ----------

# Resolve config file path relative to repo root
_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_config_path = os.path.join(_repo_root, "configs", f"{ENV}.json")

try:
    with open(_config_path, "r") as _f:
        _config = json.load(_f)
    print(f"[_context] Loaded config from: {_config_path}")
except FileNotFoundError:
    # Fallback: inline defaults when file path doesn't resolve (e.g. Workspace notebooks)
    _config = {
        "catalog": f"{ENV}_lakehouse",
        "extract_schema": "bronze",
        "refined_schema": "silver",
        "views_schema": "gold",
        "source_catalog": "samples",
        "source_schema": "tpch",
    }
    print(f"[_context] Using inline defaults for env: {ENV}")

# COMMAND ----------

# Expose config as top-level variables for pipeline notebooks
CATALOG        = _config["catalog"]
EXTRACT_SCHEMA = _config["extract_schema"]
REFINED_SCHEMA = _config["refined_schema"]
VIEWS_SCHEMA   = _config["views_schema"]
SOURCE_CATALOG = _config["source_catalog"]
SOURCE_SCHEMA  = _config["source_schema"]

# COMMAND ----------

print("=" * 60)
print(f"  PIPELINE CONTEXT: {ENV.upper()}")
print("=" * 60)
print(f"  Catalog:        {CATALOG}")
print(f"  Bronze Schema:  {EXTRACT_SCHEMA}")
print(f"  Silver Schema:  {REFINED_SCHEMA}")
print(f"  Gold Schema:    {VIEWS_SCHEMA}")
print(f"  Source:          {SOURCE_CATALOG}.{SOURCE_SCHEMA}")
print("=" * 60)
