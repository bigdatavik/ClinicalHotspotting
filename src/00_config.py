# Databricks notebook source
# DBTITLE 1,Clinical Hotspotting — Shared Configuration
# MAGIC %md
# MAGIC # Clinical Hotspotting — Shared Configuration
# MAGIC
# MAGIC Every notebook in this project reads **`catalog`** and **`schema`** from widgets / job parameters,
# MAGIC so nothing is hardcoded. When deployed with the Databricks Asset Bundle, values come from the bundle
# MAGIC variables in `databricks.yml` (`catalog`, `schema`). When run interactively, set the widgets at the top.
# MAGIC
# MAGIC | Constant | Source |
# MAGIC |----------|-------|
# MAGIC | `CATALOG` | widget `catalog` (bundle var `catalog`) |
# MAGIC | `SCHEMA` | widget `schema` (bundle var `schema`, default `clinical_hotspotting`) |
# MAGIC | `FQSN` | `{CATALOG}.{SCHEMA}` |

# COMMAND ----------

# DBTITLE 1,Config constants
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "clinical_hotspotting", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FQSN    = f"{CATALOG}.{SCHEMA}"   # fully-qualified schema name
SEED    = 42                       # global random seed for reproducibility

# Pipeline target (SDP notebooks 02-04 write here too)
PIPELINE_TARGET_CATALOG = CATALOG
PIPELINE_TARGET_SCHEMA  = SCHEMA

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Full    : {FQSN}")

# COMMAND ----------

