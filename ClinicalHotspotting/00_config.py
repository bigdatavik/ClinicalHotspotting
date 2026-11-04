# Databricks notebook source
# DBTITLE 1,Clinical Hotspotting — Shared Configuration
# MAGIC %md
# MAGIC # Clinical Hotspotting — Shared Configuration
# MAGIC
# MAGIC Shared constants imported by all pipeline notebooks in this folder.
# MAGIC
# MAGIC | Constant | Value |
# MAGIC |----------|-------|
# MAGIC | `CATALOG` | `payer_catalog_vm` |
# MAGIC | `SCHEMA` | `clinical_hotspotting` |
# MAGIC | `FQSN` | `payer_catalog_vm.clinical_hotspotting` |

# COMMAND ----------

# DBTITLE 1,Config constants
CATALOG = "payer_catalog_vm"
SCHEMA  = "clinical_hotspotting"
FQSN    = f"{CATALOG}.{SCHEMA}"   # fully-qualified schema name
SEED    = 42                       # global random seed for reproducibility

# Pipeline target (SDP notebooks 02-04 write here too)
PIPELINE_TARGET_CATALOG = CATALOG
PIPELINE_TARGET_SCHEMA  = SCHEMA

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Full    : {FQSN}")

# COMMAND ----------

