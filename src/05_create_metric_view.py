# Databricks notebook source
# DBTITLE 1,05 — Create Metric View
# MAGIC %md
# MAGIC # 05 — Create Governed Metric View
# MAGIC
# MAGIC Creates `{catalog}.{schema}.mv_clinical_hotspotting` — a **Databricks Metric View** with governed KPIs.
# MAGIC
# MAGIC Run after the SDP pipeline completes (gold tables must exist).
# MAGIC
# MAGIC Metric views cannot be defined inside SDP pipelines — they require `CREATE METRIC VIEW` DDL.
# MAGIC
# MAGIC | Dimension | Values |
# MAGIC |-----------|--------|
# MAGIC | Market | 10 markets |
# MAGIC | Risk Band | Very High / High / Rising / Low |
# MAGIC | Encounter Type | ED / Inpatient / Observation |
# MAGIC | Provider Group | 10 groups |
# MAGIC | Peer Group | 5 groups |
# MAGIC | Primary Dx Category | 13 categories |
# MAGIC | Transplant Type | Kidney / Liver / Heart |
# MAGIC | SDoH Risk Level | High / Moderate / Low |
# MAGIC | Admit Month | YYYY-MM |
# MAGIC
# MAGIC | Measure | Formula |
# MAGIC |---------|---------|
# MAGIC | `total_members` | COUNT DISTINCT member_id |
# MAGIC | `actionable_hotspot_members` | COUNT WHERE is_actionable |
# MAGIC | `actionable_hotspot_rate` | actionable / total |
# MAGIC | `avg_risk_score` | AVG(risk_score) |
# MAGIC | `ed_visits_per_1000` | SUM(ed_visits)*1000/total |
# MAGIC | `admits_per_1000` | SUM(inpatient_admits)*1000/total |
# MAGIC | `readmission_rate` | AVG(had_readmission) |
# MAGIC | `avg_pdc` | AVG(avg_medication_adherence_pdc) |
# MAGIC | `avg_open_care_gaps` | AVG(open_care_gaps) |
# MAGIC | `avg_hotspot_score` | AVG(hotspot_score) |
# MAGIC | `total_allowed_cost` | SUM(total_allowed_cost) |
# MAGIC | `pmpm_cost` | SUM/COUNT |
# MAGIC | `high_sdoh_risk_rate` | COUNT(sdoh_risk_level='High')/total |

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "clinical_hotspotting", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FQSN    = f"{CATALOG}.{SCHEMA}"
print(f"Creating metric view in {FQSN}")

# COMMAND ----------

# DBTITLE 1,Drop and recreate metric view
spark.sql(f"""
DROP VIEW IF EXISTS {FQSN}.mv_clinical_hotspotting
""")

# COMMAND ----------

# DBTITLE 1,Create metric view DDL
spark.sql(f"""
CREATE OR REPLACE VIEW {FQSN}.mv_clinical_hotspotting
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: >
    SELECT
      mh.member_id, mh.market, mh.risk_band, mh.zip_area, mh.sdoh_risk_level,
      mh.is_actionable_hotspot, mh.risk_score, mh.hotspot_score,
      mh.ed_visits, mh.inpatient_admits, mh.had_readmission,
      mh.avg_medication_adherence_pdc, mh.open_care_gaps, mh.sdoh_barrier_count,
      mh.total_allowed_cost,
      DATE_TRUNC('MONTH', e.admit_date) AS admit_month,
      e.encounter_type, e.primary_dx_category, e.transplant_type,
      p.provider_group, p.peer_group
    FROM {FQSN}.gold_member_hotspots mh
    LEFT JOIN {FQSN}.silver_encounters e ON mh.member_id = e.member_id
    LEFT JOIN {FQSN}.silver_providers  p ON e.provider_id = p.provider_id
  comment: Governed clinical hotspotting KPIs population health and utilization metrics
  dimensions:
    - name: market
      expr: source.market
      comment: Geographic market (10 markets)
    - name: risk_band
      expr: source.risk_band
      comment: Very High / High / Rising / Low
    - name: zip_area
      expr: source.zip_area
      comment: ZIP code area within market
    - name: sdoh_risk_level
      expr: source.sdoh_risk_level
      comment: High / Moderate / Low social determinant risk
    - name: admit_month
      expr: source.admit_month
      comment: Calendar month of encounter admission
      format:
        type: date
        date_format: year_month_day
    - name: encounter_type
      expr: source.encounter_type
      comment: ED / Inpatient / Observation
    - name: primary_dx_category
      expr: source.primary_dx_category
      comment: Primary diagnosis category
    - name: transplant_type
      expr: source.transplant_type
      comment: Kidney / Liver / Heart (null for non-transplant)
    - name: provider_group
      expr: source.provider_group
      comment: Provider organization group
    - name: peer_group
      expr: source.peer_group
      comment: Provider peer group benchmark
  measures:
    - name: total_members
      expr: COUNT(DISTINCT source.member_id)
      comment: Count of distinct members
    - name: actionable_hotspot_members
      expr: SUM(CAST(source.is_actionable_hotspot AS INT))
      comment: Members flagged as actionable hotspots
    - name: actionable_hotspot_rate
      expr: ROUND(AVG(CAST(source.is_actionable_hotspot AS DOUBLE)), 3)
      comment: Fraction of members who are actionable hotspots
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: avg_risk_score
      expr: ROUND(AVG(source.risk_score), 2)
      comment: Average clinical risk score
    - name: avg_hotspot_score
      expr: ROUND(AVG(source.hotspot_score), 1)
      comment: Average composite hotspot score
    - name: ed_visits_per_1000
      expr: ROUND(SUM(source.ed_visits) * 1000.0 / NULLIF(COUNT(DISTINCT source.member_id), 0), 1)
      comment: ED visits per 1000 members
    - name: admits_per_1000
      expr: ROUND(SUM(source.inpatient_admits) * 1000.0 / NULLIF(COUNT(DISTINCT source.member_id), 0), 1)
      comment: Inpatient admissions per 1000 members
    - name: readmission_rate
      expr: ROUND(AVG(CAST(source.had_readmission AS DOUBLE)), 3)
      comment: 30-day readmission rate
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: avg_pdc
      expr: ROUND(AVG(source.avg_medication_adherence_pdc), 3)
      comment: Average medication adherence PDC
    - name: avg_open_care_gaps
      expr: ROUND(AVG(CAST(source.open_care_gaps AS DOUBLE)), 2)
      comment: Average open care gaps per member
    - name: avg_sdoh_barriers
      expr: ROUND(AVG(CAST(source.sdoh_barrier_count AS DOUBLE)), 2)
      comment: Average SDoH barriers per member
    - name: high_sdoh_risk_rate
      expr: ROUND(SUM(CAST(source.sdoh_risk_level = 'High' AS INT)) * 1.0 / NULLIF(COUNT(DISTINCT source.member_id), 0), 3)
      comment: Fraction of members with High SDoH risk
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: total_allowed_cost
      expr: ROUND(SUM(source.total_allowed_cost), 2)
      comment: Total allowed cost
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
    - name: pmpm_cost
      expr: ROUND(SUM(source.total_allowed_cost) / NULLIF(COUNT(DISTINCT source.member_id), 0), 2)
      comment: Per-member per-month allowed cost
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
$$
""")

# COMMAND ----------

# DBTITLE 1,Validate metric view
display(spark.sql(f"""
SELECT
  market,
  MEASURE(total_members)              AS total_members,
  MEASURE(actionable_hotspot_members)  AS actionable_members,
  MEASURE(actionable_hotspot_rate)     AS actionable_rate,
  MEASURE(avg_risk_score)              AS avg_risk,
  MEASURE(ed_visits_per_1000)          AS ed_per_1000,
  MEASURE(pmpm_cost)                   AS pmpm
FROM {FQSN}.mv_clinical_hotspotting
GROUP BY ALL
ORDER BY total_members DESC
LIMIT 10
"""))

# COMMAND ----------

