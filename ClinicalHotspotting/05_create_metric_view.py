# Databricks notebook source
# DBTITLE 1,05 — Create Metric View
# MAGIC %md
# MAGIC # 05 — Create Governed Metric View
# MAGIC
# MAGIC Creates `payer_catalog_vm.clinical_hotspotting.mv_clinical_hotspotting` — a **Databricks Metric View** with governed KPIs.
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

# DBTITLE 1,Drop and recreate metric view
# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS payer_catalog_vm.clinical_hotspotting.mv_clinical_hotspotting

# COMMAND ----------

# DBTITLE 1,Create metric view DDL
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW payer_catalog_vm.clinical_hotspotting.mv_clinical_hotspotting
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC   version: 1.1
# MAGIC   source: >
# MAGIC     SELECT
# MAGIC       mh.member_id, mh.market, mh.risk_band, mh.zip_area, mh.sdoh_risk_level,
# MAGIC       mh.is_actionable_hotspot, mh.risk_score, mh.hotspot_score,
# MAGIC       mh.ed_visits, mh.inpatient_admits, mh.had_readmission,
# MAGIC       mh.avg_medication_adherence_pdc, mh.open_care_gaps, mh.sdoh_barrier_count,
# MAGIC       mh.total_allowed_cost,
# MAGIC       DATE_TRUNC('MONTH', e.admit_date) AS admit_month,
# MAGIC       e.encounter_type, e.primary_dx_category, e.transplant_type,
# MAGIC       p.provider_group, p.peer_group
# MAGIC     FROM payer_catalog_vm.clinical_hotspotting.gold_member_hotspots mh
# MAGIC     LEFT JOIN payer_catalog_vm.clinical_hotspotting.silver_encounters e ON mh.member_id = e.member_id
# MAGIC     LEFT JOIN payer_catalog_vm.clinical_hotspotting.silver_providers  p ON e.provider_id = p.provider_id
# MAGIC   comment: Governed clinical hotspotting KPIs population health and utilization metrics
# MAGIC   dimensions:
# MAGIC     - name: market
# MAGIC       expr: source.market
# MAGIC       comment: Geographic market (10 markets)
# MAGIC     - name: risk_band
# MAGIC       expr: source.risk_band
# MAGIC       comment: Very High / High / Rising / Low
# MAGIC     - name: zip_area
# MAGIC       expr: source.zip_area
# MAGIC       comment: ZIP code area within market
# MAGIC     - name: sdoh_risk_level
# MAGIC       expr: source.sdoh_risk_level
# MAGIC       comment: High / Moderate / Low social determinant risk
# MAGIC     - name: admit_month
# MAGIC       expr: source.admit_month
# MAGIC       comment: Calendar month of encounter admission
# MAGIC       format:
# MAGIC         type: date
# MAGIC         date_format: year_month_day
# MAGIC     - name: encounter_type
# MAGIC       expr: source.encounter_type
# MAGIC       comment: ED / Inpatient / Observation
# MAGIC     - name: primary_dx_category
# MAGIC       expr: source.primary_dx_category
# MAGIC       comment: Primary diagnosis category
# MAGIC     - name: transplant_type
# MAGIC       expr: source.transplant_type
# MAGIC       comment: Kidney / Liver / Heart (null for non-transplant)
# MAGIC     - name: provider_group
# MAGIC       expr: source.provider_group
# MAGIC       comment: Provider organization group
# MAGIC     - name: peer_group
# MAGIC       expr: source.peer_group
# MAGIC       comment: Provider peer group benchmark
# MAGIC   measures:
# MAGIC     - name: total_members
# MAGIC       expr: COUNT(DISTINCT source.member_id)
# MAGIC       comment: Count of distinct members
# MAGIC     - name: actionable_hotspot_members
# MAGIC       expr: SUM(CAST(source.is_actionable_hotspot AS INT))
# MAGIC       comment: Members flagged as actionable hotspots
# MAGIC     - name: actionable_hotspot_rate
# MAGIC       expr: ROUND(AVG(CAST(source.is_actionable_hotspot AS DOUBLE)), 3)
# MAGIC       comment: Fraction of members who are actionable hotspots
# MAGIC       format:
# MAGIC         type: percentage
# MAGIC         decimal_places:
# MAGIC           type: exact
# MAGIC           places: 1
# MAGIC     - name: avg_risk_score
# MAGIC       expr: ROUND(AVG(source.risk_score), 2)
# MAGIC       comment: Average clinical risk score
# MAGIC     - name: avg_hotspot_score
# MAGIC       expr: ROUND(AVG(source.hotspot_score), 1)
# MAGIC       comment: Average composite hotspot score
# MAGIC     - name: ed_visits_per_1000
# MAGIC       expr: ROUND(SUM(source.ed_visits) * 1000.0 / NULLIF(COUNT(DISTINCT source.member_id), 0), 1)
# MAGIC       comment: ED visits per 1000 members
# MAGIC     - name: admits_per_1000
# MAGIC       expr: ROUND(SUM(source.inpatient_admits) * 1000.0 / NULLIF(COUNT(DISTINCT source.member_id), 0), 1)
# MAGIC       comment: Inpatient admissions per 1000 members
# MAGIC     - name: readmission_rate
# MAGIC       expr: ROUND(AVG(CAST(source.had_readmission AS DOUBLE)), 3)
# MAGIC       comment: 30-day readmission rate
# MAGIC       format:
# MAGIC         type: percentage
# MAGIC         decimal_places:
# MAGIC           type: exact
# MAGIC           places: 1
# MAGIC     - name: avg_pdc
# MAGIC       expr: ROUND(AVG(source.avg_medication_adherence_pdc), 3)
# MAGIC       comment: Average medication adherence PDC
# MAGIC     - name: avg_open_care_gaps
# MAGIC       expr: ROUND(AVG(CAST(source.open_care_gaps AS DOUBLE)), 2)
# MAGIC       comment: Average open care gaps per member
# MAGIC     - name: avg_sdoh_barriers
# MAGIC       expr: ROUND(AVG(CAST(source.sdoh_barrier_count AS DOUBLE)), 2)
# MAGIC       comment: Average SDoH barriers per member
# MAGIC     - name: high_sdoh_risk_rate
# MAGIC       expr: ROUND(SUM(CAST(source.sdoh_risk_level = 'High' AS INT)) * 1.0 / NULLIF(COUNT(DISTINCT source.member_id), 0), 3)
# MAGIC       comment: Fraction of members with High SDoH risk
# MAGIC       format:
# MAGIC         type: percentage
# MAGIC         decimal_places:
# MAGIC           type: exact
# MAGIC           places: 1
# MAGIC     - name: total_allowed_cost
# MAGIC       expr: ROUND(SUM(source.total_allowed_cost), 2)
# MAGIC       comment: Total allowed cost
# MAGIC       format:
# MAGIC         type: currency
# MAGIC         currency_code: USD
# MAGIC         decimal_places:
# MAGIC           type: exact
# MAGIC           places: 2
# MAGIC     - name: pmpm_cost
# MAGIC       expr: ROUND(SUM(source.total_allowed_cost) / NULLIF(COUNT(DISTINCT source.member_id), 0), 2)
# MAGIC       comment: Per-member per-month allowed cost
# MAGIC       format:
# MAGIC         type: currency
# MAGIC         currency_code: USD
# MAGIC         decimal_places:
# MAGIC           type: exact
# MAGIC           places: 2
# MAGIC $$

# COMMAND ----------

# DBTITLE 1,Validate metric view
# MAGIC %sql
# MAGIC SELECT
# MAGIC   market,
# MAGIC   MEASURE(total_members)              AS total_members,
# MAGIC   MEASURE(actionable_hotspot_members)  AS actionable_members,
# MAGIC   MEASURE(actionable_hotspot_rate)     AS actionable_rate,
# MAGIC   MEASURE(avg_risk_score)              AS avg_risk,
# MAGIC   MEASURE(ed_visits_per_1000)          AS ed_per_1000,
# MAGIC   MEASURE(pmpm_cost)                   AS pmpm
# MAGIC FROM payer_catalog_vm.clinical_hotspotting.mv_clinical_hotspotting
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_members DESC
# MAGIC LIMIT 10

# COMMAND ----------

