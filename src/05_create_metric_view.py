# Databricks notebook source
# DBTITLE 1,05 — Create Metric Views
# MAGIC %md
# MAGIC # 05 — Create Governed Metric Views
# MAGIC
# MAGIC Creates two **Databricks Metric Views** with governed KPIs. Run after the SDP pipeline (gold tables must exist).
# MAGIC Metric views cannot be defined inside SDP pipelines; they require `CREATE VIEW ... WITH METRICS` DDL.
# MAGIC
# MAGIC Each view has **exactly one grain**, so every measure counts each row once. An earlier version joined
# MAGIC member-level rows to encounters, which repeated each member once per encounter and inflated member KPIs
# MAGIC (for example, the actionable rate read ~0.80 instead of the true ~0.65). The reconciliation cell at the end fails this
# MAGIC notebook if that ever happens again.
# MAGIC
# MAGIC ### `mv_clinical_hotspotting` — one row per member (source: `gold_member_hotspots`)
# MAGIC
# MAGIC | Dimensions | Measures |
# MAGIC |---|---|
# MAGIC | market, zip_area, risk_band, sdoh_risk_level, is_actionable_hotspot, has_chf, has_diabetes, has_ckd, has_copd, has_behavioral, has_pcp_followup | total_members, actionable_hotspot_members, actionable_hotspot_rate, avg_risk_score, avg_hotspot_score, ed_visits_per_1000, admits_per_1000, readmission_rate, avg_pdc, avg_open_care_gaps, avg_sdoh_barriers, high_sdoh_risk_rate, total_allowed_cost, pmpm_cost |
# MAGIC
# MAGIC ### `mv_encounter_utilization` — one row per encounter (source: `silver_encounters` + provider + member attributes)
# MAGIC
# MAGIC | Dimensions | Measures |
# MAGIC |---|---|
# MAGIC | admit_month, encounter_type, primary_dx_category, transplant_type, provider_group, peer_group, specialty, market, risk_band | total_encounters, members_with_encounters, ed_visits, inpatient_admits, readmission_rate_30d, complication_rate, avg_length_of_stay, total_encounter_cost, avg_cost_per_encounter |
# MAGIC
# MAGIC Use the member view for "how many / what share of members" questions, and the encounter view for "how much utilization,
# MAGIC by diagnosis / provider / month" questions.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "clinical_hotspotting", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = dbutils.widgets.get("schema")
FQSN    = f"{CATALOG}.{SCHEMA}"
print(f"Creating metric views in {FQSN}")

# COMMAND ----------

# DBTITLE 1,Member-grain metric view: mv_clinical_hotspotting
spark.sql(f"DROP VIEW IF EXISTS {FQSN}.mv_clinical_hotspotting")
spark.sql(f"""
CREATE OR REPLACE VIEW {FQSN}.mv_clinical_hotspotting
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: {FQSN}.gold_member_hotspots
  comment: Governed member-level clinical hotspotting KPIs (one row per member — no fan-out)
  dimensions:
    - name: market
      expr: market
      comment: Geographic market (10 markets)
    - name: zip_area
      expr: zip_area
      comment: ZIP code area within market
    - name: risk_band
      expr: risk_band
      comment: Very High / High / Rising / Low
    - name: sdoh_risk_level
      expr: sdoh_risk_level
      comment: High / Moderate / Low social determinant risk
    - name: is_actionable_hotspot
      expr: is_actionable_hotspot
      comment: High utilization AND something the plan can fix
    - name: has_chf
      expr: has_chf
      comment: Member has congestive heart failure
    - name: has_diabetes
      expr: has_diabetes
      comment: Member has diabetes
    - name: has_ckd
      expr: has_ckd
      comment: Member has chronic kidney disease
    - name: has_copd
      expr: has_copd
      comment: Member has COPD
    - name: has_behavioral
      expr: has_behavioral
      comment: Member has a behavioral health condition
    - name: has_pcp_followup
      expr: has_pcp_followup
      comment: Member had at least one primary-care office visit
  measures:
    - name: total_members
      expr: COUNT(member_id)
      comment: Count of members
    - name: actionable_hotspot_members
      expr: SUM(CAST(is_actionable_hotspot AS INT))
      comment: Members flagged as actionable hotspots
    - name: actionable_hotspot_rate
      expr: ROUND(AVG(CAST(is_actionable_hotspot AS DOUBLE)), 3)
      comment: Fraction of members who are actionable hotspots
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: avg_risk_score
      expr: ROUND(AVG(risk_score), 2)
      comment: Average clinical risk score
    - name: avg_hotspot_score
      expr: ROUND(AVG(hotspot_score), 1)
      comment: Average composite hotspot score
    - name: ed_visits_per_1000
      expr: ROUND(SUM(ed_visits) * 1000.0 / NULLIF(COUNT(member_id), 0), 1)
      comment: ED visits per 1,000 members
    - name: admits_per_1000
      expr: ROUND(SUM(inpatient_admits) * 1000.0 / NULLIF(COUNT(member_id), 0), 1)
      comment: Inpatient admissions per 1,000 members
    - name: readmission_rate
      expr: ROUND(AVG(CAST(had_readmission AS DOUBLE)), 3)
      comment: Share of members with at least one 30-day readmission
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: avg_pdc
      expr: ROUND(AVG(avg_medication_adherence_pdc), 3)
      comment: Average medication adherence (proportion of days covered); >= 0.80 is adherent
    - name: avg_open_care_gaps
      expr: ROUND(AVG(CAST(open_care_gaps AS DOUBLE)), 2)
      comment: Average open care gaps per member
    - name: avg_sdoh_barriers
      expr: ROUND(AVG(CAST(sdoh_barrier_count AS DOUBLE)), 2)
      comment: Average SDoH barriers per member
    - name: high_sdoh_risk_rate
      expr: ROUND(AVG(CASE WHEN sdoh_risk_level = 'High' THEN 1.0 ELSE 0.0 END), 3)
      comment: Fraction of members with High SDoH risk
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: total_allowed_cost
      expr: ROUND(SUM(total_allowed_cost), 2)
      comment: Total allowed cost (facility + medical + pharmacy)
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
    - name: pmpm_cost
      expr: ROUND(SUM(total_allowed_cost) / NULLIF(COUNT(member_id), 0), 2)
      comment: Allowed cost per member over the data window
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
$$
""")
print("✓ mv_clinical_hotspotting created")

# COMMAND ----------

# DBTITLE 1,Encounter-grain metric view: mv_encounter_utilization
spark.sql(f"DROP VIEW IF EXISTS {FQSN}.mv_encounter_utilization")
spark.sql(f"""
CREATE OR REPLACE VIEW {FQSN}.mv_encounter_utilization
WITH METRICS
LANGUAGE YAML
AS $$
  version: 1.1
  source: >
    SELECT
      e.encounter_id, e.member_id, e.encounter_type, e.primary_dx_category, e.transplant_type,
      e.admit_date, e.los_days, e.readmission_30d, e.complication_flag, e.allowed_amount,
      e.market, p.provider_group, p.peer_group, p.specialty, m.risk_band
    FROM {FQSN}.silver_encounters e
    LEFT JOIN {FQSN}.silver_providers p ON e.provider_id = p.provider_id
    LEFT JOIN {FQSN}.gold_member_hotspots m ON e.member_id = m.member_id
  comment: Governed encounter-level utilization KPIs (one row per encounter; provider and member joins are many-to-one)
  dimensions:
    - name: admit_month
      expr: DATE_TRUNC('MONTH', source.admit_date)
      comment: Calendar month of admission
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
    - name: specialty
      expr: source.specialty
      comment: Provider specialty
    - name: market
      expr: source.market
      comment: Geographic market
    - name: risk_band
      expr: source.risk_band
      comment: Member risk band (Very High / High / Rising / Low)
  measures:
    - name: total_encounters
      expr: COUNT(source.encounter_id)
      comment: Count of encounters
    - name: members_with_encounters
      expr: COUNT(DISTINCT source.member_id)
      comment: Distinct members with at least one encounter
    - name: ed_visits
      expr: SUM(CASE WHEN source.encounter_type = 'ED' THEN 1 ELSE 0 END)
      comment: ED encounters
    - name: inpatient_admits
      expr: SUM(CASE WHEN source.encounter_type = 'Inpatient' THEN 1 ELSE 0 END)
      comment: Inpatient encounters
    - name: readmission_rate_30d
      expr: ROUND(AVG(CAST(source.readmission_30d AS DOUBLE)), 3)
      comment: Share of encounters followed by a 30-day readmission
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: complication_rate
      expr: ROUND(AVG(CAST(source.complication_flag AS DOUBLE)), 3)
      comment: Share of encounters with a documented complication
      format:
        type: percentage
        decimal_places:
          type: exact
          places: 1
    - name: avg_length_of_stay
      expr: ROUND(AVG(source.los_days), 1)
      comment: Average length of stay (days)
    - name: total_encounter_cost
      expr: ROUND(SUM(source.allowed_amount), 2)
      comment: Total allowed facility cost
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
    - name: avg_cost_per_encounter
      expr: ROUND(AVG(source.allowed_amount), 2)
      comment: Average allowed cost per encounter
      format:
        type: currency
        currency_code: USD
        decimal_places:
          type: exact
          places: 2
$$
""")
print("✓ mv_encounter_utilization created")

# COMMAND ----------

# DBTITLE 1,Validate: member KPIs by market
display(spark.sql(f"""
SELECT
  market,
  MEASURE(total_members)              AS total_members,
  MEASURE(actionable_hotspot_members) AS actionable_members,
  MEASURE(actionable_hotspot_rate)    AS actionable_rate,
  MEASURE(avg_risk_score)             AS avg_risk,
  MEASURE(ed_visits_per_1000)         AS ed_per_1000,
  MEASURE(pmpm_cost)                  AS pmpm
FROM {FQSN}.mv_clinical_hotspotting
GROUP BY ALL
ORDER BY total_members DESC
"""))

# COMMAND ----------

# DBTITLE 1,Validate: encounter KPIs by type and diagnosis
display(spark.sql(f"""
SELECT
  encounter_type,
  primary_dx_category,
  MEASURE(total_encounters)      AS encounters,
  MEASURE(readmission_rate_30d)  AS readmit_rate,
  MEASURE(avg_length_of_stay)    AS avg_los,
  MEASURE(avg_cost_per_encounter) AS avg_cost
FROM {FQSN}.mv_encounter_utilization
GROUP BY ALL
ORDER BY encounters DESC
LIMIT 20
"""))

# COMMAND ----------

# DBTITLE 1,Reconcile metric views with the gold / silver tables (fails on fan-out)
mv = spark.sql(f"""
SELECT MEASURE(total_members) AS members, MEASURE(actionable_hotspot_members) AS actionable,
       MEASURE(ed_visits_per_1000) AS ed_per_1000, MEASURE(total_allowed_cost) AS cost
FROM {FQSN}.mv_clinical_hotspotting
""").first()
gold = spark.sql(f"""
SELECT COUNT(*) AS members, SUM(CAST(is_actionable_hotspot AS INT)) AS actionable,
       ROUND(SUM(ed_visits) * 1000.0 / COUNT(*), 1) AS ed_per_1000, ROUND(SUM(total_allowed_cost), 2) AS cost
FROM {FQSN}.gold_member_hotspots
""").first()
enc_mv = spark.sql(f"SELECT MEASURE(total_encounters) AS n, MEASURE(ed_visits) AS ed FROM {FQSN}.mv_encounter_utilization").first()
enc = spark.sql(f"SELECT COUNT(*) AS n, SUM(CASE WHEN encounter_type = 'ED' THEN 1 ELSE 0 END) AS ed FROM {FQSN}.silver_encounters").first()

checks = {
    "members":         (mv.members, gold.members),
    "actionable":      (mv.actionable, gold.actionable),
    "ed_per_1000":     (float(mv.ed_per_1000), float(gold.ed_per_1000)),
    "total_cost":      (float(mv.cost), float(gold.cost)),
    "encounters":      (enc_mv.n, enc.n),
    "ed_encounters":   (enc_mv.ed, enc.ed),
    "ed_member_vs_enc": (round(float(gold.ed_per_1000) * gold.members / 1000), enc.ed),
}
bad = {k: v for k, v in checks.items() if abs(float(v[0]) - float(v[1])) > 0.01 * max(1.0, abs(float(v[1])))}
for k, (a, b) in checks.items():
    print(f"{'✗' if k in bad else '✓'} {k:<18} metric view = {a}   source = {b}")
if bad:
    raise AssertionError(f"Metric view does not reconcile with source tables: {bad}")
print(f"\n✓ Metric views reconcile. Actionable rate = {gold.actionable / gold.members:.3f}")
