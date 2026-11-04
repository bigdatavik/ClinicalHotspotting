# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Clinical Hotspotting & Outcomes Intelligence
# MAGIC
# MAGIC Identifying the members who drive avoidable cost, understanding why, and routing them to the right program — end to end on Databricks.
# MAGIC
# MAGIC - **Ask the data** — plain-English answers from the Genie agent
# MAGIC - **One set of definitions** — the metric view
# MAGIC - **Who needs help now** — hotspot scoring
# MAGIC - **How the data is prepared** — Gold, Silver with AI, Bronze
# MAGIC - **The source feeds**
# MAGIC - **One governed place**
# MAGIC - **Always up to date**
# MAGIC
# MAGIC **Schema:** `payer_catalog_vm.clinical_hotspotting`  
# MAGIC **Pipeline:** `clinical-hotspotting-sdp` (serverless, triggered)  
# MAGIC **Genie Agent:** Clinical Hotspotting & Outcomes Intelligence (`01f1cf40908913ea983a926f9f815ce8`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## End To End Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1793768752818.png](./image_1793768752818.png "image_1793768752818.png")

# COMMAND ----------

# DBTITLE 1,Key concepts
# MAGIC %md
# MAGIC ## Key concepts
# MAGIC
# MAGIC | Concept | What it is, in plain terms |
# MAGIC | --- | --- |
# MAGIC | **Genie agent** | An AI assistant that answers business questions in plain English. It writes and runs the SQL against approved tables, using the plan's own definitions and synonyms, and shows the query behind every answer. |
# MAGIC | **Metric view** | A single, shared definition of each KPI (for example readmission rate or PMPM). Genie, dashboards and notebooks all calculate the number the same way, so every team sees the same result. |
# MAGIC | **Pipeline** | An automated assembly line that takes raw data and turns it into trusted, business-ready tables, refreshing as new data arrives. Built with Lakeflow Spark Declarative Pipelines. |
# MAGIC | **Bronze layer** | The landing zone. Each feed is stored exactly as received and checked for basic quality problems. |
# MAGIC | **Silver layer** | The cleaned layer. Data is standardized and matched across feeds, and AI adds insight to each record. |
# MAGIC | **Gold layer** | The answer layer. Tables built for specific business questions — who needs help, why, and which program fits. |
# MAGIC | **AI functions** | AI built into SQL (classify, summarize, analyze sentiment) that reads each record as part of the pipeline — no separate model to build or host. |
# MAGIC | **Unity Catalog** | One place to govern all data and AI assets: who can see what, where each number came from, and who accessed it. |
# MAGIC | **Job** | A scheduled workflow that runs the steps in order so everything stays up to date without manual effort. |

# COMMAND ----------

# DBTITLE 1,Acronyms and terms
# MAGIC %md
# MAGIC ## Acronyms and terms
# MAGIC
# MAGIC | Term | Meaning |
# MAGIC | --- | --- |
# MAGIC | ED / ER | Emergency department / emergency room visit |
# MAGIC | ED visits per 1,000 | ED visits per 1,000 members |
# MAGIC | Admits per 1,000 | Inpatient admissions per 1,000 members |
# MAGIC | Readmission (30-day) | Return inpatient admission within 30 days of discharge |
# MAGIC | LOS | Length of stay |
# MAGIC | O/E ratio | Observed ÷ expected rate, adjusted for member risk (above 1.20 = high outlier) |
# MAGIC | CHF / COPD / CKD / BH | Congestive heart failure / chronic obstructive pulmonary disease / chronic kidney disease / behavioral health |
# MAGIC | PCP | Primary care provider |
# MAGIC | Actionable hotspot | High-utilization member with something the plan can fix |
# MAGIC | PDC | Proportion of days covered — medication adherence; ≥ 0.80 = adherent (Star Ratings) |
# MAGIC | Part D | Medicare prescription drug benefit |
# MAGIC | HEDIS | Healthcare Effectiveness Data and Information Set — quality measures / care gaps |
# MAGIC | Stars | CMS Medicare Advantage Star Ratings |
# MAGIC | PMPM | Per member per month cost |
# MAGIC | SDoH | Social determinants of health (housing, food, transportation, isolation) |
# MAGIC | ADI | Area deprivation index (higher = more disadvantaged neighborhood) |
# MAGIC | CHW | Community health worker |
# MAGIC | ADT | Admit / discharge / transfer notifications |
# MAGIC | PHI | Protected health information |
# MAGIC | SDP | Lakeflow Spark Declarative Pipeline |
# MAGIC | UC | Unity Catalog |
# MAGIC | Metric view | UC object that defines KPIs once; queried with `MEASURE()` |

# COMMAND ----------

# DBTITLE 1,Setup
# Setup
CATALOG = "payer_catalog_vm"
SCHEMA = "clinical_hotspotting"
SEED = 42
FQSN = f"{CATALOG}.{SCHEMA}"

print(f"Target schema: {FQSN}")
print(f"Seed: {SEED}")

tables = spark.sql(f"SHOW TABLES IN {FQSN}").collect()
print(f"\nObjects in {FQSN}: {len(tables)}")
for t in sorted(tables, key=lambda x: x.tableName):
    print(f"  {t.tableName}")

# COMMAND ----------

# DBTITLE 1,Ask the data: Genie agent
# MAGIC %md
# MAGIC ## Ask the data: the Genie agent
# MAGIC
# MAGIC > **What is a Genie agent?** An AI assistant for business users. You ask a question in plain English; it works out which approved tables to use, writes the SQL, runs it, and returns the answer with the query shown — no dashboard to build and no ticket to file.
# MAGIC
# MAGIC [Clinical Hotspotting & Outcomes Intelligence](https://<your-workspace>.cloud.databricks.com/genie/rooms/01f1cf40908913ea983a926f9f815ce8?o=7474651608287314) answers questions in plain English over curated, governed tables. It knows the plan's business definitions and synonyms, shows the SQL behind every answer, and is tested against benchmark questions with known answers.
# MAGIC
# MAGIC ### Example questions
# MAGIC 1. How many actionable hotspot members are there in each market?
# MAGIC 2. What is the average medication adherence PDC by risk band?
# MAGIC 3. What AI intervention is recommended for Very High risk members with high SDoH burden?
# MAGIC 4. Which providers are high readmission outliers?
# MAGIC 5. How many open care gaps exist?
# MAGIC 6. How many members are recommended for Complex Case Management?
# MAGIC 7. Show transplant volume and outcomes by organ type
# MAGIC
# MAGIC The results below come from the same queries Genie generates.

# COMMAND ----------

# DBTITLE 1,Genie answers
# Genie answers

# Q1: Actionable hotspots by market
print("Q1: Actionable hotspot members by market")
display(spark.sql(f"""
SELECT market,
  COUNT(*) AS total_members,
  SUM(CAST(is_actionable_hotspot AS INT)) AS actionable_members,
  ROUND(AVG(CAST(is_actionable_hotspot AS DOUBLE)) * 100, 1) AS actionable_pct
FROM {FQSN}.gold_member_hotspots
GROUP BY market ORDER BY actionable_members DESC
"""))

# Q2: AI intervention recommendations for Very High risk + SDoH burden
print("Q2: AI interventions for Very High risk members with SDoH burden")
display(spark.sql(f"""
SELECT ir.member_id, mh.market, mh.risk_band,
  mh.ed_visits, mh.sdoh_risk_level, mh.sdoh_barrier_count,
  ir.ai_recommended_intervention,
  LEFT(ir.ai_recommendation_rationale, 200) AS rationale
FROM {FQSN}.gold_intervention_recs ir
JOIN {FQSN}.gold_member_hotspots mh ON ir.member_id = mh.member_id
WHERE mh.risk_band = 'Very High' AND mh.sdoh_barrier_count >= 2
ORDER BY mh.hotspot_score DESC LIMIT 10
"""))

# Q3: Provider readmission outliers
print("Q3: High readmission outlier providers")
display(spark.sql(f"""
SELECT provider_name, specialty, provider_group, market,
  admissions, readmission_oe_ratio,
  ROUND(avg_member_risk_score, 2) AS avg_risk
FROM {FQSN}.gold_provider_outcomes
WHERE is_high_readmission_outlier = true
ORDER BY readmission_oe_ratio DESC LIMIT 10
"""))

# Q4: AI clinical narrative sample
print("Q4: Sample AI clinical narratives")
display(spark.sql(f"""
SELECT member_id, market, risk_band, risk_score,
  LEFT(ai_clinical_summary, 300) AS narrative_preview
FROM {FQSN}.gold_clinical_narratives
WHERE risk_band = 'Very High'
ORDER BY risk_score DESC LIMIT 3
"""))

# COMMAND ----------

# DBTITLE 1,AI-recommended programs
# AI-recommended program mix
display(spark.sql(f"""
SELECT ai_recommended_intervention,
  COUNT(*) AS member_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {FQSN}.gold_intervention_recs
GROUP BY ai_recommended_intervention
ORDER BY member_count DESC
"""))

# COMMAND ----------

# DBTITLE 1,How it's built
# MAGIC %md
# MAGIC # How it's built
# MAGIC
# MAGIC | Layer | What it does |
# MAGIC | --- | --- |
# MAGIC | Metric view | One definition for every KPI |
# MAGIC | Hotspot scoring | Finds the members who need help now |
# MAGIC | Gold | Answers for each team |
# MAGIC | Silver + AI | Cleans the data and adds AI insight to every record |
# MAGIC | Bronze | Lands each feed as sent and checks it |
# MAGIC | Source feeds | The data the plan already has |

# COMMAND ----------

# DBTITLE 1,One set of definitions: metric view
# MAGIC %md
# MAGIC ## One set of definitions: the metric view
# MAGIC
# MAGIC > **What is a metric view?** A shared, governed definition of each business KPI. Instead of every report calculating "readmission rate" its own way, the formula lives in one place and every tool — Genie, dashboards, notebooks — uses it.
# MAGIC
# MAGIC [mv_clinical_hotspotting](https://<your-workspace>.cloud.databricks.com/explore/data/payer_catalog_vm/clinical_hotspotting/mv_clinical_hotspotting?o=7474651608287314) — 10 dimensions x 14 measures, including:
# MAGIC - ED visits per 1,000, admits per 1,000, readmission rate
# MAGIC - Actionable hotspot rate, average risk score, average hotspot score
# MAGIC - PMPM cost, total allowed cost
# MAGIC - SDoH barriers, open care gaps, medication adherence (PDC)
# MAGIC
# MAGIC Each KPI is defined once in Unity Catalog, so Genie, dashboards and notebooks all return the same number.

# COMMAND ----------

# DBTITLE 1,KPIs by market
# KPIs by market
df = spark.sql(f"""
SELECT
  market,
  COUNT(*) AS total_members,
  SUM(CAST(is_actionable_hotspot AS INT)) AS actionable_members,
  ROUND(AVG(CAST(is_actionable_hotspot AS DOUBLE)) * 100, 1) AS actionable_pct,
  ROUND(AVG(risk_score), 2) AS avg_risk,
  ROUND(SUM(ed_visits) * 1000.0 / COUNT(*), 0) AS ed_per_1000,
  ROUND(SUM(inpatient_admits) * 1000.0 / COUNT(*), 0) AS admits_per_1000,
  ROUND(SUM(total_allowed_cost) / COUNT(*), 0) AS cost_per_member
FROM {FQSN}.gold_member_hotspots
GROUP BY market
ORDER BY actionable_members DESC
""")
display(df)

# COMMAND ----------

# DBTITLE 1,Who needs help now: hotspot scoring
# MAGIC %md
# MAGIC ## Who needs help now: hotspot scoring
# MAGIC
# MAGIC > **What is hotspotting?** Finding the small group of members who drive a large share of avoidable cost — repeat ER visits, admissions, readmissions — while there is still time to help them.
# MAGIC
# MAGIC [gold_member_hotspots](https://<your-workspace>.cloud.databricks.com/explore/data/payer_catalog_vm/clinical_hotspotting/gold_member_hotspots?o=7474651608287314) has one row per member with a composite **hotspot score** (ED visits, admissions, 30-day readmissions, open care gaps, non-adherent medications, social barriers) and an **actionable** flag: high utilization *and* something the plan can fix. 3,225 of 5,000 members are actionable.
# MAGIC
# MAGIC Below: the member profile by risk band, and the ZIP areas with the highest share of actionable members.

# COMMAND ----------

# DBTITLE 1,Members by risk band
# Members by risk band
display(spark.sql(f"""
SELECT risk_band,
  COUNT(*) AS members,
  SUM(CAST(is_actionable_hotspot AS INT)) AS actionable,
  ROUND(AVG(risk_score), 2) AS avg_risk,
  ROUND(AVG(hotspot_score), 1) AS avg_hotspot,
  ROUND(AVG(ed_visits), 1) AS avg_ed,
  ROUND(AVG(total_allowed_cost), 0) AS avg_cost,
  ROUND(AVG(avg_medication_adherence_pdc), 3) AS avg_pdc
FROM {FQSN}.gold_member_hotspots
GROUP BY risk_band
ORDER BY CASE risk_band
  WHEN 'Very High' THEN 1 WHEN 'High' THEN 2 WHEN 'Rising' THEN 3 ELSE 4 END
"""))

# COMMAND ----------

# DBTITLE 1,Geographic hotspots
# Geographic hotspots
display(spark.sql(f"""
SELECT market, zip_area, member_count,
  ROUND(actionable_hotspot_rate * 100, 1) AS actionable_pct,
  ROUND(avg_risk_score, 2) AS avg_risk,
  high_sdoh_risk_members,
  ROUND(ed_visits_per_1000) AS ed_per_1000
FROM {FQSN}.gold_geographic_hotspots
WHERE member_count >= 5
ORDER BY actionable_hotspot_rate DESC LIMIT 15
"""))

# COMMAND ----------

# DBTITLE 1,How the data is prepared
# MAGIC %md
# MAGIC ## How the data is prepared
# MAGIC
# MAGIC > **What is the pipeline?** An automated assembly line that turns raw feeds into trusted, business-ready tables in three layers — **Bronze** (raw, checked), **Silver** (cleaned, with AI insight) and **Gold** (answers for each team). It refreshes as new data arrives.
# MAGIC
# MAGIC The [clinical-hotspotting-sdp](https://<your-workspace>.cloud.databricks.com/pipelines/3a787df4-c258-4195-89e2-7c8df1cd5d7e?o=7474651608287314) pipeline: Gold (answers for each team) ← Silver (cleaned, with AI insight) ← Bronze (raw data, checked).
# MAGIC
# MAGIC ### Gold: answers for each team (8 tables)
# MAGIC
# MAGIC > **What is a Gold table?** A table built to answer a specific business question for a specific team. This is what Genie and the metric view read from.
# MAGIC
# MAGIC - `gold_member_hotspots` — 5,000 members, 3,225 actionable
# MAGIC - `gold_cohort_summary` — 70 rows (7 cohorts x 10 markets)
# MAGIC - `gold_geographic_hotspots` — 293 ZIP-area combinations
# MAGIC - `gold_intervention_outcomes` — pre/post ED reduction
# MAGIC - `gold_provider_outcomes` — 400 providers, O/E readmission ratios
# MAGIC - `gold_transplant_outcomes` — transplant program outcomes
# MAGIC - `gold_clinical_narratives` — 3,225 AI clinical summaries
# MAGIC - `gold_intervention_recs` — 3,225 AI program recommendations

# COMMAND ----------

# DBTITLE 1,Gold tables
# Gold tables
gold_tables = {
    "gold_member_hotspots": "Member-level hotspotting",
    "gold_cohort_summary": "Cohort x market rollups",
    "gold_geographic_hotspots": "Geographic hotspot analysis",
    "gold_intervention_outcomes": "Intervention effectiveness",
    "gold_provider_outcomes": "Provider benchmarking",
    "gold_transplant_outcomes": "Transplant program outcomes",
    "gold_clinical_narratives": "AI clinical narratives",
    "gold_intervention_recs": "AI intervention recommendations",
}

print("Gold Table Row Counts:")
print("-" * 70)
for t, desc in gold_tables.items():
    cnt = spark.table(f"{FQSN}.{t}").count()
    print(f"  {t:30s} {cnt:>6,}  {desc}")
print("-" * 70)

row = spark.sql(f"""
    SELECT COUNT(*) AS total, SUM(CAST(is_actionable_hotspot AS INT)) AS actionable
    FROM {FQSN}.gold_member_hotspots
""").collect()[0]
print(f"\nActionable hotspot members: {row.actionable:,} of {row.total:,} ({100.0 * row.actionable / row.total:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Program outcomes
# Program outcomes
display(spark.sql(f"""
SELECT cohort, intervention_type, market,
  intervention_count,
  ROUND(engagement_rate * 100, 1) AS engagement_pct,
  ROUND(improved_rate * 100, 1) AS improved_pct,
  ed_reduction_pct
FROM {FQSN}.gold_intervention_outcomes
WHERE intervention_count >= 5
ORDER BY ed_reduction_pct DESC NULLS LAST
LIMIT 10
"""))

# COMMAND ----------

# DBTITLE 1,Silver: cleaned, with AI insight
# MAGIC %md
# MAGIC ### Silver: cleaned, with AI insight
# MAGIC
# MAGIC > **What is a Silver table?** The cleaned version of each feed — standardized, with quality rules applied, ready to combine. **AI functions** are SQL functions that use AI to classify, summarize or score sentiment on each record as it passes through.
# MAGIC
# MAGIC As each record flows through Silver, AI reads it and adds what a person would conclude: which open gaps are urgent, which members may stop their medications, and whether outreach landed. Below: how many records received each AI label.

# COMMAND ----------

# DBTITLE 1,AI labels in Silver
# AI labels added in Silver
display(spark.sql(f"""
SELECT 'Open care gap priority' AS ai_insight, ai_priority AS ai_label, COUNT(*) AS records
FROM {FQSN}.silver_care_gaps WHERE gap_status = 'Open' GROUP BY ai_priority
UNION ALL
SELECT 'Medication adherence risk', ai_adherence_risk, COUNT(*)
FROM {FQSN}.silver_pharmacy_claims GROUP BY ai_adherence_risk
UNION ALL
SELECT 'Outreach engagement sentiment', ai_engagement_sentiment, COUNT(*)
FROM {FQSN}.silver_care_interventions GROUP BY ai_engagement_sentiment
ORDER BY ai_insight, records DESC
"""))

# COMMAND ----------

# DBTITLE 1,Bronze: raw data, checked
# MAGIC %md
# MAGIC ### Bronze: raw data, checked
# MAGIC
# MAGIC > **What is a Bronze table?** The landing zone — each feed stored exactly as it arrived, so there is always a record of the original data.
# MAGIC
# MAGIC Every feed lands in Bronze exactly as sent, and basic quality rules (valid member IDs, sensible dates and amounts) are checked on every run, so problems are caught before anyone acts on the data. The pipeline graph shows pass/fail counts for each check: [clinical-hotspotting-sdp](https://<your-workspace>.cloud.databricks.com/pipelines/3a787df4-c258-4195-89e2-7c8df1cd5d7e?o=7474651608287314).

# COMMAND ----------

# DBTITLE 1,The source feeds
# MAGIC %md
# MAGIC ## The source feeds
# MAGIC
# MAGIC Eight synthetic feeds (no PHI) that mirror the data a plan already has:
# MAGIC - **members** — enrollment: demographics, risk, chronic conditions
# MAGIC - **providers** — network roster: specialty, groups, transplant centers
# MAGIC - **encounters** — ED / inpatient / observation visits
# MAGIC - **medical_claims** — facility and professional claims
# MAGIC - **pharmacy_claims** — Part D fills, adherence
# MAGIC - **care_gaps** — HEDIS / Stars gaps
# MAGIC - **care_interventions** — care management outreach
# MAGIC - **sdoh** — social needs screenings
# MAGIC
# MAGIC In production these arrive through Lakeflow Connect or Auto Loader.

# COMMAND ----------

# DBTITLE 1,Source row counts
# Source feed row counts
source_tables = [
    "members", "providers", "encounters", "medical_claims",
    "pharmacy_claims", "care_gaps", "care_interventions", "sdoh"
]

print("Source Table Row Counts:")
print("-" * 40)
total = 0
for t in source_tables:
    cnt = spark.table(f"{FQSN}.{t}").count()
    total += cnt
    print(f"  {t:25s} {cnt:>8,}")
print("-" * 40)
print(f"  {'TOTAL':25s} {total:>8,}")

# COMMAND ----------

# DBTITLE 1,One governed place
# MAGIC %md
# MAGIC ## One governed place
# MAGIC
# MAGIC > **What is Unity Catalog?** One place to govern every data and AI asset: who can see what, where each number came from (lineage), and who accessed it (audit).
# MAGIC
# MAGIC Everything above — feeds, pipeline layers, AI output, metric view — lives in one Unity Catalog schema, [payer_catalog_vm.clinical_hotspotting](https://<your-workspace>.cloud.databricks.com/explore/data/payer_catalog_vm/clinical_hotspotting?o=7474651608287314), with the same access control, lineage and audit. Below: every object, by layer.

# COMMAND ----------

# DBTITLE 1,Objects by layer
# Objects in the governed schema, by layer
objects = [r.tableName for r in spark.sql(f"SHOW TABLES IN {FQSN}").collect()]
layers = {"Source feeds": [], "Bronze": [], "Silver": [], "Gold": [], "Metric view": []}
for name in sorted(objects):
    if name.startswith("bz_"):
        layers["Bronze"].append(name)
    elif name.startswith("silver_"):
        layers["Silver"].append(name)
    elif name.startswith("gold_"):
        layers["Gold"].append(name)
    elif name.startswith("mv_"):
        layers["Metric view"].append(name)
    else:
        layers["Source feeds"].append(name)

print(f"{FQSN}: {len(objects)} objects\n")
for layer, names in layers.items():
    print(f"{layer} ({len(names)}): {', '.join(names)}")

# COMMAND ----------

# DBTITLE 1,Always up to date
# MAGIC %md
# MAGIC ## Always up to date
# MAGIC
# MAGIC > **What is a job?** A scheduled workflow that runs each step in the right order, so the data, AI output and answers stay current without anyone running them by hand.
# MAGIC
# MAGIC [Clinical Hotspotting End-to-End Pipeline](https://<your-workspace>.cloud.databricks.com/jobs/223148564459396?o=7474651608287314) runs three tasks in order — load data → run the pipeline → validate the results — on a schedule or when new data lands ([run history](https://<your-workspace>.cloud.databricks.com/jobs/223148564459396/runs?o=7474651608287314)). The metric view and Genie read Gold directly, so they always reflect the latest run.

# COMMAND ----------

# DBTITLE 1,Build summary
# MAGIC %md
# MAGIC ## Build summary
# MAGIC
# MAGIC | Step | Component | Objects | Notes |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | Config | Schema setup | `00_config` |
# MAGIC | 2 | Data generation | 8 source tables | ~102K rows total |
# MAGIC | 3 | Bronze (SDP) | 8 `bz_*` tables | Mirrors source with expectations |
# MAGIC | 4 | Silver (SDP + AI) | 8 `silver_*` tables | 5 AI-enriched |
# MAGIC | 5 | Gold (SDP) | 8 `gold_*` tables | 3,225 actionable members |
# MAGIC | 6 | Metric view | `mv_clinical_hotspotting` | 14 measures x 10 dimensions |
# MAGIC | 7 | Genie agent | 9 tables, 13 benchmarks | 10/13 passing |
# MAGIC
# MAGIC ### Notebooks in this project
# MAGIC - `00_config` — shared constants
# MAGIC - `01_generate_synthetic_data` — data generation
# MAGIC - `02_sdp_bronze` — Bronze layer
# MAGIC - `03_sdp_silver_ai` — Silver layer with AI functions
# MAGIC - `04_sdp_gold` — Gold layer
# MAGIC - `05_create_metric_view` — metric view DDL
# MAGIC - `06_demo` — this notebook
# MAGIC
# MAGIC ### To rebuild from scratch
# MAGIC 1. Run `01_generate_synthetic_data`
# MAGIC 2. Trigger pipeline `clinical-hotspotting-sdp`
# MAGIC 3. Run the metric view DDL from `05_create_metric_view` on a SQL warehouse
# MAGIC 4. The Genie agent picks up refreshed data from its attached tables automatically