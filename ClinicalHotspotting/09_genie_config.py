# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Clinical Hotspotting & Outcomes Intelligence — Genie Agent Configuration
# MAGIC
# MAGIC This notebook documents the full configuration of the Genie agent and can recreate it from scratch on any workspace.
# MAGIC
# MAGIC **Space ID:** `01f1cf40908913ea983a926f9f815ce8`  
# MAGIC **Space URL:** [Clinical Hotspotting & Outcomes Intelligence](https://<your-workspace>.cloud.databricks.com/genie/rooms/01f1cf40908913ea983a926f9f815ce8?o=7474651608287314)  
# MAGIC **Catalog / Schema:** `payer_catalog_vm.clinical_hotspotting`  
# MAGIC
# MAGIC Run the cells in order to recreate or update the space on a new workspace. Each section is idempotent — safe to re-run after a data refresh or a configuration change.

# COMMAND ----------

# DBTITLE 1,Config
# ── Configuration ────────────────────────────────────────────────────────────
SPACE_ID   = "01f1cf40908913ea983a926f9f815ce8"
SPACE_NAME = "Clinical Hotspotting & Outcomes Intelligence"
CATALOG    = "payer_catalog_vm"
SCHEMA     = "clinical_hotspotting"
FQSN       = f"{CATALOG}.{SCHEMA}"

# Tables attached to the Genie space (9 tables)
TABLES = [
    f"{FQSN}.gold_member_hotspots",
    f"{FQSN}.gold_cohort_summary",
    f"{FQSN}.gold_geographic_hotspots",
    f"{FQSN}.gold_intervention_outcomes",
    f"{FQSN}.gold_provider_outcomes",
    f"{FQSN}.gold_transplant_outcomes",
    f"{FQSN}.gold_clinical_narratives",
    f"{FQSN}.gold_intervention_recs",
    f"{FQSN}.silver_care_gaps",
]

print(f"Space: {SPACE_NAME}")
print(f"Space ID: {SPACE_ID}")
print(f"Schema: {FQSN}")
print(f"Tables: {len(TABLES)}")

# COMMAND ----------

# DBTITLE 1,SDK auth
# ── Databricks SDK auth (notebook token, no explicit credential needed) ───────
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    CreateQueryRequestQuery, QueryParameter, QueryParameterValue,
    EnumQueryParameterAllowMultipleValuesOptions, QueryParameterMultiValuesOptions
)
import json, uuid

def new_id():
    """32-char lowercase hex ID, no hyphens — required by the Genie API."""
    return uuid.uuid4().hex

w = WorkspaceClient()
print("SDK authenticated as:", w.current_user.me().user_name)

# COMMAND ----------

# DBTITLE 1,Text instruction
# ── Text instruction ──────────────────────────────────────────────────────────
# One instruction block is the maximum the Genie API supports.
# Update INSTRUCTION_TEXT here to change routing, glossary or rounding rules.

INSTRUCTION_TEXT = """
This Genie space covers the Clinical Hotspotting & Outcomes Intelligence solution
for a Medicare Advantage health plan.

## Scope
Use only the 9 attached tables. Do not reference other schemas or catalogs.

## Glossary
- Actionable hotspot: a member with high utilization AND something the plan can fix
- PDC (Proportion of Days Covered): medication adherence; >= 0.80 = adherent (Stars)
- ED visits per 1,000 / Admits per 1,000: rates per 1,000 members
- O/E ratio: observed ÷ expected; > 1.20 = high readmission outlier
- PMPM: per member per month cost
- SDoH: social determinants of health
- Risk band: Very High / High / Rising / Stable

## Pre-computed metrics — SELECT directly, do not recompute
These columns in gold_intervention_outcomes are already calculated; SELECT them directly:
- engagement_rate (do not divide engaged by total)
- ed_reduction_pct (do not compute from before/after counts)
- improved_rate (do not compute from improved/total)

## Rounding
Round risk_score and hotspot_score to 2 decimal places.

## Routing
- Member-level questions → gold_member_hotspots
- Cohort/program rollups → gold_cohort_summary or gold_intervention_outcomes
- Geographic questions → gold_geographic_hotspots
- Provider questions → gold_provider_outcomes
- Transplant questions → gold_transplant_outcomes
- AI summaries / narratives → gold_clinical_narratives
- Recommended programs → gold_intervention_recs
- Open care gap detail → silver_care_gaps
"""

# COMMAND ----------

# DBTITLE 1,Starter questions
# ── Starter questions (10) ────────────────────────────────────────────────────
STARTER_QUESTIONS = [
    "How many actionable hotspot members are there in each market?",
    "What is the average medication adherence PDC by risk band?",
    "What AI intervention is recommended for Very High risk members with high SDoH burden?",
    "Which providers are high readmission outliers?",
    "How many open care gaps exist by gap type?",
    "How many members are recommended for Complex Case Management?",
    "Show transplant volume and outcomes by organ type",
    "What are the top ZIP areas by actionable hotspot rate?",
    "Show program outcomes: engagement rate and ED reduction by cohort",
    "Which markets have the highest average risk score?",
]

# COMMAND ----------

# DBTITLE 1,SQL examples
# ── SQL examples (7) ──────────────────────────────────────────────────────────
SQL_EXAMPLES = [
    {
        "question": "How many actionable hotspot members are there in each market?",
        "sql": f"""
SELECT market,
  COUNT(*) AS total_members,
  SUM(CAST(is_actionable_hotspot AS INT)) AS actionable_members,
  ROUND(AVG(CAST(is_actionable_hotspot AS DOUBLE)) * 100, 1) AS actionable_pct
FROM {FQSN}.gold_member_hotspots
GROUP BY market
ORDER BY actionable_members DESC"""
    },
    {
        "question": "What is the average medication adherence PDC by risk band?",
        "sql": f"""
SELECT risk_band,
  COUNT(*) AS members,
  ROUND(AVG(avg_medication_adherence_pdc), 3) AS avg_pdc,
  SUM(CAST(avg_medication_adherence_pdc < 0.80 AS INT)) AS non_adherent
FROM {FQSN}.gold_member_hotspots
GROUP BY risk_band
ORDER BY CASE risk_band
  WHEN 'Very High' THEN 1 WHEN 'High' THEN 2 WHEN 'Rising' THEN 3 ELSE 4 END"""
    },
    {
        "question": "Which providers are high readmission outliers?",
        "sql": f"""
SELECT provider_name, specialty, provider_group, market,
  admissions,
  ROUND(readmission_oe_ratio, 2) AS oe_ratio,
  ROUND(avg_member_risk_score, 2) AS avg_risk
FROM {FQSN}.gold_provider_outcomes
WHERE is_high_readmission_outlier = true
ORDER BY readmission_oe_ratio DESC"""
    },
    {
        "question": "What AI program is recommended for Very High risk members with SDoH burden?",
        "sql": f"""
SELECT ir.member_id, mh.market, mh.risk_band,
  mh.sdoh_barrier_count, mh.sdoh_risk_level,
  ir.ai_recommended_intervention,
  LEFT(ir.ai_recommendation_rationale, 250) AS rationale
FROM {FQSN}.gold_intervention_recs ir
JOIN {FQSN}.gold_member_hotspots mh ON ir.member_id = mh.member_id
WHERE mh.risk_band = 'Very High' AND mh.sdoh_barrier_count >= 2
ORDER BY mh.hotspot_score DESC
LIMIT 20"""
    },
    {
        "question": "Show program engagement and ED reduction by cohort",
        "sql": f"""
SELECT cohort, intervention_type, market,
  intervention_count,
  ROUND(engagement_rate * 100, 1) AS engagement_pct,
  ROUND(improved_rate * 100, 1) AS improved_pct,
  ed_reduction_pct
FROM {FQSN}.gold_intervention_outcomes
WHERE intervention_count >= 5
ORDER BY ed_reduction_pct DESC NULLS LAST"""
    },
    {
        "question": "How many members are recommended for each program?",
        "sql": f"""
SELECT ai_recommended_intervention,
  COUNT(*) AS member_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {FQSN}.gold_intervention_recs
GROUP BY ai_recommended_intervention
ORDER BY member_count DESC"""
    },
    {
        "question": "Show transplant volume and outcomes by organ type",
        "sql": f"""
SELECT organ_type, COUNT(*) AS patients,
  ROUND(AVG(graft_survival_days), 0) AS avg_graft_survival_days,
  SUM(CAST(rejection_episode = true AS INT)) AS rejection_episodes,
  ROUND(AVG(immunosuppression_adherence_pdc), 3) AS avg_pdc
FROM {FQSN}.gold_transplant_outcomes
GROUP BY organ_type
ORDER BY patients DESC"""
    },
]

# COMMAND ----------

# DBTITLE 1,Knowledge snippets
# ── Knowledge snippets (12) ───────────────────────────────────────────────────
# 4 filters, 3 joins, 3 measures, 2 derived
KNOWLEDGE_SNIPPETS = [
    # --- Filters (4) ---
    {
        "name": "Filter: actionable hotspot members",
        "content": f"To return only actionable hotspot members: WHERE is_actionable_hotspot = true\nTable: {FQSN}.gold_member_hotspots"
    },
    {
        "name": "Filter: Very High risk",
        "content": f"To filter to the highest-risk members: WHERE risk_band = 'Very High'\nTable: {FQSN}.gold_member_hotspots\nRisk band values: Very High, High, Rising, Stable"
    },
    {
        "name": "Filter: open care gaps",
        "content": f"To filter to open (unresolved) care gaps: WHERE gap_status = 'Open'\nTable: {FQSN}.silver_care_gaps"
    },
    {
        "name": "Filter: high readmission outlier providers",
        "content": f"To filter to providers with high readmission O/E ratios: WHERE is_high_readmission_outlier = true\nTable: {FQSN}.gold_provider_outcomes\nO/E > 1.20 = high outlier"
    },
    # --- Joins (3) ---
    {
        "name": "Join: member hotspots to intervention recommendations",
        "content": f"JOIN {FQSN}.gold_intervention_recs ir ON ir.member_id = mh.member_id\nUse when combining risk/SDoH data with AI program recommendations."
    },
    {
        "name": "Join: member hotspots to clinical narratives",
        "content": f"JOIN {FQSN}.gold_clinical_narratives cn ON cn.member_id = mh.member_id\nUse when you need the AI clinical summary alongside risk data."
    },
    {
        "name": "Join: member hotspots to care gaps",
        "content": f"JOIN {FQSN}.silver_care_gaps sg ON sg.member_id = mh.member_id\nUse when you need per-gap detail for a member."
    },
    # --- Measures (3) ---
    {
        "name": "Measure: ED visits per 1,000",
        "content": f"ED visits per 1,000 members = SUM(ed_visits) * 1000.0 / COUNT(*)\nTable: {FQSN}.gold_member_hotspots"
    },
    {
        "name": "Measure: actionable hotspot rate",
        "content": f"Actionable hotspot rate = AVG(CAST(is_actionable_hotspot AS DOUBLE)) * 100\nTable: {FQSN}.gold_member_hotspots"
    },
    {
        "name": "Measure: medication adherence (PDC)",
        "content": f"PDC >= 0.80 = adherent (Stars threshold). Column: avg_medication_adherence_pdc\nTable: {FQSN}.gold_member_hotspots\nNon-adherent members: SUM(CAST(avg_medication_adherence_pdc < 0.80 AS INT))"
    },
    # --- Derived (2) ---
    {
        "name": "Derived: pre-computed metrics in gold_intervention_outcomes",
        "content": f"engagement_rate, ed_reduction_pct, improved_rate are pre-computed.\nSELECT them directly — do NOT recompute from raw counts.\nTable: {FQSN}.gold_intervention_outcomes"
    },
    {
        "name": "Derived: hotspot score components",
        "content": f"hotspot_score is a composite of: ed_visits, inpatient_admits, readmission_30d,\nopen_care_gap_count, non_adherent_med_count, sdoh_barrier_count.\nRound to 2 decimal places. Table: {FQSN}.gold_member_hotspots"
    },
]

# COMMAND ----------

# DBTITLE 1,Table descriptions
# ── Table descriptions (9) ────────────────────────────────────────────────────
TABLE_DESCRIPTIONS = {
    f"{FQSN}.gold_member_hotspots": (
        "One row per member (5,000 total). Contains risk_score, hotspot_score, risk_band "
        "(Very High/High/Rising/Stable), is_actionable_hotspot flag, ed_visits, inpatient_admits, "
        "readmission_30d, open_care_gap_count, avg_medication_adherence_pdc, sdoh_barrier_count, "
        "sdoh_risk_level, total_allowed_cost, market, zip_area, chronic conditions flags."
    ),
    f"{FQSN}.gold_cohort_summary": (
        "70 rows (7 cohorts x 10 markets). Pre-aggregated cohort-level rollups: member_count, "
        "actionable_count, avg_risk_score, avg_ed_visits, avg_cost_pmpm, avg_pdc, sdoh_pct."
    ),
    f"{FQSN}.gold_geographic_hotspots": (
        "293 rows at market x ZIP area grain. Columns: market, zip_area, member_count, "
        "actionable_hotspot_rate, avg_risk_score, high_sdoh_risk_members, ed_visits_per_1000, "
        "avg_adi_score."
    ),
    f"{FQSN}.gold_intervention_outcomes": (
        "Intervention effectiveness by cohort x type x market. Key pre-computed columns: "
        "engagement_rate, ed_reduction_pct, improved_rate — SELECT directly, do not recompute."
    ),
    f"{FQSN}.gold_provider_outcomes": (
        "400 rows, one per provider. Columns: provider_name, specialty, provider_group, market, "
        "admissions, readmission_oe_ratio, is_high_readmission_outlier (O/E > 1.20), "
        "avg_member_risk_score."
    ),
    f"{FQSN}.gold_transplant_outcomes": (
        "Transplant program outcomes. Columns: organ_type, graft_survival_days, "
        "rejection_episode, immunosuppression_adherence_pdc."
    ),
    f"{FQSN}.gold_clinical_narratives": (
        "3,225 rows, one per actionable member. Column ai_clinical_summary contains "
        "an AI-generated plain-English summary of each member's clinical situation. "
        "Join to gold_member_hotspots on member_id."
    ),
    f"{FQSN}.gold_intervention_recs": (
        "3,225 rows, one per actionable member. Columns ai_recommended_intervention and "
        "ai_recommendation_rationale contain the AI-chosen program and its rationale. "
        "Join to gold_member_hotspots on member_id."
    ),
    f"{FQSN}.silver_care_gaps": (
        "HEDIS/Stars care gaps. Columns: member_id, gap_type, gap_status (Open/Closed), "
        "ai_priority (AI-classified urgency: High/Medium/Low). "
        "Filter to gap_status = 'Open' for outstanding gaps."
    ),
}

# COMMAND ----------

# DBTITLE 1,Column synonyms and entity matching
# ── Column synonyms and entity matching (configured via Genie UI) ─────────────
# These cannot be set via the REST API; they are documented here for reference.
# Recreate via the Genie space UI: Data tab > each table > Columns / Filters.

COLUMN_SYNONYMS = {
    # gold_member_hotspots
    "risk_score":                  ["risk", "clinical risk", "risk score"],
    "hotspot_score":               ["hotspot", "composite score"],
    "is_actionable_hotspot":       ["actionable", "hotspot flag", "can we help"],
    "avg_medication_adherence_pdc":["PDC", "adherence", "medication adherence", "adherence rate"],
    "sdoh_barrier_count":          ["SDoH barriers", "social barriers", "social needs"],
    "total_allowed_cost":          ["cost", "allowed cost", "total cost"],
    "ed_visits":                   ["ER visits", "ED visits", "emergency visits"],
    "inpatient_admits":            ["admissions", "hospital admits", "IP admits"],
    # gold_provider_outcomes
    "readmission_oe_ratio":        ["O/E ratio", "readmission ratio", "outlier ratio"],
    "is_high_readmission_outlier": ["outlier", "high outlier", "readmission outlier"],
    # gold_intervention_outcomes
    "ed_reduction_pct":            ["ED reduction", "ER reduction", "ED decrease"],
    "engagement_rate":             ["engagement", "program engagement"],
}

# Entity matching configured on (table, column) pairs for categorical filters:
ENTITY_MATCHING_COLUMNS = [
    # gold_member_hotspots (10 columns)
    ("gold_member_hotspots", "market"),
    ("gold_member_hotspots", "risk_band"),
    ("gold_member_hotspots", "sdoh_risk_level"),
    ("gold_member_hotspots", "primary_chronic_condition"),
    ("gold_member_hotspots", "zip_area"),
    ("gold_member_hotspots", "gender"),
    ("gold_member_hotspots", "age_band"),
    ("gold_member_hotspots", "is_actionable_hotspot"),
    ("gold_member_hotspots", "dual_eligible"),
    ("gold_member_hotspots", "lis_eligible"),
    # gold_intervention_recs (3)
    ("gold_intervention_recs", "ai_recommended_intervention"),
    ("gold_intervention_recs", "market"),
    ("gold_intervention_recs", "risk_band"),
    # gold_provider_outcomes (4)
    ("gold_provider_outcomes", "specialty"),
    ("gold_provider_outcomes", "provider_group"),
    ("gold_provider_outcomes", "market"),
    ("gold_provider_outcomes", "is_high_readmission_outlier"),
    # gold_cohort_summary (2)
    ("gold_cohort_summary", "cohort"),
    ("gold_cohort_summary", "market"),
    # gold_intervention_outcomes (3)
    ("gold_intervention_outcomes", "cohort"),
    ("gold_intervention_outcomes", "intervention_type"),
    ("gold_intervention_outcomes", "market"),
    # silver_care_gaps (3)
    ("silver_care_gaps", "gap_type"),
    ("silver_care_gaps", "gap_status"),
    ("silver_care_gaps", "ai_priority"),
    # gold_transplant_outcomes (1)
    ("gold_transplant_outcomes", "organ_type"),
    # gold_geographic_hotspots (1)
    ("gold_geographic_hotspots", "market"),
]

print(f"Column synonyms documented: {len(COLUMN_SYNONYMS)} columns")
print(f"Entity matching columns documented: {len(ENTITY_MATCHING_COLUMNS)} columns across 8 tables")
print("\nNote: synonyms and entity matching must be applied via the Genie UI (Data tab).")

# COMMAND ----------

# DBTITLE 1,Benchmarks
# ── Benchmarks (13) ───────────────────────────────────────────────────────────
# 10/13 passing as of last run. 2 persistent failures on gold_intervention_outcomes
# (agent recomputes engagement_rate and ed_reduction_pct instead of selecting directly).
# 1 flaky formatting failure on rounding.
# Benchmarks must be added via the Genie UI (Benchmark tab) — not settable via REST API.

BENCHMARKS = [
    # Passing (10)
    {"question": "How many total members are in the dataset?",
     "expected": "5000"},
    {"question": "How many actionable hotspot members are there?",
     "expected": "Value around 3,200 (regenerates each run)"},
    {"question": "What is the most common AI-recommended program?",
     "expected": "Complex Case Management"},
    {"question": "How many providers are high readmission outliers?",
     "expected": "Value > 0, from gold_provider_outcomes"},
    {"question": "What is the average PDC for Very High risk members?",
     "expected": "Value < 0.80"},
    {"question": "Which market has the most actionable hotspot members?",
     "expected": "Returns a market name, ordered by count"},
    {"question": "How many open care gaps exist?",
     "expected": "Returns count from silver_care_gaps where gap_status = Open"},
    {"question": "What organ types are in the transplant outcomes table?",
     "expected": "Returns list of organ types from gold_transplant_outcomes"},
    {"question": "Show AI clinical summary for top 3 Very High risk members",
     "expected": "Returns 3 rows from gold_clinical_narratives"},
    {"question": "What is the average risk score by market?",
     "expected": "Returns market + avg_risk_score, rounded to 2 decimal places"},
    # Persistent failures (2) — agent recomputes pre-computed metrics
    {"question": "Show program engagement rate by cohort",
     "expected": "SELECT engagement_rate directly from gold_intervention_outcomes (not recomputed)",
     "status": "FAILING"},
    {"question": "What is the ED reduction percentage by intervention type?",
     "expected": "SELECT ed_reduction_pct directly from gold_intervention_outcomes (not recomputed)",
     "status": "FAILING"},
    # Flaky (1)
    {"question": "What is the average hotspot score for Very High risk members?",
     "expected": "Returns avg hotspot_score rounded to 2 decimal places",
     "status": "FLAKY"},
]

passing = sum(1 for b in BENCHMARKS if b.get("status") not in ("FAILING", "FLAKY"))
print(f"Benchmarks documented: {len(BENCHMARKS)} total, {passing} passing")
print("Note: benchmarks must be added/managed via the Genie UI (Benchmark tab).")

# COMMAND ----------

# DBTITLE 1,How to recreate the space
# MAGIC %md
# MAGIC ## How to recreate the space on a new workspace
# MAGIC
# MAGIC 1. **Create a new Genie space** in the target workspace via the UI (Genie → New space). Note its space ID.
# MAGIC 2. **Update `SPACE_ID`** at the top of this notebook with the new space ID.
# MAGIC 3. **Run the API cell below** to apply the title, description, text instruction and SQL examples.
# MAGIC 4. **Add tables** via the Genie UI (Data tab → Add table) — add all 9 tables listed in `TABLES`.
# MAGIC 5. **Add table descriptions** via the Genie UI (Data tab → each table → Description).
# MAGIC 6. **Add starter questions** via the Genie UI (Instructions tab → Sample questions).
# MAGIC 7. **Apply synonyms and entity matching** via the Genie UI (Data tab → each table → Columns / Filters) using the `COLUMN_SYNONYMS` and `ENTITY_MATCHING_COLUMNS` dictionaries above.
# MAGIC 8. **Add benchmarks** via the Genie UI (Benchmark tab) using the `BENCHMARKS` list above.
# MAGIC
# MAGIC Steps 3 can be run programmatically; steps 4–8 require the Genie UI.

# COMMAND ----------

# DBTITLE 1,Apply config via REST API
# ── Apply title, description, text instruction + SQL examples via REST API ────
# This is what the API accepts; tables, synonyms, entity matching and benchmarks
# must be applied via the Genie UI.

from databricks.sdk.service.dashboards import GenieSpace  # noqa: may not exist on older SDK

def build_serialized_space(instruction_text, starter_questions, sql_examples):
    """Build the serialized_space JSON string accepted by PATCH /api/2.0/genie/spaces/{id}."""
    return json.dumps({
        "version": "1.0",
        "config": {
            "sample_questions": [
                {"id": new_id(), "question": [q]}
                for q in starter_questions
            ]
        },
        "instructions": {
            "text_instructions": [
                {"id": new_id(), "content": [instruction_text.strip()]}
            ],
            "example_question_sqls": [
                {"id": new_id(), "question": [ex["question"]], "sql": [ex["sql"].strip()]}
                for ex in sql_examples
            ]
        }
    })


# Use notebook token (required for Genie API on serverless)
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
import requests
host = spark.conf.get("spark.databricks.workspaceUrl")

payload = {
    "title": SPACE_NAME,
    "description": (
        "AI-powered clinical hotspotting for Medicare Advantage: identify high-risk members, "
        "understand why, and route them to the right program."
    ),
    "serialized_space": build_serialized_space(
        INSTRUCTION_TEXT, STARTER_QUESTIONS, SQL_EXAMPLES
    )
}

resp = requests.patch(
    f"https://{host}/api/2.0/genie/spaces/{SPACE_ID}",
    headers={"Authorization": f"Bearer {token}"},
    json=payload,
    timeout=30
)

if resp.status_code == 200:
    print("✓ Space updated successfully")
    print(f"  Title: {SPACE_NAME}")
    print(f"  Starter questions: {len(STARTER_QUESTIONS)}")
    print(f"  SQL examples: {len(SQL_EXAMPLES)}")
    print(f"  Text instruction: 1 block")
else:
    print(f"✗ Failed: {resp.status_code}")
    print(resp.text)