# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 09 — Genie Agent: Clinical Hotspotting & Outcomes Intelligence
# MAGIC
# MAGIC Creates (or updates, if one with the same title already exists) the **Genie agent** on top of the Gold tables,
# MAGIC fully configured as code — no UI steps:
# MAGIC
# MAGIC | Surface | What is configured |
# MAGIC |---|---|
# MAGIC | Data | 9 tables (8 gold + `silver_care_gaps`) with column descriptions, synonyms and entity matching |
# MAGIC | Instructions | Glossary, routing rules, rounding, "use pre-computed metrics" rule |
# MAGIC | Example SQL | 8 question → SQL pairs |
# MAGIC | Join specs | member hotspots ↔ narratives / recommendations / care gaps |
# MAGIC | SQL snippets | Filters, measures and expressions (business definitions) |
# MAGIC | Sample questions | 10 starter questions shown in the UI |
# MAGIC | Benchmarks | 12 questions with reference SQL for Genie evaluation |
# MAGIC
# MAGIC Runs as the last task of the **Clinical Hotspotting End-to-End Pipeline** job (the tables must exist before a Genie
# MAGIC space can reference them). Idempotent — safe to re-run after a data refresh or a config change.
# MAGIC
# MAGIC Parameters (widgets / job parameters): `catalog`, `schema`, `warehouse_id`, `genie_title`, `parent_path`, `smoke_test`.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "clinical_hotspotting", "Schema")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID")
dbutils.widgets.text("genie_title", "Clinical Hotspotting & Outcomes Intelligence", "Genie agent title")
dbutils.widgets.text("parent_path", "", "Workspace folder for the Genie agent (optional)")
dbutils.widgets.dropdown("smoke_test", "true", ["true", "false"], "Ask Genie a test question")

CATALOG      = dbutils.widgets.get("catalog")
SCHEMA       = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
TITLE        = dbutils.widgets.get("genie_title").strip()
PARENT_PATH  = dbutils.widgets.get("parent_path").strip()
SMOKE_TEST   = dbutils.widgets.get("smoke_test") == "true"
FQSN         = f"{CATALOG}.{SCHEMA}"

DESCRIPTION = (
    "AI-powered clinical hotspotting for a Medicare Advantage plan: identify high-risk members, "
    "understand why, and route them to the right care-management program. Synthetic, PHI-free data."
)

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# The Genie agent must run on a SERVERLESS SQL warehouse (project requirement).
if WAREHOUSE_ID:
    wh = w.warehouses.get(WAREHOUSE_ID)
    if not wh.enable_serverless_compute:
        raise ValueError(
            f"Warehouse '{wh.name}' ({WAREHOUSE_ID}) is not serverless. The Genie agent requires a serverless "
            "SQL warehouse — pass a serverless `warehouse_id`, or leave it empty to auto-pick one."
        )
else:
    serverless = sorted((x for x in w.warehouses.list() if x.enable_serverless_compute), key=lambda x: x.name or "")
    if not serverless:
        raise ValueError(
            "No serverless SQL warehouse found. Create one (SQL Warehouses → Create → Type: Serverless) "
            "or ask an admin to enable serverless SQL, then pass its ID as `warehouse_id`."
        )
    WAREHOUSE_ID = serverless[0].id
    print(f"Auto-picked serverless warehouse: {serverless[0].name}")

print(f"Schema       : {FQSN}")
print(f"Warehouse ID : {WAREHOUSE_ID}")
print(f"Genie title  : {TITLE}")
print(f"Running as   : {w.current_user.me().user_name}")

# COMMAND ----------

# DBTITLE 1,Data sources: tables + column configs
def col(name, description=None, synonyms=None, entity=False):
    c = {"column_name": name}
    if description:
        c["description"] = [description]
    if synonyms:
        c["synonyms"] = synonyms
    if entity:
        c["enable_entity_matching"] = True
        c["enable_format_assistance"] = True
    return c

TABLES = {
    "gold_member_hotspots": [
        col("market", "Geographic market (10 US states)", ["state", "region"], entity=True),
        col("zip_area", "ZIP-code area within the market", ["zip", "neighborhood"], entity=True),
        col("risk_band", "Very High (>=3.0) / High (>=2.0) / Rising (>=1.2) / Low risk_score band", ["risk tier", "risk level"], entity=True),
        col("sdoh_risk_level", "Social determinants of health risk: High / Moderate / Low", ["social risk", "SDoH risk"], entity=True),
        col("risk_score", "Clinical risk score, 0.3 to 4.5", ["risk", "clinical risk"]),
        col("hotspot_score", "Composite hotspot score: ED*1 + IP*3 + readmits*4 + open gaps*0.5 + nonadherent classes*1 + SDoH barriers*0.5", ["hotspot", "composite score"]),
        col("is_actionable_hotspot", "True when high utilization AND something the plan can fix (care gap, non-adherence, no PCP visit, or high SDoH)", ["actionable", "hotspot flag", "can we help"]),
        col("avg_medication_adherence_pdc", "Average proportion of days covered across drug classes; >= 0.80 is adherent", ["PDC", "adherence", "medication adherence"]),
        col("nonadherent_med_classes", "Number of drug classes with PDC < 0.80", ["non-adherent medications", "missed meds"]),
        col("sdoh_barrier_count", "Count of social barriers (housing, food, transportation, isolation), 0-4", ["SDoH barriers", "social barriers", "social needs"]),
        col("total_allowed_cost", "Total allowed cost across facility, medical and pharmacy claims (USD)", ["cost", "allowed cost", "total cost", "spend"]),
        col("ed_visits", "Emergency department visits", ["ER visits", "emergency visits"]),
        col("inpatient_admits", "Inpatient admissions", ["admissions", "hospital admits", "IP admits"]),
        col("readmissions_30d", "30-day readmissions", ["readmits", "readmissions"]),
        col("open_care_gaps", "Open HEDIS/Stars care gaps", ["care gaps", "open gaps"]),
        col("has_pcp_followup", "True if the member had at least one primary-care office visit", ["PCP visit", "primary care follow-up"]),
        col("ai_sdoh_narrative", "AI-generated (ai_summarize) plain-English summary of the member's social risk"),
    ],
    "gold_cohort_summary": [
        col("cohort", "Clinical cohort: CHF + Recent Admission, Diabetes Poor Adherence, ED Super-Utilizer, 30-Day Readmission, Rising Risk, High SDoH Risk, Complex / Polypharmacy", ["segment", "population"], entity=True),
        col("market", "Geographic market", ["state"], entity=True),
    ],
    "gold_geographic_hotspots": [
        col("market", "Geographic market", ["state"], entity=True),
        col("zip_area", "ZIP-code area", ["zip"], entity=True),
        col("pmpm_allowed_cost", "Allowed cost per member in the ZIP area", ["PMPM", "cost per member"]),
        col("avg_area_deprivation_index", "Average Area Deprivation Index (0-100, higher = more deprived)", ["ADI", "deprivation"]),
    ],
    "gold_intervention_outcomes": [
        col("cohort", "Care-management cohort", ["program cohort"], entity=True),
        col("intervention_type", "Type of outreach / program", ["program", "intervention"], entity=True),
        col("market", "Geographic market", ["state"], entity=True),
        col("engagement_rate", "PRE-COMPUTED share of interventions with status Engaged or Completed — select directly", ["engagement", "program engagement"]),
        col("improved_rate", "PRE-COMPUTED share of engaged interventions with outcome Improved — select directly", ["improvement rate", "success rate"]),
        col("ed_reduction_pct", "PRE-COMPUTED % reduction in ED visits, 90 days pre vs post — select directly", ["ED reduction", "ER reduction", "ED decrease"]),
    ],
    "gold_provider_outcomes": [
        col("specialty", "Provider specialty", ["provider type"], entity=True),
        col("provider_group", "Provider organization", ["health system", "medical group"], entity=True),
        col("peer_group", "Provider peer group used for benchmarking", None, entity=True),
        col("market", "Geographic market", ["state"], entity=True),
        col("readmission_oe_ratio", "Observed / expected 30-day readmission rate (risk-adjusted)", ["O/E ratio", "readmission ratio"]),
        col("is_high_readmission_outlier", "True when O/E > 1.20 and admissions >= 20", ["outlier", "high outlier", "readmission outlier"]),
    ],
    "gold_transplant_outcomes": [
        col("transplant_type", "Organ transplanted: Kidney / Liver / Heart", ["organ", "organ type"], entity=True),
        col("provider_group", "Transplant program (provider organization)", ["program", "transplant center"], entity=True),
        col("market", "Geographic market", ["state"], entity=True),
        col("transplant_volume", "Number of transplant encounters", ["volume", "transplants"]),
        col("is_outlier_program", "True when readmission O/E > 1.15 and volume >= 15", ["outlier program"]),
    ],
    "gold_clinical_narratives": [
        col("market", "Geographic market", ["state"], entity=True),
        col("risk_band", "Risk band", ["risk tier"], entity=True),
        col("ai_clinical_summary", "AI-generated (ai_summarize) care-team summary of the member's clinical situation", ["summary", "narrative", "clinical summary"]),
    ],
    "gold_intervention_recs": [
        col("ai_recommended_intervention", "AI-recommended (ai_classify) care-management program", ["recommended program", "recommendation", "next best action"], entity=True),
        col("market", "Geographic market", ["state"], entity=True),
        col("risk_band", "Risk band", ["risk tier"], entity=True),
        col("ai_recommendation_rationale", "AI-generated (ai_summarize) rationale for the recommendation", ["rationale", "why"]),
    ],
    "silver_care_gaps": [
        col("measure_name", "HEDIS/Stars measure name", ["gap type", "measure", "quality measure"], entity=True),
        col("measure_code", "HEDIS measure code (AWV, CDC-HBA1C, CBP, ...)", None, entity=True),
        col("gap_status", "Open or Closed", ["status"], entity=True),
        col("ai_priority", "AI-classified (ai_classify) urgency for open gaps: High / Medium / Low Priority; 'N/A - Closed' for closed gaps", ["priority", "urgency"], entity=True),
        col("market", "Geographic market", ["state"], entity=True),
    ],
}

data_sources = {"tables": sorted(
    [{"identifier": f"{FQSN}.{t}", "column_configs": sorted(cfgs, key=lambda c: c["column_name"])}
     for t, cfgs in TABLES.items()],
    key=lambda t: t["identifier"])}
print(f"Tables: {len(data_sources['tables'])}")

# COMMAND ----------

# DBTITLE 1,Text instruction
INSTRUCTION_TEXT = f"""
This Genie agent covers the Clinical Hotspotting & Outcomes Intelligence solution for a Medicare Advantage health plan.
All data is synthetic and PHI-free.

## Scope
Use only the attached tables in {FQSN}. Do not reference other schemas or catalogs.

## Glossary
- Actionable hotspot: a member with high utilization (>=2 ED visits, or >=1 inpatient admit, or >=1 readmission) AND
  something the plan can fix (open care gap, non-adherent medication, no PCP follow-up, or High SDoH risk). Column: is_actionable_hotspot.
- PDC (Proportion of Days Covered): medication adherence; >= 0.80 = adherent (Stars threshold).
- ED visits per 1,000 / Admits per 1,000: SUM(visits) * 1000 / member count.
- O/E ratio: observed / expected; > 1.20 = high readmission outlier for providers, > 1.15 = outlier transplant program.
- PMPM: per-member cost (total_allowed_cost / member count).
- SDoH: social determinants of health (housing, food, transportation, social isolation).
- Risk band values are exactly: 'Very High', 'High', 'Rising', 'Low'.

## Pre-computed metrics — SELECT directly, never recompute
In gold_intervention_outcomes, engagement_rate, improved_rate and ed_reduction_pct are already calculated.
SELECT them directly (AVG across groups if you must roll up). Do not divide engaged_count by intervention_count, etc.

## Rounding
Round scores and ratios to 2 decimal places, percentages to 1 decimal place, currency to whole dollars.

## Routing
- Member-level questions → gold_member_hotspots
- Cohort rollups → gold_cohort_summary; program effectiveness → gold_intervention_outcomes
- Geographic / ZIP questions → gold_geographic_hotspots
- Provider questions → gold_provider_outcomes
- Transplant questions → gold_transplant_outcomes (organ = transplant_type)
- AI summaries / narratives → gold_clinical_narratives (ai_clinical_summary)
- Recommended programs → gold_intervention_recs (ai_recommended_intervention)
- Care gap detail ("gap type" = measure_name) → silver_care_gaps
"""

# COMMAND ----------

# DBTITLE 1,Sample questions, example SQL, join specs, snippets, benchmarks
SAMPLE_QUESTIONS = [
    "How many actionable hotspot members are there in each market?",
    "What is the average medication adherence PDC by risk band?",
    "What AI intervention is recommended for Very High risk members with high SDoH burden?",
    "Which providers are high readmission outliers?",
    "How many open care gaps exist by measure?",
    "How many members are recommended for Complex Case Management?",
    "Show transplant volume and outcomes by organ type",
    "What are the top ZIP areas by actionable hotspot rate?",
    "Show program outcomes: engagement rate and ED reduction by cohort",
    "Which markets have the highest average risk score?",
]

EXAMPLE_SQLS = [
    ("How many actionable hotspot members are there in each market?", f"""
SELECT market,
  COUNT(*) AS total_members,
  SUM(CAST(is_actionable_hotspot AS INT)) AS actionable_members,
  ROUND(AVG(CAST(is_actionable_hotspot AS DOUBLE)) * 100, 1) AS actionable_pct
FROM {FQSN}.gold_member_hotspots
GROUP BY market
ORDER BY actionable_members DESC"""),
    ("What is the average medication adherence PDC by risk band?", f"""
SELECT risk_band,
  COUNT(*) AS members,
  ROUND(AVG(avg_medication_adherence_pdc), 3) AS avg_pdc,
  SUM(CAST(avg_medication_adherence_pdc < 0.80 AS INT)) AS non_adherent_members
FROM {FQSN}.gold_member_hotspots
GROUP BY risk_band
ORDER BY CASE risk_band WHEN 'Very High' THEN 1 WHEN 'High' THEN 2 WHEN 'Rising' THEN 3 ELSE 4 END"""),
    ("Which providers are high readmission outliers?", f"""
SELECT provider_name, specialty, provider_group, market, admissions,
  ROUND(readmission_oe_ratio, 2) AS oe_ratio,
  ROUND(avg_member_risk_score, 2) AS avg_risk
FROM {FQSN}.gold_provider_outcomes
WHERE is_high_readmission_outlier = true
ORDER BY readmission_oe_ratio DESC"""),
    ("What AI program is recommended for Very High risk members with SDoH burden?", f"""
SELECT ir.member_id, mh.market, mh.risk_band, mh.sdoh_barrier_count, mh.sdoh_risk_level,
  ir.ai_recommended_intervention,
  LEFT(ir.ai_recommendation_rationale, 250) AS rationale
FROM {FQSN}.gold_intervention_recs ir
JOIN {FQSN}.gold_member_hotspots mh ON ir.member_id = mh.member_id
WHERE mh.risk_band = 'Very High' AND mh.sdoh_barrier_count >= 2
ORDER BY mh.hotspot_score DESC
LIMIT 20"""),
    ("Show program engagement and ED reduction by cohort", f"""
SELECT cohort, intervention_type, market, intervention_count,
  ROUND(engagement_rate * 100, 1) AS engagement_pct,
  ROUND(improved_rate * 100, 1) AS improved_pct,
  ed_reduction_pct
FROM {FQSN}.gold_intervention_outcomes
WHERE intervention_count >= 5
ORDER BY ed_reduction_pct DESC NULLS LAST"""),
    ("How many members are recommended for each program?", f"""
SELECT ai_recommended_intervention,
  COUNT(*) AS member_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM {FQSN}.gold_intervention_recs
GROUP BY ai_recommended_intervention
ORDER BY member_count DESC"""),
    ("Show transplant volume and outcomes by organ type", f"""
SELECT transplant_type,
  SUM(transplant_volume) AS transplants,
  ROUND(SUM(observed_readmission_rate * transplant_volume) / SUM(transplant_volume), 3) AS readmission_rate,
  ROUND(SUM(complication_rate * transplant_volume) / SUM(transplant_volume), 3) AS complication_rate,
  ROUND(SUM(mortality_rate * transplant_volume) / SUM(transplant_volume), 3) AS mortality_rate,
  ROUND(SUM(avg_length_of_stay * transplant_volume) / SUM(transplant_volume), 1) AS avg_los_days
FROM {FQSN}.gold_transplant_outcomes
GROUP BY transplant_type
ORDER BY transplants DESC"""),
    ("How many open care gaps exist by measure, and how many are high priority?", f"""
SELECT measure_name,
  COUNT(*) AS open_gaps,
  SUM(CASE WHEN ai_priority = 'High Priority' THEN 1 ELSE 0 END) AS high_priority_gaps
FROM {FQSN}.silver_care_gaps
WHERE gap_status = 'Open'
GROUP BY measure_name
ORDER BY open_gaps DESC"""),
]

JOIN_SPECS = [
    ("gold_member_hotspots", "mh", "gold_intervention_recs", "ir", "ONE_TO_ONE"),
    ("gold_member_hotspots", "mh", "gold_clinical_narratives", "cn", "ONE_TO_ONE"),
    ("gold_member_hotspots", "mh", "silver_care_gaps", "cg", "ONE_TO_MANY"),
]

SNIPPETS = {
    "filters": [
        ("Actionable hotspot members", "gold_member_hotspots.is_actionable_hotspot = true", ["actionable", "hotspots"]),
        ("Very High risk members", "gold_member_hotspots.risk_band = 'Very High'", ["highest risk", "top risk"]),
        ("Open care gaps", "silver_care_gaps.gap_status = 'Open'", ["open gaps", "unresolved gaps"]),
        ("High readmission outlier providers", "gold_provider_outcomes.is_high_readmission_outlier = true", ["outlier providers"]),
    ],
    "measures": [
        ("ed_visits_per_1000", "ED visits per 1,000", "SUM(gold_member_hotspots.ed_visits) * 1000.0 / COUNT(*)", ["ED per thousand", "ER rate"]),
        ("actionable_hotspot_rate", "Actionable hotspot rate (%)", "ROUND(AVG(CAST(gold_member_hotspots.is_actionable_hotspot AS DOUBLE)) * 100, 1)", ["hotspot rate"]),
        ("non_adherent_members", "Non-adherent members (PDC < 0.80)", "SUM(CAST(gold_member_hotspots.avg_medication_adherence_pdc < 0.80 AS INT))", ["non-adherent", "poor adherence"]),
        ("pmpm_cost", "Cost per member", "ROUND(SUM(gold_member_hotspots.total_allowed_cost) / COUNT(*), 0)", ["PMPM", "cost per member"]),
    ],
    "expressions": [
        ("is_adherent", "Adherent (PDC >= 0.80)", "gold_member_hotspots.avg_medication_adherence_pdc >= 0.80"),
    ],
}

BENCHMARKS = [
    ("How many total members are there?", f"SELECT COUNT(*) AS total_members FROM {FQSN}.gold_member_hotspots"),
    ("How many actionable hotspot members are there?", f"SELECT COUNT(*) AS actionable_members FROM {FQSN}.gold_member_hotspots WHERE is_actionable_hotspot = true"),
    ("What is the most common AI-recommended program?", f"SELECT ai_recommended_intervention, COUNT(*) AS members FROM {FQSN}.gold_intervention_recs GROUP BY ai_recommended_intervention ORDER BY members DESC LIMIT 1"),
    ("How many providers are high readmission outliers?", f"SELECT COUNT(*) AS outlier_providers FROM {FQSN}.gold_provider_outcomes WHERE is_high_readmission_outlier = true"),
    ("What is the average PDC for Very High risk members?", f"SELECT ROUND(AVG(avg_medication_adherence_pdc), 3) AS avg_pdc FROM {FQSN}.gold_member_hotspots WHERE risk_band = 'Very High'"),
    ("Which market has the most actionable hotspot members?", f"SELECT market, COUNT(*) AS actionable_members FROM {FQSN}.gold_member_hotspots WHERE is_actionable_hotspot = true GROUP BY market ORDER BY actionable_members DESC LIMIT 1"),
    ("How many open care gaps are there?", f"SELECT COUNT(*) AS open_gaps FROM {FQSN}.silver_care_gaps WHERE gap_status = 'Open'"),
    ("What organ types are in the transplant outcomes?", f"SELECT DISTINCT transplant_type FROM {FQSN}.gold_transplant_outcomes ORDER BY transplant_type"),
    ("What is the average risk score by market?", f"SELECT market, ROUND(AVG(risk_score), 2) AS avg_risk_score FROM {FQSN}.gold_member_hotspots GROUP BY market ORDER BY avg_risk_score DESC"),
    ("Show program engagement rate by cohort", f"SELECT cohort, ROUND(AVG(engagement_rate) * 100, 1) AS engagement_pct FROM {FQSN}.gold_intervention_outcomes GROUP BY cohort ORDER BY engagement_pct DESC"),
    ("What is the ED reduction percentage by intervention type?", f"SELECT intervention_type, ROUND(AVG(ed_reduction_pct), 1) AS ed_reduction_pct FROM {FQSN}.gold_intervention_outcomes GROUP BY intervention_type ORDER BY ed_reduction_pct DESC"),
    ("What is the average hotspot score for Very High risk members?", f"SELECT ROUND(AVG(hotspot_score), 2) AS avg_hotspot_score FROM {FQSN}.gold_member_hotspots WHERE risk_band = 'Very High'"),
]

# COMMAND ----------

# DBTITLE 1,Build serialized_space (v2)
import json, uuid

def sid(*parts):
    """Deterministic 32-hex id (stable across re-runs so updates don't churn)."""
    return uuid.uuid5(uuid.NAMESPACE_URL, "clinical-hotspotting/" + "/".join(parts)).hex

def by_id(items):
    return sorted(items, key=lambda x: x["id"])

instructions = {
    "text_instructions": [{"id": sid("text"), "content": [INSTRUCTION_TEXT.strip()]}],
    "example_question_sqls": by_id([
        {"id": sid("example", q), "question": [q], "sql": [s.strip()]} for q, s in EXAMPLE_SQLS
    ]),
    "join_specs": by_id([
        {"id": sid("join", l, r),
         "left":  {"identifier": f"{FQSN}.{l}", "alias": la},
         "right": {"identifier": f"{FQSN}.{r}", "alias": ra},
         "sql": [f"`{la}`.`member_id` = `{ra}`.`member_id`", f"--rt=FROM_RELATIONSHIP_TYPE_{rel}--"]}
        for l, la, r, ra, rel in JOIN_SPECS
    ]),
    "sql_snippets": {
        "filters": by_id([
            {"id": sid("filter", n), "display_name": n, "sql": [s], "synonyms": syn}
            for n, s, syn in SNIPPETS["filters"]
        ]),
        "measures": by_id([
            {"id": sid("measure", a), "alias": a, "display_name": n, "sql": [s], "synonyms": syn}
            for a, n, s, syn in SNIPPETS["measures"]
        ]),
        "expressions": by_id([
            {"id": sid("expr", a), "alias": a, "display_name": n, "sql": [s]}
            for a, n, s in SNIPPETS["expressions"]
        ]),
    },
}

serialized_space = {
    "version": 2,
    "config": {"sample_questions": by_id([
        {"id": sid("sample", q), "question": [q]} for q in SAMPLE_QUESTIONS
    ])},
    "data_sources": data_sources,
    "instructions": instructions,
    "benchmarks": {"questions": by_id([
        {"id": sid("benchmark", q), "question": [q], "answer": [{"format": "SQL", "content": [s]}]}
        for q, s in BENCHMARKS
    ])},
}
payload_str = json.dumps(serialized_space)
print(f"serialized_space size: {len(payload_str):,} bytes")

# COMMAND ----------

# DBTITLE 1,Validate every SQL statement against the warehouse data
# Fail fast (before touching Genie) if any example / benchmark SQL is broken.
errors = []
for q, s in EXAMPLE_SQLS + BENCHMARKS:
    try:
        spark.sql(s).limit(1).collect()
    except Exception as e:
        errors.append(f"{q}: {str(e).splitlines()[0]}")
if errors:
    raise ValueError("Broken Genie SQL:\n" + "\n".join(errors))
print(f"✓ All {len(EXAMPLE_SQLS) + len(BENCHMARKS)} example/benchmark SQL statements run")

# COMMAND ----------

# DBTITLE 1,Create or update the Genie agent
def find_space_by_title(title):
    page_token = None
    while True:
        q = {"page_size": 100}
        if page_token:
            q["page_token"] = page_token
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query=q)
        for sp in resp.get("spaces", []):
            if sp.get("title") == title:
                return sp["space_id"]
        page_token = resp.get("next_page_token")
        if not page_token:
            return None

body = {
    "title": TITLE,
    "description": DESCRIPTION,
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": payload_str,
}

space_id = find_space_by_title(TITLE)
if space_id:
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
    action = "Updated"
else:
    if PARENT_PATH:
        body["parent_path"] = PARENT_PATH
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
    action = "Created"

host = w.config.host.rstrip("/")
space_url = f"{host}/genie/rooms/{space_id}"
dbutils.jobs.taskValues.set(key="genie_space_id", value=space_id)
print(f"✓ {action} Genie agent '{TITLE}'")
print(f"  Space ID : {space_id}")
print(f"  URL      : {space_url}")

# COMMAND ----------

# DBTITLE 1,Smoke test: ask Genie a question
import time

if SMOKE_TEST:
    question = "How many actionable hotspot members are there in each market?"
    msg = w.api_client.do("POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation",
                          body={"content": question})
    conv_id, msg_id = msg["conversation_id"], msg["message_id"]
    status = None
    for _ in range(60):  # up to ~5 minutes (warehouse may be cold)
        m = w.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}")
        status = m.get("status")
        if status in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            break
        time.sleep(5)
    print(f"Q: {question}\nStatus: {status}")
    if status != "COMPLETED":
        raise RuntimeError(f"Genie smoke test did not complete: {status} {m.get('error')}")
    for a in m.get("attachments", []):
        if a.get("text"):
            print("Answer:", a["text"].get("content"))
        if a.get("query"):
            print("SQL:\n", a["query"].get("query"))
    print("✓ Genie answered end to end")
else:
    print("Smoke test skipped")
