# Databricks notebook source
# DBTITLE 1,Title and asset links
# MAGIC %md
# MAGIC # Clinical Hotspotting & Outcomes Intelligence — Demo Talk Track
# MAGIC
# MAGIC Read-along script for a 30–40 minute demo, with discovery questions and pain-point quantification woven in.
# MAGIC Each step has **Open**, **Show**, **Say**, **Ask**, and where relevant **How much is it costing?**.
# MAGIC
# MAGIC ## Assets to open in new tabs before the call
# MAGIC
# MAGIC After `databricks bundle deploy`, the notebooks live under the bundle folder in your workspace (`.bundle/clinical-hotspotting/<target>/files/src`), the job and pipeline are under **Jobs & Pipelines**, and the Genie agent is under **Genie**.
# MAGIC
# MAGIC | Asset | Name in workspace |
# MAGIC | --- | --- |
# MAGIC | Project folder | `.bundle/clinical-hotspotting/<target>/files/src` |
# MAGIC | Config notebook | `00_config` |
# MAGIC | Data generation | `01_generate_synthetic_data` |
# MAGIC | Bronze layer | `02_sdp_bronze` |
# MAGIC | Silver + AI layer | `03_sdp_silver_ai` |
# MAGIC | Gold layer | `04_sdp_gold` |
# MAGIC | Metric view DDL | `05_create_metric_view` |
# MAGIC | Demo walkthrough | `06_demo` |
# MAGIC | Pipeline | `clinical-hotspotting-sdp-<schema>` |
# MAGIC | Orchestration job | **Clinical Hotspotting End-to-End Pipeline** |
# MAGIC | Schema (Catalog Explorer) | `{catalog}.{schema}` |
# MAGIC | Metric views | `mv_clinical_hotspotting` (per member), `mv_encounter_utilization` (per encounter) |
# MAGIC | Member hotspots table | `gold_member_hotspots` |
# MAGIC | AI narratives table | `gold_clinical_narratives` |
# MAGIC | AI recommendations table | `gold_intervention_recs` |
# MAGIC # MAGIC | Genie agent | **Clinical Hotspotting & Outcomes Intelligence** |

# COMMAND ----------

# DBTITLE 1,Parameters (used by the live SQL cells)
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "clinical_hotspotting", "Schema")

# COMMAND ----------

# DBTITLE 1,Acronyms and terms
# MAGIC %md
# MAGIC ## Acronyms and terms
# MAGIC
# MAGIC **Clinical and utilization**
# MAGIC
# MAGIC | Term | Meaning |
# MAGIC | --- | --- |
# MAGIC | ED / ER | Emergency department / emergency room visit |
# MAGIC | ED visits per 1,000 | ED visits per 1,000 members — standard utilization rate |
# MAGIC | Admits per 1,000 | Inpatient admissions per 1,000 members |
# MAGIC | Readmission (30-day) | Return inpatient admission within 30 days of discharge |
# MAGIC | LOS | Length of stay (days in hospital) |
# MAGIC | O/E ratio | Observed ÷ expected rate, adjusted for member risk. Above 1.20 = high outlier; below 0.80 = better than expected |
# MAGIC | CHF / COPD / CKD / BH | Congestive heart failure / chronic obstructive pulmonary disease / chronic kidney disease / behavioral health |
# MAGIC | PCP | Primary care provider |
# MAGIC | Hotspot | Member with high avoidable utilization and cost |
# MAGIC | Actionable hotspot | Hotspot who also has something the plan can fix (open gap, non-adherence, social barrier, no PCP follow-up) |
# MAGIC | Risk band | Very High / High / Rising / Low, based on risk score |
# MAGIC
# MAGIC **Medication and quality**
# MAGIC
# MAGIC | Term | Meaning |
# MAGIC | --- | --- |
# MAGIC | PDC | Proportion of days covered — share of days a member had their medication on hand. PDC ≥ 0.80 counts as adherent for Star Ratings |
# MAGIC | Part D | Medicare prescription drug benefit |
# MAGIC | RAS | Renin-angiotensin system antagonists (blood pressure drugs such as ACE inhibitors and ARBs) — a Part D adherence measure, alongside diabetes drugs and statins |
# MAGIC | HEDIS | Healthcare Effectiveness Data and Information Set — standard quality measures (care gaps) |
# MAGIC | Stars / Star Ratings | CMS 1–5 star quality rating for Medicare Advantage plans; drives quality bonus payments |
# MAGIC | Care gap | A recommended service a member hasn't received yet (e.g. annual wellness visit, A1c test) |
# MAGIC | AWV | Annual wellness visit |
# MAGIC | A1c | Blood test for average blood sugar (diabetes control) |
# MAGIC | BP | Blood pressure |
# MAGIC | TRC | Transitions of Care — HEDIS measure for follow-up after hospital discharge |
# MAGIC | FUH | Follow-up after hospitalization for mental illness |
# MAGIC
# MAGIC **Payer and business**
# MAGIC
# MAGIC | Term | Meaning |
# MAGIC | --- | --- |
# MAGIC | MA | Medicare Advantage |
# MAGIC | CMS | Centers for Medicare & Medicaid Services |
# MAGIC | PMPM | Per member per month cost (total cost ÷ member months) |
# MAGIC | MLR | Medical loss ratio — share of premium spent on medical care |
# MAGIC | Dual-eligible | Member eligible for both Medicare and Medicaid |
# MAGIC | HMO / PPO | Health maintenance organization / preferred provider organization plan types |
# MAGIC | Value-based care | Provider contracts that pay for outcomes and cost, not just volume |
# MAGIC | SDoH | Social determinants of health — housing, food, transportation, isolation |
# MAGIC | ADI | Area deprivation index — neighborhood disadvantage score (higher = more deprived) |
# MAGIC | CHW | Community health worker |
# MAGIC | ADT | Admit / discharge / transfer notifications from hospitals |
# MAGIC | PHI | Protected health information |
# MAGIC | CMO / CFO / VP | Chief medical officer / chief financial officer / vice president |
# MAGIC
# MAGIC **Databricks and data**
# MAGIC
# MAGIC | Term | Meaning |
# MAGIC | --- | --- |
# MAGIC | SDP | Lakeflow Spark Declarative Pipeline — declarative data pipeline |
# MAGIC | Bronze / Silver / Gold | Raw landing → cleaned and enriched → business-ready tables |
# MAGIC | UC | Unity Catalog — governance, access control and lineage |
# MAGIC | Metric view | Unity Catalog object that defines KPIs once; queried with `MEASURE()` |
# MAGIC | Genie agent | Natural-language Q&A over curated tables |
# MAGIC | Benchmark | Test question with a known correct answer, used to check Genie's accuracy |
# MAGIC | ETL | Extract, transform, load |
# MAGIC | KPI | Key performance indicator |
# MAGIC | AI functions | SQL functions such as `ai_summarize`, `ai_classify`, `ai_analyze_sentiment` |

# COMMAND ----------

# DBTITLE 1,Pre-call checklist
# MAGIC %md
# MAGIC ## Before the call
# MAGIC
# MAGIC - [ ] Run the **orchestration job** and confirm the latest run succeeded.
# MAGIC - [ ] Open all tabs from the asset table above.
# MAGIC - [ ] Pre-run `06_demo` so results are already visible.
# MAGIC - [ ] Open the **Genie agent** and start a fresh conversation.
# MAGIC
# MAGIC **Safe to ask Genie live:** market counts, PDC (proportion of days covered, i.e. medication adherence) by risk band, AI recommendations for Very High risk members, provider readmission outliers (risk-adjusted observed/expected ratio above 1.20), open care gaps (HEDIS quality measures not yet met), transplant volume.
# MAGIC
# MAGIC **Avoid live:**
# MAGIC - Intervention engagement and ED (emergency department) visit reduction questions (those benchmarks still fail).
# MAGIC - The 100% ED reductions — they come from groups of 1–2 members.
# MAGIC
# MAGIC **Say once, up front:** the data is synthetic — 5,000 members across 10 markets, no PHI (protected health information).
# MAGIC
# MAGIC **Key numbers to have ready:** 5,000 members, 3,180 actionable hotspots (63.6%), Tennessee leads with 344, 20 provider readmission outliers, 4,832 open care gaps, 2,575 members routed to Complex Case Management, 13 benchmarks (10 passing).

# COMMAND ----------

# DBTITLE 1,Step 1: Short opening and discovery
# MAGIC %md
# MAGIC ## Step 1 — Short opening and discovery (3 min)
# MAGIC
# MAGIC **Open:** Nothing yet. Keep the screen on a title slide or the **project folder**.
# MAGIC
# MAGIC **Say:** "Care management teams can't reach everyone. Hotspotting means finding the small group of members driving most of the avoidable utilization and cost, while there's still time to change the outcome. I'm going to start by showing you the end result, then walk backwards through how we built it. First, a couple of quick questions so I focus on what matters to you."
# MAGIC
# MAGIC > Ask only 2–3 current-state questions here. Save the pain and cost questions for after Genie (Step 2) — they land harder once the audience has seen the answer.
# MAGIC
# MAGIC **Ask — current state:**
# MAGIC - How do you decide today which members get outreach first?
# MAGIC - Is that list a vendor risk score, an internal model, a spreadsheet, or clinician judgment?
# MAGIC - How often is it refreshed: daily, weekly, monthly, quarterly?
# MAGIC - Who builds it, and how many people and systems are involved? Claims, pharmacy, care gaps, SDoH?
# MAGIC - When a care manager opens a member, how long does it take to understand why that member is high risk?
# MAGIC
# MAGIC **Ask — pain points:**
# MAGIC - What's the lag between a member's ER visit or admission and your team knowing about it?
# MAGIC - How often do two teams report different numbers for the same metric, like readmission rate or PMPM?
# MAGIC - How many ad-hoc data requests does analytics get from clinical ops each week? How long is the turnaround?
# MAGIC
# MAGIC **How much is it costing?**
# MAGIC - On a 1–5 scale, how much does list staleness limit your team's impact?
# MAGIC - How many care managers, and what share of their week goes to chart review versus talking to members?
# MAGIC - If you reached the right members 30 days sooner, what's that worth in avoided admissions or readmissions? *(Let them supply their own cost per admission.)*
# MAGIC - Are any of these metrics tied to Stars, value-based contracts, or shared savings?
# MAGIC
# MAGIC > Write down their numbers — you'll read them back at the close (Step 9).
# MAGIC
# MAGIC | Discovery note | Their answer |
# MAGIC | --- | --- |
# MAGIC | List refresh cadence | |
# MAGIC | Staleness impact (1–5) | |
# MAGIC | # care managers | |
# MAGIC | Chart prep minutes per member | |
# MAGIC | Analyst hours/month on ad-hoc requests | |
# MAGIC | Cost per avoidable admission | |
# MAGIC | Contracts / Stars exposure | |

# COMMAND ----------

# DBTITLE 1,Step 2: Start with the end — Genie agent
# MAGIC %md
# MAGIC ## Step 2 — Start with the end: the Genie agent (10 min, the centerpiece)
# MAGIC
# MAGIC **Open:** **Clinical Hotspotting & Outcomes Intelligence** (new chat)
# MAGIC
# MAGIC **Say (opening):** "Let me start with the end result. This is what a care management director, a market leader or a Stars lead would actually use. No dashboard to learn, no ticket to file — they just ask."
# MAGIC
# MAGIC **Show** (click a starter question first, then type the rest):
# MAGIC 1. "How many actionable hotspot members are there in each market?" → Tennessee leads with 344.
# MAGIC 2. "What is the average medication adherence PDC by risk band?"
# MAGIC 3. "What AI intervention is recommended for Very High risk members with high SDoH burden?" → AI recommendation and rationale per member.
# MAGIC 4. "Which providers are high readmission outliers?" → 20 providers with an observed/expected ratio above 1.20.
# MAGIC 5. "How many open care gaps exist?" → 4,832.
# MAGIC 6. Let the audience ask their own question.
# MAGIC
# MAGIC **Say:** "A care management director gets the answer in seconds. Genie knows our definitions — 'PDC' and 'adherence' mean the same thing, 'high need' means actionable hotspot. Every answer shows its SQL, so it can be audited. And it's tested: we keep 13 questions with known correct answers and run them as an accuracy check before rollout."
# MAGIC
# MAGIC **Point out:** click **Show code** on one answer; open the agent's Instructions and Benchmarks.
# MAGIC
# MAGIC **Ask (now that they've seen it):**
# MAGIC - How would you get these answers today, and how long would it take?
# MAGIC - Who asks your analysts questions like these, and how long do they wait?
# MAGIC - Which questions do clinical leaders stop asking because the answer takes too long?
# MAGIC - What would need to be true for you to trust an AI-generated answer? *(Point to the visible SQL and benchmark testing.)*
# MAGIC
# MAGIC **How much is it costing?**
# MAGIC - How many analyst hours a month go to repeat ad-hoc questions?
# MAGIC - If directors could answer these themselves, what would analysts work on instead?
# MAGIC
# MAGIC > If an answer looks off: "This is why we test against known answers. We tune the instructions and re-run the tests before anyone relies on it."
# MAGIC
# MAGIC **Transition to the build:**
# MAGIC > "Everything you just saw — the answers, the AI recommendations, the numbers — came from data the plan already has. Let me walk backwards and show you how we built it, starting with what Genie reads and ending at the raw feeds."

# COMMAND ----------

# DBTITLE 1,How we built it — working backwards
# MAGIC %md
# MAGIC # How we built it — working backwards from the answer
# MAGIC
# MAGIC Put the end-to-end picture from `06_demo` (first chart) on screen and walk it **right to left** while you talk.
# MAGIC
# MAGIC | Order | You just saw | It comes from | Step |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | Genie's answers | One set of KPI definitions | 3 — Metric view |
# MAGIC | 2 | Consistent KPIs | A scored, actionable member list | 4 — Hotspot scoring |
# MAGIC | 3 | The member list and AI recommendations | Gold ← Silver (AI) ← Bronze | 5 — Pipeline, walked backwards |
# MAGIC | 4 | Bronze | Eight payer feeds | 6 — Source data |
# MAGIC | 5 | All of it | One governed project and schema | 7 — Governance |
# MAGIC | 6 | Fresh every day | One orchestration job | 8 — Automation |
# MAGIC
# MAGIC > Read the room: with executives, compress Steps 3–8 to about 5 minutes and spend the time on discovery. With data or IT teams, expand them.

# COMMAND ----------

# DBTITLE 1,Step 3: Metric view
# MAGIC %md
# MAGIC ## Step 3 — Why every number matches: the metric view (3 min)
# MAGIC
# MAGIC **Open:** `mv_clinical_hotspotting` and `mv_encounter_utilization` in Catalog Explorer, and `05_create_metric_view`
# MAGIC
# MAGIC **Show:** Dimensions, measures, comments and formats. Point out the two grains: member KPIs (rates, per-1,000, cost per member) live in the member view, utilization by diagnosis, provider and month in the encounter view, so no number is double-counted. The last cell of `05_create_metric_view` reconciles both views with the source tables on every run.
# MAGIC
# MAGIC **Say:** "One step back from Genie. ED visits per 1,000, readmission rate, PMPM, adherence and actionable rate are each defined once, here. Genie, dashboards and notebooks all use the same definitions, so the CFO, the CMO and the Stars team see the same number."
# MAGIC
# MAGIC **Ask:**
# MAGIC - How many versions of "readmission rate" exist in your organization today?
# MAGIC - How much time goes into reconciling numbers before a leadership or board meeting?
# MAGIC
# MAGIC **How much is it costing?** "When the numbers don't match, what happens — decisions delayed, meetings spent arguing about the data, trust lost?"

# COMMAND ----------

# DBTITLE 1,Step 4: Hotspot scoring
# MAGIC %md
# MAGIC ## Step 4 — Where the actionable list comes from: hotspot scoring (3 min)
# MAGIC
# MAGIC **Open:** `gold_member_hotspots`, logic in `04_sdp_gold`, and the risk band and geographic cells in `06_demo`
# MAGIC
# MAGIC **Say:** "Behind those definitions is a scored list of every member. Each one gets a hotspot score from ED visits, admissions, 30-day readmissions, open care gaps, non-adherent medications and social barriers. Then we flag who is actionable: high utilization *and* something we can actually fix. That's 3,180 of 5,000 members. The scoring is plain SQL, so your medical directors can review and change the weights."
# MAGIC
# MAGIC **Ask:**
# MAGIC - Do you trust your current risk scores? Can a care manager explain why a member is on the list?
# MAGIC - Does the list separate members who are high risk from those who are high risk *and* reachable?
# MAGIC - Do you look at hotspots by geography — for example, to plan where community health workers go?

# COMMAND ----------

# DBTITLE 1,Step 5: The pipeline, walked backwards
# MAGIC %md
# MAGIC ## Step 5 — How the data gets made: the pipeline, walked backwards (8 min)
# MAGIC
# MAGIC **Open:** **clinical-hotspotting-sdp-<schema> pipeline graph**. Walk the graph **right to left**: Gold ← Silver ← Bronze. Code is in `02_sdp_bronze`, `03_sdp_silver_ai` and `04_sdp_gold`, but you don't need to open it.
# MAGIC
# MAGIC **Say (framing):** "One more level down — how that member list gets made. Think of this as an assembly line for member intelligence, and we're reading it from the finished product backwards. On the right is Gold: the answers. In the middle is Silver, where AI reads every record. On the left is Bronze, where raw files come in and get checked. It refreshes automatically every time new data arrives."
# MAGIC
# MAGIC ### 5a — Gold: ready for decisions (AI writes the member story here)
# MAGIC
# MAGIC **Business purpose:** answer specific questions for specific teams. This is what Genie reads. The AI output from Silver rolls up into member-level summaries and a recommended program.
# MAGIC
# MAGIC | Gold table | Who uses it | Business question it answers |
# MAGIC | --- | --- | --- |
# MAGIC | `gold_member_hotspots` | Care management | Who is high need and fixable right now? (3,180 of 5,000 actionable) |
# MAGIC | `gold_clinical_narratives` *(AI)* | Care managers | Why is this member on my list? One-paragraph clinical summary |
# MAGIC | `gold_intervention_recs` *(AI)* | Care management leads | Which program should this member go to, and why? |
# MAGIC | `gold_cohort_summary` | Clinical program owners | How are CHF, COPD, diabetes and BH cohorts doing by market? |
# MAGIC | `gold_geographic_hotspots` | Market leaders | Which ZIP areas need CHWs or more clinic capacity? |
# MAGIC | `gold_intervention_outcomes` | Care management leadership | Did ED visits fall in the 90 days after outreach? |
# MAGIC | `gold_provider_outcomes` | Network / value-based care | Which providers have higher readmissions than their patient mix predicts? |
# MAGIC | `gold_transplant_outcomes` | Specialty programs | How do transplant programs compare on readmissions, complications and cost? |
# MAGIC
# MAGIC **Say:** "Gold is the answer layer — it's what Genie was reading a few minutes ago. Each table exists because a specific team asks a specific question. The two AI tables turn everything upstream into what a care manager actually needs: a short story of the member and a recommended next step."
# MAGIC
# MAGIC ### 5b — Silver: clean, standardized and understood (this is where most of the AI runs)
# MAGIC
# MAGIC **Business purpose:** make each feed consistent and add the context a care manager would otherwise have to work out by hand. Five Silver tables are enriched with AI as the data flows through — no separate project, no data leaving the platform.
# MAGIC
# MAGIC | Silver table | AI added in the pipeline | What the business gets |
# MAGIC | --- | --- | --- |
# MAGIC | `silver_care_gaps` | `ai_classify` → High / Medium / Low priority | Stars and quality teams work the open gaps that matter most first |
# MAGIC | `silver_pharmacy_claims` | `ai_classify` → Adherent / At Risk / Non-Adherent | Pharmacy outreach can reach members before PDC drops below 0.80 on Part D adherence measures |
# MAGIC | `silver_care_interventions` | `ai_analyze_sentiment` → positive / neutral / negative | Shows whether outreach actually landed with the member, by program and care team |
# MAGIC | `silver_encounters` | `ai_summarize` → short visit summary | A care manager can read what happened at each ED visit or admission without decoding claim codes |
# MAGIC | `silver_sdoh` | `ai_summarize` → plain-English social needs | CHWs and social services see the barriers (housing, food, transport, isolation) at a glance |
# MAGIC | `silver_members`, `silver_providers`, `silver_medical_claims` | none — cleaned and standardized | One consistent version of members, providers and cost |
# MAGIC
# MAGIC **Say:** "Silver is where the data gets understood. As each record flows through, AI reads it and adds what a person would conclude: is this gap urgent, is this member falling off their statin, did that outreach call go well, what's going on in this member's life. It's applied to every record, the same way, every refresh."
# MAGIC
# MAGIC ### 5c — Bronze: trusted intake
# MAGIC
# MAGIC **Business purpose:** take every feed in exactly as it arrived and check it before anyone relies on it. A bad eligibility or claims file shouldn't send care managers to the wrong members or put Stars reporting at risk.
# MAGIC
# MAGIC | Bronze table | What it is to the business |
# MAGIC | --- | --- |
# MAGIC | `bz_members` | Enrollment roster: who we cover, plan type, dual-eligible status, chronic conditions |
# MAGIC | `bz_providers` | Provider network roster: specialty, group, peer group |
# MAGIC | `bz_encounters` | ED visits, admissions and observation stays (from claims or ADT feeds) |
# MAGIC | `bz_medical_claims` | What we paid for professional and facility care |
# MAGIC | `bz_pharmacy_claims` | Part D prescription fills |
# MAGIC | `bz_care_gaps` | HEDIS / Stars gaps by member |
# MAGIC | `bz_care_interventions` | Care management outreach history and outcomes |
# MAGIC | `bz_sdoh` | Social needs screenings and neighborhood deprivation |
# MAGIC
# MAGIC **Say:** "Bronze is the front door. Every file is kept as received, and basic quality rules — valid member IDs, sensible dates and amounts — are checked and reported on every run. Problems show up here, not in a care manager's call list or a Stars submission."
# MAGIC
# MAGIC **Ask:**
# MAGIC - How is your current ETL built and maintained? How many engineers keep it running?
# MAGIC - When a feed arrives late or malformed, how do you find out — before or after a report is wrong?
# MAGIC - How long does it take to add a new data source or a new metric today: days, weeks, a quarter?
# MAGIC
# MAGIC **How much is it costing?** "In the last quarter, how many care management decisions were made on data that later turned out to be wrong or stale?"

# COMMAND ----------

# DBTITLE 1,Step 5d: Show the AI in the pipeline
# MAGIC %md
# MAGIC ### 5d — Show the AI working inside the pipeline (part of Step 5)
# MAGIC
# MAGIC **Open:** `03_sdp_silver_ai`, then sample rows in `gold_clinical_narratives` and `gold_intervention_recs`
# MAGIC
# MAGIC **Show:** `ai_summarize`, `ai_classify`, `ai_analyze_sentiment` running as SQL inside the pipeline.
# MAGIC
# MAGIC **Say:** "There's no separate ML project here. These AI functions are plain SQL running inside the pipeline. Every one of the 3,180 actionable members gets a plain-English clinical summary and a recommended intervention with a rationale. 2,575 are routed to Complex Case Management, 300 to Medication Reconciliation, 244 to Transitions of Care. Every summary is regenerated on each refresh."
# MAGIC
# MAGIC **Ask:**
# MAGIC - How long does a care manager spend reviewing a chart before the first call?
# MAGIC - How is the intervention type chosen today? Is it consistent across care managers and markets?
# MAGIC - Have you tried GenAI for clinical summarization? What got in the way — governance, PHI, cost, integration?
# MAGIC
# MAGIC **How much is it costing?**
# MAGIC - If chart prep fell from roughly X minutes to about 1 minute per member, how many more members could each care manager reach per week? *(Let them name X.)*
# MAGIC - What does inconsistent program assignment cost in engagement and outcomes?
# MAGIC
# MAGIC > Positioning: these recommendations support decisions. The clinician still decides.

# COMMAND ----------

# DBTITLE 1,Step 4 AI example
# MAGIC %md
# MAGIC #### Example — what one AI step in the pipeline looks like
# MAGIC
# MAGIC Each AI function is one SQL expression inside a pipeline table definition in `03_sdp_silver_ai`. The pattern is always the same: **build a short text description of the row with `CONCAT`, then pass it to an AI function**. The result is stored as a new column and refreshed on every pipeline run.
# MAGIC
# MAGIC | Silver table | AI function | New column | What it returns |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `silver_care_gaps` | `ai_classify` | `ai_priority` | High / Medium / Low Priority for each open HEDIS gap |
# MAGIC | `silver_pharmacy_claims` | `ai_classify` | `ai_adherence_risk` | Adherent / At Risk / Non-Adherent per fill |
# MAGIC | `silver_care_interventions` | `ai_analyze_sentiment` | `ai_engagement_sentiment` | positive / neutral / negative engagement |
# MAGIC | `silver_encounters` | `ai_summarize` | `ai_risk_narrative` | Short risk summary of each ED / inpatient visit |
# MAGIC | `silver_sdoh` | `ai_summarize` | `ai_sdoh_narrative` | Plain-English summary of a member's social barriers |
# MAGIC
# MAGIC The Gold layer then uses the same approach to write the member-level clinical summary (`gold_clinical_narratives.ai_clinical_summary`) and the recommended program with rationale (`gold_intervention_recs.ai_recommended_intervention`, `ai_recommendation_rationale`).
# MAGIC
# MAGIC **Show this code** — the care gap prioritization, taken from the pipeline:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="silver_care_gaps")
# MAGIC def silver_care_gaps():
# MAGIC     return dlt.read("bz_care_gaps").withColumn(
# MAGIC         "ai_priority",
# MAGIC         expr("""
# MAGIC             CASE WHEN gap_status = 'Open'
# MAGIC             THEN ai_classify(
# MAGIC                 CONCAT('Care gap: ', measure_name, ' (', measure_code, '). ',
# MAGIC                        'Status: ', gap_status, '. ',
# MAGIC                        'Identified: ', CAST(identified_date AS STRING), '. ',
# MAGIC                        'Due: ', CAST(due_date AS STRING), '.'),
# MAGIC                 ARRAY('High Priority', 'Medium Priority', 'Low Priority'))
# MAGIC             ELSE 'N/A - Closed' END
# MAGIC         """))
# MAGIC ```
# MAGIC
# MAGIC **Say while showing it:**
# MAGIC > "This is the whole AI step. We describe each open care gap in a sentence — the measure, when it was found, when it's due — and ask the model to pick one of three labels. No model training, no separate service, no data leaving the platform. It's a column in a governed table, so a Stars lead can filter to High Priority gaps like any other field."
# MAGIC
# MAGIC **Then show the result:** open `silver_care_gaps` → **Sample Data** and point at the `ai_priority` column, or run the live cell below.

# COMMAND ----------

# DBTITLE 1,Live: try the AI functions
# MAGIC %sql
# MAGIC -- LIVE DEMO: the same three AI functions on one example member, run on the spot.
# MAGIC -- Run on serverless or a SQL warehouse. Each call returns in a few seconds.
# MAGIC SELECT
# MAGIC   ai_classify(
# MAGIC     'Care gap: Statin Therapy for Patients with Cardiovascular Disease (SPC). Status: Open. Identified: 2026-03-02. Due: 2026-10-31.',
# MAGIC     ARRAY('High Priority', 'Medium Priority', 'Low Priority')
# MAGIC   ) AS care_gap_priority,
# MAGIC
# MAGIC   ai_classify(
# MAGIC     'Drug: Atorvastatin (Statin). Days supply: 30. Last fill 75 days ago. No refill on file.',
# MAGIC     ARRAY('Adherent', 'At Risk', 'Non-Adherent')
# MAGIC   ) AS adherence_risk,
# MAGIC
# MAGIC   ai_analyze_sentiment(
# MAGIC     'Intervention: Transitions of Care call after CHF discharge. Member engaged, booked PCP follow-up within 7 days and confirmed medications.'
# MAGIC   ) AS engagement_sentiment,
# MAGIC
# MAGIC   ai_summarize(
# MAGIC     'Member in Tennessee, area deprivation index 88. Lives alone, no reliable transportation, reports food insecurity. 3 ED visits and 1 inpatient admission for CHF in the last 6 months. 2 open care gaps. Statin PDC 0.62.',
# MAGIC     40
# MAGIC   ) AS sdoh_and_risk_summary

# COMMAND ----------

# DBTITLE 1,Live: AI output stored in the tables
# MAGIC %sql
# MAGIC -- LIVE DEMO: AI output already produced by the pipeline for a real (synthetic) member.
# MAGIC SELECT
# MAGIC   ir.member_id,
# MAGIC   mh.market,
# MAGIC   mh.risk_band,
# MAGIC   mh.ed_visits,
# MAGIC   mh.sdoh_barrier_count,
# MAGIC   cn.ai_clinical_summary,
# MAGIC   ir.ai_recommended_intervention,
# MAGIC   ir.ai_recommendation_rationale
# MAGIC FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_intervention_recs') ir
# MAGIC JOIN IDENTIFIER(:catalog || '.' || :schema || '.gold_clinical_narratives') cn ON ir.member_id = cn.member_id
# MAGIC JOIN IDENTIFIER(:catalog || '.' || :schema || '.gold_member_hotspots') mh ON ir.member_id = mh.member_id
# MAGIC WHERE mh.risk_band = 'Very High'
# MAGIC ORDER BY mh.hotspot_score DESC
# MAGIC LIMIT 3

# COMMAND ----------

# DBTITLE 1,Step 6: Source data
# MAGIC %md
# MAGIC ## Step 6 — Where it all starts: the source feeds (3 min)
# MAGIC
# MAGIC **Open:** `01_generate_synthetic_data`, then the row-count cell in `06_demo`
# MAGIC
# MAGIC **Show:** 8 source tables — members, providers, encounters, medical and pharmacy claims, care gaps, interventions, SDoH.
# MAGIC
# MAGIC **Say:** "And this is where it all starts — the feeds every plan already has: enrollment, claims, Part D, HEDIS gaps, care management activity and social needs screenings. This is synthetic data, but the shape is real. In production these land through Lakeflow Connect or Auto Loader instead of this notebook."
# MAGIC
# MAGIC **Ask:**
# MAGIC - Which of these sources do you combine today, and which stay in separate systems?
# MAGIC - Is SDoH data used in risk targeting, or does it sit in assessments nobody looks at?
# MAGIC - Do you get ADT feeds? How quickly do they reach care management?
# MAGIC
# MAGIC **How much is it costing?** "If SDoH and pharmacy adherence aren't in the risk view, how many high-need members do you think are being missed?"

# COMMAND ----------

# DBTITLE 1,Step 7: One governed project
# MAGIC %md
# MAGIC ## Step 7 — All in one governed place (2 min)
# MAGIC
# MAGIC **Open:** **Project folder** and the `{catalog}.{schema}` schema
# MAGIC
# MAGIC **Show:** Notebooks `00`–`06`, the job, and the single Unity Catalog schema. Click any gold table → **Lineage** to show it traces back to the source tables.
# MAGIC
# MAGIC **Say:** "Stepping all the way back: everything you've seen — the feeds, the pipeline, the AI, the metrics and the Genie agent — sits in one project and one Unity Catalog schema, with the same access control, lineage and audit throughout. Nothing is exported to another tool, which is what makes compliance comfortable putting AI on member data."
# MAGIC
# MAGIC **Ask:**
# MAGIC - How many platforms does member data pass through before it reaches a care manager?
# MAGIC - Can you trace a number on a care management report back to the claim it came from? How long does that take?

# COMMAND ----------

# DBTITLE 1,Step 8: Automation
# MAGIC %md
# MAGIC ## Step 8 — Automation (2 min)
# MAGIC
# MAGIC **Open:** **Clinical Hotspotting End-to-End Pipeline job**
# MAGIC
# MAGIC **Show:** Three tasks — `generate_data` → `run_sdp_pipeline` → `run_demo` — and the run history.
# MAGIC
# MAGIC **Say:** "The whole flow is one job. It can run on a schedule or start automatically when new claims or ADT data arrives. The metric view and Genie always reflect the latest Gold data, with no manual refresh."
# MAGIC
# MAGIC **Ask:** "If your hotspot list refreshed daily instead of monthly, what would change in how your team works?"

# COMMAND ----------

# DBTITLE 1,Step 9: Close
# MAGIC %md
# MAGIC ## Step 9 — Close: connect the value to their numbers (3–5 min)
# MAGIC
# MAGIC **Read back what they told you** (from the Step 1 notes table):
# MAGIC - "You said the list refreshes every ___ and the delay matters ___ out of 5."
# MAGIC - "Chart prep takes ___ minutes and you have ___ care managers. That's ___ hours a week that could go to member outreach."
# MAGIC - "Analysts spend ___ hours a month on ad-hoc questions."
# MAGIC - "Metric disagreements cost you ___ in meetings and trust."
# MAGIC
# MAGIC **Quick value math** (fill in live):
# MAGIC
# MAGIC | Lever | Formula | Result |
# MAGIC | --- | --- | --- |
# MAGIC | Care manager time recovered | (# care managers) x (members/week) x (minutes saved per chart) / 60 | ___ hrs/week |
# MAGIC | Analyst time recovered | ad-hoc hours/month x % self-served by Genie | ___ hrs/month |
# MAGIC | Earlier identification | members reached sooner x their cost per avoidable admission x expected reduction | $___ |
# MAGIC
# MAGIC **Ask:**
# MAGIC - "Of these, which one would your CMO or VP of Care Management care about most?"
# MAGIC - "If we ran a 4–6 week pilot on one market with your real data, what result would make it a clear win?" *(Get a measurable criterion: time-to-identify, chart prep time, analyst hours saved.)*
# MAGIC - "Who else needs to see this — clinical, IT security, compliance? What's your process for approving AI on PHI?"
# MAGIC
# MAGIC **Next steps to propose:**
# MAGIC 1. Data source mapping session
# MAGIC 2. One-market pilot plan with agreed success metrics
# MAGIC 3. Governance review: Unity Catalog access controls and PHI handling