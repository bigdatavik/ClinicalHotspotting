# Databricks notebook source
# DBTITLE 1,03 — SDP Silver Layer + AI Enrichment
# MAGIC %md
# MAGIC # 03 — SDP Silver Layer + AI Enrichment
# MAGIC
# MAGIC 8 Silver materialized views cleansed from Bronze. **5 tables enriched with Databricks SQL AI functions**:
# MAGIC
# MAGIC | Silver Table | AI Function | New Column | Description |
# MAGIC |---|---|---|---|
# MAGIC | `silver_encounters` | `ai_summarize` | `ai_risk_narrative` | Encounter risk summary |
# MAGIC | `silver_pharmacy_claims` | `ai_classify` | `ai_adherence_risk` | Adherent / At Risk / Non-Adherent |
# MAGIC | `silver_care_gaps` | `ai_classify` | `ai_priority` | High / Medium / Low priority |
# MAGIC | `silver_care_interventions` | `ai_analyze_sentiment` | `ai_engagement_sentiment` | positive / neutral / negative |
# MAGIC | `silver_sdoh` | `ai_summarize` | `ai_sdoh_narrative` | Human-readable SDoH risk summary |
# MAGIC | `silver_members` | — | — | Pass-through cleansed |
# MAGIC | `silver_providers` | — | — | Pass-through cleansed |
# MAGIC | `silver_medical_claims` | — | — | Pass-through cleansed |

# COMMAND ----------

# DBTITLE 1,Silver: imports
import dlt
from pyspark.sql.functions import current_timestamp, expr, coalesce, lit, col, round as spark_round

# COMMAND ----------

# DBTITLE 1,silver_members (pass-through)
@dlt.table(
    name="silver_members",
    comment="Silver members — cleansed, typed",
    table_properties={"quality": "silver", "layer": "silver"}
)
@dlt.expect("valid_member_id", "member_id IS NOT NULL")
def silver_members():
    return (
        dlt.read("bz_members")
        .drop("_source_table")
        .withColumnRenamed("_ingest_ts", "_silver_ts")
    )

# COMMAND ----------

# DBTITLE 1,silver_providers (pass-through)
@dlt.table(
    name="silver_providers",
    comment="Silver providers — cleansed, typed",
    table_properties={"quality": "silver", "layer": "silver"}
)
@dlt.expect("valid_provider_id", "provider_id IS NOT NULL")
def silver_providers():
    return (
        dlt.read("bz_providers")
        .drop("_source_table")
        .withColumnRenamed("_ingest_ts", "_silver_ts")
    )

# COMMAND ----------

# DBTITLE 1,silver_medical_claims (pass-through)
@dlt.table(
    name="silver_medical_claims",
    comment="Silver medical claims — cleansed, typed",
    table_properties={"quality": "silver", "layer": "silver"}
)
@dlt.expect("non_negative_cost", "allowed_amount >= 0")
def silver_medical_claims():
    return (
        dlt.read("bz_medical_claims")
        .drop("_source_table")
        .withColumnRenamed("_ingest_ts", "_silver_ts")
    )

# COMMAND ----------

# DBTITLE 1,silver_encounters + ai_summarize
@dlt.table(
    name="silver_encounters",
    comment="Silver encounters — AI risk narrative per encounter (ai_summarize)",
    table_properties={"quality": "silver", "layer": "silver", "ai_enriched": "true"}
)
@dlt.expect("valid_encounter_type", "encounter_type IN ('ED','Inpatient','Observation')")
def silver_encounters():
    return dlt.read("bz_encounters").withColumn(
        "ai_risk_narrative",
        expr("""
            ai_summarize(
                CONCAT(
                    'Encounter ', encounter_type, ' for ',
                    COALESCE(primary_dx_category, 'unknown'), '. ',
                    'Admission: ', COALESCE(admission_type, 'unknown'), '. ',
                    'LOS: ', CAST(los_days AS STRING), ' days. ',
                    'Discharge: ', COALESCE(discharge_disposition, 'unknown'),
                    CASE WHEN readmission_30d  THEN '. 30-day readmission.' ELSE '' END,
                    CASE WHEN complication_flag THEN '. Complication documented.' ELSE '' END,
                    CASE WHEN is_transplant
                         THEN CONCAT('. Transplant: ', COALESCE(transplant_type, ''), '.')
                         ELSE '' END,
                    '. Allowed cost: $', CAST(ROUND(allowed_amount, 0) AS STRING), '.'
                )
            )
        """)
    ).withColumn("_silver_ts", current_timestamp()).drop("_source_table")

# COMMAND ----------

# DBTITLE 1,silver_pharmacy_claims + ai_classify
@dlt.table(
    name="silver_pharmacy_claims",
    comment="Silver pharmacy claims — AI adherence risk classification (ai_classify)",
    table_properties={"quality": "silver", "layer": "silver", "ai_enriched": "true"}
)
@dlt.expect("valid_days_supply", "days_supply > 0")
def silver_pharmacy_claims():
    return dlt.read("bz_pharmacy_claims").withColumn(
        "ai_adherence_risk",
        expr("""
            ai_classify(
                CONCAT(
                    'Drug: ', drug_name, ' (', therapeutic_class, '). ',
                    'Days supply: ', CAST(days_supply AS STRING), '. ',
                    'Fill date: ', CAST(fill_date AS STRING), '.'
                ),
                ARRAY('Adherent', 'At Risk', 'Non-Adherent')
            )
        """)
    ).withColumn("_silver_ts", current_timestamp()).drop("_source_table")

# COMMAND ----------

# DBTITLE 1,silver_care_gaps + ai_classify (AI Demo 3)
@dlt.table(
    name="silver_care_gaps",
    comment="Silver care gaps — AI priority classification for open gaps (ai_classify)",
    table_properties={"quality": "silver", "layer": "silver", "ai_enriched": "true"}
)
def silver_care_gaps():
    return dlt.read("bz_care_gaps").withColumn(
        "ai_priority",
        expr("""
            CASE WHEN gap_status = 'Open'
            THEN ai_classify(
                CONCAT(
                    'Care gap: ', measure_name, ' (', measure_code, '). ',
                    'Status: ', gap_status, '. ',
                    'Identified: ', CAST(identified_date AS STRING), '. ',
                    'Due: ', CAST(due_date AS STRING), '.'
                ),
                ARRAY('High Priority', 'Medium Priority', 'Low Priority')
            )
            ELSE 'N/A - Closed' END
        """)
    ).withColumn("_silver_ts", current_timestamp()).drop("_source_table")

# COMMAND ----------

# DBTITLE 1,silver_care_interventions + ai_analyze_sentiment (AI Demo 4)
@dlt.table(
    name="silver_care_interventions",
    comment="Silver care interventions — AI engagement sentiment (ai_analyze_sentiment)",
    table_properties={"quality": "silver", "layer": "silver", "ai_enriched": "true"}
)
def silver_care_interventions():
    return dlt.read("bz_care_interventions").withColumn(
        "ai_engagement_sentiment",
        expr("""
            ai_analyze_sentiment(
                CONCAT(
                    'Intervention: ', intervention_type, ' for ', cohort, ' cohort. ',
                    'Status: ', status, '. ',
                    'Outcome: ', COALESCE(outcome, 'pending'), '. ',
                    'Care team: ', assigned_care_team, '. ',
                    'ED visits pre-90d: ', CAST(ed_visits_pre_90d AS STRING),
                    ', post-90d: ', CAST(ed_visits_post_90d AS STRING), '.'
                )
            )
        """)
    ).withColumn("_silver_ts", current_timestamp()).drop("_source_table")

# COMMAND ----------

# DBTITLE 1,silver_sdoh + ai_summarize
@dlt.table(
    name="silver_sdoh",
    comment="Silver SDoH — AI social risk narrative per member (ai_summarize)",
    table_properties={"quality": "silver", "layer": "silver", "ai_enriched": "true"}
)
@dlt.expect("valid_member_id", "member_id IS NOT NULL")
def silver_sdoh():
    return dlt.read("bz_sdoh").withColumn(
        "ai_sdoh_narrative",
        expr("""
            ai_summarize(
                CONCAT(
                    'Member ', member_id, ' in ', market, ' (', zip_area, '). ',
                    'Area Deprivation Index: ', CAST(area_deprivation_index AS STRING), '. ',
                    CASE WHEN housing_insecurity       THEN 'Housing insecurity. '       ELSE '' END,
                    CASE WHEN food_insecurity           THEN 'Food insecurity. '           ELSE '' END,
                    CASE WHEN transportation_barrier   THEN 'Transportation barrier. '   ELSE '' END,
                    CASE WHEN social_isolation         THEN 'Social isolation. '         ELSE '' END,
                    'Total barriers: ', CAST(sdoh_barrier_count AS STRING),
                    '. Risk level: ', sdoh_risk_level, '.'
                )
            )
        """)
    ).withColumn("_silver_ts", current_timestamp()).drop("_source_table")

# COMMAND ----------

