# Databricks notebook source
# DBTITLE 1,04 — SDP Gold Layer
# MAGIC %md
# MAGIC # 04 — SDP Gold Layer
# MAGIC
# MAGIC 8 Gold materialized views built from Silver tables. The last 2 use AI functions for member-level intelligence.
# MAGIC
# MAGIC | Gold Table | Source | AI? | Description |
# MAGIC |---|---|---|---|
# MAGIC | `gold_member_hotspots` | silver_members + 4 silver tables | — | Composite hotspot scorecard, one row per member |
# MAGIC | `gold_cohort_summary` | gold_member_hotspots | — | Cohort rollups (CHF, Diabetes, ED, etc.) |
# MAGIC | `gold_geographic_hotspots` | gold_member_hotspots | — | ZIP-area concentration of high-need members |
# MAGIC | `gold_intervention_outcomes` | silver_care_interventions | — | Pre/post ED reduction by cohort + intervention |
# MAGIC | `gold_provider_outcomes` | silver_encounters + silver_providers | — | Risk-adjusted O/E readmission ratios |
# MAGIC | `gold_transplant_outcomes` | silver_encounters + silver_providers + silver_members | — | Program-level transplant outcomes |
# MAGIC | `gold_clinical_narratives` | gold_member_hotspots | `ai_summarize` | ★ LLM care-team summary per actionable member |
# MAGIC | `gold_intervention_recs` | gold_member_hotspots | `ai_classify` + `ai_summarize` | ★ AI-recommended intervention + rationale |

# COMMAND ----------

# DBTITLE 1,Gold: imports
import dlt
from pyspark.sql.functions import current_timestamp, expr

# COMMAND ----------

# DBTITLE 1,gold_member_hotspots
@dlt.table(
    name="gold_member_hotspots",
    comment="Gold member hotspots — composite scorecard (one row per member)",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_member_hotspots():
    return spark.sql("""
    WITH enc AS (
        SELECT member_id,
               SUM(CASE WHEN encounter_type='ED'        THEN 1 ELSE 0 END) AS ed_visits,
               SUM(CASE WHEN encounter_type='Inpatient' THEN 1 ELSE 0 END) AS inpatient_admits,
               SUM(CAST(readmission_30d AS INT))                           AS readmissions_30d,
               SUM(allowed_amount)                                         AS facility_cost
        FROM LIVE.silver_encounters GROUP BY member_id
    ),
    rx AS (
        SELECT member_id,
               AVG(pdc)                                          AS avg_pdc,
               SUM(CASE WHEN pdc < 0.80 THEN 1 ELSE 0 END)     AS nonadherent_classes,
               SUM(rx_cost)                                     AS rx_cost
        FROM (
            SELECT member_id, therapeutic_class,
                   LEAST(1.0, SUM(days_supply)/365.0) AS pdc,
                   SUM(allowed_amount)                AS rx_cost
            FROM LIVE.silver_pharmacy_claims
            GROUP BY member_id, therapeutic_class
        ) t GROUP BY member_id
    ),
    gaps AS (
        SELECT member_id,
               SUM(CASE WHEN gap_status='Open' THEN 1 ELSE 0 END) AS open_care_gaps
        FROM LIVE.silver_care_gaps GROUP BY member_id
    ),
    med AS (
        SELECT member_id,
               SUM(allowed_amount)                                           AS med_cost,
               MAX(CASE WHEN claim_type='Office Visit' THEN 1 ELSE 0 END)   AS had_office_visit
        FROM LIVE.silver_medical_claims GROUP BY member_id
    )
    SELECT
        m.member_id, m.market, m.zip_area, m.age, m.risk_score,
        CASE WHEN m.risk_score >= 3.0 THEN 'Very High'
             WHEN m.risk_score >= 2.0 THEN 'High'
             WHEN m.risk_score >= 1.2 THEN 'Rising'
             ELSE 'Low' END                                            AS risk_band,
        m.chronic_condition_count, m.has_chf, m.has_diabetes,
        m.has_ckd, m.has_copd, m.has_behavioral,
        COALESCE(enc.ed_visits, 0)                                     AS ed_visits,
        COALESCE(enc.inpatient_admits, 0)                              AS inpatient_admits,
        COALESCE(enc.readmissions_30d, 0)                              AS readmissions_30d,
        (COALESCE(enc.readmissions_30d, 0) > 0)                        AS had_readmission,
        ROUND(COALESCE(rx.avg_pdc, 1.0), 3)                           AS avg_medication_adherence_pdc,
        COALESCE(rx.nonadherent_classes, 0)                            AS nonadherent_med_classes,
        COALESCE(gaps.open_care_gaps, 0)                               AS open_care_gaps,
        (COALESCE(med.had_office_visit, 0) = 1)                        AS has_pcp_followup,
        s.sdoh_risk_level, s.sdoh_barrier_count, s.area_deprivation_index,
        s.ai_sdoh_narrative,
        ROUND(COALESCE(enc.facility_cost,0) + COALESCE(rx.rx_cost,0)
              + COALESCE(med.med_cost,0), 2)                           AS total_allowed_cost,
        ROUND(
            COALESCE(enc.ed_visits,0)*1.0
          + COALESCE(enc.inpatient_admits,0)*3.0
          + COALESCE(enc.readmissions_30d,0)*4.0
          + COALESCE(gaps.open_care_gaps,0)*0.5
          + COALESCE(rx.nonadherent_classes,0)*1.0
          + COALESCE(s.sdoh_barrier_count,0)*0.5, 1
        )                                                              AS hotspot_score,
        (
            (
                COALESCE(enc.ed_visits,0) >= 2
             OR COALESCE(enc.inpatient_admits,0) >= 1
             OR COALESCE(enc.readmissions_30d,0) >= 1
            )
            AND
            (
                COALESCE(gaps.open_care_gaps,0) >= 1
             OR COALESCE(rx.nonadherent_classes,0) >= 1
             OR COALESCE(med.had_office_visit,0) = 0
             OR s.sdoh_risk_level = 'High'
            )
        )                                                              AS is_actionable_hotspot
    FROM LIVE.silver_members m
    LEFT JOIN enc  ON m.member_id = enc.member_id
    LEFT JOIN rx   ON m.member_id = rx.member_id
    LEFT JOIN gaps ON m.member_id = gaps.member_id
    LEFT JOIN med  ON m.member_id = med.member_id
    LEFT JOIN LIVE.silver_sdoh s ON m.member_id = s.member_id
    """)

# COMMAND ----------

# DBTITLE 1,gold_cohort_summary
@dlt.table(
    name="gold_cohort_summary",
    comment="Gold cohort rollups — CHF, Diabetes, ED super-utilizer, Readmission, Rising-Risk, SDoH, Polypharmacy",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_cohort_summary():
    return spark.sql("""
    WITH mh AS (SELECT * FROM LIVE.gold_member_hotspots),
    tagged AS (
        SELECT 'CHF + Recent Admission'    AS cohort, * FROM mh WHERE has_chf      AND inpatient_admits >= 1
        UNION ALL
        SELECT 'Diabetes Poor Adherence',          * FROM mh WHERE has_diabetes   AND nonadherent_med_classes >= 1
        UNION ALL
        SELECT 'ED Super-Utilizer',                * FROM mh WHERE ed_visits >= 3
        UNION ALL
        SELECT '30-Day Readmission',               * FROM mh WHERE readmissions_30d >= 1
        UNION ALL
        SELECT 'Rising Risk',                      * FROM mh WHERE risk_band = 'Rising'
        UNION ALL
        SELECT 'High SDoH Risk',                   * FROM mh WHERE sdoh_risk_level = 'High'
        UNION ALL
        SELECT 'Complex / Polypharmacy',           * FROM mh WHERE chronic_condition_count >= 3
    )
    SELECT
        cohort, market,
        COUNT(*)                                          AS member_count,
        ROUND(AVG(risk_score), 2)                         AS avg_risk_score,
        ROUND(AVG(ed_visits), 2)                          AS avg_ed_visits,
        ROUND(AVG(inpatient_admits), 2)                   AS avg_inpatient_admits,
        ROUND(AVG(CAST(had_readmission AS INT)), 3)       AS readmission_rate,
        ROUND(AVG(open_care_gaps), 2)                     AS avg_open_care_gaps,
        ROUND(AVG(avg_medication_adherence_pdc), 3)       AS avg_medication_adherence_pdc,
        SUM(CAST(is_actionable_hotspot AS INT))           AS actionable_hotspot_count,
        ROUND(SUM(total_allowed_cost), 2)                 AS total_allowed_cost,
        ROUND(AVG(total_allowed_cost), 2)                 AS avg_allowed_cost_per_member
    FROM tagged
    GROUP BY cohort, market
    """)

# COMMAND ----------

# DBTITLE 1,gold_geographic_hotspots
@dlt.table(
    name="gold_geographic_hotspots",
    comment="Gold geographic hotspots — ZIP-area concentration of high-need members",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_geographic_hotspots():
    return spark.sql("""
    SELECT
        market, zip_area,
        COUNT(*)                                                        AS member_count,
        ROUND(AVG(risk_score), 2)                                       AS avg_risk_score,
        ROUND(AVG(area_deprivation_index), 1)                          AS avg_area_deprivation_index,
        SUM(CASE WHEN sdoh_risk_level='High' THEN 1 ELSE 0 END)        AS high_sdoh_risk_members,
        ROUND(SUM(ed_visits)*1000.0/COUNT(*), 1)                       AS ed_visits_per_1000,
        ROUND(SUM(inpatient_admits)*1000.0/COUNT(*), 1)                AS inpatient_admits_per_1000,
        ROUND(AVG(CAST(had_readmission AS INT)), 3)                    AS readmission_rate,
        SUM(CAST(is_actionable_hotspot AS INT))                        AS actionable_hotspot_members,
        ROUND(AVG(CAST(is_actionable_hotspot AS INT)), 3)              AS actionable_hotspot_rate,
        ROUND(SUM(total_allowed_cost), 2)                              AS total_allowed_cost,
        ROUND(SUM(total_allowed_cost)/COUNT(*), 2)                     AS pmpm_allowed_cost
    FROM LIVE.gold_member_hotspots
    GROUP BY market, zip_area
    """)

# COMMAND ----------

# DBTITLE 1,gold_intervention_outcomes
@dlt.table(
    name="gold_intervention_outcomes",
    comment="Gold intervention outcomes — closed-loop pre/post ED reduction by cohort",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_intervention_outcomes():
    return spark.sql("""
    SELECT
        cohort, intervention_type, market,
        COUNT(*)                                                                AS intervention_count,
        SUM(CASE WHEN status IN ('Engaged','Completed') THEN 1 ELSE 0 END)    AS engaged_count,
        ROUND(AVG(CASE WHEN status IN ('Engaged','Completed') THEN 1.0 ELSE 0 END), 3) AS engagement_rate,
        SUM(CASE WHEN outcome='Improved' THEN 1 ELSE 0 END)                   AS improved_count,
        ROUND(
            SUM(CASE WHEN outcome='Improved' THEN 1 ELSE 0 END) * 1.0
            / NULLIF(SUM(CASE WHEN status IN ('Engaged','Completed') THEN 1 ELSE 0 END), 0),
        3)                                                                     AS improved_rate,
        ROUND(AVG(ed_visits_pre_90d), 2)                                       AS avg_ed_visits_pre_90d,
        ROUND(AVG(ed_visits_post_90d), 2)                                      AS avg_ed_visits_post_90d,
        ROUND(AVG(ed_visits_pre_90d) - AVG(ed_visits_post_90d), 2)            AS avg_ed_reduction,
        ROUND(
            (AVG(ed_visits_pre_90d) - AVG(ed_visits_post_90d)) * 100.0
            / NULLIF(AVG(ed_visits_pre_90d), 0),
        1)                                                                     AS ed_reduction_pct
    FROM LIVE.silver_care_interventions
    GROUP BY cohort, intervention_type, market
    """)

# COMMAND ----------

# DBTITLE 1,gold_provider_outcomes
@dlt.table(
    name="gold_provider_outcomes",
    comment="Gold provider outcomes — risk-adjusted O/E readmission ratios per provider",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_provider_outcomes():
    return spark.sql("""
    WITH adm AS (
        SELECT e.provider_id,
               e.readmission_30d, e.complication_flag, e.los_days, e.allowed_amount,
               m.risk_score,
               LEAST(0.7, 0.05 + m.risk_score * 0.03) AS pred_readmit,
               LEAST(0.6, 0.05 + m.risk_score * 0.02) AS pred_comp
        FROM LIVE.silver_encounters e
        JOIN LIVE.silver_members m ON e.member_id = m.member_id
        WHERE e.encounter_type IN ('Inpatient','Observation')
    ),
    agg AS (
        SELECT provider_id,
               COUNT(*)                                AS admissions,
               AVG(CAST(readmission_30d AS DOUBLE))    AS obs_readmit,
               AVG(pred_readmit)                       AS exp_readmit,
               AVG(CAST(complication_flag AS DOUBLE))  AS obs_comp,
               AVG(pred_comp)                          AS exp_comp,
               AVG(risk_score)                         AS avg_risk,
               AVG(los_days)                           AS avg_los,
               SUM(allowed_amount)                     AS total_cost
        FROM adm GROUP BY provider_id
    )
    SELECT
        p.provider_id, p.provider_name, p.specialty, p.peer_group,
        p.provider_group, p.market, p.is_transplant_center,
        a.admissions,
        ROUND(a.obs_readmit, 3)                                        AS observed_readmission_rate,
        ROUND(a.exp_readmit, 3)                                        AS expected_readmission_rate,
        ROUND(a.obs_readmit / NULLIF(a.exp_readmit, 0), 2)            AS readmission_oe_ratio,
        ROUND(a.obs_comp, 3)                                           AS observed_complication_rate,
        ROUND(a.obs_comp / NULLIF(a.exp_comp, 0), 2)                  AS complication_oe_ratio,
        ROUND(a.avg_risk, 2)                                           AS avg_member_risk_score,
        ROUND(a.avg_los, 1)                                            AS avg_length_of_stay,
        ROUND(a.total_cost, 2)                                         AS total_allowed_cost,
        (a.obs_readmit / NULLIF(a.exp_readmit, 0) > 1.20
         AND a.admissions >= 20)                                        AS is_high_readmission_outlier
    FROM LIVE.silver_providers p
    JOIN agg a ON p.provider_id = a.provider_id
    """)

# COMMAND ----------

# DBTITLE 1,gold_transplant_outcomes
@dlt.table(
    name="gold_transplant_outcomes",
    comment="Gold transplant outcomes — program-level readmission, morbidity, mortality by organ type",
    table_properties={"quality": "gold", "layer": "gold"}
)
def gold_transplant_outcomes():
    return spark.sql("""
    WITH t AS (
        SELECT p.provider_group, e.transplant_type, e.market,
               e.readmission_30d, e.complication_flag, e.los_days,
               e.allowed_amount, e.discharge_disposition,
               m.risk_score,
               LEAST(0.7, 0.05 + m.risk_score * 0.03) AS pred_readmit
        FROM LIVE.silver_encounters e
        JOIN LIVE.silver_providers p ON e.provider_id = p.provider_id
        JOIN LIVE.silver_members  m ON e.member_id  = m.member_id
        WHERE e.is_transplant = true
    )
    SELECT
        provider_group, transplant_type, market,
        COUNT(*)                                                       AS transplant_volume,
        ROUND(AVG(CAST(readmission_30d AS DOUBLE)), 3)                AS observed_readmission_rate,
        ROUND(AVG(pred_readmit), 3)                                   AS expected_readmission_rate,
        ROUND(AVG(CAST(readmission_30d AS DOUBLE))
              / NULLIF(AVG(pred_readmit), 0), 2)                     AS readmission_oe_ratio,
        ROUND(AVG(CAST(complication_flag AS DOUBLE)), 3)              AS complication_rate,
        ROUND(AVG(CASE WHEN discharge_disposition='Expired'
                       THEN 1.0 ELSE 0 END), 3)                      AS mortality_rate,
        ROUND(AVG(los_days), 1)                                       AS avg_length_of_stay,
        ROUND(AVG(risk_score), 2)                                     AS avg_member_risk_score,
        ROUND(AVG(allowed_amount), 2)                                 AS avg_allowed_cost,
        (AVG(CAST(readmission_30d AS DOUBLE))
         / NULLIF(AVG(pred_readmit), 0) > 1.15
         AND COUNT(*) >= 15)                                          AS is_outlier_program
    FROM t
    GROUP BY provider_group, transplant_type, market
    """)

# COMMAND ----------

# DBTITLE 1,gold_clinical_narratives (AI Demo 6 — ai_summarize)
@dlt.table(
    name="gold_clinical_narratives",
    comment="Gold AI clinical narratives — LLM care-team summary per actionable hotspot member (ai_summarize)",
    table_properties={"quality": "gold", "layer": "gold", "ai_enriched": "true"}
)
def gold_clinical_narratives():
    return spark.sql("""
    SELECT
        mh.member_id, mh.market, mh.risk_band, mh.risk_score,
        mh.chronic_condition_count, mh.has_chf, mh.has_diabetes,
        mh.has_ckd, mh.has_copd, mh.has_behavioral,
        mh.ed_visits, mh.inpatient_admits, mh.readmissions_30d,
        mh.open_care_gaps, mh.avg_medication_adherence_pdc,
        mh.nonadherent_med_classes, mh.has_pcp_followup,
        mh.sdoh_risk_level, mh.sdoh_barrier_count, mh.area_deprivation_index,
        mh.total_allowed_cost, mh.hotspot_score,
        ai_summarize(
            CONCAT(
                'Member ', mh.member_id, ' (',  mh.market,
                ', risk band: ', mh.risk_band,
                ', risk score: ', CAST(mh.risk_score AS STRING), '). ',
                'Chronic conditions: ', CAST(mh.chronic_condition_count AS STRING),
                CASE WHEN mh.has_chf       THEN ', CHF'       ELSE '' END,
                CASE WHEN mh.has_diabetes  THEN ', Diabetes'  ELSE '' END,
                CASE WHEN mh.has_ckd       THEN ', CKD'       ELSE '' END,
                CASE WHEN mh.has_copd      THEN ', COPD'      ELSE '' END,
                CASE WHEN mh.has_behavioral THEN ', Behavioral Health' ELSE '' END,
                '. Utilization: ', CAST(mh.ed_visits AS STRING), ' ED, ',
                CAST(mh.inpatient_admits AS STRING), ' inpatient, ',
                CAST(mh.readmissions_30d AS STRING), ' readmissions. ',
                'Care gaps: ', CAST(mh.open_care_gaps AS STRING), ' open. ',
                'PDC: ', CAST(mh.avg_medication_adherence_pdc AS STRING),
                ', nonadherent classes: ', CAST(mh.nonadherent_med_classes AS STRING), '. ',
                'SDoH: ', mh.sdoh_risk_level,
                ' (', CAST(mh.sdoh_barrier_count AS STRING), ' barriers, ADI: ',
                CAST(mh.area_deprivation_index AS STRING), '). ',
                'Total cost: $', CAST(ROUND(mh.total_allowed_cost, 0) AS STRING), '. ',
                'Hotspot score: ', CAST(mh.hotspot_score AS STRING), '.'
            )
        ) AS ai_clinical_summary
    FROM LIVE.gold_member_hotspots mh
    WHERE mh.is_actionable_hotspot = true
    """)

# COMMAND ----------

# DBTITLE 1,gold_intervention_recs (AI Demo 7 — ai_classify + ai_summarize)
@dlt.table(
    name="gold_intervention_recs",
    comment="Gold AI intervention recommendations — recommended intervention type + rationale per actionable member",
    table_properties={"quality": "gold", "layer": "gold", "ai_enriched": "true"}
)
def gold_intervention_recs():
    return spark.sql("""
    SELECT
        mh.member_id, mh.market, mh.risk_band,
        mh.ed_visits, mh.inpatient_admits, mh.readmissions_30d,
        mh.open_care_gaps, mh.nonadherent_med_classes,
        mh.has_pcp_followup, mh.sdoh_risk_level, mh.sdoh_barrier_count,
        mh.hotspot_score,
        ai_classify(
            CONCAT(
                'Risk band: ', mh.risk_band, '. ',
                'ED visits: ',         CAST(mh.ed_visits AS STRING),          '. ',
                'Inpatient admits: ',  CAST(mh.inpatient_admits AS STRING),   '. ',
                'Readmissions: ',      CAST(mh.readmissions_30d AS STRING),   '. ',
                'Open care gaps: ',    CAST(mh.open_care_gaps AS STRING),     '. ',
                'Nonadherent med classes: ', CAST(mh.nonadherent_med_classes AS STRING), '. ',
                'PCP follow-up: ', CASE WHEN mh.has_pcp_followup THEN 'yes' ELSE 'no' END, '. ',
                'SDoH risk: ', mh.sdoh_risk_level,
                ' (', CAST(mh.sdoh_barrier_count AS STRING), ' barriers).'
            ),
            ARRAY(
                'Nurse Outreach',
                'Transitions of Care',
                'Medication Reconciliation',
                'Community Health Worker',
                'Complex Case Management',
                'Behavioral Health Integration'
            )
        ) AS ai_recommended_intervention,
        ai_summarize(
            CONCAT(
                'Member ', mh.member_id, ': risk ', mh.risk_band, ', ',
                CAST(mh.ed_visits AS STRING), ' ED visits, ',
                CAST(mh.readmissions_30d AS STRING), ' readmissions, ',
                CAST(mh.open_care_gaps AS STRING), ' open care gaps, ',
                CAST(mh.nonadherent_med_classes AS STRING), ' nonadherent med classes. ',
                'PCP follow-up: ', CASE WHEN mh.has_pcp_followup THEN 'established' ELSE 'absent' END, '. ',
                'SDoH: ', mh.sdoh_risk_level,
                ' (', CAST(mh.sdoh_barrier_count AS STRING), ' barriers).'
            )
        ) AS ai_recommendation_rationale
    FROM LIVE.gold_member_hotspots mh
    WHERE mh.is_actionable_hotspot = true
    """)