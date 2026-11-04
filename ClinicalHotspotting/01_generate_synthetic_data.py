# Databricks notebook source
# DBTITLE 1,01 — Generate Synthetic Source Data
# MAGIC %md
# MAGIC # 01 — Generate Synthetic Source Data
# MAGIC
# MAGIC Generates **8 PHI-free synthetic source tables** in `payer_catalog_vm.clinical_hotspotting`.
# MAGIC Run this notebook **before** triggering the SDP pipeline (notebooks 02–04).
# MAGIC
# MAGIC | Table | Rows | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `members` | 5,000 | Member demographics, risk scores, chronic conditions |
# MAGIC | `providers` | 400 | Provider roster with specialty, peer group, transplant flag |
# MAGIC | `encounters` | ~9,100 | ED, Inpatient, Observation — including transplant cases |
# MAGIC | `medical_claims` | ~24,000 | Office visits, labs, imaging, specialist, home health, DME |
# MAGIC | `pharmacy_claims` | ~79,000 | Drug fills by therapeutic class with days supply |
# MAGIC | `care_gaps` | ~19,500 | HEDIS-aligned open and closed care gaps |
# MAGIC | `care_interventions` | ~1,400 | Care management outreach with pre/post ED utilization |
# MAGIC | `sdoh` | 5,000 | Social determinants: housing, food, transport, isolation, ADI |

# COMMAND ----------

# DBTITLE 1,Setup: create schema
CATALOG = "payer_catalog_vm"
SCHEMA  = "clinical_hotspotting"
FQSN    = f"{CATALOG}.{SCHEMA}"
SEED    = 42

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQSN}")
spark.sql(f"USE {FQSN}")
print(f"Target schema ready: {FQSN}")

# COMMAND ----------

# DBTITLE 1,Imports and seed (PySpark-safe)
import random, datetime, math
from pyspark.sql import Row
# NOTE: do NOT import pyspark.sql.functions.* here — it shadows round/min/max/sum

# Serverless PySpark shadows round/min/max/sum in builtins.
# Use these safe wrappers instead:
def r2(x):  return math.floor(float(x)*100 + 0.5) / 100.0
def r3(x):  return math.floor(float(x)*1000 + 0.5) / 1000.0
def smin(a,b): return a if a < b else b
def smax(a,b): return a if a > b else b

random.seed(SEED)

# --- domain constants ---------------------------------------------------------
MARKETS = ["Georgia", "Texas", "Kentucky", "North Carolina", "Ohio",
           "Illinois", "Tennessee", "Florida", "Indiana", "Virginia"]

SPECIALTIES = ["Primary Care", "Cardiology", "Nephrology", "Endocrinology",
               "Pulmonology", "Internal Medicine", "General Surgery",
               "Transplant Surgery", "Oncology", "Behavioral Health"]

PEER_GROUPS = ["Academic Medical Center", "Community Hospital",
               "Specialty Clinic", "Rural Health System", "Urban Safety Net"]

PROVIDER_GROUPS = ["Lakeview Health", "Summit Medical", "Coastal Care",
                   "Prairie Health", "Mountain View Medical", "Riverside Clinic",
                   "Valley Health Partners", "Metro Primary Care",
                   "Sunrise Health", "Heritage Medical"]

ENCOUNTER_TYPES = ["ED", "Inpatient", "Observation"]
DX_CATEGORIES   = ["CHF", "Diabetes", "CKD", "COPD", "AMI", "Pneumonia",
                   "Sepsis", "UTI", "Cellulitis", "Fall/Injury",
                   "Behavioral Health", "Transplant", "Other"]
ADMIT_TYPES      = ["Emergency", "Urgent", "Elective"]
DISCH_DISP       = ["Home", "SNF", "Home Health", "AMA", "Expired"]
TRANSPLANT_TYPES = ["Kidney", "Liver", "Heart"]

CLAIM_TYPES  = ["Office Visit", "Specialist Visit", "Lab", "Imaging",
                "Home Health", "Physical Therapy", "DME"]

THERA_CLASSES = [
    ("ACE/ARB Inhibitor",       ["Lisinopril", "Losartan", "Enalapril", "Valsartan"]),
    ("Beta Blocker",            ["Metoprolol", "Carvedilol", "Atenolol"]),
    ("Diuretic",                ["Furosemide", "Spironolactone", "Hydrochlorothiazide"]),
    ("Oral Antidiabetic",       ["Metformin", "Empagliflozin", "Semaglutide", "Sitagliptin"]),
    ("Insulin",                 ["Insulin Glargine", "Insulin Lispro", "NPH Insulin"]),
    ("Statins",                 ["Atorvastatin", "Rosuvastatin", "Simvastatin"]),
    ("Inhaled Corticosteroid",  ["Fluticasone", "Budesonide", "Beclomethasone"]),
    ("Antidepressant",          ["Sertraline", "Escitalopram", "Fluoxetine"]),
]

MEASURE_NAMES = [
    ("Annual Wellness Visit",                 "AWV"),
    ("Diabetes A1c Testing",                  "CDC-HBA1C"),
    ("Controlling High Blood Pressure",       "CBP"),
    ("COPD Medication Management",            "COPD-Med"),
    ("Statin Therapy for Cardiovascular Disease", "SPC"),
    ("Post-Discharge Follow-up (7-day)",      "TRC"),
    ("Behavioral Health Follow-up",           "FUH"),
    ("Nephropathy Screening",                 "CDC-NEP"),
]

COHORTS = [
    "CHF Transitions of Care", "ED Super-Utilizer", "COPD Management",
    "Complex Case Management", "Rising-Risk Diabetes", "Behavioral Health Integration"
]

INTERVENTION_TYPES = [
    "Nurse Outreach", "Transitions of Care", "Medication Reconciliation",
    "Community Health Worker", "Complex Case Management", "Behavioral Health Integration"
]

STATUSES  = ["Completed", "Engaged", "Attempted", "Declined"]
OUTCOMES  = ["Improved", "No Change", "Escalated"]
CARE_TEAMS = ["Care Team Alpha", "Care Team Bravo", "Care Team Charlie", "Care Team Delta"]

BASE_DATE = datetime.date(2024, 1, 1)
END_DATE  = datetime.date(2025, 12, 31)

def rand_date(start=BASE_DATE, end=END_DATE):
    delta = (end - start).days
    return start + datetime.timedelta(days=random.randint(0, delta))

print("Constants loaded.")

# COMMAND ----------

# DBTITLE 1,Generate members (5,000 rows)
random.seed(SEED)

NUM_MEMBERS  = 5000
NUM_PROVIDERS = 400

member_ids   = [f"M{str(i).zfill(7)}" for i in range(NUM_MEMBERS)]
provider_ids = [f"P{str(i+1).zfill(5)}" for i in range(NUM_PROVIDERS)]

zip_areas = {}
for mkt in MARKETS:
    zip_areas[mkt] = [f"{mkt} ZIP-{str(random.randint(100,999))}" for _ in range(30)]

members = []
for mid in member_ids:
    market  = random.choice(MARKETS)
    zip_a   = random.choice(zip_areas[market])
    age     = random.randint(18, 85)
    rs      = random.randint(30, 450) / 100.0  # 2dp, avoids round()
    n_cond  = random.choices([0, 1, 2, 3, 4, 5], weights=[15, 25, 25, 20, 10, 5])[0]
    has_chf  = n_cond >= 2 and random.random() < 0.40
    has_diab = n_cond >= 1 and random.random() < 0.45
    has_ckd  = n_cond >= 2 and random.random() < 0.30
    has_copd = n_cond >= 1 and random.random() < 0.20
    has_chd  = n_cond >= 2 and random.random() < 0.25
    has_beh  = random.random() < 0.15
    members.append(Row(
        member_id=mid, age=age,
        gender=random.choice(["M", "F"]),
        plan_type=random.choice(["HMO", "PPO", "EPO"]),
        dual_eligible=random.random() < 0.25,
        months_enrolled=random.randint(6, 36),
        attributed_pcp_id=random.choice(provider_ids),
        market=market, zip_area=zip_a,
        has_chf=has_chf, has_diabetes=has_diab, has_ckd=has_ckd,
        has_copd=has_copd, has_chd=has_chd, has_behavioral=has_beh,
        chronic_condition_count=n_cond,
        risk_score=rs  # float 0.30–4.50
    ))

df_members = spark.createDataFrame(members)
df_members.write.mode("overwrite").saveAsTable(f"{FQSN}.members")
print(f"members: {df_members.count():,} rows")
display(df_members.limit(3))

# COMMAND ----------

# DBTITLE 1,Generate providers (400 rows)
random.seed(SEED + 1)

providers = []
for pid in provider_ids:
    spec   = random.choice(SPECIALTIES)
    is_tx  = spec == "Transplant Surgery"
    market = random.choice(MARKETS)
    providers.append(Row(
        provider_id=pid,
        npi=random.randint(1000000000, 1999999999),
        provider_name=f"Dr. {random.choice(['Smith','Jones','Patel','Nguyen','Garcia','Kim','Brown','Davis'])} {pid}",
        specialty=spec,
        peer_group=random.choice(PEER_GROUPS),
        provider_group=random.choice(PROVIDER_GROUPS),
        facility_name=f"{random.choice(PROVIDER_GROUPS)} — {market}",
        market=market,
        is_transplant_center=is_tx,
        readmit_propensity=random.randint(50, 250) / 1000.0  # 3dp, avoids round()
    ))

df_providers = spark.createDataFrame(providers)
df_providers.write.mode("overwrite").saveAsTable(f"{FQSN}.providers")
print(f"providers: {df_providers.count():,} rows")
display(df_providers.limit(3))

# COMMAND ----------

# DBTITLE 1,Generate encounters (~9,100 rows)
random.seed(SEED + 2)

# transplant-eligible providers
tx_providers = [p.provider_id for p in providers if p.is_transplant_center]
if not tx_providers:
    tx_providers = provider_ids[:20]

encounters  = []
enc_id      = 1

for mid in member_ids:
    m      = next(x for x in members if x.member_id == mid)
    rs     = m.risk_score
    n_enc  = random.choices([0,1,2,3,4,5,6], weights=[20,28,22,14,8,5,3])[0]
    for _ in range(n_enc):
        etype = random.choices(ENCOUNTER_TYPES, weights=[40, 45, 15])[0]
        is_tx = m.has_chf is False and random.random() < 0.018   # ~1.8% transplant rate
        if is_tx:
            dx_cat = "Transplant"
            tx_type = random.choice(TRANSPLANT_TYPES)
            los    = random.randint(5, 21)
            atype  = "Elective"
            pid    = random.choice(tx_providers)
        else:
            dx_cat  = random.choice(DX_CATEGORIES[:-1])  # exclude Transplant
            tx_type = None
            los     = random.randint(1, 14) if etype == "Inpatient" else random.randint(0, 2)
            atype   = random.choice(ADMIT_TYPES)
            pid     = random.choice(provider_ids)
        admit  = rand_date()
        disch  = admit + datetime.timedelta(days=los)
        readm  = random.random() < min(0.35, 0.03 + rs * 0.06)
        comp   = random.random() < min(0.30, 0.02 + rs * 0.04)
        cost   = round(random.uniform(800, 250000) if is_tx else random.uniform(500, 40000), 2)
        mkt    = m.market
        dd     = random.choices(DISCH_DISP, weights=[60, 15, 12, 8, 5])[0]
        encounters.append(Row(
            encounter_id=f"ET{str(enc_id).zfill(7)}",
            member_id=mid, provider_id=pid, market=mkt,
            encounter_type=etype, primary_dx_category=dx_cat,
            admit_date=admit, discharge_date=disch, los_days=los,
            admission_type=atype, discharge_disposition=dd,
            is_transplant=is_tx, transplant_type=tx_type,
            readmission_30d=readm, complication_flag=comp,
            allowed_amount=cost
        ))
        enc_id += 1

df_enc = spark.createDataFrame(encounters)
df_enc.write.mode("overwrite").saveAsTable(f"{FQSN}.encounters")
print(f"encounters: {df_enc.count():,} rows")
display(df_enc.limit(3))

# COMMAND ----------

# DBTITLE 1,Generate medical_claims (~24,000 rows)
random.seed(SEED + 3)

claims = []
claim_id = 1

for mid in member_ids:
    m      = next(x for x in members if x.member_id == mid)
    n_clm  = random.choices([0,1,2,3,4,5,6,7,8], weights=[5,8,12,20,22,16,10,5,2])[0]
    for _ in range(n_clm):
        ctype  = random.choices(CLAIM_TYPES, weights=[30,20,20,15,5,5,5])[0]
        cost   = round(random.uniform(50, 3000), 2)
        svc_dt = rand_date()
        pid    = random.choice(provider_ids)
        claims.append(Row(
            claim_id=f"C{str(claim_id).zfill(9)}",
            member_id=mid, provider_id=pid, market=m.market,
            claim_type=ctype,
            service_date=svc_dt,
            allowed_amount=cost
        ))
        claim_id += 1

df_med = spark.createDataFrame(claims)
df_med.write.mode("overwrite").saveAsTable(f"{FQSN}.medical_claims")
print(f"medical_claims: {df_med.count():,} rows")
display(df_med.limit(3))

# COMMAND ----------

# DBTITLE 1,Generate pharmacy_claims (~79,000 rows)
random.seed(SEED + 4)

rx_rows  = []
rx_id    = 1

for mid in member_ids:
    m = next(x for x in members if x.member_id == mid)
    # assign 1-4 therapeutic classes per member based on conditions
    classes = []
    if m.has_chf:      classes += ["ACE/ARB Inhibitor", "Beta Blocker", "Diuretic"]
    if m.has_diabetes: classes += ["Oral Antidiabetic", "Insulin"]
    if m.has_ckd:      classes += ["ACE/ARB Inhibitor"]
    if m.has_copd:     classes += ["Inhaled Corticosteroid"]
    if m.has_behavioral: classes += ["Antidepressant"]
    # always add statins for risk > 1.5
    if m.risk_score > 1.5: classes += ["Statins"]
    # deduplicate
    classes = list(set(classes)) if classes else ["Statins"]

    for tc in classes:
        drugs = next(d for t, d in THERA_CLASSES if t == tc)
        drug  = random.choice(drugs)
        # 6-18 fills per year depending on adherence
        n_fills  = random.randint(4, 18)
        fill_dt  = rand_date(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))
        for _ in range(n_fills):
            ds   = random.choice([30, 60, 90])
            cost = round(random.uniform(30, 500), 2)
            rx_rows.append(Row(
                pharmacy_claim_id=f"RX{str(rx_id).zfill(8)}",
                member_id=mid,
                therapeutic_class=tc,
                drug_name=drug,
                fill_date=fill_dt,
                days_supply=ds,
                allowed_amount=cost
            ))
            fill_dt = fill_dt + datetime.timedelta(days=ds + random.randint(-5, 15))
            if fill_dt > datetime.date(2024, 12, 31):
                break
            rx_id += 1

df_rx = spark.createDataFrame(rx_rows)
df_rx.write.mode("overwrite").saveAsTable(f"{FQSN}.pharmacy_claims")
print(f"pharmacy_claims: {df_rx.count():,} rows")
display(df_rx.limit(3))

# COMMAND ----------

# DBTITLE 1,Generate care_gaps (~19,500 rows)
random.seed(SEED + 5)

gap_rows = []
gap_id   = 1

for mid in member_ids:
    m       = next(x for x in members if x.member_id == mid)
    n_gaps  = random.choices([0,1,2,3,4,5,6], weights=[10,15,20,22,18,10,5])[0]
    # weight measure selection toward relevant conditions
    weights = [1,1,1,1,1,1,1,1]
    if m.has_diabetes: weights[1] = 4  # Diabetes A1c
    if m.has_chf:      weights[2] = 3  # CBP
    if m.has_copd:     weights[3] = 4  # COPD
    for _ in range(n_gaps):
        mn, mc  = random.choices(MEASURE_NAMES, weights=weights)[0]
        id_dt   = rand_date(datetime.date(2024, 1, 1), datetime.date(2025, 6, 30))
        due_dt  = id_dt + datetime.timedelta(days=90)
        status  = random.choices(["Open", "Closed"], weights=[35, 65])[0]
        cl_dt   = id_dt + datetime.timedelta(days=random.randint(5, 89)) if status == "Closed" else None
        gap_rows.append(Row(
            care_gap_id=f"CG{str(gap_id).zfill(8)}",
            member_id=mid, measure_name=mn, measure_code=mc,
            market=m.market, identified_date=id_dt, due_date=due_dt,
            gap_status=status, closed_date=cl_dt
        ))
        gap_id += 1

df_gaps = spark.createDataFrame(gap_rows)
df_gaps.write.mode("overwrite").saveAsTable(f"{FQSN}.care_gaps")
print(f"care_gaps: {df_gaps.count():,} rows")
display(df_gaps.limit(3))

# COMMAND ----------

# DBTITLE 1,Generate care_interventions (~1,400 rows)
random.seed(SEED + 6)

iv_rows = []
iv_id   = 1

# target high-risk or condition-flagged members
high_risk = [m for m in members if m.risk_score >= 2.0 or m.has_chf or m.has_diabetes or m.has_copd]
n_sample = 1400 if len(high_risk) > 1400 else len(high_risk)  # avoids min()
for m in random.sample(high_risk, n_sample):
    cohort  = random.choice(COHORTS)
    iv_type = random.choice(INTERVENTION_TYPES)
    out_dt  = rand_date(datetime.date(2024, 1, 1), datetime.date(2025, 10, 1))
    status  = random.choices(STATUSES, weights=[25, 40, 20, 15])[0]
    outcome = random.choice(OUTCOMES) if status in ("Completed", "Engaged") else None
    pre_ed  = random.randint(0, 5)
    if outcome == "Improved":
        _diff = random.randint(1, 3)
        post_ed = (pre_ed - _diff) if (pre_ed - _diff) >= 0 else 0  # avoids max()
    elif outcome == "Escalated":
        post_ed = pre_ed + random.randint(1, 2)
    else:
        post_ed = pre_ed
    iv_rows.append(Row(
        intervention_id=f"IV{str(iv_id).zfill(7)}",
        member_id=m.member_id, market=m.market,
        cohort=cohort, intervention_type=iv_type,
        outreach_date=out_dt, status=status, outcome=outcome,
        assigned_care_team=random.choice(CARE_TEAMS),
        ed_visits_pre_90d=pre_ed, ed_visits_post_90d=post_ed
    ))
    iv_id += 1

df_iv = spark.createDataFrame(iv_rows)
df_iv.write.mode("overwrite").saveAsTable(f"{FQSN}.care_interventions")
print(f"care_interventions: {df_iv.count():,} rows")
display(df_iv.limit(3))

# COMMAND ----------

# DBTITLE 1,Generate sdoh (5,000 rows)
random.seed(SEED + 7)

sdoh_rows = []
for m in members:
    housing = random.random() < 0.22
    food    = random.random() < 0.18
    transp  = random.random() < 0.20
    social  = random.random() < 0.25
    n_bar   = int(housing) + int(food) + int(transp) + int(social)  # avoids sum()
    if n_bar == 0:   risk_lv = "Low"
    elif n_bar == 1: risk_lv = random.choice(["Low", "Moderate"])
    elif n_bar == 2: risk_lv = "Moderate"
    else:            risk_lv = "High"
    adi = random.randint(20, 95)
    if adi > 75:     risk_lv = "High"
    sdoh_rows.append(Row(
        member_id=m.member_id, market=m.market, zip_area=m.zip_area,
        area_deprivation_index=adi,
        housing_insecurity=housing, food_insecurity=food,
        transportation_barrier=transp, social_isolation=social,
        sdoh_barrier_count=n_bar, sdoh_risk_level=risk_lv
    ))

df_sdoh = spark.createDataFrame(sdoh_rows)
df_sdoh.write.mode("overwrite").saveAsTable(f"{FQSN}.sdoh")
print(f"sdoh: {df_sdoh.count():,} rows")
display(df_sdoh.limit(3))

# COMMAND ----------

# DBTITLE 1,Summary: row counts for all 8 tables
tables = ["members", "providers", "encounters", "medical_claims",
          "pharmacy_claims", "care_gaps", "care_interventions", "sdoh"]
print(f"{'Table':<25} {'Rows':>8}")
print("-" * 35)
for t in tables:
    cnt = spark.table(f"{FQSN}.{t}").count()
    print(f"{t:<25} {cnt:>8,}")
print("\n✅ All 8 source tables ready. Proceed to SDP pipeline.")

# COMMAND ----------

