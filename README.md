# Clinical Hotspotting & Outcomes Intelligence on Databricks

**Find the members who need help most, understand why, and act in time.**

An end-to-end, deploy-anywhere Databricks demo for a Medicare Advantage health plan: synthetic payer data flows through a
Lakeflow Spark Declarative Pipeline (bronze → silver with AI enrichment → gold), into a governed **metric view**, and out
to a fully configured **Genie agent** that care teams can ask questions in plain English. Everything is packaged as a
**Databricks Asset Bundle**, so one `deploy` + one `run` stands it up in any workspace.

All data is **synthetic and PHI-free**.

![Architecture](docs/architecture.png)

---

## What gets deployed

| Resource | Name | What it does |
|---|---|---|
| Job | `Clinical Hotspotting End-to-End Pipeline` | Orchestrates the 4 steps below on serverless compute |
| Pipeline (SDP, serverless) | `clinical-hotspotting-sdp` | 8 bronze → 8 silver (5 AI-enriched) → 8 gold tables |
| Metric view | `<catalog>.<schema>.mv_clinical_hotspotting` | 10 dimensions × 14 governed KPIs |
| Genie agent | `Clinical Hotspotting & Outcomes Intelligence` | 9 tables, instructions, example SQL, joins, snippets, synonyms, entity matching, 10 sample questions, 12 benchmarks |

Job tasks:

```
generate_synthetic_data ──► run_sdp_pipeline ──┬──► create_metric_view
                                               └──► create_genie_agent  (creates/updates the agent, then asks it a test question)
```

| Layer | Tables | AI functions |
|---|---|---|
| Source (`01`) | `members` (5,000), `providers`, `encounters`, `medical_claims`, `pharmacy_claims`, `care_gaps`, `care_interventions`, `sdoh` | — |
| Bronze (`02`) | `bz_*` — raw + audit columns + data-quality expectations | — |
| Silver (`03`) | `silver_*` — cleansed | `ai_summarize`, `ai_classify`, `ai_analyze_sentiment` on encounters, pharmacy, care gaps, interventions, SDoH |
| Gold (`04`) | `gold_member_hotspots`, `gold_cohort_summary`, `gold_geographic_hotspots`, `gold_intervention_outcomes`, `gold_provider_outcomes`, `gold_transplant_outcomes`, `gold_clinical_narratives`, `gold_intervention_recs` | `ai_summarize`, `ai_classify` for member narratives and recommended programs |

---

## Prerequisites

1. **Databricks CLI** v0.250+ (`databricks --version`), authenticated to the target workspace:
   ```bash
   databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <PROFILE>
   ```
2. A workspace with **Unity Catalog**, **serverless compute** (jobs + pipelines), **AI Functions** (Foundation Model APIs), and **Genie**.
3. An existing **catalog** where you have `USE CATALOG` and `CREATE SCHEMA`. The bundle creates the schema.
4. A **SQL warehouse** you can use (serverless is recommended). If you don't specify one, the first serverless warehouse you can see is used.

---

## Quick start

```bash
git clone https://github.com/bigdatavik/ClinicalHotspotting.git
cd ClinicalHotspotting

# 1. Validate
databricks bundle validate -t dev --profile <PROFILE> --var="catalog=<CATALOG>"

# 2. Deploy the job, pipeline and notebooks
databricks bundle deploy   -t dev --profile <PROFILE> --var="catalog=<CATALOG>"

# 3. Run everything end to end (data → pipeline → metric view → Genie agent)
databricks bundle run clinical_hotspotting_e2e -t dev --profile <PROFILE> --var="catalog=<CATALOG>"
```

At the end, the `create_genie_agent` task prints the Genie agent URL and the answer to a test question. You can also find
the agent under **Genie** in the workspace sidebar.

A first run takes about **30 minutes** (28 min in testing: data 1 min, pipeline 25 min, metric view + Genie 1 min).
Most of the pipeline time goes to the AI functions, which make about 75k LLM calls (mostly `ai_classify` on ~45k pharmacy claims).

---

## Configuration

Everything that changes per workspace is a bundle variable in [`databricks.yml`](databricks.yml):

| Variable | Default | Description |
|---|---|---|
| `catalog` | *(required)* | Existing catalog to write to |
| `schema` | `clinical_hotspotting` | Schema created inside the catalog |
| `warehouse_id` | `""` (auto-pick) | SQL warehouse used by the Genie agent |
| `genie_title` | `Clinical Hotspotting & Outcomes Intelligence` | Agent title. An agent with this exact title is **updated**, not duplicated |
| `genie_smoke_test` | `"true"` | Ask the agent a test question and fail the job if it can't answer |

Three ways to set them:

```bash
# a) on the command line
databricks bundle deploy -t dev --profile <PROFILE> --var="catalog=main" --var="warehouse_id=abc123"

# b) environment variables
export BUNDLE_VAR_catalog=main
export BUNDLE_VAR_warehouse_id=abc123

# c) a per-target overrides file (git-ignored): .databricks/bundle/dev/variable-overrides.json
{ "catalog": "main", "warehouse_id": "abc123" }
```

Or add your own target to `databricks.yml`:

```yaml
targets:
  my_workspace:
    mode: development
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
    variables:
      catalog: my_catalog
      warehouse_id: abc123
```

### Targets

| Target | Mode | Notes |
|---|---|---|
| `dev` (default) | development | Job/pipeline names are prefixed `[dev <you>]`; files go to `~/.bundle/clinical-hotspotting/dev` |
| `prod` | production | Files go to `/Workspace/Shared/.bundle/clinical-hotspotting/prod`; `users` group gets CAN_VIEW |

---

## Running the demo

After the job succeeds, open the deployed notebooks in the workspace under
`.bundle/clinical-hotspotting/<target>/files/src`:

| Notebook | Purpose |
|---|---|
| `06_demo` | Customer-facing walkthrough: Genie → metric view → gold → silver AI → bronze → sources |
| `07_demo_talk_track` | 30–40 min read-along script with discovery questions and value framing |

Set the `catalog` / `schema` widgets at the top of each notebook to match your deployment.

---

## Project layout

```
databricks.yml                         bundle config, variables, targets
resources/
  clinical_hotspotting.pipeline.yml    SDP pipeline (serverless)
  clinical_hotspotting_e2e.job.yml     orchestration job
src/
  00_config.py                         shared parameters (reference)
  01_generate_synthetic_data.py        8 synthetic source tables
  02_sdp_bronze.py                     SDP bronze + expectations
  03_sdp_silver_ai.py                  SDP silver + AI Functions
  04_sdp_gold.py                       SDP gold + AI narratives/recommendations
  05_create_metric_view.py             governed metric view
  06_demo.py                           demo walkthrough
  07_demo_talk_track.py                presenter talk track
  09_genie_config.py                   Genie agent as code (create/update + smoke test)
docs/architecture.png
```

### Why the Genie agent is created by a job task, not a bundle resource

The Genie API rejects a space whose tables don't exist yet. On a fresh workspace, the gold tables appear only after
the pipeline runs, so a `genie_spaces` bundle resource would fail on the first `deploy`. Instead, `09_genie_config.py`
runs after the pipeline. It checks every example and benchmark query against the data, then creates or updates the
agent by title. The full definition lives in that notebook.

---

## Re-running and updating

- **Refresh data:** re-run the job. Source tables are overwritten (same seed, so the data is identical), the pipeline
  recomputes, and the Genie agent is updated in place.
- **Change Genie instructions, examples or benchmarks:** edit `src/09_genie_config.py`, then `bundle deploy` and re-run
  the job (or run only the `create_genie_agent` task from the job UI).

## Clean up

```bash
# Delete the job, pipeline and deployed files
databricks bundle destroy -t dev --profile <PROFILE> --var="catalog=<CATALOG>"

# Delete the Genie agent (ID is printed by the create_genie_agent task)
databricks genie trash-space <SPACE_ID> --profile <PROFILE>

# Drop the data
databricks experimental aitools tools query "DROP SCHEMA <CATALOG>.clinical_hotspotting CASCADE" --profile <PROFILE>
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `catalog` variable has no value | Pass `--var="catalog=<CATALOG>"`, or use one of the other options above |
| `PERMISSION_DENIED` creating the schema | Grant `USE CATALOG` and `CREATE SCHEMA` on the catalog, or pick another one |
| Pipeline fails on `ai_*` functions | AI Functions / Foundation Model APIs are not enabled in this workspace or region |
| `create_genie_agent` fails with "No SQL warehouse found" | Pass `--var="warehouse_id=<ID>"` |
| Genie smoke test times out | The warehouse was cold. Re-run the task, or set `genie_smoke_test=false` |
