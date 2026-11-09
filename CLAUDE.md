# CLAUDE.md — Clinical Hotspotting & Outcomes Intelligence

A Databricks Asset Bundle for a Medicare Advantage demo. It generates synthetic, PHI-free payer data, runs a serverless
SDP pipeline (bronze → silver with AI Functions → gold), builds governed metric views, and creates a Genie agent.
README.md is the user-facing guide. This file is for working on the code.

## Layout

```
databricks.yml                      bundle: variables (catalog required), targets dev (default) / prod / classic / interactive
resources/*.pipeline.yml            SDP pipeline "clinical-hotspotting-sdp-<schema>" (notebooks 02–04)
resources/*.job.yml                 job "Clinical Hotspotting End-to-End Pipeline", key clinical_hotspotting_e2e
src/01_generate_synthetic_data.py   8 source tables (seed 42, overwrite)
src/02_sdp_bronze.py                bz_* + expectations; reads spark.conf source_catalog / source_schema
src/03_sdp_silver_ai.py             silver_* + ai_summarize / ai_classify / ai_analyze_sentiment
src/04_sdp_gold.py                  gold_* (member hotspots, cohorts, geo, interventions, providers, transplant, AI recs)
src/05_create_metric_view.py        mv_clinical_hotspotting (member grain) + mv_encounter_utilization (encounter grain) + reconciliation
src/06_demo.py, 07_demo_talk_track.py   presenter notebooks (not run by the job)
src/09_genie_config.py              Genie agent as code: validates SQL, creates or updates by title, smoke-test question
scripts/destroy.sh                  full teardown: bundle destroy → trash Genie by title → schemas delete --force → verify
prompts/                            git-ignored local prompts (not part of the repo)
```

Job DAG: `generate_synthetic_data → run_sdp_pipeline → create_metric_view → create_genie_agent` (Genie attaches both metric views, so they must exist first).

## Commands

Always pass `--profile` and `--var="catalog=..."`. Never pick a Databricks profile automatically; ask the user which one.

```bash
databricks bundle validate --strict -t dev --profile <PROFILE> --var="catalog=<CATALOG>"
databricks bundle deploy            -t dev --profile <PROFILE> --var="catalog=<CATALOG>"
databricks bundle run clinical_hotspotting_e2e -t dev --profile <PROFILE> --var="catalog=<CATALOG>"

# Re-run part of the job (much cheaper than a full run)
... bundle run clinical_hotspotting_e2e ... --only create_metric_view
... bundle run clinical_hotspotting_e2e ... --only create_genie_agent
... bundle run clinical_hotspotting_e2e ... --only run_sdp_pipeline+     # pipeline and everything downstream

# Full teardown (asks you to type the schema name; -y skips the prompt). Use the same target/schema/title as the deploy.
scripts/destroy.sh -p <PROFILE> -c <CATALOG> [-t <TARGET>] [-s <SCHEMA>] [-g "<GENIE_TITLE>"] [-- <extra --var args>]

# Ad-hoc SQL checks
databricks experimental aitools tools query "<SQL>" --profile <PROFILE> -o json
```

Run `bundle validate --strict` after every YAML change. Run `bundle deploy` before `bundle run`, because notebook edits
only reach the workspace on deploy.

## Compute targets

| Target | Notebook tasks | Pipeline | Extra vars |
|---|---|---|---|
| `dev` (default), `prod` | serverless | serverless | — |
| `classic` | shared job cluster `main` (UC SINGLE_USER, `spark_version` default 17.3 LTS; metric view YAML 1.1 needs DBR 17.2+) | classic cluster | `node_type_id` |
| `interactive` | `existing_cluster_id` | classic cluster | `existing_cluster_id`, `node_type_id` |

- There is **no automatic fallback** to classic compute. The target decides. Overrides live under `targets.<t>.resources`
  in `databricks.yml` and merge into the base job/pipeline by `task_key` / cluster `label`.
- SDP pipelines can't run on all-purpose clusters, so `interactive` still uses a classic pipeline cluster.
- Every target requires a **serverless SQL warehouse** for Genie. `09_genie_config.py` rejects non-serverless
  warehouses and auto-picks only serverless ones.
- Some workspaces are serverless-only (classic cluster creation fails with "You must use serverless compute in this
  workspace"), so the classic and interactive targets can't be run-tested there. Only `bundle validate` works.
- Each target is a separate deployment. Pipeline names must be unique per workspace, so the schema is part of the
  name. Two deployments in one workspace need different targets *and* schemas.

## Cost and runtime

A full job run takes about 30 minutes and makes about 75k AI Function calls, most of them `ai_classify` on about 45k
pharmacy claims. The silver and gold tables are materialized views, so every pipeline run recomputes all AI columns.
Prefer `--only <task>` when iterating, and don't trigger a full run without a reason.

## Conventions and invariants

- **No hardcoded catalog or schema.** Notebooks read `catalog` / `schema` from widgets (job parameters fill them in).
  The pipeline reads them from `spark.conf`. Workspace hostnames, IDs and customer-specific catalog names must never be
  committed. The repo is public, so check `git grep` for them before every push.
- **Metric views have exactly one grain.** Never join a one-to-many table (for example, encounters onto members) into a
  metric view source. That repeats rows and inflates measures; this bug already happened once. Use many-to-one joins
  only, and add a separate view for a new grain. The reconciliation cell at the end of `05_create_metric_view.py` must
  keep passing. Extend it when you add measures.
- **The Genie agent is created by a job task, not a `genie_spaces` bundle resource.** The Genie API rejects a space whose
  tables don't exist yet, which would break the first deploy on a fresh workspace. The job passes
  `parent_path = ${workspace.file_path}`, so the agent lives in the bundle folder and **`bundle destroy` deletes it**
  (verified 2026-11-05). Destroy also deletes the pipeline's tables (`cascade_on_destroy` defaults to true). The source
  tables, metric views and schema remain; drop them with `DROP SCHEMA ... CASCADE`.
- **Genie attaches both metric views** (`data_sources.metric_views`) plus 9 tables; KPI examples use `MEASURE()`.
  "Impactable" is a synonym of `is_actionable_hotspot` (the blog's term).
- **Genie config lives in `09_genie_config.py`.** It uses `serialized_space` version 2 with deterministic uuid5 ids, and
  id-keyed arrays must be sorted by id. Every example and benchmark SQL runs before the API call. Verify column names
  against the gold tables: the original config referenced columns that didn't exist (`organ_type`, `gap_type`, a
  "Stable" risk band).
- **Pre-computed rates** (`engagement_rate`, `improved_rate`, `ed_reduction_pct` in `gold_intervention_outcomes`) are
  selected directly, never recomputed from counts.
- **Risk band values** are exactly `Very High`, `High`, `Rising`, `Low`.
- **Data generation code avoids `from pyspark.sql.functions import *`**, because it shadows Python's
  round/min/max/sum. Keep that rule.
- Use the `databricks-dabs`, `databricks-pipelines`, `databricks-metric-views` and `databricks-genie-agents` skills
  for bundle, pipeline, metric view and Genie work.

## Verifying a change

After changing gold logic, a metric view or Genie config, deploy, run the affected task with `--only`, then check:

```sql
SELECT COUNT(*), SUM(CAST(is_actionable_hotspot AS INT)) FROM <catalog>.clinical_hotspotting.gold_member_hotspots;  -- 5000, ~3,225
SELECT market, MEASURE(actionable_hotspot_rate) FROM <catalog>.clinical_hotspotting.mv_clinical_hotspotting GROUP BY ALL; -- ~0.65, not ~0.80
```

For Genie, ask a question with `databricks genie start-conversation <SPACE_ID> "<question>"` and check which table it used.

## Git

- Commit messages end with: `Co-authored-by: Isaac <no-reply@databricks.com>`
- Commit or push only when the user asks.
