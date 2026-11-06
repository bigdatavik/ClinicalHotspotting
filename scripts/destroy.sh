#!/usr/bin/env bash
# Tear down everything this project created in a workspace:
#   1. bundle destroy  -> job, pipeline (+ its bronze/silver/gold tables), deployed files, Genie agent (lives in the bundle folder)
#   2. Genie agent     -> trashed by exact title if it still exists (e.g. created with a custom parent_path)
#   3. schema          -> dropped with everything left in it (8 source tables, 2 metric views)
#   4. verify          -> confirms nothing is left
#
# Usage:
#   scripts/destroy.sh -p <PROFILE> -c <CATALOG> [-t <TARGET>] [-s <SCHEMA>] [-g "<GENIE_TITLE>"] [-y] [-- <extra bundle args>]
#
# Examples:
#   scripts/destroy.sh -p my-ws -c main
#   scripts/destroy.sh -p my-ws -c main -t classic -s clinical_hotspotting_v2 -g "Clinical Hotspotting v2"
#   scripts/destroy.sh -p my-ws -c main -t interactive -- --var=existing_cluster_id=0123-456789-abcdef
#
# Use the same target, schema and Genie title you deployed with. Requires the Databricks CLI and jq.

set -euo pipefail

TARGET="dev"
SCHEMA="clinical_hotspotting"
GENIE_TITLE="Clinical Hotspotting & Outcomes Intelligence"
PROFILE=""
CATALOG=""
ASSUME_YES="false"

usage() { sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile) PROFILE="$2"; shift 2 ;;
    -c|--catalog) CATALOG="$2"; shift 2 ;;
    -t|--target)  TARGET="$2"; shift 2 ;;
    -s|--schema)  SCHEMA="$2"; shift 2 ;;
    -g|--genie-title) GENIE_TITLE="$2"; shift 2 ;;
    -y|--yes)     ASSUME_YES="true"; shift ;;
    -h|--help)    usage 0 ;;
    --)           shift; break ;;
    *) echo "Unknown option: $1" >&2; usage 1 ;;
  esac
done
EXTRA_ARGS=("$@")

[[ -n "$PROFILE" && -n "$CATALOG" ]] || { echo "Error: -p <PROFILE> and -c <CATALOG> are required." >&2; usage 1; }
[[ -n "$SCHEMA" ]] || { echo "Error: schema must not be empty." >&2; exit 1; }
command -v databricks >/dev/null || { echo "Error: databricks CLI not found." >&2; exit 1; }
command -v jq >/dev/null         || { echo "Error: jq not found." >&2; exit 1; }

# Run from the bundle root so `bundle destroy` finds databricks.yml
cd "$(dirname "$0")/.."

FULL_SCHEMA="${CATALOG}.${SCHEMA}"
BUNDLE_ARGS=(-t "$TARGET" --profile "$PROFILE"
  "--var=catalog=${CATALOG}" "--var=schema=${SCHEMA}" "--var=genie_title=${GENIE_TITLE}"
  ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"})

# All Genie spaces with this exact title (walks every page)
genie_ids() {
  local token="" page
  while :; do
    if [[ -n "$token" ]]; then
      page=$(databricks genie list-spaces --profile "$PROFILE" --page-size 100 --page-token "$token" -o json)
    else
      page=$(databricks genie list-spaces --profile "$PROFILE" --page-size 100 -o json)
    fi
    jq -r --arg t "$GENIE_TITLE" '.spaces[]? | select(.title == $t) | .space_id' <<<"$page"
    token=$(jq -r '.next_page_token // empty' <<<"$page")
    [[ -n "$token" ]] || break
  done
}

schema_exists() { databricks schemas get "$FULL_SCHEMA" --profile "$PROFILE" -o json >/dev/null 2>&1; }

echo "== Plan ======================================================="
echo "Profile      : $PROFILE  ($(databricks auth describe --profile "$PROFILE" -o json 2>/dev/null | jq -r '.details.host // "?"'))"
echo "Target       : $TARGET"
echo "Schema       : $FULL_SCHEMA  $(schema_exists && echo "(exists, $(databricks tables list "$CATALOG" "$SCHEMA" --profile "$PROFILE" -o json | jq length) objects)" || echo "(not found)")"
echo "Genie title  : $GENIE_TITLE"
EXISTING_GENIE=$(genie_ids || true)
echo "Genie agents : $(wc -w <<<"$EXISTING_GENIE" | tr -d ' ') found ${EXISTING_GENIE:+($(tr '\n' ' ' <<<"$EXISTING_GENIE"))}"
echo
echo "This permanently deletes the job, the pipeline and its tables, the deployed files, the Genie agent,"
echo "and the schema $FULL_SCHEMA with everything in it."

if [[ "$ASSUME_YES" != "true" ]]; then
  read -r -p "Type the schema name ($SCHEMA) to confirm: " answer
  [[ "$answer" == "$SCHEMA" ]] || { echo "Aborted."; exit 1; }
fi

echo
echo "== 1/4 bundle destroy ==========================================="
databricks bundle destroy "${BUNDLE_ARGS[@]}" --auto-approve

echo
echo "== 2/4 Genie agent ==============================================="
LEFT=$(genie_ids || true)
if [[ -z "$LEFT" ]]; then
  echo "No Genie agent titled '$GENIE_TITLE' left (bundle destroy removed it)."
else
  for id in $LEFT; do
    databricks genie trash-space "$id" --profile "$PROFILE" && echo "Trashed Genie agent $id"
  done
fi

echo
echo "== 3/4 schema ===================================================="
if schema_exists; then
  databricks schemas delete "$FULL_SCHEMA" --force --profile "$PROFILE"
  echo "Dropped schema $FULL_SCHEMA"
else
  echo "Schema $FULL_SCHEMA not found (already dropped)."
fi

echo
echo "== 4/4 verify ===================================================="
FAIL=0
if schema_exists; then echo "✗ schema $FULL_SCHEMA still exists"; FAIL=1; else echo "✓ schema gone"; fi
if [[ -n "$(genie_ids || true)" ]]; then echo "✗ Genie agent '$GENIE_TITLE' still exists"; FAIL=1; else echo "✓ Genie agent gone"; fi
PIPES=$(databricks pipelines list-pipelines --profile "$PROFILE" --filter "name LIKE '%clinical-hotspotting-sdp-${SCHEMA}'" -o json | jq length)
if [[ "$PIPES" != "0" ]]; then echo "✗ $PIPES pipeline(s) named *clinical-hotspotting-sdp-${SCHEMA} still exist"; FAIL=1; else echo "✓ pipeline gone"; fi

if [[ "$FAIL" == "0" ]]; then echo; echo "Done: everything for $FULL_SCHEMA ($TARGET) is deleted."; else exit 1; fi
