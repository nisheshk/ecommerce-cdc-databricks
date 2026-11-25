#!/usr/bin/env bash
# Sync Genie Code skills and instructions to the Databricks workspace.
#   scripts/sync_to_workspace.sh             # skills + workspace instructions
#   scripts/sync_to_workspace.sh --personal  # also your own .assistant_instructions.md
# Uses DATABRICKS_CONFIG_PROFILE (default: dev).
set -euo pipefail

cd "$(dirname "$0")/.."
PROFILE="${DATABRICKS_CONFIG_PROFILE:-dev}"
PERSONAL=false
[[ "${1:-}" == "--personal" ]] && PERSONAL=true

cli() { databricks "$@" --profile "$PROFILE"; }

SKILLS_TARGET=/Workspace/.assistant/skills
cli workspace mkdirs "$SKILLS_TARGET"
for dir in .agents/skills/*/; do
  [[ -f "${dir}SKILL.md" ]] || continue
  name="$(basename "$dir")"
  echo "skill        ${name} -> ${SKILLS_TARGET}/${name}"
  cli workspace mkdirs "${SKILLS_TARGET}/${name}"
  cli workspace import-dir "$dir" "${SKILLS_TARGET}/${name}" --overwrite >/dev/null
done

echo "instructions workspace -> /Workspace/.assistant_workspace_instructions.md"
cli workspace import /Workspace/.assistant_workspace_instructions.md \
  --file instructions/.assistant_workspace_instructions.md --format RAW --overwrite

if $PERSONAL; then
  me="$(cli current-user me -o json | python3 -c 'import json, sys; print(json.load(sys.stdin)["userName"])')"
  personal="$(mktemp)"
  trap 'rm -f "$personal"' EXIT
  # Fill in the template: drop its header comment and placeholder rows, insert your user name.
  awk 'NR == 1 && /^<!--/ { skip = 1 } skip { if (/-->/) skip = 0; next } { print }' \
    instructions/.assistant_instructions.md \
    | sed "s|<your email>|${me}|g" | grep -v '<your-' > "$personal"
  echo "instructions personal  -> /Workspace/Users/${me}/.assistant_instructions.md"
  cli workspace import "/Workspace/Users/${me}/.assistant_instructions.md" \
    --file "$personal" --format RAW --overwrite
fi

echo "Done. Start a new Genie Code chat so the skills and instructions load."
