#!/usr/bin/env bash
# Validate and deploy the bundle to dev, then optionally run the generator.
#   scripts/deploy_dev.sh                 # validate + deploy
#   scripts/deploy_dev.sh run cdc_chaos   # ... then run one scenario
set -euo pipefail

cd "$(dirname "$0")/.."
PROFILE="${DATABRICKS_CONFIG_PROFILE:-dev}"

databricks bundle validate --strict -t dev --profile "$PROFILE"
databricks bundle deploy -t dev --profile "$PROFILE"

if [[ "${1:-}" == "run" ]]; then
  databricks bundle run event_generator -t dev --profile "$PROFILE" \
    --params "scenario=${2:-baseline}"
fi
