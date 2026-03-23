#!/usr/bin/env bash
set -euo pipefail

# Monitor a watsonx.data Spark application until terminal state.
# Usage:
#   IBM_TOKEN=... WXD_CRN=... ./scripts/monitor_spark_app.sh <APP_ID> [POLL_SECONDS]
# Optional: SPARK_ENGINE_ID=spark16 (default: spark16)

if [ $# -lt 1 ]; then
  echo "Usage: IBM_TOKEN=... WXD_CRN=... $0 <APP_ID> [POLL_SECONDS]"
  exit 1
fi

if [ -z "${IBM_TOKEN:-}" ] || [ -z "${WXD_CRN:-}" ]; then
  echo "IBM_TOKEN and WXD_CRN must be set in the environment."
  exit 1
fi

APP_ID="$1"
POLL_SECONDS="${2:-20}"
SPARK_ENGINE_ID="${SPARK_ENGINE_ID:-spark16}"
API_BASE="https://console-ibm-cator.lakehouse.saas.ibm.com/lakehouse/api/v3/spark_engines/${SPARK_ENGINE_ID}/applications"

echo "Monitoring app: $APP_ID (every ${POLL_SECONDS}s)"
echo

while true; do
  RESP="$(curl -s -X GET \
    "$API_BASE/$APP_ID" \
    -H "Authorization: Bearer $IBM_TOKEN" \
    -H "LhInstanceId: $WXD_CRN")"

  python3 - "$RESP" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
print("state:", d.get("state"))
print("spark_ui:", d.get("spark_ui", ""))
print("submission_time:", d.get("submission_time", ""))
print("end_time:", d.get("end_time", ""))
sd = d.get("state_details")
if sd:
    print("state_details:", sd)
print("-" * 60)
PY

  STATE="$(python3 - "$RESP" <<'PY'
import json, sys
print(json.loads(sys.argv[1]).get("state",""))
PY
)"

  case "$STATE" in
    finished|failed|killed)
      echo "Terminal state reached: $STATE"
      exit 0
      ;;
  esac

  sleep "$POLL_SECONDS"
done
