#!/usr/bin/env bash
# Submit a payload template with placeholders replaced from .env.
# Usage:
#   source .env
#   ./scripts/submit_payload.sh scripts/payload_hello_base.json

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: source .env && $0 <payload-template.json>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"

# Auto-load .env with export if present
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

TPL="$1"
if [ ! -f "$TPL" ]; then
  echo "Template not found: $TPL"
  exit 1
fi

for var in IBM_API_KEY WXD_CRN SPARK_ENGINE_ID AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY; do
  if [ -z "${!var:-}" ]; then
    echo "Missing env var: $var (run: source .env)"
    exit 1
  fi
done

IBM_TOKEN=$(curl -s -X POST "https://iam.cloud.ibm.com/identity/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=$IBM_API_KEY" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

IBM_EMAIL="${IBM_EMAIL:-betul.oreilly@ibm.com}"
ZEN=$(printf "ibmlhapikey_%s:%s" "$IBM_EMAIL" "$IBM_API_KEY" | base64)
ZEN="ZenApiKey $ZEN"
export ZEN_API_KEY="$ZEN"

PAYLOAD=$(python3 - "$TPL" <<'PY'
import json, sys, os
tpl = sys.argv[1]
text = open(tpl).read()
text = text.replace("__ZEN_API_KEY__", os.environ["ZEN_API_KEY"])
text = text.replace("__AWS_ACCESS_KEY_ID__", os.environ["AWS_ACCESS_KEY_ID"])
text = text.replace("__AWS_SECRET_ACCESS_KEY__", os.environ["AWS_SECRET_ACCESS_KEY"])
if "__OPENAI_API_KEY__" in text:
    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("Missing env var: OPENAI_API_KEY (required by payload placeholder __OPENAI_API_KEY__)")
    text = text.replace("__OPENAI_API_KEY__", os.environ["OPENAI_API_KEY"])
print(text)
PY
)

API="https://console-ibm-cator.lakehouse.saas.ibm.com/lakehouse/api/v3/spark_engines/${SPARK_ENGINE_ID}/applications"

RESP=$(curl -s -X POST "$API" \
  -H "Authorization: Bearer $IBM_TOKEN" \
  -H "LhInstanceId: $WXD_CRN" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

echo "$RESP" | python3 -m json.tool
APP_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))")
if [ -z "$APP_ID" ]; then
  echo "Submission failed."
  exit 1
fi
echo "APP_ID=$APP_ID"
SPARK_ENGINE_ID="$SPARK_ENGINE_ID" IBM_TOKEN="$IBM_TOKEN" WXD_CRN="$WXD_CRN" ./scripts/monitor_spark_app.sh "$APP_ID"
