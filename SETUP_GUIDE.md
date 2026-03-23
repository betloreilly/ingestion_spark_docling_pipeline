# Setup Guide (From Scratch)

This guide is the complete replication path for:
- Docling parsing on watsonx.data Spark
- embedding generation
- OpenSearch indexing

---

## 1) Prerequisites

You need:
- IBM Cloud account with access to:
  - watsonx.data (Spark + OpenSearch)
  - watsonx.ai (if using watsonx embeddings)
- AWS S3 bucket (or S3-compatible object storage)
- Local terminal with:
  - `bash`/`zsh`
  - `curl`
  - `python3`
  - `aws` CLI

---

## 2) Prepare environment variables

Copy and edit:

```bash
cp .env.example .env
```

Fill these in `.env`:
- `IBM_API_KEY`
- `WXD_CRN`
- `SPARK_ENGINE_ID` (example: `spark16`)
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `OPENAI_API_KEY` (if embeddings provider = openai)
- OpenSearch auth (`OPENSEARCH_HOST`, `OPENSEARCH_USER`, `OPENSEARCH_PASSWORD`)

Load env:

```bash
source .env
```

---

## 3) Configure pipeline YAML

Edit `config/pipeline_config_saas.yaml` placeholders:

- `source.cos_bucket`, `source.cos_endpoint`, `source.cos_region`
- `source.cos_access_key`, `source.cos_secret_key`
- `opensearch.host`, `opensearch.username`, `opensearch.password`
- `opensearch.index_name`
- `embeddings.provider` (`openai` or `watsonx`)
- embedding model and matching `opensearch.embedding_dimension`

If using OpenAI `text-embedding-3-small`:
- `embeddings.provider: openai`
- `embeddings.model: text-embedding-3-small`
- `opensearch.embedding_dimension: 1536`

---

## 4) Access watsonx.data Spark engine

From watsonx.data console:

1. Open your instance
2. Go to Infrastructure / Spark engines
3. Find the target engine ID (`spark16` etc.)
4. Ensure engine is healthy and accepts applications

This repo submits via API (through `scripts/submit_payload.sh`), not manual UI upload.

---

## 5) Upload artifacts to S3

```bash
aws s3 cp config/pipeline_config_saas.yaml s3://boreillyopensearch/config/pipeline_config_saas.yaml

cd src && zip -r ../src_bundle.zip . -x "__pycache__/*" "*.pyc" && cd ..
aws s3 cp src_bundle.zip s3://boreillyopensearch/src/src_bundle.zip
aws s3 cp src/main.py s3://boreillyopensearch/src/main.py
```

If your bucket path differs, update commands accordingly.

---

## 6) Submit Spark ingestion job

```bash
./scripts/submit_payload.sh scripts/payload_main_bootstrap.json
```

This script:
- gets IAM token from `IBM_API_KEY`
- injects placeholders into payload (`ZEN`, AWS keys, OpenAI key)
- submits to Spark applications API
- monitors state until terminal state

---

## 7) Validate logs

Read latest pipeline log from S3:

```bash
source .env
aws s3 ls s3://boreillyopensearch/logs/ --recursive | grep 'pipeline_' | grep 'part-00000' | sort -k1,1 -k2,2 | tail -1 | awk '{print $4}' > /tmp/latest_pipeline_key.txt
aws s3 cp "s3://boreillyopensearch/$(cat /tmp/latest_pipeline_key.txt)" -
```

Expected success signals:
- `PDFs found: >0`
- `chunks: >0`
- `indexed: >0`

---

## 8) Validate records in OpenSearch

Count:

```bash
source .env
curl -s -k -u "${OPENSEARCH_USER}:${OPENSEARCH_PASSWORD}" \
  -H "Content-Type: application/json" \
  "https://${OPENSEARCH_HOST}:${OPENSEARCH_PORT}/spark_demo_documents_openai/_count"
```

Sample records:

```bash
source .env
curl -s -k -u "${OPENSEARCH_USER}:${OPENSEARCH_PASSWORD}" \
  -H "Content-Type: application/json" \
  "https://${OPENSEARCH_HOST}:${OPENSEARCH_PORT}/spark_demo_documents_openai/_search" \
  -d '{"size":3,"query":{"match_all":{}}}'
```

---

## 9) Common issues and fixes

### A) `Python worker exited unexpectedly (EOFException)`
- usually worker crash / native crash / memory pressure
- use payload with faulthandler enabled (already configured)
- reduce executor parallelism and increase executor memory for debugging

### B) `model_not_supported` (watsonx embeddings)
- switch embedding provider to OpenAI, or choose supported watsonx model in your region/project

### C) `KeyError: chunk_id`
- fixed in current code by chunk normalization before index

### D) 0 records indexed although job finished
- check latest `pipeline_failed` log
- ensure OpenSearch index dimension matches embedding dimension

### E) No useful error in Spark History
- use S3 pipeline logs as source of truth (`logs/pipeline_failed_*`)

---

## 10) Minimal operational runbook

For each rerun:

1. update config if needed
2. upload config + `src_bundle.zip` + `src/main.py`
3. submit payload
4. read latest pipeline log
5. verify OpenSearch count

