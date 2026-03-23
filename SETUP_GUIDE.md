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

If using watsonx embeddings, set:
- `embeddings.provider: watsonx`
- `watsonx_ai.api_key: <IBM Cloud IAM API key>`
- `watsonx_ai.endpoint: <regional watsonx endpoint>`
- `watsonx_ai.project_id: <watsonx project GUID>`
- `watsonx_ai.embedding_model: <model available in your region/project>`
- `opensearch.embedding_dimension: <must match embedding model output>`

---

## 3.1) watsonx.ai API key and project setup (detailed)

Use this section if embeddings provider is `watsonx`.

1) Create IBM Cloud API key
- Open IBM Cloud console.
- Navigate to **Manage -> Access (IAM) -> API keys**.
- Click **Create** and save the key securely.
- Put it in `.env` as `IBM_API_KEY` and in config as `watsonx_ai.api_key` (or template it from env during your deployment process).

2) Create/open watsonx project
- Open watsonx.ai and create a project (or open existing).
- Confirm the project has access to a Watson Machine Learning service instance.
- Keep project and endpoint in the same region.

3) Get `project_id`
- In project details/settings, copy the project GUID.
- Set `watsonx_ai.project_id` to that exact GUID.

4) Set the correct regional endpoint
- Example endpoints:
  - `https://us-south.ml.cloud.ibm.com`
  - `https://ca-tor.ml.cloud.ibm.com`
  - `https://eu-de.ml.cloud.ibm.com`
- Set `watsonx_ai.endpoint` to the endpoint matching your project region.

5) Pick a supported embedding model
- Set `watsonx_ai.embedding_model` to a model your project can access in that region.
- If you receive `model_not_supported`, switch to a model available in your region/project.

6) Match OpenSearch vector dimension
- Set `opensearch.embedding_dimension` to the exact output size of your embedding model.
- If dimensions do not match, indexing fails even if Spark processing succeeds.

7) Validate with a small run first
- Use a small PDF set and low executor count.
- Check logs for:
  - embeddings client initialization success
  - non-zero chunk and index counts

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

Entity extraction note:
- Entity extraction is currently regex-based baseline.
- Production alternatives: spaCy, GLiNER, transformer NER, or LLM schema extraction.

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

## 10) Scaling up for large PDF volumes

Before scaling from a small test to production:

1. **Executor count** — increase `spark.executor.instances` in `scripts/payload_main_bootstrap.json`.
   - Default is 4. Increase based on your PDF volume and engine tier.

2. **OCR** — `docling.do_ocr` in config is `false` by default.
   - Enable only if PDFs are scanned / image-only. OCR adds 3–10x processing time.

3. **Embedding rate limits** — OpenAI `text-embedding-3-small` has per-minute token limits.
   - If you have thousands of chunks, reduce `spark.executor.instances` or add retry logic in `src/embeddings.py`.

4. **OpenSearch bulk size** — `opensearch.bulk_size` controls how many docs per bulk request per partition.
   - Default 500 is safe. Raise to 1000 if your OpenSearch cluster is well-resourced.

5. **Re-ingest behaviour** — chunks are upserted by `chunk_id` (MD5 of document + page + text).
   - Same file re-ingested = same IDs = existing docs overwritten (no duplicates).
   - If a PDF shrinks between runs, old extra chunks remain. Delete by `document_name` first if needed.

---

## 11) Minimal operational runbook

For each rerun:

1. update config if needed
2. upload config + `src_bundle.zip` + `src/main.py`
3. submit payload
4. read latest pipeline log
5. verify OpenSearch count

