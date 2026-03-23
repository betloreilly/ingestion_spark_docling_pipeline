"""
Main Spark Pipeline for PDF Ingestion with Docling.

IMPORTANT: Only pyspark.* imports are allowed at module level.
The watsonx.data launcher pre-validates the file and rejects any
non-PySpark top-level imports or statements.
All other imports and module-level code are deferred inside functions.
"""

from pyspark.sql import SparkSession

# Module-level singleton caches — one instance per executor Python process.
# Docling's DocumentConverter loads ML models on first init; caching here
# means models load once per executor, not once per PDF.
_PDF_PROCESSOR_CACHE = {}
_EMBEDDINGS_CACHE = {}


def _get_pdf_processor(config):
    """Return (or create) a cached PDFProcessor for this executor process."""
    if "inst" not in _PDF_PROCESSOR_CACHE:
        from pdf_processor import PDFProcessor
        _PDF_PROCESSOR_CACHE["inst"] = PDFProcessor(config)
    return _PDF_PROCESSOR_CACHE["inst"]


def _get_embeddings_client(config):
    """Return (or create) a cached embeddings client for this executor process."""
    if "inst" not in _EMBEDDINGS_CACHE:
        from embeddings import WatsonxEmbeddings
        _EMBEDDINGS_CACHE["inst"] = WatsonxEmbeddings(config)
    return _EMBEDDINGS_CACHE["inst"]


def create_spark_session(config):
    import logging
    logger = logging.getLogger(__name__)

    spark_conf = config['spark']
    source_conf = config['source']

    master = spark_conf.get('master') or ""
    if master and (master.startswith("http://") or master.startswith("https://")):
        logger.warning(
            "Spark 'master' looks like an API URL – ignoring. "
            "Leave master unset when submitting to watsonx.data."
        )
        master = ""

    builder = SparkSession.builder.appName(spark_conf['app_name'])
    if master:
        builder = builder.master(master)

    spark = (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", source_conf['cos_endpoint'])
        .config("spark.hadoop.fs.s3a.access.key", source_conf['cos_access_key'])
        .config("spark.hadoop.fs.s3a.secret.key", source_conf['cos_secret_key'])
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    logger.info(f"Spark session created: {spark.sparkContext.applicationId}")
    return spark


def process_pdf_batch(pdf_items, config):
    """Runs on Spark executors — all imports deferred."""
    import logging
    import os
    import tempfile
    from datetime import datetime

    logger = logging.getLogger(__name__)
    # Singletons: models load once per executor process, reused for all PDFs.
    pdf_processor = _get_pdf_processor(config)
    embeddings_client = _get_embeddings_client(config)

    all_chunks = []
    for item in pdf_items:
        pdf_path = item.get("path")
        pdf_content = item.get("content")
        try:
            logger.info(f"Processing PDF: {pdf_path}")

            if pdf_content is None:
                raise ValueError(f"Missing binary content for {pdf_path}")

            # Docling parser needs a local file path on executor.
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(bytes(pdf_content))
                local_pdf = tmp.name

            try:
                doc_result = pdf_processor.parse_pdf(local_pdf)
            finally:
                try:
                    os.unlink(local_pdf)
                except Exception:
                    pass

            chunks = pdf_processor.chunk_document(doc_result)
            logger.info(f"Created {len(chunks)} chunks from {pdf_path}")

            for chunk in chunks:
                entities = pdf_processor.extract_entities(chunk['chunk_text'])
                chunk['entities'] = entities
                chunk['entity_count'] = sum(len(v) for v in entities.values())

            texts = [c['chunk_text'] for c in chunks]
            embeddings = embeddings_client.generate_embeddings(texts)

            for chunk, embedding in zip(chunks, embeddings):
                chunk['chunk_embedding'] = embedding
                chunk['processed_at'] = datetime.utcnow().isoformat()
                all_chunks.append(chunk)

            logger.info(f"Successfully processed {pdf_path}: {len(chunks)} chunks")
        except Exception as e:
            # Fail fast so the real executor error surfaces in Spark logs.
            # Silent continue makes the job look "successful" with 0 chunks.
            logger.error(f"Error processing {pdf_path}: {str(e)}", exc_info=True)
            raise RuntimeError(f"PDF processing failed for {pdf_path}: {type(e).__name__}: {e}") from e

    return all_chunks


def _write_log_to_s3(spark, lines, label="log"):
    """Write a list of log lines to S3 using Spark so we can read them later."""
    import datetime
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    path = f"s3a://boreillyopensearch/logs/pipeline_{label}_{ts}"
    try:
        spark.sparkContext.parallelize(lines, 1).saveAsTextFile(path)
    except Exception as ex:
        print(f"[LOG WRITE FAILED] {ex}", flush=True)


def _preflight_dependency_check():
    """
    Validate critical third-party deps up front.
    This prevents expensive Spark task retries when a package is missing.
    """
    missing = []
    for mod in ("docling", "opensearchpy"):
        try:
            __import__(mod)
        except Exception:
            missing.append(mod)
    if missing:
        raise ModuleNotFoundError(
            "Missing required Python packages on Spark runtime: "
            + ", ".join(missing)
            + ". Install them on the Spark engine image or run Docling outside Spark."
        )
    # Docling's transitive stack is heavy and sensitive to transformer/accelerate
    # mismatches; force an actual converter import so incompatibilities fail early.
    try:
        from docling.document_converter import DocumentConverter  # noqa: F401
    except Exception as e:
        raise ImportError(
            "Docling dependency stack import failed. "
            "This usually means incompatible transformers/accelerate versions. "
            f"Original error: {e}"
        ) from e


def _log_dependency_versions(step):
    """Print dependency versions and import paths for diagnostics."""
    import importlib
    for mod in ("docling", "transformers", "accelerate", "opensearchpy"):
        try:
            m = importlib.import_module(mod)
            ver = getattr(m, "__version__", "unknown")
            path = getattr(m, "__file__", "n/a")
            step(f"dep {mod}: version={ver} path={path}")
        except Exception as e:
            step(f"dep {mod}: IMPORT FAILED ({type(e).__name__}: {e})")


def _prepend_user_site_to_syspath(step):
    """Ensure pip-installed user site-packages are searched before system paths."""
    import os
    import sys
    import site
    candidates = []
    try:
        candidates.extend(site.getsitepackages())
    except Exception:
        pass
    try:
        candidates.append(site.getusersitepackages())
    except Exception:
        pass
    extra = [
        "/home/spark/.local/lib/python3.11/site-packages",
        "/home/spark/.local/lib/python/site-packages",
    ]
    for p in extra:
        if p not in candidates:
            candidates.append(p)
    moved = []
    for p in candidates:
        if p and os.path.isdir(p):
            if p in sys.path:
                sys.path.remove(p)
            sys.path.insert(0, p)
            moved.append(p)
    step(f"user site precedence applied: {moved}")


def _normalize_docs_for_index(rows):
    """Flatten Spark rows and ensure required index fields exist."""
    import hashlib
    normalized = []
    for i, row in enumerate(rows):
        d = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
        # explode(chunks) as chunk -> each row is {"chunk": {...}}
        if "chunk" in d and isinstance(d["chunk"], dict):
            d = d["chunk"]
        # Ensure required fields expected by indexer/mapping
        d.setdefault("chunk_text", "")
        d.setdefault("document_name", "unknown")
        d.setdefault("page_number", 1)
        d.setdefault("section_title", "Unknown")
        d.setdefault("chunk_index", i)
        # Generate deterministic chunk_id if missing
        if not d.get("chunk_id"):
            base = f"{d.get('document_name','unknown')}|{d.get('page_number',1)}|{d.get('chunk_index',i)}|{d.get('chunk_text','')[:80]}"
            d["chunk_id"] = hashlib.md5(base.encode("utf-8")).hexdigest()
        normalized.append(d)
    return normalized


def main():
    import argparse
    import logging
    import traceback

    def step(msg):
        print(f"[PIPELINE] {msg}", flush=True)

    step("main() started")

    parser = argparse.ArgumentParser(description='PDF Ingestion Pipeline with Docling')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    args = parser.parse_args()
    step(f"args parsed: config={args.config}")

    step("creating SparkSession...")
    spark = SparkSession.builder.appName("pdf-ingestion-pipeline").getOrCreate()
    logger = logging.getLogger(__name__)
    step(f"SparkSession OK: {spark.sparkContext.applicationId}  Spark {spark.version}")

    log_lines = [f"[PIPELINE] SparkSession: {spark.sparkContext.applicationId}"]
    config = None

    try:
        step("importing utils...")
        from utils import setup_logging, load_config, validate_config, upload_pipeline_log_to_cos
        step("utils OK")

        step("importing opensearch_indexer...")
        from opensearch_indexer import OpenSearchIndexer
        step("opensearch_indexer OK")

        step("running dependency preflight...")
        try:
            _preflight_dependency_check()
            step("dependency preflight OK")
        except Exception as dep_err:
            step(f"dependency preflight failed: {type(dep_err).__name__}: {dep_err}")
            step("attempting bootstrap install on driver + executors...")
            from bootstrap import install_on_driver, broadcast_install
            install_on_driver()
            broadcast_install(spark)
            _prepend_user_site_to_syspath(step)
            step("bootstrap install completed, re-checking dependencies...")
            _preflight_dependency_check()
            step("dependency preflight OK after bootstrap")
        _log_dependency_versions(step)

        step(f"loading config from {args.config} ...")
        config = load_config(args.config, spark=spark)
        step(f"config loaded: sections={list(config.keys())}")
        log_lines.append(f"[PIPELINE] config loaded: {list(config.keys())}")

        setup_logging(config['pipeline']['log_level'])
        logger.info("Logging configured")

        step("validating config...")
        validate_config(config)
        step("config validated OK")

        src = config['source']
        spark.conf.set("spark.hadoop.fs.s3a.endpoint",   src['cos_endpoint'])
        spark.conf.set("spark.hadoop.fs.s3a.access.key", src['cos_access_key'])
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", src['cos_secret_key'])
        step("S3 conf updated on SparkSession")

        prefix = config['source'].get('cos_prefix', '')
        cos_path = f"s3a://{config['source']['cos_bucket']}/{prefix}*.pdf"
        step(f"reading PDFs from {cos_path} ...")

        pdf_df = spark.read.format("binaryFile").load(cos_path)
        pdf_count = pdf_df.count()
        step(f"found {pdf_count} PDF files")
        log_lines.append(f"[PIPELINE] PDFs found: {pdf_count}")

        if pdf_count == 0:
            step("no PDFs found — exiting cleanly")
            _write_log_to_s3(spark, log_lines + ["[PIPELINE] DONE: no PDFs"], "finished")
            return

        step("importing pyspark UDF types...")
        from pyspark.sql.functions import udf, col
        from pyspark.sql.types import (
            StructType, StructField, StringType,
            ArrayType, IntegerType, MapType, FloatType
        )
        step("pyspark UDF types OK")

        step("processing PDFs on executors via UDF...")
        chunk_schema = StructType([
            StructField("chunk_id",        StringType(),                                   False),
            StructField("chunk_text",      StringType(),                                   False),
            StructField("chunk_embedding", ArrayType(FloatType()),                         False),
            StructField("document_name",   StringType(),                                   False),
            StructField("page_number",     IntegerType(),                                  False),
            StructField("section_title",   StringType(),                                   True),
            StructField("chunk_index",     IntegerType(),                                  False),
            StructField("entities",        MapType(StringType(), ArrayType(StringType())), False),
            StructField("entity_count",    IntegerType(),                                  False),
            StructField("word_count",      IntegerType(),                                  False),
            StructField("processed_at",    StringType(),                                   False),
        ])

        process_pdf_udf = udf(
            lambda path, content: process_pdf_batch([{"path": path, "content": content}], config),
            ArrayType(chunk_schema)
        )

        chunks_df = (
            pdf_df
            .withColumn("chunks", process_pdf_udf(col("path"), col("content")))
            .select("chunks")
            .selectExpr("explode(chunks) as chunk")
        )

        # Broadcast config to executors so they can connect to OpenSearch directly.
        # This avoids collecting all chunks to the driver — the key scalability fix.
        config_bc = spark.sparkContext.broadcast(config)
        indexed_acc = spark.sparkContext.accumulator(0)
        chunks_acc = spark.sparkContext.accumulator(0)

        # Create index once on the driver to avoid concurrent-creation races
        # when multiple executor partitions start simultaneously.
        step("creating OpenSearch index on driver...")
        indexer_driver = OpenSearchIndexer(config['opensearch'])
        indexer_driver.create_index_if_not_exists()

        def _index_partition(rows):
            """Runs on each executor partition: normalize + bulk-index to OpenSearch."""
            import hashlib
            from opensearch_indexer import OpenSearchIndexer as _OSI
            cfg = config_bc.value
            indexer = _OSI(cfg['opensearch'])
            docs = []
            for i, row in enumerate(rows):
                d = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
                if "chunk" in d and isinstance(d["chunk"], dict):
                    d = d["chunk"]
                d.setdefault("chunk_text", "")
                d.setdefault("document_name", "unknown")
                d.setdefault("page_number", 1)
                d.setdefault("section_title", "Unknown")
                d.setdefault("chunk_index", i)
                if not d.get("chunk_id"):
                    base = (
                        f"{d.get('document_name','unknown')}|"
                        f"{d.get('page_number',1)}|"
                        f"{d.get('chunk_index',i)}|"
                        f"{d.get('chunk_text','')[:80]}"
                    )
                    d["chunk_id"] = hashlib.md5(base.encode("utf-8")).hexdigest()
                docs.append(d)
            chunks_acc.add(len(docs))
            if docs:
                n = indexer.bulk_index(docs)
                indexed_acc.add(n)

        step("indexing chunks from executors via foreachPartition...")
        chunks_df.foreachPartition(_index_partition)

        total_chunks = chunks_acc.value
        success_count = indexed_acc.value
        step(f"processed {total_chunks} chunks from {pdf_count} PDFs")
        log_lines.append(f"[PIPELINE] chunks: {total_chunks}")

        if total_chunks == 0:
            step("no chunks produced — exiting")
            _write_log_to_s3(spark, log_lines + ["[PIPELINE] DONE: 0 chunks"], "finished")
            return

        step(f"indexed {success_count}/{total_chunks} chunks total")
        log_lines.append(f"[PIPELINE] indexed: {success_count}/{total_chunks}")

        index_stats = indexer_driver.get_index_stats()
        step(f"OpenSearch doc count: {index_stats.get('document_count', 0)}")

        step("=" * 60)
        step("PIPELINE COMPLETED SUCCESSFULLY")
        step(f"  PDFs:    {pdf_count}")
        step(f"  Chunks:  {total_chunks}")
        step(f"  Indexed: {success_count}")
        step("=" * 60)

        log_lines.append("[PIPELINE] STATUS: SUCCESS")
        _write_log_to_s3(spark, log_lines, "success")

    except Exception as e:
        err = traceback.format_exc()
        step(f"PIPELINE FAILED: {type(e).__name__}: {e}")
        step(err)
        log_lines.append(f"[PIPELINE] FAILED: {type(e).__name__}: {e}")
        log_lines.append(err)
        _write_log_to_s3(spark, log_lines, "failed")
        raise

    finally:
        spark.stop()
        step("SparkSession stopped")


if __name__ == "__main__":
    main()
