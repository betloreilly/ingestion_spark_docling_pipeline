"""
Step 2 of the two-step pipeline: runs on watsonx.data Spark.
No docling, no ibm-watsonx-ai needed — only opensearch-py.

Reads pre-processed chunk JSON from S3 (written by extract_local.py)
and bulk-indexes them into OpenSearch.

Usage via submit_payload.sh with payload_index_only.json
"""

from pyspark.sql import SparkSession


def main():
    import argparse
    import json
    import logging

    def step(msg):
        print(f"[INDEX] {msg}", flush=True)

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--chunks-prefix", required=True,
                        help="S3 prefix where extract_local.py wrote chunk JSON files, "
                             "e.g. s3a://boreillyopensearch/chunks/20260320T130000/")
    args = parser.parse_args()

    step("Creating SparkSession...")
    spark = SparkSession.builder.appName("opensearch-index-only").getOrCreate()
    step(f"SparkSession OK: {spark.sparkContext.applicationId}")

    try:
        from utils import load_config, validate_config, setup_logging
        from opensearch_indexer import OpenSearchIndexer

        step(f"Loading config from {args.config}")
        config = load_config(args.config, spark=spark)
        setup_logging(config["pipeline"]["log_level"])
        logger = logging.getLogger(__name__)
        validate_config(config)
        step("Config OK")

        src = config["source"]
        spark.conf.set("spark.hadoop.fs.s3a.endpoint",   src["cos_endpoint"])
        spark.conf.set("spark.hadoop.fs.s3a.access.key", src["cos_access_key"])
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", src["cos_secret_key"])

        chunks_path = args.chunks_prefix.rstrip("/") + "/*.json"
        step(f"Reading chunk JSON from {chunks_path}")

        chunk_df = spark.read.option("multiLine", True).json(chunks_path)
        total = chunk_df.count()
        step(f"Loaded {total} chunks")

        if total == 0:
            step("No chunks found — exiting")
            return

        step("Indexing to OpenSearch...")
        indexer = OpenSearchIndexer(config["opensearch"])
        indexer.create_index_if_not_exists()

        chunks = [row.asDict(recursive=True) for row in chunk_df.collect()]
        batch_size = config["pipeline"]["batch_size"]
        success = 0

        for i in range(0, len(chunks), batch_size):
            batch = chunks[i : i + batch_size]
            indexed = indexer.bulk_index(batch)
            success += indexed
            step(f"Batch {i // batch_size + 1}: {indexed}/{len(batch)}")

        step(f"Indexed {success}/{total} chunks total")

        stats = indexer.get_index_stats()
        step(f"OpenSearch total docs: {stats.get('document_count', 0)}")

        step("=" * 60)
        step("INDEXING COMPLETE")
        step(f"  Chunks indexed: {success}/{total}")
        step("=" * 60)

    except Exception as e:
        import traceback
        step(f"FAILED: {type(e).__name__}: {e}")
        step(traceback.format_exc())
        raise
    finally:
        spark.stop()
        step("SparkSession stopped")


if __name__ == "__main__":
    main()
