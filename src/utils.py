"""
Utility functions for the PDF ingestion pipeline.

This module provides:
- Logging setup
- Configuration validation
- Checkpoint management
- Helper functions
"""

import logging
import sys
import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone
# yaml (pyyaml) is imported lazily inside functions — may not be pre-installed on Spark engine


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure logging for the pipeline.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # Convert string to logging level
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('pipeline.log')
        ]
    )
    
    # Reduce noise from third-party libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('opensearchpy').setLevel(logging.WARNING)
    logging.getLogger('ibm_watsonx_ai').setLevel(logging.WARNING)


def load_config(config_path: str, spark=None) -> Dict[str, Any]:
    """
    Load YAML configuration file from a local path or S3/COS URI.

    If a SparkSession is provided (and the path is an S3 URI), Spark's own
    S3A filesystem is used — no boto3 required. This is the preferred method
    when running on the Spark engine where S3 credentials are already set in
    the submission conf.

    Falls back to boto3 if spark is not provided.

    Args:
        config_path: Local path or S3/COS URI (s3a://, s3://, cos://)
        spark: Optional active SparkSession for S3 reads without boto3

    Returns:
        Configuration dictionary
    """
    import yaml

    path = config_path.strip()

    if path.startswith(("s3a://", "s3://", "cos://")):
        if spark is not None:
            # Use Spark's built-in S3A filesystem — no boto3 dependency
            lines = spark.sparkContext.textFile(path).collect()
            return yaml.safe_load("\n".join(lines))
        else:
            # Fallback: boto3 (local runs or when Spark is not yet initialized)
            return _load_config_from_cos_boto3(path)

    with open(path, 'r') as f:
        return yaml.safe_load(f)


def _load_config_from_cos_boto3(uri: str) -> Dict[str, Any]:
    """Load YAML from S3/COS URI using boto3 (local/fallback path)."""
    import re
    import os
    import yaml
    m = re.match(r"^(?:s3a?|cos)://([^/]+)/(.*)$", uri.strip())
    if not m:
        raise ValueError(f"Invalid COS/S3 URI: {uri}")
    bucket, key = m.group(1), m.group(2)
    endpoint = os.environ.get("COS_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint and "cos" in uri:
        endpoint = "https://s3.us-south.cloud-object-storage.appdomain.cloud"
    access_key = os.environ.get("COS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("COS_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise ValueError(
            "Loading config from COS requires COS_ACCESS_KEY_ID/COS_SECRET_ACCESS_KEY or "
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY in the environment"
        )
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise ValueError("boto3 is required to load config from COS without a SparkSession")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name=os.environ.get("COS_REGION", "us-south"),
    )
    resp = client.get_object(Bucket=bucket, Key=key)
    body = resp["Body"].read().decode("utf-8")
    return yaml.safe_load(body)


def upload_pipeline_log_to_cos(
    config: Dict[str, Any],
    status: str,
    local_log_path: str = "pipeline.log",
    fallback_error: str = ""
) -> str:
    """
    Upload local pipeline log to COS for debugging Spark jobs.

    Args:
        config: Configuration dictionary
        status: Run status label (e.g. success, failed)
        local_log_path: Local log file path to upload
        fallback_error: Extra error text when log file is missing

    Returns:
        Uploaded COS URI, or empty string if upload failed
    """
    source = config.get("source", {})
    bucket = source.get("cos_bucket")
    endpoint = source.get("cos_endpoint")
    access_key = source.get("cos_access_key")
    secret_key = source.get("cos_secret_key")
    region = source.get("cos_region", "us-south")

    if not all([bucket, endpoint, access_key, secret_key]):
        logging.warning("Skipping COS log upload: source COS credentials are incomplete")
        return ""

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_prefix = config.get("pipeline", {}).get("log_cos_prefix", "logs/spark")
    key = f"{log_prefix.rstrip('/')}/pipeline_{status}_{ts}.log"
    endpoint_url = endpoint if str(endpoint).startswith("http") else f"https://{endpoint}"

    payload = ""
    try:
        if Path(local_log_path).exists():
            payload = Path(local_log_path).read_text(encoding="utf-8", errors="replace")
    except Exception as ex:
        logging.warning(f"Failed to read local log file {local_log_path}: {ex}")

    if fallback_error:
        payload = f"{payload}\n\n--- FALLBACK ERROR ---\n{fallback_error}\n"
    if not payload:
        payload = "No local pipeline.log content available."

    try:
        import boto3
        from botocore.config import Config
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name=region,
        )
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload.encode("utf-8"),
            ContentType="text/plain",
        )
        uri = f"cos://{bucket}/{key}"
        logging.info(f"Uploaded pipeline log to {uri}")
        return uri
    except Exception as ex:
        logging.warning(f"Failed to upload pipeline log to COS: {ex}")
        return ""


def validate_config(config: Dict[str, Any]) -> None:
    """
    Validate pipeline configuration.
    
    Args:
        config: Configuration dictionary
        
    Raises:
        ValueError: If configuration is invalid
    """
    required_sections = ['spark', 'source', 'docling', 'opensearch', 'pipeline']
    
    # Check required sections
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required configuration section: {section}")
    
    # Validate embeddings provider config
    emb_cfg = config.get("embeddings", {})
    provider = emb_cfg.get("provider", "watsonx").lower()
    if provider == "watsonx":
        if "watsonx_ai" not in config:
            raise ValueError("Missing required configuration section: watsonx_ai (provider=watsonx)")
        watsonx_config = config['watsonx_ai']
        required_watsonx_fields = ['api_key', 'endpoint', 'project_id', 'embedding_model']
        for field in required_watsonx_fields:
            if field not in watsonx_config or not watsonx_config[field]:
                raise ValueError(f"Missing required watsonx.ai configuration: {field}")
            if isinstance(watsonx_config[field], str) and watsonx_config[field].startswith('YOUR_'):
                raise ValueError(f"Please configure watsonx.ai {field} in config file")
    elif provider == "openai":
        model = emb_cfg.get("model", "text-embedding-3-small")
        if not model:
            raise ValueError("embeddings.model is required for provider=openai")
    else:
        raise ValueError(f"Unsupported embeddings.provider: {provider}")
    
    # Validate OpenSearch config
    opensearch_config = config['opensearch']
    required_opensearch_fields = ['host', 'port', 'username', 'password', 'index_name']
    for field in required_opensearch_fields:
        if field not in opensearch_config or not opensearch_config[field]:
            raise ValueError(f"Missing required OpenSearch configuration: {field}")
        if isinstance(opensearch_config[field], str) and opensearch_config[field].startswith('YOUR_'):
            raise ValueError(f"Please configure OpenSearch {field} in config file")
    
    # Validate source path (only for local storage mode)
    source_config = config['source']
    if source_config.get('storage_type', 'cos') == 'local':
        source_path = source_config.get('pdf_path', '')
        if not source_path:
            raise ValueError("Missing 'pdf_path' in source config for local storage mode")
        if not Path(source_path).exists():
            raise ValueError(f"Source PDF path does not exist: {source_path}")
    
    logging.info("Configuration validation passed")


def create_checkpoint(checkpoint_dir: str, data: Dict[str, Any]) -> None:
    """
    Create a checkpoint file for fault tolerance.
    
    Args:
        checkpoint_dir: Directory for checkpoint files
        data: Data to checkpoint
    """
    checkpoint_path = Path(checkpoint_dir)
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    
    checkpoint_file = checkpoint_path / f"checkpoint_{data.get('batch_id', 'unknown')}.json"
    
    with open(checkpoint_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    logging.debug(f"Checkpoint created: {checkpoint_file}")


def load_checkpoint(checkpoint_dir: str, batch_id: str) -> Dict[str, Any]:
    """
    Load checkpoint data.
    
    Args:
        checkpoint_dir: Directory for checkpoint files
        batch_id: Batch identifier
        
    Returns:
        Checkpoint data or empty dict if not found
    """
    checkpoint_file = Path(checkpoint_dir) / f"checkpoint_{batch_id}.json"
    
    if not checkpoint_file.exists():
        return {}
    
    with open(checkpoint_file, 'r') as f:
        return json.load(f)


def calculate_chunk_statistics(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate statistics for chunks.
    
    Args:
        chunks: List of chunk dictionaries
        
    Returns:
        Statistics dictionary
    """
    if not chunks:
        return {
            'total_chunks': 0,
            'avg_length': 0,
            'min_length': 0,
            'max_length': 0
        }
    
    lengths = [len(chunk['chunk_text']) for chunk in chunks]
    
    return {
        'total_chunks': len(chunks),
        'avg_length': sum(lengths) / len(lengths),
        'min_length': min(lengths),
        'max_length': max(lengths),
        'total_characters': sum(lengths)
    }


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def sanitize_text(text: str, max_length: int = None) -> str:
    """
    Sanitize text for indexing.
    
    Args:
        text: Input text
        max_length: Maximum length (optional)
        
    Returns:
        Sanitized text
    """
    # Remove null bytes and control characters
    text = text.replace('\x00', '')
    text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
    
    # Truncate if needed
    if max_length and len(text) > max_length:
        text = text[:max_length]
    
    return text.strip()


def batch_iterator(items: List[Any], batch_size: int):
    """
    Iterate over items in batches.
    
    Args:
        items: List of items
        batch_size: Size of each batch
        
    Yields:
        Batches of items
    """
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def merge_metadata(base_metadata: Dict[str, Any], additional_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge metadata dictionaries.
    
    Args:
        base_metadata: Base metadata
        additional_metadata: Additional metadata to merge
        
    Returns:
        Merged metadata
    """
    merged = base_metadata.copy()
    merged.update(additional_metadata)
    return merged


def validate_embedding_dimension(embeddings: List[List[float]], expected_dim: int) -> bool:
    """
    Validate embedding dimensions.
    
    Args:
        embeddings: List of embedding vectors
        expected_dim: Expected dimension
        
    Returns:
        True if all embeddings have correct dimension
    """
    for emb in embeddings:
        if len(emb) != expected_dim:
            return False
    return True


def get_document_type(filename: str) -> str:
    """
    Determine document type from filename.
    
    Args:
        filename: Document filename
        
    Returns:
        Document type
    """
    extension = Path(filename).suffix.lower()
    
    type_mapping = {
        '.pdf': 'pdf',
        '.docx': 'word',
        '.doc': 'word',
        '.txt': 'text',
        '.md': 'markdown',
        '.html': 'html',
        '.htm': 'html'
    }
    
    return type_mapping.get(extension, 'unknown')

# Made with Bob
