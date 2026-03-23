"""
Embeddings generation with pluggable providers.
Supports:
- watsonx.ai
- OpenAI
"""

import logging
import os
from typing import List

logger = logging.getLogger(__name__)


class WatsonxEmbeddings:
    """
    Embeddings client for watsonx.ai
    
    Generates dense vector representations of text for semantic search.
    Uses IBM's slate-125m-english-rtrvr model (768 dimensions).
    """
    
    def __init__(self, config: dict):
        """
        Initialize watsonx.ai embeddings client
        
        Args:
            config: Configuration dictionary with watsonx.ai credentials
        """
        self.config = config
        emb_cfg = config.get("embeddings", {})
        self.provider = emb_cfg.get("provider", "watsonx").lower()

        if self.provider == "openai":
            self._init_openai(config, emb_cfg)
        else:
            self._init_watsonx(config, emb_cfg)

    def _init_watsonx(self, config: dict, emb_cfg: dict):
        from ibm_watsonx_ai import APIClient
        from ibm_watsonx_ai.foundation_models import Embeddings

        wx = config["watsonx_ai"]
        self.model_id = emb_cfg.get("model") or wx["embedding_model"]
        self.dimension = emb_cfg.get("dimension") or wx.get("embedding_dimension", 768)

        credentials = {
            "url": wx["endpoint"],
            "apikey": wx["api_key"],
        }
        self.client = APIClient(credentials)
        self.embeddings = Embeddings(
            model_id=self.model_id,
            credentials=credentials,
            project_id=wx["project_id"],
        )
        logger.info(f"Initialized watsonx embeddings with model: {self.model_id}")

    def _init_openai(self, config: dict, emb_cfg: dict):
        from openai import OpenAI

        api_key = emb_cfg.get("api_key") or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OpenAI embeddings selected but OPENAI_API_KEY is missing "
                "(set env var or embeddings.api_key in config)."
            )
        base_url = emb_cfg.get("base_url") or os.environ.get("OPENAI_BASE_URL")
        self.model_id = emb_cfg.get("model", "text-embedding-3-small")
        self.dimension = emb_cfg.get("dimension")
        self.openai_client = OpenAI(api_key=api_key, base_url=base_url)
        logger.info(f"Initialized OpenAI embeddings with model: {self.model_id}")
    
    def generate_embeddings(self, texts: List[str], batch_size: int = 10) -> List[List[float]]:
        """
        Generate embeddings for a list of texts
        
        Args:
            texts: List of text strings to embed
            batch_size: Number of texts to process in each batch
            
        Returns:
            List of embedding vectors (each is a list of 768 floats)
        """
        if not texts:
            return []
        
        logger.info(f"Generating embeddings for {len(texts)} texts")
        
        all_embeddings = []
        
        # Process in batches to avoid rate limits
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            try:
                if self.provider == "openai":
                    resp = self.openai_client.embeddings.create(
                        model=self.model_id,
                        input=batch,
                    )
                    batch_embeddings = [row.embedding for row in resp.data]
                else:
                    result = self.embeddings.embed_documents(batch)
                    batch_embeddings = [emb for emb in result]
                all_embeddings.extend(batch_embeddings)
                
                logger.debug(f"Generated embeddings for batch {i//batch_size + 1}")
                
            except Exception as e:
                logger.error(f"Error generating embeddings for batch {i//batch_size + 1}: {str(e)}")
                # Do not use zero vectors: OpenSearch cosinesimil rejects them. Re-raise so pipeline fails visibly.
                raise
        
        logger.info(f"Successfully generated {len(all_embeddings)} embeddings")
        return all_embeddings
    
    def generate_query_embedding(self, query: str) -> List[float]:
        """
        Generate embedding for a single query text
        
        Args:
            query: Query text
            
        Returns:
            Embedding vector (list of 768 floats)
        """
        try:
            if self.provider == "openai":
                resp = self.openai_client.embeddings.create(
                    model=self.model_id,
                    input=[query],
                )
                return resp.data[0].embedding
            return self.embeddings.embed_query(query)
        except Exception as e:
            logger.error(f"Error generating query embedding: {str(e)}")
            raise

# Made with Bob
