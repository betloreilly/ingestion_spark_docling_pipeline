"""
OpenSearch indexing module.

This module handles:
- OpenSearch connection and authentication
- Index creation with proper mappings for text and vectors
- Bulk indexing operations
- Search and validation queries
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
# opensearch-py imported lazily inside __init__ — may not be pre-installed on Spark engine


class OpenSearchIndexer:
    """Handles OpenSearch indexing operations."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize OpenSearch client.
        
        Args:
            config: OpenSearch configuration dictionary
        """
        from opensearchpy import OpenSearch, helpers
        from opensearchpy.exceptions import RequestError
        self._OpenSearch = OpenSearch
        self._helpers = helpers
        self._RequestError = RequestError

        self.config = config
        self.logger = logging.getLogger(__name__)

        # Initialize OpenSearch client
        self.client = OpenSearch(
            hosts=[{
                'host': config['host'],
                'port': config['port']
            }],
            http_auth=(config['username'], config['password']),
            use_ssl=config.get('use_ssl', True),
            verify_certs=config.get('verify_certs', True),
            ssl_show_warn=False,
            timeout=config.get('bulk_timeout', 300)
        )
        
        self.index_name = config['index_name']
        self.bulk_size = config.get('bulk_size', 500)
        
        # Test connection
        try:
            info = self.client.info()
            self.logger.info(f"Connected to OpenSearch cluster: {info['cluster_name']}")
        except Exception as e:
            self.logger.error(f"Failed to connect to OpenSearch: {e}")
            raise
    
    def create_index_if_not_exists(self) -> bool:
        """
        Create index with proper mappings if it doesn't exist.
        
        Returns:
            True if index was created, False if it already exists
        """
        if self.client.indices.exists(index=self.index_name):
            self.logger.info(f"Index '{self.index_name}' already exists")
            return False
        
        # Define index mappings
        index_body = {
            "settings": {
                "index": {
                    "number_of_shards": self.config.get('number_of_shards', 2),
                    "number_of_replicas": self.config.get('number_of_replicas', 1),
                    "knn": True,
                    "knn.algo_param.ef_search": 512
                }
            },
            "mappings": {
                "properties": {
                    "chunk_id": {
                        "type": "keyword"
                    },
                    "document_id": {
                        "type": "keyword"
                    },
                    "document_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "chunk_text": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "chunk_embedding": {
                        "type": "knn_vector",
                        "dimension": self.config.get('embedding_dimension', 768),
                        "method": {
                            "name": self.config.get('knn_algo', 'hnsw'),
                            "space_type": self.config.get('knn_space_type', 'cosinesimil'),
                            "engine": self.config.get('knn_engine', 'lucene'),
                            "parameters": {
                                "ef_construction": self.config.get('knn_ef_construction', 512),
                                "m": self.config.get('knn_m', 16)
                            }
                        }
                    },
                    "page_number": {
                        "type": "integer"
                    },
                    "section_title": {
                        "type": "text"
                    },
                    "chunk_index": {
                        "type": "integer"
                    },
                    "metadata": {
                        "type": "object",
                        "enabled": True
                    },
                    "timestamp": {
                        "type": "date"
                    }
                }
            }
        }
        
        try:
            self.client.indices.create(index=self.index_name, body=index_body)
            self.logger.info(f"Created index '{self.index_name}' with KNN support")
            return True
        except self._RequestError as e:
            self.logger.error(f"Failed to create index: {e}")
            raise
    
    def bulk_index(self, documents: List[Any]) -> int:
        """
        Bulk index documents to OpenSearch.
        
        Args:
            documents: List of document dictionaries or Row objects
            
        Returns:
            Number of successfully indexed documents
        """
        if not documents:
            self.logger.warning("No documents to index")
            return 0
        
        # Prepare documents for bulk indexing
        actions = []
        for doc in documents:
            # Convert Spark Row to dict if needed
            if hasattr(doc, 'asDict'):
                doc_dict = doc.asDict()
            else:
                doc_dict = doc
            
            # Add timestamp
            doc_dict['timestamp'] = datetime.utcnow().isoformat()
            
            # Create bulk action
            action = {
                "_index": self.index_name,
                "_id": doc_dict['chunk_id'],
                "_source": doc_dict
            }
            actions.append(action)
        
        # Perform bulk indexing
        try:
            success_count = 0
            error_count = 0
            
            # Use helpers.bulk for efficient bulk indexing
            for success, info in self._helpers.parallel_bulk(
                self.client,
                actions,
                chunk_size=self.bulk_size,
                raise_on_error=False
            ):
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    self.logger.warning(f"Failed to index document: {info}")
            
            self.logger.info(
                f"Bulk indexing complete: {success_count} successful, {error_count} failed"
            )
            return success_count
            
        except Exception as e:
            self.logger.error(f"Bulk indexing failed: {e}")
            raise
    
    def search_by_text(
        self,
        query: str,
        size: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform keyword search.
        
        Args:
            query: Search query text
            size: Number of results to return
            filters: Optional filters (e.g., document_id, page_number)
            
        Returns:
            List of search results
        """
        search_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "chunk_text": query
                            }
                        }
                    ]
                }
            },
            "size": size
        }
        
        # Add filters if provided
        if filters:
            filter_clauses = []
            for field, value in filters.items():
                filter_clauses.append({"term": {field: value}})
            search_body["query"]["bool"]["filter"] = filter_clauses
        
        try:
            response = self.client.search(index=self.index_name, body=search_body)
            return [hit["_source"] for hit in response["hits"]["hits"]]
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            return []
    
    def search_by_vector(
        self,
        query_vector: List[float],
        size: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Perform vector similarity search.
        
        Args:
            query_vector: Query embedding vector
            size: Number of results to return
            filters: Optional filters
            
        Returns:
            List of search results with similarity scores
        """
        search_body = {
            "query": {
                "knn": {
                    "chunk_embedding": {
                        "vector": query_vector,
                        "k": size
                    }
                }
            },
            "size": size
        }
        
        # Add filters if provided
        if filters:
            filter_clauses = []
            for field, value in filters.items():
                filter_clauses.append({"term": {field: value}})
            search_body["query"]["knn"]["chunk_embedding"]["filter"] = {
                "bool": {"must": filter_clauses}
            }
        
        try:
            response = self.client.search(index=self.index_name, body=search_body)
            results = []
            for hit in response["hits"]["hits"]:
                result = hit["_source"]
                result["_score"] = hit["_score"]
                results.append(result)
            return results
        except Exception as e:
            self.logger.error(f"Vector search failed: {e}")
            return []
    
    def hybrid_search(
        self,
        query_text: str,
        query_vector: List[float],
        size: int = 10,
        text_weight: float = 0.5,
        vector_weight: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining keyword and vector search.
        
        Args:
            query_text: Text query
            query_vector: Query embedding vector
            size: Number of results
            text_weight: Weight for text search (0-1)
            vector_weight: Weight for vector search (0-1)
            
        Returns:
            List of search results
        """
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "chunk_text": {
                                    "query": query_text,
                                    "boost": text_weight
                                }
                            }
                        },
                        {
                            "knn": {
                                "chunk_embedding": {
                                    "vector": query_vector,
                                    "k": size,
                                    "boost": vector_weight
                                }
                            }
                        }
                    ]
                }
            },
            "size": size
        }
        
        try:
            response = self.client.search(index=self.index_name, body=search_body)
            results = []
            for hit in response["hits"]["hits"]:
                result = hit["_source"]
                result["_score"] = hit["_score"]
                results.append(result)
            return results
        except Exception as e:
            self.logger.error(f"Hybrid search failed: {e}")
            return []
    
    def get_index_stats(self) -> Dict[str, Any]:
        """
        Get index statistics.
        
        Returns:
            Dictionary with index statistics
        """
        try:
            stats = self.client.indices.stats(index=self.index_name)
            count = self.client.count(index=self.index_name)
            
            return {
                "index_name": self.index_name,
                "document_count": count["count"],
                "size_in_bytes": stats["_all"]["total"]["store"]["size_in_bytes"],
                "number_of_shards": stats["_shards"]["total"],
                "status": "healthy" if stats["_shards"]["failed"] == 0 else "degraded"
            }
        except Exception as e:
            self.logger.error(f"Failed to get index stats: {e}")
            return {}
    
    def delete_index(self) -> bool:
        """
        Delete the index.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.client.indices.exists(index=self.index_name):
                self.client.indices.delete(index=self.index_name)
                self.logger.info(f"Deleted index '{self.index_name}'")
                return True
            else:
                self.logger.warning(f"Index '{self.index_name}' does not exist")
                return False
        except Exception as e:
            self.logger.error(f"Failed to delete index: {e}")
            return False

# Made with Bob
