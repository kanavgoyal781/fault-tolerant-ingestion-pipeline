#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import logging
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.http import models

# Configure Logging
logger = logging.getLogger("CapitolPipeline")

class VectorDatabase:
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        
        # 1. Connect to Qdrant
        # If QDRANT_HOST is set (for Cloud/Render), use it. Otherwise, default to memory/local.
        self.host = os.getenv("QDRANT_HOST")
        self.api_key = os.getenv("QDRANT_API_KEY")
        
        if self.host:
            self.client = QdrantClient(url=self.host, api_key=self.api_key)
            logger.info(f"✅ Connected to Qdrant at {self.host}")
        else:
            # Fallback for local testing if no host provided
            logger.warning("⚠️ No QDRANT_HOST set. Using in-memory Qdrant for testing.")
            self.client = QdrantClient(":memory:")

    def get_or_create_collection(self, vector_size: int = 1536):
        """
        Ensures the collection exists. If not, creates it.
        """
        if not self.client.collection_exists(self.collection_name):
            logger.info(f"Creating collection '{self.collection_name}' with size {vector_size}...")
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=vector_size,
                    distance=models.Distance.COSINE
                ),
            )
        else:
            logger.info(f"Collection '{self.collection_name}' already exists.")

    def upsert_documents(self, docs: List[Dict[str, Any]]):
        """
        Uploads a list of documents (with 'vector' and 'text' fields) to Qdrant.
        """
        points = []
        for i, doc in enumerate(docs):
            vector = doc.get("vector")
            text = doc.get("text")
            
            # Basic Validation
            if not vector or not text:
                continue
                
            # Create Payload (Metadata + Text)
            # Remove 'vector' from payload to save space (it's stored as the vector itself)
            payload = doc.copy()
            if "vector" in payload:
                del payload["vector"]
            
            # Create Point
            point = models.PointStruct(
                id=i,  # Simple integer ID (in prod, use UUIDs)
                vector=vector,
                payload=payload
            )
            points.append(point)

        # Batch Upload
        if points:
            self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )
            logger.info(f"✅ Successfully upserted {len(points)} points.")

    def search(self, query_vector: List[float], limit: int = 3):
        """
        Searches the collection using a query vector.
        """
        if not self.client.collection_exists(self.collection_name):
            logger.warning("Collection does not exist. Returning empty results.")
            return []

        search_result = self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            limit=limit
        )
        
        # Convert Qdrant Points to clean dictionaries
        results = []
        for hit in search_result:
            results.append({
                "score": hit.score,
                "payload": hit.payload
            })
            
        return results

# Helper class for search results (optional, keeps type safety)
class SearchResult:
    def __init__(self, score, title, snippet, url):
        self.score = score
        self.title = title
        self.snippet = snippet
        self.url = url

