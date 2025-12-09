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
        
        # 1. Connect
        self.host = "localhost"
        self.api_key = os.getenv("QDRANT_API_KEY")
        
        self.client = QdrantClient(host="localhost", port=6333)
        logger.info(f"✅ Connected to Qdrant at {self.host}")


    def get_or_create_collection(self, vector_size: int = 1536):
        """
        Recreates collection to ensure fresh state.
        """
        if self.client.collection_exists(self.collection_name):
            self.client.delete_collection(self.collection_name)

        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE,
            ),
        )
        logger.info(f"Created/Reset collection '{self.collection_name}'")

    def upsert_documents(self, docs: List[Dict[str, Any]]):
        """
        Uploads documents to Qdrant.
        """
        points = []
        for i, doc in enumerate(docs):
            vector = doc.get("vector") or doc.get("embedding")
            text = doc.get("text")
            metadata = doc.get("metadata", {})
            
            if not vector or not text:
                continue

            # --- CRITICAL FIX: Keep Structure Intact ---
            # We store 'metadata' as a nested object, exactly like your input JSON.
            payload = {
                "text": text,
                "metadata": metadata
            }
            
            point = models.PointStruct(
                id=i, 
                vector=vector,
                payload=payload
            )
            points.append(point)

        if points:
            self.client.upsert(
                collection_name=self.collection_name, 
                points=points
            )
            logger.info(f"✅ Uploaded {len(points)} points to collection '{self.collection_name}'")

    def search(self, query_vector: List[float], limit: int = 3):
        """
        Searches and returns the FULL document structure.
        """
        if not self.client.collection_exists(self.collection_name):
            logger.warning("Collection does not exist.")
            return []

        results = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=limit,
        )
        
        hits = results.points or []
        formatted_results = []
        
        for hit in hits:
            # Merge the score with the full original payload
            # Output format: { "score": 0.9, "text": "...", "metadata": {...} }
            full_doc = hit.payload or {}
            full_doc["score"] = hit.score
            formatted_results.append(full_doc)
            
        return formatted_results