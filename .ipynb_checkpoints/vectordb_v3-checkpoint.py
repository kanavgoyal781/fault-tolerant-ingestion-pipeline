# #!/usr/bin/env python
# # coding: utf-8

# # In[ ]:


# import os
# import logging
# from typing import List, Dict, Any
# from qdrant_client import QdrantClient
# from qdrant_client.http import models

# # Configure Logging
# logger = logging.getLogger("CapitolPipeline")

# class VectorDatabase:
#     def __init__(self, collection_name: str):
#         self.collection_name = collection_name
        
#         # 1. Connect to Qdrant
#         # If QDRANT_HOST is set (for Cloud/Render), use it. Otherwise, default to memory/local.
#         self.host = os.getenv("QDRANT_HOST")
#         self.api_key = os.getenv("QDRANT_API_KEY")
        
#         # if self.host:
#         # try:
#         # self.client = QdrantClient(url=self.host, api_key=self.api_key)
#         self.client = QdrantClient(host="localhost", port=6333)
#         logger.info(f"✅ Connected to Qdrant at {self.host}")
#         # else:
#             # # Fallback for local testing if no host provided
#             # logger.warning("⚠️ No QDRANT_HOST set. Using in-memory Qdrant for testing.")
#             # self.client = QdrantClient(":memory:")

#     def get_or_create_collection(self, vector_size: int = 1536):
#         """
#         Ensures the collection exists. If not, creates it.
#         """
#         if not self.client.collection_exists(self.collection_name):
#             logger.info(f"Creating collection '{self.collection_name}' with size {vector_size}...")
#             self.client.create_collection(
#                 collection_name=self.collection_name,
#                 vectors_config=models.VectorParams(
#                     size=vector_size,
#                     distance=models.Distance.COSINE
#                 ),
#             )
#         else:
#             logger.info(f"Collection '{self.collection_name}' already exists.")

#     def upsert_documents(self, docs: List[Dict[str, Any]]):
#         """
#         Uploads a list of documents (with 'vector' and 'text' fields) to Qdrant.
#         """
#         points = []
#         for i, doc in enumerate(docs):
#             vector = doc.get("vector")
#             text = doc.get("text")
            
#             # Basic Validation
#             if not vector or not text:
#                 continue
                
#             # Create Payload (Metadata + Text)
#             # Remove 'vector' from payload to save space (it's stored as the vector itself)
#             payload = doc.copy()
#             if "vector" in payload:
#                 del payload["vector"]
            
#             # Create Point
#             point = models.PointStruct(
#                 id=i,  # Simple integer ID (in prod, use UUIDs)
#                 vector=vector,
#                 payload=payload
#             )
#             points.append(point)

#         # Batch Upload
#         if points:
#             self.client.upsert(
#                 collection_name=self.collection_name,
#                 points=points
#             )
#             logger.info(f"✅ Successfully upserted {len(points)} points.")

#     def search(self, query_vector: List[float], limit: int = 3):
#         """
#         Searches the collection using a query vector.
#         """
#         if not self.client.collection_exists(self.collection_name):
#             logger.warning("Collection does not exist. Returning empty results.")
#             return []

#         search_result = self.client.search(
#             collection_name=self.collection_name,
#             query_vector=query_vector,
#             limit=limit
#         )
        
#         # Convert Qdrant Points to clean dictionaries
#         results = []
#         for hit in search_result:
#             results.append({
#                 "score": hit.score,
#                 "payload": hit.payload
#             })
            
#         return results

# # Helper class for search results (optional, keeps type safety)
# class SearchResult:
#     def __init__(self, score, title, snippet, url):
#         self.score = score
#         self.title = title
#         self.snippet = snippet
#         self.url = url



# import os
# import logging
# from typing import List, Dict, Any
# from qdrant_client import QdrantClient
# from qdrant_client.http import models
# from pydantic import BaseModel

# # Configure Logging
# logger = logging.getLogger("CapitolPipeline")

# # --- Shared Pydantic Model (Exactly as in your original code) ---
# class SearchResult(BaseModel):
#     score: float
#     title: str
#     snippet: str
#     url: str

# class VectorDatabase:
#     def __init__(self, collection_name: str):
#         self.collection_name = collection_name
        
#         # 1. Connect (Logic from your get_qdrant_client)
#         self.host = os.getenv("QDRANT_HOST")
#         self.api_key = os.getenv("QDRANT_API_KEY")
        
#         # if self.host:
#             # self.client = QdrantClient(url=self.host, api_key=self.api_key)
#         self.client = QdrantClient(host="localhost", port=6333)
#         logger.info(f"✅ Connected to Qdrant at {self.host}")
#         # else:
#             # Default to localhost as per your original code
#             # self.client = QdrantClient(host="localhost", port=6333)
#             # logger.info("✅ Connected to local Qdrant")

#     def get_or_create_collection(self, vector_size: int = 1536):
#         """
#         Recreates collection to ensure fresh state (Idempotent).
#         """
#         if self.client.collection_exists(self.collection_name):
#             self.client.delete_collection(self.collection_name)

#         self.client.create_collection(
#             collection_name=self.collection_name,
#             vectors_config=models.VectorParams(
#                 size=vector_size,
#                 distance=models.Distance.COSINE,
#             ),
#         )
#         logger.info(f"Created/Reset collection '{self.collection_name}'")

#     def upsert_documents(self, docs: List[Dict[str, Any]]):
#         """
#         Uploads documents to Qdrant.
#         """
#         points = []
#         for i, doc in enumerate(docs):
#             # Support both 'vector' (app.py) and 'embedding' (your old script) keys
#             vector = doc.get("vector") or doc.get("embedding")
            
#             if not vector:
#                 continue

#             # Create Payload (Metadata + Text)
#             # Remove vector from payload to save space
#             payload = doc.copy()
#             if "vector" in payload: del payload["vector"]
#             if "embedding" in payload: del payload["embedding"]
            
#             # Create Point
#             point = models.PointStruct(
#                 id=i, 
#                 vector=vector,
#                 payload=payload
#             )
#             points.append(point)

#         if points:
#             self.client.upsert(
#                 collection_name=self.collection_name, 
#                 points=points
#             )
#             logger.info(f"✅ Uploaded {len(points)} points to collection '{self.collection_name}'")

#     def search(self, query_vector: List[float], limit: int = 3):
#         """
#         Searches using query_points (Exactly as in your original code).
#         """
#         if not self.client.collection_exists(self.collection_name):
#             logger.warning("Collection does not exist.")
#             return []

#         # Using query_points as requested
#         results = self.client.query_points(
#             collection_name=self.collection_name,
#             query=query_vector,
#             limit=limit,
#         )
        
#         hits = results.points or []
#         formatted_results = []
        
#         for hit in hits:
#             payload = hit.payload or {}
#             formatted_results.append(SearchResult(
#                 score=hit.score,
#                 title=payload.get("title", "No Title"),
#                 snippet=payload.get("text", "")[:500] + "...", 
#                 url=payload.get("url", "No URL")
#             ))
            
#         return formatted_results


# import os
# import logging
# from typing import List, Dict, Any
# from qdrant_client import QdrantClient
# from qdrant_client.http import models
# from pydantic import BaseModel

# # Configure Logging
# logger = logging.getLogger("CapitolPipeline")

# # --- Shared Pydantic Model (Exactly as in your original code) ---
# class SearchResult(BaseModel):
#     score: float
#     title: str
#     snippet: str
#     url: str

# class VectorDatabase:
#     def __init__(self, collection_name: str):
#         self.collection_name = collection_name
        
#         # 1. Connect (Logic from your get_qdrant_client)
#         self.host = os.getenv("QDRANT_HOST")
#         self.api_key = os.getenv("QDRANT_API_KEY")
        
#         self.client = QdrantClient(host="localhost", port=6333)
#         self.client = QdrantClient(url=self.host, api_key=self.api_key)

#     def get_or_create_collection(self, vector_size: int = 1536):
#         """
#         Recreates collection to ensure fresh state (Idempotent).
#         """
#         if self.client.collection_exists(self.collection_name):
#             self.client.delete_collection(self.collection_name)

#         self.client.create_collection(
#             collection_name=self.collection_name,
#             vectors_config=models.VectorParams(
#                 size=vector_size,
#                 distance=models.Distance.COSINE,
#             ),
#         )
#         logger.info(f"Created/Reset collection '{self.collection_name}'")

#     def upsert_documents(self, docs: List[Dict[str, Any]]):
#         """
#         Uploads documents to Qdrant.
#         """
#         points = []
#         for i, doc in enumerate(docs):
#             # Support both 'vector' (app.py) and 'embedding' (your old script) keys
#             vector = doc.get("vector") or doc.get("embedding")
#             text = doc.get("text")
            
#             if not vector or not text:
#                 continue

#             # --- FIX: Flatten Metadata (Match Original Logic) ---
#             # Instead of keeping metadata nested, we unpack it into the root payload.
#             metadata = doc.get("metadata", {})
            
#             # Construct payload: text + all metadata fields at the top level
#             payload = {"text": text, **metadata}
            
#             # Create Point
#             point = models.PointStruct(
#                 id=i, 
#                 vector=vector,
#                 payload=payload
#             )
#             points.append(point)

#         if points:
#             self.client.upsert(
#                 collection_name=self.collection_name, 
#                 points=points
#             )
#             logger.info(f"✅ Uploaded {len(points)} points to collection '{self.collection_name}'")

#     def search(self, query_vector: List[float], limit: int = 3):
#         """
#         Searches using query_points (Exactly as in your original code).
#         """
#         if not self.client.collection_exists(self.collection_name):
#             logger.warning("Collection does not exist.")
#             return []

#         # Using query_points as requested
#         results = self.client.query_points(
#             collection_name=self.collection_name,
#             query=query_vector,
#             limit=limit,
#         )
        
#         hits = results.points or []
#         formatted_results = []
        
#         for hit in hits:
#             payload = hit.payload or {}
#             # Now payload.get("title") will work because we flattened the data!
#             formatted_results.append(SearchResult(
#                 score=hit.score,
#                 title=payload.get("title", "No Title"),
#                 snippet=payload.get("text", "")[:500] + "...", 
#                 url=payload.get("url", "No URL")
#             ))
            
#         return formatted_results

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