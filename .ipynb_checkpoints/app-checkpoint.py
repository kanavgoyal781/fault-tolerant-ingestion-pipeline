import logging
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException

# --- IMPORTS ---
# Matches the filenames currently on your Render instance
from pipeline import DataTransformer
from embeddings_V2 import EmbeddingModel
from vector_db_V2 import VectorDatabase

# --- CONFIG & LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CapitolPipeline")

app = FastAPI(title="Modular RAG Pipeline (Stateless)")
COLLECTION_NAME = "capitol_assessment"

# --- ROOT ENDPOINT ---
@app.get("/")
def read_root():
    return {"status": "System is live", "docs_url": "/docs"}

# ==============================================================================
# 1. TRANSFORM ENDPOINT
# Returns: List[Dict] (The cleaned documents, exactly as they would be in the file)
# ==============================================================================
@app.post("/pipeline/transform", response_model=List[Dict[str, Any]])
def api_transform_data(raw_data: List[Dict[str, Any]]):
    try:
        transformer = DataTransformer()
        valid_docs = []
        seen_ids = set()

        logger.info(f"Transforming {len(raw_data)} documents...")

        for doc in raw_data:
            doc_id = doc.get('_id')
            if doc_id in seen_ids: 
                continue
            if doc_id: 
                seen_ids.add(doc_id)

            result, _ = transformer.process_document(doc)
            if result:
                valid_docs.append(result)

        # RETURN RAW LIST (No wrapper)
        return valid_docs

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# 2. EMBED ENDPOINT
# Returns: List[Dict] (The documents with 'vector' added)
# ==============================================================================
@app.post("/pipeline/embed", response_model=List[Dict[str, Any]])
def api_embed_documents(processed_docs: List[Dict[str, Any]]):
    try:
        embedder = EmbeddingModel()
        embedded_docs = []

        logger.info(f"Embedding {len(processed_docs)} documents...")

        for doc in processed_docs:
            text = doc.get("text", "")
            if not text:
                continue
            
            vector = embedder.generate_embedding(text)
            doc["vector"] = vector
            embedded_docs.append(doc)

        # RETURN RAW LIST (No wrapper)
        return embedded_docs

    except Exception as e:
        logger.error(f"Embedding failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# 3. INDEX ENDPOINT
# Returns: Minimal status (since indexing has no data output)
# ==============================================================================
@app.post("/pipeline/index")
def api_index_documents(embedded_docs: List[Dict[str, Any]]):
    try:
        if not embedded_docs:
            return {"indexed": 0}

        vector_db = VectorDatabase(collection_name=COLLECTION_NAME)
        
        # Check vector size from first doc
        sample_vector = embedded_docs[0].get("vector")
        vector_size = len(sample_vector) if sample_vector else 384
        
        vector_db.get_or_create_collection(vector_size=vector_size)
        vector_db.upsert_documents(embedded_docs)

        return {"indexed": len(embedded_docs)}

    except Exception as e:
        logger.error(f"Indexing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# 4. FULL PIPELINE
# Returns: Minimal status summary
# ==============================================================================
@app.post("/pipeline/run_full")
def api_run_full_pipeline(raw_data: List[Dict[str, Any]]):
    try:
        # 1. Transform
        transformer = DataTransformer()
        clean_docs = []
        seen_ids = set()
        for doc in raw_data:
            doc_id = doc.get('_id')
            if doc_id in seen_ids: continue
            if doc_id: seen_ids.add(doc_id)
            
            res, _ = transformer.process_document(doc)
            if res: clean_docs.append(res)
        
        if not clean_docs:
            return {"processed": 0, "indexed": 0}

        # 2. Embed
        embedder = EmbeddingModel()
        embedded_docs = []
        for doc in clean_docs:
            doc["vector"] = embedder.generate_embedding(doc.get("text", ""))
            embedded_docs.append(doc)

        # 3. Index
        vector_db = VectorDatabase(collection_name=COLLECTION_NAME)
        vector_db.get_or_create_collection(vector_size=len(embedded_docs[0]["vector"]))
        vector_db.upsert_documents(embedded_docs)

        return {
            "processed": len(clean_docs),
            "indexed": len(embedded_docs)
        }

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# SEARCH ENDPOINT
# ==============================================================================
@app.get("/search")
def api_search(query: str, k: int = 3):
    try:
        embedder = EmbeddingModel()
        query_vector = embedder.generate_embedding(query)
        
        vector_db = VectorDatabase(collection_name=COLLECTION_NAME)
        results = vector_db.search(query_vector, limit=k)
        
        return results
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)