import logging
import os
import json
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException

# --- IMPORTS ---
# Ensure pipeline.py exists and exports DataTransformer and dead_letter_path
from pipeline import DataTransformer, dead_letter_path
from embedding_v3 import EmbeddingModel
from vectordb_v3 import VectorDatabase

# --- CONFIG & LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CapitolPipeline")

app = FastAPI(title="Capitol-AI")
COLLECTION_NAME = "capitol_assessment"


@app.get("/")
def read_root():
    return {"status": "System is live", "docs_url": "/docs"}


# ==============================================================================
# 1. TRANSFORM ENDPOINT (Robust)
# ==============================================================================
@app.post("/pipeline/transform", response_model=List[Dict[str, Any]])
def api_transform_data(raw_data: List[Any]):  # 1. Use List[Any] to accept mixed types
    try:
        transformer = DataTransformer()
        valid_docs: List[Dict[str, Any]] = []
        # seen_ids = set()
        valid_docs_map = {}


        # 2. Basic Type Check: Is the whole body a list?
        if not isinstance(raw_data, list):
            raise HTTPException(status_code=400, detail="Body must be a JSON array")

        logger.info(f"Transforming {len(raw_data)} items...")

        # Ensure dead-letter directory exists
        output_dir = os.path.dirname(dead_letter_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        for idx, doc in enumerate(raw_data):
            # 3. PER-ITEM GUARD: Skip garbage (strings, ints, nulls)
            if not isinstance(doc, dict):
                logger.warning(f"⚠️ Skipping non-dict item at index {idx}: {doc}")
                
                # Optional: Write garbage to dead letter queue
                with open(dead_letter_path, "a", encoding="utf-8") as dl:
                    dl.write(json.dumps({"id": f"INVALID_TYPE_{idx}", "error": "Not a dictionary", "raw": doc}) + "\n")
                continue

            # ✅ Safe to use dictionary methods now
            doc_id = doc.get('_id')

            # # Deduplicate
            # if doc_id in seen_ids:
            #     continue
            # if doc_id:
            #     seen_ids.add(doc_id)

            # Process
            result, report = transformer.process_document(doc)

            if result:
                # valid_docs.append(result)

                ext_id = result.get('metadata', {}).get('external_id')
                if ext_id:
                    if ext_id in valid_docs_map:
                        logger.info(f"   🔄 UPDATING: Overwriting existing record for ID {ext_id}")
                    
                    # This line handles both Insert (new key) and Update (overwrite value)
                    valid_docs_map[ext_id] = result
                
            else:
                # Log failed validations
                with open(dead_letter_path, "a", encoding="utf-8") as dl:
                    record = {
                        "id": doc_id or f"UNKNOWN_{idx}",
                        "reason": report.get("reason", "unknown"),
                        "raw_doc": doc,
                    }
                    dl.write(json.dumps(record, ensure_ascii=False) + "\n")

        valid_docs = list(valid_docs_map.values())
        return valid_docs

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# 2. EMBED ENDPOINT (Robust)
# ==============================================================================
@app.post("/pipeline/embed", response_model=List[Dict[str, Any]])
def api_embed_documents(processed_docs: List[Any]): # 1. Use List[Any]
    try:
        if not isinstance(processed_docs, list):
             raise HTTPException(status_code=400, detail="Input must be a list")

        embedder = EmbeddingModel()
        embedded_docs = []

        logger.info(f"Embedding {len(processed_docs)} items...")

        for idx, doc in enumerate(processed_docs):
            # 2. Guard against garbage
            if not isinstance(doc, dict):
                logger.warning(f"Skipping non-dict item at index {idx} in embed endpoint")
                continue

            text = doc.get("text", "")
            if not text:
                continue

            try:
                vector = embedder.generate_embedding(text)
                doc["vector"] = vector
                embedded_docs.append(doc)
            except Exception as e:
                logger.error(f"Failed to embed doc index {idx}: {e}")
                continue

        return embedded_docs

    except Exception as e:
        logger.error(f"Embedding failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# 3. INDEX ENDPOINT (Robust)
# ==============================================================================
@app.post("/pipeline/index")
def api_index_documents(embedded_docs: List[Any]): # 1. Use List[Any]
    try:
        if not isinstance(embedded_docs, list):
             raise HTTPException(status_code=400, detail="Input must be a list")

        # 2. Filter valid docs immediately
        valid_inputs = [d for d in embedded_docs if isinstance(d, dict) and d.get("vector")]

        if not valid_inputs:
            return {"indexed": 0, "message": "No valid documents with vectors found"}

        vector_db = VectorDatabase(collection_name=COLLECTION_NAME)

        # Check vector size from first valid doc
        sample_vector = valid_inputs[0].get("vector")
        vector_size = len(sample_vector) if sample_vector else 384

        vector_db.get_or_create_collection(vector_size=vector_size)
        vector_db.upsert_documents(valid_inputs)

        return {"indexed": len(valid_inputs)}

    except Exception as e:
        logger.error(f"Indexing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==============================================================================
# 4. FULL PIPELINE (Robust)
# ==============================================================================
@app.post("/pipeline/run_full")
def api_run_full_pipeline(raw_data: List[Any]): # 1. Use List[Any]
    try:
        # --- STAGE 1: TRANSFORM ---
        transformer = DataTransformer()
        clean_docs = []
        # seen_ids = set()
        valid_docs_map = {}

        if not isinstance(raw_data, list):
             raise HTTPException(status_code=400, detail="Input must be a list")

        # Ensure dead-letter directory exists
        output_dir = os.path.dirname(dead_letter_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        for idx, doc in enumerate(raw_data):
            # 2. Guard against garbage
            if not isinstance(doc, dict):
                logger.warning(f"RunFull: Skipping non-dict item at index {idx}")
                continue

            doc_id = doc.get('_id')
            # if doc_id in seen_ids: continue
            # if doc_id: seen_ids.add(doc_id)

            res, report = transformer.process_document(doc)
            if res:
                # clean_docs.append(res)

                ext_id = res.get('metadata', {}).get('external_id')
                if ext_id:
                    if ext_id in valid_docs_map:
                        logger.info(f"   🔄 UPDATING: Overwriting existing record for ID {ext_id}")
                    
                    valid_docs_map[ext_id] = res
                
            else:
                # Log failures
                with open(dead_letter_path, "a", encoding="utf-8") as dl:
                    record = {
                        "id": doc_id or f"UNKNOWN_{idx}",
                        "reason": report.get("reason", "unknown"),
                        "raw_doc": doc,
                    }
                    dl.write(json.dumps(record, ensure_ascii=False) + "\n")

        clean_docs = list(valid_docs_map.values())
        
        if not clean_docs:
            return {"processed": 0, "indexed": 0}

        # --- STAGE 2: EMBED ---
        embedder = EmbeddingModel()
        embedded_docs = []
        for doc in clean_docs:
            try:
                doc["vector"] = embedder.generate_embedding(doc.get("text", ""))
                embedded_docs.append(doc)
            except Exception as e:
                logger.error(f"RunFull: Embedding failed for doc {doc.get('external_id')}: {e}")
                continue

        # --- STAGE 3: INDEX ---
        if embedded_docs:
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
# SEARCH ENDPOINT (Safe)
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


