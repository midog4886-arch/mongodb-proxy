from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId
import os
from datetime import datetime, timezone

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MONGO_URL = os.environ.get("MONGO_URL", "")
PROXY_API_KEY = os.environ.get("PROXY_API_KEY", "champions-proxy-key-2024")
_client = None

def get_client():
    global _client
    if not MONGO_URL:
        raise Exception("MONGO_URL environment variable is not set")
    if _client is None:
        _client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=15000)
    return _client

def parse_ejson(value):
    if isinstance(value, dict):
        if "$oid" in value:
            try:
                return ObjectId(value["$oid"])
            except Exception:
                return value["$oid"]
        if "$date" in value:
            d = value["$date"]
            if isinstance(d, dict) and "$numberLong" in d:
                return datetime.fromtimestamp(int(d["$numberLong"]) / 1000, tz=timezone.utc).replace(tzinfo=None)
            if isinstance(d, (int, float)):
                return datetime.fromtimestamp(d / 1000, tz=timezone.utc).replace(tzinfo=None)
            return d
        if "$numberLong" in value:
            return int(value["$numberLong"])
        if "$numberInt" in value:
            return int(value["$numberInt"])
        if "$numberDouble" in value:
            return float(value["$numberDouble"])
        return {k: parse_ejson(v) for k, v in value.items()}
    if isinstance(value, list):
        return [parse_ejson(item) for item in value]
    return value

def to_ejson(value):
    if isinstance(value, ObjectId):
        return {"$oid": str(value)}
    if isinstance(value, datetime):
        return {"$date": {"$numberLong": str(int(value.timestamp() * 1000))}}
    if isinstance(value, dict):
        return {k: to_ejson(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_ejson(item) for item in value]
    return value

def clean_doc(doc):
    if doc is None:
        return None
    doc.pop("_id", None)
    return doc

@app.get("/health")
async def health():
    return {"status": "ok", "mongo_configured": bool(MONGO_URL), "version": "1.1"}

@app.post("/action/{action}")
async def handle_action(action: str, request: Request, api_key: str = Header(None)):
    if api_key != PROXY_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    body = await request.json()
    db_name = body.get("database", "champions_academy")
    col_name = body.get("collection", "")
    try:
        client = get_client()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database not configured: {e}")
    col = client[db_name][col_name]
    try:
        if action == "findOne":
            filter_ = parse_ejson(body.get("filter", {}))
            doc = col.find_one(filter_, body.get("projection"))
            clean_doc(doc)
            return {"document": to_ejson(doc) if doc else None}
        elif action == "find":
            filter_ = parse_ejson(body.get("filter", {}))
            sort = body.get("sort")
            cursor = col.find(filter_, body.get("projection")).skip(body.get("skip", 0)).limit(body.get("limit", 0))
            if sort:
                cursor = cursor.sort([(k, ASCENDING if v == 1 else DESCENDING) for k, v in sort.items()])
            docs = list(cursor)
            for doc in docs:
                clean_doc(doc)
            return {"documents": [to_ejson(d) for d in docs]}
        elif action == "insertOne":
            doc = parse_ejson(body.get("document", {}))
            doc.pop("_id", None)
            result = col.insert_one(doc)
            return {"insertedId": str(result.inserted_id)}
        elif action == "insertMany":
            docs = [parse_ejson(d) for d in body.get("documents", [])]
            for d in docs:
                d.pop("_id", None)
            result = col.insert_many(docs)
            return {"insertedIds": [str(i) for i in result.inserted_ids]}
        elif action == "updateOne":
            filter_ = parse_ejson(body.get("filter", {}))
            update = parse_ejson(body.get("update", {}))
            result = col.update_one(filter_, update, upsert=body.get("upsert", False))
            return {"matchedCount": result.matched_count, "modifiedCount": result.modified_count, "upsertedId": str(result.upserted_id) if result.upserted_id else None}
        elif action == "updateMany":
            filter_ = parse_ejson(body.get("filter", {}))
            update = parse_ejson(body.get("update", {}))
            result = col.update_many(filter_, update, upsert=body.get("upsert", False))
            return {"matchedCount": result.matched_count, "modifiedCount": result.modified_count}
        elif action == "deleteOne":
            return {"deletedCount": col.delete_one(parse_ejson(body.get("filter", {}))).deleted_count}
        elif action == "deleteMany":
            return {"deletedCount": col.delete_many(parse_ejson(body.get("filter", {}))).deleted_count}
        elif action == "aggregate":
            pipeline = [parse_ejson(stage) for stage in body.get("pipeline", [])]
            docs = list(col.aggregate(pipeline))
            for doc in docs:
                clean_doc(doc)
            return {"documents": [to_ejson(d) for d in docs]}
        else:
            raise HTTPException(status_code=400, detail=f"Unknown action: {action}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
