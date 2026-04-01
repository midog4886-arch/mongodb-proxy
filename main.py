from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId
import os
from datetime import datetime, timezone

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MONGO_URL = os.environ["MONGO_URL"]
PROXY_API_KEY = os.environ.get("PROXY_API_KEY", "champions-proxy-key-2024")
_client = None

def get_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=10000)
    return _client

def parse_ejson(value):
    if isinstance(value, dict):
        if "$oid" in value:
            try: return ObjectId(value["$oid"])
            except: return value["$oid"]
        if "$date" in value:
            d = value["$date"]
            if isinstance(d, dict) and "$numberLong" in d:
                return datetime.fromtimestamp(int(d["$numberLong"])/1000, tz=timezone.utc).replace(tzinfo=None)
            if isinstance(d, (int, float)):
                return datetime.fromtimestamp(d/1000, tz=timezone.utc).replace(tzinfo=None)
            return d
        if "$numberLong" in value: return int(value["$numberLong"])
        if "$numberInt" in value: return int(value["$numberInt"])
        if "$numberDouble" in value: return float(value["$numberDouble"])
        return {k: parse_ejson(v) for k, v in value.items()}
    if isinstance(value, list): return [parse_ejson(item) for item in value]
    return value

def to_ejson(value):
    if isinstance(value, ObjectId): return {"$oid": str(value)}
    if isinstance(value, datetime): return {"$date": {"$numberLong": str(int(value.timestamp()*1000))}}
    if isinstance(value, dict): return {k: to_ejson(v) for k, v in value.items()}
    if isinstance(value, list): return [to_ejson(item) for item in value]
    return value

def clean(doc):
    if doc: doc.pop("_id", None)
    return doc

@app.get("/health")
async def health(): return {"status": "ok"}

@app.post("/action/{action}")
async def handle(action: str, request: Request, api_key: str = Header(None)):
    if api_key != PROXY_API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    body = await request.json()
    col = get_client()[body.get("database","champions_academy")][body.get("collection","")]
    try:
        if action == "findOne":
            doc = col.find_one(parse_ejson(body.get("filter",{})), body.get("projection"))
            return {"document": to_ejson(clean(doc)) if doc else None}
        elif action == "find":
            f=parse_ejson(body.get("filter",{})); s=body.get("sort"); lim=body.get("limit",0); sk=body.get("skip",0)
            cur=col.find(f,body.get("projection")).skip(sk).limit(lim)
            if s: cur=cur.sort([(k,ASCENDING if v==1 else DESCENDING) for k,v in s.items()])
            docs=list(cur); [clean(d) for d in docs]
            return {"documents": [to_ejson(d) for d in docs]}
        elif action == "insertOne":
            doc=parse_ejson(body.get("document",{})); doc.pop("_id",None)
            return {"insertedId": str(col.insert_one(doc).inserted_id)}
        elif action == "insertMany":
            docs=[parse_ejson(d) for d in body.get("documents",[])]; [d.pop("_id",None) for d in docs]
            return {"insertedIds": [str(i) for i in col.insert_many(docs).inserted_ids]}
        elif action == "updateOne":
            r=col.update_one(parse_ejson(body.
