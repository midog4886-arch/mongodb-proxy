import { MongoClient } from 'mongodb';
let _client = null;
async function getClient(url) {
  if (!_client) { _client = new MongoClient(url, { serverSelectionTimeoutMS: 30000 }); await _client.connect(); }
  return _client;
}
const cors = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type, api-key' };
const strip = (doc) => { if (!doc) return null; const { _id, ...r } = doc; return r; };
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });
    if (url.pathname === '/health') return Response.json({ status: 'ok', mongo_configured: !!env.MONGO_URL, version: '5.0' }, { headers: cors });
    if (request.headers.get('api-key') !== (env.PROXY_API_KEY || 'champions-proxy-key-2024'))
      return Response.json({ detail: 'Unauthorized' }, { status: 401, headers: cors });
    const m = url.pathname.match(/^\/action\/(\w+)$/);
    if (!m) return Response.json({ detail: 'Not found' }, { status: 404, headers: cors });
    const action = m[1];
    const body = await request.json();
    try {
      const col = (await getClient(env.MONGO_URL)).db(body.database || 'champions_academy').collection(body.collection || '');
      let result;
      switch (action) {
        case 'findOne': result = { document: strip(await col.findOne(body.filter || {}, { projection: body.projection })) }; break;
        case 'find': { let cur = col.find(body.filter || {}, { projection: body.projection }).skip(body.skip || 0).limit(body.limit || 0); if (body.sort) cur = cur.sort(body.sort); result = { documents: (await cur.toArray()).map(strip) }; break; }
        case 'insertOne': { const doc = { ...body.document }; delete doc._id; result = { insertedId: (await col.insertOne(doc)).insertedId.toString() }; break; }
        case 'insertMany': { const docs = (body.documents || []).map(d => { const c={...d}; delete c._id; return c; }); const r = await col.insertMany(docs); result = { insertedIds: Object.values(r.insertedIds).map(id => id.toString()) }; break; }
        case 'updateOne': { const r = await col.updateOne(body.filter || {}, body.update || {}, { upsert: body.upsert || false }); result = { matchedCount: r.matchedCount, modifiedCount: r.modifiedCount, upsertedId: r.upsertedId?.toString() || null }; break; }
        case 'updateMany': { const r = await col.updateMany(body.filter || {}, body.update || {}, { upsert: body.upsert || false }); result = { matchedCount: r.matchedCount, modifiedCount: r.modifiedCount }; break; }
        case 'deleteOne': result = { deletedCount: (await col.deleteOne(body.filter || {})).deletedCount }; break;
        case 'deleteMany': result = { deletedCount: (await col.deleteMany(body.filter || {})).deletedCount }; break;
        case 'aggregate': { const docs = await col.aggregate(body.pipeline || []).toArray(); result = { documents: docs.map(strip) }; break; }
        default: return Response.json({ detail: `Unknown action: ${action}` }, { status: 400, headers: cors });
      }
      return Response.json(result, { headers: cors });
    } catch (err) { _client = null; return Response.json({ detail: err.message }, { status: 500, headers: cors }); }
  }
};
