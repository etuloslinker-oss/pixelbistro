// Vercel serverless function: GET /api/save?id=<32 hex>  and  POST /api/save?id=<32 hex>
//
// Works with either kind of Redis that Vercel can give you:
//   1. REDIS_URL                          (standard redis:// connection string)  -> uses the "redis" package
//   2. KV_REST_API_URL + KV_REST_API_TOKEN, or
//      UPSTASH_REDIS_REST_URL + UPSTASH_REDIS_REST_TOKEN  (Upstash REST)        -> plain fetch, no package needed
const REST_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REST_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const REDIS_URL = process.env.REDIS_URL;

const TTL = 60 * 60 * 24 * 365; // saves expire after 1 year without being written
const MAX_BYTES = 100000;

let client = null; // reused between requests while the function stays warm

async function viaRest(cmd) {
  const r = await fetch(REST_URL, {
    method: 'POST',
    headers: { Authorization: `Bearer ${REST_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd),
  });
  if (!r.ok) throw new Error('redis rest ' + r.status);
  return (await r.json()).result;
}

async function viaTcp(cmd) {
  const { createClient } = require('redis');
  if (!client) {
    client = createClient({ url: REDIS_URL, socket: { connectTimeout: 5000 } });
    client.on('error', () => {}); // avoid crashing on dropped connections; the next request reconnects
  }
  if (!client.isOpen) await client.connect();
  return client.sendCommand(cmd);
}

function redis(cmd) {
  return REST_URL && REST_TOKEN ? viaRest(cmd) : viaTcp(cmd);
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  const id = String((req.query && req.query.id) || '');
  if (!/^[a-f0-9]{32}$/.test(id)) return res.status(400).json({ error: 'bad id' });
  if (!(REST_URL && REST_TOKEN) && !REDIS_URL) return res.status(500).json({ error: 'storage not configured' });

  const key = 'pixelbistro:' + id;
  try {
    if (req.method === 'GET') {
      const v = await redis(['GET', key]);
      return res.status(200).json({ data: v || null });
    }

    if (req.method === 'POST') {
      let body = req.body;
      if (typeof body !== 'string') body = JSON.stringify(body || {});
      if (body.length > MAX_BYTES) return res.status(413).json({ error: 'too large' });

      let o;
      try { o = JSON.parse(body); } catch (e) { return res.status(400).json({ error: 'bad json' }); }
      if (!o || !Array.isArray(o.tables) || !Array.isArray(o.staff) || typeof o.ts !== 'number') {
        return res.status(400).json({ error: 'not a save' });
      }

      await redis(['SET', key, body, 'EX', String(TTL)]);
      return res.status(200).json({ ok: true, ts: o.ts });
    }

    res.setHeader('Allow', 'GET, POST');
    return res.status(405).end();
  } catch (e) {
    return res.status(502).json({ error: 'storage error' });
  }
};
