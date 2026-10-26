// Vercel serverless function: GET /api/save?id=<32 hex>  and  POST /api/save?id=<32 hex>
// Storage: Upstash Redis (free plan). Vercel's Upstash integration sets these env vars for you.
const URL_ = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const TTL = 60 * 60 * 24 * 365; // saves expire after 1 year without being written
const MAX_BYTES = 100000;

async function redis(cmd) {
  const r = await fetch(URL_, {
    method: 'POST',
    headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd),
  });
  if (!r.ok) throw new Error('redis ' + r.status);
  return (await r.json()).result;
}

module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');

  const id = String((req.query && req.query.id) || '');
  if (!/^[a-f0-9]{32}$/.test(id)) return res.status(400).json({ error: 'bad id' });
  if (!URL_ || !TOKEN) return res.status(500).json({ error: 'storage not configured' });

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

      await redis(['SET', key, body, 'EX', TTL]);
      return res.status(200).json({ ok: true, ts: o.ts });
    }

    res.setHeader('Allow', 'GET, POST');
    return res.status(405).end();
  } catch (e) {
    return res.status(502).json({ error: 'storage error' });
  }
};
