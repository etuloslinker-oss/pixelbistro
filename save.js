// Vercel serverless function:
//   GET  /api/save?id=<32 hex>                      -> { data }
//   POST /api/save?id=<32 hex>&sid=<session>        -> write the save (only the session that holds the lock may write)
//   POST /api/save?id=...&sid=...&action=claim      -> take the lock ("&force=1" takes it over from another session)
//   POST /api/save?id=...&sid=...&action=beat       -> keep the lock alive
//   POST /api/save?id=...&sid=...&action=release    -> give the lock up
// Only ONE session can run a given save code at a time. The lock is a short-lived Redis key that
// the running session keeps refreshing; if it stops (tab closed, crash) it expires by itself.
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
const LOCK_TTL = 30; // seconds; the client refreshes it about every 10s

// Lua keeps the check-and-set atomic.
const LUA_CLAIM = "local c=redis.call('GET',KEYS[1]); if (not c) or c==ARGV[1] or ARGV[3]=='1' then redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2]); return 1 end; return 0";
const LUA_REFRESH = "local c=redis.call('GET',KEYS[1]); if c==ARGV[1] then redis.call('EXPIRE',KEYS[1],ARGV[2]); return 1 end; return 0";
const LUA_RELEASE = "local c=redis.call('GET',KEYS[1]); if c==ARGV[1] then redis.call('DEL',KEYS[1]); return 1 end; return 0";
// Save only if this session holds (or can freely take) the lock; refreshes the lock when it writes.
const LUA_SAVE = "local c=redis.call('GET',KEYS[1]); if c and c~=ARGV[1] then return 0 end; redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2]); redis.call('SET',KEYS[2],ARGV[3],'EX',ARGV[4]); return 1";

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
  const lockKey = 'pixelbistro:lock:' + id;
  const sid = String((req.query && req.query.sid) || '');
  const action = String((req.query && req.query.action) || '');
  const sidOk = /^[a-f0-9]{16,64}$/.test(sid);
  const ev = (lua, keys, args) => redis(['EVAL', lua, String(keys.length)].concat(keys, args));
  try {
    if (req.method === 'GET') {
      const v = await redis(['GET', key]);
      return res.status(200).json({ data: v || null });
    }

    if (req.method === 'POST') {
      if (!sidOk) return res.status(400).json({ error: 'bad session' });

      if (action === 'claim') {
        const force = req.query.force === '1' ? '1' : '0';
        const ok = await ev(LUA_CLAIM, [lockKey], [sid, String(LOCK_TTL), force]);
        return res.status(200).json({ ok: ok === 1, busy: ok !== 1 });
      }
      if (action === 'beat') {
        const ok = await ev(LUA_REFRESH, [lockKey], [sid, String(LOCK_TTL)]);
        return ok === 1 ? res.status(200).json({ ok: true }) : res.status(409).json({ error: 'locked' });
      }
      if (action === 'release') {
        await ev(LUA_RELEASE, [lockKey], [sid]);
        return res.status(200).json({ ok: true });
      }

      let body = req.body;
      if (typeof body !== 'string') body = JSON.stringify(body || {});
      if (body.length > MAX_BYTES) return res.status(413).json({ error: 'too large' });

      let o;
      try { o = JSON.parse(body); } catch (e) { return res.status(400).json({ error: 'bad json' }); }
      if (!o || !Array.isArray(o.tables) || !Array.isArray(o.staff) || typeof o.ts !== 'number') {
        return res.status(400).json({ error: 'not a save' });
      }

      const ok = await ev(LUA_SAVE, [lockKey, key], [sid, String(LOCK_TTL), body, String(TTL)]);
      if (ok !== 1) return res.status(409).json({ error: 'locked' });
      return res.status(200).json({ ok: true, ts: o.ts });
    }

    res.setHeader('Allow', 'GET, POST');
    return res.status(405).end();
  } catch (e) {
    return res.status(502).json({ error: 'storage error' });
  }
};
