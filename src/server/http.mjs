// HTTP routing and authentication. Every route except /health requires a key
// (Authorization: Bearer, ?key=, or cookie comms_key). The sender is always
// derived from the key, never from the request body (P-041, P-047).
import { HttpError } from '../common/util.mjs';
import { renderPage, renderError } from './page.mjs';

const MAX_BODY = 256 * 1024;

export function createHandler(service, store) {
  return async function handle(req, res) {
    const url = new URL(req.url, 'http://comms.local');
    try {
      if (url.pathname === '/health' && req.method === 'GET') return sendJson(res, 200, service.health());

      // Body is read before authentication so that HTML forms may carry the key.
      const body = req.method === 'POST' ? await readBody(req) : null;
      const key = findKey(req, url) || (body && body.__form === true && body.key) || null;
      const sender = store.senderForKey(key);
      if (!sender) {
        if (url.pathname === '/' && req.method === 'GET') {
          return sendHtml(res, 401, renderError(401, 'Chýba alebo je neplatný kľúč. Otvorte stránku s parametrom ?key=<kľúč>.'));
        }
        throw new HttpError(401, 'chýba alebo neplatný kľúč');
      }

      const route = `${req.method} ${url.pathname}`;
      switch (route) {
        case 'GET /': {
          if (url.searchParams.get('key')) {
            res.setHeader('Set-Cookie', `comms_key=${encodeURIComponent(key)}; HttpOnly; SameSite=Strict; Path=/`);
          }
          const showAll = url.searchParams.get('all') === '1';
          const notices = service.notices({ all: showAll });
          const pending = store.messages((m) => m.state === 'pending').sort((a, b) => a.created_at.localeCompare(b.created_at));
          return sendHtml(res, 200, renderPage({ instance: service.instance, notices, pending, key: url.searchParams.get('key'), showAll }));
        }
        case 'POST /send':
          return sendJson(res, 201, service.send(sender, body));
        case 'GET /receive':
          return sendJson(res, 200, { messages: service.receive(queryOf(url)) });
        case 'GET /count':
          return sendJson(res, 200, service.count(queryOf(url)));
        case 'POST /ack':
          return sendJson(res, 200, service.ack(body.ids));
        case 'GET /notices':
          return sendJson(res, 200, { notices: service.notices({ all: url.searchParams.get('all') === '1' }) });
        case 'POST /decide': {
          const isForm = body.__form === true;
          try {
            const out = service.decide(body);
            if (isForm) return redirect(res, body.key ? `/?key=${encodeURIComponent(body.key)}` : '/');
            return sendJson(res, 200, out);
          } catch (e) {
            if (isForm && e instanceof HttpError) return sendHtml(res, e.status, renderError(e.status, e.message, body.key ? `/?key=${encodeURIComponent(body.key)}` : '/'));
            throw e;
          }
        }
        case 'POST /register-topic':
          return sendJson(res, 200, service.registerTopic(body, sender));
        case 'GET /message':
          return sendJson(res, 200, service.messageState(url.searchParams.get('id')));
        case 'POST /bounce':
          return sendJson(res, 200, service.bounce(body));
        case 'POST /turn':
          return sendJson(res, 200, service.turn(body));
        case 'GET /turns':
          return sendJson(res, 200, { turns: service.turns({ open: url.searchParams.get('open') === '1' }) });
        case 'POST /notice':
          return sendJson(res, 201, service.createExternalNotice(sender, body));
        case 'GET /events': {
          // since=latest: no events, only the current sequence number (start of a fresh wake module).
          if (url.searchParams.get('since') === 'latest') return sendJson(res, 200, { events: [], next: service.lastSeq });
          const since = Number.parseInt(url.searchParams.get('since') || '0', 10) || 0;
          const wait = Number.parseInt(url.searchParams.get('wait') || '0', 10) || 0;
          const events = await service.eventsSince(since, wait);
          return sendJson(res, 200, { events, next: events.length ? events[events.length - 1].seq : since });
        }
        default:
          throw new HttpError(404, `neznáma operácia ${route}`);
      }
    } catch (e) {
      if (e instanceof HttpError) return sendJson(res, e.status, { error: e.message, ...e.extra });
      service.log(`error: ${e.stack || e}`);
      return sendJson(res, 500, { error: 'vnútorná chyba služby' });
    }
  };
}

function queryOf(url) {
  const p = url.searchParams;
  const ids = p.get('ids') ? p.get('ids').split(',').filter(Boolean) : null;
  return { project: p.get('project') || null, topic: p.get('topic') || null, thread: p.get('thread') || null, persona: p.get('persona') || null, ids };
}

function findKey(req, url) {
  const auth = req.headers.authorization || '';
  if (auth.toLowerCase().startsWith('bearer ')) return auth.slice(7).trim();
  if (url.searchParams.get('key')) return url.searchParams.get('key');
  const cookie = req.headers.cookie || '';
  const m = /(?:^|;\s*)comms_key=([^;]+)/.exec(cookie);
  if (m) return decodeURIComponent(m[1]);
  return null;
}

async function readBody(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY) throw new HttpError(413, 'telo požiadavky je príliš veľké');
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString('utf8');
  const type = (req.headers['content-type'] || '').split(';')[0].trim();
  if (type === 'application/x-www-form-urlencoded') {
    const out = { __form: true };
    for (const [k, v] of new URLSearchParams(raw)) out[k] = v;
    return out;
  }
  if (!raw.trim()) return {};
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error();
    return parsed;
  } catch {
    throw new HttpError(400, 'telo požiadavky musí byť JSON objekt');
  }
}

function sendJson(res, status, data) {
  const body = JSON.stringify(data);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}

function sendHtml(res, status, html) {
  res.writeHead(status, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
}

function redirect(res, location) {
  res.writeHead(303, { Location: location });
  res.end();
}
