// Client core of mail model 2: http.request over socketPath (or an http:// / https:// URL), instance_id and model check, start-on-demand,
// outbox for outages (writes with client_op_id), obligations with an offline backup, message count.
// Model (4a, I10): every request carries X-Comms-Model: 2; a service of another model (/health, 426) fails loudly and
// nothing is written or queued. The outbox is only for an unavailable service (P-041).
import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dataDir as resolveDataDir, instanceName, socketPath, clientDir } from '../common/paths.mjs';
import { readJson, writeJson, listJsonFiles, nowIso, newId, normalizeMachine } from '../common/util.mjs';

export const DEFAULT_TIMEOUT = 2000;
export const COUNT_TTL = 60_000;
/** Turn records (`turn`) give up quickly; only a `closed` record is queued on outage. */
export const TURN_TIMEOUT = 1000;
/** Mail model this client speaks (D-0087, D-0089); the service must report the same in /health. */
export const MAIL_MODEL = 2;
/** Writes that go into the outbox on outage; each carries a client_op_id (the service replays a repeated one). */
export const OUTBOX_OPS = ['/send', '/resolve', '/withdraw'];
const BIN = fileURLToPath(new URL('../../bin/comms.mjs', import.meta.url));
/** Version of this mail client (package.json); sent with every request so the service records which client a machine runs. */
export const CLIENT_VERSION = readJson(fileURLToPath(new URL('../../package.json', import.meta.url)), {}).version || null;

/** The one line of a model mismatch (3c, 4a). */
export function modelMismatchText(model) {
  return `comms model ${model ?? '?'} ≠ ${MAIL_MODEL}: update mail and workflow together`;
}

export class CommsError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.code = code; // UNAVAILABLE | INSTANCE_MISMATCH | MODEL_MISMATCH | REJECTED | BAD_RESPONSE | USAGE
    Object.assign(this, extra);
  }
}

/** Merge CLI options, environment, optional config file and defaults. */
export function resolveConfig(opts = {}, env = process.env) {
  const cfgPath = opts.config || env.COMMS_CONFIG || null;
  const cfg = cfgPath ? readJson(path.resolve(cfgPath), {}) : {};
  const o = {
    instance: opts.instance || env.COMMS_INSTANCE || cfg.instance,
    dataDir: opts.dataDir || env.COMMS_DATA_DIR || cfg.data_dir,
  };
  const dir = resolveDataDir(o, env);
  const url = opts.url || env.COMMS_URL || cfg.url || null;
  const cdir = opts.clientDir || env.COMMS_CLIENT_DIR || cfg.client_dir || clientDir(dir);
  return {
    instance: instanceName(o, env),
    dataDir: dir,
    socket: socketPath(dir),
    url,
    key: opts.key || env.COMMS_KEY || cfg.key || null,
    instanceId: opts.instanceId || env.COMMS_INSTANCE_ID || cfg.instance_id || null,
    threadId: opts.thread || env.COMMS_THREAD_ID || cfg.thread_id || env.T3CODE_THREAD_ID || null,
    // Machine of this client's threads: wake modules ring only holders on their own machine.
    machine: normalizeMachine(opts.machine || env.COMMS_MACHINE || cfg.machine || os.hostname()),
    // Version of the workflow that calls this client (the workflow sets COMMS_WORKFLOW_VERSION); recorded by the service.
    workflowVersion: opts.workflowVersion || env.COMMS_WORKFLOW_VERSION || cfg.workflow_version || null,
    outbox: opts.outbox || env.COMMS_OUTBOX || cfg.outbox || path.join(cdir, 'outbox'),
    clientDir: cdir,
    timeout: Number(opts.timeout || env.COMMS_TIMEOUT || cfg.timeout || DEFAULT_TIMEOUT),
    autostart: opts.noStart ? false : (url ? false : cfg.autostart !== false),
  };
}

export class Client {
  constructor(cfg) {
    this.cfg = cfg;
    this.health = null;
  }

  /** Forget the cached health so the next call probes (and may start) the service again. */
  reset() {
    this.health = null;
    this.spawned = false;
  }

  /**
   * Raw request; throws CommsError UNAVAILABLE on transport failure, MODEL_MISMATCH on 426, REJECTED on another
   * 4xx/5xx, BAD_RESPONSE on a 2xx whose body is not JSON. Every call has its own timeout (default cfg.timeout).
   */
  request(method, pathname, { body, query, auth = true, timeout = this.cfg.timeout } = {}) {
    const q = query ? '?' + new URLSearchParams(Object.entries(query).filter(([, v]) => v != null && v !== '')).toString() : '';
    let target;
    let transport = http;
    if (this.cfg.url) {
      try { ({ transport, target } = urlTarget(this.cfg.url, pathname + q)); } catch (e) { return Promise.reject(e); }
    } else target = { socketPath: this.cfg.socket, path: pathname + q };
    const headers = { Accept: 'application/json', 'X-Comms-Model': String(MAIL_MODEL) };
    if (auth && this.cfg.key) headers.Authorization = `Bearer ${this.cfg.key}`;
    Object.assign(headers, versionHeaders(this.cfg));
    let payload = null;
    if (body !== undefined) {
      payload = JSON.stringify(body);
      headers['Content-Type'] = 'application/json';
      headers['Content-Length'] = Buffer.byteLength(payload);
    }
    return new Promise((resolve, reject) => {
      const req = transport.request({ ...target, method, headers, timeout }, (res) => {
        let raw = '';
        res.setEncoding('utf8');
        res.on('data', (d) => { raw += d; });
        res.on('end', () => {
          const ok = res.statusCode >= 200 && res.statusCode < 300;
          let data = {};
          if (raw) {
            try { data = JSON.parse(raw); } catch {
              // A 2xx answer that is not JSON is not a success (the outbox keeps the item).
              if (ok) return reject(new CommsError('BAD_RESPONSE', `služba odpovedala nie JSON (HTTP ${res.statusCode})`, { status: res.statusCode }));
              data = { error: raw };
            }
          }
          if (ok) return resolve(data);
          if (res.statusCode === 426) return reject(new CommsError('MODEL_MISMATCH', `${modelMismatchText(data.model ?? null)} (${data.error || 'HTTP 426'})`, { status: 426, data }));
          reject(new CommsError('REJECTED', data.error || `HTTP ${res.statusCode}`, { status: res.statusCode, data }));
        });
      });
      req.on('timeout', () => { req.destroy(new Error('timeout')); });
      req.on('error', (e) => reject(new CommsError('UNAVAILABLE', `comms unavailable (${e.message})`, { cause: e })));
      if (payload !== null) req.write(payload);
      req.end();
    });
  }

  /** Ensure the service answers, is the expected instance and speaks model 2; start it if allowed. */
  async ensureRunning() {
    if (this.health) return this.health;
    let h = await this.tryHealth();
    if (!h && this.cfg.autostart && !this.instanceManaged()) {
      this.spawnServer();
      const until = Date.now() + this.cfg.timeout;
      while (!h && Date.now() < until) {
        await sleep(50);
        h = await this.tryHealth();
      }
    }
    if (!h) throw new CommsError('UNAVAILABLE', 'comms unavailable');
    const expected = this.expectedInstanceId();
    if (expected && h.instance_id !== expected) {
      throw new CommsError('INSTANCE_MISMATCH', `comms: iná inštancia (očakávané ${expected}, služba hlási ${h.instance_id})`, { expected, actual: h.instance_id });
    }
    // Another model: nothing is read, written or queued (4a); the outbox stays as it is (3c).
    if (h.model !== MAIL_MODEL) throw new CommsError('MODEL_MISMATCH', modelMismatchText(h.model), { model: h.model ?? null, version: h.version ?? null });
    this.health = h;
    return h;
  }

  async tryHealth() {
    try { return await this.request('GET', '/health', { auth: false }); } catch (e) { if (e.code === 'USAGE') throw e; return null; }
  }

  /** instance.json `managed`: the service manager starts it, clients do not (no autostart). */
  instanceManaged() {
    const inst = readJson(path.join(this.cfg.dataDir, 'instance.json'), null);
    return Boolean(inst && inst.managed);
  }

  expectedInstanceId() {
    if (this.cfg.instanceId) return this.cfg.instanceId;
    const inst = readJson(path.join(this.cfg.dataDir, 'instance.json'), null);
    return inst ? inst.instance_id : null;
  }

  spawnServer() {
    if (this.spawned) return;
    this.spawned = true;
    const child = spawn(process.execPath, [BIN, 'serve', '--data-dir', this.cfg.dataDir], {
      detached: true, stdio: 'ignore', windowsHide: true,
      env: { ...process.env, COMMS_DATA_DIR: this.cfg.dataDir },
    });
    child.on('error', () => {});
    child.unref();
  }

  // --- operations -----------------------------------------------------------------
  async call(method, pathname, opts) {
    await this.ensureRunning();
    return this.request(method, pathname, opts);
  }

  /**
   * A write of the outbox kind (send, resolve, withdraw) with a client_op_id: the outbox goes first (order), then this
   * write. Only an unavailable service queues it (`{ queued: true, client_op_id, outbox_file }`); another model, a
   * rejection or an instance mismatch throw and nothing is queued.
   */
  async write(pathname, body) {
    const item = { ...body, client_op_id: body.client_op_id || newId() };
    try {
      await this.ensureRunning();
      if (listJsonFiles(this.cfg.outbox).length) await this.flushOutbox();
      return await this.request('POST', pathname, { body: item });
    } catch (e) {
      if (e.code !== 'UNAVAILABLE') throw e;
      const file = this.queue(pathname, item);
      return { queued: true, client_op_id: item.client_op_id, outbox_file: file, message: 'comms unavailable' };
    }
  }

  /** New message (never a reply: a reply is `resolve`); queued on outage. */
  send(body) {
    return this.write('/send', this.withThread(body));
  }

  /**
   * Answer or settle a message of the caller's mailbox (one operation, 1d): `state` hotovo|otázka|čakám, `text` (without
   * text only hotovo = settled), `ack: true` (acked, from picked only), `cancels`. The mailbox: project+topic, persona or
   * thread (default the client's thread). Queued on outage.
   */
  resolve(body) {
    const b = this.withThread(body);
    return this.write('/resolve', { ...b, thread: body.thread || b.thread_id });
  }

  /** The sender takes its non-final message back; queued on outage. */
  withdraw(body) {
    return this.write('/withdraw', body);
  }

  withThread(body) {
    const thread_id = body.thread_id || this.cfg.threadId || null;
    return { ...body, thread_id, thread_machine: body.thread_machine || (thread_id ? this.cfg.machine || null : null) };
  }

  receive(q) { return this.call('GET', '/receive', { query: q }); }
  notices(all = false) { return this.call('GET', '/notices', { query: { all: all ? '1' : null } }); }
  decide(body) { return this.call('POST', '/decide', { body }); }
  /**
   * Register a topic (see service.registerTopic). `takeover: true` allows replacing another holder;
   * `release: true` clears the holder (no holder given, or the current one; else needs `takeover`);
   * `close: true` also marks the topic closed (send 410, pending mail becomes undelivered for the senders).
   */
  registerTopic(body) { return this.call('POST', '/register-topic', { body }); }
  createNotice(body) { return this.call('POST', '/notice', { body }); }
  bounce(body) { return this.call('POST', '/bounce', { body }); }
  /** State of one message (status, history); the body only for its sender and its mailbox (q names the mailbox). */
  message(id, q = {}) { return this.call('GET', '/message', { query: { id, ...q } }); }
  /** Pending messages of a mailbox without reading them (no pickup, no count cache): the wake module's check before a ring. */
  pendingMail(q) { return this.call('GET', '/count', { query: q }); }
  /** Wake events of the messages still pending after `olderMs` (the wake module's reminder; no body). */
  unpicked(olderMs = 0) { return this.call('GET', '/unpicked', { query: { older_ms: String(olderMs) } }); }
  /** Waits due now (1e): the key `wake` all of them, another key those of its own messages. */
  due() { return this.call('GET', '/obligations', { query: { due: '1' } }); }
  /**
   * Confirm the ring of a due wait (1e point 2): the next check, or the end of the wait. Without `rangAt` the service
   * takes its own clock (a wake module on another machine whose clock is behind would confirm before the due time,
   * which the service ignores).
   */
  waitRang(id, rangAt = null) { return this.call('POST', '/wait-rang', { body: { id, ...(rangAt ? { rang_at: rangAt } : {}) } }); }
  /** Hand a local wait over to the service (4b step 5): only the sender of a non-final message. */
  waitAdopt(body) { return this.call('POST', '/wait-adopt', { body }); }
  turns(open = true) { return this.call('GET', '/turns', { query: { open: open ? '1' : null } }); }
  /** Versions of mail clients and workflow per sender (persona) and machine, as the service saw them. */
  clients() { return this.call('GET', '/clients'); }

  /**
   * Record a turn (open/closed); never thrown on outage. On outage a `closed` record is
   * queued in the outbox (sent with the next flush; the service ignores it when `opened_at`
   * no longer matches) → `{ ok:false, unavailable:true, queued:true }`; `open` is not queued
   * → `{ ok:false, unavailable:true }`.
   */
  async turn(body) {
    try {
      // A `closed` queued during an outage goes first: else the service keeps an old turn open and wake skips the
      // thread as already rung (fable audit M6). Turn records run every prompt, so the outbox drains here too.
      if (listJsonFiles(this.cfg.outbox).length) { await this.ensureRunning(); await this.flushOutbox(); }
      return await this.call('POST', '/turn', { body });
    } catch (e) {
      if (e.code !== 'UNAVAILABLE') throw e;
      if (body && body.state === 'closed') {
        const file = this.queue('/turn', body);
        return { ok: false, unavailable: true, queued: true, outbox_file: file, error: e.message };
      }
      return { ok: false, unavailable: true, error: e.message };
    }
  }
  events(since, wait) {
    // Long-poll may legitimately exceed the default timeout; only this call gets the longer one.
    return this.ensureRunning().then(() => this.request('GET', '/events', { query: { since, wait }, timeout: Math.max(this.cfg.timeout, wait + 5000) }));
  }

  // --- outbox ----------------------------------------------------------------------
  queue(pathname, body) {
    // The name sorts by time (newId starts with it), so a flush keeps the order of the writes.
    const file = path.join(this.cfg.outbox, `${newId()}.json`);
    writeJson(file, { path: pathname, body, queued_at: nowIso() });
    return file;
  }

  /** Queued items in order: [{ file, item }] (an unreadable file is removed). */
  outboxItems() {
    const out = [];
    for (const f of listJsonFiles(this.cfg.outbox)) {
      const full = path.join(this.cfg.outbox, f);
      const item = readJson(full, null);
      if (!item) { try { fs.unlinkSync(full); } catch { /* gone */ } continue; }
      out.push({ file: f, item });
    }
    return out;
  }

  /**
   * Send the queued items in order (3c). The model is checked first (ensureRunning): another model leaves every item
   * where it is. Per item: 2xx → done; 409 with a final status → dropped with the line `already <resolution>`; another
   * 4xx → `rejected/` with a line; outage, 426 or 5xx → stop, the rest stays. Returns
   * `{ sent, dropped, rejected, remaining, lines }`; an outage or another model throws after recording what was sent.
   */
  async flushOutbox() {
    await this.ensureRunning();
    const items = this.outboxItems();
    const out = { sent: [], dropped: [], rejected: [], remaining: 0, lines: [] };
    for (let i = 0; i < items.length; i++) {
      const { file: f, item } = items[i];
      const full = path.join(this.cfg.outbox, f);
      try {
        const res = await this.request('POST', item.path, { body: item.body });
        fs.unlinkSync(full);
        out.sent.push({ file: f, path: item.path, id: res.id ?? null, ...(res.replayed ? { replayed: true } : {}) });
      } catch (e) {
        const st = e.data || {};
        if (e.code === 'REJECTED' && e.status === 409 && FINAL_STATUSES.includes(st.status)) {
          fs.unlinkSync(full);
          const line = `already ${st.resolution || st.status} ${st.id || item.body.id || ''}`.trim();
          out.dropped.push({ file: f, path: item.path, id: st.id || item.body.id || null, status: st.status, resolution: st.resolution || null });
          out.lines.push(line);
          continue;
        }
        if (e.code === 'REJECTED' && e.status >= 400 && e.status < 500) {
          const dir = path.join(this.cfg.outbox, 'rejected');
          fs.mkdirSync(dir, { recursive: true });
          fs.renameSync(full, path.join(dir, f));
          writeJson(path.join(dir, `${f}.error.json`), { error: e.message, status: e.status, details: e.data || null, at: nowIso() });
          out.rejected.push({ file: f, path: item.path, status: e.status, error: e.message });
          out.lines.push(`rejected ${item.path} ${item.body.id || ''} (${e.status}): ${e.message}`.replace(/\s+/g, ' '));
          continue;
        }
        out.remaining = items.length - i;
        e.flush = out;
        throw e;
      }
    }
    return out;
  }

  // --- obligations (section 2) with the offline backup (3b) ------------------------------
  obligationsCacheFile() { return path.join(this.cfg.clientDir, 'obligations-cache.json'); }

  /**
   * What the thread owes and awaits (`GET /obligations`). Live: the outbox is flushed first (3c point 2), the answer then
   * replaces the backup for this query (point 3). Unavailable: the last answer of the same query with `offline: true`,
   * `age_s`, no `recent` rows (they were shown), minus the ids with a queued operation (an owed message with a queued
   * resolve, an awaited one with a queued withdraw). No backup yet → `{ offline: true, unavailable: true, cached: false }`.
   * Another model throws (nothing is read).
   */
  async obligations(q = {}, now = Date.now()) {
    const key = ['thread', 'topics', 'project', 'persona'].map((k) => q[k] || '').join('|');
    const cache = readJson(this.obligationsCacheFile(), {});
    try {
      await this.ensureRunning();
      let flush = null;
      if (listJsonFiles(this.cfg.outbox).length) flush = await this.flushOutbox();
      const res = await this.request('GET', '/obligations', { query: q });
      const { recent: _recent, ...kept } = res;
      cache[key] = { at: now, data: kept };
      writeJson(this.obligationsCacheFile(), cache);
      return { ...res, offline: false, ...(flush && flush.lines.length ? { outbox_lines: flush.lines } : {}) };
    } catch (e) {
      if (e.code !== 'UNAVAILABLE') throw e;
      const entry = cache[key];
      if (!entry) return { offline: true, unavailable: true, cached: false, error: e.message };
      const queued = new Map();
      for (const { item } of this.outboxItems()) if (item.body && item.body.id) queued.set(String(item.body.id), item.path);
      const d = entry.data;
      const inbox = d.inbox ? { ...d.inbox, ids: (d.inbox.ids || []).filter((x) => !queued.has(x.id)) } : d.inbox;
      if (inbox) inbox.n = inbox.ids.length;
      return {
        ...d, inbox,
        owed: (d.owed || []).filter((x) => queued.get(x.id) !== '/resolve'),
        awaiting: (d.awaiting || []).filter((x) => queued.get(x.id) !== '/withdraw'),
        recent: [], offline: true, unavailable: true, cached: true, age_s: Math.max(0, Math.round((now - entry.at) / 1000)), error: e.message,
      };
    }
  }

  // --- cached count ------------------------------------------------------------------
  cacheFile() { return path.join(this.cfg.clientDir, 'count-cache.json'); }

  /**
   * Message count for the status line. Always asked live while the service answers: a cached
   * value hid mail that arrived after the previous turn, so the bell turn read `mail: 0` and the
   * message stayed pending with nobody to ring again. The file keeps the last answer for an
   * outage: no new attempt before 60 s; `first_report` marks the first failure of a window.
   */
  async count(q, now = Date.now()) {
    const key = `${q.project || ''}/${q.topic || ''}/${q.thread || ''}/${q.persona || ''}`;
    const cache = readJson(this.cacheFile(), {});
    const entry = cache[key] || {};
    if (entry.failed_at && now - entry.failed_at < COUNT_TTL) {
      return { total: entry.total ?? null, blocker: entry.blocker ?? null, ids: entry.ids || [], cached: true, unavailable: true, first_report: false };
    }
    try {
      const res = await this.call('GET', '/count', { query: q });
      cache[key] = { total: res.total, blocker: res.blocker, ids: res.ids, at: now };
      writeJson(this.cacheFile(), cache);
      return { ...res, cached: false, unavailable: false };
    } catch (e) {
      if (e.code !== 'UNAVAILABLE') throw e;
      cache[key] = { ...entry, at: null, failed_at: now };
      writeJson(this.cacheFile(), cache);
      return { total: entry.total ?? null, blocker: entry.blocker ?? null, ids: entry.ids || [], cached: entry.total != null, unavailable: true, first_report: true, error: e.message };
    }
  }
}

/** Final statuses of model 2 (1a): a 409 with one of them means the queued operation already happened another way. */
export const FINAL_STATUSES = ['done', 'discarded', 'superseded', 'undelivered'];

/** Headers naming the client version, workflow version and machine. */
/**
 * Transport and request options for a service URL: https:// goes over TLS (default port 443), http:// over plain
 * TCP (default port 80). Another protocol is a configuration error (USAGE), never a silent plain-HTTP call.
 */
export function urlTarget(url, pathAndQuery) {
  let u;
  try { u = new URL(url); } catch { throw new CommsError('USAGE', `neplatná adresa pošty: ${url}`); }
  const secure = u.protocol === 'https:';
  if (!secure && u.protocol !== 'http:') throw new CommsError('USAGE', `adresa pošty musí byť http:// alebo https:// (${u.protocol})`);
  return {
    transport: secure ? https : http,
    target: { host: u.hostname.replace(/^\[|\]$/g, ''), port: Number(u.port) || (secure ? 443 : 80), path: u.pathname.replace(/\/$/, '') + pathAndQuery },
  };
}

export function versionHeaders(cfg) {
  const h = {};
  const clean = (v) => String(v).replace(/[^\x20-\x7e]/g, '').trim().slice(0, 80);
  if (CLIENT_VERSION) h['X-Comms-Client'] = clean(CLIENT_VERSION);
  if (cfg.workflowVersion) h['X-Workflow-Version'] = clean(cfg.workflowVersion);
  if (cfg.machine) h['X-Comms-Machine'] = clean(cfg.machine);
  return h;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
