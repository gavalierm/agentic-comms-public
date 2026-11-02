// Client core: http.request over socketPath (or TCP URL), instance_id check,
// start-on-demand, outbox for outages, message count (live; after an outage a 60 s backoff).
import http from 'node:http';
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
const BIN = fileURLToPath(new URL('../../bin/comms.mjs', import.meta.url));

export class CommsError extends Error {
  constructor(code, message, extra = {}) {
    super(message);
    this.code = code; // UNAVAILABLE | INSTANCE_MISMATCH | REJECTED | USAGE
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
   * Raw request; throws CommsError UNAVAILABLE on transport failure, REJECTED on 4xx/5xx,
   * BAD_RESPONSE on a 2xx whose body is not JSON. Every call has its own timeout (default cfg.timeout).
   */
  request(method, pathname, { body, query, auth = true, timeout = this.cfg.timeout } = {}) {
    const q = query ? '?' + new URLSearchParams(Object.entries(query).filter(([, v]) => v != null && v !== '')).toString() : '';
    const target = this.cfg.url
      ? (() => { const u = new URL(this.cfg.url); return { host: u.hostname, port: u.port || 80, path: (u.pathname.replace(/\/$/, '')) + pathname + q }; })()
      : { socketPath: this.cfg.socket, path: pathname + q };
    const headers = { Accept: 'application/json' };
    if (auth && this.cfg.key) headers.Authorization = `Bearer ${this.cfg.key}`;
    let payload = null;
    if (body !== undefined) {
      payload = JSON.stringify(body);
      headers['Content-Type'] = 'application/json';
      headers['Content-Length'] = Buffer.byteLength(payload);
    }
    return new Promise((resolve, reject) => {
      const req = http.request({ ...target, method, headers, timeout }, (res) => {
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
          reject(new CommsError('REJECTED', data.error || `HTTP ${res.statusCode}`, { status: res.statusCode, data }));
        });
      });
      req.on('timeout', () => { req.destroy(new Error('timeout')); });
      req.on('error', (e) => reject(new CommsError('UNAVAILABLE', `comms unavailable (${e.message})`, { cause: e })));
      if (payload !== null) req.write(payload);
      req.end();
    });
  }

  /** Ensure the service answers and is the expected instance; start it if allowed. */
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
    this.health = h;
    return h;
  }

  async tryHealth() {
    try { return await this.request('GET', '/health', { auth: false }); } catch { return null; }
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

  send(body) {
    return this.call('POST', '/send', { body: this.withThread(body) });
  }

  /** Send; on outage queue into the outbox instead of failing. */
  async sendOrQueue(body) {
    const msg = this.withThread(body);
    try {
      await this.ensureRunning();
      await this.flushOutbox();
      return await this.request('POST', '/send', { body: msg });
    } catch (e) {
      if (e.code !== 'UNAVAILABLE') throw e;
      const file = this.queue('/send', msg);
      return { queued: true, outbox_file: file, message: 'comms unavailable' };
    }
  }

  withThread(body) {
    const thread_id = body.thread_id || this.cfg.threadId || null;
    return { ...body, thread_id, thread_machine: body.thread_machine || (thread_id ? this.cfg.machine || null : null) };
  }

  receive(q) { return this.call('GET', '/receive', { query: q }); }
  ack(ids) { return this.call('POST', '/ack', { body: { ids } }); }
  notices(all = false) { return this.call('GET', '/notices', { query: { all: all ? '1' : null } }); }
  decide(body) { return this.call('POST', '/decide', { body }); }
  /**
   * Register a topic (see service.registerTopic). `takeover: true` allows replacing another holder;
   * `release: true` clears the holder (no holder given, or the current one; else needs `takeover`);
   * `close: true` also marks the topic closed (send/reply 410, pending mail bounces to the senders).
   */
  registerTopic(body) { return this.call('POST', '/register-topic', { body }); }
  createNotice(body) { return this.call('POST', '/notice', { body }); }
  bounce(body) { return this.call('POST', '/bounce', { body }); }
  message(id) { return this.call('GET', '/message', { query: { id } }); }
  turns(open = true) { return this.call('GET', '/turns', { query: { open: open ? '1' : null } }); }

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
    const file = path.join(this.cfg.outbox, `${newId()}.json`);
    writeJson(file, { path: pathname, body, queued_at: nowIso() });
    return file;
  }

  /** Send queued messages in order. Stops at first outage. Returns summary. */
  async flushOutbox() {
    const files = listJsonFiles(this.cfg.outbox);
    const out = { sent: [], rejected: [], remaining: 0 };
    for (const f of files) {
      const full = path.join(this.cfg.outbox, f);
      const item = readJson(full, null);
      if (!item) { fs.unlinkSync(full); continue; }
      try {
        const res = await this.request('POST', item.path, { body: item.body });
        fs.unlinkSync(full);
        out.sent.push({ file: f, id: res.id });
      } catch (e) {
        if (e.code === 'REJECTED') {
          const dir = path.join(this.cfg.outbox, 'rejected');
          fs.mkdirSync(dir, { recursive: true });
          fs.renameSync(full, path.join(dir, f));
          writeJson(path.join(dir, `${f}.error.json`), { error: e.message, status: e.status, at: nowIso() });
          out.rejected.push({ file: f, error: e.message });
          continue;
        }
        out.remaining = files.length - out.sent.length - out.rejected.length;
        throw e;
      }
    }
    return out;
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
      return { total: entry.total ?? null, blocker: entry.blocker ?? null, ...idsOf(entry), cached: true, unavailable: true, first_report: false };
    }
    try {
      const res = await this.call('GET', '/count', { query: q });
      cache[key] = { total: res.total, blocker: res.blocker, ...idsOf(res), at: now };
      writeJson(this.cacheFile(), cache);
      return { ...res, cached: false, unavailable: false };
    } catch (e) {
      if (e.code === 'REJECTED') throw e;
      cache[key] = { ...entry, at: null, failed_at: now };
      writeJson(this.cacheFile(), cache);
      return { total: entry.total ?? null, blocker: entry.blocker ?? null, ...idsOf(entry), cached: entry.total != null, unavailable: true, first_report: true, error: e.message };
    }
  }
}

/** `ids` only when the service sent them (an older service has none; the workflow then uses `total`). */
function idsOf(o) {
  return Array.isArray(o && o.ids) ? { ids: o.ids } : {};
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
