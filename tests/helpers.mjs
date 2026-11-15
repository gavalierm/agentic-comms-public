// Test helpers: temporary instances in /tmp, in-process servers, clients.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { Store } from '../src/server/store.mjs';
import { startServer } from '../src/server/index.mjs';
import { Client, resolveConfig } from '../src/client/index.mjs';
import { socketPath } from '../src/common/paths.mjs';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const COMMS_BIN = fileURLToPath(new URL('../bin/comms.mjs', import.meta.url));

// COMMS_TEST_TMP moves the temporary instances elsewhere (e.g. a scratch directory).
const TMP = process.env.COMMS_TEST_TMP || (fs.existsSync('/tmp') ? '/tmp' : os.tmpdir());
const CLEAN_ENV = { HOME: os.homedir(), PATH: process.env.PATH };

export function tmpDir(prefix = 'comms-test-') {
  return fs.mkdtempSync(path.join(TMP, prefix));
}

/** Create an instance with one key; returns { dir, store, instanceId, key, sender }. */
export function makeInstance(name = 'test', sender = 'alice') {
  const dir = tmpDir(`comms-${name}-`);
  const store = new Store(dir);
  const r = store.init({ instance: name, sender });
  return { dir, store, instanceId: r.instance.instance_id, key: r.firstKey.key, sender, socket: socketPath(dir) };
}

/** Start an in-process server for the instance; returns { srv, stop }. */
export async function startInstance(inst, extra = {}) {
  const srv = await startServer({ dataDir: inst.dir, ...extra });
  return { srv, stop: () => srv.close() };
}

export function makeClient(inst, overrides = {}) {
  const cfg = resolveConfig({ dataDir: inst.dir, key: inst.key, noStart: true, ...overrides }, CLEAN_ENV);
  return new Client(cfg);
}

export function registerAndClient(inst, project = 'proj', topic = 'topic-a') {
  const c = makeClient(inst);
  return c.registerTopic({ project, topic, subject: 'Test topic' }).then(() => c);
}

/** Raw HTTP over the socket (to bypass client-side validation). */
export function rawRequest(sock, method, pathname, { body, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? null : (typeof body === 'string' ? body : JSON.stringify(body));
    const h = { ...headers };
    if (payload !== null && !h['Content-Type']) h['Content-Type'] = 'application/json';
    const req = http.request({ socketPath: sock, method, path: pathname, headers: h }, (res) => {
      let raw = '';
      res.on('data', (d) => { raw += d; });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: raw, json: tryJson(raw) }));
    });
    req.on('error', reject);
    if (payload !== null) req.write(payload);
    req.end();
  });
}

function tryJson(s) { try { return JSON.parse(s); } catch { return null; } }

/**
 * Mail model 2 API over the socket, raw HTTP without the client (server tests).
 * `call(method, path, body?)` → { status, json }; every request carries X-Comms-Model: 2 and the key.
 * Helpers return the JSON and throw on a status other than `expect` (default 2xx).
 */
export function api(inst, key = inst.key) {
  const call = (method, pathname, body, headers = {}) => rawRequest(inst.socket, method, pathname, {
    body, headers: { Authorization: `Bearer ${key}`, 'X-Comms-Model': '2', ...headers },
  });
  const ok = async (p, expect) => {
    const r = await p;
    if (expect ? r.status !== expect : (r.status < 200 || r.status > 299)) {
      const e = new Error(`HTTP ${r.status}: ${r.body}`);
      e.status = r.status; e.json = r.json;
      throw e;
    }
    return r.json;
  };
  const qs = (o = {}) => {
    const p = new URLSearchParams();
    for (const [k, v] of Object.entries(o)) if (v != null && v !== '') p.set(k, Array.isArray(v) ? v.join(',') : String(v));
    const s = p.toString();
    return s ? `?${s}` : '';
  };
  return {
    call,
    qs,
    register: (body) => ok(call('POST', '/register-topic', { subject: 'Test topic', ...body })),
    send: (body) => ok(call('POST', '/send', body)),
    receive: (q) => ok(call('GET', `/receive${qs(q)}`)).then((j) => j.messages),
    receiveId: (id, q = {}) => ok(call('GET', `/receive${qs({ id, ...q })}`)).then((j) => j.messages[0]),
    count: (q) => ok(call('GET', `/count${qs(q)}`)),
    resolve: (body) => ok(call('POST', '/resolve', body)),
    withdraw: (body) => ok(call('POST', '/withdraw', body)),
    message: (id, q = {}) => ok(call('GET', `/message${qs({ id, ...q })}`)),
    obligations: (q) => ok(call('GET', `/obligations${qs(q)}`)),
    due: () => ok(call('GET', '/obligations?due=1')),
    waitRang: (body) => ok(call('POST', '/wait-rang', body)),
    waitAdopt: (body) => ok(call('POST', '/wait-adopt', body)),
    bounce: (body) => ok(call('POST', '/bounce', body)),
    notices: (all = false) => ok(call('GET', `/notices${all ? '?all=1' : ''}`)).then((j) => j.notices),
    decide: (body) => ok(call('POST', '/decide', body)),
    turn: (body) => ok(call('POST', '/turn', body)),
    events: (since = 0) => ok(call('GET', `/events?since=${since}`)).then((j) => j.events),
    health: () => ok(call('GET', '/health')),
  };
}

/** Add a key for another sender (e.g. `wake`) to a running or stopped instance; returns the key. */
export function addKey(inst, sender) {
  const existing = inst.store.keys().find((k) => k.sender === sender);
  return existing ? existing.key : inst.store.addKey(sender).key;
}

export function cleanup(...dirs) {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * CLI run that does not block the event loop: use it when an in-process server must answer.
 * Resolves { code, out, err, json }; `timeout` kills the child (code null) after that many ms.
 */
export function runCli(args, { env = {}, timeout = 15000 } = {}) {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [COMMS_BIN, ...args], {
      env: { PATH: process.env.PATH, HOME: process.env.HOME, COMMS_SELF_RESTART: '0', ...env }, stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    let err = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), timeout);
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { err += d; });
    child.on('close', (code) => {
      clearTimeout(timer);
      let json = null;
      try { json = JSON.parse(out); } catch { /* not JSON */ }
      resolve({ code, out, err, json });
    });
  });
}

export function killPid(pid) {
  try { process.kill(pid, 'SIGTERM'); } catch { /* already gone */ }
}
