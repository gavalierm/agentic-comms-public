// File storage of one instance. One message = one JSON file. Only the service
// writes here (apart from `init` / `key`, which run before the service).
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { randomBytes, createHash } from 'node:crypto';
import { readJson, writeJson, listJsonFiles, newSecret, nowIso, isValidThread, HttpError } from '../common/util.mjs';
import { DATA_SCHEMA } from './lifecycle.mjs';

export class Store {
  constructor(dir) {
    this.dir = dir;
    this.files = {
      instance: path.join(dir, 'instance.json'),
      keys: path.join(dir, 'keys.json'),
      server: path.join(dir, 'server.json'),
      meta: path.join(dir, 'meta.json'),
    };
    this.dirs = {
      messages: path.join(dir, 'messages'),
      topics: path.join(dir, 'topics'),
      notices: path.join(dir, 'notices'),
      events: path.join(dir, 'events'),
      turns: path.join(dir, 'turns'),
      clients: path.join(dir, 'clients'),
      threads: path.join(dir, 'threads'),
    };
  }

  /** Schema of the data folder (`{schema, migrated_at?, backup?}`), or null for 0.4.x data. */
  meta() {
    return readJson(this.files.meta, null);
  }

  saveMeta(meta) {
    writeJson(this.files.meta, meta);
  }

  exists() {
    return fs.existsSync(this.files.instance);
  }

  /** Create instance (idempotent). Returns { created, instance, firstKey? } */
  init({ instance, sender } = {}) {
    if (this.exists()) return { created: false, instance: this.instance() };
    fs.mkdirSync(this.dir, { recursive: true, mode: 0o700 });
    for (const d of Object.values(this.dirs)) fs.mkdirSync(d, { recursive: true, mode: 0o700 });
    const inst = {
      instance_id: randomBytes(8).toString('hex'),
      instance: instance || path.basename(this.dir),
      created_at: nowIso(),
      notify: { command: null },
      tcp: null,
    };
    writeJson(this.files.instance, inst);
    writeJson(this.files.keys, { keys: [] });
    this.saveMeta({ schema: DATA_SCHEMA, created_at: inst.created_at });
    const firstKey = this.addKey(sender || safeUser());
    return { created: true, instance: inst, firstKey };
  }

  instance() {
    return readJson(this.files.instance);
  }

  saveInstance(inst) {
    writeJson(this.files.instance, inst);
  }

  // --- keys -----------------------------------------------------------------
  keys() {
    return readJson(this.files.keys, { keys: [] }).keys;
  }

  addKey(sender, key = newSecret()) {
    const all = this.keys();
    if (all.some((k) => k.sender === sender)) throw new Error(`odosielateľ „${sender}“ už kľúč má`);
    const rec = { sender, key, created_at: nowIso() };
    all.push(rec);
    writeJson(this.files.keys, { keys: all });
    return rec;
  }

  removeKey(sender) {
    const all = this.keys();
    const rest = all.filter((k) => k.sender !== sender);
    if (rest.length === all.length) return false;
    writeJson(this.files.keys, { keys: rest });
    return true;
  }

  senderForKey(key) {
    if (!key) return null;
    const rec = this.keys().find((k) => k.key === key);
    return rec ? rec.sender : null;
  }

  // --- server state ----------------------------------------------------------
  serverInfo() {
    return readJson(this.files.server, null);
  }

  saveServerInfo(info) {
    writeJson(this.files.server, info);
  }

  clearServerInfo() {
    try { fs.unlinkSync(this.files.server); } catch { /* ignore */ }
  }

  // --- topics ----------------------------------------------------------------
  /**
   * File of a topic key (model 3): `topics/<sha1 of brain, machine, project, topic; 16 hex>.json`. The names joined with
   * `__` are not unique, so the identity is the content: a record with other fields is another topic.
   */
  topicFile(key) {
    return path.join(this.dirs.topics, `${topicHash(key)}.json`);
  }

  /** The stored topic of a full key `{brain, machine, project, topic}` (brain null = a record from before model 3), or null. */
  topic(key) {
    if (!key || !key.project || !key.topic || !key.machine) return null;
    const rec = readJson(this.topicFile(key), null);
    return rec && sameTopicKey(rec, key) ? rec : null;
  }

  /** Refuses (409) to overwrite the file of another key (all four fields are compared). */
  saveTopic(rec) {
    const file = this.topicFile(rec);
    const other = readJson(file, null);
    if (other && !sameTopicKey(other, rec)) {
      throw new HttpError(409, `téma ${topicKeyText(rec)} koliduje so súborom témy ${topicKeyText(other)}; registrácia zamietnutá`);
    }
    writeJson(file, rec);
  }

  topics() {
    return listJsonFiles(this.dirs.topics).map((f) => readJson(path.join(this.dirs.topics, f), null)).filter(Boolean);
  }

  // --- messages ----------------------------------------------------------------
  message(id) {
    if (!/^[A-Za-z0-9-]+$/.test(id)) return null;
    return readJson(path.join(this.dirs.messages, `${id}.json`), null);
  }

  saveMessage(msg) {
    writeJson(path.join(this.dirs.messages, `${msg.id}.json`), msg);
  }

  messages(filter = () => true) {
    const out = [];
    for (const f of listJsonFiles(this.dirs.messages)) {
      const m = readJson(path.join(this.dirs.messages, f), null);
      if (m && filter(m)) out.push(m);
    }
    return out;
  }

  // --- notices -----------------------------------------------------------------
  notice(id) {
    if (!/^[A-Za-z0-9-]+$/.test(id)) return null;
    return readJson(path.join(this.dirs.notices, `${id}.json`), null);
  }

  saveNotice(n) {
    writeJson(path.join(this.dirs.notices, `${n.id}.json`), n);
  }

  notices(filter = () => true) {
    const out = [];
    for (const f of listJsonFiles(this.dirs.notices)) {
      const n = readJson(path.join(this.dirs.notices, f), null);
      if (n && filter(n)) out.push(n);
    }
    return out;
  }

  // --- turns (keepalive) -----------------------------------------------------------
  /** File of a thread's turn record; thread ids contain `:` (T3 `mcp:<uuid>`), the name is URI-encoded. */
  turnFile(thread) {
    return path.join(this.dirs.turns, `${encodeURIComponent(thread)}.json`);
  }

  turn(thread) {
    if (!isValidThread(thread)) return null;
    return readJson(this.turnFile(thread), null);
  }

  saveTurn(rec) {
    writeJson(this.turnFile(rec.thread), rec);
  }

  turns(filter = () => true) {
    return listJsonFiles(this.dirs.turns).map((f) => readJson(path.join(this.dirs.turns, f), null)).filter((t) => t && filter(t));
  }

  // --- client versions (per sender and machine) ------------------------------------
  clientFile(sender, machine) {
    return path.join(this.dirs.clients, `${encodeURIComponent(sender)}__${encodeURIComponent(machine || 'unknown')}.json`);
  }

  client(sender, machine) {
    return readJson(this.clientFile(sender, machine), null);
  }

  saveClient(rec) {
    writeJson(this.clientFile(rec.sender, rec.machine), rec);
  }

  clients() {
    return listJsonFiles(this.dirs.clients).map((f) => readJson(path.join(this.dirs.clients, f), null)).filter(Boolean);
  }

  // --- per-thread records (recent rows and their cursor, section 2) ------------------------
  /** `key` is a thread id or `topic:<brain>/<project>/<topic>` (a sender without a thread; brain empty before model 3); the name is URI-encoded. */
  threadFile(key) {
    return path.join(this.dirs.threads, `${encodeURIComponent(key)}.json`);
  }

  threadRecord(key) {
    return readJson(this.threadFile(key), null) || { key, cursor: 0, recent: [] };
  }

  saveThreadRecord(rec) {
    writeJson(this.threadFile(rec.key), rec);
  }

  removeThreadRecord(key) {
    try { fs.unlinkSync(this.threadFile(key)); } catch { /* gone */ }
  }

  // --- wake events ----------------------------------------------------------------
  lastEventSeq() {
    const files = listJsonFiles(this.dirs.events);
    if (!files.length) return 0;
    return Number.parseInt(files[files.length - 1], 10) || 0;
  }

  saveEvent(ev) {
    writeJson(path.join(this.dirs.events, `${String(ev.seq).padStart(12, '0')}.json`), ev);
  }

  eventsSince(seq, limit = 100) {
    const out = [];
    for (const f of listJsonFiles(this.dirs.events)) {
      if ((Number.parseInt(f, 10) || 0) <= seq) continue;
      out.push(readJson(path.join(this.dirs.events, f)));
      if (out.length >= limit) break;
    }
    return out;
  }
}

/** The key of a topic record or address: brain (null before model 3), machine, project, topic. */
export function topicKey(rec) {
  return { brain: rec.brain ?? null, machine: rec.machine ?? null, project: rec.project, topic: rec.topic };
}

export function sameTopicKey(a, b) {
  return (a.brain ?? null) === (b.brain ?? null) && (a.machine ?? null) === (b.machine ?? null) && a.project === b.project && a.topic === b.topic;
}

export function topicKeyText(k) {
  return `${k.brain ?? '-'}@${k.machine ?? '?'}:${k.project}/${k.topic}`;
}

/** 16 hex chars of sha1 over the JSON of the four fields (unambiguous, unlike names joined with a separator). */
export function topicHash(key) {
  return createHash('sha1').update(JSON.stringify([key.brain ?? null, key.machine ?? null, key.project, key.topic]), 'utf8').digest('hex').slice(0, 16);
}

function safeUser() {
  try { return os.userInfo().username; } catch { return 'operator'; }
}

