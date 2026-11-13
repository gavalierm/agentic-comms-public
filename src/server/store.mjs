// File storage of one instance. One message = one JSON file. Only the service
// writes here (apart from `init` / `key`, which run before the service).
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { randomBytes } from 'node:crypto';
import { readJson, writeJson, listJsonFiles, newSecret, nowIso, isValidThread, HttpError } from '../common/util.mjs';

export class Store {
  constructor(dir) {
    this.dir = dir;
    this.files = {
      instance: path.join(dir, 'instance.json'),
      keys: path.join(dir, 'keys.json'),
      server: path.join(dir, 'server.json'),
    };
    this.dirs = {
      messages: path.join(dir, 'messages'),
      topics: path.join(dir, 'topics'),
      notices: path.join(dir, 'notices'),
      events: path.join(dir, 'events'),
      turns: path.join(dir, 'turns'),
      clients: path.join(dir, 'clients'),
    };
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
  topicFile(project, topic) {
    return path.join(this.dirs.topics, `${project}__${topic}.json`);
  }

  /** The stored topic, or null. `${project}__${topic}` is not unique (`a`/`x__y` and `a__x`/`y`): another topic's record is no answer. */
  topic(project, topic) {
    const rec = readJson(this.topicFile(project, topic), null);
    if (!rec) return null;
    // Records without the names (old data) are read as they are; a record with other names is another topic.
    const same = (stored, asked) => stored == null || stored === asked;
    return same(rec.project, project) && same(rec.topic, topic) ? rec : null;
  }

  /** Refuses (409) to overwrite the file of a different project/topic. */
  saveTopic(rec) {
    const file = this.topicFile(rec.project, rec.topic);
    const other = readJson(file, null);
    if (other && (other.project !== rec.project || other.topic !== rec.topic) && (other.project != null || other.topic != null)) {
      throw new HttpError(409, `téma ${rec.project}/${rec.topic} koliduje so súborom témy ${other.project}/${other.topic}; registrácia zamietnutá`);
    }
    writeJson(file, rec);
  }

  topics() {
    return listJsonFiles(this.dirs.topics).map((f) => readJson(path.join(this.dirs.topics, f)));
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

function safeUser() {
  try { return os.userInfo().username; } catch { return 'operator'; }
}

