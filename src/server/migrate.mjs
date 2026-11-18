// Migrations of a data folder, each one step with its own backup, verification and meta.json as the last write; they run
// before the service listens, and on a failed verification the service does not start (the data stays in the backup):
// 0.4.x (no meta.json) → schema 2 (mail model 2, design 4b); schema 2 → schema 3 (mail model 3: a topic is brain + machine +
// project + topic). A stop in the middle of a step leaves the earlier schema in meta.json and the step runs again.
import fs from 'node:fs';
import path from 'node:path';
import { nowIso, normalizeMachine, listJsonFiles, readJson } from '../common/util.mjs';
import { DATA_SCHEMA, migrateRecord, verifyInvariants, statusCounts, isFinal } from './lifecycle.mjs';
import { topicKey, sameTopicKey } from './store.mjs';

/** Schema written by migration 4b (mail model 2); schema 3 follows as its own step. */
export const SCHEMA_2 = 2;

export class MigrationError extends Error {
  constructor(message, problems = [], backup = null) {
    super(message);
    this.code = 'MIGRATION_FAILED';
    this.problems = problems;
    this.backup = backup;
  }
}

/** Backup folder name: `<data>.bak-<from>-<time>` next to the data folder (`0.4` for 4b, `2` for schema 2 → 3). */
export function backupPath(dataDir, at = nowIso(), from = '0.4') {
  return `${path.resolve(dataDir)}.bak-${from}-${at.replace(/[-:.]/g, '')}`;
}

/** Copy the data folder without its socket and temp files. */
function copyData(src, dest) {
  fs.cpSync(src, dest, {
    recursive: true, errorOnExist: true, force: false, preserveTimestamps: true,
    filter: (p) => {
      let st;
      try { st = fs.lstatSync(p); } catch { return false; }
      return !st.isSocket() && !p.endsWith('.tmp');
    },
  });
}

/**
 * Bring the folder to DATA_SCHEMA: 4b when it has no meta.json, then schema 2 → 3. Returns { migrated: false } when there
 * is nothing to do, else { migrated: true, backup, steps, ... } (`backup` of the last step). Throws MigrationError (with
 * the ids) when a step's verification fails.
 */
export function migrateData(store, { log = () => {}, at = nowIso(), stopAfter = null } = {}) {
  let meta = store.meta();
  if (meta && meta.schema > DATA_SCHEMA) throw new MigrationError(`data schema ${meta.schema} is newer than ${DATA_SCHEMA}: this mail server is too old for the folder`);
  if (meta && meta.schema === DATA_SCHEMA) return { migrated: false, meta };
  const steps = [];
  let out = {};
  if (!meta) { out = migrate4b(store, { log, at }); steps.push('4b'); meta = store.meta(); }
  if (meta && meta.schema === SCHEMA_2) { out = { ...out, ...migrate2to3(store, { log, at, stopAfter }) }; steps.push('2→3'); }
  return { migrated: true, steps, ...out };
}

/**
 * Migration 4b: a 0.4.x folder (no meta.json) to schema 2. Returns { backup, counts, notices_closed }. Throws
 * MigrationError when a record has no 4b row or the invariants fail after the write.
 */
export function migrate4b(store, { log = () => {}, at = nowIso() } = {}) {
  const backup = backupPath(store.dir, at);
  copyData(store.dir, backup);
  log(`migration 4b: backup ${backup}`);

  const messages = store.messages();
  const replies = new Map();
  for (const m of messages) {
    if (!m.reply_to) continue;
    if (!replies.has(m.reply_to)) replies.set(m.reply_to, []);
    replies.get(m.reply_to).push(m);
  }
  for (const list of replies.values()) list.sort((a, b) => String(a.created_at).localeCompare(String(b.created_at)));

  // Rows are decided on the 0.4 fields of every record before anything is written (an original reads its replies' old fields).
  const problems = [];
  const out = [];
  for (const m of messages) {
    const copy = structuredClone(m);
    const r = migrateRecord(copy, (replies.get(m.id) || []).map((x) => structuredClone(x)), at);
    if (!r.ok) problems.push({ id: m.id, problem: r.reason });
    else out.push(copy);
  }
  if (problems.length) throw new MigrationError(`migration 4b: ${problems.length} record(s) without a 4b row: ${problems.map((p) => p.id).join(', ')}`, problems, backup);
  for (const m of out) store.saveMessage(m);

  // Notices of messages the migration made final are closed (decision `migrated`).
  const byId = new Map(out.map((m) => [m.id, m]));
  let closed = 0;
  for (const n of store.notices((x) => x.state === 'open' && x.message_id)) {
    const m = byId.get(n.message_id);
    if (!m || !isFinal(m.status)) continue;
    Object.assign(n, { state: 'decided', decision: 'migrated', decided_at: at });
    store.saveNotice(n);
    closed++;
  }

  const after = store.messages();
  const counts = statusCounts(after);
  log(`migration 4b: ${after.length} messages ${JSON.stringify(counts)}; notices closed ${closed}`);
  const errors = verifyInvariants(after, store.notices());
  if (errors.length) {
    for (const e of errors) log(`migration 4b: invariant ${e.id}: ${e.problem}`);
    throw new MigrationError(`migration 4b: verification failed for ${errors.length} record(s): ${[...new Set(errors.map((e) => e.id))].join(', ')}; data in backup ${backup}`, errors, backup);
  }
  store.saveMeta({ schema: SCHEMA_2, migrated_at: at, backup });
  return { backup, counts, notices_closed: closed };
}

const NEW_TOPIC_FILE = /^[0-9a-f]{16}\.json$/;

/**
 * Schema 2 → 3 (mail model 3), idempotent: (1) backup `<data>.bak-2-<time>`; (2) each topic record without a brain key
 * (file `<project>__<topic>.json`) is written to its key file with `brain: null`, `machine: holder_machine` or `unknown`;
 * (3) each message without `to_brain` gets `to_brain`/`from_brain: null`, `to_machine`/`from_machine` from the record of
 * its topic (null without one); (4) recent rows `topic:<p>/<t>` go to `topic:/<p>/<t>`; (5) verification: every
 * non-final message with a machine has the key of an existing record, at most one open record per (brain, project,
 * topic); (6) the old files are removed; (7) meta.json schema 3, the last write. `stopAfter` (tests) throws after that
 * step number, as a stop in the middle would. Returns { backup, topics, messages, recent }.
 */
export function migrate2to3(store, { log = () => {}, at = nowIso(), stopAfter = null } = {}) {
  const stop = (n) => { if (stopAfter === n) throw new MigrationError(`migration 2→3: stopped after step ${n} (test)`); };
  const backup = backupPath(store.dir, at, '2');
  copyData(store.dir, backup);
  log(`migration 2→3: backup ${backup}`);
  stop(1);

  // (2) topics: old files are read, the key files written; old files stay until the verification passed.
  const tdir = store.dirs.topics;
  const oldTopicFiles = listJsonFiles(tdir).filter((f) => !NEW_TOPIC_FILE.test(f));
  let topics = 0;
  for (const f of oldTopicFiles) {
    const rec = readJson(path.join(tdir, f), null);
    if (!rec || !rec.project || !rec.topic) continue;
    const out = { ...rec, brain: null, brain_name: null, machine: normalizeMachine(rec.holder_machine) || 'unknown', moved_to: null };
    const prev = store.topic(topicKey(out));
    if (!prev || JSON.stringify(prev) !== JSON.stringify(out)) { store.saveTopic(out); topics++; }
  }
  stop(2);

  // (3) messages: the machine of the record of their topic (brain null: before model 3 every topic was one record).
  const records = listJsonFiles(tdir).filter((f) => NEW_TOPIC_FILE.test(f)).map((f) => readJson(path.join(tdir, f), null)).filter(Boolean);
  const machineOf = (p, t) => { const r = p && t ? records.find((x) => x.brain == null && x.project === p && x.topic === t) : null; return r ? r.machine : null; };
  let messages = 0;
  for (const m of store.messages((x) => !('to_brain' in x))) {
    Object.assign(m, { to_brain: null, to_machine: machineOf(m.project, m.topic), from_brain: null, from_machine: machineOf(m.from_project, m.from_topic) });
    store.saveMessage(m);
    messages++;
  }
  stop(3);

  // (4) recent rows of topics: `topic:<p>/<t>` → `topic:/<p>/<t>` (brain empty before model 3).
  const oldRecent = [];
  for (const f of listJsonFiles(store.dirs.threads)) {
    const key = decodeURIComponent(f.slice(0, -5));
    if (!key.startsWith('topic:') || key.split('/').length !== 2) continue;
    const rec = store.threadRecord(key);
    const newKey = `topic:/${key.slice(6)}`;
    const dest = store.threadRecord(newKey);
    const seen = new Set((dest.recent || []).map((r) => r.seq));
    store.saveThreadRecord({ ...dest, key: newKey, cursor: Math.max(dest.cursor || 0, rec.cursor || 0), recent: [...(dest.recent || []), ...(rec.recent || []).filter((r) => !seen.has(r.seq))].sort((a, b) => a.seq - b.seq) });
    oldRecent.push(key);
  }
  stop(4);

  // (5) verification over the new records and messages.
  const keyed = listJsonFiles(tdir).filter((f) => NEW_TOPIC_FILE.test(f)).map((f) => readJson(path.join(tdir, f), null)).filter(Boolean);
  const errors = verifySchema3(store.messages(), keyed);
  if (errors.length) {
    for (const e of errors) log(`migration 2→3: ${e.id}: ${e.problem}`);
    throw new MigrationError(`migration 2→3: verification failed for ${errors.length} record(s): ${[...new Set(errors.map((e) => e.id))].join(', ')}; data in backup ${backup}`, errors, backup);
  }
  // (6) the old files go only now; (7) meta.json last.
  for (const f of oldTopicFiles) { try { fs.unlinkSync(path.join(tdir, f)); } catch { /* gone */ } }
  for (const key of oldRecent) store.removeThreadRecord(key);
  stop(6);
  store.saveMeta({ schema: DATA_SCHEMA, migrated_at: at, backup });
  log(`migration 2→3: ${topics} topic(s), ${messages} message(s), ${oldRecent.length} recent record(s)`);
  return { backup, topics, messages, recent: oldRecent.length };
}

/**
 * Schema 3 verification: every non-final message with a recipient machine has the key of an existing record; at most one
 * open record per (brain, project, topic) (I11). Returns a list of { id, problem }.
 */
export function verifySchema3(messages, topics) {
  const errors = [];
  for (const m of messages) {
    if (isFinal(m.status) || !m.project || !m.topic) continue;
    if (!('to_brain' in m)) { errors.push({ id: m.id, problem: 'no to_brain' }); continue; }
    if (m.to_machine && !topics.some((t) => sameTopicKey(t, { brain: m.to_brain ?? null, machine: m.to_machine, project: m.project, topic: m.topic }))) {
      errors.push({ id: m.id, problem: `no topic record for ${m.to_brain ?? '-'}@${m.to_machine}:${m.project}/${m.topic}` });
    }
  }
  const open = new Map();
  for (const t of topics) {
    if (t.state === 'closed') continue;
    const k = JSON.stringify([t.brain ?? null, t.project, t.topic]);
    if (open.has(k)) errors.push({ id: `${t.project}/${t.topic}`, problem: `two open records (${open.get(k)}, ${t.machine})` });
    else open.set(k, t.machine);
  }
  return errors;
}
