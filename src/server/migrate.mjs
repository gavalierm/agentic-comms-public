// One-step migration of a 0.4.x data folder to mail model 2 (design 4b): backup, records, notices, verification, meta.json.
// Runs before the service listens; on a failed verification the service does not start and the data stays in the backup.
import fs from 'node:fs';
import path from 'node:path';
import { nowIso } from '../common/util.mjs';
import { DATA_SCHEMA, migrateRecord, verifyInvariants, statusCounts, isFinal } from './lifecycle.mjs';

export class MigrationError extends Error {
  constructor(message, problems = [], backup = null) {
    super(message);
    this.code = 'MIGRATION_FAILED';
    this.problems = problems;
    this.backup = backup;
  }
}

/** Backup folder name: `<data>.bak-0.4-<time>` next to the data folder. */
export function backupPath(dataDir, at = nowIso()) {
  return `${path.resolve(dataDir)}.bak-0.4-${at.replace(/[-:.]/g, '')}`;
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
 * Migrate when the folder has no meta.json. Returns { migrated: false } when there is nothing to do, else
 * { migrated: true, backup, counts, notices_closed }. Throws MigrationError (with the ids) when a record has no 4b row or
 * the invariants fail after the write.
 */
export function migrateData(store, { log = () => {}, at = nowIso() } = {}) {
  const meta = store.meta();
  if (meta && meta.schema > DATA_SCHEMA) throw new MigrationError(`data schema ${meta.schema} is newer than ${DATA_SCHEMA}: this mail server is too old for the folder`);
  if (meta && meta.schema === DATA_SCHEMA) return { migrated: false, meta };
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
  store.saveMeta({ schema: DATA_SCHEMA, migrated_at: at, backup });
  return { migrated: true, backup, counts, notices_closed: closed };
}
