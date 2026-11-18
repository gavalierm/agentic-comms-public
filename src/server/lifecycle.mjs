// Life cycle of a message (mail model 3; status model D-0087): one field `status`, written only by transition().
// Also the one-step migration of 0.4.x records (`state`/`outcome`/`acked_at`/`bounced_at`) to `status`.
import { HttpError, nowIso, FINAL_REPLY_STATE } from '../common/util.mjs';

/** Mail model spoken by this server; every request carries it in X-Comms-Model (else 426). Model 3: a topic is brain + machine + project + topic. */
export const MAIL_MODEL = 3;
export const MODEL_MISMATCH_TEXT = 'mail model 3 (topic = brain + machine + project + topic): update mail and workflow together';
/** Schema of the data folder (meta.json); a folder without meta.json is 0.4.x data, schema 2 has topics without brain; both are migrated at start. */
export const DATA_SCHEMA = 3;

export const STATUSES = ['held', 'pending', 'picked', 'waiting', 'done', 'discarded', 'superseded', 'undelivered'];
export const FINAL_STATUSES = ['done', 'discarded', 'superseded', 'undelivered'];
export const RESOLUTIONS = ['reply', 'settled', 'acked', 'read', 'withdrawn'];
export const STATUS_BY = ['thread', 'code', 'operator', 'migration'];
export const HISTORY_MAX = 20;

const NONFINAL = ['held', 'pending', 'picked', 'waiting'];

/**
 * Allowed transitions (section 1b). `from: null` = a new record. `resolution` only for `done`.
 * `op: migration` may set any status on a record that has none yet (4b).
 */
export const TRANSITIONS = [
  { op: 'send', from: [null], to: ['pending', 'held'] },
  { op: 'resolve', from: [null], to: ['pending'] }, // the reply record itself
  { op: 'decide', from: ['held'], to: ['pending', 'discarded'] },
  { op: 'decide', from: ['pending'], to: ['discarded'] }, // blocker / unregistered-persona notice: discard
  { op: 'receive', from: ['pending'], to: ['picked'] },
  { op: 'receive', from: ['pending'], to: ['done'], resolution: ['read'] },
  { op: 'resolve', from: ['picked', 'waiting'], to: ['waiting'] },
  { op: 'resolve', from: ['pending', 'picked', 'waiting'], to: ['done'], resolution: ['reply', 'settled'] },
  { op: 'resolve', from: ['picked'], to: ['done'], resolution: ['acked'] },
  { op: 'resolve', from: ['done'], to: ['done'], resolution: ['reply'] }, // cancels: X → Y
  { op: 'withdraw', from: NONFINAL, to: ['done'], resolution: ['withdrawn'] },
  { op: 'bounce', from: ['pending', 'held'], to: ['undelivered'] },
  { op: 'close', from: ['pending', 'held'], to: ['undelivered'] },
  { op: 'resolve', from: ['pending'], to: ['undelivered'] }, // the reply record: sender closed / no way back
  { op: 'supersede', from: ['pending'], to: ['superseded'] },
  { op: 'repair', from: ['pending', 'picked', 'waiting'], to: ['done'], resolution: ['reply'] },
  { op: 'repair', from: ['picked', 'waiting'], to: ['waiting'] },
];

export function isFinal(status) {
  return FINAL_STATUSES.includes(status);
}

/** Current state of a message as sent in a 409 body. */
export function stateOf(m) {
  return { status: m.status || null, resolution: m.resolution || null, answer_state: m.answer_state || null, answer_id: m.answer_id || null };
}

/** The transition rule for (from, to, op, resolution), or null. */
export function findTransition(from, to, op, resolution = null) {
  if (op === 'migration') return from == null && STATUSES.includes(to) ? { op, from: [null], to: [to] } : null;
  return TRANSITIONS.find((t) => t.op === op && t.from.includes(from ?? null) && t.to.includes(to)
    && (to === 'done' ? (t.resolution || []).includes(resolution) : resolution == null)) || null;
}

/**
 * The only writer of `status` (I1). Refuses a transition outside 1b with 409 and the current state. A final status
 * clears the sender's wait (I7); the caller sets a one-shot wait afterwards where 1b says so (undelivered).
 * Mutates and returns `m`; the caller stores it.
 */
export function transition(m, to, op, by, { resolution = null, at = nowIso(), note = null } = {}) {
  const from = m.status ?? null;
  if (!findTransition(from, to, op, resolution)) {
    const what = `${from || 'new'} → ${to}${resolution ? ` (${resolution})` : ''} by ${op}`;
    throw new HttpError(409, `prechod správy ${m.id} ${what} nie je povolený; súčasný stav ${from || '-'}${m.resolution ? ` (${m.resolution})` : ''}`, { id: m.id, ...stateOf(m) });
  }
  if (!STATUS_BY.includes(by)) throw new Error(`transition by ${by}`);
  m.status = to;
  m.resolution = to === 'done' ? resolution : null;
  m.status_at = at;
  m.status_by = by;
  const h = { at, from: from ?? (note || null), to: to === 'done' ? `done/${resolution}` : to, op, by };
  m.history = [...(m.history || []), h].slice(-HISTORY_MAX);
  if (isFinal(to)) clearWait(m);
  return m;
}

export function clearWait(m) {
  m.wait_due_at = null;
  m.wait_reason = null;
}

// --- migration 4b ---------------------------------------------------------------------------------------------
const OLD_FIELDS = ['state', 'acked_at', 'outcome', 'bounced_at'];

/**
 * Status of a 0.4.x record by the first matching row of the 4b table, or null (no row: verification fails).
 */
export function migratedStatus(m) {
  const outcome = m.outcome || (m.bounced_at ? 'undelivered' : (m.expired_at ? 'expired' : null));
  const picked = !!m.picked_at;
  // 0.4 wrote a real reply's state and id over a code answer but kept `answer_origin: code` (P14 on prod data: 6× hotovo,
  // 2× otázka with an answer_id): a code answer is only one without a reply id.
  if (m.answer_origin === 'code' && m.answer_id) m.answer_origin = 'reply';
  if (m.state === 'discarded') return { status: 'discarded' };
  if (m.outcome === 'superseded' || m.superseded_by) return { status: 'superseded' };
  if (m.state === 'held') return { status: 'held' };
  if (m.reply_to && (m.state === 'acked' || picked)) return { status: 'done', resolution: 'read' };
  if (m.reply_to) return { status: 'pending' };
  if (m.level === 'steer' && (m.state === 'acked' || picked)) return { status: 'done', resolution: 'read' };
  if (m.answer_state === FINAL_REPLY_STATE && m.answer_origin !== 'code') return { status: 'done', resolution: m.answer_origin === 'settled' ? 'settled' : 'reply' };
  if (m.answer_state && m.answer_origin !== 'code') return { status: 'waiting' };
  // A code answer on a message the recipient acked in 0.4 is done (acked), not owed (P14: 20 false owed by the table order).
  if (m.answer_origin === 'code' && m.state === 'acked') return { status: 'done', resolution: 'acked', code: true };
  if (m.answer_origin === 'code') return { status: picked ? 'picked' : 'pending', code: true };
  if (m.state === 'acked') return { status: 'done', resolution: 'acked' };
  if (picked && m.state === 'pending') return { status: 'picked' };
  if (outcome === 'undelivered') return { status: 'undelivered' };
  if (outcome === 'expired' && m.state === 'pending') return { status: 'pending', expired: true };
  if (m.state === 'pending') return { status: 'pending' };
  return null;
}

/**
 * Migrate one 0.4.x record in place. `replies` (sorted by created_at) fill the answer of an original stored before
 * answer states. Returns { ok, status, resolution } or { ok: false, reason }.
 */
export function migrateRecord(m, replies = [], at = nowIso()) {
  if (m.status) return { ok: true, status: m.status, resolution: m.resolution || null, skipped: true };
  if (m.level === 'info') m.level = 'assist';
  if (m.reply_to && !m.reply_state) m.reply_state = FINAL_REPLY_STATE;
  if (!m.reply_to && !m.answer_state && replies.length) {
    const pick = replies.find((r) => (r.reply_state || FINAL_REPLY_STATE) === FINAL_REPLY_STATE) || replies[replies.length - 1];
    m.answer_state = pick.reply_state || FINAL_REPLY_STATE;
    m.answer_at = pick.created_at;
    m.answer_id = pick.id;
    m.answer_origin = 'reply';
  }
  const row = migratedStatus(m);
  if (!row) return { ok: false, reason: `no 4b row for state=${m.state} outcome=${m.outcome || '-'}` };
  const from = `${m.state || '-'}/${m.outcome || (m.bounced_at ? 'bounced' : '-')}/${m.answer_state || '-'}`;
  if (row.code) {
    m.code_answer = m.answer_state;
    m.code_answer_at = m.answer_at || null;
    for (const k of ['answer_state', 'answer_at', 'answer_id', 'answer_origin']) delete m[k];
  }
  if (row.expired && !m.expired_at) m.expired_at = m.expires_at || at;
  if (row.status === 'undelivered' && !m.undelivered_at) m.undelivered_at = m.bounced_at || at;
  if (row.status === 'done' && row.resolution === 'acked') {
    Object.assign(m, { answer_state: FINAL_REPLY_STATE, answer_at: m.acked_at || at, answer_id: null, answer_origin: 'acked' });
  } else if (row.status === 'done' && row.resolution !== 'read' && !m.answer_origin) {
    m.answer_origin = row.resolution;
  }
  for (const k of OLD_FIELDS) delete m[k];
  transition(m, row.status, 'migration', 'migration', { resolution: row.resolution || null, at, note: from });
  return { ok: true, status: row.status, resolution: row.resolution || null };
}

/**
 * Invariants over a whole folder after migration (4b step 3); a normal start runs repair() only: I1 (valid status), I2/I8 (no reply or
 * steer owed), I3 (no undelivered that was picked), I6 (no final message with an open notice), no 0.4 field left.
 * Returns a list of { id, problem }.
 */
export function verifyInvariants(messages, notices) {
  const errors = [];
  const byId = new Map(messages.map((m) => [m.id, m]));
  for (const m of messages) {
    if (!STATUSES.includes(m.status)) { errors.push({ id: m.id, problem: `status ${m.status ?? 'missing'}` }); continue; }
    if (m.status === 'done' && !RESOLUTIONS.includes(m.resolution)) errors.push({ id: m.id, problem: `done without a valid resolution (${m.resolution ?? '-'})` });
    if (m.status !== 'done' && m.resolution) errors.push({ id: m.id, problem: `resolution ${m.resolution} on ${m.status}` });
    for (const k of OLD_FIELDS) if (k in m) errors.push({ id: m.id, problem: `old field ${k}` });
    if ((m.reply_to || m.level === 'steer') && ['picked', 'waiting'].includes(m.status)) errors.push({ id: m.id, problem: `${m.reply_to ? 'reply' : 'steer'} owed (${m.status})` });
    if (m.status === 'undelivered' && m.picked_at) errors.push({ id: m.id, problem: 'undelivered but picked' });
    if (m.wait_due_at && isFinal(m.status) && !(m.status === 'undelivered' && m.wait_reason === 'undelivered')) errors.push({ id: m.id, problem: `wait on final ${m.status}` });
  }
  for (const n of notices) {
    if (n.state !== 'open' || !n.message_id) continue;
    const m = byId.get(n.message_id);
    if (m && isFinal(m.status)) errors.push({ id: m.id, problem: `open notice ${n.id} (${n.kind}) on final ${m.status}` });
  }
  return errors;
}

/** Counts per status (done split by resolution) for the log. */
export function statusCounts(messages) {
  const out = {};
  for (const m of messages) {
    const k = m.status === 'done' ? `done/${m.resolution}` : String(m.status);
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}
