// Business logic of the mail service (mail model 3; status model D-0087): send / receive / resolve / withdraw / obligations /
// waits / notices / decide / register-topic / wake events / deadlines (TTL) / turns (keepalive). Independent of HTTP.
// The state of a message is its `status`, written only by transition() (lifecycle.mjs). A topic is the key brain + machine +
// project + topic (docs/MODEL.md, model 3); every lookup of a record goes through resolveKey() (moved_to).
import { EventEmitter } from 'node:events';
import { fileURLToPath } from 'node:url';
import { LEVELS, REPLY_STATES, FINAL_REPLY_STATE, LEVEL_REMOVED_TEXT, DEFAULT_TTL, MAX_TTL, isValidName, isValidThread, newId, nowIso, textHash, HttpError, PERSONA_MAIN_TOPIC, personaProject, normalizeMachine, parseDuration, discussionOpen, turnFresh, DISCUSSION_HELD_LEVELS, readJson } from '../common/util.mjs';
import { notifyOperator } from '../notify/index.mjs';
import { MAIL_MODEL, HISTORY_MAX, transition, findTransition, isFinal, stateOf } from './lifecycle.mjs';
import { topicKey, sameTopicKey } from './store.mjs';

export const SERVER_VERSION = readJson(fileURLToPath(new URL('../../package.json', import.meta.url)), {}).version || null;

export const NOTICE_OPTIONS = {
  'unregistered-topic': [
    { id: 'redirect', label: 'Presmerovať na tému', needsTarget: true },
    { id: 'deliver', label: 'Doručiť (téma už je založená)' },
    { id: 'discard', label: 'Zahodiť' },
  ],
  blocker: [
    { id: 'ack', label: 'Beriem na vedomie' },
    { id: 'discard', label: 'Zahodiť správu' },
  ],
  'wake-limit': [
    { id: 'ack', label: 'Beriem na vedomie' },
  ],
  // Message for a persona whose main thread never registered (typo or not born yet); the message keeps waiting.
  'unregistered-persona': [
    { id: 'ack', label: 'Beriem na vedomie (správa čaká na personu)' },
    { id: 'discard', label: 'Zahodiť správu' },
  ],
  // Keepalive could not resume a turn cut by a T3 restart (operator mode or 3 failed rings).
  'turn-interrupted': [
    { id: 'ack', label: 'Beriem na vedomie' },
  ],
  // A thread ended a turn again with the same next step after a continuation ring (no progress, P-026).
  'turn-stalled': [
    { id: 'ack', label: 'Beriem na vedomie' },
  ],
  // A message still pending after the wake's reminders (or without a holder, or too old to ring): nobody picks it up.
  unpicked: [
    { id: 'ack', label: 'Beriem na vedomie' },
    { id: 'discard', label: 'Zahodiť správu' },
  ],
  // A question to the operator (answer state `otázka`) has waited QUESTION_NOTICE_MS without a final reply (P-061).
  'question-open': [
    { id: 'ack', label: 'Beriem na vedomie' },
  ],
  // A brain registered a topic that a record from before model 3 (brain null) with open mail still holds (model 3): the new
  // record exists, the old one keeps its mail until the operator moves it (it may be another brain's mail).
  'legacy-topic': [
    { id: 'move', label: 'Presunúť poštu staršej témy do novej' },
    { id: 'ack', label: 'Nechať (staršia téma ostáva)' },
  ],
};

/** Notice kinds that a client (the wake module) may create through POST /notice. */
export const EXTERNAL_NOTICE_KINDS = ['wake-limit', 'turn-interrupted', 'turn-stalled', 'unpicked'];
/** Notice kinds about a thread, not a message (no `message_id`). */
const THREAD_NOTICE_KINDS = ['turn-interrupted', 'turn-stalled'];
/** Machine of a topic registered without any (no machine, holder machine or X-Comms-Machine): the migration's value too. */
export const UNKNOWN_MACHINE = 'unknown';
/** Longest chain of `moved_to` that resolveKey() follows. */
export const MOVE_MAX_STEPS = 5;
/** Deadlines per level (`ttl` in instance.json overrides, `ttl` of a message shortens): DEFAULT_TTL / MAX_TTL in util.
 * Every message except a reply has a deadline for its pickup; a reply has none. */
export { DEFAULT_TTL, MAX_TTL };
export const TTL_LEVELS = ['blocker', 'steer', 'assist'];
/** Urgency order: a reply is at least as urgent as the message it answers. */
export const LEVEL_RANK = { assist: 1, steer: 2, blocker: 3 };
export const EXPIRY_INTERVAL_MS = 30_000;
/** An answer in state `otázka` without a final reply after this long: one operator notice (P-061 risk: an endless question). */
const SYSTEM_NOTICE_BY = 'comms-service';
export const QUESTION_NOTICE_MS = 24 * 3600 * 1000;
export const TURN_STATES = ['open', 'closed'];
/** Longest stored next step of a closed turn (characters). */
export const MAX_NEXT = 300;
/** Latest timer of a closed turn (P-061): a resume time beyond this is clamped. 30 days: a paused topic (snooze) rings at its end. */
export const MAX_RESUME_MS = 30 * 24 * 3600 * 1000;
export const HOLDER_MODES = ['operator', 'auto'];
export const VISIBILITIES = ['public', 'private'];
/** Sender of the wake module's key: it reads and confirms every due wait; another key only those of its own messages. */
export const WAKE_SENDER = 'wake';

/**
 * Base of the sender's wait in seconds (1e): a new blocker 15 min, assist 60 min (steer: its pickup deadline only, then
 * like a blocker); interim `čakám` 60 min; `otázka` and a code answer 4 h. After each confirmed ring the next check is
 * base × 2^n; after WAIT_MAX_RINGS rings the wait ends.
 */
export const WAIT_BASE_S = { blocker: 15 * 60, steer: 15 * 60, assist: 60 * 60, 'čakám': 60 * 60, 'otázka': 4 * 3600, code: 4 * 3600 };
export const WAIT_MAX_RINGS = 4;
/** Wait reasons that ring once: the sender is told, then decides (the message cannot progress by itself). */
const ONE_SHOT_WAITS = ['undelivered', 'recipient-closed'];
export const OPS_MAX = 10;
export const RECENT_MAX = 200;
const OP_ID_RE = /^[A-Za-z0-9._:-]{1,100}$/;

/** Decision written onto an open notice when its message ends (I6). */
const NOTICE_DECISION = { reply: 'answered', settled: 'settled', acked: 'acked', read: 'read', withdrawn: 'withdrawn', discarded: 'discarded', undelivered: 'undelivered', superseded: 'superseded' };

/** Timer of a closed turn (P-061): ISO time within MAX_RESUME_MS of now, else clamped; invalid → null. */
export function resumeAt(value, now = new Date().toISOString()) {
  if (value == null || value === '') return null;
  const t = Date.parse(value);
  if (!Number.isFinite(t)) throw new HttpError(400, 'resume_at musí byť čas ISO 8601');
  const base = Date.parse(now);
  return new Date(Math.min(Math.max(t, base), base + MAX_RESUME_MS)).toISOString();
}

/** Discussion with the operator of a turn (D-0015): open|closed (or a boolean); absent → undefined. */
export function discussionFlag(value) {
  if (value === undefined || value === null || value === '') return undefined;
  if (value === true || value === 'open' || value === 'true') return true;
  if (value === false || value === 'closed' || value === 'false') return false;
  throw new HttpError(400, 'diskusia ťahu (discussion) musí byť open|closed');
}

export class Service {
  constructor(store, { log = () => {} } = {}) {
    this.store = store;
    this.log = log;
    this.events = new EventEmitter();
    this.events.setMaxListeners(0);
    this.seq = store.lastEventSeq();
    this.instance = store.instance();
    this.timer = null;
    this.opIndex = null;
    this.lastRecentSeq = 0;
  }

  /** Deadline check every 30 s; also once at start (deadlines survive a restart, nothing is re-armed). */
  startTimers(intervalMs = EXPIRY_INTERVAL_MS) {
    this.stopTimers();
    const tick = () => {
      try { this.checkExpired(); } catch (e) { this.log(`expiry check failed: ${e.message}`); }
      try { this.checkOpenQuestions(); } catch (e) { this.log(`open question check failed: ${e.message}`); }
    };
    tick();
    this.timer = setInterval(tick, intervalMs);
    this.timer.unref?.();
  }

  stopTimers() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  get instanceId() {
    return this.instance.instance_id;
  }

  /**
   * Record which mail client and workflow version a sender (persona) runs on a machine (headers X-Comms-Client,
   * X-Workflow-Version, X-Comms-Machine). A call without X-Workflow-Version keeps the recorded workflow version. The file
   * is rewritten only when a value changes or `last_seen` is older than a minute.
   */
  noteClient(sender, headers = {}, now = Date.now()) {
    try {
      const h = (k) => { const v = headers[k]; return v == null || v === '' ? null : String(v).slice(0, 80); };
      const machine = normalizeMachine(h('x-comms-machine')) || 'unknown';
      const cache = this.clientSeen || (this.clientSeen = new Map());
      const key = `${sender}\u0000${machine}`;
      const prev = cache.get(key) || this.store.client(sender, machine);
      const rec = { sender, machine, comms_version: h('x-comms-client'), workflow_version: h('x-workflow-version') ?? prev?.workflow_version ?? null };
      const same = prev && prev.comms_version === rec.comms_version && prev.workflow_version === rec.workflow_version;
      if (same && prev.last_seen && now - Date.parse(prev.last_seen) < 60_000) return prev;
      const out = { ...rec, first_seen: (same && prev.first_seen) || new Date(now).toISOString(), last_seen: new Date(now).toISOString() };
      this.store.saveClient(out);
      cache.set(key, out);
      return out;
    } catch (e) {
      this.log(`noteClient: ${e.message}`);
      return null;
    }
  }

  /** Recorded client versions, newest first. */
  clients() {
    return this.store.clients().sort((a, b) => String(b.last_seen).localeCompare(String(a.last_seen)));
  }

  health() {
    return { ok: true, instance_id: this.instanceId, instance: this.instance.instance, pid: process.pid, time: nowIso(), version: SERVER_VERSION, model: MAIL_MODEL };
  }

  serverInfo() {
    return { version: SERVER_VERSION, model: MAIL_MODEL, instance_id: this.instanceId };
  }

  // --- state changes ---------------------------------------------------------------
  /**
   * Apply a transition (lifecycle.transition) and store the message. A final status closes the message's open notices
   * (I6); the wait was cleared by transition (I7).
   */
  apply(m, to, op, by, opts = {}) {
    transition(m, to, op, by, opts);
    this.store.saveMessage(m);
    if (isFinal(m.status)) this.closeNotices(m);
    return m;
  }

  /** Close the open notices of a message that reached a final status (decision by its resolution / status). */
  closeNotices(m, decision = NOTICE_DECISION[m.status === 'done' ? m.resolution : m.status] || m.status) {
    const out = [];
    for (const n of this.store.notices((x) => x.state === 'open' && x.message_id === m.id)) {
      Object.assign(n, { state: 'decided', decision, decided_at: nowIso() });
      this.store.saveNotice(n);
      out.push(n.id);
    }
    return out;
  }

  /** The sender has a way back (its thread or its source topic): only then it waits and is rung. */
  hasWayBack(m) {
    return !!(m.thread_id || (m.from_project && m.from_topic));
  }

  /** Start (or restart) the sender's wait: due at `from + base`, counter 0. */
  setWait(m, baseS, reason, from = Date.now()) {
    if (!this.hasWayBack(m) || m.reply_to) return;
    m.wait_base_s = baseS;
    m.wait_n = 0;
    m.wait_reason = reason;
    m.wait_due_at = new Date(from + baseS * 1000).toISOString();
  }

  /** The sender should hear now (deadline, undelivered, recipient closed); a wait due earlier stays. */
  waitDueNow(m, reason, now = Date.now()) {
    if (!this.hasWayBack(m) || m.reply_to) return;
    if (!m.wait_base_s) m.wait_base_s = WAIT_BASE_S[m.level] || WAIT_BASE_S.blocker;
    if (m.wait_n == null) m.wait_n = 0;
    const at = new Date(now).toISOString();
    if (!m.wait_due_at || m.wait_due_at > at || ONE_SHOT_WAITS.includes(reason)) m.wait_due_at = at;
    m.wait_reason = reason;
  }

  // --- client_op_id (outbox idempotency) --------------------------------------------------
  checkOpId(id) {
    if (id == null || id === '') return null;
    if (typeof id !== 'string' || !OP_ID_RE.test(id)) throw new HttpError(400, 'client_op_id: 1–100 znakov z písmen, číslic a ._:-');
    return id;
  }

  /** The stored confirmation of an operation already done, or null. Another operation with the same id is 409. */
  replayOp(m, opId, op) {
    if (!opId || !m) return null;
    const rec = (m.ops || []).find((o) => o.client_op_id === opId);
    if (!rec) return null;
    if (rec.op !== op) throw new HttpError(409, `client_op_id ${opId} patrí inej operácii (${rec.op}) správy ${m.id}`, { id: m.id, ...stateOf(m) });
    return { ...rec.result, replayed: true };
  }

  recordOp(m, opId, op, result) {
    if (!opId) return;
    m.ops = [...(m.ops || []), { client_op_id: opId, op, at: nowIso(), result }].slice(-OPS_MAX);
    if (op === 'send') this.opsIndex().set(opId, m.id);
  }

  /** client_op_id of sends → message id (built once from the stored messages). */
  opsIndex() {
    if (!this.opIndex) {
      this.opIndex = new Map();
      for (const m of this.store.messages((x) => Array.isArray(x.ops))) {
        for (const o of m.ops) if (o.op === 'send') this.opIndex.set(o.client_op_id, m.id);
      }
    }
    return this.opIndex;
  }

  // --- recent rows (section 2) ----------------------------------------------------------
  nextRecentSeq() {
    this.lastRecentSeq = Math.max(Date.now() * 1000, this.lastRecentSeq + 1);
    return this.lastRecentSeq;
  }

  /**
   * Key of the thread record that receives rows about a message for its sender: the sender's thread, else its source topic
   * `topic:<brain>/<project>/<topic>` (no machine: a move keeps the rows; the brain of the record the key resolves to).
   */
  senderKey(m) {
    if (m.thread_id) return m.thread_id;
    if (m.from_project && m.from_topic) {
      const rec = this.resolveKey(senderTopicKey(m));
      return recentTopicKey(rec ? rec.brain : m.from_brain, m.from_project, m.from_topic);
    }
    return null;
  }

  /** Key for the recipient: the thread it is addressed to, else its mailbox topic. */
  recipientKey(m) {
    if (m.to_thread) return m.to_thread;
    if (m.project && m.topic) return recentTopicKey(m.to_brain, m.project, m.topic);
    return null;
  }

  addRecent(key, row) {
    if (!key) return null;
    const rec = this.store.threadRecord(key);
    const full = { seq: this.nextRecentSeq(), at: nowIso(), ...row };
    rec.recent = [...(rec.recent || []), full].slice(-RECENT_MAX);
    this.store.saveThreadRecord(rec);
    return full;
  }

  // --- topics ------------------------------------------------------------------
  /** Open records of (brain, project, topic), newest first (I11: at most one; two only inside a move stopped midway). */
  openTopics(brain, project, topic) {
    return this.store.topics().filter((t) => (t.brain ?? null) === (brain ?? null) && t.project === project && t.topic === topic && t.state !== 'closed')
      .sort(byUpdatedDesc);
  }

  openTopic(brain, project, topic) {
    return this.openTopics(brain, project, topic)[0] || null;
  }

  /** Follow `moved_to` (at most MOVE_MAX_STEPS) from a record; the last record reached. */
  followMoved(rec) {
    for (let i = 0; rec && rec.moved_to && i < MOVE_MAX_STEPS; i++) {
      const next = this.store.topic(rec.moved_to);
      if (!next) break;
      rec = next;
    }
    return rec || null;
  }

  /**
   * The one resolver of a key (model 3): key → its record, followed along `moved_to`. A key without its record (no machine,
   * or a machine the record never had) is the open record of the same brain, project and topic (I11), else a moved one of
   * them. Every lookup of a topic record goes through here.
   */
  resolveKey(key) {
    if (!key || !key.project || !key.topic) return null;
    let rec = key.machine ? this.store.topic(key) : null;
    // No record, or one closed without a move (the topic was closed here and later registered anew elsewhere): the open
    // record of the same brain, project and topic wins (I11: at most one), else this record, else the newest of them.
    if (!rec || (rec.state === 'closed' && !rec.moved_to)) {
      const same = this.store.topics().filter((t) => (t.brain ?? null) === (key.brain ?? null) && t.project === key.project && t.topic === key.topic);
      rec = same.filter((t) => t.state !== 'closed').sort(byUpdatedDesc)[0] || rec || same.sort(byUpdatedDesc)[0] || null;
    }
    return this.followMoved(rec);
  }

  /**
   * Address resolution of `send` and `decide --target` (model 3): project + topic, optional `brain` (id or a unique
   * brain_name) and `machine`. (1) brain and machine: that topic; (2) brain: the open topic of the brain; (3) no brain: the
   * open topic of the sender's own brain, else the only open topic of another brain (`resolved: <brain_name>@<machine>`),
   * more → 409 `ambiguous`, nothing written. No open topic: a closed record (send answers 410) or null (unregistered).
   * Returns { rec, brain, resolved }.
   */
  resolveAddress({ brain = null, machine = null, project, topic }, fromBrain = null) {
    const all = this.store.topics().filter((t) => t.project === project && t.topic === topic);
    const live = (list) => list.filter((t) => t.state !== 'closed').sort(byUpdatedDesc);
    const closed = (list) => list.filter((t) => t.state === 'closed' && !t.moved_to).sort(byUpdatedDesc)[0] || null;
    if (present(brain)) {
      let id = String(brain);
      if (!all.some((t) => t.brain === id)) {
        const named = [...new Set(all.filter((t) => t.brain && t.brain_name === id).map((t) => t.brain))];
        if (named.length > 1) throw ambiguousError(project, topic, live(all.filter((t) => named.includes(t.brain))));
        if (named.length === 1) id = named[0];
      }
      const mine = all.filter((t) => t.brain === id);
      if (present(machine)) return { rec: this.followMoved(this.store.topic({ brain: id, machine: normalizeMachine(machine), project, topic })), brain: id, resolved: null };
      return { rec: live(mine)[0] || closed(mine), brain: id, resolved: null };
    }
    if (fromBrain != null) {
      const own = live(all.filter((t) => t.brain === fromBrain));
      if (own.length) return { rec: own[0], brain: fromBrain, resolved: null };
    }
    const others = live(all.filter((t) => fromBrain == null || t.brain !== fromBrain));
    const brains = [...new Set(others.map((t) => t.brain ?? null))];
    if (brains.length > 1) throw ambiguousError(project, topic, others);
    if (others.length) return { rec: others[0], brain: others[0].brain ?? null, resolved: whereLabel(others[0]) };
    const own = fromBrain != null ? closed(all.filter((t) => t.brain === fromBrain)) : null;
    return { rec: own || closed(all), brain: fromBrain, resolved: null };
  }

  /**
   * Register a topic. `main: true` with `persona` registers the persona's main thread
   * (`@<persona>/main`, P-045). `persona` = owner of the topic; `visibility: private` hides
   * the topic from senders other than the owner (P-041).
   *
   * Model 3: the record is the key brain + machine + project + topic. `brain` (body or X-Comms-Brain) is required; the
   * machine is `machine`, else `holder_machine`, else X-Comms-Machine (else `unknown`). At most one open record per
   * (brain, project, topic) across machines (I11): registering on another machine moves the topic (same holder without
   * takeover; another holder only with `takeover`, else 409): new record, the mailbox's non-final messages, the recent rows,
   * then the old record closed with `moved_to`. A record from before model 3 (brain null) is claimed the same way only by
   * its holder or when it has no open mail; else a new record and an operator notice (`legacy-topic`).
   *
   * Holder: a registration that would change a non-null holder (including clearing it with
   * an absent holder) is refused with 409 unless `takeover: true`; the same holder is
   * idempotent. A release is `release: true`: it clears the holder when no holder is given
   * or it is the current one (else 409 unless `takeover`). Fields that are not passed keep
   * their stored values; holder_mode / holder_machine belong to the holder, so they are kept
   * only while the holder stays the same.
   *
   * Close is `close: true` (the topic was closed with done and archived): like a release (own holder or
   * takeover), plus `state: 'closed'` and `closed_at`. A closed topic takes no new mail (send answers 410). Its
   * mailbox: `pending`/`held` → `undelivered` (I3), `picked`/`waiting` stay and their senders' waits are due now
   * (`recipient-closed`, I9). Any later registration without `close` reopens the topic.
   *
   * `sender` (authenticated key) must be the persona when the instance has a key for that
   * persona; without such a key the persona is not managed by keys (admin mode) and anyone may register it.
   */
  registerTopic(body = {}, sender = null, ctx = {}) {
    let { project, topic, subject, holder = null, holder_mode = null, holder_machine = null, persona = null, visibility = null, main = false, takeover = false, release = false, close = false } = body;
    if (main && close) throw new HttpError(400, 'hlavné vlákno persony (main) sa neuzatvára');
    if (persona != null) requireName(persona, 'persona');
    if (main) {
      if (!persona) throw new HttpError(400, 'hlavné vlákno (main) vyžaduje personu');
      if (project || topic) throw new HttpError(400, 'pri main sa project a topic nezadávajú');
      project = personaProject(persona);
      topic = PERSONA_MAIN_TOPIC;
    } else {
      requireName(project, 'project');
      requireName(topic, 'topic');
    }
    if (holder_mode != null && !HOLDER_MODES.includes(holder_mode)) throw new HttpError(400, `režim držiteľa (holder_mode) musí byť ${HOLDER_MODES.join('|')}`);
    if (visibility != null && !VISIBILITIES.includes(visibility)) throw new HttpError(400, `viditeľnosť (visibility) musí byť ${VISIBILITIES.join('|')}`);
    const brain = requestBrain(body.brain, ctx);
    if (!brain) throw new HttpError(400, 'registrácia témy vyžaduje brain (X-Comms-Brain alebo brain): téma je brain + stroj + projekt + téma (model 3)');
    const open = this.openTopic(brain, project, topic);
    // A registration that names no machine at all keeps the open record's machine (no move); `unknown` only without one.
    const machine = normalizeMachine(body.machine) || normalizeMachine(holder_machine) || normalizeMachine(ctx.machine) || (open ? open.machine : UNKNOWN_MACHINE);
    const exact = this.store.topic({ brain, machine, project, topic });
    if (close && !open && !exact) throw new HttpError(404, `téma ${project}/${topic} nie je registrovaná; nie je čo uzavrieť`);
    if (close) release = true;
    const newHolder = release ? null : (holder || null);

    // The record this registration continues (`prior`) and, for a move, the record it moves from (`source`).
    let prior = null;
    let source = null;
    let legacy = null;
    if (release) {
      prior = open || exact; // a release or close acts on the brain's open record wherever it is (no move)
    } else if (open && open.machine !== machine) {
      source = open; // I11: the open topic of this brain on another machine moves here
    } else if (open) {
      prior = open;
    } else {
      legacy = this.openTopic(null, project, topic);
      if (legacy && ((newHolder && legacy.holder === newHolder) || !this.hasOpenMail(legacy))) { source = legacy; legacy = null; }
      prior = exact;
    }
    const base = source || prior;
    const prevHolder = base && base.holder ? base.holder : null;
    const sameHolder = newHolder != null && newHolder === prevHolder;
    // A record from before model 3 is claimed by its holder or when it has no open mail (no takeover: it has no brain).
    const legacyClaim = !!source && source.brain == null;
    if (prevHolder && newHolder !== prevHolder && !legacyClaim) {
      const ownRelease = release && (!holder || holder === prevHolder);
      if (!ownRelease && !takeover) throw new HttpError(409, `téma ${project}/${topic} už má držiteľa ${prevHolder}${source && source.machine !== machine ? ` na stroji ${source.machine}` : ''}; prepísanie alebo uvoľnenie cudzieho držiteľa vyžaduje takeover: true`);
    }

    const effPersona = persona != null ? persona : (base && base.persona) || null;
    const effVisibility = main ? null : (visibility != null ? visibility : (base && base.visibility) || 'public');
    if (effVisibility === 'private' && !effPersona) throw new HttpError(400, 'súkromná téma musí mať personu (vlastníka)');
    if (effPersona) this.requirePersonaSender(effPersona, sender);

    const subjectGiven = typeof subject === 'string' && subject.trim() !== '';
    const subjectOut = subjectGiven ? subject.trim() : (base && base.subject) || (main ? `main thread of ${persona}` : null);
    if (!subjectOut) throw new HttpError(400, 'téma musí mať predmet (subject)');

    const key = release && prior ? topicKey(prior) : { brain, machine, project, topic };
    const now = nowIso();
    const rec = {
      brain: key.brain, brain_name: brainNameOf(body.brain_name ?? ctx.brain_name) || (base && base.brain === key.brain ? base.brain_name || null : null),
      machine: key.machine, project, topic, subject: subjectOut,
      holder: newHolder,
      holder_mode: newHolder ? (holder_mode != null ? holder_mode : (sameHolder ? base.holder_mode : null)) : null,
      // A move to another machine rings the holder there: its machine, unless the registration names the holder's.
      holder_machine: newHolder ? (holder_machine != null ? normalizeMachine(holder_machine) : (source && source.machine !== key.machine ? key.machine : (sameHolder ? base.holder_machine : null))) : null,
      persona: effPersona, visibility: effVisibility,
      registered_at: base ? base.registered_at : now,
      updated_at: now,
      state: close ? 'closed' : 'open',
      closed_at: close ? (prior && prior.state === 'closed' && prior.closed_at ? prior.closed_at : now) : null,
      // A registration that (re)opens a record clears its moved_to; a move names its source for repair().
      moved_to: null,
      moved_from: source ? topicKey(source) : null,
    };
    this.store.saveTopic(rec);
    const out = { registered: true, created: !base, topic: rec };
    if (source) {
      out.moved = this.moveTopicMail(source, rec);
      out.moved_from = whereLabel(source);
      this.log(`topic ${project}/${topic} moved ${whereLabel(source)} → ${whereLabel(rec)}: ${out.moved.length} message(s)`);
    }
    if (legacy) out.legacy_notice_id = this.legacyNotice(legacy, rec, sender);
    if (main) {
      // Mail for the persona before any main existed (to_brain null, no machine) is taken by the first registration of main.
      for (const m of this.store.messages((x) => !isFinal(x.status) && x.status !== 'held' && x.project === project && x.topic === topic && x.to_brain == null && x.to_machine == null)) {
        this.moveMessage(m, rec, { event: false });
      }
      this.closePersonaNotices(persona);
    }
    if (!close) return out;
    return { ...out, created: false, closed: true, ...this.closeTopicMail(rec, prevHolder) };
  }

  /** The record's mailbox has a non-final message. */
  hasOpenMail(rec) {
    return this.store.messages((m) => !isFinal(m.status) && m.project === rec.project && m.topic === rec.topic && sameTopicKey(recipientTopicKey(m), rec)).length > 0;
  }

  /**
   * A move of a topic (I11, legacy claim), after the new record is written: (2) every non-final message of the old
   * mailbox gets the new key (status unchanged, history `move`; idempotent per message), (3) the recent rows go to the new
   * brain's key, (4) the old record is closed with `moved_to`, the last write. Returns the ids moved.
   */
  moveTopicMail(oldRec, newRec) {
    const oldKey = topicKey(oldRec);
    const moved = [];
    for (const m of this.store.messages((x) => !isFinal(x.status) && x.project === oldKey.project && x.topic === oldKey.topic && sameTopicKey(recipientTopicKey(x), oldKey))) {
      this.moveMessage(m, newRec);
      moved.push(m.id);
    }
    const from = recentTopicKey(oldKey.brain, oldKey.project, oldKey.topic);
    const to = recentTopicKey(newRec.brain, newRec.project, newRec.topic);
    if (from !== to) {
      const a = this.store.threadRecord(from);
      if ((a.recent || []).length) {
        const b = this.store.threadRecord(to);
        const seen = new Set((b.recent || []).map((r) => r.seq));
        b.recent = [...(b.recent || []), ...a.recent.filter((r) => !seen.has(r.seq))].sort((x, y) => x.seq - y.seq).slice(-RECENT_MAX);
        this.store.saveThreadRecord(b);
      }
      this.store.removeThreadRecord(from);
    }
    const now = nowIso();
    Object.assign(oldRec, { state: 'closed', closed_at: now, updated_at: now, holder: null, holder_mode: null, holder_machine: null, moved_to: topicKey(newRec), moved_from: null });
    this.store.saveTopic(oldRec);
    return moved;
  }

  /** One message to another key (a move): `status` unchanged, history `move`; a pending one rings the new holder. */
  moveMessage(m, rec, { event = true } = {}) {
    const at = nowIso();
    m.to_brain = rec.brain ?? null;
    m.to_machine = rec.machine ?? null;
    m.to_brain_name = rec.brain_name || null;
    m.history = [...(m.history || []), { at, from: m.status, to: m.status, op: 'move', by: 'code' }].slice(-HISTORY_MAX);
    this.store.saveMessage(m);
    if (event && m.status === 'pending') {
      const orig = m.reply_to ? this.store.message(m.reply_to) : null;
      this.emitEvent({
        type: m.reply_to ? 'reply' : m.level, ...eventBase(m),
        ...(m.reply_to ? { reply_to: m.reply_to, reply_state: m.reply_state || null, original_level: orig ? orig.level : null } : {}),
        holder: m.to_thread || rec.holder || null, holder_mode: m.to_thread ? null : rec.holder_mode || null,
        holder_machine: m.to_thread ? m.to_thread_machine || null : rec.holder_machine || null, moved: true,
      });
    }
    return m;
  }

  /** Operator notice: a brain registered a topic whose record from before model 3 still holds open mail (one per record). */
  legacyNotice(legacy, rec, sender) {
    const dedupe_key = `legacy:${keyText(topicKey(legacy))}`;
    const open = this.store.notices((n) => n.state === 'open' && n.kind === 'legacy-topic' && n.dedupe_key === dedupe_key);
    if (open.length) return open[0].id;
    const n = this.createNotice({
      kind: 'legacy-topic',
      title: `Staršia téma ${rec.project}/${rec.topic} (bez brainu, ${legacy.machine}) má otvorenú poštu; ${whereLabel(rec)} dostal novú tému`,
      message: { id: null, project: rec.project, topic: rec.topic, level: null, sender, thread_id: rec.holder || null, to_brain: rec.brain, to_machine: rec.machine, to_brain_name: rec.brain_name },
      extra: { dedupe_key, legacy_key: topicKey(legacy), new_key: topicKey(rec), detail: `presunúť = pošta staršej témy (držiteľ ${legacy.holder || '-'}) pôjde do ${whereLabel(rec)}; nechať = ostane, rozlíšenie bez brainu ju počíta ako tému iného brainu` },
    });
    this.log(`legacy topic ${rec.project}/${rec.topic} (${legacy.machine}) has open mail: new record ${whereLabel(rec)}, notice ${n.id}`);
    return n.id;
  }

  /**
   * Mail of a closed topic: its mailbox, and messages to its last holder's thread without a topic of their own when that
   * thread holds no other open topic (it still reads its mail). `pending`/`held` → `undelivered`; `picked`/`waiting`
   * stay, their senders' waits are due now (`recipient-closed`).
   */
  closeTopicMail(rec, prevHolder) {
    const undelivered = [];
    const recipientClosed = [];
    const threadGone = prevHolder && !this.store.topics().some((t) => t.holder === prevHolder && t.state !== 'closed' && !sameTopicKey(t, rec));
    const inBox = (m) => (m.project === rec.project && m.topic === rec.topic && sameTopicKey(recipientTopicKey(m), rec)) || (threadGone && m.to_thread === prevHolder && !m.topic);
    const reason = `topic ${rec.project}/${rec.topic} closed ${rec.closed_at}`;
    for (const m of this.store.messages((x) => inBox(x) && !isFinal(x.status))) {
      if (m.status === 'pending' || m.status === 'held') {
        this.undeliver(m, 'close', { holder: prevHolder, reason });
        undelivered.push(m.id);
      } else if (!m.reply_to) {
        this.waitDueNow(m, 'recipient-closed');
        this.store.saveMessage(m);
        recipientClosed.push(m.id);
        this.log(`recipient closed ${m.id} (${m.status}) ${rec.project}/${rec.topic}`);
      }
    }
    return { undelivered, recipient_closed: recipientClosed };
  }

  /**
   * Persona P's topics and main thread are registered only by the key of P, when the instance
   * has a key for P (keys.json sender === P). Without such a key the persona is unmanaged: any sender may register it.
   */
  requirePersonaSender(persona, sender) {
    if (sender == null) return;
    const managed = this.store.keys().some((k) => k.sender === persona);
    if (managed && sender !== persona) throw new HttpError(403, `personu ${persona} môže registrovať iba jej kľúč (odosielateľ ${sender})`);
  }

  /** The persona's main thread exists now: open `unregistered-persona` notices are settled automatically. */
  closePersonaNotices(persona) {
    for (const n of this.store.notices((x) => x.state === 'open' && x.kind === 'unregistered-persona' && x.persona === persona)) {
      n.state = 'decided';
      n.decision = 'registered';
      n.decided_at = nowIso();
      this.store.saveNotice(n);
    }
  }

  // --- send ----------------------------------------------------------------------
  /** A new message (never a reply: a reply is `resolve`). `client_op_id` repeated → the original confirmation. */
  send(sender, body = {}, ctx = {}) {
    if (!sender) throw new HttpError(401, 'neplatný kľúč');
    const opId = this.checkOpId(body.client_op_id);
    if (opId) {
      const prevId = this.opsIndex().get(opId);
      const prev = prevId ? this.store.message(prevId) : null;
      const replay = this.replayOp(prev, opId, 'send');
      if (replay) return replay;
    }
    if (present(body.reply_to)) throw new HttpError(400, 'odpoveď sa posiela cez resolve (id pôvodnej správy, stav, text), nie cez send s reply_to');
    if (present(body.reply_state)) throw new HttpError(400, 'stav odpovede (reply_state) patrí resolve, nie send');
    if (present(body.cancels)) throw new HttpError(400, 'zrušenie konečnej odpovede ide cez resolve s cancels na pôvodnú správu');
    return this.deliver(sender, body, opId, ctx);
  }

  deliver(sender, body, opId, ctx = {}) {
    const { project = null, topic = null, persona = null, maintainer = false, to_thread = null, level, text, thread_id = null, thread_machine = null, from_project = null, from_topic = null, ttl = null, supersede_key = null } = body;
    // Model 3: the sender's brain and machine (a queued item carries those of its time, else the headers); the target's
    // `brain` (id or brain_name) and `machine` are optional (address resolution, resolveAddress).
    // An explicit null (a queued item of a sender without a brain) is no brain, not the flushing client's header.
    const fromBrain = body.from_brain === null ? null : requestBrain(body.from_brain, ctx);
    const fromMachine = body.from_machine === null ? null : normalizeMachine(present(body.from_machine) ? body.from_machine : ctx.machine);
    const addr = { brain: present(body.brain) ? requireBrainValue(body.brain) : null, machine: present(body.machine) ? normalizeMachine(body.machine) : null };
    if (level === 'info') throw new HttpError(400, LEVEL_REMOVED_TEXT);
    if (level == null || level === '') throw new HttpError(400, `úroveň (level) je povinná: ${LEVELS.join('|')}`);
    if (!LEVELS.includes(level)) throw new HttpError(400, `neznáma úroveň „${level}“; platné úrovne: ${LEVELS.join('|')}`);
    if (typeof text !== 'string' || !text.trim()) throw new HttpError(400, 'text správy je povinný');
    if (thread_id != null && thread_id !== '') requireThread(thread_id);
    const ttl_s = this.resolveTtl(level, ttl);
    const created_at = nowIso();
    const msg = {
      id: newId(), project: null, topic: null, persona: null, ref_project: null, ref_topic: null, to_thread: null,
      to_brain: null, to_machine: null, to_brain_name: null, from_brain: fromBrain, from_machine: fromMachine,
      level, text, sender, thread_id: thread_id || null, thread_machine: normalizeMachine(thread_machine), from_project, from_topic, reply_to: null,
      reply_state: null, wait_for: null, cancels: null,
      supersede_key: present(supersede_key) ? String(supersede_key).slice(0, 200) : null,
      created_at, notice_id: null, ttl_s, expires_at: new Date(Date.parse(created_at) + ttl_s * 1000).toISOString(), expired_at: null,
      picked_at: null, picked_by_thread: null,
    };
    // A blocker and an assist wait for an answer from the start (1b); a steer only for its pickup.
    if (level !== 'steer') this.setWait(msg, WAIT_BASE_S[level], 'no-reply', Date.parse(created_at));
    let resolved = null;

    if (present(to_thread)) {
      // Thread address (operator 2026-11-18): a new message to a thread by its id, only from the maintainer's thread; the
      // machine comes from what the server knows of that thread, so wake rings it where it runs (else it would bounce).
      if (maintainer === true || project != null || topic != null || persona != null) throw new HttpError(400, 'adresa vlákna (to_thread) sa nekombinuje s maintainer, project, topic ani persona');
      requireThread(to_thread);
      const mt = this.maintainerThread();
      // The thread and, when the instance names it, the machine of the maintainer (both declared, as every thread mailbox).
      if (!mt || !thread_id || thread_id !== mt.thread || (mt.machine && normalizeMachine(thread_machine) !== normalizeMachine(mt.machine))) throw new HttpError(403, 'správu vláknu podľa ID (to_thread) posiela iba vlákno správcu (instance.json maintainer.thread a maintainer.machine)');
      const machine = this.threadMachine(to_thread);
      if (!machine) throw new HttpError(404, `vlákno ${to_thread} tento server nepozná (nemá záznam ťahu ani od neho správu): nie je kde zazvoniť; vlákno musí najprv otvoriť ťah alebo poslať správu cez tento server`);
      msg.to_thread = to_thread;
      msg.to_thread_machine = machine;
      transition(msg, 'pending', 'send', 'thread', { at: created_at });
      this.store.saveMessage(msg);
      this.afterDelivery(msg);
      const result = { id: msg.id, status: msg.status, level, notice_id: msg.notice_id, expires_at: msg.expires_at, wait_due_at: msg.wait_due_at || null, to_thread: msg.to_thread };
      this.recordOp(msg, opId, 'send', result);
      this.store.saveMessage(msg);
      this.log(`thread ${msg.id} → vlákno ${msg.to_thread} (${machine})`);
      return result;
    }
    if (maintainer === true) {
      // Maintainer address (operator 2026-11-16): the T3 thread of the workflow maintainer, set as maintainer.thread in
      // instance.json (read at every send, no restart). No thread set: refused, the sender files an issue instead. A
      // thread that is archived or gone bounces in wake (undelivered), so the sender learns that too.
      if (project != null || topic != null || persona != null) throw new HttpError(400, 'adresa správcu (maintainer) sa nekombinuje s project, topic ani persona');
      const mt = this.maintainerThread();
      if (!mt) throw new HttpError(404, 'na tomto serveri nie je nastavené vlákno správcu (instance.json maintainer.thread)');
      msg.to_thread = mt.thread;
      msg.to_thread_machine = mt.machine;
      transition(msg, 'pending', 'send', 'thread', { at: created_at });
      this.store.saveMessage(msg);
      this.afterDelivery(msg);
      const result = { id: msg.id, status: msg.status, level, notice_id: msg.notice_id, expires_at: msg.expires_at, wait_due_at: msg.wait_due_at || null, to_thread: msg.to_thread };
      this.recordOp(msg, opId, 'send', result);
      this.store.saveMessage(msg);
      this.log(`maintainer ${msg.id} → vlákno ${msg.to_thread}`);
      return result;
    }
    if (persona != null) {
      // Persona address (P-045): persona [+ project [+ topic]]. Delivered to the topic only when the persona owns it (and,
      // if private, the sender is the owner); everything else goes to the persona's main thread, which triages.
      requireName(persona, 'persona');
      if (topic != null && project == null) throw new HttpError(400, 'téma (topic) vyžaduje projekt (project)');
      if (project != null) requireName(project, 'project');
      if (topic != null) requireName(topic, 'topic');
      const t = project && topic ? this.personaTopic(persona, project, topic, addr, fromBrain) : null;
      if (!(t && (t.visibility !== 'private' || sender === persona))) {
        return this.toPersonaMain(msg, persona, { project, topic }, opId, fromBrain);
      }
      msg.project = project;
      msg.topic = topic;
      msg.persona = persona;
      setRecipient(msg, t);
    } else {
      requireName(project, 'project');
      requireName(topic, 'topic');
      msg.project = project;
      msg.topic = topic;
      const r = this.resolveAddress({ ...addr, project, topic }, fromBrain);
      const t = r.rec;
      refuseClosed(t, 'téma je uzavretá');
      // `<persona>/main` without `@` means the persona's main thread when the persona has one and no such topic exists.
      if (!t && topic === PERSONA_MAIN_TOPIC && isValidName(project) && this.store.topics().some((x) => x.project === personaProject(project) && x.topic === PERSONA_MAIN_TOPIC)) {
        return this.toPersonaMain(msg, project, {}, opId, fromBrain);
      }
      if (!t || (t.visibility === 'private' && sender !== t.persona)) {
        // Unregistered topic, or a private topic addressed by someone else than its owner: the same answer as for an
        // unknown topic (no oracle), held + notice, never create the topic (P-045). Model 3: to_brain is the brain asked
        // (else the sender's), no machine; `address` keeps what the sender gave, so a later delivery resolves the same way.
        if (t) msg.private_owner = t.persona;
        msg.to_brain = t ? t.brain ?? null : (r.brain ?? fromBrain);
        msg.to_machine = null;
        msg.to_brain_name = t ? t.brain_name || null : (msg.to_brain === fromBrain ? brainNameOf(ctx.brain_name) : null);
        msg.address = addr;
        transition(msg, 'held', 'send', 'thread', { at: created_at });
        const notice = this.createNotice({
          kind: 'unregistered-topic',
          title: t ? `Súkromná téma persony ${t.persona}: ${project}/${topic} (doručiť = do hlavného vlákna persony)` : `Nová téma? / presmerovať: ${project}/${topic}`,
          message: msg,
        });
        msg.notice_id = notice.id;
        const result = { id: msg.id, status: msg.status, level, notice_id: notice.id, expires_at: msg.expires_at, wait_due_at: msg.wait_due_at || null, project_known: this.projectKnown(project, fromBrain) };
        this.recordOp(msg, opId, 'send', result);
        this.store.saveMessage(msg);
        this.log(`held ${msg.id} for ${t ? 'private' : 'unregistered'} ${project}/${topic}`);
        return result;
      }
      setRecipient(msg, t);
      resolved = r.resolved;
    }
    transition(msg, 'pending', 'send', 'thread', { at: created_at });
    this.store.saveMessage(msg);
    this.afterDelivery(msg);
    const result = { id: msg.id, status: msg.status, level, notice_id: msg.notice_id, expires_at: msg.expires_at, wait_due_at: msg.wait_due_at || null, ...(resolved ? { resolved } : {}) };
    this.recordOp(msg, opId, 'send', result);
    this.store.saveMessage(msg);
    return result;
  }

  /**
   * The workflow maintainer's T3 thread from instance.json `maintainer` ({ thread, machine }), read fresh at every call
   * so a new thread takes effect without a restart; null when none is set.
   */
  /** Machine of a thread this server knows: its turn record, else its newest message as a sender; null when unknown. */
  threadMachine(thread) {
    const t = this.store.turn(thread);
    if (t && t.machine) return normalizeMachine(t.machine);
    let best = null;
    for (const m of this.store.messages((x) => x.thread_id === thread && x.thread_machine)) if (!best || String(m.created_at) > String(best.created_at)) best = m;
    return best ? normalizeMachine(best.thread_machine) : null;
  }

  maintainerThread() {
    let inst = null;
    try { inst = this.store.instance(); } catch { inst = this.instance; }
    const m = inst && typeof inst.maintainer === 'object' && inst.maintainer ? inst.maintainer : {};
    const thread = typeof m.thread === 'string' && m.thread.trim() ? m.thread.trim() : null;
    if (!thread) return null;
    try { requireThread(thread); } catch { this.log(`neplatné maintainer.thread „${thread}“ v instance.json`); return null; }
    return { thread, machine: m.machine ? normalizeMachine(m.machine) : null };
  }

  /**
   * The project has at least one open registered topic in the sender's brain (D-0024: the sender of a held message learns
   * that someone serves the project; model 3: only its own brain counts). It depends on the project only, never on the
   * addressed topic (no oracle, P-045).
   */
  projectKnown(project, fromBrain = null) {
    return this.store.topics().some((t) => t && t.project === project && t.state !== 'closed' && (t.brain ?? null) === (fromBrain ?? null));
  }

  /**
   * Topic of a persona address (persona + project + topic): an open record the persona owns (the given brain only, when
   * named), the sender's brain first, else the only brain that has one; else null (the message goes to the persona's main).
   */
  personaTopic(persona, project, topic, addr = {}, fromBrain = null) {
    const list = this.store.topics().filter((t) => t.project === project && t.topic === topic && t.state !== 'closed' && t.persona === persona
      && (!addr.brain || t.brain === addr.brain || t.brain_name === addr.brain) && (!addr.machine || t.machine === addr.machine)).sort(byUpdatedDesc);
    const own = fromBrain != null ? list.find((t) => t.brain === fromBrain) : null;
    if (own) return own;
    return new Set(list.map((t) => t.brain ?? null)).size === 1 ? list[0] : null;
  }

  /**
   * Deadline in seconds (for the pickup) of a new message: explicit `ttl` (shortens, never extends), else `ttl.<level>`
   * of instance.json, else DEFAULT_TTL. Explicit 0 or more than MAX_TTL → 400; an instance.json value of 0, invalid or
   * over the maximum falls back to the default / maximum (logged).
   */
  resolveTtl(level, ttl) {
    if (ttl != null && ttl !== '') {
      const s = parseDuration(ttl);
      if (s == null) throw new HttpError(400, `neplatná lehota (ttl) „${ttl}“: napr. 90s, 30m, 4h alebo počet sekúnd`);
      if (s <= 0) throw new HttpError(400, `lehota (ttl) musí byť kladná; ${level} najviac ${MAX_TTL[level]}`);
      if (s > parseDuration(MAX_TTL[level])) throw new HttpError(400, `lehota (ttl) „${ttl}“ je dlhšia ako maximum ${MAX_TTL[level]} pre ${level}`);
      return s;
    }
    const cfg = (this.instance.ttl && this.instance.ttl[level] != null) ? this.instance.ttl[level] : DEFAULT_TTL[level];
    const s = parseDuration(cfg);
    const max = parseDuration(MAX_TTL[level]);
    if (s == null || s <= 0) { this.log(`neplatná lehota ttl.${level} „${cfg}“ v instance.json; použité ${DEFAULT_TTL[level]}`); return parseDuration(DEFAULT_TTL[level]); }
    if (s > max) { this.log(`lehota ttl.${level} „${cfg}“ v instance.json je nad maximom; použité ${MAX_TTL[level]}`); return max; }
    return s;
  }

  /**
   * Deliver into the persona's main mailbox. Always accepted and pending, also when the persona (its main thread) does not
   * exist yet: it picks the mail up after (re)birth. A persona that never registered gets one open operator notice
   * (typo guard), the message keeps waiting. A held message delivered here by the operator moves `held → pending`.
   */
  toPersonaMain(msg, persona, ref = {}, opId = null, fromBrain = null) {
    // Model 3: the persona's main is its name and its brain (own brain first, else the only one; more → 409 ambiguous).
    // Without a main the message has no brain and no machine; the first registration of main takes it.
    const main = this.resolveAddress({ project: personaProject(persona), topic: PERSONA_MAIN_TOPIC }, fromBrain).rec;
    const live = main && main.state !== 'closed' ? main : null;
    msg.project = personaProject(persona);
    msg.topic = PERSONA_MAIN_TOPIC;
    msg.persona = persona;
    msg.ref_project = ref.project || null;
    msg.ref_topic = ref.topic || null;
    delete msg.address;
    setRecipient(msg, live);
    if (msg.status === 'held') transition(msg, 'pending', 'decide', 'operator');
    else transition(msg, 'pending', 'send', 'thread', { at: msg.created_at });
    this.store.saveMessage(msg);
    let warning = null;
    if (!live) {
      warning = `persona ${persona} has no main thread yet; the message waits`;
      const open = this.store.notices((n) => n.state === 'open' && n.kind === 'unregistered-persona' && n.persona === persona);
      if (!open.length) {
        this.createNotice({ kind: 'unregistered-persona', title: `Neznáma persona? ${persona} (správa čaká)`, message: msg, extra: { persona } });
      }
    }
    this.afterDelivery(msg);
    const result = { id: msg.id, status: msg.status, level: msg.level, notice_id: msg.notice_id, expires_at: msg.expires_at || null, wait_due_at: msg.wait_due_at || null, ...(warning ? { warning } : {}) };
    this.recordOp(msg, opId, 'send', result);
    this.store.saveMessage(msg);
    return result;
  }

  /**
   * `supersede_key`: a new message of the same sender to the same address with the same key replaces the earlier one
   * nobody picked up (also an expired one): `pending → superseded`. Its open operator notice moves to the new message.
   */
  supersede(msg) {
    const same = (m) => m.id !== msg.id && m.status === 'pending' && !m.reply_to && m.sender === msg.sender
      && m.supersede_key === msg.supersede_key && m.project === msg.project && m.topic === msg.topic && (m.persona || null) === (msg.persona || null)
      && (m.to_brain ?? null) === (msg.to_brain ?? null) && (m.to_machine ?? null) === (msg.to_machine ?? null)
      && (m.to_thread || null) === (msg.to_thread || null);
    for (const old of this.store.messages(same)) {
      const notice = old.notice_id && !msg.notice_id ? this.store.notice(old.notice_id) : null;
      if (notice && notice.state === 'open') {
        notice.message_id = msg.id;
        this.store.saveNotice(notice);
        msg.notice_id = notice.id;
        this.store.saveMessage(msg);
      }
      Object.assign(old, { superseded_by: msg.id, superseded_at: nowIso() });
      this.apply(old, 'superseded', 'supersede', 'thread');
      this.log(`superseded ${old.id} by ${msg.id} (key ${msg.supersede_key})`);
    }
  }

  /** Wake events and operator notices once a new message is pending in a mailbox. */
  afterDelivery(msg) {
    if (msg.supersede_key) this.supersede(msg);
    const t = msg.to_thread ? null : this.resolveKey(recipientTopicKey(msg));
    // Events never carry the body (P-043); `text_hash` lets the wake module detect repeats. A message addressed to a
    // thread (maintainer) rings that thread on its machine.
    this.emitEvent({
      type: msg.level, ...eventBase(msg),
      ...(msg.to_thread
        ? { holder: msg.to_thread, holder_mode: null, holder_machine: msg.to_thread_machine || null }
        : { holder: t ? t.holder : null, holder_mode: t ? t.holder_mode || null : null, holder_machine: t ? t.holder_machine || null : null }),
    });
    // A keyed blocker (repeated by a script) raises its notice only when nobody picked it up: checkExpired.
    if (msg.level === 'blocker' && !msg.supersede_key && !msg.notice_id) {
      const notice = this.createNotice({ kind: 'blocker', title: msg.to_thread ? `Blocker pre vlákno ${msg.to_thread}` : `Blocker pre tému ${msg.project}/${msg.topic}`, message: msg });
      msg.notice_id = notice.id;
      this.store.saveMessage(msg);
    }
  }

  /** Event of a delivered reply for the original's sender. */
  replyEvent(reply, original) {
    const t = reply.project && reply.topic ? this.resolveKey(recipientTopicKey(reply)) : null;
    return this.emitEvent({
      type: 'reply', ...eventBase(reply), reply_to: original.id, reply_state: reply.reply_state, original_level: original.level,
      holder: original.thread_id || (t ? t.holder : null), holder_mode: original.thread_id ? null : (t ? t.holder_mode || null : null),
      holder_machine: original.thread_id ? original.thread_machine || null : (t ? t.holder_machine || null : null),
    });
  }

  // --- mailboxes -------------------------------------------------------------------
  /**
   * The mailbox a request names: thread, project+topic or persona (its main thread). null when none. Model 3: a topic
   * mailbox is the full key; `brain` and `machine` of the query, else the request's (X-Comms-Brain, X-Comms-Machine); a
   * request without a machine means the brain's open record (I11).
   */
  mailbox(q = {}, ctx = {}) {
    let { project = null, topic = null } = q;
    const { thread = null, persona = null } = q;
    if (present(persona)) {
      requireName(persona, 'persona');
      project = personaProject(persona);
      topic = PERSONA_MAIN_TOPIC;
    }
    if (present(thread)) requireThread(thread);
    const byTopic = present(project) && present(topic);
    if (!byTopic && !present(thread)) return null;
    let brain = null;
    let machine = null;
    if (byTopic) {
      // An empty `brain=` / `machine=` parameter means exactly none (wake asks the event's key, also a key without them).
      brain = q.brain === '' ? null : requestBrain(q.brain, ctx);
      if (q.machine === '') machine = null;
      else {
        machine = normalizeMachine(present(q.machine) ? q.machine : ctx.machine);
        if (!machine) { const open = this.openTopic(brain, project, topic); machine = open ? open.machine : UNKNOWN_MACHINE; }
      }
    }
    return { brain, machine, project: byTopic ? project : null, topic: byTopic ? topic : null, thread: present(thread) ? thread : null };
  }

  requireMailbox(q, ctx = {}) {
    const mb = this.mailbox(q, ctx);
    if (!mb) throw new HttpError(400, 'zadaj schránku: project a topic, persona alebo thread');
    return mb;
  }

  /** Model 3: a topic mailbox matches on the whole key (brain, machine, project, topic), never on project/topic alone. */
  inMailbox(m, mb) {
    if (!mb) return false;
    return (mb.thread && m.to_thread === mb.thread) || (mb.project && mb.topic && m.project === mb.project && m.topic === mb.topic && sameTopicKey(recipientTopicKey(m), mb)) || false;
  }

  /** Persona that owns the mailbox of a message (a reply: the original's sender; else the topic's persona). */
  mailboxOwner(m) {
    if (m.reply_to) {
      const o = this.store.message(m.reply_to);
      return o ? o.sender : null;
    }
    if (m.persona) return m.persona;
    const t = m.project && m.topic ? this.resolveKey(recipientTopicKey(m)) : null;
    return t ? t.persona || null : null;
  }

  /** The key may act for the mailbox of `m`: the mailbox matches and, when its owner persona has a key, it is that key. */
  actsForMailbox(m, mb, sender) {
    if (!this.inMailbox(m, mb)) return false;
    const owner = this.mailboxOwner(m);
    if (!owner || owner === sender) return true;
    return !this.store.keys().some((k) => k.sender === owner);
  }

  /** Body of a message is readable by its sender and by its mailbox (1d). */
  mayRead(m, mb, sender) {
    return m.sender === sender || this.actsForMailbox(m, mb, sender);
  }

  // --- receive -------------------------------------------------------------------
  /** Pending messages of a mailbox (I2: inbox = status pending), optionally only the given ids. No state change. */
  pending(q, ids = null, ctx = {}) {
    const mb = this.requireMailbox(q, ctx);
    const list = this.store.messages((m) => m.status === 'pending' && this.inMailbox(m, mb));
    const want = Array.isArray(ids) && ids.length ? new Set(ids.map(String)) : null;
    const out = want ? list.filter((m) => want.has(m.id)) : list;
    out.sort((a, b) => a.created_at.localeCompare(b.created_at) || a.id.localeCompare(b.id));
    return out;
  }

  /**
   * Read the bodies of pending messages: the PICKUP (`pending → picked`, `picked_at`, `picked_by_thread`); a reply and a
   * steer need no answer and end `done (read)`. `ids` limits the read to those messages.
   */
  receive(q, ctx = {}) {
    const list = this.pending(q, q.ids, ctx);
    return list.map((m) => publicMessage(this.pickUp(m, q.thread || null)));
  }

  pickUp(m, thread) {
    if (m.status !== 'pending') return m;
    m.picked_at = nowIso();
    m.picked_by_thread = thread || null;
    if (m.reply_to || m.level === 'steer') this.apply(m, 'done', 'receive', 'thread', { resolution: 'read' });
    else this.apply(m, 'picked', 'receive', 'thread');
    return m;
  }

  /**
   * One message by id in any status (1d): its sender or its mailbox reads the body again (a lost HTTP answer loses
   * nothing; thread B of the topic reads what thread A picked up). The mailbox picks a pending message up.
   */
  receiveOne(id, q, sender, ctx = {}) {
    const m = this.store.message(String(id || ''));
    if (!m) throw new HttpError(404, `správa ${id} neexistuje`);
    const mb = this.mailbox(q, ctx);
    const forMailbox = this.actsForMailbox(m, mb, sender);
    if (!forMailbox && m.sender !== sender) throw new HttpError(403, `správa ${id} nie je vaša ani vo vašej schránke`);
    if (forMailbox && m.status === 'pending') this.pickUp(m, (mb && mb.thread) || null);
    return publicMessage(m);
  }

  /** Count without reading (no pickup): the inbox of a mailbox. */
  count(q, ctx = {}) {
    const list = this.pending(q, null, ctx);
    return {
      total: list.length, blocker: list.filter((m) => m.level === 'blocker').length, ids: list.map((m) => m.id),
      items: list.map((m) => ({
        id: m.id, level: m.level, reply_to: m.reply_to || null, reply_state: m.reply_state || null, cancels: m.cancels || null,
        code_answer: m.code_answer || null, expired_at: m.expired_at || null, to_thread: m.to_thread || null, sender: m.sender || null, created_at: m.created_at,
      })),
    };
  }

  // --- resolve / withdraw ------------------------------------------------------------
  /**
   * The one operation that answers or settles a message (1b, 1d). `state`: `hotovo` (final; with text a reply, without
   * text settled, `ack: true` acked from `picked` only) or `otázka`/`čakám` (interim, text required). `cancels: X`
   * replaces the final reply X of a `done` message with a new final reply. The caller names its mailbox (thread,
   * project+topic or persona); the message must be in it (403). Order of writes (I5): the reply, the cancelled reply, then
   * the original. A closed source topic of the sender does not stop it (I9): `answer_delivered: false`.
   */
  resolve(sender, body = {}, ctx = {}) {
    const { id = null, state = null, text = null, ack = false, cancels = null, thread_id = null, thread_machine = null, wait_for = null } = body;
    const opId = this.checkOpId(body.client_op_id);
    if (!present(id)) throw new HttpError(400, 'zadaj id správy');
    // An explicit null brain or machine (a queued resolve of a client without one) does not take the flushing client's.
    const mb = this.requireMailbox(body, { ...ctx, ...(body.brain === null ? { brain: null } : {}), ...(body.machine === null ? { machine: null } : {}) });
    const m = this.store.message(String(id));
    if (!m) throw new HttpError(404, `správa ${id} neexistuje`);
    if (!this.actsForMailbox(m, mb, sender)) throw new HttpError(403, `správa ${id} nie je vo vašej schránke: vybaviť ju smie iba jej príjemca`);
    const replay = this.replayOp(m, opId, 'resolve');
    if (replay) return replay;
    if (m.reply_to) throw new HttpError(400, `správa ${id} je odpoveď: odpoveď sa nevybavuje`);
    if (!REPLY_STATES.includes(state)) throw new HttpError(400, `stav (state) musí byť ${REPLY_STATES.join('|')}`);
    const hasText = typeof text === 'string' && text.trim() !== '';
    if (text != null && typeof text !== 'string') throw new HttpError(400, 'text musí byť reťazec');
    const isAck = ack === true || ack === 'true';
    if (!hasText && state !== FINAL_REPLY_STATE) throw new HttpError(400, `priebežná odpoveď (${state}) musí mať text`);
    if (isAck && (state !== FINAL_REPLY_STATE || hasText || present(cancels))) throw new HttpError(400, 'ack je vybavenie hotovo bez textu a bez cancels');
    if (present(thread_id)) requireThread(thread_id);
    // A reply to your own message would come straight back to your thread (bob 2026-11-01).
    const own = thread_id || mb.thread;
    if (own && m.thread_id === own) throw new HttpError(400, `správa ${id} je vaša vlastná: odpoveď by sa vrátila do toho istého vlákna`);

    let to;
    let resolution = null;
    let target = null;
    if (present(cancels)) {
      if (state !== FINAL_REPLY_STATE || !hasText) throw new HttpError(400, 'cancels nahrádza konečnú odpoveď: stav hotovo a text');
      if (m.status !== 'done' || m.resolution !== 'reply' || m.answer_id !== String(cancels)) {
        throw new HttpError(409, `cancels ${cancels}: nie je súčasná konečná odpoveď správy ${id}`, { id: m.id, ...stateOf(m) });
      }
      target = this.store.message(String(cancels));
      if (!target) throw new HttpError(404, `rušená odpoveď ${cancels} neexistuje`);
      if (target.sender !== sender) throw new HttpError(403, `odpoveď ${cancels} poslal niekto iný: zrušiť ju smie iba jej odosielateľ`);
      to = 'done'; resolution = 'reply';
    } else if (state === FINAL_REPLY_STATE) {
      to = 'done'; resolution = isAck ? 'acked' : (hasText ? 'reply' : 'settled');
      if (m.status === 'done') throw new HttpError(409, `správa ${id} je už vybavená (${m.resolution}${m.answer_id ? ` ${m.answer_id}` : ''})`, { id: m.id, ...stateOf(m) });
    } else {
      to = 'waiting';
    }
    // Checked before the reply is stored: a refused transition writes nothing.
    if (!findTransition(m.status, to, 'resolve', resolution)) {
      throw new HttpError(409, `správa ${id} (${m.status}${m.resolution ? `/${m.resolution}` : ''}) sa nedá vybaviť ako ${resolution || to}`, { id: m.id, ...stateOf(m) });
    }

    const now = nowIso();
    const firstAnswer = !m.answer_state;
    let reply = null;
    if (hasText) {
      reply = this.makeReply(sender, m, { state, text, thread_id, thread_machine, wait_for, cancels: target ? target.id : null, mb, at: now });
    }
    if (target) {
      target.cancelled_by = reply.id;
      target.cancelled_at = now;
      this.store.saveMessage(target);
      this.log(`reply ${target.id} cancelled by ${reply.id}`);
    }
    const delivered = reply ? reply.status === 'pending' : null;
    Object.assign(m, {
      answer_state: state, answer_at: now, answer_id: reply ? reply.id : null,
      answer_origin: isAck ? 'acked' : (hasText ? 'reply' : 'settled'),
      answer_delivered: delivered, answer_undelivered_reason: reply && !delivered ? reply.undelivered_reason : null,
    });
    if (to === 'waiting') {
      clearWaitFields(m);
      if (delivered) this.setWait(m, WAIT_BASE_S[state], 'check');
    }
    transition(m, to, 'resolve', 'thread', { resolution, at: now });
    const result = {
      id: m.id, status: m.status, resolution: m.resolution, answer_state: m.answer_state, answer_id: m.answer_id,
      answer_delivered: m.answer_delivered, ...(m.answer_undelivered_reason ? { reason: m.answer_undelivered_reason } : {}),
    };
    this.recordOp(m, opId, 'resolve', result);
    this.store.saveMessage(m);
    if (isFinal(m.status)) this.closeNotices(m);
    if (reply && delivered && (firstAnswer || state === FINAL_REPLY_STATE)) this.replyEvent(reply, m);
    if (!reply) this.addRecent(this.senderKey(m), { kind: resolution, id: m.id, level: m.level, line: `${resolution} ${m.id}` });
    this.log(`resolve ${m.id} → ${m.status}${m.resolution ? `/${m.resolution}` : ''}${reply ? ` reply ${reply.id}${delivered ? '' : ` undelivered (${reply.undelivered_reason})`}` : ''}`);
    return result;
  }

  /**
   * The reply record (to the original's sender); undeliverable at once when the sender's topic is closed or there is no way
   * back. Model 3: the sender's topic is resolveKey() of its key (along moved_to); a sender without a brain (before model 3)
   * gets the reply only through its old record (moved_to, else brain null), never into a guessed brain (`no-return-path`).
   */
  makeReply(sender, original, { state, text, thread_id, thread_machine, wait_for, cancels, mb, at }) {
    const hasTopic = !!(original.from_project && original.from_topic);
    const back = hasTopic ? this.resolveKey(senderTopicKey(original)) : null;
    const reply = {
      id: newId(), project: original.from_project || null, topic: original.from_topic || null, persona: null, ref_project: null, ref_topic: null,
      to_thread: original.thread_id || null,
      to_brain: back ? back.brain ?? null : original.from_brain ?? null, to_machine: back ? back.machine : original.from_machine ?? null, to_brain_name: back ? back.brain_name || null : null,
      from_brain: original.to_brain ?? null, from_machine: original.to_machine ?? null,
      level: original.level, text, sender,
      thread_id: thread_id || mb.thread || null, thread_machine: normalizeMachine(thread_machine),
      from_project: original.project || null, from_topic: original.topic || null, reply_to: original.id, reply_state: state,
      wait_for: present(wait_for) ? String(wait_for) : null, cancels: cancels || null, supersede_key: null,
      created_at: at, notice_id: null, ttl_s: null, expires_at: null, expired_at: null, picked_at: null, picked_by_thread: null,
    };
    transition(reply, 'pending', 'resolve', 'thread', { at });
    let why = null;
    if (!reply.to_thread && !hasTopic) why = 'no-return-path';
    else if (!reply.to_thread && !back && original.from_brain == null) why = 'no-return-path';
    else if (back && back.state === 'closed') why = 'sender-closed';
    if (why) {
      Object.assign(reply, { undelivered_at: at, undelivered_holder: null, undelivered_reason: why });
      transition(reply, 'undelivered', 'resolve', 'thread', { at });
    }
    this.store.saveMessage(reply);
    return reply;
  }

  /** The sender takes its message back (any non-final status): `done (withdrawn)`, the recipient gets a recent row. */
  withdraw(sender, body = {}) {
    const opId = this.checkOpId(body.client_op_id);
    if (!present(body.id)) throw new HttpError(400, 'zadaj id správy');
    const m = this.store.message(String(body.id));
    if (!m) throw new HttpError(404, `správa ${body.id} neexistuje`);
    if (m.sender !== sender) throw new HttpError(403, `správu ${m.id} môže stiahnuť iba jej odosielateľ`);
    const replay = this.replayOp(m, opId, 'withdraw');
    if (replay) return replay;
    if (isFinal(m.status)) throw new HttpError(409, `správa ${m.id} je už konečná (${m.status}${m.resolution ? `/${m.resolution}` : ''})`, { id: m.id, ...stateOf(m) });
    const wasOwed = m.status === 'picked' || m.status === 'waiting';
    transition(m, 'done', 'withdraw', 'thread', { resolution: 'withdrawn' });
    const result = { id: m.id, status: m.status, resolution: m.resolution };
    this.recordOp(m, opId, 'withdraw', result);
    this.store.saveMessage(m);
    this.closeNotices(m);
    this.addRecent(this.recipientKey(m), { kind: 'withdrawn', id: m.id, level: m.level, line: `withdrawn ${m.id}` });
    this.log(`withdrawn ${m.id}${wasOwed ? ' (owed)' : ''}`);
    return result;
  }

  /** State of one message (metadata, status, history); the body only for its sender and its mailbox (1d). */
  messageState(id, q = {}, sender = null, ctx = {}) {
    const m = this.store.message(String(id || ''));
    if (!m) throw new HttpError(404, `správa ${id} neexistuje`);
    const out = {
      id: m.id, status: m.status, resolution: m.resolution || null, status_at: m.status_at || null, status_by: m.status_by || null, history: m.history || [],
      level: m.level, reply_to: m.reply_to || null, reply_state: m.reply_state || null,
      answer_state: m.answer_state || null, answer_at: m.answer_at || null, answer_id: m.answer_id || null, answer_origin: m.answer_origin || null,
      answer_delivered: m.answer_delivered ?? null, answer_undelivered_reason: m.answer_undelivered_reason || null,
      cancels: m.cancels || null, cancelled_by: m.cancelled_by || null, project: m.project || null, topic: m.topic || null, to: addressOf(m),
      to_brain: m.to_brain ?? null, to_machine: m.to_machine ?? null, to_brain_name: m.to_brain_name || null, from_brain: m.from_brain ?? null, from_machine: m.from_machine ?? null,
      to_thread: m.to_thread || null, sender: m.sender, thread_id: m.thread_id || null, from_project: m.from_project || null, from_topic: m.from_topic || null,
      created_at: m.created_at, expires_at: m.expires_at || null, expired_at: m.expired_at || null, ttl_s: m.ttl_s || null,
      code_answer: m.code_answer || null, code_answer_at: m.code_answer_at || null, picked_at: m.picked_at || null, picked_by_thread: m.picked_by_thread || null,
      wait_due_at: m.wait_due_at || null, wait_n: m.wait_n ?? null, wait_reason: m.wait_reason || null, wait_rang_at: m.wait_rang_at || null,
      undelivered_at: m.undelivered_at || null, undelivered_holder: m.undelivered_holder || null, undelivered_reason: m.undelivered_reason || null,
    };
    // `held` without its reason was read as "the recipient is working" (flow-public#107): the only cause of `held` is an
    // address no thread receives; the kind of its operator notice says which.
    if (m.status === 'held') {
      out.notice_id = m.notice_id || null;
      out.held_reason = (m.notice_id && this.store.notice(m.notice_id)?.kind) || 'unregistered-topic';
      out.held_note = 'no thread receives this address (no such topic, or a private one): the message waits for the operator to route it (notice), nobody works on it';
    }
    if (this.mayRead(m, this.mailbox(q, ctx), sender)) out.text = m.text;
    return out;
  }

  // --- obligations (section 2) -----------------------------------------------------------
  /**
   * What a thread has to do and waits for, in one answer: `inbox` (pending), `owed` (picked/waiting of the mailboxes it
   * holds, I8), `awaiting` (its own non-final messages), `recent` rows after `since` (rows up to `since` are deleted and
   * the cursor stored), `topics`, `server`.
   */
  obligations(q = {}, ctx = {}) {
    const thread = present(q.thread) ? q.thread : null;
    if (thread) requireThread(thread);
    // Model 3: a mailbox is the full key; `topics` items and `persona` mean the brain and machine of the query (else the
    // request's; without a machine the brain's open record, I11). The recent rows of a topic are keyed without the machine.
    const brain = requestBrain(q.brain, ctx);
    const machineOf = (p, t) => normalizeMachine(present(q.machine) ? q.machine : ctx.machine) || (this.openTopic(brain, p, t) || {}).machine || UNKNOWN_MACHINE;
    const boxes = new Map();
    const addBox = (k) => boxes.set(keyText(k), k);
    const project = present(q.project) ? q.project : null;
    if (project) requireName(project, 'project');
    for (const raw of String(q.topics || '').split(',').map((s) => s.trim()).filter(Boolean)) {
      const [p, t] = raw.includes('/') ? raw.split('/') : [project, raw];
      if (!isValidName(p) || !isValidName(t)) throw new HttpError(400, `neplatná téma „${raw}“ v topics (project/topic alebo topic s project)`);
      addBox({ brain, machine: machineOf(p, t), project: p, topic: t });
    }
    if (present(q.persona)) {
      requireName(q.persona, 'persona');
      const p = personaProject(q.persona);
      addBox({ brain, machine: machineOf(p, PERSONA_MAIN_TOPIC), project: p, topic: PERSONA_MAIN_TOPIC });
    }
    const topicRecs = this.store.topics();
    if (thread) for (const t of topicRecs) if (t.holder === thread && t.state !== 'closed') addBox(topicKey(t));
    if (!thread && !boxes.size) throw new HttpError(400, 'zadaj thread alebo topics');
    const inBox = (m) => (thread && m.to_thread === thread) || (m.project && m.topic && boxes.has(keyText(recipientTopicKey(m))));
    // A sender's topic may have moved since it sent (I11): its key resolves along moved_to (once per key).
    const senderBox = new Map();
    const fromBox = (m) => {
      const k = keyText(senderTopicKey(m));
      if (boxes.has(k)) return true;
      if (!senderBox.has(k)) { const rec = this.resolveKey(senderTopicKey(m)); senderBox.set(k, !!rec && boxes.has(keyText(topicKey(rec)))); }
      return senderBox.get(k);
    };
    const fromMe = (m) => !m.reply_to && ((thread && m.thread_id === thread) || (m.from_project && m.from_topic && fromBox(m)));
    const byTime = (a, b) => a.created_at.localeCompare(b.created_at) || a.id.localeCompare(b.id);
    const all = this.store.messages();
    const inbox = all.filter((m) => m.status === 'pending' && inBox(m)).sort(byTime);
    const owed = all.filter((m) => (m.status === 'picked' || m.status === 'waiting') && !m.reply_to && m.level !== 'steer' && inBox(m)).sort(byTime);
    const awaiting = all.filter((m) => !isFinal(m.status) && fromMe(m)).sort(byTime);

    const keys = [...new Set([...(thread ? [thread] : []), ...[...boxes.values()].map((b) => recentTopicKey(b.brain, b.project, b.topic))])];
    const sinceGiven = q.since != null && q.since !== '';
    const since = sinceGiven ? Number(q.since) : null;
    if (sinceGiven && !Number.isFinite(since)) throw new HttpError(400, 'since musí byť číslo (cursor)');
    const recent = [];
    let cursor = since ?? 0;
    for (const key of keys) {
      const rec = this.store.threadRecord(key);
      const from = sinceGiven ? since : (rec.cursor || 0);
      for (const r of rec.recent || []) if (r.seq > from) recent.push({ ...r, ...(key.startsWith('topic:') ? { for_topic: key.slice(6) } : {}) });
      cursor = Math.max(cursor, rec.cursor || 0);
      if (sinceGiven) {
        // The client has seen everything up to `since`: older rows go, the thread keeps its cursor.
        const rows = rec.recent || [];
        const kept = rows.filter((r) => r.seq > since);
        const cursorMoved = key === thread && (rec.cursor || 0) < since;
        if (kept.length !== rows.length || cursorMoved) this.store.saveThreadRecord({ ...rec, cursor: Math.max(rec.cursor || 0, since), recent: kept });
      }
    }
    recent.sort((a, b) => a.seq - b.seq);
    if (recent.length) cursor = Math.max(cursor, recent[recent.length - 1].seq);
    return {
      inbox: { n: inbox.length, ids: inbox.map((m) => ({ id: m.id, level: m.level, code_answer: m.code_answer || null, reply_to: m.reply_to || null, reply_state: m.reply_state || null, expired_at: m.expired_at || null })) },
      owed: owed.map((m) => ({ id: m.id, level: m.level, from_topic: m.from_topic || null, from_project: m.from_project || null, sender: m.sender, status: m.status, picked_at: m.picked_at || null, picked_by_thread: m.picked_by_thread || null, answer_state: m.answer_state || null })),
      awaiting: awaiting.map((m) => ({ id: m.id, to: addressOf(m), level: m.level, status: m.status, answer_state: m.answer_state || null, code_answer: m.code_answer || null, wait_due_at: m.wait_due_at || null, wait_n: m.wait_n ?? null, wait_reason: m.wait_reason || null })),
      recent,
      cursor,
      topics: [...boxes.values()].map((b) => {
        const rec = topicRecs.find((x) => sameTopicKey(x, b));
        return {
          brain: b.brain, brain_name: rec ? rec.brain_name || null : null, machine: b.machine, project: b.project, topic: b.topic, registered: !!rec, state: rec ? rec.state || 'open' : null,
          holder: rec ? rec.holder || null : null, holder_mode: rec ? rec.holder_mode || null : null, closed_at: rec ? rec.closed_at || null : null, moved_to: rec && rec.moved_to ? rec.moved_to : null,
        };
      }),
      server: this.serverInfo(),
    };
  }

  /**
   * Who may read and confirm the wait of `m` (1e): the key `wake` (wake module of the service's machine) every wait, any
   * other key the waits of the messages it sent (a wake module on another machine with its persona's key, e.g. carol on
   * the Mac over TCP, rings the threads of that persona).
   */
  mayRing(m, sender) {
    return sender === WAKE_SENDER || (!!sender && m.sender === sender);
  }

  /** Waits due now (1e): wake rings each sender and confirms with wait-rang. Unconfirmed stays due regardless of age. */
  due(sender, now = Date.now()) {
    const at = new Date(now).toISOString();
    const list = this.store.messages((m) => m.wait_due_at && m.wait_due_at <= at && this.mayRing(m, sender));
    list.sort((a, b) => a.wait_due_at.localeCompare(b.wait_due_at) || a.id.localeCompare(b.id));
    return {
      due: list.map((m) => ({
        id: m.id, level: m.level, status: m.status, resolution: m.resolution || null, wait_due_at: m.wait_due_at, wait_n: m.wait_n ?? 0, wait_reason: m.wait_reason,
        answer_state: m.answer_state || null, code_answer: m.code_answer || null, project: m.project || null, topic: m.topic || null, to: addressOf(m),
        sender: m.sender, thread_id: m.thread_id || null, from_project: m.from_project || null, from_topic: m.from_topic || null,
        ...(this.senderHolder(m) || { holder: null, holder_machine: null, holder_mode: null }),
      })),
      server: this.serverInfo(),
    };
  }

  /**
   * Wake rang the sender (1e point 2): `wait_n += 1`, next check `rang_at + base × 2^n`; after WAIT_MAX_RINGS the wait
   * ends with a recent row. A one-shot wait (undelivered, recipient closed) ends at once. A confirmation of a wait that is
   * not due at `rang_at` (repeated confirmation) changes nothing. 409 for a final message without a wait.
   */
  waitRang(sender, { id = null, rang_at = null } = {}) {
    const m = this.store.message(String(id || ''));
    if (!m) throw new HttpError(404, `správa ${id} neexistuje`);
    if (!this.mayRing(m, sender)) throw new HttpError(403, `čakanie správy ${m.id} potvrdzuje kľúč ${WAKE_SENDER} alebo odosielateľ správy`);
    const at = present(rang_at) ? new Date(Date.parse(rang_at)) : new Date();
    if (!Number.isFinite(at.getTime())) throw new HttpError(400, 'rang_at musí byť čas ISO 8601');
    const rang = at.toISOString();
    const view = () => ({ id: m.id, status: m.status, wait_due_at: m.wait_due_at || null, wait_n: m.wait_n ?? 0, wait_reason: m.wait_reason || null });
    if (!m.wait_due_at) {
      if (isFinal(m.status)) throw new HttpError(409, `správa ${m.id} je konečná (${m.status}); čakanie už nie je`, { id: m.id, ...stateOf(m) });
      return view();
    }
    if (m.wait_due_at > rang) return view();
    m.wait_rang_at = rang;
    if (ONE_SHOT_WAITS.includes(m.wait_reason)) {
      m.wait_due_at = null;
    } else {
      m.wait_n = (m.wait_n || 0) + 1;
      if (m.wait_n >= WAIT_MAX_RINGS) {
        m.wait_due_at = null;
        this.addRecent(this.senderKey(m), { kind: 'no-answer', id: m.id, level: m.level, line: `no answer after ${WAIT_MAX_RINGS} checks ${m.id}` });
      } else {
        const base = m.wait_base_s || WAIT_BASE_S[m.level] || WAIT_BASE_S.blocker;
        m.wait_due_at = new Date(at.getTime() + base * 1000 * 2 ** m.wait_n).toISOString();
      }
    }
    this.store.saveMessage(m);
    return view();
  }

  /** Client 1.8.0 hands over a local wait (4b step 5): only the sender, only a non-final message. */
  waitAdopt(sender, { id = null, due_at = null, base_s = null } = {}) {
    const m = this.store.message(String(id || ''));
    if (!m) throw new HttpError(404, `správa ${id} neexistuje`);
    if (m.sender !== sender) throw new HttpError(403, `čakanie správy ${m.id} môže prevziať iba jej odosielateľ`);
    if (m.reply_to) throw new HttpError(400, `správa ${m.id} je odpoveď: na odpoveď sa nečaká`);
    if (isFinal(m.status)) throw new HttpError(409, `správa ${m.id} je konečná (${m.status})`, { id: m.id, ...stateOf(m) });
    const due = Date.parse(due_at);
    if (!Number.isFinite(due)) throw new HttpError(400, 'due_at musí byť čas ISO 8601');
    const base = base_s == null || base_s === '' ? null : Number(base_s);
    if (base != null && !(Number.isInteger(base) && base > 0)) throw new HttpError(400, 'base_s musí byť kladné celé číslo sekúnd');
    m.wait_due_at = new Date(due).toISOString();
    m.wait_base_s = base || m.wait_base_s || WAIT_BASE_S[m.answer_state] || WAIT_BASE_S[m.level] || WAIT_BASE_S.blocker;
    if (m.wait_n == null) m.wait_n = 0;
    if (!m.wait_reason) m.wait_reason = m.status === 'waiting' ? 'check' : 'no-reply';
    this.store.saveMessage(m);
    return { id: m.id, status: m.status, wait_due_at: m.wait_due_at, wait_base_s: m.wait_base_s, wait_n: m.wait_n, wait_reason: m.wait_reason };
  }

  /** Who should hear about a message of its sender: the sender's thread, else the holder of its source topic; null = no way back. */
  senderHolder(m) {
    if (m.thread_id) return { holder: m.thread_id, holder_machine: m.thread_machine || null, holder_mode: null };
    if (m.from_project && m.from_topic) {
      const t = this.resolveKey(senderTopicKey(m));
      if (t && t.holder) return { holder: t.holder, holder_machine: t.holder_machine || null, holder_mode: t.holder_mode || null };
    }
    return null;
  }

  // --- deadlines (TTL) ------------------------------------------------------------
  /**
   * Pending or held messages past `expires_at` (no `expired_at`, no `code_answer`). When the recipient lives but cannot
   * pick up (dialog with the operator, a running turn), the service writes `code_answer` (`otázka`/`busy`): the message
   * stays pending in the inbox (I2) and the sender's wait restarts as for an interim answer. Else `expired_at`: the
   * sender's wait is due now (`no-reply`). Returns the ids expired.
   */
  checkExpired(now = Date.now()) {
    const out = [];
    const at = new Date(now).toISOString();
    for (const m of this.store.messages((x) => (x.status === 'pending' || x.status === 'held') && !x.reply_to && x.expires_at && !x.expired_at && !x.code_answer)) {
      if (Date.parse(m.expires_at) > now) continue;
      const codeState = m.status === 'pending' ? this.recipientCodeAnswer(m, now) : null;
      if (codeState) {
        m.code_answer = codeState;
        m.code_answer_at = at;
        this.setWait(m, WAIT_BASE_S.code, 'check', now);
        this.store.saveMessage(m);
        const why = codeState === 'busy' ? 'recipient runs a turn, delivered after it' : 'recipient waits for the operator';
        this.log(`code answer ${codeState} ${m.id} (${m.level}) ${m.project}/${m.topic}: ${why}`);
        continue;
      }
      m.expired_at = at;
      this.waitDueNow(m, 'no-reply', now);
      // Keyed blocker nobody picked up: the operator notice appears now (one per chain, the replaced messages passed it on).
      if (m.level === 'blocker' && m.supersede_key && m.status === 'pending' && !m.notice_id) {
        m.notice_id = this.createNotice({ kind: 'blocker', title: `Blocker pre tému ${m.project}/${m.topic}`, message: m }).id;
      }
      this.store.saveMessage(m);
      this.log(`expired ${m.id} (${m.level}) ${m.project}/${m.topic}${m.wait_due_at ? ', sender wait due' : ', no way back (log only)'}`);
      out.push(m.id);
    }
    return out;
  }

  /** Thread that receives a message: the thread it is addressed to, else the holder of its topic. */
  recipientThread(m) {
    if (m.to_thread) return m.to_thread;
    const t = m.project && m.topic ? this.resolveKey(recipientTopicKey(m)) : null;
    return t && t.holder ? t.holder : null;
  }

  /**
   * Wake events of the messages still in a mailbox (pending) at least `olderMs` old, oldest first: the wake module's
   * reminder rings their recipient again (operator 2026-11-17, critical: a message nobody picks up is lost mail; a ring
   * dropped above the hourly limit, an event older than the wake's max age or a bell turn that could not read the mail
   * left messages pending for days). The shape of the delivery event (afterDelivery, replyEvent), never the body.
   */
  unpicked({ olderMs = 0, now = Date.now() } = {}) {
    const out = [];
    for (const m of this.store.messages((x) => x.status === 'pending')) {
      if (now - Date.parse(m.created_at) < olderMs) continue;
      const t = m.to_thread ? null : (m.project && m.topic ? this.resolveKey(recipientTopicKey(m)) : null);
      const orig = m.reply_to && m.to_thread ? this.store.message(m.reply_to) : null;
      out.push({
        type: m.reply_to ? 'reply' : m.level, ...eventBase(m), created_at: m.created_at,
        ...(m.reply_to ? { reply_to: m.reply_to, reply_state: m.reply_state || null } : {}),
        holder: m.to_thread || (t ? t.holder : null), holder_mode: m.to_thread ? null : (t ? t.holder_mode || null : null),
        holder_machine: m.to_thread ? (m.to_thread_machine || orig?.thread_machine || null) : (t ? t.holder_machine || null : null),
      });
    }
    return out.sort((a, b) => String(a.created_at).localeCompare(String(b.created_at)));
  }

  /**
   * Code answer for an unpicked message past its deadline, or null (`no reply`):
   * - `otázka`: the recipient waits for the operator: its open turn is in the dialog (`waiting_operator`), or, for a
   *   steer/assist, its last turn ended with a question (closed turn with `discussion`, D-0015, D-0022);
   * - `busy`: a steer/assist the recipient's open running turn holds back (`discussion` without `waiting_operator`).
   *   Only for a thread that runs: the service cannot read the thread's state, so an open record older than its
   *   deadline (TURN_BUSY_MAX_MS: interrupted, archived, Stop lost) gives no `busy` and the message expires (`no reply`).
   * A blocker passes the discussion, so its expiry stays `no reply`.
   */
  recipientCodeAnswer(m, now = Date.now()) {
    const thread = this.recipientThread(m);
    const turn = thread ? this.store.turn(thread) : null;
    if (turn && turn.state === 'open' && turn.waiting_operator) return 'otázka';
    if (!DISCUSSION_HELD_LEVELS.includes(m.level) || !discussionOpen(turn)) return null;
    if (turn.state !== 'open') return 'otázka';
    return turnFresh(turn, now) ? 'busy' : null;
  }

  /**
   * One operator notice per non-final message whose answer has stayed `otázka` (interim reply or code answer) for
   * QUESTION_NOTICE_MS (P-061: an endless question). Once: the message keeps `question_notice_id`.
   */
  checkOpenQuestions(now = Date.now()) {
    const out = [];
    const since = (m) => (m.status === 'waiting' && m.answer_state === 'otázka' ? m.answer_at : (m.status === 'pending' && m.code_answer === 'otázka' ? m.code_answer_at : null));
    for (const m of this.store.messages((x) => !x.reply_to && !x.question_notice_id && !isFinal(x.status) && since(x))) {
      const from = since(m);
      if (now - Date.parse(from) < QUESTION_NOTICE_MS) continue;
      const dedupe_key = `question:${m.id}`;
      const open = this.store.notices((n) => n.state === 'open' && n.kind === 'question-open' && n.dedupe_key === dedupe_key);
      const n = open[0] || this.createNotice({
        kind: 'question-open', title: `Otázka čaká na operátora viac ako 24 h: ${addressOf(m)}`, message: m, extra: { created_by: SYSTEM_NOTICE_BY, dedupe_key, detail: `odpoveď „otázka“ od ${from}, správa ${m.id}, bez konečnej odpovede` },
      });
      m.question_notice_id = n.id;
      this.store.saveMessage(m);
      this.log(`question open ${m.id}: notice ${n.id}`);
      out.push(m.id);
    }
    return out;
  }

  // --- bounce / undelivered ------------------------------------------------------------
  /**
   * The wake module could not ring the recipient for good (thread archived or gone). Only a message nobody picked up
   * (`pending`/`held`, I3) becomes `undelivered`; any other status is left as it is. A message of a topic that another
   * thread holds now is left too: the new holder receives it (undelivered only for a topic without another holder).
   */
  bounce({ message_id, holder = null, reason = null } = {}) {
    const m = this.store.message(message_id || '');
    if (!m) throw new HttpError(404, `správa ${message_id} neexistuje`);
    if (m.status !== 'pending' && m.status !== 'held') return { bounced: false, status: m.status, reason: `status ${m.status}` };
    if (holder && !m.to_thread && m.project && m.topic) {
      const t = this.resolveKey(recipientTopicKey(m));
      if (t && t.holder && t.holder !== String(holder)) return { bounced: false, status: m.status, reason: `holder ${t.holder}` };
    }
    this.undeliver(m, 'bounce', { holder, reason: reason || 'cannot receive messages' });
    return { bounced: true, status: m.status };
  }

  /** `pending`/`held` → `undelivered`; the sender's wait is due now once (`undelivered`), the notice is closed (I6). */
  undeliver(m, op, { holder = null, reason = null } = {}) {
    Object.assign(m, {
      undelivered_at: nowIso(), undelivered_holder: holder ? String(holder) : null,
      undelivered_reason: String(reason || 'cannot receive messages').replace(/\s+/g, ' ').trim().slice(0, 200),
    });
    this.apply(m, 'undelivered', op, op === 'close' ? 'thread' : 'code');
    if (!m.reply_to) { this.waitDueNow(m, 'undelivered'); this.store.saveMessage(m); }
    this.log(`undelivered ${m.id} (${op}, holder ${holder || '?'})${m.wait_due_at ? ', sender wait due' : ', no way back (log only)'}`);
    return m;
  }

  // --- repair (I5) -------------------------------------------------------------------------
  /**
   * At start: a move stopped between its writes (I11) is finished first: a new record whose source (`moved_from`) is not
   * yet closed with `moved_to` to it gets the rest of the move; every non-final message whose key points to a record with
   * `moved_to` goes to the record its key resolves to. Returns the ids moved.
   */
  repairMoves() {
    const out = [];
    for (const listed of this.store.topics()) {
      const rec = this.store.topic(listed); // fresh: an earlier repair in this loop may have closed it
      if (!rec || rec.state === 'closed' || !rec.moved_from) continue;
      const old = this.store.topic(rec.moved_from);
      if (!old || old.state === 'closed') continue;
      // Only toward the newer record: a return move B→A stopped midway leaves B open with its own moved_from A (older
      // than the new A); finishing "A→B" too would close both records with moved_to pointing at each other.
      if (!(String(old.updated_at || '') < String(rec.updated_at || ''))) continue;
      out.push(...this.moveTopicMail(old, rec));
      this.log(`repair: move ${whereLabel(old)} → ${whereLabel(rec)} finished`);
    }
    const target = new Map();
    for (const m of this.store.messages((x) => !isFinal(x.status) && x.project && x.topic && x.to_machine)) {
      const k = recipientTopicKey(m);
      const ks = keyText(k);
      if (!target.has(ks)) { const r = this.store.topic(k); target.set(ks, r && r.moved_to ? this.resolveKey(k) : null); }
      const to = target.get(ks);
      if (!to || sameTopicKey(to, k) || to.state === 'closed') continue;
      this.moveMessage(m, to);
      out.push(m.id);
    }
    return out;
  }

  /**
   * At start: a reply is written before its original (I5). An original whose latest reply is not its recorded answer
   * is finished from that reply: answer fields, the transition, its notices, the cancelled reply, the reply event.
   * A move stopped midway is finished before (repairMoves). Returns the ids repaired.
   */
  repair() {
    const out = this.repairMoves();
    const all = this.store.messages();
    const byId = new Map(all.map((m) => [m.id, m]));
    const replies = new Map();
    for (const r of all) {
      if (!r.reply_to) continue;
      if (!replies.has(r.reply_to)) replies.set(r.reply_to, []);
      replies.get(r.reply_to).push(r);
    }
    for (const [oid, list] of replies) {
      const o = byId.get(oid);
      if (!o || o.reply_to) continue;
      list.sort((a, b) => a.created_at.localeCompare(b.created_at) || a.id.localeCompare(b.id));
      const last = list[list.length - 1];
      if (last.cancels) {
        const target = byId.get(last.cancels);
        if (target && target.cancelled_by !== last.id) { target.cancelled_by = last.id; target.cancelled_at = last.created_at; this.store.saveMessage(target); }
      }
      if (o.answer_id === last.id) continue;
      if (o.answer_at && o.answer_at >= last.created_at) continue;
      const final = last.reply_state === FINAL_REPLY_STATE;
      const delivered = last.status !== 'undelivered';
      let to = null;
      if (final && ['pending', 'picked', 'waiting'].includes(o.status)) to = 'done';
      else if (final && o.status === 'done' && o.resolution === 'reply' && last.cancels === o.answer_id) to = 'same';
      else if (!final && o.status === 'picked') to = 'waiting';
      else if (!final && o.status === 'waiting') to = 'same';
      if (!to) { this.log(`repair: reply ${last.id} of ${o.id} (${o.status}) left as it is`); continue; }
      const first = !o.answer_state;
      Object.assign(o, {
        answer_state: last.reply_state, answer_at: last.created_at, answer_id: last.id, answer_origin: 'reply',
        answer_delivered: delivered, answer_undelivered_reason: delivered ? null : last.undelivered_reason || null,
      });
      if (!final) { clearWaitFields(o); if (delivered) this.setWait(o, WAIT_BASE_S[last.reply_state] || WAIT_BASE_S.code, 'check', Date.parse(last.created_at)); }
      if (to === 'same') this.store.saveMessage(o);
      else this.apply(o, to, 'repair', 'code', { resolution: to === 'done' ? 'reply' : null });
      if (last.status === 'pending' && (first || final)) this.replyEvent(last, o);
      this.log(`repair: ${o.id} → ${o.status}${o.resolution ? `/${o.resolution}` : ''} from reply ${last.id}`);
      out.push(o.id);
    }
    return out;
  }

  // --- turns (keepalive) ------------------------------------------------------------
  /**
   * Record the turn of a thread (hook UserPromptSubmit → open, Stop → closed). One record
   * per thread; `open` starts a new turn (new `opened_at`, keepalive counter reset).
   * `attempt: true` (wake) only counts a keepalive ring of the open turn. `closed` with
   * `opened_at` closes only that turn: a late close (queued in an outbox during an outage)
   * of an older turn leaves the current record as it is (`stale: true`). `next` (closed only): the next step left in the
   * handover of the held topic; wake rings the thread to continue it (continuation, P-026).
   */
  turn({ thread, state = null, machine = null, project = null, topic = null, holder_mode = null, reason = null, attempt = false, opened_at = null, next = null, resume_at = null, waiting_operator = undefined, discussion = undefined, brain = null } = {}, ctx = {}) {
    requireThread(thread);
    const prev = this.store.turn(thread);
    const now = nowIso();
    const wasDiscussing = discussionOpen(prev);
    // `waiting_operator` without a state (P-061, hook PreToolUse/PostToolUse around AskUserQuestion): a flag of the OPEN turn
    // record, so the record's end (Stop, keepalive close, new turn) clears it. `opened_at` binds it to that very turn.
    if (!state && !attempt && waiting_operator !== undefined) {
      if (!prev || prev.state !== 'open' || (opened_at != null && opened_at !== '' && prev.opened_at !== opened_at)) {
        this.log(`waiting_operator of ${thread} ignored: no such open turn`);
        return { ok: true, ignored: true };
      }
      prev.waiting_operator = waiting_operator === true || waiting_operator === 'true' || waiting_operator === 1;
      prev.waiting_operator_at = prev.waiting_operator ? now : null;
      this.store.saveTurn(prev);
      this.discussionChanged(wasDiscussing, prev);
      return { ok: true, turn: prev };
    }
    if (attempt) {
      if (!prev) throw new HttpError(404, `ťah vlákna ${thread} neexistuje`);
      prev.keepalive = { count: ((prev.keepalive && prev.keepalive.count) || 0) + 1, last_at: now };
      this.store.saveTurn(prev);
      return { ok: true, turn: prev };
    }
    if (!TURN_STATES.includes(state)) throw new HttpError(400, `stav ťahu (state) musí byť ${TURN_STATES.join('|')}`);
    if (holder_mode != null && !HOLDER_MODES.includes(holder_mode)) throw new HttpError(400, `režim držiteľa (holder_mode) musí byť ${HOLDER_MODES.join('|')}`);
    if (project != null) requireName(project, 'project');
    if (topic != null) requireName(topic, 'topic');
    const disc = discussionFlag(discussion);
    let rec;
    if (state === 'open') {
      rec = {
        thread, machine: normalizeMachine(machine), project: project || null, topic: topic || null, holder_mode: holder_mode || null,
        // Model 3: the brain is a label of the turn record, not its identity (the record is the thread).
        brain: requestBrain(brain, ctx),
        state: 'open', opened_at: now, closed_at: null, close_reason: null, keepalive: { count: 0, last_at: null },
        // The run (one T3 turn of the provider): a prompt steered into the running turn opens without a Stop in between
        // and keeps the run; wake keeps a ring queued behind the run until the next run starts (wake-stale-ring).
        run_at: prev && prev.state === 'open' ? prev.run_at || prev.opened_at || now : now,
        ...(disc !== undefined ? { discussion: disc, discussion_at: disc ? now : null } : {}),
      };
    } else {
      if (opened_at != null && opened_at !== '' && (!prev || prev.opened_at !== opened_at)) {
        this.log(`stale close of ${thread} (opened_at ${opened_at}, current ${prev ? prev.opened_at : 'none'})`);
        return { ok: true, stale: true };
      }
      // Closed with an open discussion: the turn ended with a question to the operator (D-0015). The wake module's close
      // of an interrupted turn (keepalive, no flag) keeps the discussion of that turn.
      const keep = disc === undefined && reason === 'keepalive' && prev ? !!prev.discussion : !!disc;
      rec = { ...(prev || { thread, machine: normalizeMachine(machine), project, topic, holder_mode, opened_at: null, keepalive: { count: 0, last_at: null } }), state: 'closed', closed_at: now, close_reason: reason || null, waiting_operator: false, waiting_operator_at: null, next: next ? String(next).slice(0, MAX_NEXT) : null, resume_at: resumeAt(resume_at, now), discussion: keep, discussion_at: keep ? (prev && prev.discussion && prev.discussion_at) || now : null };
    }
    this.store.saveTurn(rec);
    this.discussionChanged(wasDiscussing, rec);
    return { ok: true, turn: rec };
  }

  /** The thread's discussion with the operator ended (D-0015): one event, the wake module rings the steer/assist it held. */
  discussionChanged(wasDiscussing, rec) {
    if (!wasDiscussing || discussionOpen(rec)) return null;
    const ev = this.emitEvent({ type: 'discussion-closed', holder: rec.thread, holder_machine: rec.machine || null, project: rec.project || null, topic: rec.topic || null });
    this.log(`discussion closed ${rec.thread}: event ${ev.seq}`);
    return ev;
  }

  /* resume_at (closed only, P-061): the thread's timer; wake rings the thread at that time with its next step. */
  turns({ open = false } = {}) {
    const list = this.store.turns((t) => !open || t.state === 'open');
    list.sort((a, b) => String(a.opened_at).localeCompare(String(b.opened_at)) || a.thread.localeCompare(b.thread));
    return list;
  }

  // --- notices -------------------------------------------------------------------
  createNotice({ kind, title, message, extra = {} }) {
    const n = {
      id: newId(), kind, title, state: 'open', ...extra,
      message_id: message.id, project: message.project, topic: message.topic,
      brain: message.to_brain ?? null, machine: message.to_machine ?? null, where: message.project && message.topic ? whereLabel({ brain: message.to_brain, brain_name: message.to_brain_name, machine: message.to_machine }) : null,
      level: message.level, sender: message.sender, thread_id: message.thread_id,
      options: NOTICE_OPTIONS[kind].map(({ id, label, needsTarget }) => ({ id, label, needs_target: !!needsTarget })),
      created_at: nowIso(), decided_at: null, decision: null, target: null,
    };
    this.store.saveNotice(n);
    if (message.level === 'blocker') {
      notifyOperator(this.instance, { notice: n, message, log: this.log });
    }
    return n;
  }

  /** Notice created by a client (wake module over limit, P-043). Kind is restricted; an open notice with the same `dedupe_key` is reused. */
  createExternalNotice(sender, { kind, title, message_id, detail = null, dedupe_key = null, thread = null, project = null, topic = null } = {}) {
    if (!EXTERNAL_NOTICE_KINDS.includes(kind)) throw new HttpError(400, `oznam typu „${kind}“ nemôže vytvoriť klient`);
    if (typeof title !== 'string' || !title.trim()) throw new HttpError(400, 'oznam musí mať názov (title)');
    let message;
    if (THREAD_NOTICE_KINDS.includes(kind)) {
      requireThread(thread);
      message = { id: null, project: project || null, topic: topic || null, level: null, sender, thread_id: thread };
      dedupe_key = `thread:${thread}`;
    } else {
      message = this.store.message(message_id || '');
      if (!message) throw new HttpError(404, `správa ${message_id} neexistuje`);
      // I6: an open notice exists only for a non-final message.
      if (isFinal(message.status)) throw new HttpError(409, `správa ${message.id} je konečná (${message.status}); oznam k nej nevznikne`, { id: message.id, ...stateOf(message) });
    }
    if (dedupe_key) {
      const open = this.store.notices((n) => n.state === 'open' && n.kind === kind && n.dedupe_key === dedupe_key);
      if (open.length) return { notice_id: open[0].id, created: false };
    }
    const n = this.createNotice({ kind, title: title.trim(), message, extra: { created_by: sender, detail, dedupe_key } });
    return { notice_id: n.id, created: true };
  }

  notices({ all = false } = {}) {
    const list = this.store.notices((n) => all || n.state === 'open');
    list.sort((a, b) => a.created_at.localeCompare(b.created_at) || a.id.localeCompare(b.id));
    return list.map((n) => {
      const m = n.message_id ? this.store.message(n.message_id) : null;
      return { ...n, text: m ? m.text : null, message_status: m ? m.status : null, waiting_since: m ? m.created_at : n.created_at };
    });
  }

  /** Operator decision, executed directly by the service; no model involved (P-046). */
  decide({ notice_id, decision, target = null }, ctx = {}) {
    const n = this.store.notice(notice_id || '');
    if (!n) throw new HttpError(404, `oznam ${notice_id} neexistuje`);
    if (n.state !== 'open') throw new HttpError(409, 'oznam je už rozhodnutý');
    const opt = (NOTICE_OPTIONS[n.kind] || n.options || []).find((o) => o.id === decision);
    if (!opt) throw new HttpError(400, `neznáme rozhodnutie „${decision}“ pre oznam typu ${n.kind}`);
    const msg = n.message_id ? this.store.message(n.message_id) : null;
    const decided = () => { Object.assign(n, { state: 'decided', decision, target, decided_at: nowIso() }); this.store.saveNotice(n); };
    let result = {};

    if (n.kind === 'unregistered-topic') {
      if (!msg || msg.status !== 'held') throw new HttpError(409, 'správa už nie je pozdržaná', msg ? { id: msg.id, ...stateOf(msg) } : {});
      if (decision === 'discard') {
        decided();
        this.apply(msg, 'discarded', 'decide', 'operator');
        return { notice_id: n.id, decision };
      }
      // Every decision but discard (deliver, redirect) puts the message into a mailbox: one check for all of them.
      refuseExpiredHeld(msg, decision);
      if (decision === 'deliver' && msg.private_owner) {
        // Private topic: deliver to the owner's main thread, which handles it itself.
        const owner = msg.private_owner;
        const ref = { project: msg.project, topic: msg.topic };
        msg.notice_id = null;
        delete msg.private_owner;
        decided();
        this.toPersonaMain(msg, owner, ref, null, msg.from_brain ?? null);
        return { notice_id: n.id, decision, delivered_to: { project: personaProject(owner), topic: PERSONA_MAIN_TOPIC } };
      }
      // Model 3: `deliver` resolves the sender's address again (resolveAddress, its own brain first); `redirect` resolves the
      // target `[<brain>[@<machine>]:]<project>/<topic>` from the operator's brain (X-Comms-Brain, usually none).
      let addr = { ...(msg.address || {}), project: msg.project, topic: msg.topic };
      let fromBrain = msg.from_brain ?? null;
      if (decision === 'redirect') { addr = parseTarget(target); fromBrain = requestBrain(null, ctx); }
      const dest = { project: addr.project, topic: addr.topic };
      const t = this.resolveAddress(addr, fromBrain).rec;
      if (!t) throw new HttpError(409, `téma ${dest.project}/${dest.topic}${addr.brain ? ` brainu ${addr.brain}` : ''} nie je zaregistrovaná; pošta tému nezakladá`);
      if (decision === 'redirect') {
        msg.redirected_from = { project: msg.project, topic: msg.topic, brain: msg.to_brain ?? null };
        delete msg.private_owner;
        msg.project = dest.project;
        msg.topic = dest.topic;
      }
      delete msg.address;
      setRecipient(msg, t);
      msg.notice_id = null;
      decided();
      this.apply(msg, 'pending', 'decide', 'operator');
      this.afterDelivery(msg);
      result = { delivered_to: dest, resolved: whereLabel(t) };
    } else if (n.kind === 'legacy-topic' && decision === 'move') {
      // The operator moves the mail of the record from before model 3 into the brain's new record.
      const old = this.store.topic(n.legacy_key);
      const neu = this.resolveKey(n.new_key);
      if (!old || old.state === 'closed') throw new HttpError(409, 'staršia téma už nie je otvorená');
      if (!neu || neu.state === 'closed') throw new HttpError(409, 'nová téma už nie je otvorená');
      neu.moved_from = topicKey(old);
      this.store.saveTopic(neu);
      result = { moved: this.moveTopicMail(old, neu), resolved: whereLabel(neu) };
      decided();
    } else if (n.kind === 'blocker' || n.kind === 'unregistered-persona' || n.kind === 'unpicked') {
      decided();
      if (decision === 'discard' && msg && msg.status === 'pending') this.apply(msg, 'discarded', 'decide', 'operator');
      return { notice_id: n.id, decision };
    } else {
      decided();
    }
    return { notice_id: n.id, decision, ...result };
  }

  // --- wake events ----------------------------------------------------------------
  emitEvent(ev) {
    const full = { seq: ++this.seq, created_at: nowIso(), ...ev };
    this.store.saveEvent(full);
    this.events.emit('event', full);
    return full;
  }

  get lastSeq() {
    return this.seq;
  }

  /** Long-poll: resolve with events after `since`, or [] after `wait` ms. */
  eventsSince(since, wait = 0) {
    const now = this.store.eventsSince(since);
    if (now.length || wait <= 0) return Promise.resolve(now);
    return new Promise((resolve) => {
      const done = () => { clearTimeout(timer); this.events.off('event', done); resolve(this.store.eventsSince(since)); };
      const timer = setTimeout(done, Math.min(wait, 30_000));
      this.events.on('event', done);
    });
  }
}

/** Seconds as the shortest whole unit: 90s, 30m, 4h. */
export function formatDuration(sec) {
  if (sec && sec % 3600 === 0) return `${sec / 3600}h`;
  if (sec && sec % 60 === 0) return `${sec / 60}m`;
  return `${sec}s`;
}

/** Model 3: events carry the recipient's and the sender's brain and machine; wake counts with these, never with its own. */
function eventBase(m) {
  return {
    message_id: m.id, level: m.level, project: m.project, topic: m.topic, brain: m.to_brain ?? null, machine: m.to_machine ?? null,
    sender: m.sender, thread_id: m.thread_id, from_project: m.from_project, from_topic: m.from_topic, from_brain: m.from_brain ?? null, from_machine: m.from_machine ?? null,
    text_hash: textHash(m.text),
  };
}

/** Key of a message's recipient topic (to_brain, to_machine, project, topic). */
function recipientTopicKey(m) {
  return { brain: m.to_brain ?? null, machine: m.to_machine ?? null, project: m.project, topic: m.topic };
}

/** Key of a message's source topic (from_brain, from_machine, from_project, from_topic). */
function senderTopicKey(m) {
  return { brain: m.from_brain ?? null, machine: m.from_machine ?? null, project: m.from_project, topic: m.from_topic };
}

/** Key of the recent rows of a topic: `topic:<brain>/<project>/<topic>` (no machine; brain empty before model 3). */
function recentTopicKey(brain, project, topic) {
  return `topic:${brain ?? ''}/${project}/${topic}`;
}

function keyText(k) {
  return JSON.stringify([k.brain ?? null, k.machine ?? null, k.project, k.topic]);
}

/** `<brain_name>@<machine>` of a record (brain id without a name; `-` before model 3). */
function whereLabel(rec) {
  return `${rec.brain_name || rec.brain || '-'}@${rec.machine || '?'}`;
}

/** The message's recipient is this topic record (key and readable brain name); null clears it. */
function setRecipient(msg, rec) {
  msg.to_brain = rec ? rec.brain ?? null : null;
  msg.to_machine = rec ? rec.machine ?? null : null;
  msg.to_brain_name = rec ? rec.brain_name || null : null;
}

function byUpdatedDesc(a, b) {
  return String(b.updated_at || '').localeCompare(String(a.updated_at || ''));
}

/** 409 for an address that names a topic of more than one brain (nothing is written). */
function ambiguousError(project, topic, recs) {
  const list = recs.map((t) => `${t.brain_name || t.brain || '-'} (${t.brain ?? '-'})@${t.machine || '?'}`);
  return new HttpError(409, `adresa ${project}/${topic} je nejednoznačná: téma je otvorená vo viacerých brainoch (${list.join(', ')}); zadaj brain: <brain>[@<stroj>]:${project}/${topic}`, { ambiguous: list, project, topic });
}

/** Brain of a request: the body or query value, else X-Comms-Brain; null when none (a sender without a brain). */
function requestBrain(v, ctx = {}) {
  if (present(v)) return requireBrainValue(v);
  return present(ctx.brain) ? requireBrainValue(ctx.brain) : null;
}

function requireBrainValue(v) {
  if (!isValidName(String(v))) throw new HttpError(400, 'neplatný brain: povolené sú písmená, číslice, bodka, pomlčka a podčiarkovník');
  return String(v);
}

/** Readable brain name (directory name of the brain): a label, not an identity. */
function brainNameOf(v) {
  if (!present(v)) return null;
  const s = String(v).replace(/[\x00-\x1f]/g, '').trim().slice(0, 128);
  return s || null;
}

function clearWaitFields(m) {
  m.wait_due_at = null;
  m.wait_reason = null;
}

/** A closed topic takes no mail: 410 with the time it was closed and where to write instead. */
function refuseClosed(t, why) {
  if (!t || t.state !== 'closed') return;
  const owner = t.persona || t.project;
  throw new HttpError(410, `${why}: ${t.project}/${t.topic} uzavretá ${t.closed_at || '?'}; nikto ju nedrží a správu by nikto neprečítal. Pošli ju do hlavného vlákna @${owner}/main alebo do inej otvorenej témy (ID pôvodnej správy uveď v texte)`, { closed_at: t.closed_at || null, project: t.project, topic: t.topic });
}

/**
 * A held message past its deadline is not put into any mailbox (decide deliver or redirect): its sender already heard
 * `no reply <id>` and may have sent it anew, so delivering it would double it (D-0024). 409; the notice stays open for discard.
 */
function refuseExpiredHeld(m, decision, now = Date.now()) {
  const expired = !!m.expired_at || (!!m.expires_at && Date.parse(m.expires_at) <= now);
  if (!expired) return;
  const at = m.expired_at || m.expires_at;
  const what = decision === 'redirect' ? 'presmerovanie' : 'doručenie';
  throw new HttpError(409, `správa ${m.id} vypršala ${at} bez doručenia; odosielateľ už dostal no reply a mohol ju poslať znova alebo inak, ${what} by ju zdvojilo. Zahoď ju (discard)`, { message_id: m.id, expired_at: at, decision });
}

/** Address of the recipient: `<project>/<topic>`, else `thread:<id>`. */
function addressOf(m) {
  if (m.project && m.topic) return `${m.project}/${m.topic}`;
  if (m.to_thread) return `thread:${m.to_thread}`;
  return null;
}

function requireThread(v) {
  if (!isValidThread(v)) throw new HttpError(400, 'neplatné pole thread: povolené sú písmená, číslice, bodka, pomlčka, podčiarkovník a dvojbodka');
}

/** A body field carries a value (null and empty string count as absent). */
function present(v) {
  return v != null && v !== '';
}

function requireName(v, field) {
  if (!isValidName(v)) throw new HttpError(400, `neplatné pole ${field}: povolené sú písmená, číslice, bodka, pomlčka a podčiarkovník`);
}

/** Target of a redirect: `[<brain>[@<machine>]:]<project>/<topic>` (brain = id or brain_name). */
export function parseTarget(target) {
  const raw = String(target || '');
  const colon = raw.lastIndexOf(':');
  const head = colon >= 0 ? raw.slice(0, colon) : null;
  const [project, topic, extra] = raw.slice(colon + 1).split('/');
  const [brain, machine] = head != null ? head.split('@') : [null, null];
  if (!isValidName(project) || !isValidName(topic) || extra !== undefined || (head != null && !isValidName(brain)) || (machine != null && !normalizeMachine(machine))) {
    throw new HttpError(400, 'cieľ (target) musí byť v tvare [<brain>[@<stroj>]:]<project>/<topic>');
  }
  return { brain: brain || null, machine: machine ? normalizeMachine(machine) : null, project, topic };
}

export function publicMessage(m) {
  return {
    id: m.id, project: m.project, topic: m.topic, persona: m.persona || null, ref_project: m.ref_project || null, ref_topic: m.ref_topic || null,
    to_brain: m.to_brain ?? null, to_machine: m.to_machine ?? null, from_brain: m.from_brain ?? null, from_machine: m.from_machine ?? null,
    to_thread: m.to_thread, level: m.level, text: m.text, status: m.status, resolution: m.resolution || null,
    picked_at: m.picked_at || null, picked_by_thread: m.picked_by_thread || null,
    sender: m.sender, thread_id: m.thread_id, from_project: m.from_project, from_topic: m.from_topic,
    reply_to: m.reply_to, reply_state: m.reply_state || null, wait_for: m.wait_for || null,
    answer_state: m.answer_state || null, answer_id: m.answer_id || null, code_answer: m.code_answer || null, expired_at: m.expired_at || null,
    cancels: m.cancels || null, cancelled_by: m.cancelled_by || null, created_at: m.created_at,
  };
}
