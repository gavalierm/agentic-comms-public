// Wake module (P-043): a separate process above the mail service. It consumes
// wake events (`blocker`/`steer`/`assist` → holder of the topic, `reply` to any level → the
// sender's thread) through the long-poll `events` operation and calls the
// adapter `ring(holder, text)`. The text is always the fixed template, never the
// body. Limits: at most `limitPerHour` rings per pair of topics and a stop when
// the same message arrives `stopOnRepeat` times in a row; above the limit the
// message stays in the mailbox and the operator gets a notice (P-046).
// The mail works without this module: every message waits for the next turn.
// Reminder (remindOnce, operator 2026-11-17: mail not picked up is lost mail): a message still pending remindAfterMs
// after its last ring rings again through handle(), at most remindMax times, then one operator notice `unpicked`.
// The sender's wait is a state of the message on the service, not an event (1e): every cycle reads
// `GET /obligations?due=1`, rings the sender's thread when it is not busy (`no reply <id>`; wait reason
// `undelivered` → `undelivered <id>`, `recipient-closed` → `recipient closed <id>`) and confirms with
// `POST /wait-rang`. An unconfirmed ring (wake off, crashed, thread unreachable) stays due and rings in a later cycle
// regardless of its age. At most one wait ring per thread per cycle.
// A blocker/steer/assist/reply ring that fails for good (thread archived or gone) bounces: the service marks the
// unpicked message undelivered and the sender's wait becomes due. A busy thread gets a ring as its next turn
// (T3): at most one queued ring per run of the holder (`turns`, wake-state `queued`). A run is the turn record's `run_at`:
// a prompt steered into the running turn (UserPromptSubmit without a Stop) opens a new record of the same run, and the
// queued ring still waits behind it; after the run's close the queued ring starts within `queuedStartMs`, and that
// starting turn sees all the mail (two stale bells on picked-up mail, wake-stale-ring 2026-11-13).
// Keepalive (optional, `keepalive.since_command`): open turns of this machine opened
// before the last T3 start whose thread no longer runs are rung with
// KEEPALIVE_TEMPLATE (at most 3 attempts per hour per thread, ≥ 60 s apart) or, in
// operator mode / after the 3rd failure, reported as `turn-interrupted`.
// Discussion (D-0015, D-0022): a steer/assist to a holder that discusses with the operator (turn record, `discussionOpen`)
// is held (wake-state `held`) and rung when the discussion ends (event `discussion-closed`, else the next poll); a
// blocker passes.
// Busy = the thread runs now (comms-client-wake, 2026-11-15): an open turn record alone is not enough (interrupted,
// archived, Stop lost). Where an open record, a hold or a queued mark decides, wake reads the thread's state from T3
// (adapter `status`, as keepalive and continuation do): busy only while T3 runs it, a queued mark only while T3 runs the
// run it was rung into. Without the state the open record counts only within its deadline (TURN_BUSY_MAX_MS).
// A thread without any turn record (it runs outside the workflow, e.g. the maintainer's thread of send --maintainer) has
// no hook that drops a stale bell at its turn start: while T3 runs it, its ring is held (`held_reason: running`) and rung
// after the run ends, only when the mail still waits (operator 2026-11-16: a bell queued behind the running turn came
// after the mail was picked and cost an empty turn).
// A holder that T3 reports archived or gone gets no hold and no ring: its message becomes undelivered (bounce; the
// service leaves it to a new holder of the topic), and the sender's wait is due (`undelivered <id>`, 1e); a held message
// whose holder is archived later goes the same way at the next poll.
// Jobs (D-0023, D-0024): `jobs` in wake.json are shell commands run periodically next to the long-poll (e.g. the reporter's
// issue watch), one run of a job at a time, with a time limit; a failure is logged, it never stops the module.
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { readJson, writeJson, nowIso, normalizeMachine, parseTimestamp, discussionOpen, turnFresh, TURN_BUSY_MAX_MS, DISCUSSION_HELD_LEVELS } from '../common/util.mjs';
import { resolveConfig, Client } from '../client/index.mjs';
import { Store } from '../server/store.mjs';
import { selfRestartEnabled } from '../common/selfrestart.mjs';
import { createT3Adapter, DEFAULT_T3_COMMAND, DEFAULT_T3_STATUS_COMMAND } from '../adapters/t3/index.mjs';

/** Fixed template; the workflow hook recognises this prefix (policy `wake_template`, P-047). */
export const WAKE_TEMPLATE = 'you have mail in topic';
/** Fixed templates for the sender of a due wait (`<template> <message_id>`, P-047), by `wait_reason`. */
export const NO_REPLY_TEMPLATE = 'no reply';
export const UNDELIVERED_TEMPLATE = 'undelivered';
export const RECIPIENT_CLOSED_TEMPLATE = 'recipient closed';
/** Event types the module rings (new mail and replies); a permanent ring failure bounces the message. */
export const WAKE_TYPES = ['blocker', 'steer', 'assist', 'reply'];
/** A due wait whose ring failed is tried again after this long (it stays due on the service meanwhile). */
const DUE_RETRY_MS = 60_000;
/** The service's event when a thread's discussion with the operator ends: the held rings of that holder go out. */
export const DISCUSSION_CLOSED = 'discussion-closed';
/** Held rings at most this long and this many (the message itself waits in the mailbox anyway). */
const HELD_MAX_MS = 24 * 60 * 60 * 1000;
const HELD_MAX = 200;
/** Fixed keepalive template (policy `prompt.keepalive_template` in the workflow). */
export const KEEPALIVE_TEMPLATE = 'turn interrupted, continue from handover';
/** Continuation (P-026): a turn closed with a next step left in the handover is rung once per step. */
export const CONTINUE_TEMPLATE = 'turn ended with a next step, continue from handover';
/**
 * Hourly ring limit of a pair for an event: a blocker and a steer get twice the limit (report 2026-11-17: a steer to a thread
 * working with another one waited 20 min behind six assists of the hour). They still count in the pair, so a loop still
 * stops; repeated text stops by `stopOnRepeat`.
 */
export const URGENT_TYPES = ['blocker', 'steer'];
export function pairLimit(ev, limitPerHour) {
  return URGENT_TYPES.includes(ev?.type) ? limitPerHour * 2 : limitPerHour;
}

export const DEFAULTS = {
  limitPerHour: 6, stopOnRepeat: 2, maxEventAgeMs: 60 * 60 * 1000, waitMs: 25_000, reconnectMs: 1000, maxReconnectMs: 30_000,
  keepaliveIntervalMs: 60_000, keepaliveMaxPerHour: 3, keepaliveDelayMs: 60_000, continueDelayMs: 60_000, idleDedupeMs: 120_000,
  // goneCheckMs: a held message's holder is asked T3 whether it still exists at most this often (each poll costs a call).
  queuedStartMs: 120_000, turnBusyMaxMs: TURN_BUSY_MAX_MS, goneCheckMs: 60_000,
  // Reminder (operator 2026-11-17, critical: mail not picked up is lost mail): every remindEveryMs the messages still
  // pending after remindAfterMs ring their recipient again, at most remindMax times each (then one operator notice);
  // older than remindMaxAgeMs: one summary notice, no ring days later.
  remindEveryMs: 5 * 60_000, remindAfterMs: 30 * 60_000, remindMax: 3, remindMaxAgeMs: 48 * 60 * 60_000,
};

/** The run of a turn record: `run_at` (kept across prompts steered into a running turn), else `opened_at` (older mail). */
export function runOf(rec) {
  return rec ? rec.run_at || rec.opened_at || null : null;
}
const HOUR = 60 * 60 * 1000;
/** Periodic jobs: shortest interval (a smaller one is raised), default interval and time limit of one run. */
export const JOB_MIN_INTERVAL_MS = 60_000;
export const JOB_DEFAULT_INTERVAL_MS = 900_000;
export const JOB_DEFAULT_TIMEOUT_MS = 300_000;
const JOB_NAME_RE = /^[A-Za-z0-9._-]{1,64}$/;
/** Characters of a job's output kept for the log line of a failed run. */
const JOB_OUTPUT_MAX = 2000;
/** Longest single timer (setTimeout overflows above it); longer waits are slept in parts. */
const MAX_TIMER_MS = 2_147_483_647;
export const WAKE_SENDER = 'wake';
/** Ring (and T3 status) errors after which no later turn of the thread will come (the sender gets a bounce). */
export const PERMANENT_RING_FAILURE = /archived|not found|no longer available|does not exist|no such thread|unknown thread/i;

/** Topic label of an event: `project/topic`, or the thread when the recipient has no topic. */
export function topicLabel(ev) {
  if (ev.project && ev.topic) return `${ev.project}/${ev.topic}`;
  return `thread/${ev.holder}`;
}

/**
 * Sender label of an event (operator 2026-11-17: the bell says to whom and from whom): the sender's topic
 * `project/topic`, else its key, else `thread/<id>`. Characters outside the label class become `-` (the flow
 * recognises a bell only as the exact whole prompt).
 */
export function fromLabel(ev) {
  const clean = (s) => String(s).replace(/[^A-Za-z0-9_.:@-]/g, '-').replace(/^[^A-Za-z0-9@]+/, '');
  if (ev.from_project && ev.from_topic) return `${clean(ev.from_project)}/${clean(ev.from_topic)}`;
  if (ev.sender && clean(ev.sender)) return clean(ev.sender);
  if (ev.thread_id) return `thread/${clean(ev.thread_id)}`;
  return null;
}

/** `you have mail in topic <to> from <from>` (without a known sender: no `from`). */
export function wakeText(ev) {
  const from = fromLabel(ev);
  return `${WAKE_TEMPLATE} ${topicLabel(ev)}${from ? ` from ${from}` : ''}`;
}

/**
 * Recipient part of a bell: the wake text without its sender. A duplicate bell is a bell to the same recipient (that
 * turn reads all its mail), whoever sent the mail; other bells are compared whole.
 */
export function bellKey(text) {
  const t = String(text || '');
  return t.startsWith(`${WAKE_TEMPLATE} `) ? t.replace(/ from \S+$/, '') : t;
}

/** Exact ring text of an event (the whole prompt, never the body). */
export function ringText(ev) {
  return wakeText(ev);
}

/** Exact ring text of a due wait (1e point 1): by `wait_reason`, never the body. */
export function dueText(item) {
  if (item.wait_reason === 'undelivered') return `${UNDELIVERED_TEMPLATE} ${item.id}`;
  if (item.wait_reason === 'recipient-closed') return `${RECIPIENT_CLOSED_TEMPLATE} ${item.id}`;
  return `${NO_REPLY_TEMPLATE} ${item.id}`;
}

/** Address of one side of an exchange: topic, else thread, else sender. */
export function endpoint(project, topic, thread, sender) {
  if (project && topic) return `${project}/${topic}`;
  if (thread) return `thread:${thread}`;
  return `sender:${sender || '?'}`;
}

export function sourceOf(ev) { return endpoint(ev.from_project, ev.from_topic, ev.thread_id, ev.sender); }
export function targetOf(ev) { return endpoint(ev.project, ev.topic, ev.holder, null); }

/** Unordered pair of topics (both directions share the hourly limit). */
export function pairKey(ev) {
  return [sourceOf(ev), targetOf(ev)].sort().join(' <> ');
}

/** Ordered direction (repeat detection: the same sender repeating the same text). */
export function directionKey(ev) {
  return `${sourceOf(ev)} -> ${targetOf(ev)}`;
}

/** Persistent state: cursor into the event stream, ring timestamps per pair, last text per direction. */
export class WakeState {
  constructor(file) {
    this.file = file;
    const d = file ? readJson(file, {}) : {};
    this.cursor = Number.isInteger(d.cursor) ? d.cursor : null;
    this.rings = d.rings || {};
    this.last = d.last || {};
    this.keepalive = d.keepalive || {};
    // thread → { next, closed_at, at, stalled }: the last continuation ring (one per next step).
    this.continued = d.continued || {};
    // holder → run (`runOf`) of the turn that already has one ring queued behind it.
    this.queued = d.queued || {};
    // holder → T3 run id (adapter status `run`) the queued ring was rung into: another run of T3 means it has started.
    this.queuedRun = d.queued_run || {};
    // holder → { at, text }: the last ring of a thread without an open turn (its turn opens seconds later).
    this.idle = d.idle || {};
    // message id → event of a steer/assist held while its holder discusses with the operator (D-0015).
    this.held = d.held || {};
    // job name → { started_at, finished_at, ok, code, ms, error }: the last run of a periodic job (its next run survives a restart).
    this.jobs = d.jobs || {};
    // message id → { count, at, noticed }: the reminder rings of a message still pending (Waker.remindOnce).
    this.reminded = d.reminded || {};
    this.updated_at = d.updated_at || null;
  }

  prune(now) {
    for (const [k, v] of Object.entries(this.idle)) if (!v || now - v.at >= HOUR) delete this.idle[k];
    for (const [k, v] of Object.entries(this.held)) if (!v || !(now - Date.parse(v.held_at) < HELD_MAX_MS)) delete this.held[k];
    for (const map of [this.rings, this.keepalive]) {
      for (const [k, list] of Object.entries(map)) {
        const kept = list.filter((t) => now - t < HOUR);
        if (kept.length) map[k] = kept; else delete map[k];
      }
    }
  }

  /**
   * Keep only the queued marks whose ring has not started yet: the holder's run is still the one rung into (open, also
   * with prompts steered into it), or that run closed less than `startMs` ago (the queued ring is starting now). A new
   * run is the queued ring's turn (or a later one): it sees all the mail.
   */
  pruneQueued(turns, now, startMs) {
    const cur = new Map(turns.map((t) => [t.thread, t]));
    for (const [holder, run] of Object.entries(this.queued)) {
      const rec = cur.get(holder);
      const same = !!rec && runOf(rec) === run;
      const waiting = same && (rec.state === 'open' || (rec.closed_at && now - Date.parse(rec.closed_at) < startMs));
      if (!waiting) this.unqueue(holder);
    }
    for (const holder of Object.keys(this.queuedRun)) if (!this.queued[holder]) delete this.queuedRun[holder];
  }

  unqueue(holder) {
    delete this.queued[holder];
    delete this.queuedRun[holder];
  }

  save() {
    this.updated_at = nowIso();
    if (this.file) writeJson(this.file, { cursor: this.cursor, rings: this.rings, last: this.last, keepalive: this.keepalive, continued: this.continued, queued: this.queued, queued_run: this.queuedRun, idle: this.idle, held: this.held, jobs: this.jobs, reminded: this.reminded, updated_at: this.updated_at });
  }
}

export class Waker {
  /**
   * @param {object} o
   * @param {Client} o.client  mail client (events, createNotice)
   * @param {{name:string, ring(holder:string, text:string):Promise<{ok:boolean}>}|null} o.adapter  null → nothing is woken
   * @param {string|null} o.stateFile
   * @param {(() => Promise<number|null>)|null} o.keepaliveSince  last T3 start (epoch ms); null → keepalive off
   * @param {Client} [o.keepaliveClient]  separate client for keepalive (its own connection state next to the long-poll)
   * @param {{name:string, command:string, interval_ms:number, timeout_ms?:number}[]} [o.jobs]  periodic jobs (normalizeJobs)
   */
  constructor({ client, adapter = null, stateFile = null, log = () => {}, now = Date.now, keepaliveSince = null, keepaliveClient = null, ...opts }) {
    this.client = client;
    this.keepaliveSince = keepaliveSince;
    this.kaClient = keepaliveClient || client;
    this.adapter = adapter;
    this.log = log;
    this.now = now;
    this.opts = { ...DEFAULTS, ...opts };
    this.state = new WakeState(stateFile);
    this.stats = { seen: 0, rang: 0, due_rang: 0, limited: 0, skipped: 0, failed: 0, keepalive_rang: 0, keepalive_notified: 0, continue_rang: 0, continue_stalled: 0, jobs_ok: 0, jobs_failed: 0 };
    this.jobsRunning = new Set();
  }

  /** Decide and act on one event. Returns { action: 'rang'|'limited'|'skipped', reason, ... }. */
  async handle(ev) {
    this.stats.seen += 1;
    const skip = (reason) => { this.stats.skipped += 1; this.log(`skip ${ev.seq} (${reason}) ${ev.type} ${topicLabel(ev)}`); return { action: 'skipped', reason }; };
    if (ev.type === DISCUSSION_CLOSED) {
      if (!ev.holder) return skip('no-holder');
      if (ev.holder_machine && this.opts.machine && normalizeMachine(ev.holder_machine) !== normalizeMachine(this.opts.machine)) return skip('other-machine');
      return { action: 'released', count: await this.releaseHeld(String(ev.holder)) };
    }
    if (!WAKE_TYPES.includes(ev.type)) return skip('unknown-type');
    if (!ev.holder) return skip('no-holder');
    // Every wake module sees every event; only the one on the holder's machine rings (P-043).
    // Events without a machine (older registrations) are rung everywhere, as before.
    if (ev.holder_machine && this.opts.machine && normalizeMachine(ev.holder_machine) !== normalizeMachine(this.opts.machine)) return skip('other-machine');
    const now = this.now();
    if (ev.created_at && now - Date.parse(ev.created_at) > this.opts.maxEventAgeMs) return skip('stale');
    // Operator mode does not stop a ring (bob 2026-11-02: a blocker into an operator-led topic got only a notice nobody saw
    // and no reply): the holder answers in the wake turn; what needs the operator it reports to the operator (P-044, P-047).
    if (!this.adapter) return skip('no-adapter');

    // Mail bell only while mail waits (inbox = status pending, I2): the thread picked it up meanwhile (receive) → no
    // ring; the bell turn would find no new mail and the workflow hook would block its prompt (wake-picked-check 2026-11-14).
    {
      const q = { thread: String(ev.holder) };
      if (ev.project && ev.topic) { q.project = ev.project; q.topic = ev.topic; }
      try {
        const c = await this.client.pendingMail(q);
        if (!c.total) {
          this.log(`mail ${topicLabel(ev)}: žiadna čakajúca; správa ${ev.message_id || '-'} nezvoní`);
          return skip('picked');
        }
      } catch (e) {
        this.log(`count ${topicLabel(ev)}: ${e.message}; zvoní sa bez kontroly`);
      }
    }

    // A busy thread gets the ring as its next turn: at most one queued ring per run.
    const holder = String(ev.holder);
    let record = null;
    let turnsKnown = false;
    try {
      const { turns } = await this.client.turns(false);
      this.state.pruneQueued(turns, now, this.opts.queuedStartMs);
      record = turns.find((x) => x.thread === holder) || null;
      turnsKnown = true;
    } catch (e) {
      this.log(`turns: ${e.message}; bez kontroly zaradeného zvončeka`);
    }
    const discussing = turnsKnown && DISCUSSION_HELD_LEVELS.includes(ev.type) && discussionOpen(record);
    const queuedMark = turnsKnown ? this.state.queued[holder] : null;
    // The thread's state decides where the turn record alone would: an open record (busy), a hold, a queued mark.
    // No record at all: a thread outside the workflow; only T3 tells whether it runs now.
    const outside = turnsKnown && !record;
    const th = turnsKnown && ((record && record.state === 'open') || discussing || queuedMark || outside)
      ? await this.threadNow(holder, record, now) : { busy: false, gone: false, known: false, running: false, run: null };
    // Archived or gone: no later turn comes, no hold and no ring; the message goes back to its sender (undelivered).
    if (th.gone) return this.undeliverGone(ev, th.why, { holdOnError: discussing, now });
    // The holder discusses with the operator (D-0015, D-0022): a steer/assist waits for the end of the discussion; a
    // blocker passes. Without known turn records it rings, as before.
    if (discussing) {
      this.hold(ev, now);
      return skip('discussion');
    }
    // Outside the workflow and running now: a ring would queue behind the run and arrive after the mail may be picked.
    if (outside && th.busy) {
      this.hold(ev, now, 'running');
      return skip('running');
    }
    // A ring queued behind this run (pruned above) has not started yet: its turn will see this mail too. Behind an open
    // record only while the thread really runs that run: a stale record (interrupted, Stop lost) or another T3 run means
    // the queued ring has started or was lost, so this mail rings. A closed run within queuedStartMs: the ring is starting.
    if (queuedMark) {
      const runChanged = th.known && th.running && !!th.run && !!this.state.queuedRun[holder] && th.run !== this.state.queuedRun[holder];
      if (record && record.state === 'open' && (!th.busy || runChanged)) {
        this.log(`queued ${holder}: vlákno ${runChanged ? 'beží iný beh' : 'nebeží'}; zaradený zvonček už nečaká, zvoní sa`);
        this.state.unqueue(holder);
      } else {
        this.state.save();
        return skip('queued');
      }
    }
    // An idle thread rung with the same bell moments ago, whose turn has not opened since: that turn will see this mail
    // too (fable audit M1). Only with known turn records: a lost wake is worse than a double one.
    const idle = this.state.idle[holder];
    if (!th.busy && turnsKnown && idle && bellKey(idle.text) === bellKey(ringText(ev)) && now - idle.at < this.opts.idleDedupeMs
      && !(record && record.opened_at && Date.parse(record.opened_at) >= idle.at)) {
      return skip('queued');
    }

    const dir = directionKey(ev);
    const pair = pairKey(ev);
    const prev = this.state.last[dir];
    // A released event was counted when it came (its hold is not a second copy of the text).
    const repeats = ev.released ? (prev?.count || 1) : prev && prev.hash === ev.text_hash ? prev.count + 1 : 1;
    // Repeat detection only for events with a body hash (blocker/reply).
    if (ev.text_hash && !ev.released) this.state.last[dir] = { hash: ev.text_hash, count: repeats, at: now };
    this.state.prune(now);
    const recent = this.state.rings[pair] || [];

    let limit = null;
    if (ev.text_hash && !ev.released && repeats >= this.opts.stopOnRepeat) limit = { reason: 'repeat', detail: `rovnaká správa ${repeats}× za sebou (${dir})` };
    else if (recent.length >= pairLimit(ev, this.opts.limitPerHour)) limit = { reason: 'hour', detail: `${recent.length} zobudení za poslednú hodinu (${pair}), limit ${pairLimit(ev, this.opts.limitPerHour)}` };

    if (limit) {
      this.stats.limited += 1;
      // report 2026-11-17 (a topic thread): a message above the hourly limit waited for a turn the thread never started (31 min,
      // read only at the operator's next prompt). It is held and rung once the pair's window has room (releaseHeld);
      // the limit stays a rate, never a lost ring. A repeated text stays stopped (a loop, not a rate).
      if (limit.reason === 'hour' && ev.message_id) {
        limit.free_at = new Date(Math.min(...recent) + HOUR).toISOString();
        this.hold(ev, now, 'limit');
      }
      this.state.save();
      this.log(`limit ${ev.seq} (${limit.reason}): ${limit.detail}; správa ${ev.message_id} ${limit.free_at ? `zazvoní, keď sa okno uvoľní (${limit.free_at})` : 'čaká na ďalší ťah'}`);
      const notice = await this.notifyLimit(ev, limit);
      return { action: 'limited', reason: limit.reason, detail: limit.detail, notice };
    }

    const text = ringText(ev);
    recent.push(now);
    this.state.rings[pair] = recent;
    this.state.save();
    let result;
    try {
      result = await this.adapter.ring(String(ev.holder), text);
    } catch (e) {
      result = { ok: false, stderr: e.message };
    }
    if (result && result.ok) {
      this.stats.rang += 1;
      if (th.busy) {
        this.state.queued[holder] = runOf(record);
        if (th.run) this.state.queuedRun[holder] = th.run; else delete this.state.queuedRun[holder];
      } else this.state.idle[holder] = { at: now, text };
      // The reminder counts from the last ring of the message (first ring, released hold or reminder).
      if (ev.message_id) this.state.reminded[ev.message_id] = { count: 0, ...this.state.reminded[ev.message_id], at: now };
      this.state.save();
      this.log(`rang ${ev.seq} ${ev.type} → ${ev.holder}: "${text}" (${recent.length}/${this.opts.limitPerHour} za hodinu, ${pair})${th.busy ? '; vlákno beží, zvonček zaradený' : ''}`);
    } else {
      this.stats.failed += 1;
      const err = (result && (result.stderr || result.stdout)) || 'neznáma chyba';
      this.log(`ring failed ${ev.seq} → ${ev.holder} (${this.adapter.name}): ${err}; správa čaká na ďalší ťah`);
      // Thread archived or gone: no later turn will come; the service makes an unpicked message undelivered and the
      // sender's wait due (a picked one stays, I3).
      if (PERMANENT_RING_FAILURE.test(err) && ev.message_id) {
        try {
          const b = await this.client.bounce({ message_id: ev.message_id, holder: String(ev.holder), reason: err });
          if (b.bounced) this.log(`bounce ${ev.message_id} → undelivered, čakanie odosielateľa splatné`);
          else this.log(`bounce ${ev.message_id} nevrátené (${b.reason})`);
          return { action: 'rang', ok: false, holder: String(ev.holder), text, result, bounce: b };
        } catch (e) {
          this.log(`bounce failed ${ev.message_id}: ${e.message}`);
        }
      }
    }
    return { action: 'rang', ok: !!(result && result.ok), holder: String(ev.holder), text, result };
  }

  /**
   * State of a thread in T3 (adapter `status`, as keepalive and continuation read it): { known, running, gone, run, why }.
   * gone = archived or no longer there (no later turn comes). Unknown without `status` or when T3 does not answer.
   */
  async t3State(thread) {
    if (!this.adapter || typeof this.adapter.status !== 'function') return { known: false, running: false, gone: false, run: null };
    let st;
    try { st = await this.adapter.status(String(thread)); } catch (e) { st = { ok: false, error: e.message }; }
    if (st && st.ok) {
      if (st.archived) return { known: true, running: false, gone: true, run: null, why: `Thread ${thread} is archived (T3 status)` };
      return { known: true, running: !!st.running, gone: false, run: st.run ?? null };
    }
    const err = String((st && st.error) || '');
    if (PERMANENT_RING_FAILURE.test(err)) return { known: true, running: false, gone: true, run: null, why: err.replace(/\s+/g, ' ').trim() };
    return { known: false, running: false, gone: false, run: null, error: err || null };
  }

  /**
   * Whether the holder is busy now: an open turn record of a thread that runs. T3 decides when it answers; else the open
   * record only within its deadline (`turnBusyMaxMs`). Returns the T3 state with `busy`.
   */
  async threadNow(holder, record, now = this.now()) {
    const t3 = await this.t3State(holder);
    const open = !!record && record.state === 'open';
    // Without any record only T3 can say it runs (a thread outside the workflow).
    const busy = !t3.gone && (open ? (t3.known ? t3.running : turnFresh(record, now, this.opts.turnBusyMaxMs)) : (!record && t3.known && t3.running));
    return { ...t3, busy };
  }

  /**
   * The holder is archived or gone (T3): its message becomes undelivered and the sender's wait due (`undelivered <id>`,
   * 1e). The service keeps it when another thread holds the topic now. A failed bounce of a discussion's message holds it
   * (the next poll checks the holder again).
   */
  async undeliverGone(ev, why, { holdOnError = false, now = this.now() } = {}) {
    const holder = String(ev.holder);
    this.stats.failed += 1;
    this.log(`vlákno ${holder}: ${why}; ďalší ťah nepríde, správa ${ev.message_id || '-'} sa nezvoní`);
    if (!ev.message_id) return { action: 'skipped', reason: 'gone', holder };
    try {
      const b = await this.client.bounce({ message_id: ev.message_id, holder, reason: why });
      if (b.bounced) this.log(`bounce ${ev.message_id} → undelivered, čakanie odosielateľa splatné`);
      else this.log(`bounce ${ev.message_id} nevrátené (${b.reason})`);
      return { action: 'undelivered', bounced: !!b.bounced, holder, bounce: b };
    } catch (e) {
      this.log(`bounce failed ${ev.message_id}: ${e.message}`);
      if (holdOnError) this.hold(ev, now);
      return { action: 'undelivered', bounced: false, holder, error: e.message };
    }
  }

  /** A held message's holder is archived or gone: the reason, else null (asked T3 at most once per `goneCheckMs`). */
  async heldGone(holder) {
    const seen = (this.goneSeen ||= {})[holder];
    const now = this.now();
    if (seen && now - seen.at < this.opts.goneCheckMs) return seen.why;
    const t3 = await this.t3State(holder);
    this.goneSeen[holder] = { at: now, why: t3.gone ? t3.why : null };
    return this.goneSeen[holder].why;
  }

  /**
   * Keep an event until its holder's discussion ends, (`reason` running) until the run of a thread outside the workflow
   * ends, or (`reason` limit) until its pair of topics has room under the hourly limit (newest HELD_MAX).
   */
  hold(ev, now = this.now(), reason = 'discussion') {
    const key = ev.message_id || `seq:${ev.seq}`;
    this.state.held[key] = { ...ev, held_at: new Date(now).toISOString(), held_reason: reason };
    const keys = Object.keys(this.state.held);
    for (const k of keys.slice(0, Math.max(0, keys.length - HELD_MAX))) delete this.state.held[k];
    this.state.save();
  }

  /**
   * Ring the held steer/assist events (of one holder, else all) whose holder no longer discusses with the operator.
   * A message picked up, answered or gone meanwhile is dropped without a ring. A holder still discussing that T3 reports
   * archived or gone never ends the discussion: its held message becomes undelivered. Returns the number rung.
   */
  async releaseHeld(holder = null) {
    this.state.prune(this.now());
    const entries = Object.entries(this.state.held).filter(([, ev]) => !holder || String(ev.holder) === holder);
    if (!entries.length) return 0;
    let turns;
    try {
      ({ turns } = await this.client.turns(false));
    } catch (e) {
      this.log(`turns: ${e.message}; pozdržané správy čakajú`);
      return 0;
    }
    let released = 0;
    for (const [key, ev] of entries) {
      if (ev.held_reason === 'limit') {
        // Held above the hourly limit: rung once the pair's window has room again (handle counts the ring).
        if ((this.state.rings[pairKey(ev)] || []).length >= pairLimit(ev, this.opts.limitPerHour)) continue;
        // A bell of the same topic rang the idle holder after the hold: that turn sees this mail too.
        const idle = this.state.idle[String(ev.holder)];
        if (idle && bellKey(idle.text) === bellKey(ringText(ev)) && idle.at > Date.parse(ev.held_at)) {
          delete this.state.held[key];
          this.state.save();
          this.log(`held ${ev.message_id || key}: neskorší zvonček témy ho pokryl; nezvoní sa`);
          continue;
        }
      } else if (ev.held_reason === 'running') {
        // Held while the thread ran (outside the workflow): rung only once T3 no longer runs it; archived or gone bounces.
        const t3 = await this.t3State(String(ev.holder));
        if (t3.gone) {
          delete this.state.held[key];
          this.state.save();
          await this.undeliverGone(ev, t3.why, { holdOnError: true });
          continue;
        }
        if (!t3.known || t3.running) continue;
      } else if (discussionOpen(turns.find((x) => x.thread === String(ev.holder)) || null)) {
        const why = await this.heldGone(String(ev.holder));
        if (!why) continue;
        delete this.state.held[key];
        this.state.save();
        await this.undeliverGone(ev, why, { holdOnError: true });
        continue;
      }
      delete this.state.held[key];
      this.state.save();
      if (ev.message_id) {
        try {
          const st = await this.client.message(ev.message_id);
          // Only a message still in the inbox (pending) is rung; picked, answered, withdrawn or undelivered is not.
          if (st.status !== 'pending') {
            this.log(`held ${ev.message_id}: ${st.status}${st.resolution ? `/${st.resolution}` : ''}; nezvoní sa`);
            continue;
          }
        } catch (e) {
          this.log(`message ${ev.message_id}: ${e.message}; zvoní sa bez kontroly`);
        }
      }
      this.log(`release ${ev.message_id || key} → ${ev.holder}: ${ev.held_reason === 'running' ? 'ťah vlákna skončil' : ev.held_reason === 'limit' ? 'okno limitu sa uvoľnilo' : 'diskusia s operátorom skončila'}`);
      released += 1;
      const { held_at: _a, held_reason: _r, ...orig } = ev;
      await this.handle({ ...orig, created_at: new Date(this.now()).toISOString(), released: true });
    }
    return released;
  }

  // --- due waits of senders (1e) ---------------------------------------------------------
  /**
   * One round over `GET /obligations?due=1`: each due wait of a thread on this machine that is not busy (an open turn of a
   * thread that runs, `threadNow`) is rung with `dueText` and confirmed with `POST /wait-rang` (then the service sets the
   * next check or ends the wait). Not confirmed, so due again in a later cycle: no adapter, a busy thread, turn records unknown, a ring that failed (tried
   * again after DUE_RETRY_MS), another wait of the same thread rung in this round. A ring that fails for good (thread
   * archived or gone) is confirmed: no later turn will come. Returns one result per due item.
   */
  async dueOnce() {
    if (!this.adapter) return [];
    const { due = [] } = await this.client.due();
    if (!due.length) return [];
    const now = this.now();
    for (const [id, at] of Object.entries(this.dueFailed ||= {})) if (now - at >= DUE_RETRY_MS) delete this.dueFailed[id];
    let open = null;
    try {
      const { turns } = await this.client.turns(true);
      open = new Map(turns.filter((t) => t.state === 'open').map((t) => [String(t.thread), t]));
    } catch (e) {
      this.log(`turns: ${e.message}; splatné čakania počkajú na ďalší cyklus`);
    }
    // Busy = an open record of a thread that runs (T3, else the record's deadline); asked once per thread per round.
    const busyNow = new Map();
    const busy = async (holder) => {
      if (!open.has(holder)) return false;
      if (!busyNow.has(holder)) busyNow.set(holder, (await this.threadNow(holder, open.get(holder), now)).busy);
      return busyNow.get(holder);
    };
    const rungNow = new Set();
    const out = [];
    for (const item of due) {
      const res = (action, reason, extra = {}) => {
        if (action !== 'skipped' || !['other-machine', 'running', 'this-round', 'retry'].includes(reason)) this.log(`due ${action} ${item.id} (${item.wait_reason}) → ${item.holder || '-'}${reason ? ` (${reason})` : ''}`);
        return { id: item.id, action, reason, ...extra };
      };
      if (!item.holder) { out.push(res('skipped', 'no-holder')); continue; }
      const holder = String(item.holder);
      if (item.holder_machine && this.opts.machine && normalizeMachine(item.holder_machine) !== normalizeMachine(this.opts.machine)) { out.push(res('skipped', 'other-machine')); continue; }
      if (!open) { out.push(res('skipped', 'turns-unknown')); continue; }
      if (rungNow.has(holder)) { out.push(res('skipped', 'this-round')); continue; }
      if (this.dueFailed[item.id]) { out.push(res('skipped', 'retry')); continue; }
      if (await busy(holder)) { out.push(res('skipped', 'running')); continue; }
      const text = dueText(item);
      let result;
      try { result = await this.adapter.ring(holder, text); } catch (e) { result = { ok: false, stderr: e.message }; }
      const err = result && result.ok ? null : ((result && (result.stderr || result.stdout)) || 'neznáma chyba');
      if (err && !PERMANENT_RING_FAILURE.test(err)) {
        this.stats.failed += 1;
        this.dueFailed[item.id] = this.now();
        out.push(res('failed', 'ring', { text, error: err }));
        continue;
      }
      rungNow.add(holder);
      if (!err) { this.stats.rang += 1; this.stats.due_rang += 1; this.state.idle[holder] = { at: this.now(), text }; this.state.save(); }
      let confirmed = null;
      try {
        // The service's clock decides (no rang_at): the clock of this machine may be behind the service's.
        confirmed = await this.client.waitRang(item.id);
        if (confirmed && confirmed.wait_due_at === item.wait_due_at && (confirmed.wait_n ?? 0) === (item.wait_n ?? 0)) this.log(`wait-rang ${item.id}: služba čakanie nezmenila (wait_n ${confirmed.wait_n})`);
      } catch (e) {
        this.log(`wait-rang ${item.id}: ${e.message}`);
      }
      this.log(`${err ? `due ring failed for good ${item.id} → ${holder}: ${err}; čakanie potvrdené` : `rang due ${item.id} → ${holder}: "${text}"`}${confirmed ? ` (wait_n ${confirmed.wait_n}, ďalšia kontrola ${confirmed.wait_due_at || 'žiadna'})` : ''}`);
      out.push({ id: item.id, action: err ? 'failed' : 'rang', reason: err ? 'permanent' : null, text, holder, confirmed });
    }
    return out;
  }

  async notifyLimit(ev, limit) {
    try {
      return await this.client.createNotice({
        kind: 'wake-limit',
        title: `Limit budenia (${limit.reason === 'repeat' ? 'opakovaná správa' : '6 za hodinu'}): ${topicLabel(ev)}`,
        message_id: ev.message_id,
        detail: `${limit.detail}; vlákno ${ev.holder} nebolo zobudené, ${limit.free_at ? `správa zazvoní, keď sa okno uvoľní (${limit.free_at})` : 'správa čaká na ďalší ťah'}.`,
        dedupe_key: `${limit.reason}:${limit.reason === 'repeat' ? directionKey(ev) : pairKey(ev)}`,
      });
    } catch (e) {
      this.log(`notice failed: ${e.message}`);
      return null;
    }
  }

  // --- keepalive ----------------------------------------------------------------------
  /** One keepalive round over the open turns; returns one result per turn. */
  async keepaliveOnce(signal) {
    if (!this.keepaliveSince) return [];
    const since = await this.keepaliveSince();
    if (since == null) {
      if (this.lastSince !== null) this.log('keepalive: čas štartu T3 neznámy; kolo preskočené');
      this.lastSince = null;
      return [];
    }
    if (since !== this.lastSince) { this.log(`keepalive: štart T3 ${new Date(since).toISOString()}`); this.lastSince = since; }
    this.kaClient.reset?.();
    const { turns } = await this.kaClient.turns(true);
    const out = [];
    for (const turn of turns) {
      if (signal && signal.aborted) break;   // SIGTERM ends the round at the next thread, not after all of them
      out.push(await this.keepaliveTurn(turn, since));
    }
    return out;
  }

  async keepaliveTurn(turn, since) {
    const res = (action, reason, extra = {}) => {
      if (action !== 'skipped' || !['other-machine', 'after-restart', 'running'].includes(reason)) this.log(`keepalive ${action} ${turn.thread}${reason ? ` (${reason})` : ''}`);
      return { thread: turn.thread, action, reason, ...extra };
    };
    if (turn.state !== 'open') return res('skipped', 'closed');
    // Only turns of this machine: a turn without a machine may live on another T3.
    if (!turn.machine || normalizeMachine(turn.machine) !== normalizeMachine(this.opts.machine)) return res('skipped', 'other-machine');
    if (!turn.opened_at || Date.parse(turn.opened_at) >= since) return res('skipped', 'after-restart');
    if (!this.adapter || typeof this.adapter.status !== 'function') return res('skipped', 'no-adapter');
    const st = await this.adapter.status(turn.thread);
    if (!st || !st.ok) return res('skipped', 'status-unknown', { error: st && st.error });
    if (st.running) return res('skipped', 'running');

    if (turn.holder_mode === 'operator') {
      await this.turnInterrupted(turn, 'režim operator: vlákno sa nebudí, pokračovanie rozhodne operátor');
      return res('notified', 'operator-mode');
    }
    const now = this.now();
    this.state.prune(now);
    const recent = this.state.keepalive[turn.thread] || [];
    const max = this.opts.keepaliveMaxPerHour;
    if (recent.length >= max) {
      await this.turnInterrupted(turn, `${recent.length} pokusy o obnovenie za poslednú hodinu, limit ${max}`);
      return res('notified', 'limit');
    }
    if (recent.length && now - recent[recent.length - 1] < this.opts.keepaliveDelayMs) return res('skipped', 'backoff');
    recent.push(now);
    this.state.keepalive[turn.thread] = recent;
    this.state.save();
    try { await this.kaClient.turn({ thread: turn.thread, attempt: true }); } catch (e) { this.log(`keepalive: záznam pokusu zlyhal: ${e.message}`); }
    let result;
    try { result = await this.adapter.ring(String(turn.thread), KEEPALIVE_TEMPLATE); } catch (e) { result = { ok: false, stderr: e.message }; }
    if (result && result.ok) {
      this.stats.keepalive_rang += 1;
      // The resumed turn opens a new record (new opened_at), so it is not rung again.
      await this.closeTurn(turn);
      return res('rang', null, { ok: true, attempt: recent.length });
    }
    this.log(`keepalive ring failed ${turn.thread}: ${(result && (result.stderr || result.stdout)) || 'neznáma chyba'}`);
    if (recent.length >= max) {
      await this.turnInterrupted(turn, `${recent.length}. pokus o obnovenie zlyhal: ${(result && (result.stderr || result.stdout)) || 'neznáma chyba'}`);
      return res('notified', 'failed', { ok: false, attempt: recent.length });
    }
    return res('rang', 'failed', { ok: false, attempt: recent.length });
  }

  /** Operator notice `turn-interrupted` (one open per thread) and close the turn. */
  async turnInterrupted(turn, detail) {
    this.stats.keepalive_notified += 1;
    const where = turn.project && turn.topic ? `${turn.project}/${turn.topic}` : `vlákno ${turn.thread}`;
    try {
      await this.kaClient.createNotice({
        kind: 'turn-interrupted', title: `Prerušený ťah po reštarte T3: ${where}`,
        thread: turn.thread, project: turn.project || null, topic: turn.topic || null,
        detail: `${detail}; ťah otvorený ${turn.opened_at}, vlákno ${turn.thread}.`,
      });
    } catch (e) {
      this.log(`notice failed: ${e.message}`);
    }
    await this.closeTurn(turn);
  }

  /** Close exactly this turn: `opened_at` scopes the close, so a newer turn of the thread stays open. */
  async closeTurn(turn) {
    try { await this.kaClient.turn({ thread: turn.thread, state: 'closed', reason: 'keepalive', opened_at: turn.opened_at || null }); } catch (e) { this.log(`keepalive: zatvorenie ťahu zlyhalo: ${e.message}`); }
  }

  // --- continuation (P-026) -----------------------------------------------------------
  /**
   * Closed turns of this machine that left a next step in the handover: ring the thread once per step after
   * `continueDelayMs` (an operator prompt in between opens a new turn and wins). The same step again after a
   * ring is no progress: one `turn-stalled` notice to the operator, no further ring. Code, not the agent, keeps the loop going.
   * A turn closed with `resume_at` (the thread's timer, P-061) is rung at that time, also with the same step again.
   */
  async continueOnce(signal) {
    if (!this.adapter) return [];
    const { turns } = await this.kaClient.turns(false);
    const out = [];
    for (const turn of turns) {
      if (signal && signal.aborted) break;   // SIGTERM ends the round at the next thread (T3 status takes ~1 s each)
      if (turn.state !== 'closed' || !turn.next || turn.close_reason === 'keepalive') continue;
      out.push(await this.continueTurn(turn));
    }
    return out;
  }

  async continueTurn(turn) {
    const res = (action, reason) => {
      if (action !== 'skipped' || !['other-machine', 'done', 'delay', 'stalled'].includes(reason)) this.log(`continue ${action} ${turn.thread}${reason ? ` (${reason})` : ''}`);
      return { thread: turn.thread, action, reason };
    };
    if (!turn.machine || normalizeMachine(turn.machine) !== normalizeMachine(this.opts.machine)) return res('skipped', 'other-machine');
    const prev = this.state.continued[turn.thread];
    if (prev && prev.closed_at === turn.closed_at) return res('skipped', prev.stalled ? 'stalled' : 'done');
    const now = this.now();
    // P-061 timer: a turn closed with resume_at is rung at that time (a deferral), else after continueDelayMs.
    const due = turn.resume_at ? Date.parse(turn.resume_at) : Date.parse(turn.closed_at) + this.opts.continueDelayMs;
    if (now < due) return res('skipped', 'delay');
    if (typeof this.adapter.status === 'function') {
      const st = await this.adapter.status(turn.thread);
      if (!st || !st.ok) return res('skipped', 'status-unknown');
      if (st.running) return res('skipped', 'running');
    }
    // A timer is a deliberate deferral: it rings even with the same step (the work goes on, P-061).
    if (!turn.resume_at && prev && prev.next === turn.next) {
      this.state.continued[turn.thread] = { ...prev, closed_at: turn.closed_at, stalled: true };
      this.state.save();
      const where = turn.project && turn.topic ? `${turn.project}/${turn.topic}` : `vlákno ${turn.thread}`;
      try {
        await this.kaClient.createNotice({
          kind: 'turn-stalled', title: `Vlákno stojí s ďalším krokom: ${where}`,
          thread: turn.thread, project: turn.project || null, topic: turn.topic || null,
          detail: `Po zobudení na pokračovanie skončilo vlákno ${turn.thread} znova s rovnakým ďalším krokom: „${turn.next}“.`,
        });
      } catch (e) { this.log(`notice failed: ${e.message}`); }
      this.stats.continue_stalled += 1;
      return res('notified', 'stalled');
    }
    this.state.continued[turn.thread] = { next: turn.next, closed_at: turn.closed_at, at: now, stalled: false };
    this.state.save();
    let result;
    try { result = await this.adapter.ring(String(turn.thread), CONTINUE_TEMPLATE); } catch (e) { result = { ok: false, stderr: e.message }; }
    if (result && result.ok) { this.stats.continue_rang += 1; return res('rang', null); }
    this.log(`continue ring failed ${turn.thread}: ${(result && (result.stderr || result.stdout)) || 'neznáma chyba'}`);
    return res('failed', 'ring');
  }

  async keepaliveLoop(signal) {
    while (!signal || !signal.aborted) {
      try {
        await this.continueOnce(signal);
      } catch (e) {
        if (signal && signal.aborted) break;
        this.log(`continue: ${e.code === 'UNAVAILABLE' ? 'comms unavailable' : `error: ${e.message}`}`);
      }
      try {
        await this.keepaliveOnce(signal);
      } catch (e) {
        if (signal && signal.aborted) break;
        this.log(`keepalive: ${e.code === 'UNAVAILABLE' ? 'comms unavailable' : `error: ${e.message}`}`);
      }
      await sleep(this.opts.keepaliveIntervalMs, signal);
    }
  }

  /**
   * One run of a periodic job (`jobs` in wake.json). A second run of the same job does not start while the first one
   * runs; a failure (exit code, time limit, spawn error) is logged and counted, it never throws.
   */
  async runJobOnce(job, signal) {
    if (this.jobsRunning.has(job.name)) {
      this.log(`úloha ${job.name}: predchádzajúci beh ešte beží, ďalší sa nespúšťa`);
      return { action: 'skipped', reason: 'running' };
    }
    this.jobsRunning.add(job.name);
    try {
      const prev = this.state.jobs[job.name];
      this.state.jobs[job.name] = { ...(prev && typeof prev === 'object' ? prev : {}), started_at: new Date().toISOString() };
      this.state.save();
      const r = await runJob(job.command, { timeout: job.timeout_ms, signal });
      const failure = r.ok ? null : jobFailure(r);
      this.state.jobs[job.name] = { started_at: this.state.jobs[job.name].started_at, finished_at: new Date().toISOString(), ok: r.ok, code: r.code, ms: r.ms, error: failure };
      this.state.save();
      if (r.ok) {
        this.stats.jobs_ok += 1;
        this.log(`úloha ${job.name}: ok (${r.ms} ms)`);
        return { action: 'ok', ms: r.ms };
      }
      if (r.aborted) {
        this.log(`úloha ${job.name}: prerušená pri ukončení budenia`);
        return { action: 'aborted' };
      }
      this.stats.jobs_failed += 1;
      const tail = r.output ? r.output.split(/\r?\n/).filter(Boolean).join(' | ').slice(-300) : '';
      this.log(`úloha ${job.name}: zlyhala (${failure})${tail ? `: ${tail}` : ''}`);
      return { action: 'failed', reason: failure, code: r.code };
    } finally {
      this.jobsRunning.delete(job.name);
    }
  }

  /**
   * When the job runs next (epoch ms, wall clock): never run → now; else the last start + interval, after a run longer
   * than the interval its end + interval. Never later than now + interval (a clock set back, a start in the future).
   */
  jobDue(job, now = Date.now()) {
    const st = this.state.jobs[job.name];
    const started = st ? Date.parse(st.started_at) : NaN;
    if (!Number.isFinite(started)) return now;
    const finished = Date.parse(st.finished_at);
    let due = started + job.interval_ms;
    if (Number.isFinite(finished) && finished - started >= job.interval_ms) due = finished + job.interval_ms;
    return Math.min(due, now + job.interval_ms);
  }

  /** Loop of one periodic job; it ends with the signal. Errors are logged and the job waits one interval. */
  async jobLoop(job, signal) {
    while (!signal || !signal.aborted) {
      try {
        const wait = this.jobDue(job) - Date.now();
        if (wait > 0) { await sleep(Math.min(wait, MAX_TIMER_MS), signal); continue; }
        await this.runJobOnce(job, signal);
      } catch (e) {
        if (signal && signal.aborted) break;
        this.log(`úloha ${job.name}: chyba: ${e.message}`);
        await sleep(Math.min(job.interval_ms, MAX_TIMER_MS), signal);
      }
    }
  }

  /** All periodic jobs side by side (each in its own loop); ends when the signal aborts. */
  async jobsLoop(signal) {
    await Promise.all((this.opts.jobs || []).map((job) => this.jobLoop(job, signal)));
  }

  /** Where to start: persisted cursor, else the current end of the stream (no replay of history). */
  async initCursor() {
    if (this.state.cursor != null) return this.state.cursor;
    const r = await this.client.events('latest', 0);
    this.state.cursor = r.next || 0;
    this.state.save();
    this.log(`start at seq ${this.state.cursor}`);
    return this.state.cursor;
  }

  /**
   * One cycle: the due waits first (a restart rings what stayed unconfirmed at once), then one long-poll round; returns
   * the number of events processed. A refused due read (4xx) is logged, the events still run.
   */
  async pollOnce(waitMs = this.opts.waitMs) {
    try {
      await this.dueOnce();
    } catch (e) {
      if (e.code !== 'REJECTED') throw e;
      this.log(`due: ${e.message}`);
    }
    const since = await this.initCursor();
    const r = await this.client.events(since, waitMs);
    for (const ev of r.events) {
      await this.handle(ev);
      this.state.cursor = ev.seq;
      this.state.save();
    }
    // Held rings whose discussion ended without a `discussion-closed` event (older service, missed event).
    if (Object.keys(this.state.held).length) {
      try { await this.releaseHeld(); } catch (e) { this.log(`release: ${e.message}`); }
    }
    try { await this.remindOnce(); } catch (e) { this.log(`remind: ${e.message}`); }
    return r.events.length;
  }

  /**
   * Reminder: a message still pending after `remindAfterMs` rings its recipient again through handle() (the same checks
   * as its first ring: picked, busy, discussion, hourly limit, archived → undelivered, other machine), at most `remindMax`
   * rings, then one operator notice; a message older than `remindMaxAgeMs` gets no ring, only one summary notice of all
   * such messages. Cause (operator 2026-11-17, critical): a ring above the hourly limit, an event older than
   * `maxEventAgeMs` (wake down) or a bell turn that could not read the mail left messages unread for days, and nobody
   * rang again. Returns the number rung. An older service without `/unpicked` (404) turns it off until a restart.
   */
  async remindOnce() {
    const now = this.now();
    if (this.remindOff || !this.adapter || now - (this.lastRemind || 0) < this.opts.remindEveryMs) return 0;
    this.lastRemind = now;
    let events;
    try { ({ events } = await this.client.unpicked(0)); } catch (e) {
      if (e.status === 404 || /404|not found/i.test(String(e.message))) { this.remindOff = true; this.log('remind: služba nemá /unpicked; pripomienky vypnuté do reštartu'); return 0; }
      throw e;
    }
    const ids = new Set(events.map((ev) => ev.message_id));
    for (const id of Object.keys(this.state.reminded)) if (!ids.has(id)) delete this.state.reminded[id];
    const mine = (ev) => !(ev.holder_machine && this.opts.machine && normalizeMachine(ev.holder_machine) !== normalizeMachine(this.opts.machine));
    const old = [];
    let rang = 0;
    for (const ev of events) {
      if (!ev.message_id || !mine(ev) || this.state.held[ev.message_id]) continue;
      const age = now - Date.parse(ev.created_at);
      if (age < this.opts.remindAfterMs) continue;
      // Too old to ring: once in the summary notice (an acked summary is not made again for the same messages).
      if (age > this.opts.remindMaxAgeMs) { if (!this.state.reminded[ev.message_id]?.noticed) old.push(ev); continue; }
      const r = this.state.reminded[ev.message_id] || { count: 0, at: 0 };
      if (r.count >= this.opts.remindMax) {
        if (!r.noticed) {
          r.noticed = true;
          this.state.reminded[ev.message_id] = r;
          await this.notice({ kind: 'unpicked', title: `Správa nevyzdvihnutá po ${r.count} pripomienkach: ${topicLabel(ev)}`, message_id: ev.message_id,
            detail: `${ev.type} ${ev.message_id} od ${ev.sender || '?'} čaká od ${ev.created_at}; vlákno ${ev.holder || '(bez držiteľa)'} ju po ${r.count} zvončekoch nevyzdvihlo. Over vlákno (beží? vie čítať poštu?) alebo tému prevezmi.`,
            dedupe_key: `unpicked:${ev.message_id}` });
        }
        continue;
      }
      if (now - r.at < this.opts.remindAfterMs) continue;
      if (!ev.holder) {
        if (!r.noticed) {
          r.noticed = true; r.at = now;
          this.state.reminded[ev.message_id] = r;
          await this.notice({ kind: 'unpicked', title: `Správa bez príjemcu: ${topicLabel(ev)}`, message_id: ev.message_id,
            detail: `${ev.type} ${ev.message_id} od ${ev.sender || '?'} čaká od ${ev.created_at} v schránke ${topicLabel(ev)}, ktorú nedrží žiadne vlákno: nikto ju nezvolá. Tému prevezmi alebo správu presmeruj.`,
            dedupe_key: `unpicked:${ev.message_id}` });
        }
        continue;
      }
      const res = await this.handle({ ...ev, created_at: new Date(now).toISOString(), released: true, reminder: true });
      this.state.reminded[ev.message_id] = { count: r.count + (res.action === 'rang' ? 1 : 0), at: now };
      if (res.action === 'rang') { rang += 1; this.log(`remind ${ev.message_id}: ${r.count + 1}. pripomienka → ${ev.holder}`); }
    }
    if (old.length) {
      await this.notice({ kind: 'unpicked', title: `${old.length} správ(y) nevyzdvihnutých dlhšie ako ${Math.round(this.opts.remindMaxAgeMs / HOUR)} h`, message_id: old[0].message_id,
        detail: `Najstaršia ${old[0].message_id} (${topicLabel(old[0])}, od ${old[0].created_at}). Bez zvončeka (staré); over príjemcov: ${[...new Set(old.map(topicLabel))].slice(0, 10).join(', ')}.`,
        dedupe_key: 'unpicked:old' });
      for (const ev of old) this.state.reminded[ev.message_id] = { count: 0, ...this.state.reminded[ev.message_id], at: now, noticed: true };
    }
    this.state.save();
    return rang;
  }

  async notice(body) {
    try { return await this.client.createNotice(body); } catch (e) { this.log(`notice failed: ${e.message}`); return null; }
  }

  /** Run until the signal aborts; reconnects with back-off while the mail is unavailable. */
  async run({ signal } = {}) {
    // Keepalive runs next to the long-poll and stops with it (also when run throws).
    const kac = new AbortController();
    const onAbort = () => kac.abort();
    signal?.addEventListener('abort', onAbort, { once: true });
    const keepalive = (this.keepaliveSince || this.adapter) ? this.keepaliveLoop(kac.signal) : null;
    // Periodic jobs (wake.json `jobs`) run next to it, independent of the mail (a job runs also while the mail is down).
    const jobs = this.opts.jobs?.length ? this.jobsLoop(kac.signal) : null;
    try {
      return await this.pollLoop(signal);
    } finally {
      kac.abort();
      signal?.removeEventListener('abort', onAbort);
      if (keepalive) await keepalive;
      if (jobs) await jobs;
      this.state.save();
    }
  }

  async pollLoop(signal) {
    let backoff = this.opts.reconnectMs;
    let outage = false;
    while (!signal || !signal.aborted) {
      try {
        await this.pollOnce();
        if (outage) { this.log('comms available again'); outage = false; }
        backoff = this.opts.reconnectMs;
      } catch (e) {
        if (signal && signal.aborted) break;
        if (e.code === 'INSTANCE_MISMATCH') throw e;
        // Another model (4a): one line per outage, nothing is rung or confirmed; tried again until mail and workflow match.
        const quiet = outage === e.code && (e.code === 'UNAVAILABLE' || e.code === 'MODEL_MISMATCH');
        if (!quiet) this.log(`${e.code === 'UNAVAILABLE' ? 'comms unavailable' : e.code === 'MODEL_MISMATCH' ? e.message : `error: ${e.message}`}; nový pokus o ${backoff} ms`);
        outage = e.code || true;
        this.client.reset?.();
        await sleep(backoff, signal);
        backoff = Math.min(backoff * 2, this.opts.maxReconnectMs);
      }
    }
    return this.stats;
  }
}

/**
 * Last start of the T3 server from the output of `since_command` (run through the
 * shell): ISO, epoch seconds/ms, `@epoch` or the systemd format. null when unknown.
 */
export function readSince(command, { timeout = 10_000, env = process.env } = {}) {
  return new Promise((resolve) => {
    let child;
    try { child = spawn(command, { shell: true, stdio: ['ignore', 'pipe', 'ignore'], env, windowsHide: true }); } catch { return resolve(null); }
    let out = '';
    const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* gone */ } resolve(null); }, timeout);
    child.stdout.on('data', (d) => { out += d; });
    child.on('error', () => { clearTimeout(timer); resolve(null); });
    child.on('close', (code) => { clearTimeout(timer); resolve(code === 0 ? parseTimestamp(out) : null); });
  });
}

/**
 * Run one periodic job command through the shell (like readSince) with a time limit; on the limit or the signal the
 * whole process group is killed. Never rejects: { ok, code, signal, timedOut, aborted, error, output (tail), ms }.
 */
export function runJob(command, { timeout = JOB_DEFAULT_TIMEOUT_MS, env = process.env, signal = null } = {}) {
  const t0 = Date.now();
  return new Promise((resolve) => {
    let child = null;
    let out = '';
    let settled = false;
    let timedOut = false;
    let aborted = false;
    let timer = null;
    const finish = ({ code = null, signal: sig = null, error = null } = {}) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
      resolve({ ok: !error && !timedOut && !aborted && code === 0, code, signal: sig, timedOut, aborted, error, output: out.trim(), ms: Date.now() - t0 });
    };
    // After the kill: `close` waits for the pipes; a grandchild that escaped the group would hold them, so finish anyway.
    const kill = () => { killTree(child); const t = setTimeout(() => finish({ signal: 'SIGKILL' }), 2000); t.unref?.(); };
    const onAbort = () => { aborted = true; kill(); };
    try {
      child = spawn(command, { shell: true, stdio: ['ignore', 'pipe', 'pipe'], env, windowsHide: true, detached: process.platform !== 'win32' });
    } catch (e) {
      return finish({ error: e.message });
    }
    const keep = (d) => { out = (out + d).slice(-JOB_OUTPUT_MAX); };
    child.stdout.on('data', keep);
    child.stderr.on('data', keep);
    child.on('error', (e) => finish({ error: e.message }));
    child.on('close', (code, sig) => finish({ code, signal: sig }));
    timer = setTimeout(() => { timedOut = true; kill(); }, Math.min(timeout, MAX_TIMER_MS));
    if (signal) { if (signal.aborted) onAbort(); else signal.addEventListener('abort', onAbort, { once: true }); }
  });
}

/** Kill a job with its children: the process group on POSIX (spawned detached), the tree on Windows. */
function killTree(child) {
  if (!child || child.pid == null) return;
  try {
    if (process.platform === 'win32') spawn('taskkill', ['/pid', String(child.pid), '/T', '/F'], { stdio: 'ignore', windowsHide: true }).on('error', () => {});
    else process.kill(-child.pid, 'SIGKILL');
  } catch {
    try { child.kill('SIGKILL'); } catch { /* gone */ }
  }
}

function jobFailure(r) {
  if (r.error) return `nespustila sa: ${r.error}`;
  if (r.aborted) return 'prerušená';
  if (r.timedOut) return 'prekročila časový limit';
  return r.code != null ? `kód ${r.code}` : `signál ${r.signal}`;
}

/**
 * Periodic jobs from wake.json `jobs`: [{ name, command, interval_ms?, timeout_ms? }]. An invalid entry is skipped,
 * an interval under the minimum is raised to it; each case gives one line in `problems` (logged at start).
 * interval_ms defaults to 15 min, timeout_ms to 5 min and never exceeds the interval.
 */
export function normalizeJobs(raw, { minIntervalMs = JOB_MIN_INTERVAL_MS } = {}) {
  const jobs = [];
  const problems = [];
  if (raw == null) return { jobs, problems };
  if (!Array.isArray(raw)) {
    problems.push('jobs: musí byť pole úloh; ignorované');
    return { jobs, problems };
  }
  const toNum = (v) => (typeof v === 'number' ? v : typeof v === 'string' && v.trim() ? Number(v) : NaN);
  const names = new Set();
  raw.forEach((j, i) => {
    const at = `jobs[${i}]`;
    if (!j || typeof j !== 'object' || Array.isArray(j)) return problems.push(`${at}: nie je objekt; preskočená`);
    const name = typeof j.name === 'string' ? j.name.trim() : '';
    const label = name ? `${at} (${name})` : at;
    if (!JOB_NAME_RE.test(name)) return problems.push(`${label}: chýba platné meno (písmená, číslice, „.“, „_“, „-“, najviac 64 znakov); preskočená`);
    if (names.has(name)) return problems.push(`${label}: meno sa opakuje; preskočená`);
    const command = typeof j.command === 'string' ? j.command.trim() : '';
    if (!command) return problems.push(`${label}: chýba príkaz (command); preskočená`);
    let interval = JOB_DEFAULT_INTERVAL_MS;
    if (j.interval_ms != null) {
      interval = toNum(j.interval_ms);
      if (!Number.isFinite(interval) || interval <= 0) return problems.push(`${label}: neplatný interval_ms ${JSON.stringify(j.interval_ms)}; preskočená`);
      if (interval < minIntervalMs) {
        problems.push(`${label}: interval_ms ${interval} je pod minimom ${minIntervalMs}; použije sa ${minIntervalMs}`);
        interval = minIntervalMs;
      }
    }
    let timeout = JOB_DEFAULT_TIMEOUT_MS;
    if (j.timeout_ms != null) {
      timeout = toNum(j.timeout_ms);
      if (!Number.isFinite(timeout) || timeout <= 0) return problems.push(`${label}: neplatný timeout_ms ${JSON.stringify(j.timeout_ms)}; preskočená`);
    }
    names.add(name);
    jobs.push({ name, command, interval_ms: Math.round(interval), timeout_ms: Math.round(Math.min(timeout, interval, MAX_TIMER_MS)) });
  });
  return { jobs, problems };
}

/**
 * Resolve wake configuration: CLI options > environment > `--config` file (keys
 * `wake.adapter`, `wake.command`, `wake.limit_per_hour`, `wake.stop_on_repeat`,
 * `wake.max_event_age_ms`, `wake.state_dir`, `wake.wait_ms`, `wake.reconnect_ms`,
 * `wake.status_command`, `wake.keepalive.since_command`, `wake.keepalive.interval_ms`, `wake.jobs`)
 * > `<dataDir>/wake.json` (only without --config/COMMS_CONFIG; same keys as `wake`)
 * > defaults. The mail part reuses the client configuration.
 */
export function resolveWakeConfig(opts = {}, env = process.env) {
  const comms = resolveConfig(opts, env);
  const cfgPath = opts.config || env.COMMS_CONFIG || null;
  const file = cfgPath ? readJson(path.resolve(cfgPath), {}) : {};
  // Without --config/COMMS_CONFIG the wake section comes from <dataDir>/wake.json (same keys).
  const wakeFile = cfgPath ? null : path.join(comms.dataDir, 'wake.json');
  const wake = (cfgPath ? file.wake : readJson(wakeFile, null)) || {};
  const ka = wake.keepalive || {};
  const num = (a, b, c, d) => { const v = a ?? b ?? c; const n = Number(v); return v == null || v === '' || Number.isNaN(n) ? d : n; };
  const stateDir = opts.stateDir || env.WAKE_STATE_DIR || wake.state_dir || path.join(comms.clientDir, 'wake');
  // Periodic jobs come only from the file (no flag, no variable); invalid entries are reported in jobProblems.
  const { jobs, problems: jobProblems } = normalizeJobs(wake.jobs);
  return {
    comms,
    machine: normalizeMachine(opts.machine || env.WAKE_MACHINE || wake.machine || comms.machine || os.hostname()),
    adapter: opts.adapter || env.WAKE_ADAPTER || wake.adapter || 't3',
    command: opts.command || env.WAKE_COMMAND || wake.command || DEFAULT_T3_COMMAND,
    statusCommand: opts.statusCommand || env.WAKE_STATUS_COMMAND || wake.status_command || DEFAULT_T3_STATUS_COMMAND,
    // Keepalive is off unless the command printing the last T3 start is configured.
    keepaliveSinceCommand: opts.keepaliveSinceCommand || env.WAKE_KEEPALIVE_SINCE_COMMAND || ka.since_command || null,
    keepaliveIntervalMs: num(opts.keepaliveIntervalMs, env.WAKE_KEEPALIVE_INTERVAL_MS, ka.interval_ms, DEFAULTS.keepaliveIntervalMs),
    limitPerHour: num(opts.limitPerHour, env.WAKE_LIMIT_PER_HOUR, wake.limit_per_hour, DEFAULTS.limitPerHour),
    stopOnRepeat: num(opts.stopOnRepeat, env.WAKE_STOP_ON_REPEAT, wake.stop_on_repeat, DEFAULTS.stopOnRepeat),
    maxEventAgeMs: num(opts.maxEventAgeMs, env.WAKE_MAX_EVENT_AGE_MS, wake.max_event_age_ms, DEFAULTS.maxEventAgeMs),
    waitMs: num(opts.waitMs, env.WAKE_WAIT_MS, wake.wait_ms, DEFAULTS.waitMs),
    reconnectMs: num(opts.reconnectMs, env.WAKE_RECONNECT_MS, wake.reconnect_ms, DEFAULTS.reconnectMs),
    remindAfterMs: num(opts.remindAfterMs, env.WAKE_REMIND_AFTER_MS, wake.remind_after_ms, DEFAULTS.remindAfterMs),
    remindMax: num(opts.remindMax, env.WAKE_REMIND_MAX, wake.remind_max, DEFAULTS.remindMax),
    stateDir,
    stateFile: path.join(stateDir, 'wake-state.json'),
    wakeFile,
    selfRestart: selfRestartEnabled(opts, env, file, wake),
    jobs,
    jobProblems,
  };
}

/** Adapter by name; `none` logs only (nothing is woken). */
export function createAdapter(cfg) {
  switch (cfg.adapter) {
    case 't3': return createT3Adapter({ command: cfg.command, statusCommand: cfg.statusCommand });
    case 'none': case '': case null: case undefined: return null;
    default: throw new Error(`neznámy adaptér budenia „${cfg.adapter}“ (pilot má iba t3)`);
  }
}

/**
 * Key for the mail: configured key, else (local instance on this machine) the
 * key of sender `wake` from keys.json, created when missing.
 */
export function ensureKey(cfg) {
  if (cfg.comms.key) return cfg.comms.key;
  if (cfg.comms.url) throw new Error('pri --url je potrebný kľúč (--key, WAKE/COMMS_KEY alebo "key" v --config)');
  const store = new Store(cfg.comms.dataDir);
  if (!store.exists()) throw new Error(`inštancia v ${cfg.comms.dataDir} neexistuje; spusťte najprv „comms init“`);
  const existing = store.keys().find((k) => k.sender === WAKE_SENDER);
  const key = existing ? existing.key : store.addKey(WAKE_SENDER).key;
  cfg.comms.key = key;
  return key;
}

export function createWaker(cfg, { log = () => {}, now } = {}) {
  ensureKey(cfg);
  const client = new Client(cfg.comms);
  const adapter = createAdapter(cfg);
  const keepaliveSince = cfg.keepaliveSinceCommand ? () => readSince(cfg.keepaliveSinceCommand) : null;
  return new Waker({
    client, adapter, stateFile: cfg.stateFile, log, now,
    limitPerHour: cfg.limitPerHour, stopOnRepeat: cfg.stopOnRepeat, maxEventAgeMs: cfg.maxEventAgeMs, waitMs: cfg.waitMs, reconnectMs: cfg.reconnectMs,
    remindAfterMs: cfg.remindAfterMs, remindMax: cfg.remindMax,
    machine: cfg.machine,
    keepaliveSince, keepaliveClient: keepaliveSince ? new Client(cfg.comms) : null, keepaliveIntervalMs: cfg.keepaliveIntervalMs,
    jobs: cfg.jobs || [],
  });
}

function sleep(ms, signal) {
  return new Promise((resolve) => {
    if (signal && signal.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() { clearTimeout(t); signal?.removeEventListener('abort', done); resolve(); }
    signal?.addEventListener('abort', done, { once: true });
  });
}
