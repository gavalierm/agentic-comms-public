// Wake module (P-043): a separate process above the mail service. It consumes
// wake events (`blocker`/`steer`/`assist` → holder of the topic, `reply` to any level → the
// sender's thread) through the long-poll `events` operation and calls the
// adapter `ring(holder, text)`. The text is always the fixed template, never the
// body. Limits: at most `limitPerHour` rings per pair of topics and a stop when
// the same message arrives `stopOnRepeat` times in a row; above the limit the
// message stays in the mailbox and the operator gets a notice (P-046).
// The mail works without this module: every message waits for the next turn.
// A message not picked up (body read) within its TTL arrives as an `expired` event for the sender, rung with
// `no reply <id>` unless the message was answered or picked up meanwhile (checked with `message`).
// A blocker/steer/assist/reply ring that fails for good (thread archived or gone) bounces: the service
// marks the message undelivered and the sender gets an `undelivered` event, rung with
// `undelivered <id>`; those two never bounce. A busy thread gets a ring as its next turn
// (T3): at most one queued ring per open turn of the holder (`turns`, wake-state `queued`).
// Keepalive (optional, `keepalive.since_command`): open turns of this machine opened
// before the last T3 start whose thread no longer runs are rung with
// KEEPALIVE_TEMPLATE (at most 3 attempts per hour per thread, ≥ 60 s apart) or, in
// operator mode / after the 3rd failure, reported as `turn-interrupted`.
// Discussion (D-0015, D-0022): a steer/assist to a holder that discusses with the operator (turn record, `discussionOpen`)
// is held (wake-state `held`) and rung when the discussion ends (event `discussion-closed`, else the next poll); a
// blocker passes.
// Jobs (D-0023, D-0024): `jobs` in wake.json are shell commands run periodically next to the long-poll (e.g. the reporter's
// issue watch), one run of a job at a time, with a time limit; a failure is logged, it never stops the module.
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { readJson, writeJson, nowIso, normalizeMachine, parseTimestamp, discussionOpen, DISCUSSION_HELD_LEVELS } from '../common/util.mjs';
import { resolveConfig, Client } from '../client/index.mjs';
import { Store } from '../server/store.mjs';
import { selfRestartEnabled } from '../common/selfrestart.mjs';
import { createT3Adapter, DEFAULT_T3_COMMAND, DEFAULT_T3_STATUS_COMMAND } from '../adapters/t3/index.mjs';

/** Fixed template; the workflow hook recognises this prefix (policy `wake_template`, P-047). */
export const WAKE_TEMPLATE = 'you have mail in topic';
/** Fixed templates for the sender of an unanswered / undelivered message (`<template> <message_id>`). */
export const NO_REPLY_TEMPLATE = 'no reply';
export const UNDELIVERED_TEMPLATE = 'undelivered';
/** Event types the module rings; only `blocker`/`steer`/`assist`/`reply` bounce on a permanent failure. */
export const WAKE_TYPES = ['blocker', 'steer', 'assist', 'reply', 'expired', 'undelivered'];
const BOUNCE_TYPES = ['blocker', 'steer', 'assist', 'reply'];
/** The service's event when a thread's discussion with the operator ends: the held rings of that holder go out. */
export const DISCUSSION_CLOSED = 'discussion-closed';
/** Held rings at most this long and this many (the message itself waits in the mailbox anyway). */
const HELD_MAX_MS = 24 * 60 * 60 * 1000;
const HELD_MAX = 200;
/** Fixed keepalive template (policy `prompt.keepalive_template` in the workflow). */
export const KEEPALIVE_TEMPLATE = 'turn interrupted, continue from handover';
/** Continuation (P-026): a turn closed with a next step left in the handover is rung once per step. */
export const CONTINUE_TEMPLATE = 'turn ended with a next step, continue from handover';
export const DEFAULTS = {
  limitPerHour: 6, stopOnRepeat: 2, maxEventAgeMs: 60 * 60 * 1000, waitMs: 25_000, reconnectMs: 1000, maxReconnectMs: 30_000,
  keepaliveIntervalMs: 60_000, keepaliveMaxPerHour: 3, keepaliveDelayMs: 60_000, continueDelayMs: 60_000, idleDedupeMs: 120_000,
};
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
/** Ring errors after which no later turn of the thread will come (the sender gets a bounce). */
export const PERMANENT_RING_FAILURE = /archived|not found|does not exist|no such thread|unknown thread/i;

/** Topic label of an event: `project/topic`, or the thread when the recipient has no topic. */
export function topicLabel(ev) {
  if (ev.project && ev.topic) return `${ev.project}/${ev.topic}`;
  return `thread/${ev.holder}`;
}

export function wakeText(ev) {
  return `${WAKE_TEMPLATE} ${topicLabel(ev)}`;
}

/** Exact ring text of an event (the whole prompt, never the body). */
export function ringText(ev) {
  if (ev.type === 'expired') return `${NO_REPLY_TEMPLATE} ${ev.message_id}`;
  if (ev.type === 'undelivered') return `${UNDELIVERED_TEMPLATE} ${ev.message_id}`;
  return wakeText(ev);
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
    // holder → opened_at of the open turn that already has one ring queued.
    this.queued = d.queued || {};
    // holder → { at, text }: the last ring of a thread without an open turn (its turn opens seconds later).
    this.idle = d.idle || {};
    // message id → event of a steer/assist held while its holder discusses with the operator (D-0015).
    this.held = d.held || {};
    // job name → { started_at, finished_at, ok, code, ms, error }: the last run of a periodic job (its next run survives a restart).
    this.jobs = d.jobs || {};
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

  /** Keep only the queued marks whose open turn is still the current one. */
  pruneQueued(openTurns) {
    const cur = new Map(openTurns.map((t) => [t.thread, t.opened_at]));
    for (const [holder, at] of Object.entries(this.queued)) if (cur.get(holder) !== at) delete this.queued[holder];
  }

  save() {
    this.updated_at = nowIso();
    if (this.file) writeJson(this.file, { cursor: this.cursor, rings: this.rings, last: this.last, keepalive: this.keepalive, continued: this.continued, queued: this.queued, idle: this.idle, held: this.held, jobs: this.jobs, updated_at: this.updated_at });
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
    this.stats = { seen: 0, rang: 0, limited: 0, skipped: 0, failed: 0, keepalive_rang: 0, keepalive_notified: 0, continue_rang: 0, continue_stalled: 0, jobs_ok: 0, jobs_failed: 0 };
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

    // Expired: answered, picked up (acked/discarded) meanwhile → nothing to say.
    if (ev.type === 'expired' && ev.message_id) {
      try {
        const st = await this.client.message(ev.message_id);
        // Interim `otázka` of origin `code` (the recipient waits for the operator's dialog): the ring tells the sender so.
        const byCode = st.answer_origin === 'code' && st.answer_state === 'otázka';
        // A held message (unregistered topic, persona without a main thread) is open as a pending one: its expiry tells the
        // sender nobody got it (4 silent expiries, audit M5 2026-11-11).
        const open = st.state === 'pending' || st.state === 'held';
        if (byCode ? !open : (st.replied || !open || st.picked_at)) return skip('answered');
      } catch (e) {
        this.log(`message ${ev.message_id}: ${e.message}; zvoní sa bez kontroly`);
      }
    }

    // A busy thread gets the ring as its next turn: at most one queued ring per open turn.
    let openTurn = null;
    let record = null;
    let turnsKnown = false;
    try {
      const { turns } = await this.client.turns(false);
      const open = turns.filter((x) => x.state === 'open');
      this.state.pruneQueued(open);
      record = turns.find((x) => x.thread === String(ev.holder)) || null;
      openTurn = open.find((x) => x.thread === String(ev.holder) && x.opened_at) || null;
      turnsKnown = true;
    } catch (e) {
      this.log(`turns: ${e.message}; bez kontroly zaradeného zvončeka`);
    }
    // The holder discusses with the operator (D-0015, D-0022): a steer/assist waits for the end of the discussion; a
    // blocker passes. Without known turn records it rings, as before.
    if (turnsKnown && DISCUSSION_HELD_LEVELS.includes(ev.type) && discussionOpen(record)) {
      this.hold(ev, now);
      return skip('discussion');
    }
    if (openTurn && this.state.queued[String(ev.holder)] === openTurn.opened_at) {
      this.state.save();
      return skip('queued');
    }
    // An idle thread rung with the same bell moments ago, whose turn has not opened since: that turn will see this mail
    // too (fable audit M1). Only with known turn records: a lost wake is worse than a double one.
    const idle = this.state.idle[String(ev.holder)];
    if (!openTurn && turnsKnown && idle && idle.text === ringText(ev) && now - idle.at < this.opts.idleDedupeMs
      && !(record && record.opened_at && Date.parse(record.opened_at) >= idle.at)) {
      return skip('queued');
    }

    const dir = directionKey(ev);
    const pair = pairKey(ev);
    const prev = this.state.last[dir];
    const repeats = prev && prev.hash === ev.text_hash ? prev.count + 1 : 1;
    // Repeat detection only for events with a body hash (blocker/reply).
    if (ev.text_hash) this.state.last[dir] = { hash: ev.text_hash, count: repeats, at: now };
    this.state.prune(now);
    const recent = this.state.rings[pair] || [];

    let limit = null;
    if (ev.text_hash && repeats >= this.opts.stopOnRepeat) limit = { reason: 'repeat', detail: `rovnaká správa ${repeats}× za sebou (${dir})` };
    else if (recent.length >= this.opts.limitPerHour) limit = { reason: 'hour', detail: `${recent.length} zobudení za poslednú hodinu (${pair}), limit ${this.opts.limitPerHour}` };

    if (limit) {
      this.stats.limited += 1;
      this.state.save();
      this.log(`limit ${ev.seq} (${limit.reason}): ${limit.detail}; správa ${ev.message_id} čaká na ďalší ťah`);
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
      if (openTurn) this.state.queued[String(ev.holder)] = openTurn.opened_at;
      else this.state.idle[String(ev.holder)] = { at: now, text };
      this.state.save();
      this.log(`rang ${ev.seq} ${ev.type} → ${ev.holder}: "${text}" (${recent.length}/${this.opts.limitPerHour} za hodinu, ${pair})${openTurn ? '; vlákno má otvorený ťah, zvonček zaradený' : ''}`);
    } else {
      this.stats.failed += 1;
      const err = (result && (result.stderr || result.stdout)) || 'neznáma chyba';
      this.log(`ring failed ${ev.seq} → ${ev.holder} (${this.adapter.name}): ${err}; správa čaká na ďalší ťah`);
      // Thread archived or gone: no later turn will come; tell the sender (`undelivered` event).
      // A ring about the sender's own message (expired/undelivered) never bounces.
      if (PERMANENT_RING_FAILURE.test(err) && ev.message_id && BOUNCE_TYPES.includes(ev.type)) {
        try {
          const b = await this.client.bounce({ message_id: ev.message_id, holder: String(ev.holder), reason: err });
          if (b.bounced) this.log(`bounce ${ev.message_id} → udalosť undelivered pre odosielateľa${b.event_seq ? ` (seq ${b.event_seq})` : ' (bez cesty späť)'}`);
          else this.log(`bounce ${ev.message_id} nevrátené (${b.reason})`);
          return { action: 'rang', ok: false, holder: String(ev.holder), text, result, bounce: b };
        } catch (e) {
          this.log(`bounce failed ${ev.message_id}: ${e.message}`);
        }
      }
    }
    return { action: 'rang', ok: !!(result && result.ok), holder: String(ev.holder), text, result };
  }

  /** Keep a steer/assist event until its holder's discussion ends (newest HELD_MAX). */
  hold(ev, now = this.now()) {
    const key = ev.message_id || `seq:${ev.seq}`;
    this.state.held[key] = { ...ev, held_at: new Date(now).toISOString() };
    const keys = Object.keys(this.state.held);
    for (const k of keys.slice(0, Math.max(0, keys.length - HELD_MAX))) delete this.state.held[k];
    this.state.save();
  }

  /**
   * Ring the held steer/assist events (of one holder, else all) whose holder no longer discusses with the operator.
   * A message picked up, answered or gone meanwhile is dropped without a ring. Returns the number rung.
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
      if (discussionOpen(turns.find((x) => x.thread === String(ev.holder)) || null)) continue;
      delete this.state.held[key];
      this.state.save();
      if (ev.message_id) {
        try {
          const st = await this.client.message(ev.message_id);
          const answered = st.replied && st.answer_origin !== 'code';
          if (st.state !== 'pending' || st.picked_at || answered || st.outcome === 'undelivered') {
            this.log(`held ${ev.message_id}: ${st.state}${st.picked_at ? ', prevzatá' : ''}${answered ? ', zodpovedaná' : ''}; nezvoní sa`);
            continue;
          }
        } catch (e) {
          this.log(`message ${ev.message_id}: ${e.message}; zvoní sa bez kontroly`);
        }
      }
      this.log(`release ${ev.message_id || key} → ${ev.holder}: diskusia s operátorom skončila`);
      released += 1;
      await this.handle({ ...ev, created_at: new Date(this.now()).toISOString(), released: true });
    }
    return released;
  }

  async notifyLimit(ev, limit) {
    try {
      return await this.client.createNotice({
        kind: 'wake-limit',
        title: `Limit budenia (${limit.reason === 'repeat' ? 'opakovaná správa' : '6 za hodinu'}): ${topicLabel(ev)}`,
        message_id: ev.message_id,
        detail: `${limit.detail}; vlákno ${ev.holder} nebolo zobudené, správa čaká na ďalší ťah.`,
        dedupe_key: `${limit.reason}:${limit.reason === 'repeat' ? directionKey(ev) : pairKey(ev)}`,
      });
    } catch (e) {
      this.log(`notice failed: ${e.message}`);
      return null;
    }
  }

  // --- keepalive ----------------------------------------------------------------------
  /** One keepalive round over the open turns; returns one result per turn. */
  async keepaliveOnce() {
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
    for (const turn of turns) out.push(await this.keepaliveTurn(turn, since));
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
  async continueOnce() {
    if (!this.adapter) return [];
    const { turns } = await this.kaClient.turns(false);
    const out = [];
    for (const turn of turns) {
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
        await this.continueOnce();
      } catch (e) {
        if (signal && signal.aborted) break;
        this.log(`continue: ${e.code === 'UNAVAILABLE' ? 'comms unavailable' : `error: ${e.message}`}`);
      }
      try {
        await this.keepaliveOnce();
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

  /** One long-poll round; returns the number of events processed. */
  async pollOnce(waitMs = this.opts.waitMs) {
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
    return r.events.length;
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
        if (!outage || e.code !== 'UNAVAILABLE') this.log(`${e.code === 'UNAVAILABLE' ? 'comms unavailable' : `error: ${e.message}`}; nový pokus o ${backoff} ms`);
        outage = true;
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
