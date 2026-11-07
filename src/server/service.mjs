// Business logic of the mail service: send / receive / notices / decide /
// register-topic / wake events / message deadlines (TTL) / turns (keepalive).
// Independent of HTTP.
import { EventEmitter } from 'node:events';
import { LEVELS, LEVEL_REMOVED_TEXT, DEFAULT_TTL, MAX_TTL, isValidName, isValidThread, newId, nowIso, textHash, HttpError, PERSONA_MAIN_TOPIC, personaProject, normalizeMachine, parseDuration } from '../common/util.mjs';
import { notifyOperator } from '../notify/index.mjs';

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
};

/** Notice kinds that a client (the wake module) may create through POST /notice. */
export const EXTERNAL_NOTICE_KINDS = ['wake-limit', 'turn-interrupted', 'turn-stalled'];
/** Notice kinds about a thread, not a message (no `message_id`). */
const THREAD_NOTICE_KINDS = ['turn-interrupted', 'turn-stalled'];
/** Deadlines per level (`ttl` in instance.json overrides, `ttl` of a message shortens): DEFAULT_TTL / MAX_TTL in util.
 * Every message except a reply has a deadline for its pickup; a reply has none. */
export { DEFAULT_TTL, MAX_TTL };
export const TTL_LEVELS = ['blocker', 'steer', 'assist'];
/** Urgency order: a reply is at least as urgent as the message it answers. */
export const LEVEL_RANK = { assist: 1, steer: 2, blocker: 3 };
export const EXPIRY_INTERVAL_MS = 30_000;
export const TURN_STATES = ['open', 'closed'];
/** Longest stored next step of a closed turn (characters). */
export const MAX_NEXT = 300;
/** Latest timer of a closed turn (P-061): a resume time beyond this is clamped. 30 days: a paused topic (snooze) rings at its end. */
export const MAX_RESUME_MS = 30 * 24 * 3600 * 1000;
/** Sender of the service's own replies in 0.2.x (old data only; since 0.3.0 the service sends no mail). */
export const SYSTEM_SENDER = 'comms';
/** Final outcome of a message with no answer (the state for the recipient stays `pending`). */
export const OUTCOMES = ['expired', 'undelivered'];
export const HOLDER_MODES = ['operator', 'auto'];
export const VISIBILITIES = ['public', 'private'];

/** Timer of a closed turn (P-061): ISO time within MAX_RESUME_MS of now, else clamped; invalid → null. */
export function resumeAt(value, now = new Date().toISOString()) {
  if (value == null || value === '') return null;
  const t = Date.parse(value);
  if (!Number.isFinite(t)) throw new HttpError(400, 'resume_at musí byť čas ISO 8601');
  const base = Date.parse(now);
  return new Date(Math.min(Math.max(t, base), base + MAX_RESUME_MS)).toISOString();
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
  }

  /** Deadline check every 30 s; also once at start (deadlines survive a restart, nothing is re-armed). */
  startTimers(intervalMs = EXPIRY_INTERVAL_MS) {
    this.stopTimers();
    const tick = () => { try { this.checkExpired(); } catch (e) { this.log(`expiry check failed: ${e.message}`); } };
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

  health() {
    return { ok: true, instance_id: this.instanceId, instance: this.instance.instance, pid: process.pid, time: nowIso() };
  }

  // --- topics ------------------------------------------------------------------
  /**
   * Register a topic. `main: true` with `persona` registers the persona's main thread
   * (`@<persona>/main`, P-045). `persona` = owner of the topic; `visibility: private` hides
   * the topic from senders other than the owner (P-041).
   *
   * Holder: a registration that would change a non-null holder (including clearing it with
   * an absent holder) is refused with 409 unless `takeover: true`; the same holder is
   * idempotent. A release is `release: true`: it clears the holder when no holder is given
   * or it is the current one (else 409 unless `takeover`). Fields that are not passed keep
   * their stored values; holder_mode / holder_machine belong to the holder, so they are kept
   * only while the holder stays the same.
   *
   * Close is `close: true` (the topic was finished and archived): like a release (own holder or
   * takeover), plus `state: 'closed'` and `closed_at`. A closed topic takes no new mail (send and
   * reply answer 410 at once, alice 2026-11-02: an assist to an archived topic waited 4 h for nobody);
   * messages still pending in its mailbox bounce to their senders (`undelivered`, reason topic closed).
   * Any later registration without `close` reopens the topic (a new topic with the same id).
   *
   * `sender` (authenticated key) must be the persona when the instance has a key for that
   * persona; without such a key the persona is not managed by keys (admin mode) and anyone may register it.
   */
  registerTopic({ project, topic, subject, holder = null, holder_mode = null, holder_machine = null, persona = null, visibility = null, main = false, takeover = false, release = false, close = false } = {}, sender = null) {
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
    const prior = this.store.topic(project, topic);
    if (close && !prior) throw new HttpError(404, `téma ${project}/${topic} nie je registrovaná; nie je čo uzavrieť`);
    if (close) release = true;
    const prevHolder = prior && prior.holder ? prior.holder : null;
    const newHolder = release ? null : (holder || null);
    const sameHolder = newHolder != null && newHolder === prevHolder;

    // Holder change (or clearing) needs takeover; a release may clear its own holder.
    if (prevHolder && newHolder !== prevHolder) {
      const ownRelease = release && (!holder || holder === prevHolder);
      if (!ownRelease && !takeover) throw new HttpError(409, `téma ${project}/${topic} už má držiteľa ${prevHolder}; prepísanie alebo uvoľnenie cudzieho držiteľa vyžaduje takeover: true`);
    }

    // Persona and visibility not passed keep the stored values (main has no visibility).
    const effPersona = persona != null ? persona : (prior && prior.persona) || null;
    const effVisibility = main ? null : (visibility != null ? visibility : (prior && prior.visibility) || 'public');
    if (effVisibility === 'private' && !effPersona) throw new HttpError(400, 'súkromná téma musí mať personu (vlastníka)');
    if (effPersona) this.requirePersonaSender(effPersona, sender);

    const subjectGiven = typeof subject === 'string' && subject.trim() !== '';
    const subjectOut = subjectGiven ? subject.trim() : (prior && prior.subject) || (main ? `main thread of ${persona}` : null);
    if (!subjectOut) throw new HttpError(400, 'téma musí mať predmet (subject)');

    const keepHolderFields = sameHolder;
    const rec = {
      project, topic, subject: subjectOut,
      holder: newHolder,
      holder_mode: newHolder ? (holder_mode != null ? holder_mode : (keepHolderFields ? prior.holder_mode : null)) : null,
      holder_machine: newHolder ? (holder_machine != null ? normalizeMachine(holder_machine) : (keepHolderFields ? prior.holder_machine : null)) : null,
      persona: effPersona, visibility: effVisibility,
      registered_at: prior ? prior.registered_at : nowIso(),
      updated_at: nowIso(),
      state: close ? 'closed' : 'open',
      closed_at: close ? (prior && prior.state === 'closed' && prior.closed_at ? prior.closed_at : nowIso()) : null,
    };
    this.store.saveTopic(rec);
    if (main) this.closePersonaNotices(persona);
    const bounced = close ? this.bounceClosed(rec, prevHolder) : [];
    return { registered: true, created: !prior, topic: rec, ...(close ? { bounced } : {}) };
  }

  /**
   * Pending messages of a closed topic bounce to their senders: its mailbox, and messages to its last holder's thread
   * without a topic of their own, but only when that thread holds no other open topic (it still reads its mail).
   */
  bounceClosed(rec, prevHolder) {
    const out = [];
    const threadGone = prevHolder && !this.store.topics().some((t) => t.holder === prevHolder && t.state !== 'closed'
      && !(t.project === rec.project && t.topic === rec.topic));
    const list = this.store.messages((m) => m.state === 'pending' &&
      ((m.project === rec.project && m.topic === rec.topic) || (threadGone && m.to_thread === prevHolder && !m.topic)));
    for (const m of list) {
      const r = this.bounce({ message_id: m.id, holder: prevHolder, reason: `topic ${rec.project}/${rec.topic} closed ${rec.closed_at}` });
      if (r.bounced) out.push(m.id);
    }
    return out;
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
  send(sender, body) {
    if (!sender) throw new HttpError(401, 'neplatný kľúč');
    const { project = null, topic = null, persona = null, level: rawLevel = null, text, reply_to = null, thread_id = null, thread_machine = null, from_project = null, from_topic = null, ttl = null } = body || {};
    if (rawLevel === 'info') throw new HttpError(400, LEVEL_REMOVED_TEXT);
    // Level is required, except for a reply: it inherits the level of the message it answers (see below).
    if (rawLevel == null || rawLevel === '') {
      if (!reply_to) throw new HttpError(400, `úroveň (level) je povinná: ${LEVELS.join('|')}`);
    } else if (!LEVELS.includes(rawLevel)) {
      throw new HttpError(400, `neznáma úroveň „${rawLevel}“; platné úrovne: ${LEVELS.join('|')}`);
    }
    const level = rawLevel || null;
    if (typeof text !== 'string' || !text.trim()) throw new HttpError(400, 'text správy je povinný');
    // A reply has no deadline: it answers, nothing waits for it.
    const ttl_s = reply_to ? 0 : this.resolveTtl(level, ttl);
    const created_at = nowIso();

    const msg = {
      id: newId(), project: null, topic: null, persona: null, ref_project: null, ref_topic: null, to_thread: null,
      level, text, sender, thread_id, thread_machine: normalizeMachine(thread_machine), from_project, from_topic, reply_to,
      created_at, state: 'pending', acked_at: null, notice_id: null,
      ttl_s: ttl_s || null, expires_at: ttl_s ? new Date(Date.parse(created_at) + ttl_s * 1000).toISOString() : null, expired_at: null, outcome: null,
    };

    let original = null;
    if (reply_to) {
      // A reply goes to the original's sender: an address in the body would be ignored, so refuse it.
      if (present(persona) || present(project) || present(topic)) throw new HttpError(400, 'odpoveď (reply_to) nemôže mať persona, project ani topic: adresát je odosielateľ pôvodnej správy');
      original = this.store.message(reply_to);
      if (!original) throw new HttpError(404, `pôvodná správa ${reply_to} neexistuje`);
      // A reply to your own message would come straight back to you (bob 2026-11-01): refuse it.
      const ownByThread = thread_id && original.thread_id === thread_id;
      const ownBySource = !thread_id && original.sender === sender && present(from_project) && present(from_topic)
        && original.from_project === from_project && original.from_topic === from_topic;
      if (ownByThread || ownBySource) throw new HttpError(400, `správa ${reply_to} je vaša vlastná: odpoveď by sa vrátila do toho istého vlákna; pošlite novú správu adresátovi a ID uveďte v texte`);
      // A reply never lowers the urgency of what it answers (operator 2026-11-02: a blocker waits for an answer
      // as urgent as itself): a lower level is raised to the original's.
      const originalLevel = levelOf(original);
      if (!msg.level) msg.level = originalLevel;
      else if (LEVEL_RANK[msg.level] < LEVEL_RANK[originalLevel]) { msg.level_raised_from = msg.level; msg.level = originalLevel; }
      // Reply goes back to the sender's thread (and its topic, if it had one).
      msg.to_thread = original.thread_id;
      msg.project = original.from_project;
      msg.topic = original.from_topic;
      if (!msg.to_thread && !(msg.project && msg.topic)) {
        throw new HttpError(400, 'na pôvodnú správu sa nedá odpovedať: nemá vlákno ani zdrojovú tému');
      }
      if (msg.project && msg.topic) this.refuseClosed(this.store.topic(msg.project, msg.topic), 'odosielateľ pôvodnej správy už tému uzavrel');
    } else if (persona != null) {
      // Persona address (P-045): persona [+ project [+ topic]]. Delivered to the topic only when the
      // persona owns it (and, if private, the sender is the owner); everything else goes to the
      // persona's main thread, which triages. The answer to the sender is the same in every case.
      requireName(persona, 'persona');
      if (topic != null && project == null) throw new HttpError(400, 'téma (topic) vyžaduje projekt (project)');
      if (project != null) requireName(project, 'project');
      if (topic != null) requireName(topic, 'topic');
      const t = project && topic ? this.store.topic(project, topic) : null;
      if (t && t.state !== 'closed' && t.persona === persona && (t.visibility !== 'private' || sender === persona)) {
        msg.project = project;
        msg.topic = topic;
        msg.persona = persona;
      } else {
        return this.toPersonaMain(msg, persona, { project, topic });
      }
    } else {
      requireName(project, 'project');
      requireName(topic, 'topic');
      msg.project = project;
      msg.topic = topic;
      const t = this.store.topic(project, topic);
      this.refuseClosed(t, 'téma je uzavretá');
      if (!t || (t.visibility === 'private' && sender !== t.persona)) {
        // Unregistered topic, or a private topic addressed by someone else than its owner: the same
        // answer as for an unknown topic (no oracle), hold + notice, never create the topic (P-045).
        if (t) msg.private_owner = t.persona;
        msg.state = 'held';
        const notice = this.createNotice({
          kind: 'unregistered-topic',
          title: t ? `Súkromná téma persony ${t.persona}: ${project}/${topic} (doručiť = do hlavného vlákna persony)` : `Nová téma? / presmerovať: ${project}/${topic}`,
          message: msg,
        });
        msg.notice_id = notice.id;
        this.store.saveMessage(msg);
        this.log(`held ${msg.id} for ${t ? 'private' : 'unregistered'} ${project}/${topic}`);
        return { id: msg.id, state: msg.state, notice_id: notice.id, expires_at: msg.expires_at };
      }
    }

    this.store.saveMessage(msg);
    this.afterDelivery(msg, original);
    return { id: msg.id, state: msg.state, level: msg.level, ...(msg.level_raised_from ? { level_raised_from: msg.level_raised_from } : {}), notice_id: msg.notice_id, expires_at: msg.expires_at };
  }

  /** A closed topic takes no mail: 410 with the time it was closed and where to write instead. */
  refuseClosed(t, why) {
    if (!t || t.state !== 'closed') return;
    const owner = t.persona || t.project;
    throw new HttpError(410, `${why}: ${t.project}/${t.topic} uzavretá ${t.closed_at || '?'}; nikto ju nedrží a správu by nikto neprečítal. Pošli ju do hlavného vlákna @${owner}/main alebo do inej otvorenej témy (ID pôvodnej správy uveď v texte)`, { closed_at: t.closed_at || null, project: t.project, topic: t.topic });
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
   * Deliver into the persona's main mailbox. Always accepted and pending, also when the persona
   * (its main thread) does not exist yet: it picks the mail up after (re)birth. A persona that
   * never registered gets one open operator notice (typo guard), the message keeps waiting.
   */
  toPersonaMain(msg, persona, ref = {}) {
    msg.project = personaProject(persona);
    msg.topic = PERSONA_MAIN_TOPIC;
    msg.persona = persona;
    msg.ref_project = ref.project || null;
    msg.ref_topic = ref.topic || null;
    msg.state = 'pending';
    this.store.saveMessage(msg);
    let warning = null;
    if (!this.store.topic(msg.project, msg.topic)) {
      warning = `persona ${persona} has no main thread yet; the message waits`;
      const open = this.store.notices((n) => n.state === 'open' && n.kind === 'unregistered-persona' && n.persona === persona);
      if (!open.length) {
        this.createNotice({ kind: 'unregistered-persona', title: `Neznáma persona? ${persona} (správa čaká)`, message: msg, extra: { persona } });
      }
    }
    this.afterDelivery(msg);
    return { id: msg.id, state: msg.state, notice_id: msg.notice_id, expires_at: msg.expires_at || null, ...(warning ? { warning } : {}) };
  }

  /** Wake events and operator notices once a message is pending in a mailbox. */
  afterDelivery(msg, original = null) {
    // Events never carry the body (P-043); `text_hash` lets the wake module detect repeats.
    const base = {
      message_id: msg.id, level: msg.level, project: msg.project, topic: msg.topic,
      sender: msg.sender, thread_id: msg.thread_id, from_project: msg.from_project, from_topic: msg.from_topic,
      text_hash: textHash(msg.text),
    };
    if (original) {
      const t = msg.project && msg.topic ? this.store.topic(msg.project, msg.topic) : null;
      this.emitEvent({
        type: 'reply', ...base, reply_to: original.id, original_level: levelOf(original),
        holder: original.thread_id || (t ? t.holder : null), holder_mode: original.thread_id ? null : (t ? t.holder_mode || null : null),
        holder_machine: original.thread_id ? original.thread_machine || null : (t ? t.holder_machine || null : null),
      });
      return;
    }
    // A steer rings the holder of its topic at once (relevance by address); it has no operator notice.
    if (msg.level === 'steer') {
      const t = this.store.topic(msg.project, msg.topic);
      this.emitEvent({ type: 'steer', ...base, holder: t ? t.holder : null, holder_mode: t ? t.holder_mode || null : null, holder_machine: t ? t.holder_machine || null : null });
    }
    // An assist rings the holder too (bob 2026-11-02: an assist to an idle main thread waited 4 h unseen);
    // a ring that fails for good bounces it to the sender. No operator notice for assist.
    if (msg.level === 'assist') {
      const t = this.store.topic(msg.project, msg.topic);
      this.emitEvent({ type: 'assist', ...base, holder: t ? t.holder : null, holder_mode: t ? t.holder_mode || null : null, holder_machine: t ? t.holder_machine || null : null });
    }
    if (msg.level === 'blocker') {
      const t = this.store.topic(msg.project, msg.topic);
      this.emitEvent({ type: 'blocker', ...base, holder: t ? t.holder : null, holder_mode: t ? t.holder_mode || null : null, holder_machine: t ? t.holder_machine || null : null });
      const notice = this.createNotice({ kind: 'blocker', title: `Blocker pre tému ${msg.project}/${msg.topic}`, message: msg });
      msg.notice_id = notice.id;
      this.store.saveMessage(msg);
    }
  }

  // --- receive -------------------------------------------------------------------
  /** Pending messages of a mailbox (or of a thread), optionally only the given ids. No state change. */
  pending({ project, topic, thread, persona = null }, ids = null) {
    if (persona) {
      requireName(persona, 'persona');
      project = personaProject(persona);
      topic = PERSONA_MAIN_TOPIC;
    }
    if (!(project && topic) && !thread) throw new HttpError(400, 'zadaj project a topic, persona alebo thread');
    const list = this.store.messages((m) =>
      m.state === 'pending' && (
        (project && topic && m.project === project && m.topic === topic) ||
        (thread && m.to_thread === thread)
      ));
    const want = Array.isArray(ids) && ids.length ? new Set(ids.map(String)) : null;
    const picked = want ? list.filter((m) => want.has(m.id)) : list;
    picked.sort((a, b) => a.created_at.localeCompare(b.created_at) || a.id.localeCompare(b.id));
    return picked;
  }

  /**
   * Read the bodies of pending messages. Reading the body is the PICKUP (`picked_at`, first time only): the TTL
   * measures pickup, so a message read in time gets no `no reply` bell. `ids` limits the read to those messages.
   */
  receive({ project, topic, thread, persona = null, ids = null }) {
    const list = this.pending({ project, topic, thread, persona }, ids);
    const now = nowIso();
    for (const m of list) {
      if (m.picked_at) continue;
      m.picked_at = now;
      this.store.saveMessage(m);
    }
    return list.map(publicMessage);
  }

  /** Count without reading: no pickup. `items` carry what the workflow needs to route each message (no bodies). */
  count(q) {
    const list = this.pending(q);
    return {
      total: list.length, blocker: list.filter((m) => levelOf(m) === 'blocker').length, ids: list.map((m) => m.id),
      items: list.map((m) => ({ id: m.id, level: levelOf(m), reply_to: m.reply_to || null, to_thread: m.to_thread || null, sender: m.sender || null, created_at: m.created_at })),
    };
  }

  /**
   * Acknowledge pending messages. Unknown ids are an error (404, nothing is acked); ids that
   * were already acked are listed in `already`, not an error.
   */
  ack(ids) {
    if (!Array.isArray(ids) || !ids.length) throw new HttpError(400, 'zadaj aspoň jedno id');
    const found = ids.map((id) => [id, this.store.message(String(id))]);
    const missing = found.filter(([, m]) => !m).map(([id]) => id);
    if (missing.length) throw new HttpError(404, `správy neexistujú: ${missing.join(', ')}`, { missing });
    const acked = [];
    const already = [];
    for (const [id, m] of found) {
      if (m.state === 'acked') { already.push(id); continue; }
      if (m.state !== 'pending') continue;
      m.state = 'acked';
      m.acked_at = nowIso();
      this.store.saveMessage(m);
      acked.push(id);
    }
    return { acked, already };
  }

  /** State of one message without its body (wake checks it before ringing an `expired` event). */
  messageState(id) {
    const m = this.store.message(id || '');
    if (!m) throw new HttpError(404, `správa ${id} neexistuje`);
    const replied = this.store.messages((x) => x.reply_to === m.id).length > 0;
    return {
      id: m.id, state: m.state, level: levelOf(m), reply_to: m.reply_to || null, project: m.project || null, topic: m.topic || null, to: addressOf(m),
      outcome: outcomeOf(m), replied, expires_at: m.expires_at || null, ttl_s: m.ttl_s || null, picked_at: m.picked_at || null,
      undelivered_holder: m.undelivered_holder || null, undelivered_reason: m.undelivered_reason || null,
    };
  }

  /**
   * Who should hear about a message of its sender: the sender's thread (`thread_id`,
   * machine `thread_machine`), else the holder of its source topic. null = no way back.
   */
  senderHolder(m) {
    if (m.thread_id) return { holder: m.thread_id, holder_machine: m.thread_machine || null, holder_mode: null };
    if (m.from_project && m.from_topic) {
      const t = this.store.topic(m.from_project, m.from_topic);
      if (t && t.holder) return { holder: t.holder, holder_machine: t.holder_machine || null, holder_mode: t.holder_mode || null };
    }
    return null;
  }

  /** Event for the sender about its own message (`expired` / `undelivered`); none without a way back. */
  emitOutcome(type, m) {
    const h = this.senderHolder(m);
    if (!h) return null;
    return this.emitEvent({
      type, message_id: m.id, level: m.level, project: m.project, topic: m.topic, ...h,
      sender: m.sender, thread_id: m.thread_id, from_project: m.from_project, from_topic: m.from_topic,
    });
  }

  // --- deadlines (TTL) ------------------------------------------------------------
  /**
   * Expire pending or held messages past `expires_at` that have no reply and no outcome yet (a held message
   * waits for the operator, but its sender still learns that nobody answered in time).
   * The message gets `outcome: 'expired'` and keeps its state (pending in the mailbox, held with a decidable notice);
   * the sender is told by one `expired` event (its thread, else the holder of its source
   * topic). No mail and no operator notice are created; without a way back only a log line.
   * Returns the ids.
   */
  checkExpired(now = Date.now()) {
    const all = this.store.messages();
    const replied = new Set(all.filter((m) => m.reply_to).map((m) => m.reply_to));
    const out = [];
    for (const m of all) {
      if ((m.state !== 'pending' && m.state !== 'held') || !m.expires_at || m.expired_at || outcomeOf(m) || replied.has(m.id)) continue;
      if (Date.parse(m.expires_at) > now) continue;
      // Picked up within the deadline: it was read in time, so no `no reply` bell.
      if (m.picked_at && Date.parse(m.picked_at) <= Date.parse(m.expires_at)) continue;
      m.expired_at = new Date(now).toISOString();
      m.outcome = 'expired';
      this.store.saveMessage(m);
      const ev = this.emitOutcome('expired', m);
      this.log(`expired ${m.id} (${m.level}) ${m.project}/${m.topic}${ev ? `, event ${ev.seq} → ${ev.holder}` : ', no way back (log only)'}`);
      out.push(m.id);
    }
    return out;
  }

  // --- bounce ---------------------------------------------------------------------
  /**
   * The wake module could not ring the recipient for good (thread archived or gone).
   * Once per message: `outcome: 'undelivered'` and one `undelivered` event for the sender
   * (no mail, no operator notice). The message stays pending in the mailbox. Messages
   * from `comms` (old data) are never bounced.
   */
  bounce({ message_id, holder = null, reason = null } = {}) {
    const m = this.store.message(message_id || '');
    if (!m) throw new HttpError(404, `správa ${message_id} neexistuje`);
    if (m.sender === SYSTEM_SENDER) return { bounced: false, reason: 'system' };
    const prior = outcomeOf(m);
    if (prior === 'undelivered') return { bounced: false, reason: 'already' };
    if (prior) return { bounced: false, reason: prior };
    if (m.state !== 'pending') return { bounced: false, reason: 'not-pending' };
    m.outcome = 'undelivered';
    m.undelivered_at = nowIso();
    m.undelivered_holder = holder ? String(holder) : null;
    m.undelivered_reason = String(reason || 'cannot receive messages').replace(/\s+/g, ' ').trim().slice(0, 200);
    this.store.saveMessage(m);
    const ev = this.emitOutcome('undelivered', m);
    this.log(`undelivered ${m.id} (holder ${holder || '?'})${ev ? `, event ${ev.seq} → ${ev.holder}` : ', no way back (log only)'}`);
    return { bounced: true, event_seq: ev ? ev.seq : null };
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
  turn({ thread, state = null, machine = null, project = null, topic = null, holder_mode = null, reason = null, attempt = false, opened_at = null, next = null, resume_at = null } = {}) {
    requireThread(thread);
    const prev = this.store.turn(thread);
    const now = nowIso();
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
    let rec;
    if (state === 'open') {
      rec = {
        thread, machine: normalizeMachine(machine), project: project || null, topic: topic || null, holder_mode: holder_mode || null,
        state: 'open', opened_at: now, closed_at: null, close_reason: null, keepalive: { count: 0, last_at: null },
      };
    } else {
      if (opened_at != null && opened_at !== '' && (!prev || prev.opened_at !== opened_at)) {
        this.log(`stale close of ${thread} (opened_at ${opened_at}, current ${prev ? prev.opened_at : 'none'})`);
        return { ok: true, stale: true };
      }
      rec = { ...(prev || { thread, machine: normalizeMachine(machine), project, topic, holder_mode, opened_at: null, keepalive: { count: 0, last_at: null } }), state: 'closed', closed_at: now, close_reason: reason || null, next: next ? String(next).slice(0, MAX_NEXT) : null, resume_at: resumeAt(resume_at, now) };
    }
    this.store.saveTurn(rec);
    return { ok: true, turn: rec };
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

  /**
   * Notice created by a client (wake module over limit, P-043). Kind is
   * restricted; an open notice with the same `dedupe_key` is reused.
   */
  createExternalNotice(sender, { kind, title, message_id, detail = null, dedupe_key = null, thread = null, project = null, topic = null } = {}) {
    if (!EXTERNAL_NOTICE_KINDS.includes(kind)) throw new HttpError(400, `oznam typu „${kind}“ nemôže vytvoriť klient`);
    if (typeof title !== 'string' || !title.trim()) throw new HttpError(400, 'oznam musí mať názov (title)');
    let message;
    if (THREAD_NOTICE_KINDS.includes(kind)) {
      // About a thread, not a message: one open notice per thread.
      requireThread(thread);
      message = { id: null, project: project || null, topic: topic || null, level: null, sender, thread_id: thread };
      dedupe_key = `thread:${thread}`;
    } else {
      message = this.store.message(message_id || '');
      if (!message) throw new HttpError(404, `správa ${message_id} neexistuje`);
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
      const m = this.store.message(n.message_id);
      return { ...n, text: m ? m.text : null, waiting_since: m ? m.created_at : n.created_at };
    });
  }

  /** Operator decision, executed directly by the service; no model involved (P-046). */
  decide({ notice_id, decision, target = null }) {
    const n = this.store.notice(notice_id || '');
    if (!n) throw new HttpError(404, `oznam ${notice_id} neexistuje`);
    if (n.state !== 'open') throw new HttpError(409, 'oznam je už rozhodnutý');
    // Kinds removed later (e.g. `no-reply` of 0.2.x) stay decidable with the options stored on the notice.
    const opt = (NOTICE_OPTIONS[n.kind] || n.options || []).find((o) => o.id === decision);
    if (!opt) throw new HttpError(400, `neznáme rozhodnutie „${decision}“ pre oznam typu ${n.kind}`);
    const msg = this.store.message(n.message_id);
    let result = {};

    if (n.kind === 'unregistered-topic') {
      if (!msg || msg.state !== 'held') throw new HttpError(409, 'správa už nie je pozdržaná');
      if (decision === 'discard') {
        msg.state = 'discarded';
      } else {
        let dest = { project: msg.project, topic: msg.topic };
        if (decision === 'deliver' && msg.private_owner) {
          // Private topic: deliver to the owner's main thread, which handles it itself.
          const owner = msg.private_owner;
          msg.state = 'pending';
          msg.notice_id = null;
          delete msg.private_owner;
          this.toPersonaMain(msg, owner, { project: msg.project, topic: msg.topic });
          n.state = 'decided'; n.decision = decision; n.target = target; n.decided_at = nowIso();
          this.store.saveNotice(n);
          return { notice_id: n.id, decision, delivered_to: { project: personaProject(owner), topic: PERSONA_MAIN_TOPIC } };
        }
        if (decision === 'redirect') {
          dest = parseTarget(target);
          msg.redirected_from = { project: msg.project, topic: msg.topic };
          delete msg.private_owner;
          msg.project = dest.project;
          msg.topic = dest.topic;
        }
        if (!this.store.topic(dest.project, dest.topic)) {
          throw new HttpError(409, `téma ${dest.project}/${dest.topic} nie je zaregistrovaná; pošta tému nezakladá`);
        }
        msg.state = 'pending';
        msg.notice_id = null;
        this.store.saveMessage(msg);
        this.afterDelivery(msg);
        result = { delivered_to: dest };
      }
      this.store.saveMessage(msg);
    } else if (n.kind === 'blocker' || n.kind === 'unregistered-persona') {
      if (decision === 'discard' && msg && msg.state === 'pending') {
        msg.state = 'discarded';
        this.store.saveMessage(msg);
      }
    }

    n.state = 'decided';
    n.decision = decision;
    n.target = target;
    n.decided_at = nowIso();
    this.store.saveNotice(n);
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

/** Outcome of a message; old data (0.2.x) knew only `expired_at` / `bounced_at`. */
/** Level of a stored message. `info` (removed 2026-11-02) is read as `assist`, without deadline (no ttl was stored for it). */
export function levelOf(m) {
  return m.level === 'info' ? 'assist' : m.level;
}

function outcomeOf(m) {
  if (m.outcome) return m.outcome;
  if (m.bounced_at) return 'undelivered';
  if (m.expired_at) return 'expired';
  return null;
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

function parseTarget(target) {
  const [project, topic] = String(target || '').split('/');
  if (!isValidName(project) || !isValidName(topic)) throw new HttpError(400, 'cieľ (target) musí byť v tvare project/topic');
  return { project, topic };
}

export function publicMessage(m) {
  return {
    id: m.id, project: m.project, topic: m.topic, persona: m.persona || null, ref_project: m.ref_project || null, ref_topic: m.ref_topic || null,
    to_thread: m.to_thread, level: levelOf(m), text: m.text, picked_at: m.picked_at || null,
    sender: m.sender, thread_id: m.thread_id, from_project: m.from_project, from_topic: m.from_topic,
    reply_to: m.reply_to, created_at: m.created_at, state: m.state,
  };
}
