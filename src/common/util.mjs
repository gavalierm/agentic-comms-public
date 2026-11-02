import fs from 'node:fs';
import path from 'node:path';
import { randomBytes, createHash } from 'node:crypto';

export const LEVELS = ['blocker', 'steer', 'assist'];
/** Level removed (operator 2026-11-02): refused with this text, pointing to the valid levels. */
export const LEVEL_REMOVED_TEXT = 'úroveň „info“ bola zrušená; použi assist (nečakáš, príjemca prečíta pri voľnom bode), steer (príjemca rieši hneď) alebo blocker (čakáš na odpoveď). Platné úrovne: ' + 'blocker|steer|assist';
/** Deadline for PICKUP (the body is read), not for the answer. `ttl` may shorten it, never extend it. Replies have none. */
export const DEFAULT_TTL = { blocker: '5m', steer: '5m', assist: '30m' };
export const MAX_TTL = { blocker: '5m', steer: '5m', assist: '30m' };
const NAME_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function isValidName(s) {
  return typeof s === 'string' && NAME_RE.test(s);
}

/** Mailbox of a persona's main thread: reserved project `@<persona>`, topic `main`. */
export const PERSONA_MAIN_TOPIC = 'main';
export function personaProject(persona) {
  return `@${persona}`;
}

/** Machine key: lower case, only [a-z0-9.-]; the same host name gives the same key on every OS. */
export function normalizeMachine(name) {
  if (name == null || name === '') return null;
  const k = String(name).trim().toLowerCase().replace(/[^a-z0-9.-]+/g, '-').replace(/^-+|-+$/g, '');
  return k || null;
}

export function nowIso() {
  return new Date().toISOString();
}

/**
 * Time-sortable id: 20261101T001234567Z-00a1b2c3. Ids created in the same
 * millisecond by one process stay in creation order (monotonic counter).
 */
let lastIdTs = '';
let idCounter = 0;
export function newId() {
  const ts = nowIso().replace(/[-:.]/g, '');
  if (ts === lastIdTs) idCounter = (idCounter + 1) % 0x10000; else { lastIdTs = ts; idCounter = 0; }
  return `${ts}-${idCounter.toString(16).padStart(4, '0')}${randomBytes(2).toString('hex')}`;
}

export function textHash(text) {
  return createHash('sha1').update(String(text ?? ''), 'utf8').digest('hex').slice(0, 16);
}

export function newSecret() {
  return randomBytes(24).toString('base64url');
}

export function readJson(file, fallback = undefined) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (e) {
    if (e.code === 'ENOENT' && fallback !== undefined) return fallback;
    throw e;
  }
}

/** Atomic write: temp file in the same dir, then rename. */
export function writeJson(file, data, mode = 0o600) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const tmp = `${file}.${process.pid}.${randomBytes(2).toString('hex')}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(data, null, 2) + '\n', { mode });
  fs.renameSync(tmp, file);
}

export function listJsonFiles(dir) {
  try {
    return fs.readdirSync(dir).filter((f) => f.endsWith('.json')).sort();
  } catch (e) {
    if (e.code === 'ENOENT') return [];
    throw e;
  }
}

/** Human readable waiting time for the operator page (Slovak). */
export function waitingText(sinceIso, now = Date.now()) {
  const s = Math.max(0, Math.round((now - Date.parse(sinceIso)) / 1000));
  if (s < 60) return `${s} s`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m} min`;
  const h = Math.round(m / 60);
  if (h < 48) return `${h} h`;
  return `${Math.round(h / 24)} d`;
}

export class HttpError extends Error {
  constructor(status, message, extra = {}) {
    super(message);
    this.status = status;
    this.extra = extra;
  }
}

/**
 * Duration to whole seconds: `90s`, `30m`, `4h`, `1d` or a plain number of seconds.
 * `0` = no limit. Returns null for an invalid value.
 */
export function parseDuration(v) {
  if (typeof v === 'number') return Number.isFinite(v) && v >= 0 ? Math.round(v) : null;
  const m = /^\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*$/i.exec(String(v ?? ''));
  if (!m) return null;
  const mult = { '': 1, s: 1, m: 60, h: 3600, d: 86400 }[m[2].toLowerCase()];
  return Math.round(Number(m[1]) * mult);
}

/**
 * Point in time from a command output: ISO 8601, epoch seconds or milliseconds
 * (also `@<epoch>` of `systemctl --timestamp=unix`), or the systemd format
 * `Sun 2026-11-01 16:32:21 UTC`. Returns epoch ms or null.
 */
export function parseTimestamp(s) {
  const t = String(s ?? '').trim().split('\n')[0].trim().replace(/^@/, '');
  if (!t || t === 'n/a') return null;
  if (/^\d+(\.\d+)?$/.test(t)) { const n = Number(t); return n > 1e12 ? Math.round(n) : Math.round(n * 1000); }
  let ms = Date.parse(t);
  if (Number.isNaN(ms)) ms = Date.parse(t.replace(/^[A-Za-z]{3}\s+/, ''));
  return Number.isNaN(ms) ? null : ms;
}

/** Thread id as harnesses give it (e.g. T3 `mcp:<uuid>`): letters, digits and `. _ - :`, at most 200 chars. */
export function isValidThread(v) {
  return typeof v === 'string' && v.length <= 200 && /^[A-Za-z0-9._:-]+$/.test(v);
}
