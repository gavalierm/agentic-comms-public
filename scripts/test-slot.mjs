#!/usr/bin/env node
// Slot of a full test run (agent-alice/D-0069; overload 2026-11-15: ten topics ran their whole test suites at once, load 41
// on 4 cores). `npm test` of agentic-comms runs its suite through this script: it waits until a slot is free, at most
// AGENTIC_TEST_SLOTS (default 1) full runs on this machine, of any repo that runs its tests this way (agentic-flow keeps
// the same script and the same slot dir, so the slots are shared by both repos).
// A slot is the file slot-<n>.json in the shared state dir ~/.local/state/agentic/test-slots (AGENTIC_TEST_SLOTS_DIR),
// taken with an exclusive create under a short directory lock; its run refreshes it every few seconds, so the slot of a
// killed run (dead pid or no refresh for a minute) is taken over. Node, not flock: one script for Linux, macOS and
// Windows. A single `node --test <file>` does not come here and is not limited; a run inside a held slot
// (AGENTIC_TEST_SLOT_HELD, e.g. a test that runs npm test of a fixture) does not wait for a second one.
//   node scripts/test-slot.mjs [--] <command> [args...]    exit code of the command; `node` is this node binary
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

// One full run at a time (P-079, operator 2026-11-17: several threads running full suites at once overloaded the machine,
// 4 cores, load 8; one run of node --test already uses all cores). The run gets a lower CPU priority (AGENTIC_TEST_NICE=0
// keeps the normal one), so the threads' live work stays responsive.
export const DEFAULT_SLOTS = 1;
/** Test files run at once inside one run: AGENTIC_TEST_CONCURRENCY, else half the cores (at least 1). */
export function testConcurrency(env = process.env, cores = (os.availableParallelism?.() || os.cpus().length)) {
  const n = Number(env.AGENTIC_TEST_CONCURRENCY);
  return Number.isInteger(n) && n > 0 ? n : Math.max(1, Math.floor(cores / 2));
}
const HEARTBEAT_MS = 5000;
const STALE_MS = 60000;
const LOCK_STALE_MS = 10000;

export function slotsDir(env = process.env) {
  return env.AGENTIC_TEST_SLOTS_DIR || path.join(os.homedir(), '.local', 'state', 'agentic', 'test-slots');
}

export function slotCount(env = process.env) {
  const n = Number(env.AGENTIC_TEST_SLOTS);
  return Number.isInteger(n) && n >= 1 ? n : DEFAULT_SLOTS;
}

function alive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (err) { return err.code === 'EPERM'; }
}

/** The record of a slot file, or null when the slot is free or its run is gone (dead pid, no refresh). */
function holderOf(file, now = Date.now()) {
  let rec;
  let mtime;
  try { rec = JSON.parse(fs.readFileSync(file, 'utf8')); mtime = fs.statSync(file).mtimeMs; } catch { return null; }
  if (now - mtime > STALE_MS) return null;
  if (rec?.host === os.hostname() && !alive(rec.pid)) return null;
  return rec;
}

/** Short lock of the slot dir (mkdir is atomic): only its holder takes or clears a slot. */
function withDirLock(dir, fn) {
  const lock = path.join(dir, '.lock');
  try { fs.mkdirSync(lock); } catch (err) {
    if (err.code !== 'EEXIST') throw err;
    try { if (Date.now() - fs.statSync(lock).mtimeMs > LOCK_STALE_MS) fs.rmSync(lock, { recursive: true, force: true }); } catch { /* gone meanwhile */ }
    return undefined;
  }
  try { return fn(); } finally { fs.rmSync(lock, { recursive: true, force: true }); }
}

/**
 * Take a free slot: { file } (this process holds it), { busy: [records] } (all slots held), or undefined (the dir lock
 * is held by another process for a moment: try again).
 */
export function tryTakeSlot({ dir = slotsDir(), slots = slotCount(), record = {} } = {}) {
  fs.mkdirSync(dir, { recursive: true });
  return withDirLock(dir, () => {
    const busy = [];
    for (let i = 1; i <= slots; i++) {
      const file = path.join(dir, `slot-${i}.json`);
      const rec = holderOf(file);
      if (rec) { busy.push(rec); continue; }
      fs.rmSync(file, { force: true });
      try {
        fs.writeFileSync(file, `${JSON.stringify({ pid: process.pid, host: os.hostname(), since: new Date().toISOString(), ...record })}\n`, { flag: 'wx' });
      } catch (err) { if (err.code === 'EEXIST') continue; throw err; }
      return { file };
    }
    return { busy };
  });
}

/** Free the slot when this process still holds it (a stalled run may have lost it to another). */
export function releaseSlot(file) {
  try {
    const rec = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (rec.pid === process.pid && rec.host === os.hostname()) fs.rmSync(file, { force: true });
  } catch { /* already gone */ }
}

const sleep = (ms) => new Promise((r) => { setTimeout(r, ms); });
const describe = (busy) => busy.map((r) => `pid ${r.pid}${r.cwd ? ` in ${path.basename(r.cwd)}` : ''}`).join(', ');

async function main(argv) {
  const cmd = argv[0] === '--' ? argv.slice(1) : argv;
  if (!cmd.length) {
    process.stderr.write('usage: node scripts/test-slot.mjs [--] <command> [args...]\n');
    return 2;
  }
  const env = { ...process.env };
  let slot = null;
  if (!env.AGENTIC_TEST_SLOT_HELD) {
    const dir = slotsDir(env);
    const slots = slotCount(env);
    const poll = Number(env.AGENTIC_TEST_SLOT_POLL_MS) > 0 ? Number(env.AGENTIC_TEST_SLOT_POLL_MS) : 2000;
    const started = Date.now();
    let told = false;
    for (;;) {
      const r = tryTakeSlot({ dir, slots, record: { cwd: process.cwd(), command: cmd.join(' ').slice(0, 200) } });
      if (r?.file) { slot = r.file; break; }
      if (r?.busy && !told) {
        process.stderr.write(`waiting for a test slot (${r.busy.length} of ${slots} full test runs on this machine: ${describe(r.busy)}; dir ${dir})\n`);
        told = true;
      }
      await sleep(r ? poll : 50);
    }
    if (told) process.stderr.write(`test slot taken after ${Math.round((Date.now() - started) / 1000)} s\n`);
    env.AGENTIC_TEST_SLOT_HELD = slot;
  }
  const beat = slot ? setInterval(() => { try { const t = new Date(); fs.utimesSync(slot, t, t); } catch { /* lost */ } }, HEARTBEAT_MS) : null;
  beat?.unref();
  if (slot) process.on('exit', () => releaseSlot(slot));
  const [bin, ...rest] = cmd;
  // At most half the cores for test files at once (operator 2026-11-17: a release took the whole CPU of 4 cores; node
  // --test runs cores-1 files, each starting more processes). AGENTIC_TEST_CONCURRENCY sets it; an explicit flag wins.
  if (bin === 'node' && rest.includes('--test') && !rest.some((a) => a.startsWith('--test-concurrency'))) {
    rest.splice(rest.indexOf('--test') + 1, 0, `--test-concurrency=${testConcurrency(env)}`);
  }
  const child = spawn(bin === 'node' ? process.execPath : bin, rest, { stdio: 'inherit', env, shell: process.platform === 'win32' && bin !== 'node' });
  // Below normal priority for the run and what it starts (inherited on Linux and macOS; Windows: the run's own process).
  if (env.AGENTIC_TEST_NICE !== '0' && child.pid) { try { os.setPriority(child.pid, os.constants.priority.PRIORITY_BELOW_NORMAL); } catch { /* not allowed: normal priority */ } }
  const forward = (sig) => { try { child.kill(sig); } catch { /* gone */ } };
  for (const sig of ['SIGINT', 'SIGTERM', 'SIGHUP']) process.on(sig, () => forward(sig));
  return new Promise((resolve) => {
    child.on('error', (err) => { process.stderr.write(`test-slot: ${bin}: ${err.message}\n`); resolve(127); });
    child.on('exit', (code, signal) => resolve(code ?? (signal ? 128 + (os.constants.signals[signal] || 0) : 1)));
  }).finally(() => { if (beat) clearInterval(beat); if (slot) releaseSlot(slot); });
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).then((code) => { process.exitCode = code; });
}
