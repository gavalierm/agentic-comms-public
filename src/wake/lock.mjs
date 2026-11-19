// One `wake run` per instance (issue comms-public#1): Windows Task Scheduler stops only the top process of a task, so a
// restart of the task left the old `node wake.mjs run` running next to the new one (double bells, races on
// wake-state.json). The lock file <state dir>/wake.lock holds { pid, started_at, heartbeat_at }; the holder refreshes the
// heartbeat, a second process waits until the lock is released or goes stale (dead pid or no heartbeat), then takes it.
// The heartbeat also tells `wake status` whether the wake is running.
import fs from 'node:fs';
import path from 'node:path';
import { readLock, lockLive, createExclusive } from '../common/lock.mjs';

export const LOCK_HEARTBEAT_MS = 30_000;
export const LOCK_STALE_MS = 3 * LOCK_HEARTBEAT_MS;
const LOCK_POLL_MS = 10_000;

export const lockFile = (stateDir) => path.join(stateDir, 'wake.lock');

export const readWakeLock = (stateDir) => readLock(lockFile(stateDir));

/** The lock is held by another live process with a fresh heartbeat (the own pid does not hold it: one `wake run` per process). */
export function lockHeld(lock, { now = Date.now(), pid = process.pid } = {}) {
  return !!lock && lock.pid !== pid && lockLive(lock, LOCK_STALE_MS, now);
}

/**
 * Waits for the lock, then holds it until release(). Resolves null when the signal aborts first.
 * @returns {Promise<{ release(): void } | null>}
 */
export async function acquireWakeLock(stateDir, { log = () => {}, signal = null, pollMs = LOCK_POLL_MS, heartbeatMs = LOCK_HEARTBEAT_MS } = {}) {
  fs.mkdirSync(stateDir, { recursive: true });
  const file = lockFile(stateDir);
  const startedAt = new Date().toISOString();
  const rec = () => ({ pid: process.pid, started_at: startedAt, heartbeat_at: new Date().toISOString() });
  let told = false;
  for (;;) {
    if (signal?.aborted) return null;
    if (createExclusive(file, rec())) break;
    const lock = readWakeLock(stateDir);
    if (!lockHeld(lock)) {
      // Stale (dead pid, no heartbeat, unreadable) or our own: replace it.
      try { fs.rmSync(file, { force: true }); } catch { /* raced with another process */ }
      continue;
    }
    if (!told) { log(`budenie tejto inštancie už beží (pid ${lock.pid}); čakám, kým skončí`); told = true; }
    await new Promise((r) => {
      const t = setTimeout(r, pollMs);
      signal?.addEventListener('abort', () => { clearTimeout(t); r(); }, { once: true });
    });
  }
  if (told) log('zámok budenia prevzatý');
  const beat = setInterval(() => {
    // Only our own lock: a process that was taken over as stale never overwrites the new holder.
    try { if (readWakeLock(stateDir)?.pid === process.pid) fs.writeFileSync(file, JSON.stringify(rec())); } catch { /* next beat */ }
  }, heartbeatMs);
  beat.unref?.();
  return {
    release() {
      clearInterval(beat);
      const lock = readWakeLock(stateDir);
      if (lock?.pid === process.pid) fs.rmSync(file, { force: true });
    },
  };
}

/**
 * Wake of this state dir for other callers (client `count`, comms-public#4): null when wake never ran here
 * (no wake-state.json), else { running, pid, heartbeat_at }. The lock file is removed on a clean stop, so a missing
 * lock next to an existing state file means "stopped".
 */
export function wakeStatus(stateDir) {
  if (!stateDir || !fs.existsSync(path.join(stateDir, 'wake-state.json'))) return null;
  const lock = readWakeLock(stateDir);
  const running = lockHeld(lock, { pid: -1 });
  return { running, pid: running ? lock.pid : null, heartbeat_at: running ? lock.heartbeat_at : null };
}
