// Lock files of one machine, shared by the wake lock (src/wake/lock.mjs: one `wake run` per instance) and the flush lock
// of the outbox (src/client/index.mjs: one flush at a time). A lock is a JSON file made with an exclusive create; it
// holds the pid of its holder and a heartbeat time. A holder whose pid is dead or whose heartbeat is stale is replaced.
import fs from 'node:fs';
import { randomBytes } from 'node:crypto';
import { sleep } from './util.mjs';

const TOKEN_LOCK_POLL_MS = 25;

export function readLock(file) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return null; }
}

export function alive(pid) {
  try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; }
}

/** The holder of the lock record lives: its pid runs and its heartbeat is younger than `staleMs`. */
export function lockLive(lock, staleMs, now = Date.now()) {
  if (!lock || !Number.isInteger(lock.pid)) return false;
  const beat = Date.parse(lock.heartbeat_at || '');
  return alive(lock.pid) && !Number.isNaN(beat) && now - beat < staleMs;
}

/** Exclusive create (`wx`): false when the file is already there. */
export function createExclusive(file, rec) {
  try {
    fs.writeFileSync(file, JSON.stringify(rec), { flag: 'wx' });
    return true;
  } catch (e) {
    if (e.code === 'EEXIST') return false;
    throw e;
  }
}

/** A token lock is held: its record lives by the holder's own stale time; an unreadable file (between create and write) while it is young. */
function tokenLockLive(file, lock, staleMs) {
  if (lock) return lockLive(lock, lock.stale_ms > 0 ? lock.stale_ms : staleMs);
  try { return Date.now() - fs.statSync(file).mtimeMs < staleMs; } catch { return false; }
}

/**
 * Remove the lock only when it still carries `token`. The file is moved aside first (a rename gives each file to one
 * process), then read: a read followed by a remove would delete the lock that another process made in between. A lock
 * of another token is put back (link: never over a newer lock).
 */
function removeIfToken(file, token) {
  const aside = `${file}.${randomBytes(4).toString('hex')}`;
  try { fs.renameSync(file, aside); } catch { return; }
  if (readLock(aside)?.token !== token) {
    try { fs.linkSync(aside, file); } catch { /* a newer lock is there */ }
  }
  fs.rmSync(aside, { force: true });
}

/**
 * Lock of a short critical section, owned by a per-acquisition token and not by the pid: two holders in one process
 * exclude each other too. Waits (short polls) at most `waitMs` while a live holder has it; a dead or stale holder is
 * replaced. The record is { pid, token, heartbeat_at, stale_ms }: `stale_ms` is the holder's own bound, so a waiter with
 * a shorter timeout does not replace a holder that is still inside one step.
 * @returns {Promise<{ beat(): boolean, release(): void } | null>} null when the wait ran out
 */
export async function acquireTokenLock(file, { waitMs, staleMs, pollMs = TOKEN_LOCK_POLL_MS }) {
  const token = randomBytes(8).toString('hex');
  const rec = () => ({ pid: process.pid, token, heartbeat_at: new Date().toISOString(), stale_ms: staleMs });
  const until = Date.now() + waitMs;
  for (;;) {
    if (createExclusive(file, rec())) break;
    const lock = readLock(file);
    if (!tokenLockLive(file, lock, staleMs)) {
      removeIfToken(file, lock?.token);
      if (createExclusive(file, rec())) break;
    }
    // Every turn without the lock ends here, so a lock that cannot be removed never spins.
    if (Date.now() >= until) return null;
    await sleep(pollMs);
  }
  return {
    /** Refresh the heartbeat; false when the lock is no longer ours (replaced as stale): the holder must stop. */
    beat() {
      // Check and write go through one descriptor: they touch the same file, never a lock that replaced ours.
      let fd;
      try {
        fd = fs.openSync(file, 'r+');
        if (JSON.parse(fs.readFileSync(fd, 'utf8')).token !== token) return false;
        const text = JSON.stringify(rec());
        fs.writeSync(fd, text, 0);
        fs.ftruncateSync(fd, Buffer.byteLength(text));
        return true;
      } catch {
        return false;
      } finally {
        if (fd !== undefined) fs.closeSync(fd);
      }
    },
    release() {
      if (readLock(file)?.token === token) removeIfToken(file, token);
    },
  };
}
