// Windows fixes reported from another machine (comms-public #1, #2): the T3 CLI is a .cmd outside PATH, and a restart of
// the scheduled task left a second wake running.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { platformCommand } from '../src/adapters/t3/index.mjs';
import { acquireWakeLock, readWakeLock, lockHeld, lockFile } from '../src/wake/lock.mjs';

test('platformCommand: argv unchanged off Windows; on win32 t3 resolves to t3.cmd and runs through cmd.exe, quoted', () => {
  const argv = ['t3', 'thread', 'send', 'th-1', 'you have mail in topic app/fix'];
  assert.deepEqual(platformCommand(argv, { platform: 'linux' }), { file: 't3', args: argv.slice(1), verbatim: false });
  assert.deepEqual(platformCommand(argv, { platform: 'darwin' }).file, 't3');
  const cli = 'C:\\Users\\u x\\.t3\\bin\\t3.cmd';
  const env = { USERPROFILE: 'C:\\Users\\u x', ComSpec: 'C:\\Windows\\system32\\cmd.exe' };
  const w = platformCommand(argv, { platform: 'win32', env, exists: (f) => f === cli });
  assert.equal(w.file, 'C:\\Windows\\system32\\cmd.exe');
  assert.equal(w.verbatim, true);
  assert.deepEqual(w.args, ['/d', '/s', '/c', `""${cli}" "thread" "send" "th-1" "you have mail in topic app/fix""`]);
  const viaEnv = platformCommand(argv, { platform: 'win32', env: { ...env, T3CODE_CLI_PATH: 'D:\\t3\\t3.cmd' }, exists: () => true });
  assert.match(viaEnv.args[3], /^""D:\\t3\\t3\.cmd" /, 'T3CODE_CLI_PATH first');
  assert.equal(platformCommand(['C:\\t3\\t3.exe', 'x'], { platform: 'win32', env, exists: () => true }).file, 'C:\\t3\\t3.exe', 'an .exe runs directly');
  assert.equal(platformCommand(argv, { platform: 'win32', env, exists: () => false }).file, 't3', 'no CLI found: unchanged (the ring fails and is logged)');
  assert.match(platformCommand([cli, 'send', '100%'], { platform: 'win32', env }).error, /cmd\.exe rozvinie/);
});

test('wake lock: a second run waits while the first holds a fresh lock; a dead holder is replaced; release frees it', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-lock-'));
  try {
    const a = await acquireWakeLock(dir, { heartbeatMs: 50 });
    assert.equal(readWakeLock(dir).pid, process.pid);
    a.release();
    assert.equal(readWakeLock(dir), null);
    // Another live process (the parent) holds it: a second run waits and logs once.
    fs.writeFileSync(lockFile(dir), JSON.stringify({ pid: process.ppid, heartbeat_at: new Date().toISOString() }));
    assert.equal(lockHeld(readWakeLock(dir)), true);
    const logs = [];
    const ac = new AbortController();
    const waiting = acquireWakeLock(dir, { log: (m) => logs.push(m), signal: ac.signal, pollMs: 20 });
    await new Promise((r) => setTimeout(r, 80));
    assert.match(logs.join('\n'), new RegExp(`už beží \\(pid ${process.ppid}\\)`));
    ac.abort();
    assert.equal(await waiting, null, 'aborted while waiting');
    // A stale heartbeat (holder hung or pid reused) does not block.
    fs.writeFileSync(lockFile(dir), JSON.stringify({ pid: process.ppid, heartbeat_at: '2020-01-01T00:00:00Z' }));
    const b = await acquireWakeLock(dir, { pollMs: 20 });
    assert.equal(readWakeLock(dir).pid, process.pid);
    b.release();
    assert.equal(readWakeLock(dir), null);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
