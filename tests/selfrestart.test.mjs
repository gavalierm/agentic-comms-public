// Self-restart after deployment (0.2.2) and wake configuration from <dataDir>/wake.json.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readRevision, selfRestartEnabled } from '../src/common/selfrestart.mjs';
import { resolveWakeConfig } from '../src/wake/index.mjs';
import { makeInstance, startInstance, tmpDir, cleanup, sleep } from './helpers.mjs';

const COMMS_BIN = fileURLToPath(new URL('../bin/comms.mjs', import.meta.url));
const WAKE_BIN = fileURLToPath(new URL('../bin/wake.mjs', import.meta.url));
const SHA1 = 'a'.repeat(40);
const SHA2 = 'b'.repeat(40);

function fakeRepo(head = SHA1) {
  const root = tmpDir('comms-repo-');
  fs.mkdirSync(path.join(root, '.git', 'refs', 'heads'), { recursive: true });
  fs.writeFileSync(path.join(root, '.git', 'HEAD'), head + '\n');
  return root;
}

function run(bin, args, env) {
  const child = spawn(process.execPath, [bin, ...args], {
    env: { PATH: process.env.PATH, HOME: process.env.HOME, COMMS_SELF_RESTART_INTERVAL_MS: '100', ...env }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  const p = { child, err: '', exited: null };
  child.stderr.on('data', (d) => { p.err += d; });
  child.stdout.resume();
  p.exited = new Promise((r) => child.on('exit', (code, sig) => r({ code, sig })));
  return p;
}

async function until(fn, ms = 5000) {
  for (let t = 0; t < ms; t += 50) { if (fn()) return; await sleep(50); }
  throw new Error('timeout');
}

test('readRevision: detached SHA, ref resolved to SHA, packed-refs, no .git', () => {
  const root = fakeRepo();
  assert.equal(readRevision(root), SHA1);
  fs.writeFileSync(path.join(root, '.git', 'HEAD'), 'ref: refs/heads/dev\n');
  fs.writeFileSync(path.join(root, '.git', 'refs', 'heads', 'dev'), SHA2 + '\n');
  assert.equal(readRevision(root), `refs/heads/dev@${SHA2}`);
  fs.rmSync(path.join(root, '.git', 'refs', 'heads', 'dev'));
  fs.writeFileSync(path.join(root, '.git', 'packed-refs'), `# pack-refs\n${SHA1} refs/heads/dev\n`);
  assert.equal(readRevision(root), `refs/heads/dev@${SHA1}`);
  const bare = tmpDir('comms-nogit-');
  assert.equal(readRevision(bare), null);
  cleanup(root, bare);
});

test('selfRestartEnabled: flag, env and config switch it off', () => {
  assert.equal(selfRestartEnabled({}, {}), true);
  assert.equal(selfRestartEnabled({ noSelfRestart: true }, {}), false);
  assert.equal(selfRestartEnabled({}, { COMMS_SELF_RESTART: '0' }), false);
  assert.equal(selfRestartEnabled({}, { COMMS_SELF_RESTART: '1' }), true);
  assert.equal(selfRestartEnabled({}, {}, null, { self_restart: false }), false);
});

test('serve: HEAD change in the install repo ends the process with code 0', async (t) => {
  const inst = makeInstance('selfsrv');
  const root = fakeRepo();
  const p = run(COMMS_BIN, ['serve', '--data-dir', inst.dir], { COMMS_INSTALL_ROOT: root });
  t.after(async () => { try { p.child.kill('SIGKILL'); } catch { /* gone */ } await p.exited; cleanup(inst.dir, root); });
  await until(() => p.err.includes('samoreštart: revízia'));
  fs.writeFileSync(path.join(root, '.git', 'HEAD'), SHA2 + '\n');
  const r = await p.exited;
  assert.equal(r.code, 0, p.err);
  assert.match(p.err, new RegExp(`install changed ${SHA1} -> ${SHA2}, exiting for restart`));
  assert.equal(inst.store.serverInfo(), null, 'čisté ukončenie zmaže server.json');
});

test('serve: --no-self-restart and missing .git do not watch', async (t) => {
  const inst = makeInstance('selfoff');
  const bare = tmpDir('comms-nogit-');
  const a = run(COMMS_BIN, ['serve', '--data-dir', inst.dir, '--no-self-restart'], { COMMS_INSTALL_ROOT: bare });
  t.after(async () => { try { a.child.kill('SIGKILL'); } catch { /* gone */ } await a.exited; cleanup(inst.dir, bare); });
  await until(() => a.err.includes('listening on'));
  await sleep(300);
  assert.ok(!a.err.includes('samoreštart'), a.err);
  a.child.kill('SIGTERM'); await a.exited;
  const inst2 = makeInstance('selfnogit');
  const b = run(COMMS_BIN, ['serve', '--data-dir', inst2.dir], { COMMS_INSTALL_ROOT: bare });
  t.after(async () => { try { b.child.kill('SIGKILL'); } catch { /* gone */ } await b.exited; cleanup(inst2.dir); });
  await until(() => b.err.includes('samoreštart'));
  assert.match(b.err, /bez \.git, nekontroluje sa/);
});

test('wake run: HEAD change and wake.json change each end the process with code 0', async (t) => {
  const inst = makeInstance('selfwake');
  const srv = await startInstance(inst);
  const root = fakeRepo();
  const state = tmpDir('comms-selfwake-state-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, root, state); });
  const args = ['run', '--data-dir', inst.dir, '--no-start', '--adapter', 'none', '--wait-ms', '200', '--state-dir', state];

  const a = run(WAKE_BIN, args, { COMMS_INSTALL_ROOT: root });
  await until(() => a.err.includes('samoreštart: revízia'));
  fs.writeFileSync(path.join(root, '.git', 'HEAD'), SHA2 + '\n');
  const ra = await a.exited;
  assert.equal(ra.code, 0, a.err);
  assert.match(a.err, /install changed .* exiting for restart/);
  assert.ok(fs.existsSync(path.join(state, 'wake-state.json')), 'stav wake zapísaný');

  const b = run(WAKE_BIN, args, { COMMS_INSTALL_ROOT: root });
  await until(() => b.err.includes('sleduje'));
  await sleep(150);
  fs.writeFileSync(path.join(inst.dir, 'wake.json'), JSON.stringify({ limit_per_hour: 3 }));
  const rb = await b.exited;
  assert.equal(rb.code, 0, b.err);
  assert.match(b.err, /config changed .*wake\.json, exiting for restart/);
});

test('resolveWakeConfig: wake.json from the data dir; env and --config take precedence', () => {
  const inst = makeInstance('wakejson');
  fs.writeFileSync(path.join(inst.dir, 'wake.json'), JSON.stringify({ limit_per_hour: 3, keepalive: { since_command: 'echo 0' }, self_restart: false }));
  const c = resolveWakeConfig({ dataDir: inst.dir }, {});
  assert.equal(c.limitPerHour, 3);
  assert.equal(c.keepaliveSinceCommand, 'echo 0');
  assert.equal(c.selfRestart, false);
  assert.equal(c.wakeFile, path.join(inst.dir, 'wake.json'));
  assert.equal(resolveWakeConfig({ dataDir: inst.dir }, { WAKE_LIMIT_PER_HOUR: '5' }).limitPerHour, 5);
  assert.equal(resolveWakeConfig({ dataDir: inst.dir, limitPerHour: '7' }, {}).limitPerHour, 7);
  const cfgFile = path.join(inst.dir, 'cfg.json');
  fs.writeFileSync(cfgFile, JSON.stringify({ wake: { limit_per_hour: 9 } }));
  const d = resolveWakeConfig({ dataDir: inst.dir, config: cfgFile }, {});
  assert.equal(d.limitPerHour, 9);
  assert.equal(d.keepaliveSinceCommand, null);
  assert.equal(d.wakeFile, null);
  cleanup(inst.dir);
});
