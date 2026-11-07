// `wake install` (comms-public#4): one command per system for the permanent wake run, and the wake status in `count`
// for the session status line. The plans of all three systems are checked here on any machine.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { installPlan, formatPlan, applyPlan, serviceNames } from '../src/wake/install.mjs';
import { wakeStatus, lockFile } from '../src/wake/lock.mjs';
import { makeInstance, tmpDir, cleanup, killPid, sleep } from './helpers.mjs';

const WAKE_BIN = fileURLToPath(new URL('../bin/wake.mjs', import.meta.url));
const COMMS_BIN = fileURLToPath(new URL('../bin/comms.mjs', import.meta.url));
const tryJson = (s) => { try { return JSON.parse(s); } catch { return null; } };
const node = (bin, args, env = {}) => {
  const r = spawnSync(process.execPath, [bin, ...args], { encoding: 'utf8', env: { PATH: process.env.PATH, HOME: process.env.HOME, COMMS_SELF_RESTART: '0', ...env } });
  return { code: r.status, out: r.stdout, err: r.stderr, json: tryJson(r.stdout) };
};

const posix = { instance: 'default', runArgs: ['--instance', 'default'], node: '/usr/bin/node', bin: '/opt/my comms/bin/wake.mjs', home: '/srv/u', base: '/srv/u/.agentic-comms', env: { PATH: '/usr/bin:/bin' }, uid: 501 };
const win = { instance: 'default', runArgs: ['--instance', 'default'], node: 'C:\\Program Files\\nodejs\\node.exe', bin: 'C:\\Users\\u\\comms\\bin\\wake.mjs', home: 'C:\\Users\\u', base: 'C:\\Users\\u\\.agentic-comms', env: {} };

test('linux: systemd user unit with Restart=always, enable and restart; --remove disables and deletes it', () => {
  const p = installPlan({ ...posix, platform: 'linux' });
  assert.equal(p.files.length, 1);
  assert.equal(p.files[0].path, '/srv/u/.config/systemd/user/agentic-wake-default.service');
  const unit = p.files[0].content;
  assert.match(unit, /^ExecStart="\/usr\/bin\/node" "\/opt\/my comms\/bin\/wake.mjs" "run" "--instance" "default"$/m, 'paths with spaces are quoted');
  assert.match(unit, /^Restart=always$/m);
  assert.match(unit, /^Environment="PATH=\/usr\/bin:\/bin"$/m);
  assert.match(unit, /^WantedBy=default.target$/m);
  assert.deepEqual(p.after.map((c) => c.args.slice(1).join(' ')), ['daemon-reload', 'enable agentic-wake-default.service', 'restart agentic-wake-default.service']);
  assert.ok(p.notes.some((n) => /enable-linger/.test(n)));
  assert.deepEqual(p.before[0].args, ['--user', 'show-environment'], 'checks the user manager first');
  const dir = tmpDir('wake-nobus-');
  try {
    const res = applyPlan({ ...p, files: [{ path: path.join(dir, 'u.service'), content: 'x' }] }, { spawn: () => ({ status: 1, stderr: 'Failed to connect to user scope bus' }) });
    assert.equal(res.ok, false);
    assert.match(res.errors[0], /používateľský systemd nebeží.*enable-linger/);
    assert.equal(fs.existsSync(path.join(dir, 'u.service')), false, 'nothing is written without a user manager');
  } finally { cleanup(dir); }
  assert.equal(installPlan({ ...posix, platform: 'linux', env: { XDG_CONFIG_HOME: '/x' } }).files[0].path, '/x/systemd/user/agentic-wake-default.service');

  const r = installPlan({ ...posix, platform: 'linux', remove: true });
  assert.deepEqual([r.files, r.remove], [[], ['/srv/u/.config/systemd/user/agentic-wake-default.service']]);
  assert.deepEqual(r.before[0].args, ['--user', 'disable', '--now', 'agentic-wake-default.service']);
});

test('macOS: LaunchAgent with KeepAlive and a log file, replaced by bootout + bootstrap; --remove boots it out', () => {
  const p = installPlan({ ...posix, platform: 'darwin' });
  assert.equal(p.files[0].path, '/srv/u/Library/LaunchAgents/com.agentic-comms.wake.default.plist');
  const plist = p.files[0].content;
  assert.match(plist, /<string>\/opt\/my comms\/bin\/wake.mjs<\/string>\n {4}<string>run<\/string>/);
  assert.match(plist, /<key>KeepAlive<\/key><true\/>/);
  assert.match(plist, /<key>RunAtLoad<\/key><true\/>/);
  assert.match(plist, /<key>StandardErrorPath<\/key><string>\/srv\/u\/.agentic-comms\/wake-default.log<\/string>/);
  assert.deepEqual(p.before[0], { cmd: 'launchctl', args: ['bootout', 'gui/501/com.agentic-comms.wake.default'], ignoreFail: true });
  assert.deepEqual(p.after[0].args, ['bootstrap', 'gui/501', '/srv/u/Library/LaunchAgents/com.agentic-comms.wake.default.plist']);
  const r = installPlan({ ...posix, platform: 'darwin', remove: true });
  assert.deepEqual([r.files.length, r.after.length, r.remove], [0, 0, ['/srv/u/Library/LaunchAgents/com.agentic-comms.wake.default.plist']]);
});

test('Windows: the old loop and wake are stopped first, CRLF loop that ends when the install is gone, hidden logon task', () => {
  const p = installPlan({ ...win, platform: 'win32' });
  const legacy = p.before[0].args.at(-1);
  assert.match(legacy, /Get-ScheduledTask -TaskName 'agentic-wake' .*-like '\*wake-loop.cmd\*'/, 'the hand-made task of README <= 0.4.5 is replaced');
  assert.match(legacy, /Unregister-ScheduledTask -TaskName 'agentic-wake' -Confirm:\$false/);
  assert.match(legacy, /CommandLine -like '\*\\wake-loop.cmd\*'/);
  assert.match(p.before[1].args.at(-1), /Stop-ScheduledTask -TaskName 'agentic-wake-default'/);
  assert.match(p.before[1].args.at(-1), /CommandLine -like '\*wake-loop-default.cmd\*'.*Terminate/, 'the cmd loop is ended too, else it starts node again (#3)');
  assert.deepEqual(p.before[2], { stopWake: true });
  const loop = p.files[0];
  assert.equal(loop.path, 'C:\\Users\\u\\.agentic-comms\\wake-loop-default.cmd');
  assert.ok(loop.content.includes('\r\n') && !/[^\r]\n/.test(loop.content), 'cmd file uses CRLF');
  assert.match(loop.content, /if not exist C:\\Users\\u\\comms\\bin\\wake.mjs exit \/b 0/);
  assert.match(loop.content, /"C:\\Program Files\\nodejs\\node.exe" C:\\Users\\u\\comms\\bin\\wake.mjs run --instance default >> C:\\Users\\u\\.agentic-comms\\wake-default.log 2>&1/);
  const reg = p.after[0].args.at(-1);
  assert.match(reg, /-Execute 'conhost.exe' -Argument '--headless cmd.exe \/d \/c "C:\\Users\\u\\.agentic-comms\\wake-loop-default.cmd"'/);
  assert.match(reg, /-AtLogOn/);
  assert.match(reg, /ExecutionTimeLimit \(\[TimeSpan\]::Zero\)/);
  assert.match(reg, /Register-ScheduledTask -TaskName 'agentic-wake-default'.*-Force/);
  assert.match(reg, /Start-ScheduledTask/);

  const r = installPlan({ ...win, platform: 'win32', remove: true });
  assert.match(r.before.at(-1).args.at(-1), /Unregister-ScheduledTask -TaskName 'agentic-wake-default'/);
  assert.deepEqual([r.files.length, r.after.length, r.remove], [0, 0, ['C:\\Users\\u\\.agentic-comms\\wake-loop-default.cmd']]);
});

test('instance names that cannot be a service name and unknown systems are refused', () => {
  assert.throws(() => serviceNames('a b'), /inštancia/);
  assert.throws(() => serviceNames('../x'), /inštancia/);
  assert.throws(() => installPlan({ ...posix, platform: 'aix' }), /nie je podporovaný/);
});

test('applyPlan: before, files, after in order; ignoreFail is skipped, the first real error stops the rest', () => {
  const dir = tmpDir('wake-install-');
  try {
    const calls = [];
    const plan = {
      before: [{ cmd: 'a', args: [], ignoreFail: true }, { stopWake: true }],
      files: [{ path: path.join(dir, 'sub', 'f.txt'), content: 'x' }],
      remove: [path.join(dir, 'gone.txt')],
      after: [{ cmd: 'b', args: ['1'] }, { cmd: 'c', args: [] }],
    };
    fs.writeFileSync(path.join(dir, 'gone.txt'), 'y');
    const spawn = (cmd) => { calls.push(cmd); return { status: cmd === 'c' ? 0 : 1, stderr: `${cmd} failed\nmore` }; };
    const res = applyPlan(plan, { stopWake: () => calls.push('stop'), spawn });
    assert.deepEqual(calls, ['a', 'stop', 'b']);
    assert.deepEqual(res, { ok: false, errors: ['b 1: b failed'] });
    assert.equal(fs.readFileSync(path.join(dir, 'sub', 'f.txt'), 'utf8'), 'x');
    assert.equal(fs.existsSync(path.join(dir, 'gone.txt')), false);
    assert.match(formatPlan(plan), /^> a\n> ukončiť bežiace budenie.*\n--- .*f.txt\nx\n--- zmazať .*gone.txt\n> b 1\n> c\n$/);
  } finally { cleanup(dir); }
});

test('CLI install --print: the instance is added to the run options, --key on the command line is refused', () => {
  const home = tmpDir('wake-home-');
  try {
    const env = { HOME: home, COMMS_BASE_DIR: path.join(home, '.agentic-comms'), WAKE_INSTALL_PLATFORM: 'linux', XDG_CONFIG_HOME: path.join(home, 'cfg') };
    const r = node(WAKE_BIN, ['install', '--print', '--adapter', 'none'], env);
    assert.equal(r.code, 0, r.err);
    assert.match(r.out, /--- .*cfg\/systemd\/user\/agentic-wake-default.service/);
    assert.match(r.out, new RegExp(`ExecStart="${process.execPath.replace(/[.*+?^${}()|[\]\\/]/g, '\\$&')}" ".*bin/wake.mjs" "run" "--instance" "default" "--adapter" "none"`));
    assert.equal(fs.existsSync(path.join(home, 'cfg')), false, '--print writes nothing');
    const w = node(WAKE_BIN, ['install', '--print', '--instance', 'pilot'], { ...env, WAKE_INSTALL_PLATFORM: 'win32' });
    assert.match(w.out, /wake-loop-pilot.cmd/);
    assert.equal((w.out.match(/--instance pilot/g) || []).length, 1, 'an explicit --instance is not doubled');
    const k = node(WAKE_BIN, ['install', '--print', '--key', 'secret'], env);
    assert.equal(k.code, 1);
    assert.match(k.err, /kľúč nepatrí/);
    assert.doesNotMatch(k.out + k.err, /secret/);
  } finally { cleanup(home); }
});

test('wakeStatus and count: no field before wake ever ran, running with a fresh lock, stopped without it', async (t) => {
  const a = makeInstance('a');
  const stateDir = tmpDir('wake-state-');
  t.after(async () => {
    try { const info = JSON.parse(fs.readFileSync(path.join(a.dir, 'server.json'), 'utf8')); killPid(info.pid); await sleep(100); } catch {}
    cleanup(a.dir, stateDir);
  });
  assert.equal(wakeStatus(stateDir), null);
  const count = () => node(COMMS_BIN, ['count', '--data-dir', a.dir, '--key', a.key, '--project', 'p', '--topic', 't'], { WAKE_STATE_DIR: stateDir });
  const c0 = count();
  assert.equal(c0.code, 0, c0.err);
  assert.equal(c0.json.wake, undefined, 'a machine without wake shows nothing');

  fs.writeFileSync(path.join(stateDir, 'wake-state.json'), '{}');
  assert.deepEqual(count().json.wake, { running: false, pid: null, heartbeat_at: null });
  const now = new Date().toISOString();
  fs.writeFileSync(lockFile(stateDir), JSON.stringify({ pid: process.pid, started_at: now, heartbeat_at: now }));
  assert.deepEqual(count().json.wake, { running: true, pid: process.pid, heartbeat_at: now });
  assert.deepEqual(wakeStatus(stateDir), { running: true, pid: process.pid, heartbeat_at: now });

  // Workflow passes its own --client-dir (outbox); the wake state stays where `wake run` keeps it: <data-dir>/client/wake.
  const own = path.join(stateDir, 'workflow-client');
  const defDir = path.join(a.dir, 'client', 'wake');
  fs.mkdirSync(defDir, { recursive: true });
  fs.writeFileSync(path.join(defDir, 'wake-state.json'), '{}');
  const wf = node(COMMS_BIN, ['count', '--data-dir', a.dir, '--key', a.key, '--project', 'p', '--topic', 't', '--client-dir', own, '--outbox', path.join(own, 'outbox.jsonl')]);
  assert.equal(wf.code, 0, wf.err);
  assert.deepEqual(wf.json.wake, { running: false, pid: null, heartbeat_at: null }, 'found in the default wake dir, not in --client-dir');
});
