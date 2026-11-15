// ttl: 2026-12-15
// Slot of a full test run (agent-alice/D-0069, overload 2026-11-15): npm test runs its suite through scripts/test-slot.mjs,
// at most 2 full runs on the machine (slot files in a shared state dir); a third waits and says so, then runs when a slot
// is free. A slot of a gone run (dead pid, no refresh) is taken over. The command here is a short fixture, not npm test.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const SLOT = path.join(ROOT, 'scripts', 'test-slot.mjs');
// The fixture run: marks that it started, then waits until the test lets it end.
const RUN = "const fs=require('fs'),p=require('path');const [d,n]=process.argv.slice(1);fs.writeFileSync(p.join(d,'started-'+n),'');const t=setInterval(()=>{if(fs.existsSync(p.join(d,'go-'+n))){clearInterval(t);}},20);";

function slotEnv(dir) {
  const env = { ...process.env, AGENTIC_TEST_SLOTS_DIR: path.join(dir, 'slots'), AGENTIC_TEST_SLOT_POLL_MS: '50' };
  delete env.AGENTIC_TEST_SLOTS;
  delete env.AGENTIC_TEST_SLOT_HELD; // this suite itself runs inside a slot of npm test
  return env;
}

function start(dir, name, env) {
  const child = spawn(process.execPath, [SLOT, '--', 'node', '-e', RUN, dir, name], { env });
  const res = { name, out: '', code: null };
  child.stdout.on('data', (d) => { res.out += d; });
  child.stderr.on('data', (d) => { res.out += d; });
  res.done = new Promise((resolve) => { child.on('close', (code) => { res.code = code; resolve(code); }); });
  return res;
}

async function until(cond, what, ms = 15000) {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error(`timeout: ${what}`);
    await new Promise((r) => { setTimeout(r, 25); });
  }
}

const started = (dir) => ['a', 'b', 'c'].filter((n) => fs.existsSync(path.join(dir, `started-${n}`)));
const slotFiles = (dir) => (fs.existsSync(path.join(dir, 'slots')) ? fs.readdirSync(path.join(dir, 'slots')).filter((f) => /^slot-\d+\.json$/.test(f)) : []);

test('npm test runs its full suite through the test slot', () => {
  const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
  assert.match(pkg.scripts.test, /node scripts\/test-slot\.mjs -- node --test "tests\/\*\*\/\*\.test\.mjs"/);
});

test('the slot lets two full runs pass and the third waits until one ends', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-slot-'));
  const env = slotEnv(dir);
  const runs = [];
  try {
    for (const n of ['a', 'b', 'c']) runs.push(start(dir, n, env));
    await until(() => started(dir).length >= 2, 'two runs start');
    const third = runs.find((r) => !started(dir).includes(r.name));
    await until(() => /waiting for a test slot/.test(third.out), `the third run says it waits (${third.out})`);
    await new Promise((r) => { setTimeout(r, 300); });
    assert.equal(started(dir).length, 2, 'the third run has not started');
    assert.equal(slotFiles(dir).length, 2);
    const first = runs.find((r) => r !== third);
    fs.writeFileSync(path.join(dir, `go-${first.name}`), '');
    assert.equal(await first.done, 0, first.out);
    await until(() => started(dir).includes(third.name), 'the third run starts once a slot is free');
    assert.match(third.out, /test slot taken after/);
    for (const r of runs) fs.writeFileSync(path.join(dir, `go-${r.name}`), '');
    for (const r of runs) assert.equal(await r.done, 0, r.out);
    assert.deepEqual(slotFiles(dir), [], 'every slot is freed');
  } finally {
    for (const r of runs) fs.writeFileSync(path.join(dir, `go-${r.name}`), '');
    await Promise.all(runs.map((r) => r.done));
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a slot of a gone run is taken over; a run inside a held slot does not wait', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'test-slot-'));
  const env = slotEnv(dir);
  try {
    const slots = path.join(dir, 'slots');
    fs.mkdirSync(slots, { recursive: true });
    const dead = spawnSync(process.execPath, ['-e', '0']).pid; // exited: its pid is not alive
    fs.writeFileSync(path.join(slots, 'slot-1.json'), JSON.stringify({ pid: dead, host: os.hostname() }));
    fs.writeFileSync(path.join(slots, 'slot-2.json'), JSON.stringify({ pid: process.pid, host: os.hostname() }));
    const old = new Date(Date.now() - 5 * 60000);
    fs.utimesSync(path.join(slots, 'slot-2.json'), old, old); // alive pid, but no refresh for minutes
    const r = spawnSync(process.execPath, [SLOT, 'node', '-e', 'process.exit(7)'], { env, encoding: 'utf8', timeout: 10000 });
    assert.equal(r.status, 7, `exit code of the command: ${r.stderr}`);
    assert.doesNotMatch(r.stderr, /waiting/);
    // Both slots held by live, fresh runs: a nested run (AGENTIC_TEST_SLOT_HELD) runs at once.
    for (const n of [1, 2]) fs.writeFileSync(path.join(slots, `slot-${n}.json`), JSON.stringify({ pid: process.pid, host: os.hostname() }));
    const nested = spawnSync(process.execPath, [SLOT, 'node', '-e', 'process.exit(0)'], { env: { ...env, AGENTIC_TEST_SLOT_HELD: path.join(slots, 'slot-1.json') }, encoding: 'utf8', timeout: 10000 });
    assert.equal(nested.status, 0, nested.stderr);
    assert.doesNotMatch(nested.stderr, /waiting/);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});
