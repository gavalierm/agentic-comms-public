// keep: core
// A thread outside the workflow (no turn record, e.g. the maintainer's thread of send --maintainer) has no hook that
// drops a stale bell (operator 2026-11-16: the bell queued behind the running turn came after the mail was picked and
// cost an empty turn). While T3 runs it, its ring is held; after the run it rings only when the mail still waits.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, cleanup, tmpDir } from './helpers.mjs';
import { Waker } from '../src/wake/index.mjs';

const MT = 'thread-maint';

async function setup(t) {
  const inst = makeInstance('wake-outside');
  const srv = await startInstance(inst);
  const stateDir = tmpDir('comms-wake-outside-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, stateDir); });
  const f = path.join(inst.dir, 'instance.json');
  fs.writeFileSync(f, JSON.stringify({ ...JSON.parse(fs.readFileSync(f, 'utf8')), maintainer: { thread: MT } }, null, 2));
  const client = makeClient(inst);
  const adapter = { name: 'fake', command: ['fake'], calls: [], threads: {} };
  adapter.ring = async (holder, text) => { adapter.calls.push({ holder, text }); return { ok: true }; };
  adapter.status = async (thread) => {
    const s = adapter.threads[thread];
    if (s === 'gone') return { ok: false, running: false, status: null, error: `Thread ${thread} is no longer available.` };
    return { ok: true, running: !!(s && s.run), status: s && s.run ? 'running' : 'completed', archived: false, run: (s && s.run) || null };
  };
  const logs = [];
  const waker = new Waker({ client: makeClient(inst), adapter, stateFile: path.join(stateDir, 'wake-state.json'), log: (m) => logs.push(m), maxEventAgeMs: 1e12, idleDedupeMs: 0 });
  await waker.initCursor();
  return { client, waker, adapter, logs };
}

test('a running thread outside the workflow gets no bell; picked during the run, none after it either', async (t) => {
  const { client, waker, adapter, logs } = await setup(t);
  adapter.threads[MT] = { run: 'run-1' };
  await client.send({ maintainer: true, level: 'assist', text: 'report', thread_id: 'thr-s' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [], 'no bell while it runs');
  assert.ok(logs.some((l) => /skip \d+ \(running\)/.test(l)), logs.join('\n'));
  await client.receive({ thread: MT });
  adapter.threads[MT] = {};
  await waker.releaseHeld();
  assert.deepEqual(adapter.calls, [], 'picked meanwhile: no empty bell turn');
});

test('a running thread outside the workflow is rung once its run ends, when the mail still waits', async (t) => {
  const { client, waker, adapter } = await setup(t);
  adapter.threads[MT] = { run: 'run-1' };
  await client.send({ maintainer: true, level: 'assist', text: 'report', thread_id: 'thr-s' });
  await waker.pollOnce(500);
  await waker.releaseHeld();
  assert.deepEqual(adapter.calls, [], 'still running: still held');
  adapter.threads[MT] = {};
  await waker.releaseHeld();
  assert.equal(adapter.calls.length, 1);
  assert.equal(adapter.calls[0].holder, MT);
});

test('an idle thread outside the workflow is rung at once', async (t) => {
  const { client, waker, adapter } = await setup(t);
  await client.send({ maintainer: true, level: 'assist', text: 'report', thread_id: 'thr-s' });
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 1);
});

test('a held ring whose thread is gone meanwhile bounces (undelivered)', async (t) => {
  const { client, waker, adapter } = await setup(t);
  adapter.threads[MT] = { run: 'run-1' };
  const m = await client.send({ maintainer: true, level: 'assist', text: 'report', thread_id: 'thr-s' });
  await waker.pollOnce(500);
  adapter.threads[MT] = 'gone';
  await waker.releaseHeld();
  assert.deepEqual(adapter.calls, []);
  assert.equal((await client.message(m.id)).status, 'undelivered');
});
