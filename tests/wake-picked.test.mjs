// ttl: 2026-12-14
// Mail bell only while mail waits (wake-picked-check): a message the thread picked up before the ring gives no bell
// (the bell turn would find no mail and the workflow hook would block its prompt); a waiting message rings.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, cleanup, tmpDir } from './helpers.mjs';
import { Waker, WAKE_TEMPLATE } from '../src/wake/index.mjs';

async function setup(t) {
  const inst = makeInstance('wake-picked');
  const srv = await startInstance(inst);
  const stateDir = tmpDir('comms-wake-picked-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, stateDir); });
  const client = makeClient(inst);
  await client.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  const calls = [];
  const adapter = { name: 'fake', command: ['fake'], ring: async (holder, text) => { calls.push({ holder, text }); return { ok: true }; } };
  const logs = [];
  const waker = new Waker({
    client: makeClient(inst), adapter, stateFile: path.join(stateDir, 'wake-state.json'),
    log: (m) => logs.push(m), maxEventAgeMs: 1e12, idleDedupeMs: 0,
  });
  await waker.initCursor();
  return { client, waker, calls, logs };
}

test('message picked up before the ring: no bell, the log says why', async (t) => {
  const { client, waker, calls, logs } = await setup(t);
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'help?', thread_id: 'thr-a' });
  await client.receive({ project: 'p', topic: 't' });
  // Model 2 (I2): the inbox is status pending; a picked message leaves it (count total 0) and is owed, not new mail.
  assert.equal((await client.pendingMail({ project: 'p', topic: 't' })).total, 0, 'the thread picked it up');
  const st = await client.message(m.id);
  assert.equal(st.status, 'picked');
  assert.ok(st.picked_at);
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(calls, [], 'no bell for picked-up mail');
  assert.ok(logs.some((l) => l.includes(`mail p/t: žiadna čakajúca; správa ${m.id} nezvoní`)), logs.join('\n'));
  assert.ok(logs.some((l) => /skip \d+ \(picked\)/.test(l)), logs.join('\n'));
});

test('waiting message (pending, not picked up): bell', async (t) => {
  const { client, waker, calls } = await setup(t);
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'help?', thread_id: 'thr-a' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(calls, [{ holder: 'thr-1', text: `you have mail from alice in topic p/t` }]);
});
