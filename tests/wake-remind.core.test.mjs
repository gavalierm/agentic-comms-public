// keep: core
// Mail not picked up is lost mail (operator 2026-11-17, critical): a ring dropped above the hourly limit, an event
// older than the wake's max age or a bell turn that could not read the mail left messages pending for days, and nobody
// rang again. The wake's reminder rings a message still pending after remindAfterMs again (also a reply to a thread), at
// most remindMax times, then one operator notice; a picked message is not rung; a message older than remindMaxAgeMs only
// goes into one summary notice.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, cleanup, tmpDir } from './helpers.mjs';
import { Waker, WAKE_TEMPLATE } from '../src/wake/index.mjs';

const MIN = 60_000;

async function setup(t, opts = {}) {
  const inst = makeInstance('wake-remind');
  const srv = await startInstance(inst);
  const stateDir = tmpDir('comms-wake-remind-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, stateDir); });
  const client = makeClient(inst);
  await client.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  const calls = [];
  const adapter = { name: 'fake', command: ['fake'], ring: async (holder, text) => { calls.push({ holder, text }); return { ok: true }; } };
  const logs = [];
  const clock = { now: Date.now() };
  const waker = new Waker({
    client: makeClient(inst), adapter, stateFile: path.join(stateDir, 'wake-state.json'), now: () => clock.now,
    log: (m) => logs.push(m), maxEventAgeMs: 1e12, idleDedupeMs: 0, limitPerHour: 100,
    remindEveryMs: 0, remindAfterMs: 30 * MIN, remindMax: 2, ...opts,
  });
  await waker.initCursor();
  return { client, waker, calls, logs, clock };
}

test('a message still pending rings again after remindAfterMs, at most remindMax times, then one operator notice; picked: no ring', async (t) => {
  const { client, waker, calls, clock, logs } = await setup(t);
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'help?', thread_id: 'thr-a' });
  await waker.pollOnce(300);
  assert.equal(calls.length, 1, 'the first ring: ' + logs.join(' / '));
  const bell = { holder: 'thr-1', text: `${WAKE_TEMPLATE} p/t from alice` };
  clock.now += 10 * MIN;
  await waker.pollOnce(50);
  assert.equal(calls.length, 1, 'not again before remindAfterMs');
  clock.now += 21 * MIN;
  await waker.pollOnce(50);
  assert.deepEqual(calls.at(-1), bell);
  assert.equal(calls.length, 2, 'the first reminder');
  clock.now += 31 * MIN;
  await waker.pollOnce(50);
  assert.equal(calls.length, 3, 'the second reminder');
  clock.now += 31 * MIN;
  await waker.pollOnce(50);
  clock.now += 31 * MIN;
  await waker.pollOnce(50);
  assert.equal(calls.length, 3, 'no ring after remindMax');
  const notices = (await client.notices()).notices.filter((n) => n.kind === 'unpicked');
  assert.equal(notices.length, 1, JSON.stringify(notices));
  assert.equal(notices[0].message_id, m.id);
  assert.match(notices[0].title, /nevyzdvihnutá po 2 pripomienkach: p\/t/);

  // A picked message is not rung again.
  const m2 = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'two?', thread_id: 'thr-a' });
  await waker.pollOnce(300);
  const n = calls.length;
  await client.receive({ project: 'p', topic: 't', id: m2.id });
  clock.now += 31 * MIN;
  await waker.pollOnce(50);
  assert.equal(calls.length, n, 'picked: no reminder');
});

test('a reply to a thread is reminded to that thread; a message older than remindMaxAgeMs only goes into one summary notice', async (t) => {
  const { client, waker, calls, clock } = await setup(t, { remindMaxAgeMs: 2 * 60 * MIN });
  const a = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'q', thread_id: 'thr-a' });
  await client.receive({ project: 'p', topic: 't', id: a.id });
  await client.resolve({ project: 'p', topic: 't', id: a.id, state: 'hotovo', text: 'answer' });
  await waker.pollOnce(300);
  const first = calls.filter((c) => c.holder === 'thr-a').length;
  assert.equal(first, 1, JSON.stringify(calls));
  clock.now += 31 * MIN;
  await waker.pollOnce(50);
  assert.equal(calls.filter((c) => c.holder === 'thr-a').length, 2, 'the reply is reminded to the sender thread');
  assert.equal(calls.at(-1).text, `${WAKE_TEMPLATE} thread/thr-a from p/t`);
  clock.now += 3 * 60 * MIN;
  const before = calls.length;
  await waker.pollOnce(50);
  assert.equal(calls.length, before, 'too old: no ring');
  const old = (await client.notices()).notices.filter((n) => n.kind === 'unpicked');
  assert.equal(old.length, 1);
  assert.match(old[0].title, /1 správ\(y\) nevyzdvihnutých dlhšie ako 2 h/);
  // Acked: not made again for the same message.
  await client.decide({ notice_id: old[0].id, decision: 'ack' });
  clock.now += 10 * MIN;
  await waker.pollOnce(50);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'unpicked').length, 0, 'an acked summary is not made again');
});
