// ttl: 2026-12-15
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeInstance, startInstance, makeClient, cleanup } from './helpers.mjs';

// POST /settle (topic settle-endpoint, 2026-11-15): a picked assist whose sender closed its topic could not be answered
// (reply 410), so it stayed `picked … | no answer` and came back as waiting. The recipient settles it without a reply.

async function closedSender(t, name) {
  const a = makeInstance(name);
  const bob = a.store.addKey('bob');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = makeClient(a);
  const asBob = makeClient(a, { key: bob.key });
  await alice.registerTopic({ project: 'p', topic: 'src', subject: 'S', holder: 'thr-alice' });
  await asBob.registerTopic({ project: 'p', topic: 'work', subject: 'W', holder: 'thr-bob' });
  const q = await alice.send({ project: 'p', topic: 'work', level: 'assist', text: 'decide', from_project: 'p', from_topic: 'src' });
  await asBob.receive({ project: 'p', topic: 'work' });
  await alice.registerTopic({ project: 'p', topic: 'src', holder: 'thr-alice', close: true });
  await assert.rejects(asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'done' }), (e) => e.status === 410, 'a reply to the closed topic is refused');
  return { alice, asBob, q };
}

test('settle: a picked message of a sender with a closed topic is settled; its state is final (hotovo, settled)', async (t) => {
  const { alice, asBob, q } = await closedSender(t, 'st1');
  const r = await asBob.settle({ id: q.id, project: 'p', topic: 'work' });
  assert.deepEqual([r.answer_state, r.answer_origin, r.replied], ['hotovo', 'settled', true]);
  const st = await alice.message(q.id);
  assert.deepEqual([st.answer_state, st.answer_origin, st.answer_id], ['hotovo', 'settled', null]);
  assert.ok(st.answer_at);
  assert.equal((await alice.count({ thread: 'thr-alice' })).total, 0, 'nothing is delivered');
});

test('settle: a second settle (or one after a final reply) is refused with 409', async (t) => {
  const { asBob, q } = await closedSender(t, 'st2');
  await asBob.settle({ id: q.id, project: 'p', topic: 'work' });
  await assert.rejects(asBob.settle({ id: q.id, project: 'p', topic: 'work' }), (e) => e.status === 409 && e.data?.answer_origin === 'settled');
});

test('settle: a caller whose mailbox the message is not addressed to is refused with 403; an unpicked message 409', async (t) => {
  const { alice, asBob, q } = await closedSender(t, 'st3');
  await assert.rejects(alice.settle({ id: q.id, project: 'p', topic: 'src' }), (e) => e.status === 403);
  await assert.rejects(alice.settle({ id: q.id, thread: 'thr-alice' }), (e) => e.status === 403);
  assert.equal((await alice.message(q.id)).answer_state, null, 'a refused settle writes nothing');
  const q2 = await alice.send({ project: 'p', topic: 'work', level: 'assist', text: 'later' });
  await assert.rejects(asBob.settle({ id: q2.id, project: 'p', topic: 'work' }), (e) => e.status === 409 && e.data?.reason === 'not-picked');
});
