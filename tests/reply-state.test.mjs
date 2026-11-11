import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeInstance, startInstance, makeClient, cleanup, runCli } from './helpers.mjs';

// Reply protocol (incident 2026-11-11: three replies to one message, the last a new request): a reply carries a
// state (hotovo | otázka | čakám), a message has one final reply, a reply is never answered by a reply.

async function pair(t, name) {
  const a = makeInstance(name);
  const bob = a.store.addKey('bob');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = makeClient(a, { thread: 'thr-alice' });
  const asBob = makeClient(a, { key: bob.key, thread: 'thr-bob' });
  await alice.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob' });
  return { a, bob, alice, asBob };
}

test('reply state: a second reply after the final one is refused (409) and names the final reply', async (t) => {
  const { alice, asBob } = await pair(t, 'rs1');
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide' });
  const first = await asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'decision A' });
  assert.equal(first.reply_state, 'hotovo');
  await assert.rejects(asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'correction: decision B' }),
    (e) => e.status === 409 && e.message.includes(first.id) && /--cancels/.test(e.message));
  await assert.rejects(asBob.send({ reply_to: q.id, reply_state: 'otázka', text: 'on second thought' }), (e) => e.status === 409);
  assert.equal((await alice.receive({ thread: 'thr-alice' })).messages.length, 1, 'the sender holds exactly one reply');
});

test('reply state: a reply to a reply is refused (400); the request goes as a new message', async (t) => {
  const { alice, asBob } = await pair(t, 'rs2');
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide' });
  const r = await asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'decision A; also do X' });
  await assert.rejects(alice.send({ reply_to: r.id, reply_state: 'hotovo', text: 'X done' }), (e) => e.status === 400 && /na odpoveď sa neodpovedá/.test(e.message));
  // an interim reply is not answered by a reply either
  const q2 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'second' });
  const r2 = await asBob.send({ reply_to: q2.id, reply_state: 'otázka', text: 'operator decides' });
  await assert.rejects(alice.send({ reply_to: r2.id, text: 'hurry' }), (e) => e.status === 400);
});

test('reply state: stored on the reply, shown in receive, count items and message; no state (old client) is final', async (t) => {
  const { alice, asBob } = await pair(t, 'rs3');
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide' });
  assert.equal((await alice.message(q.id)).answer_state, null);
  await assert.rejects(asBob.send({ reply_to: q.id, reply_state: 'neskôr', text: 'x' }), (e) => e.status === 400 && /hotovo\|otázka\|čakám/.test(e.message));
  await assert.rejects(alice.send({ project: 'p', topic: 't', level: 'assist', reply_state: 'hotovo', text: 'x' }), (e) => e.status === 400);

  const r = await asBob.send({ reply_to: q.id, reply_state: 'otázka', text: 'operator decides; I recommend A' });
  assert.equal(r.reply_state, 'otázka');
  const st = await alice.message(q.id);
  assert.deepEqual([st.replied, st.answer_state, st.answer_id], [true, 'otázka', r.id]);
  assert.ok(st.answer_at);
  const item = (await alice.count({ thread: 'thr-alice' })).items.find((i) => i.id === r.id);
  assert.deepEqual([item.reply_to, item.reply_state], [q.id, 'otázka']);
  const body = (await alice.receive({ thread: 'thr-alice' })).messages.find((m) => m.id === r.id);
  assert.equal(body.reply_state, 'otázka');
  // the original in the recipient's mailbox shows its answer state without the body
  assert.equal((await asBob.count({ project: 'p', topic: 't' })).items.find((i) => i.id === q.id).answer_state, 'otázka');

  // an interim state does not close the exchange: the final reply follows and closes it
  const fin = await asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'operator chose A' });
  assert.equal((await alice.message(q.id)).answer_state, 'hotovo');
  assert.equal((await alice.message(q.id)).answer_id, fin.id);
  await assert.rejects(asBob.send({ reply_to: q.id, reply_state: 'čakám', text: 'again' }), (e) => e.status === 409);

  // old client: a reply without a state is the final one
  const q2 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'old client' });
  const old = await asBob.send({ reply_to: q2.id, text: 'answer' });
  assert.equal(old.reply_state, 'hotovo');
  assert.equal((await alice.message(q2.id)).answer_state, 'hotovo');
  await assert.rejects(asBob.send({ reply_to: q2.id, text: 'more' }), (e) => e.status === 409);
});

test('reply state: the first interim state and the final reply ring the sender; a move between interim states does not', async (t) => {
  const { alice, asBob } = await pair(t, 'rs4');
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide' });
  const start = (await alice.events(0, 0)).next;
  await asBob.send({ reply_to: q.id, reply_state: 'otázka', text: 'operator decides' });
  const e1 = await alice.events(start, 0);
  assert.deepEqual(e1.events.map((e) => [e.type, e.holder, e.reply_state]), [['reply', 'thr-alice', 'otázka']]);
  const w = await asBob.send({ reply_to: q.id, reply_state: 'čakám', wait_for: 'build 42', text: 'operator decided, build runs' });
  assert.equal((await alice.events(e1.next, 0)).events.length, 0, 'interim to interim does not ring');
  assert.equal((await alice.message(q.id)).answer_state, 'čakám');
  assert.equal((await alice.receive({ thread: 'thr-alice' })).messages.find((m) => m.id === w.id).wait_for, 'build 42');
  await asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'built' });
  assert.deepEqual((await alice.events(e1.next, 0)).events.map((e) => [e.type, e.reply_state]), [['reply', 'hotovo']]);
});

test('cancels: a new message withdraws the sender\'s own final reply; anyone else, a non-final reply and a second cancel are refused', async (t) => {
  const { a, alice, asBob } = await pair(t, 'rs5');
  await asBob.registerTopic({ project: 'p', topic: 'home', subject: 'S', holder: 'thr-alice' });
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide', from_project: 'p', from_topic: 'home' });
  const interim = await asBob.send({ reply_to: q.id, reply_state: 'otázka', text: 'operator decides' });
  const send = (c, cancels, extra = {}) => c.send({ project: 'p', topic: 'home', level: 'assist', text: 'decision changed to B', cancels, ...extra });
  await assert.rejects(send(asBob, interim.id), (e) => e.status === 400, 'an interim reply is not cancelled, the final one replaces it');
  const fin = await asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'decision A' });
  await assert.rejects(send(asBob, 'missing-id'), (e) => e.status === 404);
  await assert.rejects(send(asBob, q.id), (e) => e.status === 400, 'only a reply can be cancelled');
  const carol = makeClient(a, { key: a.store.addKey('carol').key, thread: 'thr-carol' });
  await assert.rejects(send(carol, fin.id), (e) => e.status === 403);
  await assert.rejects(asBob.send({ reply_to: q.id, text: 'x', cancels: fin.id }), (e) => e.status === 400, 'cancels is not a reply');
  assert.equal((await alice.message(fin.id)).cancelled_by, null, 'refused attempts change nothing');

  const change = await send(asBob, fin.id);
  assert.equal((await alice.message(fin.id)).cancelled_by, change.id);
  const got = (await alice.receive({ project: 'p', topic: 'home' })).messages.find((m) => m.id === change.id);
  assert.equal(got.cancels, fin.id);
  assert.equal((await alice.count({ project: 'p', topic: 'home' })).items.find((i) => i.id === change.id).cancels, fin.id);
  await assert.rejects(send(asBob, fin.id), (e) => e.status === 409);
  // the cancelling message is a message of its own: it takes a reply with a state
  assert.equal((await alice.send({ reply_to: change.id, reply_state: 'hotovo', text: 'B noted' })).reply_state, 'hotovo');
});

test('cli: send --reply-to --state / --wait-for / --cancels reach the service; an unknown state is refused', async (t) => {
  const { a, bob, alice } = await pair(t, 'rs6');
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide' });
  const cli = (args) => runCli([...args, '--data-dir', a.dir, '--key', bob.key, '--thread', 'thr-bob', '--no-start']);
  const bad = await cli(['send', '--reply-to', q.id, '--state', 'neskôr', '--text', 'x']);
  assert.notEqual(bad.code, 0);
  const w = await cli(['send', '--reply-to', q.id, '--state', 'čakám', '--wait-for', 'build 42', '--text', 'runs']);
  assert.equal(w.code, 0, w.err);
  assert.equal(w.json.reply_state, 'čakám');
  const fin = await cli(['send', '--reply-to', q.id, '--state', 'hotovo', '--text', 'built']);
  assert.equal(fin.json.reply_state, 'hotovo');
  const again = await cli(['send', '--reply-to', q.id, '--state', 'hotovo', '--text', 'fix']);
  assert.notEqual(again.code, 0);
  assert.match(again.err, /"status":\s*409/);
  await makeClient(a, { key: bob.key }).registerTopic({ project: 'p', topic: 'home', subject: 'S', holder: 'thr-alice' });
  const c = await cli(['send', '--project', 'p', '--topic', 'home', '--level', 'assist', '--cancels', fin.json.id, '--text', 'changed']);
  assert.equal(c.code, 0, c.err);
  assert.equal((await alice.message(fin.json.id)).cancelled_by, c.json.id);
});
