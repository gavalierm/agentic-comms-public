import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeInstance, startInstance, makeClient, cleanup, runCli } from './helpers.mjs';
import { QUESTION_NOTICE_MS } from '../src/server/service.mjs';

// Phase 2 of the reply protocol (P-061, V1): a recipient whose open turn waits for the operator's dialog cannot pick up,
// but lives: no `no reply`, the sender is told `otázka` (origin code). An answer left `otázka` for 24 h gets one notice.

async function setup(t) {
  const a = makeInstance('wo');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = makeClient(a, { thread: 'thr-alice' });
  await alice.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob' });
  return { a, svc: sa.srv.service, alice, bob: makeClient(a, { thread: 'thr-bob' }), asBob: makeClient(a, { key: a.store.addKey('bob').key, thread: 'thr-bob' }) };
}

const send = (alice) => alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide', ttl: '60s' });

test('waiting_operator on the open turn: the unpicked message is answered `otázka` by code, not expired', async (t) => {
  const { alice, bob, svc } = await setup(t);
  const o = await bob.turn({ thread: 'thr-bob', state: 'open' });
  const flagged = await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o.turn.opened_at });
  assert.equal(flagged.turn.waiting_operator, true);
  const m = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [], 'not expired');
  const st = await alice.message(m.id);
  assert.deepEqual([st.outcome, st.answer_state, st.answer_origin, st.replied], [null, 'otázka', 'code', true]);
  assert.equal(svc.checkExpired(Date.now() + 120_000).length, 0, 'once');
  // the sender is rung once, as a no-reply bell the hook turns into the `otázka` line
  const ev = (await alice.events(0, 0)).events.filter((e) => e.type === 'expired');
  assert.equal(ev.length, 1);
  assert.equal(ev[0].answer_state, 'otázka');
  assert.equal(ev[0].holder, 'thr-alice');
});

test('without the flag the same message expires; the flag ends with the turn (Stop or keepalive close)', async (t) => {
  const { alice, bob, svc } = await setup(t);
  const m1 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [m1.id]);
  assert.equal((await alice.message(m1.id)).outcome, 'expired');

  const o = await bob.turn({ thread: 'thr-bob', state: 'open' });
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o.turn.opened_at });
  // a crashed turn is closed by keepalive: the flag is gone, the next message expires normally
  await bob.turn({ thread: 'thr-bob', state: 'closed', reason: 'keepalive', opened_at: o.turn.opened_at });
  const m2 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [m2.id]);
  assert.equal((await alice.message(m2.id)).answer_state, null);
  // a flag for a turn that is not open (or not the current one) is ignored
  assert.equal((await bob.turn({ thread: 'thr-bob', waiting_operator: true })).ignored, true);
  const o2 = await bob.turn({ thread: 'thr-bob', state: 'open' });
  assert.equal((await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: 'old' })).ignored, true);
  // PostToolUse clears it
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o2.turn.opened_at });
  assert.equal((await bob.turn({ thread: 'thr-bob', waiting_operator: false })).turn.waiting_operator, false);
  const m3 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [m3.id]);
});

test('CLI: turn --waiting-operator true|false sets the flag of the open turn', async (t) => {
  const { a, alice, svc } = await setup(t);
  const base = ['--data-dir', a.dir, '--key', a.key, '--thread', 'thr-bob', '--no-start'];
  await makeClient(a, { thread: 'thr-bob' }).turn({ thread: 'thr-bob', state: 'open' });
  const on = await runCli(['turn', '--waiting-operator', 'true', ...base]);
  assert.equal(on.code, 0, on.err);
  assert.equal(svc.store.turn('thr-bob').waiting_operator, true);
  const m = await send(alice);
  svc.checkExpired(Date.now() + 120_000);
  assert.equal((await alice.message(m.id)).answer_state, 'otázka');
  assert.equal((await runCli(['turn', '--waiting-operator', 'false', ...base])).code, 0);
  assert.equal(svc.store.turn('thr-bob').waiting_operator, false);
  assert.notEqual((await runCli(['turn', '--waiting-operator', 'maybe', ...base])).code, 0);
});

test('an answer left `otázka` for 24 h: exactly one operator notice with a dedupe key; a final reply stops it', async (t) => {
  const { alice, bob, asBob, svc } = await setup(t);
  const o = await bob.turn({ thread: 'thr-bob', state: 'open' });
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o.turn.opened_at });
  const m = await send(alice);
  svc.checkExpired(Date.now() + 120_000);
  const at = Date.parse((await alice.message(m.id)).answer_at);
  assert.deepEqual(svc.checkOpenQuestions(at + QUESTION_NOTICE_MS - 60_000), [], 'not yet');
  assert.deepEqual(svc.checkOpenQuestions(at + QUESTION_NOTICE_MS + 1000), [m.id]);
  assert.deepEqual(svc.checkOpenQuestions(at + 3 * QUESTION_NOTICE_MS), [], 'only once');
  const notices = (await alice.notices()).notices.filter((n) => n.kind === 'question-open');
  assert.equal(notices.length, 1);
  assert.equal(notices[0].dedupe_key, `question:${m.id}`);

  // a question answered finally within 24 h gets no notice
  const q = await send(alice);
  svc.checkExpired(Date.now() + 120_000);
  assert.equal((await alice.message(q.id)).answer_state, 'otázka');
  const done = await asBob.send({ reply_to: q.id, reply_state: 'hotovo', text: 'decided' });
  assert.equal((await alice.message(q.id)).answer_id, done.id);
  assert.deepEqual(svc.checkOpenQuestions(Date.now() + 3 * QUESTION_NOTICE_MS), [], 'final reply: no notice');
});

// D-0015, D-0022: while the recipient discusses with the operator (turn `discussion`), a steer/assist it cannot pick up
// is answered `otázka` by code, as in the dialog; a blocker passes the discussion and expires as before.
test('discussion with the operator: an unpicked steer/assist is answered `otázka` by code, a blocker expires', async (t) => {
  const { alice, bob, svc } = await setup(t);
  const o = await bob.turn({ thread: 'thr-bob', state: 'open', discussion: 'open' });
  assert.equal(o.turn.discussion, true);
  const steer = await alice.send({ project: 'p', topic: 't', level: 'steer', text: 'stop', ttl: '60s' });
  const assist = await send(alice);
  const blocker = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'now', ttl: '60s' });
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [blocker.id], 'only the blocker expires');
  for (const m of [steer, assist]) {
    const st = await alice.message(m.id);
    assert.deepEqual([st.outcome, st.answer_state, st.answer_origin], [null, 'otázka', 'code'], m.level);
  }
  const ev = (await alice.events(0, 0)).events.filter((e) => e.type === 'expired');
  assert.deepEqual(ev.map((e) => [e.message_id, e.answer_state || null]), [[steer.id, 'otázka'], [assist.id, 'otázka'], [blocker.id, null]]);

  // The turn ends with a question to the operator: the discussion stays open over the closed turn.
  const c = await bob.turn({ thread: 'thr-bob', state: 'closed', opened_at: o.turn.opened_at, discussion: 'open' });
  assert.equal(c.turn.discussion, true);
  const m2 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), []);
  assert.equal((await alice.message(m2.id)).answer_state, 'otázka');
  // A turn without the field (older workflow) does not discuss: the message expires as before.
  await bob.turn({ thread: 'thr-bob', state: 'open' });
  const m3 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [m3.id]);
  await assert.rejects(bob.turn({ thread: 'thr-bob', state: 'open', discussion: 'maybe' }), /discussion/);
});

test('discussion end emits one `discussion-closed` event; a keepalive close keeps the discussion; CLI --discussion', async (t) => {
  const { a, alice, bob, svc } = await setup(t);
  const closedEvents = async () => (await alice.events(0, 0)).events.filter((e) => e.type === 'discussion-closed');
  const base = ['--data-dir', a.dir, '--key', a.key, '--thread', 'thr-bob', '--no-start'];
  const open = await runCli(['turn', '--state', 'open', '--discussion', 'open', ...base]);
  assert.equal(open.code, 0, open.err);
  assert.equal(svc.store.turn('thr-bob').discussion, true);
  const openedAt = svc.store.turn('thr-bob').opened_at;
  // interrupted turn: the wake module's close keeps the discussion, no event
  await bob.turn({ thread: 'thr-bob', state: 'closed', reason: 'keepalive', opened_at: openedAt });
  assert.equal(svc.store.turn('thr-bob').discussion, true);
  assert.equal((await closedEvents()).length, 0);
  // the turn ends with hotovo: the discussion is closed, one event for the holder
  const o2 = await bob.turn({ thread: 'thr-bob', state: 'open', discussion: 'open' });
  const close = await runCli(['turn', '--state', 'closed', '--opened-at', o2.turn.opened_at, '--discussion', 'closed', ...base]);
  assert.equal(close.code, 0, close.err);
  assert.equal(svc.store.turn('thr-bob').discussion, false);
  const ev = await closedEvents();
  assert.equal(ev.length, 1);
  assert.deepEqual([ev[0].holder, ev[0].project], ['thr-bob', null]);
  // the dialog alone (older workflow): its end closes the discussion too
  const o3 = await bob.turn({ thread: 'thr-bob', state: 'open' });
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o3.turn.opened_at });
  await bob.turn({ thread: 'thr-bob', waiting_operator: false, opened_at: o3.turn.opened_at });
  assert.equal((await closedEvents()).length, 2);
  assert.notEqual((await runCli(['turn', '--state', 'open', '--discussion', 'maybe', ...base])).code, 0);
});
