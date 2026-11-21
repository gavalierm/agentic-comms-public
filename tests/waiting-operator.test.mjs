import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeInstance, startInstance, makeClient, cleanup, runCli, api, addKey } from './helpers.mjs';
import { QUESTION_NOTICE_MS, WAIT_BASE_S } from '../src/server/service.mjs';

// Phase 2 of the reply protocol (P-061, V1): a recipient whose open turn waits for the operator's dialog cannot pick up,
// but lives: no `no reply`; the service writes a code answer `otázka` on the pending message (mail model 2:
// `code_answer`, the message stays in the inbox, the sender's wait restarts). An answer left `otázka` for 24 h gets one notice.

async function setup(t) {
  const a = makeInstance('wo');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = api(a);
  await alice.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob' });
  return { a, svc: sa.srv.service, alice, bob: api(a, addKey(a, 'bob')), box: { project: 'p', topic: 't', thread: 'thr-bob' } };
}

const send = (alice, extra = {}) => alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide', ttl: '60s', thread_id: 'thr-alice', ...extra });

test('waiting_operator on the open turn: the unpicked message gets the code answer `otázka`, stays in the inbox, not expired', async (t) => {
  const { alice, bob, svc } = await setup(t);
  const o = await bob.turn({ thread: 'thr-bob', state: 'open' });
  const flagged = await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o.turn.opened_at });
  assert.equal(flagged.turn.waiting_operator, true);
  const m = await send(alice);
  const at = Date.now() + 120_000;
  assert.deepEqual(svc.checkExpired(at), [], 'not expired');
  const st = await alice.message(m.id);
  assert.deepEqual([st.status, st.expired_at, st.code_answer, st.answer_state, st.wait_reason], ['pending', null, 'otázka', null, 'check']);
  assert.ok(st.code_answer_at);
  assert.ok(Date.parse(st.wait_due_at) - Date.parse(st.code_answer_at) === WAIT_BASE_S.code * 1000 * 15, 'the sender\'s wait rings once, at the end of the span (base × 15), as after an interim answer');
  assert.equal(svc.checkExpired(Date.now() + 120_000).length, 0, 'once');
  assert.deepEqual((await alice.count({ project: 'p', topic: 't' })).items.map((i) => [i.id, i.code_answer]), [[m.id, 'otázka']]);
  const awaiting = (await alice.obligations({ thread: 'thr-alice' })).awaiting.find((x) => x.id === m.id);
  assert.deepEqual([awaiting.status, awaiting.code_answer], ['pending', 'otázka']);
  assert.equal((await alice.events(0)).filter((e) => e.message_id === m.id && e.type !== 'assist').length, 0, 'no expired event');
});

test('without the flag the same message expires; the flag ends with the turn (Stop or keepalive close)', async (t) => {
  const { alice, bob, svc } = await setup(t);
  const m1 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [m1.id]);
  const s1 = await alice.message(m1.id);
  assert.deepEqual([s1.status, typeof s1.expired_at, s1.code_answer, s1.wait_reason], ['pending', 'string', null, 'no-reply']);

  const o = await bob.turn({ thread: 'thr-bob', state: 'open' });
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o.turn.opened_at });
  // a crashed turn is closed by keepalive: the flag is gone, the next message expires normally
  await bob.turn({ thread: 'thr-bob', state: 'closed', reason: 'keepalive', opened_at: o.turn.opened_at });
  const m2 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [m2.id]);
  assert.equal((await alice.message(m2.id)).code_answer, null);
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

test('an answer left `otázka` for 24 h (code answer or interim reply): exactly one operator notice with a dedupe key; a final reply stops it', async (t) => {
  const { alice, bob, svc, box } = await setup(t);
  const o = await bob.turn({ thread: 'thr-bob', state: 'open' });
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o.turn.opened_at });
  const m = await send(alice);
  svc.checkExpired(Date.now() + 120_000);
  const at = Date.parse((await alice.message(m.id)).code_answer_at);
  assert.deepEqual(svc.checkOpenQuestions(at + QUESTION_NOTICE_MS - 60_000), [], 'not yet');
  assert.deepEqual(svc.checkOpenQuestions(at + QUESTION_NOTICE_MS + 1000), [m.id]);
  assert.deepEqual(svc.checkOpenQuestions(at + 3 * QUESTION_NOTICE_MS), [], 'only once');
  const notices = (await alice.notices()).filter((n) => n.kind === 'question-open');
  assert.equal(notices.length, 1);
  assert.equal(notices[0].dedupe_key, `question:${m.id}`);

  // an interim reply `otázka` left 24 h gets one too
  await bob.turn({ thread: 'thr-bob', state: 'closed', opened_at: o.turn.opened_at });
  const w = await send(alice, { text: 'interim' });
  await bob.receive({ ...box, ids: [w.id] });
  await bob.resolve({ ...box, id: w.id, state: 'otázka', text: 'operator decides' });
  const wat = Date.parse((await alice.message(w.id)).answer_at);
  assert.deepEqual(svc.checkOpenQuestions(wat + QUESTION_NOTICE_MS + 1000), [w.id]);

  // a question answered finally within 24 h gets no notice
  const o2 = await bob.turn({ thread: 'thr-bob', state: 'open' });
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o2.turn.opened_at });
  const q = await send(alice, { text: 'quick' });
  svc.checkExpired(Date.now() + 120_000);
  assert.equal((await alice.message(q.id)).code_answer, 'otázka');
  const done = await bob.resolve({ ...box, id: q.id, state: 'hotovo', text: 'decided' });
  const fin = await alice.message(q.id);
  assert.deepEqual([fin.status, fin.answer_id, fin.wait_due_at], ['done', done.answer_id, null]);
  assert.deepEqual(svc.checkOpenQuestions(Date.now() + 3 * QUESTION_NOTICE_MS), [], 'final reply: no notice');
});

// D-0015, D-0022: while the recipient discusses with the operator (turn `discussion`), a steer/assist it cannot pick up
// is answered by code: `busy` while its turn runs (delivered after the turn), `otázka` once it waits for the operator
// (dialog, or the last turn ended with a question); a blocker passes the discussion and expires as before.
test('discussion with the operator: an unpicked steer/assist gets the code answer `busy` in a running turn, `otázka` when waiting; a blocker expires', async (t) => {
  const { alice, bob, svc, box } = await setup(t);
  const o = await bob.turn({ thread: 'thr-bob', state: 'open', discussion: 'open' });
  assert.equal(o.turn.discussion, true);
  const steer = await send(alice, { level: 'steer', text: 'stop' });
  const assist = await send(alice);
  const blocker = await send(alice, { level: 'blocker', text: 'now' });
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [blocker.id], 'only the blocker expires');
  for (const m of [steer, assist]) {
    const st = await alice.message(m.id);
    assert.deepEqual([st.status, st.expired_at, st.code_answer, st.answer_state], ['pending', null, 'busy', null], m.level);
  }
  assert.deepEqual((await alice.count({ project: 'p', topic: 't' })).items.map((i) => [i.id, i.code_answer]),
    [[steer.id, 'busy'], [assist.id, 'busy'], [blocker.id, null]], 'all three stay in the inbox');
  // A reply cannot carry `busy` (it is not a reply state).
  await assert.rejects(bob.resolve({ ...box, id: assist.id, state: 'busy', text: 'x' }), (e) => e.status === 400);

  // The same open turn in the operator's dialog: `otázka`.
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o.turn.opened_at });
  const m1 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), []);
  assert.equal((await alice.message(m1.id)).code_answer, 'otázka');

  // The turn ends with a question to the operator: the discussion stays open over the closed turn.
  const c = await bob.turn({ thread: 'thr-bob', state: 'closed', opened_at: o.turn.opened_at, discussion: 'open' });
  assert.equal(c.turn.discussion, true);
  const m2 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), []);
  assert.equal((await alice.message(m2.id)).code_answer, 'otázka');
  // A turn without the field (older workflow) does not discuss: the message expires as before.
  await bob.turn({ thread: 'thr-bob', state: 'open' });
  const m3 = await send(alice);
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [m3.id]);
  await assert.rejects(bob.turn({ thread: 'thr-bob', state: 'open', discussion: 'maybe' }), (e) => e.status === 400 && /discussion/.test(e.json.error));
  // The recipient picks the code-answered messages up and answers them like any other.
  const got = await bob.receive(box);
  assert.ok(got.some((x) => x.id === assist.id && x.status === 'picked'));
});

test('discussion end emits one `discussion-closed` event; a keepalive close keeps the discussion', async (t) => {
  const { alice, bob, svc } = await setup(t);
  const closedEvents = async () => (await alice.events(0)).filter((e) => e.type === 'discussion-closed');
  const o1 = await bob.turn({ thread: 'thr-bob', state: 'open', discussion: 'open' });
  assert.equal(svc.store.turn('thr-bob').discussion, true);
  // interrupted turn: the wake module's close keeps the discussion, no event
  await bob.turn({ thread: 'thr-bob', state: 'closed', reason: 'keepalive', opened_at: o1.turn.opened_at });
  assert.equal(svc.store.turn('thr-bob').discussion, true);
  assert.equal((await closedEvents()).length, 0);
  // the turn ends with hotovo: the discussion is closed, one event for the holder
  const o2 = await bob.turn({ thread: 'thr-bob', state: 'open', discussion: 'open' });
  await bob.turn({ thread: 'thr-bob', state: 'closed', opened_at: o2.turn.opened_at, discussion: 'closed' });
  assert.equal(svc.store.turn('thr-bob').discussion, false);
  const ev = await closedEvents();
  assert.equal(ev.length, 1);
  assert.deepEqual([ev[0].holder, ev[0].project], ['thr-bob', null]);
  // the dialog alone (older workflow): its end closes the discussion too
  const o3 = await bob.turn({ thread: 'thr-bob', state: 'open' });
  await bob.turn({ thread: 'thr-bob', waiting_operator: true, opened_at: o3.turn.opened_at });
  await bob.turn({ thread: 'thr-bob', waiting_operator: false, opened_at: o3.turn.opened_at });
  assert.equal((await closedEvents()).length, 2);
});

// --- CLI ----------------------------------------------------------------------------------------------

test('CLI: turn --waiting-operator true|false sets the flag of the open turn', async (t) => {
  const { a, alice, svc } = await setup(t);
  const base = ['--data-dir', a.dir, '--key', a.key, '--thread', 'thr-bob', '--no-start'];
  await makeClient(a, { thread: 'thr-bob' }).turn({ thread: 'thr-bob', state: 'open' });
  const on = await runCli(['turn', '--waiting-operator', 'true', ...base]);
  assert.equal(on.code, 0, on.err);
  assert.equal(svc.store.turn('thr-bob').waiting_operator, true);
  const m = await send(alice);
  svc.checkExpired(Date.now() + 120_000);
  assert.deepEqual([(await alice.message(m.id)).status, (await alice.message(m.id)).code_answer], ['pending', 'otázka']);
  assert.equal((await runCli(['turn', '--waiting-operator', 'false', ...base])).code, 0);
  assert.equal(svc.store.turn('thr-bob').waiting_operator, false);
  assert.notEqual((await runCli(['turn', '--waiting-operator', 'maybe', ...base])).code, 0);
});

test('discussion end emits one `discussion-closed` event; a keepalive close keeps the discussion; CLI --discussion', async (t) => {
  const { a, alice, bob, svc } = await setup(t);
  const closedEvents = async () => (await alice.events(0)).filter((e) => e.type === 'discussion-closed');
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
