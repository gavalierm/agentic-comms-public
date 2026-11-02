import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, rawRequest, cleanup, sleep, runCli } from './helpers.mjs';

test('two instances on one machine do not see each other', async (t) => {
  const a = makeInstance('a');
  const b = makeInstance('b');
  const sa = await startInstance(a);
  const sb = await startInstance(b);
  t.after(async () => { await sa.stop(); await sb.stop(); cleanup(a.dir, b.dir); });

  assert.notEqual(a.instanceId, b.instanceId);
  const ca = makeClient(a);
  const cb = makeClient(b);
  await ca.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  await cb.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  await ca.send({ project: 'p', topic: 't', level: 'assist', text: 'only in A' });

  const inA = await ca.receive({ project: 'p', topic: 't' });
  const inB = await cb.receive({ project: 'p', topic: 't' });
  assert.equal(inA.messages.length, 1);
  assert.equal(inB.messages.length, 0);

  // Key of A is not valid in B.
  const cross = makeClient(b, { key: a.key });
  await assert.rejects(cross.receive({ project: 'p', topic: 't' }), (e) => e.code === 'REJECTED' && e.status === 401);
  assert.equal(fs.readdirSync(path.join(b.dir, 'messages')).length, 0);
});

test('health returns instance_id; client rejects a different instance', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });

  const h = await makeClient(a).ensureRunning();
  assert.equal(h.ok, true);
  assert.equal(h.instance_id, a.instanceId);

  const wrong = makeClient(a, { instanceId: 'deadbeef00000000' });
  await assert.rejects(wrong.ensureRunning(), (e) => e.code === 'INSTANCE_MISMATCH');
});

test('sender is derived from the key, never from the body', async (t) => {
  const a = makeInstance('a', 'alice');
  const bob = a.store.addKey('bob');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });

  const alice = makeClient(a);
  await alice.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const asBob = makeClient(a, { key: bob.key });
  await asBob.send({ project: 'p', topic: 't', level: 'assist', text: 'hi', sender: 'alice' });

  const r = await alice.receive({ project: 'p', topic: 't' });
  assert.equal(r.messages.length, 1);
  assert.equal(r.messages[0].sender, 'bob');
  const file = JSON.parse(fs.readFileSync(path.join(a.dir, 'messages', `${r.messages[0].id}.json`), 'utf8'));
  assert.equal(file.sender, 'bob');
});

test('send without level is rejected (400) and nothing is stored', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  await makeClient(a).registerTopic({ project: 'p', topic: 't', subject: 'S' });

  const res = await rawRequest(a.socket, 'POST', '/send', {
    body: { project: 'p', topic: 't', text: 'no level' }, headers: { Authorization: `Bearer ${a.key}` },
  });
  assert.equal(res.status, 400);
  assert.match(res.json.error, /level/);
  const bad = await rawRequest(a.socket, 'POST', '/send', {
    body: { project: 'p', topic: 't', level: 'urgent', text: 'x' }, headers: { Authorization: `Bearer ${a.key}` },
  });
  assert.equal(bad.status, 400);
  assert.equal(fs.readdirSync(path.join(a.dir, 'messages')).length, 0);
});

test('message to an unregistered topic creates a notice and does not create the topic', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);

  const r = await c.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'lost?' });
  assert.equal(r.state, 'held');
  assert.ok(r.notice_id);
  assert.equal(fs.readdirSync(path.join(a.dir, 'topics')).length, 0);

  const n = await c.notices();
  assert.equal(n.notices.length, 1);
  assert.equal(n.notices[0].kind, 'unregistered-topic');
  assert.match(n.notices[0].title, /Nová téma\? \/ presmerovať/);
  assert.deepEqual(n.notices[0].options.map((o) => o.id), ['redirect', 'deliver', 'discard']);

  // Not receivable until the operator decides; registering the topic alone does not deliver.
  assert.equal((await c.receive({ project: 'p', topic: 'nowhere' })).messages.length, 0);
  await c.registerTopic({ project: 'p', topic: 'nowhere', subject: 'Now exists' });
  assert.equal((await c.receive({ project: 'p', topic: 'nowhere' })).messages.length, 0);
  await c.decide({ notice_id: r.notice_id, decision: 'deliver' });
  assert.equal((await c.receive({ project: 'p', topic: 'nowhere' })).messages.length, 1);
});

test('register-topic is idempotent and requires a subject', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  const r1 = await c.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  const r2 = await c.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-2', takeover: true });
  assert.equal(r1.created, true);
  assert.equal(r2.created, false);
  assert.equal(r2.topic.holder, 'thr-2');
  assert.equal(r2.topic.registered_at, r1.topic.registered_at);
  await assert.rejects(c.registerTopic({ project: 'p', topic: 'x' }), (e) => e.status === 400);
});

test('reply goes to the sender thread; blocker/assist replies emit wake events', async (t) => {
  const a = makeInstance('a');
  const bob = a.store.addKey('bob');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = makeClient(a, { thread: 'thr-alice' });
  const asBob = makeClient(a, { key: bob.key, thread: 'thr-bob' });
  await alice.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob' });

  const q = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'need answer' });
  const ev1 = await alice.events(0, 0);
  assert.equal(ev1.events.length, 1);
  assert.equal(ev1.events[0].type, 'blocker');
  assert.equal(ev1.events[0].holder, 'thr-bob');

  const inbox = await asBob.receive({ project: 'p', topic: 't' });
  assert.equal(inbox.messages[0].id, q.id);
  // a reply to your own message is refused (it would come back to the same thread)
  await assert.rejects(alice.send({ reply_to: q.id, level: 'assist', text: 'self' }), /vlastná/);
  // operator 2026-11-02: a reply never lowers the urgency of a blocker; it has no deadline of its own
  const sent = await asBob.send({ reply_to: q.id, level: 'assist', text: 'here you go' });
  assert.equal(sent.level, 'blocker');
  assert.equal(sent.level_raised_from, 'assist');
  assert.equal(sent.expires_at, null);

  const replies = await alice.receive({ thread: 'thr-alice' });
  assert.equal(replies.messages.length, 1);
  assert.equal(replies.messages[0].level, 'blocker');
  assert.equal((await alice.notices()).notices.filter((n) => n.message?.id === sent.id || n.message_id === sent.id).length, 0, 'a reply makes no operator notice');
  assert.equal(replies.messages[0].to_thread, 'thr-alice');
  assert.equal(replies.messages[0].reply_to, q.id);

  const ev2 = await alice.events(ev1.next, 0);
  assert.equal(ev2.events.length, 1);
  assert.equal(ev2.events[0].type, 'reply');
  assert.equal(ev2.events[0].holder, 'thr-alice');

  // ack removes from pending; long-poll returns [] after timeout
  await alice.ack([replies.messages[0].id]);
  assert.equal((await alice.receive({ thread: 'thr-alice' })).messages.filter((m) => m.sender === 'comms').length, 0);
  const ev3 = await alice.events(ev2.next, 100);
  assert.equal(ev3.events.length, 0);
});

test('second server for the same instance reports ALREADY_RUNNING', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  await assert.rejects(startInstance(a), (e) => e.code === 'ALREADY_RUNNING');
});

// --- hotfix 0.2.1: deadlines (TTL), expired, message state, count ids, turns ---------

test('send: ttl defaults per level (blocker/steer 5m, assist 30m), explicit --ttl shortens; 0, over the maximum, info and an unknown level are 400', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const exp = (r) => (Date.parse(r.expires_at) - Date.parse(JSON.parse(fs.readFileSync(path.join(a.dir, 'messages', `${r.id}.json`), 'utf8')).created_at)) / 1000;

  const assist = await c.send({ project: 'p', topic: 't', level: 'assist', text: 'a' });
  assert.equal(exp(assist), 30 * 60);
  assert.equal(exp(await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'b' })), 5 * 60);
  assert.equal(exp(await c.send({ project: 'p', topic: 't', level: 'steer', text: 's' })), 5 * 60);
  assert.equal(exp(await c.send({ project: 'p', topic: 't', level: 'assist', text: 'c', ttl: '90s' })), 90);
  assert.equal(exp(await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'd', ttl: 120 })), 120);
  assert.equal(exp(await c.send({ project: 'p', topic: 't', level: 'steer', text: 'e', ttl: '1m' })), 60);
  assert.equal((await c.send({ project: 'p', topic: 't', level: 'assist', text: 'f', reply_to: null })).expires_at !== null, true);
  const stored = JSON.parse(fs.readFileSync(path.join(a.dir, 'messages', `${assist.id}.json`), 'utf8'));
  assert.deepEqual([stored.ttl_s, stored.expired_at ?? null, stored.picked_at ?? null], [30 * 60, null, null]);

  const before = fs.readdirSync(path.join(a.dir, 'messages')).length;
  const info = await rawRequest(a.socket, 'POST', '/send', { body: { project: 'p', topic: 't', level: 'info', text: 'x', ttl: '5m' }, headers: { Authorization: `Bearer ${a.key}` } });
  assert.equal(info.status, 400);
  assert.match(info.json.error, /zrušená/);
  assert.match(info.json.error, /assist/);
  await assert.rejects(c.send({ project: 'p', topic: 't', level: 'info', text: 'x' }), (e) => e.status === 400 && /assist/.test(e.message));
  await assert.rejects(c.send({ project: 'p', topic: 't', level: 'urgent', text: 'x' }), (e) => e.status === 400 && /blocker\|steer\|assist/.test(e.message));
  await assert.rejects(c.send({ project: 'p', topic: 't', level: 'assist', text: 'x', ttl: 'soon' }), (e) => e.status === 400);
  await assert.rejects(c.send({ project: 'p', topic: 't', level: 'assist', text: 'x', ttl: '0' }), (e) => e.status === 400 && /kladná/.test(e.message));
  await assert.rejects(c.send({ project: 'p', topic: 't', level: 'blocker', text: 'x', ttl: '10m' }), (e) => e.status === 400 && /maximum 5m/.test(e.message));
  await assert.rejects(c.send({ project: 'p', topic: 't', level: 'steer', text: 'x', ttl: '6m' }), (e) => e.status === 400);
  await assert.rejects(c.send({ project: 'p', topic: 't', level: 'assist', text: 'x', ttl: '31m' }), (e) => e.status === 400);
  assert.equal(fs.readdirSync(path.join(a.dir, 'messages')).length, before);
  assert.equal(exp(await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'g', ttl: '5m' })), 300);
  assert.equal(exp(await c.send({ project: 'p', topic: 't', level: 'assist', text: 'h', ttl: '30m' })), 1800);
});

test('ttl defaults come from instance.json (ttl.blocker / ttl.assist); 0 falls back to the default, over the maximum to the maximum', async (t) => {
  const a = makeInstance('a');
  a.store.saveInstance({ ...a.store.instance(), ttl: { blocker: '1m', assist: 0 } });
  const over = makeInstance('o');
  over.store.saveInstance({ ...over.store.instance(), ttl: { blocker: '3h' } });
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const b = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'b' });
  assert.ok(Math.abs(Date.parse(b.expires_at) - Date.now() - 60_000) < 5000);
  const a2 = await c.send({ project: 'p', topic: 't', level: 'assist', text: 'a' });
  assert.ok(Math.abs(Date.parse(a2.expires_at) - Date.now() - 30 * 60_000) < 5000);
  const so = await startInstance(over);
  t.after(async () => { await so.stop(); cleanup(over.dir); });
  const co = makeClient(over);
  await co.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const b2 = await co.send({ project: 'p', topic: 't', level: 'blocker', text: 'b' });
  assert.ok(Math.abs(Date.parse(b2.expires_at) - Date.now() - 5 * 60_000) < 5000);
});

test('a held message expires like a pending one: the sender learns, the notice stays decidable (fable audit M2)', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a, { thread: 'thr-a' });
  const held = await c.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'q', ttl: '60s' });
  assert.equal(held.state, 'held');
  const svc = sa.srv.service;
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [held.id]);
  const m = await c.message(held.id);
  assert.deepEqual([m.state, m.outcome], ['held', 'expired']);
  assert.equal((await c.notices()).notices[0].state, 'open');
});

test('expiry: no mail, no notice; outcome expired and one expired event for the sender; replied/acked do not expire', async (t) => {
  const a = makeInstance('a');
  const bob = a.store.addKey('bob');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = makeClient(a, { thread: 'thr-alice', machine: 'box-a' });
  const asBob = makeClient(a, { key: bob.key });
  await alice.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob', holder_machine: 'box-b' });
  await alice.registerTopic({ project: 'q', topic: 'u', subject: 'Src', holder: 'thr-q', holder_mode: 'auto', holder_machine: 'box-q' });

  const lonely = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'SECRET anyone?', ttl: '60s', from_project: 'q', from_topic: 'u' });
  const answered = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'answered', ttl: '60s' });
  const acked = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'acked', ttl: '60s' });
  const noThread = await asBob.send({ project: 'p', topic: 't', level: 'assist', text: 'from topic', ttl: '30m', from_project: 'q', from_topic: 'u' });
  const nowhere = await asBob.send({ project: 'p', topic: 't', level: 'blocker', text: 'no way back', ttl: '60s' });
  await asBob.send({ reply_to: answered.id, level: 'assist', text: 'ok' });
  await asBob.ack([acked.id]);
  const { next } = await alice.events('latest', 0);
  const nMessages = fs.readdirSync(path.join(a.dir, 'messages')).length;
  const nNotices = (await alice.notices(true)).notices.length;

  const svc = sa.srv.service;
  assert.deepEqual(svc.checkExpired(Date.now() + 30_000), [], 'not yet');
  const expired = svc.checkExpired(Date.now() + 31 * 60_000);
  assert.deepEqual(expired.sort(), [lonely.id, noThread.id, nowhere.id].sort());
  assert.deepEqual(svc.checkExpired(Date.now() + 10 * 3600_000), [], 'the second check does nothing');
  assert.equal(fs.readdirSync(path.join(a.dir, 'messages')).length, nMessages, 'no mail from comms');
  assert.equal((await alice.notices(true)).notices.length, nNotices, 'no operator notice');
  assert.equal((await alice.receive({ thread: 'thr-alice' })).messages.filter((m) => m.sender === 'comms').length, 0);

  // Events: one `expired` per message with a way back, for the sender, never for the recipient; no body.
  const ev = (await alice.events(next, 0)).events;
  assert.deepEqual(ev.map((e) => e.type), ['expired', 'expired']);
  const e1 = ev.find((e) => e.message_id === lonely.id);
  assert.deepEqual([e1.holder, e1.holder_machine, e1.holder_mode, e1.level, e1.project, e1.topic], ['thr-alice', 'box-a', null, 'assist', 'p', 't']);
  const e2 = ev.find((e) => e.message_id === noThread.id);
  assert.deepEqual([e2.holder, e2.holder_mode, e2.holder_machine], ['thr-q', 'auto', 'box-q'], 'without thread: holder of the sender topic');
  assert.ok(!ev.some((e) => e.holder === 'thr-bob' || e.message_id === nowhere.id));
  assert.ok(!JSON.stringify(ev).includes('SECRET'));

  // State: pending for the recipient (still in its mailbox), outcome expired.
  assert.equal((await alice.receive({ project: 'p', topic: 't' })).messages.filter((m) => m.id === lonely.id).length, 1);
  // The receive above read the body after the deadline: the pickup is recorded, but too late to stop the expiry.
  const { picked_at: lateRead, ...lonelyState } = await alice.message(lonely.id);
  assert.ok(lateRead, 'the late read is recorded as pickup');
  assert.deepEqual(lonelyState, {
    id: lonely.id, state: 'pending', level: 'assist', reply_to: null, project: 'p', topic: 't', to: 'p/t', outcome: 'expired', replied: false,
    expires_at: lonely.expires_at, ttl_s: 60, undelivered_holder: null, undelivered_reason: null,
  });
  const stored = JSON.parse(fs.readFileSync(path.join(a.dir, 'messages', `${lonely.id}.json`), 'utf8'));
  assert.deepEqual([stored.outcome, typeof stored.expired_at], ['expired', 'string']);
  assert.equal((await alice.message(nowhere.id)).outcome, 'expired');
  assert.deepEqual([(await alice.message(answered.id)).outcome, (await alice.message(answered.id)).replied], [null, true]);
  assert.equal((await alice.message(acked.id)).state, 'acked');
  await assert.rejects(alice.message('nope'), (e) => e.status === 404);

  // A real reply after the deadline wakes the sender (accepted).
  const { next: n2 } = await alice.events('latest', 0);
  await asBob.send({ reply_to: lonely.id, level: 'assist', text: 'late answer' });
  const late = (await alice.events(n2, 0)).events;
  assert.deepEqual(late.map((e) => [e.type, e.holder, e.sender]), [['reply', 'thr-alice', 'bob']]);
  assert.equal((await alice.message(lonely.id)).replied, true);
});

test('bounce: outcome undelivered and one undelivered event for the sender; once; no mail, no notice; old comms mail never bounces', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a, { thread: 'thr-s', machine: 'box-s' });
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-gone' });
  const m = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'x' });
  const nowhere = await makeClient(a).send({ project: 'p', topic: 't', level: 'assist', text: 'y' });
  const { next } = await c.events('latest', 0);
  const nMessages = fs.readdirSync(path.join(a.dir, 'messages')).length;
  const nNotices = (await c.notices(true)).notices.length;

  const b = await c.bounce({ message_id: m.id, holder: 'thr-gone', reason: 'Thread thr-gone is archived\n and cannot receive messages.' });
  assert.equal(b.bounced, true);
  assert.deepEqual(await c.bounce({ message_id: m.id, holder: 'thr-gone', reason: 'again' }), { bounced: false, reason: 'already' });
  assert.equal((await c.bounce({ message_id: nowhere.id, holder: 'thr-gone', reason: 'gone' })).bounced, true, 'recorded also without a way back');
  const ev = (await c.events(next, 0)).events;
  assert.deepEqual(ev.map((e) => [e.type, e.message_id, e.holder, e.holder_machine]), [['undelivered', m.id, 'thr-s', 'box-s']]);
  assert.equal(b.event_seq, ev[0].seq);
  assert.equal(fs.readdirSync(path.join(a.dir, 'messages')).length, nMessages, 'no mail from comms');
  assert.equal((await c.notices(true)).notices.length, nNotices, 'no operator notice');
  const st = await c.message(m.id);
  assert.deepEqual([st.state, st.outcome, st.undelivered_holder, st.undelivered_reason, st.to], ['pending', 'undelivered', 'thr-gone', 'Thread thr-gone is archived and cannot receive messages.', 'p/t']);
  // An undelivered message does not expire later.
  assert.deepEqual(sa.srv.service.checkExpired(Date.now() + 3600_000), []);
  assert.equal((await c.message(m.id)).outcome, 'undelivered');
  await assert.rejects(c.bounce({ message_id: 'nope' }), (e) => e.status === 404);

  // Old data (0.2.x): mail from `comms` is never bounced; a bounced_at counts as undelivered.
  const svc = sa.srv.service;
  svc.store.saveMessage({ ...svc.store.message(nowhere.id), id: 'old-comms', sender: 'comms', outcome: null, undelivered_at: null });
  assert.deepEqual(svc.bounce({ message_id: 'old-comms' }), { bounced: false, reason: 'system' });
  svc.store.saveMessage({ ...svc.store.message(nowhere.id), id: 'old-bounced', outcome: null, bounced_at: '2026-01-01T00:00:00Z' });
  assert.equal(svc.messageState('old-bounced').outcome, 'undelivered');
  assert.deepEqual(svc.bounce({ message_id: 'old-bounced' }), { bounced: false, reason: 'already' });
});

test('deadlines survive a restart: the check runs at start and on the 30 s interval', async (t) => {
  const a = makeInstance('a');
  let sa = await startInstance(a);
  t.after(async () => { if (sa) await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a, { thread: 'thr-a' });
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const m = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'b', ttl: '1s' });
  await sa.stop();
  sa = null;
  await sleep(1100);
  sa = await startInstance(a);
  const stored = JSON.parse(fs.readFileSync(path.join(a.dir, 'messages', `${m.id}.json`), 'utf8'));
  assert.deepEqual([!!stored.expired_at, stored.outcome], [true, 'expired'], 'expired on start');
  const ev = (await makeClient(a).events(0, 0)).events.filter((e) => e.type === 'expired');
  assert.deepEqual(ev.map((e) => [e.message_id, e.holder]), [[m.id, 'thr-a']]);
  assert.ok(sa.srv.service.timer, 'interval armed');
});

test('count returns ids of pending messages in created_at order', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const ids = [];
  for (const level of ['steer', 'blocker', 'assist']) ids.push((await c.send({ project: 'p', topic: 't', level, text: level })).id);
  const r = await c.count({ project: 'p', topic: 't' });
  assert.deepEqual([r.total, r.blocker, r.ids], [3, 1, ids]);
});

test('turns: POST /turn open|closed|attempt, GET /turns?open=1; turn-interrupted notice deduplicated per thread', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  const o1 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'Box-A', project: 'p', topic: 't', holder_mode: 'auto' });
  assert.equal(o1.ok, true);
  assert.deepEqual([o1.turn.machine, o1.turn.state, o1.turn.closed_at, o1.turn.keepalive.count], ['box-a', 'open', null, 0]);
  await c.turn({ thread: 'thr-2', state: 'open', machine: 'box-a' });
  await c.turn({ thread: 'thr-2', state: 'closed' });
  const open = (await c.turns(true)).turns;
  assert.deepEqual(open.map((x) => x.thread), ['thr-1']);
  assert.deepEqual(Object.keys(open[0]).sort(), ['close_reason', 'closed_at', 'holder_mode', 'keepalive', 'machine', 'opened_at', 'project', 'state', 'thread', 'topic']);
  assert.equal((await c.turns(false)).turns.length, 2);

  const at = await c.turn({ thread: 'thr-1', attempt: true });
  assert.equal(at.turn.keepalive.count, 1);
  assert.ok(at.turn.keepalive.last_at);
  const closed = await c.turn({ thread: 'thr-1', state: 'closed', reason: 'keepalive' });
  assert.deepEqual([closed.turn.state, closed.turn.close_reason, closed.turn.opened_at], ['closed', 'keepalive', o1.turn.opened_at]);
  await sleep(5);
  const o2 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'box-a' });
  assert.ok(o2.turn.opened_at > o1.turn.opened_at, 'a new turn has a new opened_at');
  assert.equal(o2.turn.keepalive.count, 0);

  await assert.rejects(c.turn({ thread: 'thr-1', state: 'paused' }), (e) => e.status === 400);
  await assert.rejects(c.turn({ thread: '../x', state: 'open' }), (e) => e.status === 400);
  await assert.rejects(c.turn({ thread: 'thr-1', state: 'open', holder_mode: 'robot' }), (e) => e.status === 400);

  const n1 = await c.createNotice({ kind: 'turn-interrupted', title: 'Prerušený ťah', thread: 'thr-1', project: 'p', topic: 't', detail: 'd' });
  const n2 = await c.createNotice({ kind: 'turn-interrupted', title: 'Prerušený ťah', thread: 'thr-1' });
  assert.equal(n1.created, true);
  assert.deepEqual([n2.created, n2.notice_id], [false, n1.notice_id]);
  const list = (await c.notices()).notices.filter((n) => n.kind === 'turn-interrupted');
  assert.equal(list.length, 1);
  assert.deepEqual([list[0].thread_id, list[0].message_id, list[0].project], ['thr-1', null, 'p']);
  await c.decide({ notice_id: n1.notice_id, decision: 'ack' });
  await assert.rejects(c.createNotice({ kind: 'no-reply', title: 'x', message_id: 'y' }), (e) => e.status === 400, 'no-reply is gone');
});

test('turn: closed with opened_at closes only that turn (stale otherwise); open returns opened_at', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  const o1 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.ok(o1.turn.opened_at);
  await sleep(5);
  const o2 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.deepEqual(await c.turn({ thread: 'thr-1', state: 'closed', opened_at: o1.turn.opened_at }), { ok: true, stale: true });
  assert.equal((await c.turns(true)).turns[0].opened_at, o2.turn.opened_at, 'the current turn stays open');
  assert.deepEqual(await c.turn({ thread: 'thr-new', state: 'closed', opened_at: o1.turn.opened_at }), { ok: true, stale: true }, 'no record');
  assert.equal((await c.turns(false)).turns.length, 1);
  const closed = await c.turn({ thread: 'thr-1', state: 'closed', opened_at: o2.turn.opened_at });
  assert.deepEqual([closed.ok, closed.stale, closed.turn.state], [true, undefined, 'closed']);
  assert.equal((await c.turns(true)).turns.length, 0);
});

test('legacy no-reply notice (0.2.x data) stays decidable', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const svc = sa.srv.service;
  svc.store.saveNotice({ id: 'old-n', kind: 'no-reply', title: 'old', state: 'open', message_id: null, options: [{ id: 'ack', label: 'ok', needs_target: false }], created_at: '2026-01-01T00:00:00Z' });
  const c = makeClient(a);
  assert.equal((await c.decide({ notice_id: 'old-n', decision: 'ack' })).decision, 'ack');
});

test('register-topic 1a: a holder change needs takeover (409); same holder is idempotent; release clears only its own holder (CLI too)', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  const base = { project: 'p', topic: 't', subject: 'S' };
  await c.registerTopic({ ...base, holder: 'thr-1', holder_machine: 'box-1' });
  await assert.rejects(c.registerTopic({ ...base, holder: 'thr-2' }), (e) => e.status === 409);
  await assert.rejects(c.registerTopic(base), (e) => e.status === 409, 'an absent holder does not clear another holder');
  assert.equal((await c.registerTopic({ ...base, holder: 'thr-1' })).topic.holder, 'thr-1');
  // Release by a thread that is not the holder: refused; by the holder itself (or with no holder given): cleared.
  await assert.rejects(c.registerTopic({ ...base, holder: 'thr-2', release: true }), (e) => e.status === 409);
  const rel = await c.registerTopic({ ...base, holder: 'thr-1', release: true });
  assert.deepEqual([rel.topic.holder, rel.topic.holder_mode, rel.topic.holder_machine], [null, null, null]);
  await c.registerTopic({ ...base, holder: 'thr-3' });
  assert.equal((await c.registerTopic({ ...base, release: true })).topic.holder, null, 'release without holder clears (current client usage)');
  // Explicit takeover replaces the holder.
  await c.registerTopic({ ...base, holder: 'thr-4' });
  assert.equal((await c.registerTopic({ ...base, holder: 'thr-5', takeover: true })).topic.holder, 'thr-5');

  // CLI: --holder without --takeover fails (exit 1), with --takeover it works; --release clears.
  const cli = (extra) => runCli(['register-topic', '--data-dir', a.dir, '--key', a.key, '--project', 'p', '--topic', 't', '--subject', 'S', ...extra]);
  assert.equal((await cli(['--holder', 'thr-6'])).code, 1);
  assert.equal((await cli(['--holder', 'thr-6', '--takeover'])).code, 0);
  const cr = await cli(['--release', '--holder', 'thr-6']);
  assert.deepEqual([cr.code, cr.json.topic.holder], [0, null]);
});

test('register-topic 1b: fields not passed keep their stored values', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'Old', holder: 'thr-1', holder_mode: 'auto', holder_machine: 'box-1', persona: 'bob', visibility: 'private' });
  // Re-registration with the same holder and only the holder: everything else stays.
  const r = await c.registerTopic({ project: 'p', topic: 't', holder: 'thr-1' });
  assert.deepEqual([r.topic.subject, r.topic.holder, r.topic.holder_mode, r.topic.holder_machine, r.topic.persona, r.topic.visibility],
    ['Old', 'thr-1', 'auto', 'box-1', 'bob', 'private']);
  // A new holder takes over: the mode and machine of the old holder are not carried over.
  const t2 = await c.registerTopic({ project: 'p', topic: 't', holder: 'thr-2', takeover: true });
  assert.deepEqual([t2.topic.holder, t2.topic.holder_mode, t2.topic.holder_machine, t2.topic.subject], ['thr-2', null, null, 'Old']);
});

test('2: "a"/"x__y" and "a__x"/"y" (same file name) do not share a record; the foreign topic is refused, not overwritten', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'a', topic: 'x__y', subject: 'One' });
  assert.equal(a.store.topicFile('a', 'x__y'), a.store.topicFile('a__x', 'y'), 'the premise: both names map to one file');
  assert.equal(a.store.topic('a__x', 'y'), null);
  await assert.rejects(c.registerTopic({ project: 'a__x', topic: 'y', subject: 'Two' }), (e) => e.status === 409);
  assert.equal(a.store.topic('a', 'x__y').subject, 'One');
  // A message to the unregistered name is held, not delivered into the other topic.
  const sent = await c.send({ project: 'a__x', topic: 'y', level: 'assist', text: 'x' });
  assert.equal(sent.state, 'held');
  assert.equal((await c.receive({ project: 'a', topic: 'x__y' })).messages.length, 0);
});

test('3: reply_to with persona/project/topic is 400; a self-reply without thread id is refused', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  const bob = a.store.addKey('bob');
  const cBob = makeClient(a, { key: bob.key });
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const m = await cBob.send({ project: 'p', topic: 't', level: 'assist', text: 'hi', from_project: 'q', from_topic: 'u' });
  await assert.rejects(c.send({ reply_to: m.id, project: 'p', level: 'assist', text: 'x' }), (e) => e.status === 400);
  await assert.rejects(c.send({ reply_to: m.id, persona: 'bob', level: 'assist', text: 'x' }), (e) => e.status === 400);
  // Bob answers his own message (no thread id) with the same source topic: it would come back to him.
  await assert.rejects(cBob.send({ reply_to: m.id, level: 'assist', text: 'self', from_project: 'q', from_topic: 'u' }), (e) => e.status === 400);
  // A plain reply (no addressing, other source) is accepted.
  const ok = await c.send({ reply_to: m.id, level: 'assist', text: 'ok' });
  assert.equal(ok.state, 'pending');
});

test('4: ack with an unknown id is 404 naming it (nothing acked); already-acked ids are listed in `already`', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const m1 = await c.send({ project: 'p', topic: 't', level: 'assist', text: '1' });
  const m2 = await c.send({ project: 'p', topic: 't', level: 'assist', text: '2' });
  await assert.rejects(c.ack([m1.id, 'nope-1']), (e) => e.status === 404 && e.message.includes('nope-1'));
  assert.equal(a.store.message(m1.id).state, 'pending', 'a 404 acks nothing');
  assert.deepEqual(await c.ack([m1.id]), { acked: [m1.id], already: [] });
  assert.deepEqual(await c.ack([m1.id, m2.id]), { acked: [m2.id], already: [m1.id] });
  const r = await runCli(['ack', '--data-dir', a.dir, '--key', a.key, '--id', 'nope-2']);
  assert.notEqual(r.code, 0);
  assert.match(r.err, /nope-2/);
});

test('register-topic close bounces thread mail only when the closing thread holds no other topic (fable audit M3)', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ persona: 'p', main: true, holder: 'thr-m' });
  await c.registerTopic({ project: 'p', topic: 'work', subject: 'Work', holder: 'thr-m' });
  await c.registerTopic({ project: 'p', topic: 'src', subject: 'Src', holder: 'thr-s' });
  // A question from the main thread without a topic; its answer goes to the thread only.
  const q = await c.send({ project: 'p', topic: 'src', level: 'assist', text: 'q', thread_id: 'thr-m' });
  const ans = await c.send({ reply_to: q.id, level: 'assist', text: 'a', thread_id: 'thr-s' });
  assert.equal((await c.registerTopic({ project: 'p', topic: 'work', holder: 'thr-m', close: true })).bounced.length, 0, 'thr-m still holds main');
  assert.equal((await c.message(ans.id)).outcome, null);
});

test('register-topic close: a closed topic takes no mail (410), pending mail bounces to its senders, re-registration reopens (alice 2026-11-02)', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 'done', subject: 'Done', holder: 'thr-d' });
  await c.registerTopic({ project: 'p', topic: 'src', subject: 'Src', holder: 'thr-s' });
  // The closing thread sent a question just before it closed; the answer to it would go to its archived thread.
  const q = await c.send({ project: 'p', topic: 'src', level: 'assist', text: 'q', thread_id: 'thr-d', from_project: 'p', from_topic: 'done' });
  const waiting = await c.send({ project: 'p', topic: 'done', level: 'assist', text: 'w', thread_id: 'thr-s', from_project: 'p', from_topic: 'src' });
  await assert.rejects(c.registerTopic({ project: 'p', topic: 'done', holder: 'thr-x', close: true }), (e) => e.status === 409, 'only the holder closes');
  const closed = await c.registerTopic({ project: 'p', topic: 'done', holder: 'thr-d', close: true });
  assert.deepEqual([closed.topic.state, closed.topic.holder, !!closed.topic.closed_at], ['closed', null, true]);
  assert.deepEqual(closed.bounced, [waiting.id]);
  assert.equal((await c.message(waiting.id)).outcome, 'undelivered');
  // New mail and replies are refused at once, with the closing time.
  await assert.rejects(c.send({ project: 'p', topic: 'done', level: 'assist', text: 'x', thread_id: 'thr-s' }), (e) => e.status === 410 && /uzavretá/.test(e.message));
  await assert.rejects(c.send({ reply_to: q.id, level: 'assist', text: 'a', thread_id: 'thr-s' }), (e) => e.status === 410);
  // Idempotent close keeps the first closing time; the main thread is never closed.
  assert.equal((await c.registerTopic({ project: 'p', topic: 'done', close: true })).topic.closed_at, closed.topic.closed_at);
  await assert.rejects(c.registerTopic({ persona: 'p', main: true, close: true }), (e) => e.status === 400);
  await assert.rejects(c.registerTopic({ project: 'p', topic: 'nope', close: true }), (e) => e.status === 404);
  // A new registration reopens it.
  const re = await c.registerTopic({ project: 'p', topic: 'done', subject: 'Again', holder: 'thr-n' });
  assert.deepEqual([re.topic.state, re.topic.closed_at], ['open', null]);
  assert.equal((await c.send({ project: 'p', topic: 'done', level: 'assist', text: 'ok' })).state, 'pending');
  // CLI flag.
  const cr = await runCli(['register-topic', '--data-dir', a.dir, '--key', a.key, '--project', 'p', '--topic', 'done', '--close']);
  assert.deepEqual([cr.code, cr.json.topic.state], [0, 'closed']);
});
