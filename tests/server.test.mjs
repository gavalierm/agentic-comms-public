import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, rawRequest, cleanup, sleep, runCli, tmpDir, api, addKey } from './helpers.mjs';
import { startServer } from '../src/server/index.mjs';

// Server tests over mail model 2 (status, resolve; design mail-service-design v3, parts 1 and 2); tests through the client
// of model 2 and the CLI at the end.

// Starts a service for the test; stopping it and removing its data is registered for the end of the test.
async function serviceUp(t, name = 'a', sender = 'alice') {
  const a = makeInstance(name, sender);
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  return { a, sa, svc: sa.srv.service, c: api(a) };
}

const readMsg = (inst, id) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${id}.json`), 'utf8'));
const nFiles = (inst, dir) => fs.readdirSync(path.join(inst.dir, dir)).length;
/** JSON of a raw api call; asserts the HTTP status. */
async function ok(p, status = 200) {
  const r = await p;
  assert.equal(r.status, status, r.body);
  return r.json;
}
const errorOf = (status, re = null) => (e) => e.status === status && (re === null || re.test(e.json?.error || ''));

test('two instances on one machine do not see each other; a key of A is 401 in B', async (t) => {
  const a = makeInstance('a');
  const b = makeInstance('b');
  const sa = await startInstance(a);
  const sb = await startInstance(b);
  t.after(async () => { await sa.stop(); await sb.stop(); cleanup(a.dir, b.dir); });

  assert.notEqual(a.instanceId, b.instanceId);
  const ca = api(a);
  const cb = api(b);
  assert.equal((await ca.health()).instance_id, a.instanceId);
  assert.equal((await cb.health()).instance_id, b.instanceId);
  await ca.register({ project: 'p', topic: 't', subject: 'S' });
  await cb.register({ project: 'p', topic: 't', subject: 'S' });
  await ca.send({ project: 'p', topic: 't', level: 'assist', text: 'only in A' });

  assert.equal((await ca.receive({ project: 'p', topic: 't' })).length, 1);
  assert.equal((await cb.receive({ project: 'p', topic: 't' })).length, 0);

  const cross = api(b, a.key);
  assert.equal((await cross.call('GET', '/receive?project=p&topic=t')).status, 401);
  assert.equal(nFiles(b, 'messages'), 0);
});

test('sender is derived from the key, never from the body', async (t) => {
  const { a, c } = await serviceUp(t);
  const bob = api(a, addKey(a, 'bob'));
  await c.register({ project: 'p', topic: 't', subject: 'S' });
  await bob.send({ project: 'p', topic: 't', level: 'assist', text: 'hi', sender: 'alice' });

  const r = await c.receive({ project: 'p', topic: 't' });
  assert.equal(r.length, 1);
  assert.equal(r[0].sender, 'bob');
  assert.equal(readMsg(a, r[0].id).sender, 'bob');
});

test('message to an unregistered topic is held with a notice and does not create the topic', async (t) => {
  const { a, c } = await serviceUp(t);

  const r = await c.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'lost?' });
  assert.equal(r.status, 'held');
  assert.ok(r.notice_id);
  assert.equal(nFiles(a, 'topics'), 0);

  const n = await c.notices();
  assert.equal(n.length, 1);
  assert.equal(n[0].kind, 'unregistered-topic');
  assert.match(n[0].title, /Nová téma\? \/ presmerovať/);
  assert.deepEqual(n[0].options.map((o) => o.id), ['redirect', 'deliver', 'discard']);

  // Not receivable until the operator decides; registering the topic alone does not deliver.
  assert.equal((await c.receive({ project: 'p', topic: 'nowhere' })).length, 0);
  await c.register({ project: 'p', topic: 'nowhere', subject: 'Now exists' });
  assert.equal((await c.receive({ project: 'p', topic: 'nowhere' })).length, 0);
  await c.decide({ notice_id: r.notice_id, decision: 'deliver' });
  const got = await c.receive({ project: 'p', topic: 'nowhere' });
  assert.deepEqual(got.map((m) => [m.id, m.status]), [[r.id, 'picked']]);
});

test('register-topic is idempotent and requires a subject', async (t) => {
  const { c } = await serviceUp(t);
  const r1 = await c.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  const r2 = await c.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-2', takeover: true });
  assert.equal(r1.created, true);
  assert.equal(r2.created, false);
  assert.equal(r2.topic.holder, 'thr-2');
  assert.equal(r2.topic.registered_at, r1.topic.registered_at);
  assert.equal((await c.call('POST', '/register-topic', { project: 'p', topic: 'x' })).status, 400);
});

test('reply (resolve) goes to the sender thread with the level of its original and no deadline; blocker and reply emit wake events', async (t) => {
  const { a, svc, c: alice } = await serviceUp(t);
  const bob = api(a, addKey(a, 'bob'));
  const box = { project: 'p', topic: 't', thread: 'thr-bob' };
  await alice.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob' });

  const q = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'need answer', thread_id: 'thr-alice' });
  const ev1 = await alice.events(0);
  assert.deepEqual(ev1.map((e) => [e.type, e.holder]), [['blocker', 'thr-bob']]);

  const inbox = await bob.receive(box);
  assert.equal(inbox[0].id, q.id);
  // operator 2026-11-02: a reply never lowers the urgency of a blocker (a level in the body is ignored); it has no deadline
  const r = await bob.resolve({ ...box, id: q.id, state: 'hotovo', text: 'here you go', level: 'assist' });
  assert.deepEqual([r.status, r.resolution, r.answer_delivered], ['done', 'reply', true]);
  const stored = readMsg(a, r.answer_id);
  assert.deepEqual([stored.level, stored.expires_at, stored.ttl_s, stored.to_thread, stored.reply_to, stored.sender], ['blocker', null, null, 'thr-alice', q.id, 'bob']);
  assert.equal((await alice.notices(true)).filter((n) => n.message_id === r.answer_id).length, 0, 'a reply makes no operator notice');

  const replies = await alice.receive({ thread: 'thr-alice' });
  assert.deepEqual(replies.map((m) => [m.id, m.level, m.to_thread, m.reply_to, m.status, m.resolution]), [[r.answer_id, 'blocker', 'thr-alice', q.id, 'done', 'read']]);
  const ev2 = await alice.events(ev1.at(-1).seq);
  assert.deepEqual(ev2.map((e) => [e.type, e.holder, e.reply_to]), [['reply', 'thr-alice', q.id]]);

  // a read reply is not pending any more; long-poll returns [] after its timeout
  assert.equal((await alice.count({ thread: 'thr-alice' })).total, 0);
  const lp = await ok(alice.call('GET', `/events?since=${svc.lastSeq}&wait=100`));
  assert.deepEqual(lp.events, []);
});

test('startServer refuses a directory without an instance', async (t) => {
  const d = tmpDir('comms-empty-');
  t.after(() => cleanup(d));
  await assert.rejects(startServer({ dataDir: d }), /comms init/);
});

test('second server for the same instance reports ALREADY_RUNNING', async (t) => {
  const { a } = await serviceUp(t);
  await assert.rejects(startInstance(a), (e) => e.code === 'ALREADY_RUNNING');
});

// --- deadlines (TTL), expired_at, message state, count ids, turns -----------------

test('send: ttl defaults per level (blocker/steer 5m, assist 30m), explicit ttl shortens; 0, over the maximum and an unparsable ttl are 400', async (t) => {
  const { a, c } = await serviceUp(t);
  await c.register({ project: 'p', topic: 't', subject: 'S' });
  const exp = (r) => (Date.parse(r.expires_at) - Date.parse(readMsg(a, r.id).created_at)) / 1000;
  const send = (level, text, extra = {}) => c.send({ project: 'p', topic: 't', level, text, ...extra });

  const assist = await send('assist', 'a');
  assert.equal(exp(assist), 30 * 60);
  const accepted = [
    ['blocker default', 'blocker', undefined, 5 * 60],
    ['steer default', 'steer', undefined, 5 * 60],
    ['assist ttl 90s', 'assist', '90s', 90],
    ['blocker numeric ttl', 'blocker', 120, 120],
    ['steer ttl 1m', 'steer', '1m', 60],
    ['blocker at its maximum', 'blocker', '5m', 300],
    ['assist at its maximum', 'assist', '30m', 1800],
  ];
  for (const [name, level, ttl, seconds] of accepted) assert.equal(exp(await send(level, name, ttl === undefined ? {} : { ttl })), seconds, name);
  assert.equal((await c.send({ project: 'p', topic: 't', level: 'assist', text: 'f', reply_to: null })).expires_at !== null, true, 'reply_to: null is no reply');
  const stored = readMsg(a, assist.id);
  assert.deepEqual([stored.ttl_s, stored.expired_at ?? null, stored.picked_at ?? null, stored.status], [30 * 60, null, null, 'pending']);

  const before = nFiles(a, 'messages');
  const info = await c.call('POST', '/send', { project: 'p', topic: 't', level: 'info', text: 'x', ttl: '5m' });
  assert.equal(info.status, 400);
  assert.match(info.json.error, /zrušená/);
  assert.match(info.json.error, /assist/);
  const refused = [
    ['unparsable ttl', 'assist', 'soon', null],
    ['zero ttl', 'assist', '0', /kladná/],
    ['blocker over 5m', 'blocker', '10m', /maximum 5m/],
    ['steer over 5m', 'steer', '6m', null],
    ['assist over 30m', 'assist', '31m', null],
  ];
  for (const [name, level, ttl, re] of refused) {
    await assert.rejects(send(level, 'x', { ttl }), errorOf(400, re), name);
  }
  assert.equal(nFiles(a, 'messages'), before);
});

test('ttl defaults come from instance.json (ttl.blocker / ttl.assist); 0 falls back to the default, over the maximum to the maximum', async (t) => {
  const a = makeInstance('a');
  a.store.saveInstance({ ...a.store.instance(), ttl: { blocker: '1m', assist: 0 } });
  const over = makeInstance('o');
  over.store.saveInstance({ ...over.store.instance(), ttl: { blocker: '3h' } });
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const c = api(a);
  await c.register({ project: 'p', topic: 't', subject: 'S' });
  const b = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'b' });
  assert.ok(Math.abs(Date.parse(b.expires_at) - Date.now() - 60_000) < 5000);
  const a2 = await c.send({ project: 'p', topic: 't', level: 'assist', text: 'a' });
  assert.ok(Math.abs(Date.parse(a2.expires_at) - Date.now() - 30 * 60_000) < 5000);
  const so = await startInstance(over);
  t.after(async () => { await so.stop(); cleanup(over.dir); });
  const co = api(over);
  await co.register({ project: 'p', topic: 't', subject: 'S' });
  const b2 = await co.send({ project: 'p', topic: 't', level: 'blocker', text: 'b' });
  assert.ok(Math.abs(Date.parse(b2.expires_at) - Date.now() - 5 * 60_000) < 5000);
});

test('a held message expires like a pending one: the sender\'s wait is due, the notice stays decidable (fable audit M2)', async (t) => {
  const { a, svc, c } = await serviceUp(t);
  const held = await c.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'q', ttl: '60s', thread_id: 'thr-a' });
  assert.equal(held.status, 'held');
  const at = Date.now() + 120_000;
  assert.deepEqual(svc.checkExpired(at), [held.id]);
  const m = await c.message(held.id);
  assert.deepEqual([m.status, typeof m.expired_at, m.wait_reason], ['held', 'string', 'no-reply']);
  assert.ok(Date.parse(m.wait_due_at) <= at, 'the sender hears now');
  assert.equal(readMsg(a, held.id).status, 'held');
  assert.equal((await c.notices())[0].state, 'open');
});

test('expiry: no mail, no notice, no event; expired_at and a due wait for a sender with a way back; answered and acked do not expire', async (t) => {
  const { a, svc, c: alice } = await serviceUp(t);
  const bob = api(a, addKey(a, 'bob'));
  const box = { project: 'p', topic: 't', thread: 'thr-bob' };
  await alice.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob', holder_machine: 'box-b' });
  await alice.register({ project: 'q', topic: 'u', subject: 'Src', holder: 'thr-q', holder_mode: 'auto', holder_machine: 'box-q' });

  const mine = { thread_id: 'thr-alice', thread_machine: 'box-a' };
  const lonely = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'SECRET anyone?', ttl: '60s', from_project: 'q', from_topic: 'u', ...mine });
  const answered = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'answered', ttl: '60s', ...mine });
  const acked = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'acked', ttl: '60s', ...mine });
  const noThread = await bob.send({ project: 'p', topic: 't', level: 'assist', text: 'from topic', ttl: '30m', from_project: 'q', from_topic: 'u' });
  const nowhere = await bob.send({ project: 'p', topic: 't', level: 'blocker', text: 'no way back', ttl: '60s' });
  await bob.resolve({ ...box, id: answered.id, state: 'hotovo', text: 'ok' });
  await bob.receive({ ...box, ids: [acked.id] });
  await bob.resolve({ ...box, id: acked.id, state: 'hotovo', ack: true });
  const seq = svc.lastSeq;
  const nMessages = nFiles(a, 'messages');
  const nNotices = (await alice.notices(true)).length;

  assert.deepEqual(svc.checkExpired(Date.now() + 30_000), [], 'not yet');
  const at = Date.now() + 31 * 60_000;
  const expired = svc.checkExpired(at);
  assert.deepEqual(expired.sort(), [lonely.id, noThread.id, nowhere.id].sort());
  assert.deepEqual(svc.checkExpired(Date.now() + 10 * 3600_000), [], 'the second check does nothing');
  assert.equal(nFiles(a, 'messages'), nMessages, 'no mail from comms');
  assert.equal((await alice.notices(true)).length, nNotices, 'no operator notice');
  assert.equal(svc.lastSeq, seq, 'no event: the sender\'s wait is a field of the message (1e)');

  // Waits: due now for the sender's thread, or the holder of its source topic; none without a way back; no body.
  const due = svc.due('wake', at + 1000).due;
  const d1 = due.find((x) => x.id === lonely.id);
  assert.deepEqual([d1.wait_reason, d1.holder, d1.holder_machine, d1.holder_mode, d1.level, d1.project, d1.topic], ['no-reply', 'thr-alice', 'box-a', null, 'assist', 'p', 't']);
  const d2 = due.find((x) => x.id === noThread.id);
  assert.deepEqual([d2.holder, d2.holder_mode, d2.holder_machine], ['thr-q', 'auto', 'box-q'], 'without thread: holder of the sender topic');
  assert.ok(!due.some((x) => x.id === nowhere.id || x.holder === 'thr-bob'));
  assert.equal(readMsg(a, nowhere.id).wait_due_at ?? null, null);
  assert.ok(!JSON.stringify(due).includes('SECRET'));

  // Still pending in the recipient's mailbox (the late read is the pickup); expired_at stays as history.
  assert.equal((await bob.receive({ ...box, ids: [lonely.id] })).length, 1);
  const st = await alice.message(lonely.id);
  assert.deepEqual([st.status, st.level, st.to, st.ttl_s, st.expires_at, st.answer_state, st.reply_to], ['picked', 'assist', 'p/t', 60, lonely.expires_at, null, null]);
  assert.ok(st.expired_at && st.picked_at);
  assert.deepEqual([readMsg(a, nowhere.id).status, typeof readMsg(a, nowhere.id).expired_at], ['pending', 'string']);
  const ans = await alice.message(answered.id);
  assert.deepEqual([ans.status, ans.resolution, ans.expired_at], ['done', 'reply', null]);
  assert.deepEqual([(await alice.message(acked.id)).status, (await alice.message(acked.id)).resolution], ['done', 'acked']);
  await assert.rejects(alice.message('nope'), errorOf(404));

  // A real reply after the deadline wakes the sender and ends its wait.
  const s2 = svc.lastSeq;
  await bob.resolve({ ...box, id: lonely.id, state: 'hotovo', text: 'late answer' });
  const late = await alice.events(s2);
  assert.deepEqual(late.map((e) => [e.type, e.holder, e.sender]), [['reply', 'thr-alice', 'bob']]);
  const after = readMsg(a, lonely.id);
  assert.deepEqual([after.status, after.resolution, after.wait_due_at], ['done', 'reply', null]);
});

test('bounce: pending → undelivered, once; the sender\'s wait is due (undelivered); no mail, no event; its notice closes', async (t) => {
  const { a, svc, c } = await serviceUp(t);
  await c.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-gone' });
  const m = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'x', thread_id: 'thr-s', thread_machine: 'box-s' });
  const nowhere = await c.send({ project: 'p', topic: 't', level: 'assist', text: 'y' });
  const seq = svc.lastSeq;
  const nMessages = nFiles(a, 'messages');
  const nNotices = (await c.notices(true)).length;

  const b = await c.bounce({ message_id: m.id, holder: 'thr-gone', reason: 'Thread thr-gone is archived\n and cannot receive messages.' });
  assert.deepEqual(b, { bounced: true, status: 'undelivered' });
  assert.deepEqual(await c.bounce({ message_id: m.id, holder: 'thr-gone', reason: 'again' }), { bounced: false, status: 'undelivered', reason: 'status undelivered' });
  assert.equal((await c.bounce({ message_id: nowhere.id, holder: 'thr-gone', reason: 'gone' })).bounced, true, 'recorded also without a way back');
  assert.equal(svc.lastSeq, seq, 'no event');
  assert.equal(nFiles(a, 'messages'), nMessages, 'no mail from comms');
  assert.equal((await c.notices(true)).length, nNotices, 'no new operator notice');
  const n = (await c.notices(true)).find((x) => x.message_id === m.id);
  assert.deepEqual([n.state, n.decision], ['decided', 'undelivered']);

  const st = await c.message(m.id);
  assert.deepEqual([st.status, st.undelivered_holder, st.undelivered_reason, st.to, st.wait_reason], ['undelivered', 'thr-gone', 'Thread thr-gone is archived and cannot receive messages.', 'p/t', 'undelivered']);
  assert.ok(Date.parse(st.wait_due_at) <= Date.now());
  assert.equal((await c.message(nowhere.id)).wait_due_at, null, 'no way back: no wait');
  const due = await api(a, addKey(a, 'wake')).due();
  assert.deepEqual(due.due.filter((x) => x.id === m.id).map((x) => [x.wait_reason, x.holder, x.holder_machine]), [['undelivered', 'thr-s', 'box-s']]);
  // An undelivered message does not expire later.
  assert.deepEqual(svc.checkExpired(Date.now() + 3600_000), []);
  assert.equal((await c.message(m.id)).expired_at, null);
  assert.equal((await c.call('POST', '/bounce', { message_id: 'nope' })).status, 404);
});

test('deadlines survive a restart: the check runs at start and on the 30 s interval', async (t) => {
  const a = makeInstance('a');
  let sa = await startInstance(a);
  t.after(async () => { if (sa) await sa.stop(); cleanup(a.dir); });
  const c = api(a);
  await c.register({ project: 'p', topic: 't', subject: 'S' });
  const m = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'b', ttl: '1s', thread_id: 'thr-a' });
  await sa.stop();
  sa = null;
  await sleep(1100);
  sa = await startInstance(a);
  const stored = readMsg(a, m.id);
  assert.deepEqual([!!stored.expired_at, stored.status, stored.wait_reason], [true, 'pending', 'no-reply'], 'expired on start');
  assert.ok(Date.parse(stored.wait_due_at) <= Date.now(), 'the sender\'s wait is due');
  assert.deepEqual((await c.events(0)).filter((e) => e.message_id === m.id).map((e) => e.type), ['blocker'], 'no expired event');
  assert.ok(sa.srv.service.timer, 'interval armed');
});

test('count returns ids of pending messages in created_at order', async (t) => {
  const { c } = await serviceUp(t);
  await c.register({ project: 'p', topic: 't', subject: 'S' });
  const ids = [];
  for (const level of ['steer', 'blocker', 'assist']) ids.push((await c.send({ project: 'p', topic: 't', level, text: level })).id);
  const r = await c.count({ project: 'p', topic: 't' });
  assert.deepEqual([r.total, r.blocker, r.ids], [3, 1, ids]);
});

test('turns: POST /turn open|closed|attempt, GET /turns?open=1; turn-interrupted notice deduplicated per thread', async (t) => {
  const { c } = await serviceUp(t);
  const turns = async (open) => (await ok(c.call('GET', `/turns${open ? '?open=1' : ''}`))).turns;
  const notice = (body) => ok(c.call('POST', '/notice', body), 201);
  const o1 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'Box-A', project: 'p', topic: 't', holder_mode: 'auto' });
  assert.equal(o1.ok, true);
  assert.deepEqual([o1.turn.machine, o1.turn.state, o1.turn.closed_at, o1.turn.keepalive.count], ['box-a', 'open', null, 0]);
  await c.turn({ thread: 'thr-2', state: 'open', machine: 'box-a' });
  await c.turn({ thread: 'thr-2', state: 'closed' });
  const open = await turns(true);
  assert.deepEqual(open.map((x) => x.thread), ['thr-1']);
  assert.deepEqual(Object.keys(open[0]).sort(), ['close_reason', 'closed_at', 'holder_mode', 'keepalive', 'machine', 'opened_at', 'project', 'run_at', 'state', 'thread', 'topic']);
  assert.equal((await turns(false)).length, 2);

  const at = await c.turn({ thread: 'thr-1', attempt: true });
  assert.equal(at.turn.keepalive.count, 1);
  assert.ok(at.turn.keepalive.last_at);
  const closed = await c.turn({ thread: 'thr-1', state: 'closed', reason: 'keepalive' });
  assert.deepEqual([closed.turn.state, closed.turn.close_reason, closed.turn.opened_at], ['closed', 'keepalive', o1.turn.opened_at]);
  await sleep(5);
  const o2 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'box-a' });
  assert.ok(o2.turn.opened_at > o1.turn.opened_at, 'a new turn has a new opened_at');
  assert.equal(o2.turn.keepalive.count, 0);

  await assert.rejects(c.turn({ thread: 'thr-1', state: 'paused' }), errorOf(400));
  await assert.rejects(c.turn({ thread: '../x', state: 'open' }), errorOf(400));
  await assert.rejects(c.turn({ thread: 'thr-1', state: 'open', holder_mode: 'robot' }), errorOf(400));

  const n1 = await notice({ kind: 'turn-interrupted', title: 'Prerušený ťah', thread: 'thr-1', project: 'p', topic: 't', detail: 'd' });
  const n2 = await notice({ kind: 'turn-interrupted', title: 'Prerušený ťah', thread: 'thr-1' });
  assert.equal(n1.created, true);
  assert.deepEqual([n2.created, n2.notice_id], [false, n1.notice_id]);
  const list = (await c.notices()).filter((n) => n.kind === 'turn-interrupted');
  assert.equal(list.length, 1);
  assert.deepEqual([list[0].thread_id, list[0].message_id, list[0].project], ['thr-1', null, 'p']);
  await c.decide({ notice_id: n1.notice_id, decision: 'ack' });
  assert.equal((await c.call('POST', '/notice', { kind: 'no-reply', title: 'x', message_id: 'y' })).status, 400, 'no-reply is gone');
});

test('turn: closed with opened_at closes only that turn (stale otherwise); open returns opened_at', async (t) => {
  const { c } = await serviceUp(t);
  const turns = async (open) => (await ok(c.call('GET', `/turns${open ? '?open=1' : ''}`))).turns;
  const o1 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.ok(o1.turn.opened_at);
  await sleep(5);
  const o2 = await c.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.deepEqual(await c.turn({ thread: 'thr-1', state: 'closed', opened_at: o1.turn.opened_at }), { ok: true, stale: true });
  assert.equal((await turns(true))[0].opened_at, o2.turn.opened_at, 'the current turn stays open');
  assert.deepEqual(await c.turn({ thread: 'thr-new', state: 'closed', opened_at: o1.turn.opened_at }), { ok: true, stale: true }, 'no record');
  assert.equal((await turns(false)).length, 1);
  const closed = await c.turn({ thread: 'thr-1', state: 'closed', opened_at: o2.turn.opened_at });
  assert.deepEqual([closed.ok, closed.stale, closed.turn.state], [true, undefined, 'closed']);
  assert.equal((await turns(true)).length, 0);
});

test('register-topic: a holder change needs takeover (409); same holder is idempotent; release clears only its own holder', async (t) => {
  const { c } = await serviceUp(t);
  const reg = (body) => c.call('POST', '/register-topic', body);
  const base = { project: 'p', topic: 't', subject: 'S' };
  await c.register({ ...base, holder: 'thr-1', holder_machine: 'box-1' });
  assert.equal((await reg({ ...base, holder: 'thr-2' })).status, 409);
  assert.equal((await reg(base)).status, 409, 'an absent holder does not clear another holder');
  assert.equal((await c.register({ ...base, holder: 'thr-1' })).topic.holder, 'thr-1');
  // Release by a thread that is not the holder: refused; by the holder itself (or with no holder given): cleared.
  assert.equal((await reg({ ...base, holder: 'thr-2', release: true })).status, 409);
  const rel = await c.register({ ...base, holder: 'thr-1', release: true });
  assert.deepEqual([rel.topic.holder, rel.topic.holder_mode, rel.topic.holder_machine], [null, null, null]);
  await c.register({ ...base, holder: 'thr-3' });
  assert.equal((await c.register({ ...base, release: true })).topic.holder, null, 'release without holder clears (current client usage)');
  // Explicit takeover replaces the holder.
  await c.register({ ...base, holder: 'thr-4' });
  assert.equal((await c.register({ ...base, holder: 'thr-5', takeover: true })).topic.holder, 'thr-5');
});

test('register-topic: fields not passed keep their stored values', async (t) => {
  const { c } = await serviceUp(t);
  await c.register({ project: 'p', topic: 't', subject: 'Old', holder: 'thr-1', holder_mode: 'auto', holder_machine: 'box-1', persona: 'bob', visibility: 'private' });
  // Re-registration with the same holder and only the holder: everything else stays.
  const r = await ok(c.call('POST', '/register-topic', { project: 'p', topic: 't', holder: 'thr-1' }));
  assert.deepEqual([r.topic.subject, r.topic.holder, r.topic.holder_mode, r.topic.holder_machine, r.topic.persona, r.topic.visibility],
    ['Old', 'thr-1', 'auto', 'box-1', 'bob', 'private']);
  // A new holder takes over: the mode and machine of the old holder are not carried over.
  const t2 = await ok(c.call('POST', '/register-topic', { project: 'p', topic: 't', holder: 'thr-2', takeover: true }));
  assert.deepEqual([t2.topic.holder, t2.topic.holder_mode, t2.topic.holder_machine, t2.topic.subject], ['thr-2', null, null, 'Old']);
});

test('register-topic: "a"/"x__y" and "a__x"/"y" (same file name) do not share a record; the foreign topic is refused, not overwritten', async (t) => {
  const { a, c } = await serviceUp(t);
  await c.register({ project: 'a', topic: 'x__y', subject: 'One' });
  assert.equal(a.store.topicFile('a', 'x__y'), a.store.topicFile('a__x', 'y'), 'the premise: both names map to one file');
  assert.equal(a.store.topic('a__x', 'y'), null);
  assert.equal((await c.call('POST', '/register-topic', { project: 'a__x', topic: 'y', subject: 'Two' })).status, 409);
  assert.equal(a.store.topic('a', 'x__y').subject, 'One');
  // A message to the unregistered name is held, not delivered into the other topic.
  const sent = await c.send({ project: 'a__x', topic: 'y', level: 'assist', text: 'x' });
  assert.equal(sent.status, 'held');
  assert.equal((await c.receive({ project: 'a', topic: 'x__y' })).length, 0);
});

test('send: reply_to, reply_state and cancels are 400 (a reply goes through resolve); resolve of an unknown id is 404 naming it', async (t) => {
  const { a, c } = await serviceUp(t);
  await c.register({ project: 'p', topic: 't', subject: 'S' });
  const m = await c.send({ project: 'p', topic: 't', level: 'assist', text: 'hi' });
  const before = nFiles(a, 'messages');
  for (const extra of [{ reply_to: m.id }, { reply_state: 'hotovo' }, { cancels: m.id }]) {
    await assert.rejects(c.send({ project: 'p', topic: 't', level: 'assist', text: 'x', ...extra }), errorOf(400, /resolve/), JSON.stringify(extra));
  }
  assert.equal(nFiles(a, 'messages'), before, 'nothing stored');
  const r = await c.call('POST', '/resolve', { project: 'p', topic: 't', id: 'nope-1', state: 'hotovo' });
  assert.equal(r.status, 404);
  assert.match(r.json.error, /nope-1/);
  assert.equal((await c.call('POST', '/resolve', { id: m.id, state: 'hotovo' })).status, 400, 'resolve names its mailbox');
  assert.equal(readMsg(a, m.id).status, 'pending', 'refused resolves change nothing');
});

test('`<persona>/main` without `@` goes to the main thread of the persona; without one it stays held (audit M5 2026-11-11)', async (t) => {
  const { c } = await serviceUp(t);
  await c.register({ persona: 'bob', main: true, holder: 'thr-m' });
  const r = await c.send({ project: 'bob', topic: 'main', level: 'assist', text: 'hi' });
  assert.equal(r.status, 'pending');
  assert.equal((await c.receive({ project: '@bob', topic: 'main' })).length, 1);
  assert.equal((await c.send({ project: 'carol', topic: 'main', level: 'assist', text: 'x' })).status, 'held', 'no persona main: held as before');
});

test('register-topic close undelivers thread mail only when the closing thread holds no other topic (fable audit M3)', async (t) => {
  const { a, c } = await serviceUp(t);
  await c.register({ persona: 'p', main: true, holder: 'thr-m' });
  await c.register({ project: 'p', topic: 'work', subject: 'Work', holder: 'thr-m' });
  await c.register({ project: 'p', topic: 'src', subject: 'Src', holder: 'thr-s' });
  // A question from the main thread without a topic; its answer goes to the thread only.
  const q = await c.send({ project: 'p', topic: 'src', level: 'assist', text: 'q', thread_id: 'thr-m' });
  const ans = await c.resolve({ project: 'p', topic: 'src', thread: 'thr-s', id: q.id, state: 'hotovo', text: 'a' });
  const reply = readMsg(a, ans.answer_id);
  assert.deepEqual([reply.to_thread, reply.topic, reply.status], ['thr-m', null, 'pending']);
  const closed = await c.register({ project: 'p', topic: 'work', holder: 'thr-m', close: true });
  assert.deepEqual([closed.undelivered, closed.recipient_closed], [[], []], 'thr-m still holds main');
  assert.equal(readMsg(a, ans.answer_id).status, 'pending');
});

test('register-topic close: a closed topic takes no mail (410), pending mail → undelivered, a reply to it is not delivered, re-registration reopens (alice 2026-11-02)', async (t) => {
  const { a, c } = await serviceUp(t);
  await c.register({ project: 'p', topic: 'done', subject: 'Done', holder: 'thr-d' });
  await c.register({ project: 'p', topic: 'src', subject: 'Src', holder: 'thr-s' });
  // The closing thread sent a question just before it closed; the answer to it would go to its archived topic.
  const q = await c.send({ project: 'p', topic: 'src', level: 'assist', text: 'q', thread_id: 'thr-d', from_project: 'p', from_topic: 'done' });
  const waiting = await c.send({ project: 'p', topic: 'done', level: 'assist', text: 'w', thread_id: 'thr-s', from_project: 'p', from_topic: 'src' });
  assert.equal((await c.call('POST', '/register-topic', { project: 'p', topic: 'done', holder: 'thr-x', close: true })).status, 409, 'only the holder closes');
  const closed = await c.register({ project: 'p', topic: 'done', holder: 'thr-d', close: true });
  assert.deepEqual([closed.closed, closed.topic.state, closed.topic.holder, !!closed.topic.closed_at], [true, 'closed', null, true]);
  assert.deepEqual([closed.undelivered, closed.recipient_closed], [[waiting.id], []]);
  assert.equal('bounced' in closed, false);
  assert.deepEqual([(await c.message(waiting.id)).status, (await c.message(waiting.id)).wait_reason], ['undelivered', 'undelivered']);
  // New mail is refused at once, with the closing time; a reply to the closed topic is stored undelivered (I9).
  await assert.rejects(c.send({ project: 'p', topic: 'done', level: 'assist', text: 'x', thread_id: 'thr-s' }), errorOf(410, /uzavretá/));
  const r = await c.resolve({ project: 'p', topic: 'src', thread: 'thr-s', id: q.id, state: 'hotovo', text: 'a' });
  assert.deepEqual([r.status, r.answer_delivered, r.reason], ['done', false, 'sender-closed']);
  // Idempotent close keeps the first closing time; the main thread is never closed.
  assert.equal((await c.register({ project: 'p', topic: 'done', close: true })).topic.closed_at, closed.topic.closed_at);
  assert.equal((await c.call('POST', '/register-topic', { persona: 'p', main: true, close: true })).status, 400);
  assert.equal((await c.call('POST', '/register-topic', { project: 'p', topic: 'nope', subject: 'S', close: true })).status, 404);
  // A new registration reopens it.
  const re = await c.register({ project: 'p', topic: 'done', subject: 'Again', holder: 'thr-n' });
  assert.deepEqual([re.topic.state, re.topic.closed_at], ['open', null]);
  assert.equal((await c.send({ project: 'p', topic: 'done', level: 'assist', text: 'ok' })).status, 'pending');
  assert.equal(readMsg(a, q.id).status, 'done');
});

// D-0024 (etapa 3b): the answer to a message held for an unregistered topic says whether its project is known (has an
// open registered topic), so the sender can tell a new topic of a known project from a wrong project. It depends on the
// project only: a private topic and an unknown topic of the same project get the same answer (no oracle, P-045).
test('held message: project_known is true for a project with an open topic, false otherwise', async (t) => {
  const { c } = await serviceUp(t);
  const empty = await c.send({ project: 'p', topic: 'report-1', level: 'assist', text: 'x' });
  assert.deepEqual([empty.status, empty.project_known], ['held', false]);

  await c.register({ project: 'p', topic: 'main-work', subject: 'S' });
  const known = await c.send({ project: 'p', topic: 'report-2', level: 'assist', text: 'x' });
  assert.deepEqual([known.status, known.project_known], ['held', true]);
  const other = await c.send({ project: 'q', topic: 'report-3', level: 'assist', text: 'x' });
  assert.deepEqual([other.status, other.project_known], ['held', false]);

  // A private topic of persona zed in project v, addressed by someone else, and an unknown topic of v: the same answer.
  await c.register({ project: 'v', topic: 'secret', subject: 'S', persona: 'zed', visibility: 'private' });
  const priv = await c.send({ project: 'v', topic: 'secret', level: 'assist', text: 'x' });
  const unknown = await c.send({ project: 'v', topic: 'nothing-here', level: 'assist', text: 'x' });
  assert.deepEqual([priv.status, priv.project_known], ['held', true]);
  assert.deepEqual(Object.keys(priv).sort(), Object.keys(unknown).sort());
  assert.equal(unknown.project_known, true);

  // A project whose only topic is closed is not known.
  await c.register({ project: 'w', topic: 'old', subject: 'S' });
  await c.register({ project: 'w', topic: 'old', close: true });
  const closed = await c.send({ project: 'w', topic: 'new-one', level: 'assist', text: 'x' });
  assert.deepEqual([closed.status, closed.project_known], ['held', false]);

  // A delivered message has no such field (only a held one answers it).
  assert.equal('project_known' in (await c.send({ project: 'p', topic: 'main-work', level: 'assist', text: 'x' })), false);
});

// D-0024: deliver of a held message whose pickup deadline passed would duplicate it (the sender already got
// `no reply <id>` and sends anew): refused with 409; discard still settles the notice.
test('decide deliver refuses an expired held message; the notice keeps project and topic; discard works', async (t) => {
  const { svc, c } = await serviceUp(t);
  const held = await c.send({ project: 'p', topic: 'later', level: 'assist', text: 'q', ttl: '60s', thread_id: 'thr-a' });
  assert.equal(held.status, 'held');
  await c.register({ project: 'p', topic: 'later', subject: 'Now exists' });
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [held.id]);

  const n = (await c.notices()).find((x) => x.message_id === held.id);
  assert.deepEqual([n.kind, n.project, n.topic, n.state], ['unregistered-topic', 'p', 'later', 'open']);
  await assert.rejects(c.decide({ notice_id: held.notice_id, decision: 'deliver' }), (e) => e.status === 409 && /vypršala/.test(e.json.error) && /discard/.test(e.json.error));
  assert.equal((await c.receive({ project: 'p', topic: 'later' })).length, 0);
  assert.equal((await c.message(held.id)).status, 'held');
  assert.equal((await c.notices()).find((x) => x.id === held.notice_id).state, 'open');
  await c.decide({ notice_id: held.notice_id, decision: 'discard' });
  assert.equal((await c.message(held.id)).status, 'discarded');

  // Past expires_at before the 30 s check marked it: refused the same way.
  const quick = await c.send({ project: 'p', topic: 'later-2', level: 'assist', text: 'q', ttl: '1s' });
  await c.register({ project: 'p', topic: 'later-2', subject: 'S' });
  await sleep(1100);
  await assert.rejects(c.decide({ notice_id: quick.notice_id, decision: 'deliver' }), errorOf(409));
  assert.equal((await c.receive({ project: 'p', topic: 'later-2' })).length, 0);

  // A held message within its deadline is still delivered.
  const fresh = await c.send({ project: 'p', topic: 'later-3', level: 'assist', text: 'q' });
  await c.register({ project: 'p', topic: 'later-3', subject: 'S' });
  await c.decide({ notice_id: fresh.notice_id, decision: 'deliver' });
  assert.equal((await c.receive({ project: 'p', topic: 'later-3' })).length, 1);
});

// D-0024, review of etapa 3b: redirect puts the held message into a mailbox too, so after its deadline it would double
// what the sender already sent anew or reported another way (an issue). Refused like deliver: 409, the notice stays
// open, discard works.
test('decide redirect refuses an expired held message like deliver; a private deliver too; within the deadline it redirects', async (t) => {
  const { svc, c } = await serviceUp(t);
  await c.register({ project: 'p', topic: 'target', subject: 'Redirect target' });
  const held = await c.send({ project: 'p', topic: 'typo', level: 'assist', text: 'q', ttl: '60s', thread_id: 'thr-a' });
  assert.equal(held.status, 'held');
  assert.deepEqual(svc.checkExpired(Date.now() + 120_000), [held.id]);

  const r1 = await c.call('POST', '/decide', { notice_id: held.notice_id, decision: 'redirect', target: 'p/target' });
  assert.equal(r1.status, 409);
  assert.match(r1.json.error, /vypršala/);
  assert.match(r1.json.error, /presmerovanie/);
  assert.match(r1.json.error, /discard/);
  assert.deepEqual([r1.json.message_id, r1.json.decision], [held.id, 'redirect']);
  assert.equal((await c.receive({ project: 'p', topic: 'target' })).length, 0);
  const kept = await c.message(held.id);
  assert.deepEqual([kept.status, kept.project, kept.topic], ['held', 'p', 'typo']);
  assert.equal((await c.notices()).find((x) => x.id === held.notice_id).state, 'open');

  await c.decide({ notice_id: held.notice_id, decision: 'discard' });
  assert.equal((await c.message(held.id)).status, 'discarded');

  // Past expires_at before the 30 s check marked it: refused the same way.
  const quick = await c.send({ project: 'p', topic: 'typo-2', level: 'assist', text: 'q', ttl: '1s' });
  await sleep(1100);
  await assert.rejects(c.decide({ notice_id: quick.notice_id, decision: 'redirect', target: 'p/target' }), errorOf(409));
  assert.equal((await c.receive({ project: 'p', topic: 'target' })).length, 0);

  // A held message for a foreign private topic: deliver (to the owner's main thread) is refused after the deadline as well.
  await c.register({ project: 'v', topic: 'secret', subject: 'S', persona: 'zed', visibility: 'private' });
  const priv = await c.send({ project: 'v', topic: 'secret', level: 'assist', text: 'q', ttl: '60s' });
  assert.equal(priv.status, 'held');
  assert.ok(svc.checkExpired(Date.now() + 120_000).includes(priv.id));
  await assert.rejects(c.decide({ notice_id: priv.notice_id, decision: 'deliver' }), errorOf(409, /doručenie/));
  assert.equal((await c.message(priv.id)).status, 'held');

  // Within its deadline a held message is still redirected.
  const fresh = await c.send({ project: 'p', topic: 'typo-3', level: 'assist', text: 'q' });
  const r = await c.decide({ notice_id: fresh.notice_id, decision: 'redirect', target: 'p/target' });
  assert.deepEqual(r.delivered_to, { project: 'p', topic: 'target' });
  assert.equal((await c.receive({ project: 'p', topic: 'target' })).length, 1);
});

// --- client and CLI ------------------------------------------------------------------------------------

test('health returns instance_id and model 2; client rejects a different instance', async (t) => {
  const { a, sa } = await serviceUp(t);

  const h = await makeClient(a).ensureRunning();
  assert.equal(h.ok, true);
  assert.equal(h.instance_id, a.instanceId);
  assert.equal(h.model, 2);

  const wrong = makeClient(a, { instanceId: 'deadbeef00000000' });
  await assert.rejects(wrong.ensureRunning(), (e) => e.code === 'INSTANCE_MISMATCH');
});

test('register-topic: a holder change needs takeover (409); same holder is idempotent; release clears only its own holder (CLI too)', async (t) => {
  const { a, sa } = await serviceUp(t);
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

test('register-topic close: a closed topic takes no mail (410), pending mail becomes undelivered, an answer to its question is not delivered, re-registration reopens (alice 2026-11-02)', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 'done', subject: 'Done', holder: 'thr-d' });
  await c.registerTopic({ project: 'p', topic: 'src', subject: 'Src', holder: 'thr-s' });
  // The closing thread sent a question just before it closed; the answer to it would go to its archived thread.
  const q = await c.send({ project: 'p', topic: 'src', level: 'assist', text: 'q', thread_id: 'thr-d', from_project: 'p', from_topic: 'done' });
  const waiting = await c.send({ project: 'p', topic: 'done', level: 'assist', text: 'w', thread_id: 'thr-s', from_project: 'p', from_topic: 'src' });
  await assert.rejects(c.registerTopic({ project: 'p', topic: 'done', holder: 'thr-x', close: true }), (e) => e.status === 409, 'only the holder closes');
  const closed = await c.registerTopic({ project: 'p', topic: 'done', holder: 'thr-d', close: true });
  assert.deepEqual([closed.topic.state, closed.topic.holder, !!closed.topic.closed_at], ['closed', null, true]);
  assert.deepEqual(closed.undelivered, [waiting.id]);
  assert.equal((await c.message(waiting.id)).status, 'undelivered');
  // New mail is refused at once, with the closing time.
  await assert.rejects(c.send({ project: 'p', topic: 'done', level: 'assist', text: 'x', thread_id: 'thr-s' }), (e) => e.status === 410 && /uzavretá/.test(e.message));
  // The answer to its question is resolved but not delivered (I9).
  await c.receive({ project: 'p', topic: 'src', thread: 'thr-s' });
  const ans = await c.resolve({ project: 'p', topic: 'src', id: q.id, state: 'hotovo', text: 'a', thread_id: 'thr-s' });
  assert.deepEqual([ans.status, ans.answer_delivered, ans.reason], ['done', false, 'sender-closed']);
  // Idempotent close keeps the first closing time; the main thread is never closed.
  assert.equal((await c.registerTopic({ project: 'p', topic: 'done', close: true })).topic.closed_at, closed.topic.closed_at);
  await assert.rejects(c.registerTopic({ persona: 'p', main: true, close: true }), (e) => e.status === 400);
  await assert.rejects(c.registerTopic({ project: 'p', topic: 'nope', close: true }), (e) => e.status === 404);
  // A new registration reopens it.
  const re = await c.registerTopic({ project: 'p', topic: 'done', subject: 'Again', holder: 'thr-n' });
  assert.deepEqual([re.topic.state, re.topic.closed_at], ['open', null]);
  assert.equal((await c.send({ project: 'p', topic: 'done', level: 'assist', text: 'ok' })).status, 'pending');
  // CLI flag.
  const cr = await runCli(['register-topic', '--data-dir', a.dir, '--key', a.key, '--project', 'p', '--topic', 'done', '--close']);
  assert.deepEqual([cr.code, cr.json.topic.state], [0, 'closed']);
});

test('held message: project_known is true for a project with an open topic, false otherwise; the CLI passes it through', async (t) => {
  const { a } = await serviceUp(t);
  const c = makeClient(a);
  const empty = await c.send({ project: 'p', topic: 'report-1', level: 'assist', text: 'x' });
  assert.deepEqual([empty.status, empty.project_known], ['held', false]);

  await c.registerTopic({ project: 'p', topic: 'main-work', subject: 'S' });
  const known = await c.send({ project: 'p', topic: 'report-2', level: 'assist', text: 'x' });
  assert.deepEqual([known.status, known.project_known], ['held', true]);
  const other = await c.send({ project: 'q', topic: 'report-3', level: 'assist', text: 'x' });
  assert.deepEqual([other.status, other.project_known], ['held', false]);

  // A private topic of persona zed in project v, addressed by someone else, and an unknown topic of v: the same answer.
  await c.registerTopic({ project: 'v', topic: 'secret', subject: 'S', persona: 'zed', visibility: 'private' });
  const priv = await c.send({ project: 'v', topic: 'secret', level: 'assist', text: 'x' });
  const unknown = await c.send({ project: 'v', topic: 'nothing-here', level: 'assist', text: 'x' });
  assert.deepEqual([priv.status, priv.project_known], ['held', true]);
  assert.deepEqual(Object.keys(priv).sort(), Object.keys(unknown).sort());
  assert.equal(unknown.project_known, true);

  // A project whose only topic is closed is not known.
  await c.registerTopic({ project: 'w', topic: 'old', subject: 'S' });
  await c.registerTopic({ project: 'w', topic: 'old', close: true });
  const closed = await c.send({ project: 'w', topic: 'new-one', level: 'assist', text: 'x' });
  assert.deepEqual([closed.status, closed.project_known], ['held', false]);

  // A delivered message has no such field (only a held one answers it).
  assert.equal('project_known' in (await c.send({ project: 'p', topic: 'main-work', level: 'assist', text: 'x' })), false);

  const cli = await runCli(['send', '--data-dir', a.dir, '--key', a.key, '--project', 'p', '--topic', 'report-4', '--level', 'assist', '--text', 'x']);
  assert.equal(cli.code, 0, cli.err);
  assert.deepEqual([cli.json.status, cli.json.project_known], ['held', true]);
});

test('decide redirect refuses an expired held message like deliver; a private deliver too; within the deadline it redirects', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a, { thread: 'thr-a' });
  await c.registerTopic({ project: 'p', topic: 'target', subject: 'Redirect target' });
  const held = await c.send({ project: 'p', topic: 'typo', level: 'assist', text: 'q', ttl: '60s' });
  assert.equal(held.status, 'held');
  assert.deepEqual(sa.srv.service.checkExpired(Date.now() + 120_000), [held.id]);

  await assert.rejects(c.decide({ notice_id: held.notice_id, decision: 'redirect', target: 'p/target' }),
    (e) => e.status === 409 && /vypršala/.test(e.message) && /presmerovanie/.test(e.message) && /discard/.test(e.message));
  assert.equal((await c.receive({ project: 'p', topic: 'target' })).messages.length, 0);
  const kept = await c.message(held.id);
  assert.deepEqual([kept.status, kept.project, kept.topic], ['held', 'p', 'typo']);
  assert.equal((await c.notices()).notices.find((x) => x.id === held.notice_id).state, 'open');

  const cli = await runCli(['decide', '--data-dir', a.dir, '--key', a.key, '--notice', held.notice_id, '--decision', 'redirect', '--target', 'p/target']);
  assert.equal(cli.code, 1, cli.out);
  const err = JSON.parse(cli.err.split('\n')[0]);
  assert.deepEqual([err.status, err.details.message_id, err.details.decision], [409, held.id, 'redirect']);
  assert.equal((await c.receive({ project: 'p', topic: 'target' })).messages.length, 0);

  await c.decide({ notice_id: held.notice_id, decision: 'discard' });
  assert.equal((await c.message(held.id)).status, 'discarded');

  // Past expires_at before the 30 s check marked it: refused the same way.
  const quick = await c.send({ project: 'p', topic: 'typo-2', level: 'assist', text: 'q', ttl: '1s' });
  await sleep(1100);
  await assert.rejects(c.decide({ notice_id: quick.notice_id, decision: 'redirect', target: 'p/target' }), (e) => e.status === 409);
  assert.equal((await c.receive({ project: 'p', topic: 'target' })).messages.length, 0);

  // A held message for a foreign private topic: deliver (to the owner's main thread) is refused after the deadline as well.
  await c.registerTopic({ project: 'v', topic: 'secret', subject: 'S', persona: 'zed', visibility: 'private' });
  const priv = await c.send({ project: 'v', topic: 'secret', level: 'assist', text: 'q', ttl: '60s' });
  assert.equal(priv.status, 'held');
  assert.ok(sa.srv.service.checkExpired(Date.now() + 120_000).includes(priv.id));
  await assert.rejects(c.decide({ notice_id: priv.notice_id, decision: 'deliver' }), (e) => e.status === 409 && /doručenie/.test(e.message));
  assert.equal((await c.message(priv.id)).status, 'held');

  // Within its deadline a held message is still redirected.
  const fresh = await c.send({ project: 'p', topic: 'typo-3', level: 'assist', text: 'q' });
  const r = await c.decide({ notice_id: fresh.notice_id, decision: 'redirect', target: 'p/target' });
  assert.deepEqual(r.delivered_to, { project: 'p', topic: 'target' });
  assert.equal((await c.receive({ project: 'p', topic: 'target' })).messages.length, 1);
});
