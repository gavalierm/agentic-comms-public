import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, rawRequest, cleanup, api, addKey } from './helpers.mjs';

// Levels (operator 2026-11-02): blocker | steer | assist; a reply inherits the level of its original; the TTL is the
// deadline for the PICKUP (the body is read), not for the answer; a read in time means no `no reply` wait due.

async function setupPair(t) {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = api(a);
  const bob = api(a, addKey(a, 'bob'));
  await alice.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob', holder_machine: 'box-b' });
  await alice.register({ project: 'q', topic: 'u', subject: 'Src', holder: 'thr-q', holder_mode: 'auto', holder_machine: 'box-q' });
  return { a, sa, svc: sa.srv.service, alice, bob, box: { project: 'p', topic: 't', thread: 'thr-bob' } };
}

const readMsg = (inst, id) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${id}.json`), 'utf8'));
const errorOf = (status, re = null) => (e) => e.status === status && (re === null || re.test(e.json?.error || ''));

test('reply inherits the level of its original (a level in resolve is ignored, never lowers it); no reply deadline', async (t) => {
  const { a, alice, bob, box } = await setupPair(t);
  const from = { thread_id: 'thr-alice', from_project: 'q', from_topic: 'u' };
  // A steer is read (done) by its pickup: it is answered before the pickup or not at all.
  const steer = await alice.send({ project: 'p', topic: 't', level: 'steer', text: 'stop', ...from });
  assert.equal(steer.expires_at !== null, true);
  const inherited = await bob.resolve({ ...box, id: steer.id, state: 'hotovo', text: 'stopped' });
  const r1 = readMsg(a, inherited.answer_id);
  assert.deepEqual([r1.level, r1.level_raised_from ?? null, r1.expires_at, r1.ttl_s], ['steer', null, null, null]);

  const blocker = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'need you', ...from });
  const raised = await bob.resolve({ ...box, id: blocker.id, state: 'hotovo', level: 'assist', text: 'on it' });
  assert.equal(readMsg(a, raised.answer_id).level, 'blocker');

  const assist = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'help', ...from });
  const higher = await bob.resolve({ ...box, id: assist.id, state: 'hotovo', level: 'blocker', text: 'urgent' });
  assert.equal(readMsg(a, higher.answer_id).level, 'assist', 'the reply has the level of its original');
});

test('send: level is required; info and unknown levels are refused with the valid list; nothing is stored', async (t) => {
  const { a, alice } = await setupPair(t);
  const res = await alice.call('POST', '/send', { project: 'p', topic: 't', text: 'no level' });
  assert.equal(res.status, 400);
  assert.match(res.json.error, /level/);
  const bad = await alice.call('POST', '/send', { project: 'p', topic: 't', level: 'urgent', text: 'x' });
  assert.equal(bad.status, 400);
  assert.equal(fs.readdirSync(path.join(a.dir, 'messages')).length, 0);
  await assert.rejects(alice.send({ project: 'p', topic: 't', text: 'x' }), errorOf(400, /blocker\|steer\|assist/));
  await assert.rejects(alice.send({ project: 'p', topic: 't', level: 'info', text: 'x' }), (e) => e.status === 400 && /zrušená/.test(e.json.error) && /assist/.test(e.json.error));
  await assert.rejects(alice.send({ project: 'p', topic: 't', level: 'urgent', text: 'x' }), errorOf(400, /platné úrovne: blocker\|steer\|assist/));
  // Without the model header nothing is accepted at all (I10).
  const old = await rawRequest(a.socket, 'POST', '/send', { body: { project: 'p', topic: 't', level: 'assist', text: 'x' }, headers: { Authorization: `Bearer ${a.key}` } });
  assert.equal(old.status, 426);
  assert.equal(fs.readdirSync(path.join(a.dir, 'messages')).length, 0);
});

test('steer rings the holder of its topic at once: an event of type steer, no operator notice, no wait', async (t) => {
  const { a, alice, bob, svc } = await setupPair(t);
  const next = svc.lastSeq;
  const s = await bob.send({ project: 'p', topic: 't', level: 'steer', text: 'change course', thread_id: 'thr-x' });
  assert.equal(s.level, 'steer');
  assert.equal(s.wait_due_at, null, 'a steer waits for its pickup only');
  const ev = await alice.events(next);
  assert.deepEqual(ev.map((e) => e.type), ['steer']);
  assert.equal(ev[0].holder, 'thr-bob');
  assert.ok(!JSON.stringify(ev).includes('change course'), 'events never carry the body');
  assert.equal(svc.store.notices((n) => n.kind === 'blocker').length, 0);
  assert.equal(readMsg(a, s.id).status, 'pending');
});

test('pickup: receive records picked_at (count does not); receive ids reads only the named messages; a second read is no pickup', async (t) => {
  const { alice, box } = await setupPair(t);
  const m1 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'one', ttl: '10m' });
  const m2 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'two', ttl: '10m' });
  const c = await alice.count({ project: 'p', topic: 't' });
  assert.deepEqual(c.ids, [m1.id, m2.id]);
  assert.deepEqual(c.items.map((x) => x.level), ['assist', 'assist']);
  assert.equal((await alice.message(m1.id)).picked_at, null, 'count is not a pickup');

  const one = await alice.receive({ ...box, ids: [m1.id] });
  assert.deepEqual(one.map((m) => [m.id, m.status, m.picked_by_thread]), [[m1.id, 'picked', 'thr-bob']]);
  assert.equal((await alice.message(m1.id)).picked_at !== null, true);
  assert.equal((await alice.message(m2.id)).picked_at, null, 'the other message stays unread');

  // Receive returns only pending mail (I2); the picked one is read again by id.
  const again = await alice.receive(box);
  assert.deepEqual(again.map((m) => m.id), [m2.id]);
  const first = (await alice.message(m1.id)).picked_at;
  const byId = await alice.receiveId(m1.id, box);
  assert.deepEqual([byId.text, byId.status], ['one', 'picked']);
  assert.equal((await alice.message(m1.id)).picked_at, first, 'the first read is the pickup');
});

test('TTL measures pickup: a message read before its deadline gets no expiry; an unread one expires', async (t) => {
  const { alice, svc, box } = await setupPair(t);
  const read = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'read', ttl: '60s', from_project: 'q', from_topic: 'u' });
  const unread = await alice.send({ project: 'p', topic: 't', level: 'steer', text: 'unread', ttl: '60s', from_project: 'q', from_topic: 'u' });
  await alice.receive({ ...box, ids: [read.id] });
  const at = Date.now() + 120_000;
  const expired = svc.checkExpired(at);
  assert.deepEqual(expired, [unread.id], 'only the unread message expires');
  const r = await alice.message(read.id);
  assert.deepEqual([r.status, r.expired_at, r.wait_reason], ['picked', null, 'no-reply']);
  assert.ok(Date.parse(r.wait_due_at) > at, 'the reader still has the time of its answer wait');
  const u = await alice.message(unread.id);
  assert.deepEqual([u.status, typeof u.expired_at, u.wait_reason], ['pending', 'string', 'no-reply']);
  assert.ok(Date.parse(u.wait_due_at) <= at, 'the sender of the unread steer hears now');
});
