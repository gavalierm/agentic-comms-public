import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, cleanup } from './helpers.mjs';

// Levels (operator 2026-11-02): blocker | steer | assist; a reply inherits the level of its original; the TTL is the
// deadline for the PICKUP (the body is read), not for the answer; a read in time means no `no reply` bell.

async function setupPair(t) {
  const a = makeInstance('a');
  const bob = a.store.addKey('bob');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = makeClient(a, { thread: 'thr-alice', machine: 'box-a' });
  const asBob = makeClient(a, { key: bob.key });
  await alice.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob', holder_machine: 'box-b' });
  await alice.registerTopic({ project: 'q', topic: 'u', subject: 'Src', holder: 'thr-q', holder_mode: 'auto', holder_machine: 'box-q' });
  return { a, sa, svc: sa.srv.service, alice, asBob };
}

test('reply inherits the level of its original; a lower explicit level is raised; no reply deadline', async (t) => {
  const { alice, asBob } = await setupPair(t);
  const steer = await alice.send({ project: 'p', topic: 't', level: 'steer', text: 'stop', thread_id: 'thr-alice', from_project: 'q', from_topic: 'u' });
  assert.equal(steer.expires_at !== null, true);
  const inherited = await asBob.send({ reply_to: steer.id, text: 'stopped' });
  assert.equal(inherited.level, 'steer');
  assert.equal(inherited.level_raised_from, undefined);
  assert.equal(inherited.expires_at, null);

  const blocker = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'need you', thread_id: 'thr-alice', from_project: 'q', from_topic: 'u' });
  const raised = await asBob.send({ reply_to: blocker.id, level: 'assist', text: 'on it' });
  assert.deepEqual([raised.level, raised.level_raised_from], ['blocker', 'assist']);

  // one final reply per message: the higher level is tried on another original
  const steer2 = await alice.send({ project: 'p', topic: 't', level: 'steer', text: 'stop again', thread_id: 'thr-alice', from_project: 'q', from_topic: 'u' });
  const higher = await asBob.send({ reply_to: steer2.id, level: 'blocker', text: 'urgent' });
  assert.equal(higher.level, 'blocker', 'an explicit higher level is kept');
  assert.equal(higher.level_raised_from, undefined);
});

test('send: level is required except for a reply; info and unknown levels are refused with the valid list', async (t) => {
  const { alice, asBob } = await setupPair(t);
  await assert.rejects(alice.send({ project: 'p', topic: 't', text: 'x' }), (e) => e.status === 400 && /blocker\|steer\|assist/.test(e.message));
  await assert.rejects(alice.send({ project: 'p', topic: 't', level: 'info', text: 'x' }), (e) => e.status === 400 && /zrušená/.test(e.message) && /assist/.test(e.message));
  await assert.rejects(alice.send({ project: 'p', topic: 't', level: 'urgent', text: 'x' }), (e) => e.status === 400 && /platné úrovne: blocker\|steer\|assist/.test(e.message));
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'q', thread_id: 'thr-alice', from_project: 'q', from_topic: 'u' });
  await assert.rejects(asBob.send({ reply_to: q.id, level: 'info', text: 'x' }), (e) => e.status === 400);
});

test('steer rings the holder of its topic at once: an event of type steer, no operator notice', async (t) => {
  const { alice, asBob, svc } = await setupPair(t);
  const { next } = await alice.events('latest', 0);
  const s = await asBob.send({ project: 'p', topic: 't', level: 'steer', text: 'change course' });
  assert.equal(s.level, 'steer');
  const ev = (await alice.events(next, 0)).events;
  assert.deepEqual(ev.map((e) => e.type), ['steer']);
  assert.equal(ev[0].holder, 'thr-bob');
  assert.ok(!JSON.stringify(ev).includes('change course'), 'events never carry the body');
  assert.equal(svc.store.notices((n) => n.kind === 'blocker').length, 0);
});

test('pickup: receive records picked_at (count does not); receive --id reads only the named messages', async (t) => {
  const { alice } = await setupPair(t);
  const m1 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'one', ttl: '10m' });
  const m2 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'two', ttl: '10m' });
  const c = await alice.count({ project: 'p', topic: 't' });
  assert.deepEqual(c.ids, [m1.id, m2.id]);
  assert.deepEqual(c.items.map((x) => x.level), ['assist', 'assist']);
  assert.equal((await alice.message(m1.id)).picked_at, null, 'count is not a pickup');

  const one = await alice.receive({ project: 'p', topic: 't', ids: m1.id });
  assert.deepEqual(one.messages.map((m) => m.id), [m1.id]);
  assert.equal((await alice.message(m1.id)).picked_at !== null, true);
  assert.equal((await alice.message(m2.id)).picked_at, null, 'the other message stays unread');

  const again = await alice.receive({ project: 'p', topic: 't' });
  assert.deepEqual(again.messages.map((m) => m.id).sort(), [m1.id, m2.id].sort());
  const first = (await alice.message(m1.id)).picked_at;
  await alice.receive({ project: 'p', topic: 't', ids: m1.id });
  assert.equal((await alice.message(m1.id)).picked_at, first, 'the first read is the pickup');
});

test('TTL measures pickup: a message read before its deadline gets no expiry; an unread one expires', async (t) => {
  const { alice, svc } = await setupPair(t);
  const read = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'read', ttl: '60s', from_project: 'q', from_topic: 'u' });
  const unread = await alice.send({ project: 'p', topic: 't', level: 'steer', text: 'unread', ttl: '60s', from_project: 'q', from_topic: 'u' });
  await alice.receive({ project: 'p', topic: 't', ids: read.id });
  const expired = svc.checkExpired(Date.now() + 120_000);
  assert.deepEqual(expired, [unread.id], 'only the unread message expires');
  assert.equal((await alice.message(read.id)).outcome, null);
  assert.equal((await alice.message(unread.id)).outcome, 'expired');
});

test('a legacy info message (stored before the change) reads as assist without a deadline', async (t) => {
  const { a, alice, svc } = await setupPair(t);
  const m = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'old', thread_id: 'thr-alice' });
  const file = path.join(a.dir, 'messages', `${m.id}.json`);
  const stored = JSON.parse(fs.readFileSync(file, 'utf8'));
  fs.writeFileSync(file, JSON.stringify({ ...stored, level: 'info', ttl_s: null, expires_at: null }));
  const st = await alice.message(m.id);
  assert.deepEqual([st.level, st.expires_at], ['assist', null]);
  const got = await alice.receive({ project: 'p', topic: 't', ids: m.id });
  assert.deepEqual(got.messages.map((x) => [x.id, x.level]), [[m.id, 'assist']]);
  assert.deepEqual(svc.checkExpired(Date.now() + 10 * 3600_000), [], 'no deadline, nothing expires');
  assert.deepEqual((await alice.ack([m.id])).acked, [m.id]);
});
