// keep: core
// Critical core of the mail model (design mail-service-design v3, part 5a; docs/MODEL.md): invariants I1–I11, migration 4b
// and schema 3.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, api, addKey, rawRequest, cleanup, sleep } from './helpers.mjs';
import { transition, TRANSITIONS, STATUSES, findTransition } from '../src/server/lifecycle.mjs';
import { startServer } from '../src/server/index.mjs';
import { Store } from '../src/server/store.mjs';
import { migrateData } from '../src/server/migrate.mjs';

const P = 'proj';
const T = 'topic-a';
const RECIPIENT = 'thread-r';
const SENDER_THREAD = 'thread-s';

/** Instance with the recipient topic (held by RECIPIENT) and the sender topic (held by SENDER_THREAD). */
async function setup() {
  const inst = makeInstance('core');
  const { srv, stop } = await startInstance(inst);
  const a = api(inst);
  await a.register({ project: P, topic: T, holder: RECIPIENT });
  await a.register({ project: P, topic: 'src', holder: SENDER_THREAD });
  const send = (extra = {}) => a.send({ project: P, topic: T, level: 'assist', text: 'please', thread_id: SENDER_THREAD, from_project: P, from_topic: 'src', ...extra });
  const box = { project: P, topic: T, thread: RECIPIENT };
  return { inst, srv, stop, a, send, box, done: async () => { await stop(); cleanup(inst.dir); } };
}

const readMsg = (inst, id) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${id}.json`), 'utf8'));
const writeMsg = (inst, m) => fs.writeFileSync(path.join(inst.dir, 'messages', `${m.id}.json`), JSON.stringify(m, null, 2));

test('I1: every transition of table 1b passes, any other is refused with 409 and the current state', () => {
  const ops = [...new Set(TRANSITIONS.map((t) => t.op))];
  const resolutionsFor = (to) => (to === 'done' ? ['reply', 'settled', 'acked', 'read', 'withdrawn'] : [null]);
  let allowed = 0;
  let refused = 0;
  for (const from of [null, ...STATUSES]) {
    for (const to of STATUSES) {
      for (const op of ops) {
        for (const resolution of resolutionsFor(to)) {
          const m = { id: 'x', status: from ?? undefined, resolution: from === 'done' ? 'reply' : null };
          const ok = !!findTransition(from, to, op, resolution);
          if (ok) {
            transition(m, to, op, 'thread', { resolution });
            assert.equal(m.status, to);
            assert.equal(m.resolution, to === 'done' ? resolution : null);
            assert.equal(m.history.at(-1).op, op);
            allowed++;
          } else {
            assert.throws(() => transition(m, to, op, 'thread', { resolution }), (e) => e.status === 409 && e.extra.status === (from ?? null));
            assert.equal(m.status, from ?? undefined, 'refused transition changes nothing');
            refused++;
          }
        }
      }
    }
  }
  assert.ok(allowed > 20 && refused > 500, `${allowed} allowed, ${refused} refused`);
  // Final statuses never move except done → done (cancels) by resolve.
  for (const from of ['discarded', 'superseded', 'undelivered']) {
    for (const to of STATUSES) for (const op of ops) assert.equal(findTransition(from, to, op, to === 'done' ? 'reply' : null), null, `${from} → ${to} by ${op}`);
  }
});

test('I1: a refused resolve over HTTP is 409 with {status, resolution, answer_state, answer_id}', async () => {
  const s = await setup();
  try {
    const { id } = await s.send();
    await s.a.receive(s.box);
    const first = await s.a.resolve({ ...s.box, id, state: 'hotovo', text: 'done' });
    const r = await s.a.call('POST', '/resolve', { ...s.box, id, state: 'čakám', text: 'later' });
    assert.equal(r.status, 409);
    assert.deepEqual({ status: r.json.status, resolution: r.json.resolution, answer_state: r.json.answer_state, answer_id: r.json.answer_id },
      { status: 'done', resolution: 'reply', answer_state: 'hotovo', answer_id: first.answer_id });
  } finally { await s.done(); }
});

test('I2: resolve hotovo with text, without text and ack → not in count, receive, inbox', async () => {
  const s = await setup();
  try {
    const ids = [];
    for (let i = 0; i < 3; i++) ids.push((await s.send({ text: `m${i}` })).id);
    assert.equal((await s.a.count(s.box)).total, 3);
    await s.a.receive(s.box);
    await s.a.resolve({ ...s.box, id: ids[0], state: 'hotovo', text: 'answer' });
    await s.a.resolve({ ...s.box, id: ids[1], state: 'hotovo' });
    await s.a.resolve({ ...s.box, id: ids[2], state: 'hotovo', ack: true });
    assert.deepEqual([ids[0], ids[1], ids[2]].map((id) => readMsg(s.inst, id).resolution), ['reply', 'settled', 'acked']);
    assert.equal((await s.a.count(s.box)).total, 0);
    assert.equal((await s.a.receive(s.box)).length, 0);
    const ob = await s.a.obligations({ thread: RECIPIENT });
    assert.equal(ob.inbox.n, 0);
    assert.equal(ob.owed.length, 0);
  } finally { await s.done(); }
});

test('I2: a code answer on a pending message keeps it in the inbox; receive returns it', async () => {
  const s = await setup();
  try {
    await s.a.turn({ thread: RECIPIENT, state: 'open' });
    await s.a.turn({ thread: RECIPIENT, waiting_operator: true });
    const { id } = await s.send({ ttl: '1s' });
    s.srv.service.checkExpired(Date.now() + 5000);
    const m = readMsg(s.inst, id);
    assert.equal(m.status, 'pending');
    assert.equal(m.code_answer, 'otázka');
    assert.equal(m.wait_reason, 'check');
    assert.deepEqual((await s.a.count(s.box)).ids, [id]);
    const ob = await s.a.obligations({ thread: RECIPIENT });
    assert.deepEqual(ob.inbox.ids.map((x) => [x.id, x.code_answer]), [[id, 'otázka']]);
    const got = await s.a.receive(s.box);
    assert.deepEqual(got.map((x) => x.id), [id]);
    assert.equal(got[0].status, 'picked');
  } finally { await s.done(); }
});

test('I2 receive of a reply and of a steer → done (read), never in the inbox nor owed (I8)', async () => {
  const s = await setup();
  try {
    const { id } = await s.send();
    await s.a.receive(s.box);
    const r = await s.a.resolve({ ...s.box, id, state: 'otázka', text: 'which one?' });
    const steer = await s.send({ level: 'steer', text: 'stop' });
    const got = await s.a.receive(s.box);
    assert.equal(got.find((x) => x.id === steer.id).status, 'done');
    assert.equal(readMsg(s.inst, steer.id).resolution, 'read');
    const replies = await s.a.receive({ project: P, topic: 'src', thread: SENDER_THREAD });
    assert.deepEqual(replies.map((x) => [x.id, x.status]), [[r.answer_id, 'done']]);
    assert.equal(readMsg(s.inst, r.answer_id).resolution, 'read');
    const owedR = (await s.a.obligations({ thread: RECIPIENT })).owed.map((x) => x.id);
    const owedS = (await s.a.obligations({ thread: SENDER_THREAD })).owed.map((x) => x.id);
    assert.deepEqual(owedR, [id]);
    assert.deepEqual(owedS, []);
  } finally { await s.done(); }
});

test('I1: ack from picked → done (acked); from waiting → 409 with the state', async () => {
  const s = await setup();
  try {
    const a1 = await s.send();
    const a2 = await s.send({ text: 'second' });
    await s.a.receive(s.box);
    const r1 = await s.a.resolve({ ...s.box, id: a1.id, state: 'hotovo', ack: true });
    assert.deepEqual([r1.status, r1.resolution], ['done', 'acked']);
    await s.a.resolve({ ...s.box, id: a2.id, state: 'čakám', text: 'on it' });
    const r2 = await s.a.call('POST', '/resolve', { ...s.box, id: a2.id, state: 'hotovo', ack: true });
    assert.equal(r2.status, 409);
    assert.equal(r2.json.status, 'waiting');
    assert.equal(readMsg(s.inst, a2.id).status, 'waiting');
  } finally { await s.done(); }
});

test('I9 closing the recipient topic keeps picked/waiting/done (waits due, recipient-closed); pending → undelivered (I3)', async () => {
  const s = await setup();
  try {
    const picked = await s.send({ text: 'p' });
    const waiting = await s.send({ text: 'w' });
    const done = await s.send({ text: 'd' });
    await s.a.receive(s.box);
    await s.a.resolve({ ...s.box, id: waiting.id, state: 'čakám', text: 'later' });
    await s.a.resolve({ ...s.box, id: done.id, state: 'hotovo' });
    const pending = await s.send({ text: 'unread' });
    const r = await s.a.register({ project: P, topic: T, holder: RECIPIENT, close: true });
    assert.deepEqual(r.undelivered, [pending.id]);
    assert.deepEqual(r.recipient_closed.sort(), [picked.id, waiting.id].sort());
    assert.equal(readMsg(s.inst, pending.id).status, 'undelivered');
    assert.equal(readMsg(s.inst, pending.id).wait_reason, 'undelivered');
    for (const [id, st] of [[picked.id, 'picked'], [waiting.id, 'waiting'], [done.id, 'done']]) assert.equal(readMsg(s.inst, id).status, st);
    const now = new Date().toISOString();
    for (const id of [picked.id, waiting.id]) {
      const m = readMsg(s.inst, id);
      assert.equal(m.wait_reason, 'recipient-closed');
      assert.ok(m.wait_due_at <= now);
    }
    assert.equal(readMsg(s.inst, done.id).wait_due_at, null);
  } finally { await s.done(); }
});

test('I3 undelivered only from pending/held: bounce of a picked or done message changes nothing; held → undelivered closes its notice (I6)', async () => {
  const s = await setup();
  try {
    const picked = await s.send({ text: 'p' });
    const done = await s.send({ text: 'd' });
    await s.a.receive(s.box);
    await s.a.resolve({ ...s.box, id: done.id, state: 'hotovo' });
    for (const id of [picked.id, done.id]) {
      const before = readMsg(s.inst, id);
      const r = await s.a.bounce({ message_id: id, reason: 'gone' });
      assert.equal(r.bounced, false);
      assert.deepEqual(readMsg(s.inst, id), before);
    }
    const held = await s.a.send({ project: P, topic: 'nobody', level: 'assist', text: 'h', thread_id: SENDER_THREAD });
    assert.equal(held.status, 'held');
    assert.equal((await s.a.notices()).filter((n) => n.message_id === held.id).length, 1);
    const hs = await s.a.message(held.id);
    assert.deepEqual([hs.held_reason, hs.notice_id], ['unregistered-topic', held.notice_id], 'held names its reason (flow-public#107)');
    assert.equal((await s.a.bounce({ message_id: held.id })).bounced, true);
    assert.equal(readMsg(s.inst, held.id).status, 'undelivered');
    const n = (await s.a.notices(true)).find((x) => x.message_id === held.id);
    assert.deepEqual([n.state, n.decision], ['decided', 'undelivered']);
  } finally { await s.done(); }
});

test('I4: a second resolve hotovo → 409; the same client_op_id → the original confirmation', async () => {
  const s = await setup();
  try {
    const { id } = await s.send();
    await s.a.receive(s.box);
    const first = await s.a.resolve({ ...s.box, id, state: 'hotovo', text: 'answer', client_op_id: 'op-1' });
    const again = await s.a.resolve({ ...s.box, id, state: 'hotovo', text: 'answer', client_op_id: 'op-1' });
    assert.equal(again.answer_id, first.answer_id);
    assert.equal(again.replayed, true);
    const second = await s.a.call('POST', '/resolve', { ...s.box, id, state: 'hotovo', text: 'other', client_op_id: 'op-2' });
    assert.equal(second.status, 409);
    assert.equal(second.json.answer_id, first.answer_id);
    const replies = fs.readdirSync(path.join(s.inst.dir, 'messages')).map((f) => JSON.parse(fs.readFileSync(path.join(s.inst.dir, 'messages', f), 'utf8'))).filter((m) => m.reply_to === id);
    assert.equal(replies.length, 1, 'one final reply');
    // send: the same client_op_id gives the same message.
    const s1 = await s.send({ client_op_id: 'send-1' });
    const s2 = await s.send({ client_op_id: 'send-1' });
    assert.equal(s2.id, s1.id);
  } finally { await s.done(); }
});

test('I5: a reply written without its original (stop between the writes) is finished by repair at start', async () => {
  const s = await setup();
  let s2;
  try {
    const { id } = await s.send({ level: 'blocker' });
    await s.a.receive(s.box);
    const n = (await s.a.notices()).find((x) => x.message_id === id);
    assert.ok(n, 'blocker notice open');
    const orig = readMsg(s.inst, id);
    await s.stop();
    // The reply is on disk, the original still `picked` (the second write never happened).
    const reply = {
      id: '20991231T000000000Z-0000aaaa', project: P, topic: 'src', persona: null, to_thread: SENDER_THREAD, level: 'blocker', text: 'answer',
      sender: s.inst.sender, thread_id: RECIPIENT, from_project: P, from_topic: T, reply_to: id, reply_state: 'hotovo', cancels: null,
      created_at: new Date().toISOString(), status: 'pending', resolution: null, history: [], picked_at: null,
    };
    writeMsg(s.inst, reply);
    assert.equal(orig.status, 'picked');
    s2 = await startInstance(s.inst);
    assert.deepEqual(s2.srv.repaired, [id]);
    const after = readMsg(s.inst, id);
    assert.deepEqual([after.status, after.resolution, after.answer_id], ['done', 'reply', reply.id]);
    assert.equal(after.history.at(-1).op, 'repair');
    const n2 = (await api(s.inst).notices(true)).find((x) => x.id === n.id);
    assert.equal(n2.state, 'decided');
    const ev = (await api(s.inst).events(0)).filter((e) => e.type === 'reply' && e.message_id === reply.id);
    assert.equal(ev.length, 1, 'reply event added');
  } finally {
    if (s2) await s2.stop();
    cleanup(s.inst.dir);
  }
});

test('I6: resolve, decide and withdraw close the open notice', async () => {
  const s = await setup();
  try {
    const b1 = await s.send({ level: 'blocker', text: 'b1' });
    const b2 = await s.send({ level: 'blocker', text: 'b2' });
    const held = await s.a.send({ project: P, topic: 'nobody', level: 'assist', text: 'h', thread_id: SENDER_THREAD });
    await s.a.receive(s.box);
    await s.a.resolve({ ...s.box, id: b1.id, state: 'hotovo', text: 'ok' });
    await s.a.withdraw({ id: b2.id });
    const heldNotice = (await s.a.notices()).find((n) => n.message_id === held.id);
    await s.a.decide({ notice_id: heldNotice.id, decision: 'discard' });
    const all = await s.a.notices(true);
    const decisionOf = (id) => all.find((n) => n.message_id === id);
    assert.deepEqual([decisionOf(b1.id).state, decisionOf(b1.id).decision], ['decided', 'answered']);
    assert.deepEqual([decisionOf(b2.id).state, decisionOf(b2.id).decision], ['decided', 'withdrawn']);
    assert.deepEqual([decisionOf(held.id).state, decisionOf(held.id).decision], ['decided', 'discard']);
    assert.equal(readMsg(s.inst, held.id).status, 'discarded');
    assert.equal((await s.a.notices()).length, 0);
  } finally { await s.done(); }
});

test('I7: due + wait-rang: unconfirmed stays due after 2 h; a confirmation doubles; after 4 it ends with a recent row; done is not due', async () => {
  const s = await setup();
  try {
    const wake = api(s.inst, addKey(s.inst, 'wake'));
    const { id } = await s.send({ level: 'blocker' });
    const other = await s.send({ text: 'other' });
    const m = readMsg(s.inst, id);
    assert.equal(m.wait_base_s, 900);
    assert.equal(Date.parse(m.wait_due_at) - Date.parse(m.created_at), 900_000);
    const due0 = Date.parse(m.wait_due_at);
    // Not confirmed: still due two hours later (wake was off).
    const later = s.srv.service.due('wake', due0 + 2 * 3600_000).due.map((x) => x.id);
    assert.ok(later.includes(id));
    // Another key (a wake module on another machine) sees and confirms only the waits of the messages it sent.
    assert.ok(s.srv.service.due(readMsg(s.inst, id).sender, due0).due.some((x) => x.id === id), 'the sender key sees its wait');
    const bob = api(s.inst, addKey(s.inst, 'bob'));
    assert.ok(!(await bob.due()).due.some((x) => x.id === id) && !s.srv.service.due('bob', due0).due.length, 'not the waits of others');
    assert.equal((await bob.call('POST', '/wait-rang', { id })).status, 403, 'confirm: the wake key or the sender');
    let rang = due0 + 2 * 3600_000;
    const dues = [];
    for (let i = 1; i <= 4; i++) {
      const r = await wake.waitRang({ id, rang_at: new Date(rang).toISOString() });
      assert.equal(r.wait_n, i);
      if (i < 4) {
        assert.equal(Date.parse(r.wait_due_at) - rang, 900_000 * 2 ** i);
        // A repeated confirmation of the same ring changes nothing.
        const again = await wake.waitRang({ id, rang_at: new Date(rang).toISOString() });
        assert.equal(again.wait_n, i);
        dues.push(r.wait_due_at);
        rang = Date.parse(r.wait_due_at);
      } else {
        assert.equal(r.wait_due_at, null);
      }
    }
    assert.deepEqual(dues.map((d, i) => (Date.parse(d) - (i ? Date.parse(dues[i - 1]) : due0 + 2 * 3600_000)) / 900_000), [2, 4, 8]);
    const ob = await s.a.obligations({ thread: SENDER_THREAD });
    assert.ok(ob.recent.some((r) => r.id === id && r.line === `no answer after 4 checks ${id}`));
    assert.equal(readMsg(s.inst, id).status, 'pending', 'the message itself stays');
    // A final message is never due; a confirmation is 409.
    await s.a.receive(s.box);
    await s.a.resolve({ ...s.box, id: other.id, state: 'hotovo' });
    assert.ok(!s.srv.service.due('wake', Date.now() + 365 * 86400_000).due.some((x) => x.id === other.id));
    assert.equal((await wake.call('POST', '/wait-rang', { id: other.id })).status, 409);
    // An interim answer of the recipient rings the sender once, at the end of the span (operator 2026-11-21: four rings
    // with no step for two messages answered `otázka`): 4 h × 15, then the wait ends with a recent row.
    const asked = await s.send({ text: 'third' });
    await s.a.receive(s.box);
    // A picked assist is with its recipient: its sender is rung once, 15 h after the pickup.
    assert.ok(Math.abs(Date.parse(readMsg(s.inst, asked.id).wait_due_at) - Date.now() - 15 * 3600_000) < 5000, 'picked assist: one check after 15 h');
    const t0 = Date.now();
    await s.a.resolve({ ...s.box, id: asked.id, state: 'otázka', text: 'which one?' });
    const w = readMsg(s.inst, asked.id);
    assert.ok(Math.abs(Date.parse(w.wait_due_at) - t0 - 60 * 3600_000) < 5000, w.wait_due_at);
    assert.ok(!s.srv.service.due('wake', t0 + 59 * 3600_000).due.some((x) => x.id === asked.id), 'no ring before the end of the span');
    assert.ok(s.srv.service.due('wake', t0 + 61 * 3600_000).due.some((x) => x.id === asked.id));
    assert.equal((await wake.waitRang({ id: asked.id, rang_at: new Date(t0 + 61 * 3600_000).toISOString() })).wait_due_at, null, 'one ring ends the wait');
    assert.ok((await s.a.obligations({ thread: SENDER_THREAD })).recent.some((r) => r.id === asked.id && /^no final answer \S+ \(interim otázka since /.test(r.line)));
  } finally { await s.done(); }
});

test('I8: owed of thread B holds a message picked by thread A of the same topic; after resolve by A it does not', async () => {
  const s = await setup();
  try {
    await s.a.register({ project: P, topic: 'shared', holder: 'thread-b' });
    const box = { project: P, topic: 'shared' };
    const { id } = await s.a.send({ ...box, level: 'assist', text: 'x', thread_id: SENDER_THREAD });
    await s.a.receive({ ...box, thread: 'thread-a' });
    const owedB = await s.a.obligations({ thread: 'thread-b' });
    assert.deepEqual(owedB.owed.map((x) => [x.id, x.picked_by_thread]), [[id, 'thread-a']]);
    // Thread B reads it again by id (the answer of A's receive was lost to B).
    const again = await s.a.receiveId(id, { ...box, thread: 'thread-b' });
    assert.equal(again.text, 'x');
    await s.a.resolve({ ...box, thread: 'thread-a', id, state: 'hotovo', text: 'done' });
    assert.deepEqual((await s.a.obligations({ thread: 'thread-b' })).owed, []);
  } finally { await s.done(); }
});

test('I9: resolve when the sender closed its topic → 200, answer_delivered=false, reason sender-closed', async () => {
  const s = await setup();
  try {
    const { id } = await s.a.send({ project: P, topic: T, level: 'assist', text: 'q', from_project: P, from_topic: 'src' });
    await s.a.receive(s.box);
    await s.a.register({ project: P, topic: 'src', holder: SENDER_THREAD, close: true });
    const r = await s.a.resolve({ ...s.box, id, state: 'hotovo', text: 'answer' });
    assert.deepEqual([r.status, r.resolution, r.answer_delivered, r.reason], ['done', 'reply', false, 'sender-closed']);
    assert.equal(readMsg(s.inst, r.answer_id).status, 'undelivered');
  } finally { await s.done(); }
});

test('I10: a request without X-Comms-Model: 3 → 426, nothing written', async () => {
  const s = await setup();
  try {
    const before = fs.readdirSync(path.join(s.inst.dir, 'messages')).length;
    const clients = fs.readdirSync(path.join(s.inst.dir, 'clients')).length;
    for (const model of [undefined, '1', '2']) {
      const headers = { Authorization: `Bearer ${s.inst.key}`, ...(model ? { 'X-Comms-Model': model } : {}) };
      const r = await rawRequest(s.inst.socket, 'POST', '/send', { body: { project: P, topic: T, level: 'assist', text: 'x' }, headers });
      assert.equal(r.status, 426);
      assert.match(r.json.error, /mail model 3 .*update mail and workflow together/);
    }
    assert.equal(fs.readdirSync(path.join(s.inst.dir, 'messages')).length, before);
    assert.equal(fs.readdirSync(path.join(s.inst.dir, 'clients')).length, clients);
    const h = await rawRequest(s.inst.socket, 'GET', '/health');
    assert.equal(h.json.model, 3);
    assert.ok(h.json.version);
  } finally { await s.done(); }
});

test('1d: GET /message and receive?id without a right to the mailbox give no body', async () => {
  const s = await setup();
  try {
    const bobKey = addKey(s.inst, 'bob');
    const carolKey = addKey(s.inst, 'carol');
    const bob = api(s.inst, bobKey);
    const carol = api(s.inst, carolKey);
    await bob.register({ project: 'bobp', topic: 'inbox', persona: 'bob', holder: 'thread-bob' });
    const { id } = await s.a.send({ project: 'bobp', topic: 'inbox', level: 'assist', text: 'secret', thread_id: SENDER_THREAD });
    const box = { project: 'bobp', topic: 'inbox', thread: 'thread-bob' };
    // carol claims bob's mailbox: refused.
    const meta = await carol.message(id, box);
    assert.equal(meta.status, 'pending');
    assert.equal('text' in meta, false);
    assert.equal((await carol.call('GET', `/receive?id=${id}&project=bobp&topic=inbox`)).status, 403);
    assert.equal((await carol.call('POST', '/resolve', { ...box, id, state: 'hotovo' })).status, 403);
    assert.equal(readMsg(s.inst, id).status, 'pending', 'nothing picked up');
    // The sender and the mailbox owner read it.
    assert.equal((await s.a.message(id)).text, 'secret');
    assert.equal((await bob.receiveId(id, box)).text, 'secret');
    assert.equal(readMsg(s.inst, id).status, 'picked');
  } finally { await s.done(); }
});

// --- migration 4b --------------------------------------------------------------------------------------
function old(id, fields) {
  return { id, project: P, topic: T, persona: null, to_thread: null, level: 'assist', text: id, sender: 'alice', thread_id: SENDER_THREAD, from_project: P, from_topic: 'src', reply_to: null, created_at: `2026-11-01T00:00:${String(id.length).padStart(2, '0')}.000Z`, state: 'pending', acked_at: null, notice_id: null, ttl_s: 1800, expires_at: '2026-11-01T00:30:00.000Z', expired_at: null, outcome: null, ...fields };
}

const SAMPLE = [
  [old('discarded', { state: 'discarded' }), 'discarded'],
  [old('superseded', { state: 'acked', outcome: 'superseded', superseded_by: 'pending' }), 'superseded'],
  [old('held', { state: 'held' }), 'held'],
  [old('reply-read', { reply_to: 'answered', reply_state: 'hotovo', picked_at: '2026-11-01T01:00:00.000Z', state: 'acked' }), 'done/read'],
  [old('reply-unread', { reply_to: 'interim', reply_state: 'otázka' }), 'pending'],
  [old('reply-nostate', { reply_to: 'nostate', picked_at: '2026-11-01T01:00:00.000Z' }), 'done/read'],
  [old('steer-read', { level: 'steer', picked_at: '2026-11-01T00:01:00.000Z' }), 'done/read'],
  [old('answered', { picked_at: '2026-11-01T00:01:00.000Z', answer_state: 'hotovo', answer_id: 'reply-read', answer_origin: 'reply' }), 'done/reply'],
  [old('settled', { picked_at: '2026-11-01T00:01:00.000Z', answer_state: 'hotovo', answer_origin: 'settled' }), 'done/settled'],
  [old('interim', { picked_at: '2026-11-01T00:01:00.000Z', answer_state: 'otázka', answer_id: 'reply-unread' }), 'waiting'],
  [old('nostate', { picked_at: '2026-11-01T00:01:00.000Z' }), 'done/reply'],
  [old('busy', { answer_state: 'busy', answer_origin: 'code', answer_at: '2026-11-01T00:30:00.000Z' }), 'pending'],
  [old('busy-picked', { answer_state: 'otázka', answer_origin: 'code', picked_at: '2026-11-01T00:40:00.000Z' }), 'picked'],
  [old('acked', { state: 'acked', acked_at: '2026-11-01T00:05:00.000Z' }), 'done/acked'],
  // P14 (prod data): a real reply over a code answer kept `answer_origin: code`; a code answer the recipient acked is not owed.
  [old('code-replied', { state: 'acked', picked_at: '2026-11-01T00:40:00.000Z', answer_state: 'hotovo', answer_origin: 'code', answer_id: 'reply-read' }), 'done/reply'],
  [old('code-acked', { state: 'acked', picked_at: '2026-11-01T00:40:00.000Z', answer_state: 'otázka', answer_origin: 'code' }), 'done/acked'],
  [old('picked-bounced', { picked_at: '2026-11-01T00:01:00.000Z', bounced_at: '2026-11-01T00:02:00.000Z' }), 'picked'],
  [old('bounced', { bounced_at: '2026-11-01T00:02:00.000Z' }), 'undelivered'],
  [old('undelivered', { outcome: 'undelivered', undelivered_at: '2026-11-01T00:02:00.000Z' }), 'undelivered'],
  [old('expired', { outcome: 'expired', expired_at: '2026-11-01T00:30:00.000Z' }), 'pending'],
  [old('pending', {}), 'pending'],
  [old('info', { level: 'info' }), 'pending'],
];

function oldInstance(records, notices = []) {
  const inst = makeInstance('migr');
  fs.rmSync(path.join(inst.dir, 'meta.json'));
  // The recipient topic as 0.4 and model 2 stored it (`<project>__<topic>.json`, no brain).
  fs.writeFileSync(path.join(inst.dir, 'topics', `${P}__${T}.json`), JSON.stringify({ project: P, topic: T, subject: 'S', holder: RECIPIENT, state: 'open' }));
  for (const m of records) writeMsg(inst, m);
  for (const n of notices) fs.writeFileSync(path.join(inst.dir, 'notices', `${n.id}.json`), JSON.stringify(n));
  return inst;
}

test('4b: migration of a 0.4 sample (bounced_at, replies without answer_state, busy, steer) → expected status; second start changes nothing', async () => {
  const notices = [
    { id: 'n-acked', kind: 'blocker', state: 'open', message_id: 'acked' },
    { id: 'n-pending', kind: 'blocker', state: 'open', message_id: 'pending' },
  ];
  const inst = oldInstance(SAMPLE.map(([m]) => m), notices);
  const logs = [];
  try {
    const srv = await startServer({ dataDir: inst.dir, log: (l) => logs.push(l) });
    try {
      assert.equal(srv.migration.migrated, true);
      assert.ok(fs.existsSync(srv.migration.backup), 'backup folder');
      assert.ok(fs.existsSync(path.join(srv.migration.backup, 'messages', 'bounced.json')));
      for (const [m, want] of SAMPLE) {
        const got = readMsg(inst, m.id);
        assert.equal(got.resolution ? `${got.status}/${got.resolution}` : got.status, want, m.id);
        for (const k of ['state', 'acked_at', 'outcome', 'bounced_at']) assert.equal(k in got, false, `${m.id} keeps ${k}`);
        assert.equal(got.history[0].op, 'migration');
      }
      assert.equal(readMsg(inst, 'busy').code_answer, 'busy');
      assert.equal('answer_state' in readMsg(inst, 'busy'), false);
      assert.equal(readMsg(inst, 'nostate').answer_id, 'reply-nostate');
      assert.equal(readMsg(inst, 'info').level, 'assist');
      assert.ok(readMsg(inst, 'expired').expired_at);
      const ns = Object.fromEntries(notices.map((n) => [n.id, JSON.parse(fs.readFileSync(path.join(inst.dir, 'notices', `${n.id}.json`), 'utf8'))]));
      assert.deepEqual([ns['n-acked'].state, ns['n-acked'].decision], ['decided', 'migrated']);
      assert.equal(ns['n-pending'].state, 'open');
      assert.ok(logs.some((l) => /migration 4b: \d+ messages/.test(l)), 'counts logged');
      // No false owed: the recipient owes only what was picked without an answer.
      const ob = (await api(inst).call('GET', `/obligations?thread=${RECIPIENT}`, undefined, { 'X-Comms-Brain': '' })).json;
      assert.deepEqual(ob.owed.map((x) => x.id).sort(), ['busy-picked', 'interim', 'picked-bounced']);
    } finally { await srv.close(); }
    const snapshot = SAMPLE.map(([m]) => fs.readFileSync(path.join(inst.dir, 'messages', `${m.id}.json`), 'utf8'));
    const srv2 = await startServer({ dataDir: inst.dir });
    try {
      assert.equal(srv2.migration.migrated, false);
      assert.deepEqual(SAMPLE.map(([m]) => fs.readFileSync(path.join(inst.dir, 'messages', `${m.id}.json`), 'utf8')), snapshot);
    } finally { await srv2.close(); }
  } finally {
    for (const d of fs.readdirSync(path.dirname(inst.dir)).filter((d) => d.startsWith(`${path.basename(inst.dir)}.bak-`))) cleanup(path.join(path.dirname(inst.dir), d));
    cleanup(inst.dir);
  }
});

test('4b: verification catches a planted error: the service does not start and names the ids', async () => {
  const inst = oldInstance([old('fine', {}), old('weird', { state: 'lost' })]);
  try {
    await assert.rejects(startServer({ dataDir: inst.dir }), (e) => e.code === 'MIGRATION_FAILED' && /weird/.test(e.message) && !/fine/.test(e.message));
    assert.equal(fs.existsSync(path.join(inst.dir, 'meta.json')), false, 'not marked migrated');
    assert.equal(fs.existsSync(path.join(inst.dir, 'comms.sock')), false, 'not listening');
    // An invariant broken after the write (a migrated record already owed as a steer) is caught too.
    fs.rmSync(path.join(inst.dir, 'messages', 'weird.json'));
    writeMsg(inst, { ...old('steer-owed', { level: 'steer' }), state: undefined, status: 'picked', resolution: null, history: [] });
    await assert.rejects(startServer({ dataDir: inst.dir }), (e) => e.code === 'MIGRATION_FAILED' && /steer-owed/.test(e.message));
  } finally {
    for (const d of fs.readdirSync(path.dirname(inst.dir)).filter((d) => d.startsWith(`${path.basename(inst.dir)}.bak-`))) cleanup(path.join(path.dirname(inst.dir), d));
    cleanup(inst.dir);
  }
});

// --- I11: topic identity brain + machine + project + topic (model 3) ------------------------------------------------
/** Raw API as a brain on a machine (X-Comms-Brain, X-Comms-Machine); brain null sends none. */
const as = (inst, brain, machine = null, key = inst.key) => api(inst, key, { 'X-Comms-Brain': brain ?? '', ...(machine ? { 'X-Comms-Machine': machine } : {}) });
const topicRecs = (inst) => fs.readdirSync(path.join(inst.dir, 'topics')).map((f) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'topics', f), 'utf8')));
const recOf = (inst, brain, machine, project, topic) => topicRecs(inst).find((t) => (t.brain ?? null) === brain && t.machine === machine && t.project === project && t.topic === topic) || null;
const bakCleanup = (inst) => { for (const d of fs.readdirSync(path.dirname(inst.dir)).filter((d) => d.startsWith(`${path.basename(inst.dir)}.bak-`))) cleanup(path.join(path.dirname(inst.dir), d)); };

test('I11: takeover on another machine moves the open messages and closes the old record; a reply follows moved_to', async () => {
  const inst = makeInstance('i11');
  const { stop } = await startInstance(inst);
  try {
    const m1 = as(inst, 'b1', 'm1');
    const m2 = as(inst, 'b1', 'm2');
    await m1.register({ project: P, topic: T, holder: RECIPIENT });
    await m1.register({ project: P, topic: 'src', holder: SENDER_THREAD });
    const sent = (text) => m1.send({ project: P, topic: T, level: 'assist', text, from_project: P, from_topic: 'src' });
    const picked = await sent('picked');
    await m1.receive({ project: P, topic: T, thread: RECIPIENT });
    const pending = await sent('pending');
    const since = (await m1.events()).at(-1).seq;
    // Another holder on m2 without takeover: 409, nothing moves.
    assert.equal((await m2.call('POST', '/register-topic', { project: P, topic: T, subject: 'S', holder: 'thread-r2' })).status, 409);
    assert.equal(readMsg(inst, pending.id).to_machine, 'm1');
    // With takeover: new record, the mailbox's non-final messages on the new key (status unchanged), old record closed last.
    const r = await m2.register({ project: P, topic: T, holder: 'thread-r2', takeover: true });
    assert.deepEqual(r.moved.sort(), [picked.id, pending.id].sort());
    assert.deepEqual([readMsg(inst, picked.id).status, readMsg(inst, pending.id).status], ['picked', 'pending']);
    for (const id of [picked.id, pending.id]) assert.deepEqual([readMsg(inst, id).to_brain, readMsg(inst, id).to_machine, readMsg(inst, id).history.at(-1).op], ['b1', 'm2', 'move']);
    const old = recOf(inst, 'b1', 'm1', P, T);
    assert.deepEqual([old.state, old.moved_to], ['closed', { brain: 'b1', machine: 'm2', project: P, topic: T }]);
    assert.equal(topicRecs(inst).filter((t) => t.brain === 'b1' && t.project === P && t.topic === T && t.state !== 'closed').length, 1, 'one open record');
    // The moved pending message rings the new holder on its machine.
    const ev = (await m1.events(since)).find((e) => e.message_id === pending.id);
    assert.deepEqual([ev.holder, ev.machine, ev.brain], ['thread-r2', 'm2', 'b1']);
    assert.deepEqual((await m2.receive({ project: P, topic: T, thread: 'thread-r2' })).map((m) => m.id), [pending.id]);
    // The sender's topic moves too (same holder, no takeover); the reply to a message sent from the old key follows moved_to.
    await m2.register({ project: P, topic: 'src', holder: SENDER_THREAD });
    const res = await m2.resolve({ project: P, topic: T, thread: 'thread-r2', id: picked.id, state: 'hotovo', text: 'answer' });
    assert.deepEqual([res.answer_delivered, readMsg(inst, res.answer_id).to_machine, readMsg(inst, res.answer_id).status], [true, 'm2', 'pending']);
    assert.deepEqual((await m2.count({ project: P, topic: 'src' })).ids, [res.answer_id]);
    assert.equal((await m1.count({ project: P, topic: 'src' })).total, 0, 'nothing left on the old key');
  } finally { await stop(); cleanup(inst.dir); }
});

test('I11: the same project/topic in two brains without brain is 409 ambiguous; own brain first', async () => {
  const inst = makeInstance('i11amb');
  const { stop } = await startInstance(inst);
  try {
    const first = api(inst, inst.key, { 'X-Comms-Brain': 'b1', 'X-Comms-Brain-Name': 'First', 'X-Comms-Machine': 'm1' });
    await first.register({ project: P, topic: T, holder: 'thread-1' });
    await api(inst, inst.key, { 'X-Comms-Brain': 'b2', 'X-Comms-Brain-Name': 'Second', 'X-Comms-Machine': 'm2' }).register({ project: P, topic: T, holder: 'thread-2' });
    await first.register({ project: P, topic: 'only-b1', holder: 'thread-1' });
    const before = fs.readdirSync(path.join(inst.dir, 'messages')).length;
    const msg = { project: P, topic: T, level: 'assist', text: 'x' };
    // A third brain (and a sender without a brain) cannot tell which one: 409 with the list, nothing written.
    for (const brain of ['b3', null]) {
      const r = await as(inst, brain, 'm3').call('POST', '/send', msg);
      assert.equal(r.status, 409);
      assert.deepEqual(r.json.ambiguous.sort(), ['First (b1)@m1', 'Second (b2)@m2']);
    }
    assert.equal(fs.readdirSync(path.join(inst.dir, 'messages')).length, before, 'nothing written');
    // Own brain first; an explicit brain (id or name) picks the other one.
    const own = await as(inst, 'b2', 'm9').send(msg);
    assert.deepEqual([readMsg(inst, own.id).to_brain, readMsg(inst, own.id).to_machine, own.resolved], ['b2', 'm2', undefined]);
    const byName = await as(inst, 'b3').send({ ...msg, brain: 'First' });
    assert.deepEqual([readMsg(inst, byName.id).to_brain, readMsg(inst, byName.id).to_machine], ['b1', 'm1']);
    // Without brain and one open topic in another brain: delivered there, the answer says where.
    const other = await as(inst, 'b3').send({ ...msg, topic: 'only-b1' });
    assert.deepEqual([other.status, other.resolved, readMsg(inst, other.id).to_brain], ['pending', 'First@m1', 'b1']);
  } finally { await stop(); cleanup(inst.dir); }
});

test('I11: a stop between the writes of a move is finished by repair at start', async () => {
  const inst = makeInstance('i11rep');
  let srv = await startServer({ dataDir: inst.dir });
  try {
    const m1 = as(inst, 'b1', 'm1');
    await m1.register({ project: P, topic: T, holder: RECIPIENT });
    const a = await m1.send({ project: P, topic: T, level: 'assist', text: 'a' });
    const b = await m1.send({ project: P, topic: T, level: 'assist', text: 'b' });
    // Stop after the new record and the first moved message: the second write of a moved message fails.
    const store = srv.service.store;
    const save = store.saveMessage.bind(store);
    let moves = 0;
    store.saveMessage = (m) => { if (m.history?.at(-1)?.op === 'move' && ++moves > 1) throw new Error('stop'); return save(m); };
    assert.equal((await as(inst, 'b1', 'm2').call('POST', '/register-topic', { project: P, topic: T, subject: 'S', holder: RECIPIENT })).status, 500);
    assert.deepEqual([recOf(inst, 'b1', 'm1', P, T).state, recOf(inst, 'b1', 'm2', P, T).state], ['open', 'open'], 'stopped midway');
    assert.deepEqual([a, b].map((x) => readMsg(inst, x.id).to_machine).sort(), ['m1', 'm2']);
    await srv.close();
    srv = await startServer({ dataDir: inst.dir });
    const old = recOf(inst, 'b1', 'm1', P, T);
    assert.deepEqual([old.state, old.moved_to?.machine], ['closed', 'm2']);
    assert.deepEqual([a, b].map((x) => [readMsg(inst, x.id).to_machine, readMsg(inst, x.id).status]), [['m2', 'pending'], ['m2', 'pending']]);
    assert.equal((await as(inst, 'b1', 'm2').count({ project: P, topic: T })).total, 2);
  } finally { await srv.close(); cleanup(inst.dir); }
});

test('I11: a stop in a return move (A→B, then B→A) is finished toward the newer record by repair; no cycle of moved_to', async () => {
  const inst = makeInstance('i11ret');
  let srv = await startServer({ dataDir: inst.dir });
  try {
    const ma = as(inst, 'b1', 'ma');
    await ma.register({ project: P, topic: T, holder: RECIPIENT });
    const m = await ma.send({ project: P, topic: T, level: 'assist', text: 'x' });
    await sleep(5);
    await as(inst, 'b1', 'mb').register({ project: P, topic: T, holder: RECIPIENT }); // A→B done: B keeps moved_from A
    await sleep(5);
    // B→A stops after the new A is written, before the message moves and B is closed.
    const store = srv.service.store;
    const save = store.saveMessage.bind(store);
    store.saveMessage = (x) => { if (x.history?.at(-1)?.op === 'move') throw new Error('stop'); return save(x); };
    assert.equal((await ma.call('POST', '/register-topic', { project: P, topic: T, subject: 'S', holder: RECIPIENT })).status, 500);
    assert.deepEqual([recOf(inst, 'b1', 'ma', P, T).state, recOf(inst, 'b1', 'mb', P, T).state], ['open', 'open'], 'stopped midway');
    await srv.close();
    srv = await startServer({ dataDir: inst.dir });
    const [a, b] = [recOf(inst, 'b1', 'ma', P, T), recOf(inst, 'b1', 'mb', P, T)];
    assert.deepEqual([a.state, a.moved_to, b.state, b.moved_to?.machine, b.moved_from], ['open', null, 'closed', 'ma', null]);
    assert.deepEqual([readMsg(inst, m.id).to_machine, readMsg(inst, m.id).status], ['ma', 'pending']);
    assert.equal((await ma.count({ project: P, topic: T })).total, 1);
  } finally { await srv.close(); cleanup(inst.dir); }
});

test('I11: a legacy record (brain null) is claimed only by its holder or when it has no open messages; a reply to from_brain null never goes to a guessed brain', async () => {
  const inst = makeInstance('i11leg');
  const { srv, stop } = await startInstance(inst);
  try {
    const store = srv.service.store;
    const legacy = (topic, holder) => store.saveTopic({ brain: null, brain_name: null, machine: 'old', project: P, topic, subject: 'S', holder, state: 'open', registered_at: '2026-11-01T00:00:00.000Z', updated_at: '2026-11-01T00:00:00.000Z', closed_at: null, moved_to: null });
    const legacyMail = (id, topic, extra = {}) => writeMsg(inst, { id, project: P, topic, to_brain: null, to_machine: 'old', from_brain: null, from_machine: null, level: 'assist', text: id, sender: 'alice', thread_id: null, from_project: null, from_topic: null, reply_to: null, status: 'pending', resolution: null, created_at: '2026-11-01T00:00:00.000Z', history: [], ...extra });
    const b1 = as(inst, 'b1', 'm1');
    // Another holder and open mail: a new record, the old one stays with its mail, the operator decides.
    legacy('foreign', 'thread-other');
    legacyMail('l-foreign', 'foreign');
    const r1 = await b1.register({ project: P, topic: 'foreign', holder: 'thread-1' });
    assert.ok(r1.legacy_notice_id);
    assert.equal((await b1.notices()).find((n) => n.id === r1.legacy_notice_id).kind, 'legacy-topic');
    assert.deepEqual([recOf(inst, null, 'old', P, 'foreign').state, readMsg(inst, 'l-foreign').to_brain], ['open', null]);
    // While it exists, an address without brain counts it as another brain's topic.
    assert.equal((await as(inst, 'b9').call('POST', '/send', { project: P, topic: 'foreign', level: 'assist', text: 'x' })).status, 409);
    // Its holder claims it: moved like I11.
    legacy('mine', 'thread-1');
    legacyMail('l-mine', 'mine');
    await b1.register({ project: P, topic: 'mine', holder: 'thread-1' });
    assert.deepEqual(recOf(inst, null, 'old', P, 'mine').moved_to, { brain: 'b1', machine: 'm1', project: P, topic: 'mine' });
    assert.deepEqual([readMsg(inst, 'l-mine').to_brain, readMsg(inst, 'l-mine').to_machine, readMsg(inst, 'l-mine').status], ['b1', 'm1', 'pending']);
    // No open mail: claimed by anyone.
    legacy('empty', 'thread-other');
    await b1.register({ project: P, topic: 'empty', holder: 'thread-1' });
    assert.equal(recOf(inst, null, 'old', P, 'empty').state, 'closed');
    // A reply to from_brain null: through the old record's moved_to; without any old record undelivered, never a guess.
    await b1.register({ project: P, topic: 'box', holder: 'thread-1' });
    await b1.register({ project: P, topic: 'gone', holder: 'thread-1' });
    const box = { to_brain: 'b1', to_machine: 'm1', status: 'picked' };
    legacyMail('q-moved', 'box', { ...box, from_project: P, from_topic: 'mine' });
    legacyMail('q-gone', 'box', { ...box, from_project: P, from_topic: 'gone' });
    const via = await b1.resolve({ project: P, topic: 'box', thread: 'thread-1', id: 'q-moved', state: 'hotovo', text: 'ok' });
    assert.deepEqual([via.answer_delivered, readMsg(inst, via.answer_id).to_brain], [true, 'b1']);
    const none = await b1.resolve({ project: P, topic: 'box', thread: 'thread-1', id: 'q-gone', state: 'hotovo', text: 'ok' });
    assert.deepEqual([none.answer_delivered, none.reason, readMsg(inst, none.answer_id).status], [false, 'no-return-path', 'undelivered']);
  } finally { await stop(); cleanup(inst.dir); }
});

test('schema 3: migration of a schema-2 sample keys topics and messages by brain and machine, verifies, a second start changes nothing; a stop in the middle leaves schema 2', async () => {
  const inst = makeInstance('schema3');
  try {
    const dir = (...p) => path.join(inst.dir, ...p);
    fs.writeFileSync(dir('meta.json'), JSON.stringify({ schema: 2 }));
    fs.writeFileSync(dir('topics', `${P}__${T}.json`), JSON.stringify({ project: P, topic: T, subject: 'S', holder: RECIPIENT, holder_machine: 'Box-1', state: 'open' }));
    fs.writeFileSync(dir('topics', `${P}__src.json`), JSON.stringify({ project: P, topic: 'src', subject: 'S', holder: SENDER_THREAD, state: 'open' }));
    const base = { project: P, topic: T, level: 'assist', sender: 'alice', thread_id: null, from_project: P, from_topic: 'src', reply_to: null, created_at: '2026-11-01T00:00:00.000Z', history: [] };
    writeMsg(inst, { ...base, id: 'm-pending', text: 'p', status: 'pending', resolution: null });
    writeMsg(inst, { ...base, id: 'm-done', text: 'd', status: 'done', resolution: 'settled' });
    fs.writeFileSync(dir('threads', `${encodeURIComponent(`topic:${P}/src`)}.json`), JSON.stringify({ key: `topic:${P}/src`, cursor: 0, recent: [{ seq: 1, at: '2026-11-01T00:00:00.000Z', kind: 'settled', id: 'm-done', line: 'settled m-done' }] }));
    // A stop in the middle: meta.json still says schema 2 and the old files are there.
    assert.throws(() => migrateData(new Store(inst.dir), { stopAfter: 3 }), /stopped after step 3/);
    assert.equal(JSON.parse(fs.readFileSync(dir('meta.json'), 'utf8')).schema, 2);
    assert.ok(fs.existsSync(dir('topics', `${P}__${T}.json`)));
    const srv = await startServer({ dataDir: inst.dir });
    try {
      assert.deepEqual([srv.migration.migrated, srv.migration.steps], [true, ['2→3']]);
      assert.match(path.basename(srv.migration.backup), /\.bak-2-/);
      assert.equal(JSON.parse(fs.readFileSync(dir('meta.json'), 'utf8')).schema, 3);
      assert.deepEqual(fs.readdirSync(dir('topics')).filter((f) => f.includes('__')), [], 'old topic files gone');
      assert.deepEqual([recOf(inst, null, 'box-1', P, T)?.holder, recOf(inst, null, 'unknown', P, 'src')?.holder], [RECIPIENT, SENDER_THREAD]);
      const m = readMsg(inst, 'm-pending');
      assert.deepEqual([m.to_brain, m.to_machine, m.from_brain, m.from_machine, m.status], [null, 'box-1', null, 'unknown', 'pending']);
      assert.ok(fs.existsSync(dir('threads', `${encodeURIComponent(`topic:/${P}/src`)}.json`)));
      assert.ok(!fs.existsSync(dir('threads', `${encodeURIComponent(`topic:${P}/src`)}.json`)));
      // The holder's mailbox reads it (a client without brain: the record from before model 3).
      const ob = (await as(inst, null).call('GET', `/obligations?thread=${RECIPIENT}`)).json;
      assert.deepEqual(ob.inbox.ids.map((x) => x.id), ['m-pending']);
    } finally { await srv.close(); }
    const snapshot = ['m-pending', 'm-done'].map((id) => fs.readFileSync(dir('messages', `${id}.json`), 'utf8'));
    const topicsBefore = fs.readdirSync(dir('topics')).sort();
    const srv2 = await startServer({ dataDir: inst.dir });
    try {
      assert.equal(srv2.migration.migrated, false);
      assert.deepEqual(['m-pending', 'm-done'].map((id) => fs.readFileSync(dir('messages', `${id}.json`), 'utf8')), snapshot);
      assert.deepEqual(fs.readdirSync(dir('topics')).sort(), topicsBefore);
    } finally { await srv2.close(); }
  } finally { bakCleanup(inst); cleanup(inst.dir); }
});
