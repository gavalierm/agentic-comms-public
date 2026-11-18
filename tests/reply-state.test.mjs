import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, cleanup, runCli, api, addKey, LOCAL_MACHINE } from './helpers.mjs';

// Reply protocol (incident 2026-11-11: three replies to one message, the last a new request): a reply carries a
// state (hotovo | otázka | čakám), a message has one final reply, a reply is never answered by a reply.
// The recipient answers with POST /resolve (design mail-service-design v3, 1b and 1d). Model 3: the topics are
// registered on this machine, where the CLI runs (X-Comms-Machine), so its mailbox queries name the same key.

async function pair(t, name) {
  const a = makeInstance(name);
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = api(a, a.key, { 'X-Comms-Machine': LOCAL_MACHINE });
  const bob = api(a, addKey(a, 'bob'), { 'X-Comms-Machine': LOCAL_MACHINE });
  await alice.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob' });
  const box = { project: 'p', topic: 't', thread: 'thr-bob' };
  const ask = (text = 'decide', extra = {}) => alice.send({ project: 'p', topic: 't', level: 'assist', text, thread_id: 'thr-alice', ...extra });
  return { a, svc: sa.srv.service, alice, bob, box, ask };
}

const readMsg = (inst, id) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${id}.json`), 'utf8'));
const errorOf = (status, re = null) => (e) => e.status === status && (re === null || re.test(e.json?.error || ''));

test('reply state: a second reply after the final one is refused (409) and names the final reply', async (t) => {
  const { alice, bob, box, ask } = await pair(t, 'rs1');
  const q = await ask();
  const first = await bob.resolve({ ...box, id: q.id, state: 'hotovo', text: 'decision A' });
  assert.equal(first.answer_state, 'hotovo');
  const again = await bob.call('POST', '/resolve', { ...box, id: q.id, state: 'hotovo', text: 'correction: decision B' });
  assert.equal(again.status, 409);
  assert.ok(again.json.error.includes(first.answer_id), again.json.error);
  assert.deepEqual([again.json.status, again.json.answer_id], ['done', first.answer_id]);
  assert.equal((await bob.call('POST', '/resolve', { ...box, id: q.id, state: 'otázka', text: 'on second thought' })).status, 409);
  assert.equal((await alice.receive({ thread: 'thr-alice' })).length, 1, 'the sender holds exactly one reply');
});

test('reply state: a reply to a reply is refused (400); the request goes as a new message', async (t) => {
  const { alice, bob, box, ask } = await pair(t, 'rs2');
  const q = await ask();
  const r = await bob.resolve({ ...box, id: q.id, state: 'hotovo', text: 'decision A; also do X' });
  await assert.rejects(alice.resolve({ thread: 'thr-alice', id: r.answer_id, state: 'hotovo', text: 'X done' }), errorOf(400, /je odpoveď/));
  // an interim reply is not answered by a reply either
  const q2 = await ask('second');
  await bob.receive({ ...box, ids: [q2.id] });
  const r2 = await bob.resolve({ ...box, id: q2.id, state: 'otázka', text: 'operator decides' });
  await assert.rejects(alice.resolve({ thread: 'thr-alice', id: r2.answer_id, state: 'čakám', text: 'hurry' }), errorOf(400));
});

test('reply state: stored on the reply, shown in receive, count items, message and owed; resolve needs a known state', async (t) => {
  const { alice, bob, box, ask } = await pair(t, 'rs3');
  const q = await ask();
  assert.equal((await alice.message(q.id)).answer_state, null);
  await bob.receive(box);
  await assert.rejects(bob.resolve({ ...box, id: q.id, state: 'neskôr', text: 'x' }), errorOf(400, /hotovo\|otázka\|čakám/));
  await assert.rejects(bob.resolve({ ...box, id: q.id, text: 'no state' }), errorOf(400), 'a reply without a state is gone with send reply_to');
  await assert.rejects(alice.send({ project: 'p', topic: 't', level: 'assist', reply_state: 'hotovo', text: 'x' }), errorOf(400));

  const r = await bob.resolve({ ...box, id: q.id, state: 'otázka', text: 'operator decides; I recommend A' });
  assert.deepEqual([r.status, r.answer_state, r.answer_delivered], ['waiting', 'otázka', true]);
  const st = await alice.message(q.id);
  assert.deepEqual([st.status, st.answer_state, st.answer_id], ['waiting', 'otázka', r.answer_id]);
  assert.ok(st.answer_at);
  const item = (await alice.count({ thread: 'thr-alice' })).items.find((i) => i.id === r.answer_id);
  assert.deepEqual([item.reply_to, item.reply_state], [q.id, 'otázka']);
  const body = (await alice.receive({ thread: 'thr-alice' })).find((m) => m.id === r.answer_id);
  assert.equal(body.reply_state, 'otázka');
  // the original is still owed by the recipient, with its answer state
  const owed = (await bob.obligations({ thread: 'thr-bob' })).owed.find((x) => x.id === q.id);
  assert.deepEqual([owed.status, owed.answer_state], ['waiting', 'otázka']);

  // an interim state does not close the exchange: the final reply follows and closes it
  const fin = await bob.resolve({ ...box, id: q.id, state: 'hotovo', text: 'operator chose A' });
  const after = await alice.message(q.id);
  assert.deepEqual([after.status, after.resolution, after.answer_state, after.answer_id], ['done', 'reply', 'hotovo', fin.answer_id]);
  assert.deepEqual((await bob.obligations({ thread: 'thr-bob' })).owed, []);
});

test('reply state: the first interim state and the final reply ring the sender; a move between interim states does not', async (t) => {
  const { svc, alice, bob, box, ask } = await pair(t, 'rs4');
  const q = await ask();
  await bob.receive(box);
  const start = svc.lastSeq;
  await bob.resolve({ ...box, id: q.id, state: 'otázka', text: 'operator decides' });
  const e1 = await alice.events(start);
  assert.deepEqual(e1.map((e) => [e.type, e.holder, e.reply_state]), [['reply', 'thr-alice', 'otázka']]);
  const w = await bob.resolve({ ...box, id: q.id, state: 'čakám', wait_for: 'build 42', text: 'operator decided, build runs' });
  assert.equal((await alice.events(e1.at(-1).seq)).length, 0, 'interim to interim does not ring');
  assert.equal((await alice.message(q.id)).answer_state, 'čakám');
  assert.equal((await alice.receive({ thread: 'thr-alice' })).find((m) => m.id === w.answer_id).wait_for, 'build 42');
  await bob.resolve({ ...box, id: q.id, state: 'hotovo', text: 'built' });
  assert.deepEqual((await alice.events(e1.at(-1).seq)).map((e) => [e.type, e.reply_state]), [['reply', 'hotovo']]);
});

test('cancels: resolve with cancels replaces the own final reply; anyone else, a non-final reply, another id and a second cancel are refused', async (t) => {
  const { a, alice, bob, box } = await pair(t, 'rs5');
  await bob.register({ project: 'p', topic: 'home', subject: 'S', holder: 'thr-alice' });
  const q = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'decide', from_project: 'p', from_topic: 'home' });
  await bob.receive(box);
  const interim = await bob.resolve({ ...box, id: q.id, state: 'otázka', text: 'operator decides' });
  const cancel = (c, cancels, extra = {}) => c.call('POST', '/resolve', { ...box, id: q.id, state: 'hotovo', text: 'decision changed to B', cancels, ...extra });
  assert.equal((await cancel(bob, interim.answer_id)).status, 409, 'an interim reply is not cancelled, the final one replaces it');
  const fin = await bob.resolve({ ...box, id: q.id, state: 'hotovo', text: 'decision A' });
  assert.equal((await cancel(bob, 'missing-id')).status, 409, 'not the current final reply');
  assert.equal((await cancel(bob, q.id)).status, 409, 'only the final reply can be cancelled');
  const carol = api(a, addKey(a, 'carol'));
  assert.equal((await cancel(carol, fin.answer_id)).status, 403, 'only the sender of the final reply');
  assert.equal((await cancel(bob, fin.answer_id, { text: null })).status, 400, 'cancels needs a new text');
  assert.equal((await cancel(bob, fin.answer_id, { state: 'otázka' })).status, 400, 'cancels needs hotovo');
  assert.equal(readMsg(a, fin.answer_id).cancelled_by ?? null, null, 'refused attempts change nothing');

  const change = await (async () => { const r = await cancel(bob, fin.answer_id); assert.equal(r.status, 200, r.body); return r.json; })();
  assert.notEqual(change.answer_id, fin.answer_id);
  assert.equal(readMsg(a, fin.answer_id).cancelled_by, change.answer_id);
  const st = await alice.message(q.id);
  assert.deepEqual([st.status, st.resolution, st.answer_id], ['done', 'reply', change.answer_id]);
  assert.equal((await alice.count({ project: 'p', topic: 'home' })).items.find((i) => i.id === change.answer_id).cancels, fin.answer_id);
  const got = (await alice.receive({ project: 'p', topic: 'home' })).find((m) => m.id === change.answer_id);
  assert.equal(got.cancels, fin.answer_id);
  assert.equal((await cancel(bob, fin.answer_id)).status, 409, 'a second cancel of the same reply');
});

// Audit 2.4 (2026-11-12): a cron sent the same blocker every 15 min (26 messages, 25 expired, 30 notices), and a final reply
// needed a separate ack (33 ack calls). `supersede_key` replaces an unpicked message of the same sender, address and key; the notice
// of a keyed blocker exists only when nobody picked it up; a reply is read (done) by its pickup.

async function supersedeSetup(t) {
  const a = makeInstance('sup');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const alice = api(a);
  const bob = api(a, addKey(a, 'bob'));
  await alice.register({ project: 'p', topic: 't', subject: 'S', holder: 'thr-bob' });
  await alice.register({ project: 'p', topic: 'u', subject: 'U', holder: 'thr-bob' });
  return { a, svc: sa.srv.service, alice, bob, box: { project: 'p', topic: 't', thread: 'thr-bob' } };
}

const blockers = (svc) => svc.store.notices((n) => n.kind === 'blocker');

test('supersede_key: a repeated blocker replaces the unpicked one (also an expired one); one message waits, one notice after the expiry', async (t) => {
  const { a, svc, alice, bob, box } = await supersedeSetup(t);
  const m1 = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'issue 25', supersede_key: 'k1' });
  assert.equal(blockers(svc).length, 0, 'a keyed blocker has no notice at delivery');
  assert.equal(svc.checkExpired(Date.now() + 6 * 60_000).length, 1, 'the first one expired unpicked');
  assert.equal(blockers(svc).length, 1, 'the notice appears when it was not picked up');
  const m2 = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'issue 25 again', supersede_key: 'k1' });
  const m3 = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'issue 25 third', supersede_key: 'k1' });
  const c = await bob.count({ project: 'p', topic: 't' });
  assert.deepEqual(c.ids, [m3.id], 'only the newest message waits');
  const s1 = readMsg(a, m1.id);
  assert.deepEqual([s1.status, s1.superseded_by, typeof s1.expired_at], ['superseded', m2.id, 'string']);
  assert.deepEqual([(await alice.message(m2.id)).status, svc.store.message(m2.id).superseded_by], ['superseded', m3.id]);
  assert.equal(blockers(svc).length, 1, 'no further notice');
  assert.deepEqual([blockers(svc)[0].message_id, blockers(svc)[0].state], [m3.id, 'open'], 'the notice follows the newest message');
  const got = await bob.receive(box);
  assert.deepEqual(got.map((m) => m.text), ['issue 25 third']);
});

test('supersede_key: a picked message is not replaced and a picked blocker raises no notice', async (t) => {
  const { svc, alice, bob, box } = await supersedeSetup(t);
  const m1 = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', supersede_key: 'k1' });
  await bob.receive({ ...box, ids: [m1.id] });
  const m2 = await alice.send({ project: 'p', topic: 't', level: 'blocker', text: 'two', supersede_key: 'k1' });
  assert.deepEqual((await bob.count({ project: 'p', topic: 't' })).ids, [m2.id], 'the picked one is not in the inbox');
  const s1 = svc.store.message(m1.id);
  assert.deepEqual([s1.status, s1.superseded_by ?? null], ['picked', null], 'the picked one is untouched');
  svc.checkExpired(Date.now() + 6 * 60_000);
  assert.equal(blockers(svc).length, 1, 'only the unpicked second one raised a notice');
  assert.equal(blockers(svc)[0].message_id, m2.id);
});

test('supersede_key: other key, other topic or other sender is not replaced; without a key nothing changes', async (t) => {
  const { alice, bob } = await supersedeSetup(t);
  const a = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'a', supersede_key: 'k1' });
  const b = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'b', supersede_key: 'k2' });
  const c = await alice.send({ project: 'p', topic: 'u', level: 'assist', text: 'c', supersede_key: 'k1' });
  const d = await bob.send({ project: 'p', topic: 't', level: 'assist', text: 'd', supersede_key: 'k1' });
  const e = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'e' });
  const f = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'f' });
  assert.deepEqual((await bob.count({ project: 'p', topic: 't' })).ids, [a.id, b.id, d.id, e.id, f.id]);
  assert.deepEqual((await bob.count({ project: 'p', topic: 'u' })).ids, [c.id]);
});

test('replies are read by their pickup (done/read), interim and final alike; the originals keep their own status', async (t) => {
  const { a, alice, bob, box } = await supersedeSetup(t);
  const q1 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'q1', thread_id: 'thr-alice' });
  const q2 = await alice.send({ project: 'p', topic: 't', level: 'assist', text: 'q2', thread_id: 'thr-alice' });
  await bob.receive(box);
  const interim = await bob.resolve({ ...box, id: q1.id, state: 'otázka', text: 'operator decides' });
  const final = await bob.resolve({ ...box, id: q2.id, state: 'hotovo', text: 'done' });
  const got = await alice.receive({ thread: 'thr-alice' });
  assert.deepEqual(got.map((m) => [m.id, m.status, m.resolution]).sort(), [[interim.answer_id, 'done', 'read'], [final.answer_id, 'done', 'read']].sort());
  assert.equal((await alice.count({ thread: 'thr-alice' })).total, 0, 'nothing waits, nothing to ack');
  assert.equal(readMsg(a, q1.id).status, 'waiting');
  assert.deepEqual([readMsg(a, q2.id).status, readMsg(a, q2.id).resolution], ['done', 'reply']);
  const awaiting = (await alice.obligations({ thread: 'thr-alice' })).awaiting.map((x) => [x.id, x.status, x.answer_state]);
  assert.deepEqual(awaiting, [[q1.id, 'waiting', 'otázka']]);
});

// --- CLI ----------------------------------------------------------------------------------------------

test('CLI resolve: --state / --wait-for / --cancels reach the service; an unknown state, an interim without text and send --state are refused', async (t) => {
  const { a, alice, ask } = await pair(t, 'rs6');
  const q = await ask();
  const cli = (args) => runCli([...args, '--data-dir', a.dir, '--key', addKey(a, 'bob'), '--thread', 'thr-bob', '--no-start', ...(args[0] === 'resolve' ? ['--project', 'p', '--topic', 't'] : [])]);
  assert.equal((await cli(['receive', '--project', 'p', '--topic', 't'])).json.messages[0].status, 'picked');
  const threadOnly = await runCli(['settle', '--id', q.id, '--data-dir', a.dir, '--key', addKey(a, 'bob'), '--thread', 'thr-bob', '--no-start']);
  assert.match(threadOnly.err, /"status":403/, 'a topic message is resolved in its topic mailbox');
  assert.notEqual((await cli(['resolve', '--id', q.id, '--state', 'neskôr', '--text', 'x'])).code, 0);
  const noText = await cli(['resolve', '--id', q.id, '--state', 'otázka']);
  assert.equal(noText.code, 1);
  assert.match(noText.err, /musí mať --text/);
  const oldSend = await cli(['send', '--project', 'p', '--topic', 't', '--level', 'assist', '--state', 'hotovo', '--text', 'x']);
  assert.equal(oldSend.code, 1);
  assert.match(oldSend.err, /comms resolve/);
  assert.match((await cli(['send', '--reply-to', q.id, '--text', 'x'])).err, /neznáma voľba --reply-to/);
  const w = await cli(['resolve', '--id', q.id, '--state', 'čakám', '--wait-for', 'build 42', '--text', 'runs']);
  assert.equal(w.code, 0, w.err);
  assert.deepEqual([w.json.status, w.json.answer_state, w.json.answer_delivered], ['waiting', 'čakám', true]);
  assert.equal(readMsg(a, w.json.answer_id).wait_for, 'build 42');
  const fin = await cli(['resolve', '--id', q.id, '--state', 'hotovo', '--text', 'built']);
  assert.deepEqual([fin.json.status, fin.json.resolution], ['done', 'reply']);
  const again = await cli(['resolve', '--id', q.id, '--state', 'hotovo', '--text', 'fix']);
  assert.equal(again.code, 1);
  assert.match(again.err, /"status":\s*409/);
  const c = await cli(['resolve', '--id', q.id, '--state', 'hotovo', '--cancels', fin.json.answer_id, '--text', 'changed']);
  assert.equal(c.code, 0, c.err);
  assert.equal((await alice.message(fin.json.answer_id)).cancelled_by, c.json.answer_id);
});

test('CLI settle, ack, withdraw and obligations: one resolve of the service each; ack after an interim answer is 409', async (t) => {
  const { a, ask } = await pair(t, 'rs7');
  // The mailbox of a topic message is its topic (--thread alone names only mail addressed to the thread).
  const bob = (args) => runCli([...args, '--data-dir', a.dir, '--key', addKey(a, 'bob'), '--thread', 'thr-bob', '--no-start', ...(args[0] === 'obligations' ? [] : ['--project', 'p', '--topic', 't'])]);
  const alice = (args) => runCli([...args, '--data-dir', a.dir, '--key', a.key, '--thread', 'thr-alice', '--no-start']);
  const [s1, s2, s3, s4] = [await ask('one'), await ask('two'), await ask('three'), await ask('four')];
  await bob(['receive', '--project', 'p', '--topic', 't']);
  const ob = await bob(['obligations']);
  assert.equal(ob.code, 0, ob.err);
  const owed = ob.json;
  assert.deepEqual(owed.owed.map((x) => x.id), [s1.id, s2.id, s3.id, s4.id]);
  assert.equal(owed.offline, false);
  const st = await bob(['settle', '--id', s1.id]);
  assert.deepEqual([st.code, st.json.status, st.json.resolution], [0, 'done', 'settled'], st.err);
  const ack = await bob(['ack', '--id', s2.id]);
  assert.deepEqual([ack.json.status, ack.json.resolution], ['done', 'acked']);
  await bob(['resolve', '--id', s3.id, '--state', 'otázka', '--text', 'which one?']);
  const late = await bob(['ack', '--id', s3.id]);
  assert.equal(late.code, 1);
  assert.match(late.err, /"status":\s*409/);
  assert.equal(JSON.parse(late.err.split('\n')[0]).details.status, 'waiting');
  assert.notEqual((await bob(['withdraw', '--id', s4.id])).code, 0, 'only the sender withdraws');
  const wd = await alice(['withdraw', '--id', s4.id]);
  assert.deepEqual([wd.code, wd.json.status, wd.json.resolution], [0, 'done', 'withdrawn']);
  const mine = (await alice(['obligations'])).json;
  assert.deepEqual(mine.awaiting.map((x) => [x.id, x.status]), [[s3.id, 'waiting']]);
  assert.deepEqual(mine.recent.map((r) => r.line), [`settled ${s1.id}`, `acked ${s2.id}`]);
  const theirs = (await bob(['obligations'])).json;
  assert.deepEqual(theirs.owed.map((x) => x.id), [s3.id]);
  assert.ok(theirs.recent.some((r) => r.line === `withdrawn ${s4.id}`) || readMsg(a, s4.id).resolution === 'withdrawn');
  const due = await runCli(['obligations', '--due', '--data-dir', a.dir, '--key', addKey(a, 'wake'), '--no-start']);
  assert.deepEqual([due.code, Array.isArray(due.json.due)], [0, true]);
});

test('CLI wait-adopt: the sender hands its local wait over (due time, base); another key 403, a missing --due-at is usage', async (t) => {
  const { a, ask } = await pair(t, 'rs8');
  const s = await ask('adopt me');
  const due = new Date(Date.now() + 10 * 60_000).toISOString();
  const alice = (args) => runCli([...args, '--data-dir', a.dir, '--key', a.key, '--no-start']);
  const r = await alice(['wait-adopt', '--id', s.id, '--due-at', due, '--base-s', '900']);
  assert.equal(r.code, 0, r.err);
  assert.deepEqual([r.json.id, r.json.wait_due_at, r.json.wait_base_s, r.json.wait_n], [s.id, due, 900, 0]);
  assert.deepEqual([readMsg(a, s.id).wait_due_at, readMsg(a, s.id).wait_base_s], [due, 900]);
  const other = await runCli(['wait-adopt', '--id', s.id, '--due-at', due, '--data-dir', a.dir, '--key', addKey(a, 'bob'), '--no-start']);
  assert.equal(other.code, 1);
  assert.match(other.err, /"status":\s*403/);
  const usage = await alice(['wait-adopt', '--id', s.id]);
  assert.equal(usage.code, 1);
  assert.match(usage.err, /--due-at/);
});

test('resolve of your own message (the reply would return to your thread) is 400', async () => {
  const inst = makeInstance('self-reply');
  const { stop } = await startInstance(inst);
  try {
    const a = api(inst);
    await a.register({ project: 'p', topic: 't', holder: 'thr-x' });
    const { id } = await a.send({ project: 'p', topic: 't', level: 'assist', text: 'mine', thread_id: 'thr-x' });
    await a.receive({ project: 'p', topic: 't', thread: 'thr-x' });
    const r = await a.call('POST', '/resolve', { project: 'p', topic: 't', thread: 'thr-x', id, state: 'hotovo', text: 'self' });
    assert.equal(r.status, 400);
    assert.equal((await a.message(id)).status, 'picked');
  } finally { await stop(); cleanup(inst.dir); }
});
