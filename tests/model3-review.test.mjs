// ttl: 2027-05-18
// Model 3 review fixes (2026-11-18): a closed record without a move does not hide the open one of the same brain; a move
// to another machine rings the holder there; a registration without any machine does not move the open topic.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, api, cleanup } from './helpers.mjs';

const P = 'proj';
const as = (inst, brain, machine = null) => api(inst, inst.key, { 'X-Comms-Brain': brain, ...(machine ? { 'X-Comms-Machine': machine } : {}) });
const readMsg = (inst, id) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${id}.json`), 'utf8'));
const recs = (inst, topic) => fs.readdirSync(path.join(inst.dir, 'topics')).map((f) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'topics', f), 'utf8'))).filter((t) => t.topic === topic);

async function up() {
  const inst = makeInstance('m3rev');
  const { srv, stop } = await startInstance(inst);
  return { inst, srv, done: async () => { await stop(); cleanup(inst.dir); } };
}

test('a sender topic closed on ma and registered anew on mb (no move): the reply, awaiting and the wait go to mb', async () => {
  const { inst, done } = await up();
  try {
    const ma = as(inst, 'b1', 'ma');
    const mb = as(inst, 'b1', 'mb');
    await ma.register({ project: P, topic: 'dst', holder: 'thr-r' });
    await ma.register({ project: P, topic: 'src', holder: 'thr-s' });
    const q = await ma.send({ project: P, topic: 'dst', level: 'assist', text: 'q', from_project: P, from_topic: 'src' });
    await ma.register({ project: P, topic: 'src', holder: 'thr-s', close: true });
    await mb.register({ project: P, topic: 'src', holder: 'thr-s2' });
    const ob = await mb.obligations({ thread: 'thr-s2' });
    assert.deepEqual(ob.awaiting.map((x) => x.id), [q.id]);
    const due = (await ma.due()).due; // not due yet; senderHolder is read through the same resolver
    assert.equal(due.length, 0);
    await ma.receive({ project: P, topic: 'dst', thread: 'thr-r' });
    const r = await ma.resolve({ project: P, topic: 'dst', thread: 'thr-r', id: q.id, state: 'hotovo', text: 'a' });
    assert.deepEqual([r.answer_delivered, readMsg(inst, r.answer_id).to_machine, readMsg(inst, r.answer_id).status], [true, 'mb', 'pending']);
  } finally { await done(); }
});

test('a move by the same holder to mb without holder_machine rings the holder on mb', async () => {
  const { inst, done } = await up();
  try {
    await as(inst, 'b1', 'ma').register({ project: P, topic: 't', holder: 'thr-1', holder_machine: 'ma' });
    const m = await as(inst, 'b1', 'ma').send({ project: P, topic: 't', level: 'assist', text: 'x' });
    const since = (await as(inst, 'b1').events()).at(-1).seq;
    const r = await as(inst, 'b1', 'mb').register({ project: P, topic: 't', holder: 'thr-1' });
    assert.deepEqual([r.topic.machine, r.topic.holder_machine], ['mb', 'mb']);
    const ev = (await as(inst, 'b1').events(since)).find((e) => e.message_id === m.id);
    assert.equal(ev.holder_machine, 'mb');
  } finally { await done(); }
});

test('a registration without any machine keeps the open topic where it is (no move to unknown)', async () => {
  const { inst, done } = await up();
  try {
    await as(inst, 'b1', 'ma').register({ project: P, topic: 't', holder: 'thr-1' });
    const r = await as(inst, 'b1').register({ project: P, topic: 't', holder: 'thr-1' });
    assert.equal(r.topic.machine, 'ma');
    assert.equal(r.moved, undefined);
    assert.deepEqual(recs(inst, 't').map((t) => [t.machine, t.state]), [['ma', 'open']]);
    // Nothing open: `unknown`.
    const n = await as(inst, 'b1').register({ project: P, topic: 'fresh', holder: 'thr-2' });
    assert.equal(n.topic.machine, 'unknown');
  } finally { await done(); }
});
