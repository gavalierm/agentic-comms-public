// Maintainer address (operator 2026-11-16): `send --maintainer` goes to the T3 thread of instance.json maintainer.thread,
// read at every send; without it 404 (the sender files an issue); an archived thread bounces like any other (undelivered).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, api, cleanup } from './helpers.mjs';

const MT = 'thread-maintainer';
const SENDER = 'thread-s';

function setMaintainer(inst, m) {
  const f = path.join(inst.dir, 'instance.json');
  const j = JSON.parse(fs.readFileSync(f, 'utf8'));
  if (m) j.maintainer = m; else delete j.maintainer;
  fs.writeFileSync(f, JSON.stringify(j, null, 2));
}

async function setup() {
  const inst = makeInstance('maint');
  const { stop } = await startInstance(inst);
  return { inst, a: api(inst), done: async () => { await stop(); cleanup(inst.dir); } };
}

test('send --maintainer without maintainer.thread is refused with 404 and stores nothing', async () => {
  const { inst, a, done } = await setup();
  try {
    await assert.rejects(a.send({ maintainer: true, level: 'assist', text: 'report', thread_id: SENDER }), (e) => e.status === 404 && /maintainer\.thread/.test(e.message));
    assert.equal(fs.readdirSync(path.join(inst.dir, 'messages')).filter((f) => f.endsWith('.json')).length, 0);
  } finally { await done(); }
});

test('send --maintainer is pending in the thread mailbox, rings that thread on its machine, and is read without restart', async () => {
  const { inst, a, done } = await setup();
  try {
    setMaintainer(inst, { thread: MT, machine: 'myserver' });
    const r = await a.send({ maintainer: true, level: 'assist', text: 'report one', thread_id: SENDER });
    assert.equal(r.status, 'pending');
    assert.equal(r.to_thread, MT);
    const ev = (await a.events(0)).find((e) => e.message_id === r.id);
    assert.equal(ev.holder, MT);
    assert.equal(ev.holder_machine, 'myserver');
    const got = await a.receive({ thread: MT });
    assert.deepEqual(got.map((m) => m.id), [r.id]);
    assert.match(got[0].text, /report one/);
  } finally { await done(); }
});

test('send --maintainer does not combine with another address', async () => {
  const { inst, a, done } = await setup();
  try {
    setMaintainer(inst, { thread: MT });
    await assert.rejects(a.send({ maintainer: true, project: 'p', topic: 't', level: 'assist', text: 'x' }), (e) => e.status === 400);
  } finally { await done(); }
});

test('a maintainer message to an archived thread bounces: undelivered, the sender wait is due', async () => {
  const { inst, a, done } = await setup();
  try {
    setMaintainer(inst, { thread: MT });
    const r = await a.send({ maintainer: true, level: 'assist', text: 'report', thread_id: SENDER });
    const b = await a.bounce({ message_id: r.id, holder: MT, reason: 'thread archived' });
    assert.equal(b.bounced, true);
    const m = JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${r.id}.json`), 'utf8'));
    assert.equal(m.status, 'undelivered');
    assert.equal(m.wait_reason, 'undelivered');
  } finally { await done(); }
});

test('supersede_key replaces an unpicked maintainer message of the same sender', async () => {
  const { inst, a, done } = await setup();
  try {
    setMaintainer(inst, { thread: MT });
    const r1 = await a.send({ maintainer: true, level: 'assist', text: 'v1', thread_id: SENDER, supersede_key: 'report-x' });
    const r2 = await a.send({ maintainer: true, level: 'assist', text: 'v2', thread_id: SENDER, supersede_key: 'report-x' });
    const m1 = JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${r1.id}.json`), 'utf8'));
    assert.equal(m1.status, 'superseded');
    assert.deepEqual((await a.receive({ thread: MT })).map((m) => m.id), [r2.id]);
  } finally { await done(); }
});
