// Persona addressing (P-045), private topics (P-041) and machine-aware wake (P-043).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, cleanup, tmpDir } from './helpers.mjs';
import { Waker } from '../src/wake/index.mjs';

async function setup(t) {
  const inst = makeInstance('persona');
  const srv = await startInstance(inst);
  t.after(async () => { await srv.stop(); cleanup(inst.dir); });
  const bob = inst.store.addKey('bob');
  const client = makeClient(inst, { machine: 'server' });
  const asRon = makeClient(inst, { key: bob.key, machine: 'server' });
  return { inst, client, asRon };
}

test('persona alone goes to the main thread and waits even when the persona was never born', async (t) => {
  const { client, asRon } = await setup(t);
  const r = await client.send({ persona: 'bob', level: 'assist', text: 'ahoj' });
  assert.equal(r.state, 'pending');
  assert.match(r.warning, /no main thread yet/);
  const n = await client.notices();
  assert.equal(n.notices.filter((x) => x.kind === 'unregistered-persona').length, 1);
  // A second message does not create a second notice.
  await client.send({ persona: 'bob', level: 'assist', text: 'druhá' });
  assert.equal((await client.notices()).notices.filter((x) => x.kind === 'unregistered-persona').length, 1);
  // Birth: the main thread registers and picks up everything; the notice is settled.
  await asRon.registerTopic({ persona: 'bob', main: true, holder: 'thr-bob-main', holder_machine: 'server' });
  const got = await asRon.receive({ persona: 'bob' });
  assert.deepEqual(got.messages.map((m) => m.text), ['ahoj', 'druhá']);
  assert.equal(got.messages[0].project, '@bob');
  assert.equal(got.messages[0].topic, 'main');
  assert.equal((await client.notices()).notices.filter((x) => x.kind === 'unregistered-persona').length, 0);
  // After birth no warning.
  const r2 = await client.send({ persona: 'bob', level: 'assist', text: 'tretia' });
  assert.equal(r2.warning, undefined);
});

test('persona + project + topic: to the topic only when the persona owns it, else to main with a reference', async (t) => {
  const { client, asRon } = await setup(t);
  await asRon.registerTopic({ persona: 'bob', main: true, holder: 'thr-main' });
  await asRon.registerTopic({ project: 'srv', topic: 'disk', subject: 'Disk', persona: 'bob', holder: 'thr-disk' });
  await client.registerTopic({ project: 'srv', topic: 'web', subject: 'Web', persona: 'alice' });

  await client.send({ persona: 'bob', project: 'srv', topic: 'disk', level: 'assist', text: 'owned' });
  await client.send({ persona: 'bob', project: 'srv', topic: 'web', level: 'assist', text: 'not owned' });
  await client.send({ persona: 'bob', project: 'srv', topic: 'missing', level: 'assist', text: 'missing' });
  await client.send({ persona: 'bob', project: 'srv', level: 'assist', text: 'project only' });

  assert.deepEqual((await asRon.receive({ project: 'srv', topic: 'disk' })).messages.map((m) => m.text), ['owned']);
  const main = (await asRon.receive({ persona: 'bob' })).messages;
  assert.deepEqual(main.map((m) => [m.text, m.ref_project, m.ref_topic]), [
    ['not owned', 'srv', 'web'], ['missing', 'srv', 'missing'], ['project only', 'srv', null],
  ]);
  await assert.rejects(client.send({ persona: 'bob', topic: 'disk', level: 'assist', text: 'x' }), /vyžaduje projekt/);
});

test('private topic: invisible to other senders (same answer as an unknown topic), open to its owner', async (t) => {
  const { client, asRon } = await setup(t);
  await asRon.registerTopic({ persona: 'bob', main: true, holder: 'thr-main' });
  await asRon.registerTopic({ project: 'srv', topic: 'root-keys', subject: 'Root', persona: 'bob', visibility: 'private', holder: 'thr-priv' });

  const priv = await client.send({ project: 'srv', topic: 'root-keys', level: 'assist', text: 'peek' });
  const unknown = await client.send({ project: 'srv', topic: 'nothing', level: 'assist', text: 'x' });
  assert.deepEqual(Object.keys(priv).sort(), Object.keys(unknown).sort());
  assert.equal(priv.state, 'held');
  assert.equal(unknown.state, 'held');
  assert.equal((await asRon.receive({ project: 'srv', topic: 'root-keys' })).messages.length, 0);

  // Persona address from an outsider also does not reach the private topic: it goes to main.
  await client.send({ persona: 'bob', project: 'srv', topic: 'root-keys', level: 'assist', text: 'via persona' });
  assert.equal((await asRon.receive({ project: 'srv', topic: 'root-keys' })).messages.length, 0);
  assert.deepEqual((await asRon.receive({ persona: 'bob' })).messages.map((m) => m.text), ['via persona']);

  // Operator decides "deliver": the held message goes to the owner's main thread, not into the topic.
  const notice = (await client.notices()).notices.find((n) => n.message_id === priv.id);
  assert.match(notice.title, /Súkromná téma persony bob/);
  const d = await client.decide({ notice_id: notice.id, decision: 'deliver' });
  assert.deepEqual(d.delivered_to, { project: '@bob', topic: 'main' });
  assert.deepEqual((await asRon.receive({ persona: 'bob' })).messages.map((m) => m.text), ['peek', 'via persona']); // ordered by creation time

  // The owner itself reaches its private topic directly.
  const own = await asRon.send({ project: 'srv', topic: 'root-keys', level: 'assist', text: 'own' });
  assert.equal(own.state, 'pending');
  assert.deepEqual((await asRon.receive({ project: 'srv', topic: 'root-keys' })).messages.map((m) => m.text), ['own']);

  await assert.rejects(client.registerTopic({ project: 'srv', topic: 'p2', subject: 'S', visibility: 'private' }), /musí mať personu/);
});

test('wake rings only the holder on its own machine; replies ring the sender on its machine', async (t) => {
  const inst = makeInstance('machine');
  const srv = await startInstance(inst);
  const stateDir = tmpDir('comms-wake-machine-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, stateDir); });
  const onLaptop = makeClient(inst, { machine: 'Laptop' });
  const onServer = makeClient(inst, { machine: 'server' });
  // Topic taken over on the laptop (holder machine from the claiming client).
  await onLaptop.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-laptop', holder_machine: 'Laptop' });

  const mk = (machine) => {
    const calls = [];
    const adapter = { name: 'fake', command: ['fake'], ring: async (holder, text) => { calls.push({ holder, text }); return { ok: true }; } };
    const w = new Waker({ client: makeClient(inst), adapter, stateFile: path.join(stateDir, `${machine}.json`), maxEventAgeMs: 1e12, machine });
    return { w, calls };
  };
  const server = mk('server');
  const laptop = mk('laptop');
  await server.w.initCursor();
  await laptop.w.initCursor();

  const sent = await onServer.send({ project: 'p', topic: 't', level: 'blocker', text: 'need help', thread_id: 'thr-srv' });
  await server.w.pollOnce(300);
  await laptop.w.pollOnce(300);
  assert.equal(server.calls.length, 0);
  assert.deepEqual(laptop.calls.map((c) => c.holder), ['thr-laptop']);

  // Reply from the laptop wakes the original sender's thread on the server only.
  await onLaptop.send({ reply_to: sent.id, level: 'assist', text: 'done', thread_id: 'thr-laptop' });
  await server.w.pollOnce(300);
  await laptop.w.pollOnce(300);
  assert.deepEqual(server.calls.map((c) => c.holder), ['thr-srv']);
  assert.equal(laptop.calls.length, 1);
});

test('registration without a machine keeps the old behaviour (rung everywhere)', async (t) => {
  const inst = makeInstance('nomachine');
  const srv = await startInstance(inst);
  const stateDir = tmpDir('comms-wake-nomachine-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, stateDir); });
  const c = makeClient(inst);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  const calls = [];
  const w = new Waker({ client: makeClient(inst), adapter: { name: 'f', command: ['f'], ring: async (h) => { calls.push(h); return { ok: true }; } }, stateFile: path.join(stateDir, 's.json'), maxEventAgeMs: 1e12, machine: 'anything' });
  await w.initCursor();
  await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'x' });
  await w.pollOnce(300);
  assert.deepEqual(calls, ['thr-1']);
});

test('register-topic 1c: persona main and its topics are registered only with the persona key; an unmanaged persona stays open', async (t) => {
  const { client, asRon } = await setup(t);
  await assert.rejects(client.registerTopic({ persona: 'bob', main: true, holder: 'thr-x' }), (e) => e.status === 403);
  await assert.rejects(client.registerTopic({ project: 'srv', topic: 'disk', subject: 'D', persona: 'bob' }), (e) => e.status === 403);
  await asRon.registerTopic({ project: 'srv', topic: 'disk', subject: 'D', persona: 'bob', holder: 'thr-d' });
  // Re-registration by another key without naming the persona is refused too (the stored owner is bob).
  await assert.rejects(client.registerTopic({ project: 'srv', topic: 'disk', subject: 'D', holder: 'thr-d', takeover: true }), (e) => e.status === 403);
  await asRon.registerTopic({ persona: 'bob', main: true, holder: 'thr-main' });
  // Persona without a key of its own (alice) is not managed by keys: the operator key may register it.
  await client.registerTopic({ project: 'srv', topic: 'web', subject: 'W', persona: 'alice' });
  assert.equal((await asRon.registerTopic({ project: 'srv', topic: 'disk', subject: 'D', persona: 'bob', holder: 'thr-d' })).topic.persona, 'bob');
});
