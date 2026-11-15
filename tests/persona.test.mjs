// Persona addressing (P-045), private topics (P-041) and machine-aware wake (P-043).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, cleanup, tmpDir, api, addKey } from './helpers.mjs';
import { Waker } from '../src/wake/index.mjs';

async function setup(t) {
  const inst = makeInstance('persona');
  const srv = await startInstance(inst);
  t.after(async () => { await srv.stop(); cleanup(inst.dir); });
  return { inst, client: api(inst), asRon: api(inst, addKey(inst, 'bob')) };
}

const errorOf = (status, re = null) => (e) => e.status === status && (re === null || re.test(e.json?.error || ''));
const texts = (list) => list.map((m) => m.text);

test('persona alone goes to the main thread and waits even when the persona was never born', async (t) => {
  const { client, asRon } = await setup(t);
  const r = await client.send({ persona: 'bob', level: 'assist', text: 'ahoj' });
  assert.equal(r.status, 'pending');
  assert.match(r.warning, /no main thread yet/);
  const personaNotices = async () => (await client.notices()).filter((x) => x.kind === 'unregistered-persona');
  assert.equal((await personaNotices()).length, 1);
  // A second message does not create a second notice.
  await client.send({ persona: 'bob', level: 'assist', text: 'druhá' });
  assert.equal((await personaNotices()).length, 1);
  // Birth: the main thread registers and picks up everything; the notice is settled.
  await asRon.register({ persona: 'bob', main: true, holder: 'thr-bob-main', holder_machine: 'server' });
  const got = await asRon.receive({ persona: 'bob' });
  assert.deepEqual(texts(got), ['ahoj', 'druhá']);
  assert.deepEqual([got[0].project, got[0].topic, got[0].status], ['@bob', 'main', 'picked']);
  assert.equal((await personaNotices()).length, 0);
  // After birth no warning.
  const r2 = await client.send({ persona: 'bob', level: 'assist', text: 'tretia' });
  assert.equal(r2.warning, undefined);
});

test('persona + project + topic: to the topic only when the persona owns it, else to main with a reference', async (t) => {
  const { client, asRon } = await setup(t);
  await asRon.register({ persona: 'bob', main: true, holder: 'thr-main' });
  await asRon.register({ project: 'srv', topic: 'disk', subject: 'Disk', persona: 'bob', holder: 'thr-disk' });
  await client.register({ project: 'srv', topic: 'web', subject: 'Web', persona: 'alice' });

  await client.send({ persona: 'bob', project: 'srv', topic: 'disk', level: 'assist', text: 'owned' });
  await client.send({ persona: 'bob', project: 'srv', topic: 'web', level: 'assist', text: 'not owned' });
  await client.send({ persona: 'bob', project: 'srv', topic: 'missing', level: 'assist', text: 'missing' });
  await client.send({ persona: 'bob', project: 'srv', level: 'assist', text: 'project only' });

  assert.deepEqual(texts(await asRon.receive({ project: 'srv', topic: 'disk' })), ['owned']);
  const main = await asRon.receive({ persona: 'bob' });
  assert.deepEqual(main.map((m) => [m.text, m.ref_project, m.ref_topic]), [
    ['not owned', 'srv', 'web'], ['missing', 'srv', 'missing'], ['project only', 'srv', null],
  ]);
  await assert.rejects(client.send({ persona: 'bob', topic: 'disk', level: 'assist', text: 'x' }), errorOf(400, /vyžaduje projekt/));
});

test('private topic: invisible to other senders (same answer as an unknown topic), open to its owner', async (t) => {
  const { client, asRon } = await setup(t);
  await asRon.register({ persona: 'bob', main: true, holder: 'thr-main' });
  await asRon.register({ project: 'srv', topic: 'root-keys', subject: 'Root', persona: 'bob', visibility: 'private', holder: 'thr-priv' });

  const priv = await client.send({ project: 'srv', topic: 'root-keys', level: 'assist', text: 'peek' });
  const unknown = await client.send({ project: 'srv', topic: 'nothing', level: 'assist', text: 'x' });
  assert.deepEqual(Object.keys(priv).sort(), Object.keys(unknown).sort());
  assert.equal(priv.status, 'held');
  assert.equal(unknown.status, 'held');
  assert.equal((await asRon.receive({ project: 'srv', topic: 'root-keys' })).length, 0);

  // Persona address from an outsider also does not reach the private topic: it goes to main.
  await client.send({ persona: 'bob', project: 'srv', topic: 'root-keys', level: 'assist', text: 'via persona' });
  assert.equal((await asRon.receive({ project: 'srv', topic: 'root-keys' })).length, 0);
  assert.deepEqual(texts(await asRon.receive({ persona: 'bob' })), ['via persona']);

  // Operator decides "deliver": the held message goes to the owner's main thread, not into the topic.
  const notice = (await client.notices()).find((n) => n.message_id === priv.id);
  assert.match(notice.title, /Súkromná téma persony bob/);
  const d = await client.decide({ notice_id: notice.id, decision: 'deliver' });
  assert.deepEqual(d.delivered_to, { project: '@bob', topic: 'main' });
  // Receive returns pending mail only (I2): the one read above is picked and not returned again.
  assert.deepEqual(texts(await asRon.receive({ persona: 'bob' })), ['peek']);
  assert.equal((await client.message(priv.id)).status, 'picked');

  // The owner itself reaches its private topic directly.
  const own = await asRon.send({ project: 'srv', topic: 'root-keys', level: 'assist', text: 'own' });
  assert.equal(own.status, 'pending');
  assert.deepEqual(texts(await asRon.receive({ project: 'srv', topic: 'root-keys' })), ['own']);

  await assert.rejects(client.register({ project: 'srv', topic: 'p2', subject: 'S', visibility: 'private' }), errorOf(400, /musí mať personu/));
});

test('a message to a persona mailbox is read and resolved only by the persona key; its sender reads it back', async (t) => {
  const { inst, client, asRon } = await setup(t);
  await asRon.register({ persona: 'bob', main: true, holder: 'thr-main' });
  const m = await client.send({ persona: 'bob', level: 'assist', text: 'for bob', thread_id: 'thr-op' });
  const carol = api(inst, addKey(inst, 'carol'));
  assert.equal((await carol.call('GET', `/receive?id=${m.id}&persona=bob`)).status, 403);
  assert.equal((await carol.call('POST', '/resolve', { persona: 'bob', id: m.id, state: 'hotovo' })).status, 403);
  assert.equal((await client.message(m.id)).status, 'pending', 'nothing picked up');
  assert.equal((await client.receiveId(m.id)).text, 'for bob', 'the sender reads its message');
  assert.equal((await client.message(m.id)).status, 'pending', 'the sender\'s read is no pickup');
  await asRon.receive({ persona: 'bob', thread: 'thr-main' });
  const r = await asRon.resolve({ persona: 'bob', thread: 'thr-main', id: m.id, state: 'hotovo', text: 'done' });
  assert.deepEqual([r.status, r.resolution, r.answer_delivered], ['done', 'reply', true]);
  const replies = await client.receive({ thread: 'thr-op' });
  assert.deepEqual(replies.map((x) => [x.reply_to, x.sender, x.text]), [[m.id, 'bob', 'done']]);
});

test('register-topic 1c: persona main and its topics are registered only with the persona key; an unmanaged persona stays open', async (t) => {
  const { client, asRon } = await setup(t);
  await assert.rejects(client.register({ persona: 'bob', main: true, holder: 'thr-x' }), errorOf(403));
  await assert.rejects(client.register({ project: 'srv', topic: 'disk', subject: 'D', persona: 'bob' }), errorOf(403));
  await asRon.register({ project: 'srv', topic: 'disk', subject: 'D', persona: 'bob', holder: 'thr-d' });
  // Re-registration by another key without naming the persona is refused too (the stored owner is bob).
  await assert.rejects(client.register({ project: 'srv', topic: 'disk', subject: 'D', holder: 'thr-d', takeover: true }), errorOf(403));
  await asRon.register({ persona: 'bob', main: true, holder: 'thr-main' });
  // Persona without a key of its own (alice) is not managed by keys: the operator key may register it.
  await client.register({ project: 'srv', topic: 'web', subject: 'W', persona: 'alice' });
  assert.equal((await asRon.register({ project: 'srv', topic: 'disk', subject: 'D', persona: 'bob', holder: 'thr-d' })).topic.persona, 'bob');
});

// --- wake ---------------------------------------------------------------------------------------------

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
  await onLaptop.receive({ project: 'p', topic: 't', thread: 'thr-laptop' });
  await onLaptop.resolve({ project: 'p', topic: 't', id: sent.id, state: 'hotovo', text: 'done', thread_id: 'thr-laptop' });
  await server.w.pollOnce(300);
  await laptop.w.pollOnce(300);
  assert.deepEqual(server.calls.map((c) => c.holder), ['thr-srv']);
  assert.equal(laptop.calls.length, 1);
});

test('a holder registered without a machine is rung by every wake module', async (t) => {
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
