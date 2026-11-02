// M6: wake module (P-043). Limit 6/h per pair, stop on repeat, fixed template,
// adapter t3 called with the holder id, mail delivers without wake.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeInstance, startInstance, makeClient, cleanup, sleep, tmpDir } from './helpers.mjs';
import { Waker, WAKE_TEMPLATE, KEEPALIVE_TEMPLATE, CONTINUE_TEMPLATE, NO_REPLY_TEMPLATE, UNDELIVERED_TEMPLATE, wakeText, ringText, pairKey, resolveWakeConfig, readSince } from '../src/wake/index.mjs';
import { parseTimestamp, parseDuration } from '../src/common/util.mjs';
import { createT3Adapter, parseCommand } from '../src/adapters/t3/index.mjs';

const FAKE_T3 = fileURLToPath(new URL('./fixtures/fake-t3.mjs', import.meta.url));
const WAKE_BIN = fileURLToPath(new URL('../bin/wake.mjs', import.meta.url));

function fakeAdapter(ok = true) {
  const calls = [];
  return { name: 'fake', command: ['fake'], calls, ring: async (holder, text) => { calls.push({ holder, text }); return { ok }; } };
}

/** Fake adapter with `status`: `running[thread] = true` marks a running thread; `ok` switches ring success. */
function keepaliveAdapter() {
  const a = { name: 'fake', command: ['fake'], calls: [], statusCalls: [], running: {}, ok: true };
  a.ring = async (holder, text) => { a.calls.push({ holder, text }); return { ok: a.ok }; };
  a.status = async (thread) => { a.statusCalls.push(thread); return { ok: true, running: !!a.running[thread], status: a.running[thread] ? 'running' : 'completed' }; };
  return a;
}

/** Instance with a registered topic p/t held by thread `thr-1`, a waker on a fake adapter and a fake clock. */
async function setup(t, { holderMode = null, adapter = fakeAdapter(), ...wakeOpts } = {}) {
  const inst = makeInstance('wake');
  const srv = await startInstance(inst);
  const stateDir = tmpDir('comms-wake-state-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, stateDir); });
  const client = makeClient(inst);
  await client.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1', holder_mode: holderMode });
  const clock = { t: Date.now() };
  const logs = [];
  const waker = new Waker({
    client: makeClient(inst), adapter, stateFile: path.join(stateDir, 'wake-state.json'),
    log: (m) => logs.push(m), now: () => clock.t, maxEventAgeMs: 1e12, idleDedupeMs: 0, ...wakeOpts,
  });
  await waker.initCursor();
  return { inst, client, waker, adapter, clock, logs, stateDir, svc: srv.srv.service };
}

async function waitFor(fn, timeout = 5000, step = 25) {
  const until = Date.now() + timeout;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > until) throw new Error('waitFor: timeout');
    await sleep(step);
  }
}

test('blocker rings the topic holder with the fixed template, never the body', async (t) => {
  const { client, waker, adapter } = await setup(t);
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'SECRET BODY: prevezmi tému X', thread_id: 'thr-sender' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `${WAKE_TEMPLATE} p/t` }]);
  assert.ok(!adapter.calls[0].text.includes('SECRET'));
  assert.equal(wakeText({ project: 'p', topic: 't' }), 'you have mail in topic p/t');
  assert.equal(wakeText({ holder: 'thr-9' }), 'you have mail in topic thread/thr-9');
  // Cursor persisted; no replay on the next poll.
  assert.equal(await waker.pollOnce(0), 0);
  assert.equal(JSON.parse(fs.readFileSync(waker.state.file, 'utf8')).cursor, 1);
});

test('assist and steer wake the holder (bob 2026-11-02); a reply to any level wakes the sender thread', async (t) => {
  const { inst, client, waker, adapter } = await setup(t);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  await client.registerTopic({ project: 'q', topic: 'u', subject: 'S2', holder: 'thr-q' });
  const a = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'help?', thread_id: 'thr-a', from_project: 'q', from_topic: 'u' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: 'you have mail in topic p/t' }]);
  adapter.calls.length = 0;

  await bob.send({ reply_to: a.id, text: 'answer' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-a', text: 'you have mail in topic q/u' }]);
  const s = await bob.send({ project: 'p', topic: 't', level: 'steer', text: 'stop that', thread_id: 'thr-a' });
  assert.equal(s.level, 'steer');
  adapter.calls.length = 0;
  const st = await waker.pollOnce(500);
  assert.equal(st, 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: 'you have mail in topic p/t' }]);
  adapter.calls.length = 0;

  // Blocker from a main thread without a topic: the reply names the thread.
  const b = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'urgent', thread_id: 'thr-main' });
  await bob.send({ reply_to: b.id, level: 'assist', text: 'done' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.deepEqual(adapter.calls, [
    { holder: 'thr-1', text: 'you have mail in topic p/t' },
    { holder: 'thr-main', text: 'you have mail in topic thread/thr-main' },
  ]);
});

test('limit 6 per hour per pair of topics: above it a notice, no ring; resumes after an hour', async (t) => {
  const { inst, client, waker, adapter, clock } = await setup(t);
  for (let n = 1; n <= 8; n++) {
    await client.send({ project: 'p', topic: 't', level: 'blocker', text: `message ${n}`, from_project: 'q', from_topic: 'u' });
  }
  assert.equal(await waker.pollOnce(500), 8);
  assert.equal(adapter.calls.length, 6);
  assert.equal(waker.stats.limited, 2);
  const notices = (await client.notices()).notices.filter((n) => n.kind === 'wake-limit');
  assert.equal(notices.length, 1, 'one open notice per pair (deduplicated)');
  assert.match(notices[0].title, /Limit budenia/);
  assert.equal(notices[0].message_id, (await client.receive({ project: 'p', topic: 't' })).messages[6].id);
  assert.deepEqual(notices[0].options.map((o) => o.id), ['ack']);
  // Messages above the limit are still in the mailbox.
  assert.equal((await client.receive({ project: 'p', topic: 't' })).messages.length, 8);

  // Another pair is not affected.
  await client.registerTopic({ project: 'p', topic: 'other', subject: 'O', holder: 'thr-2' });
  await client.send({ project: 'p', topic: 'other', level: 'blocker', text: 'x', from_project: 'q', from_topic: 'u' });
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 7);
  assert.equal(adapter.calls[6].holder, 'thr-2');

  // Both directions share the limit.
  assert.equal(pairKey({ from_project: 'q', from_topic: 'u', project: 'p', topic: 't' }), pairKey({ from_project: 'p', from_topic: 't', project: 'q', topic: 'u' }));

  // After an hour the window is free again; the decided notice can be re-created.
  await client.decide({ notice_id: notices[0].id, decision: 'ack' });
  clock.t += 61 * 60 * 1000;
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'message 9', from_project: 'q', from_topic: 'u' });
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 8);
});

test('the same message twice in a row stops waking; a different one resumes', async (t) => {
  const { client, waker, adapter } = await setup(t);
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'same text', from_project: 'q', from_topic: 'u' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'same text', from_project: 'q', from_topic: 'u' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'same text', from_project: 'q', from_topic: 'u' });
  assert.equal(await waker.pollOnce(500), 3);
  assert.equal(adapter.calls.length, 1);
  const notices = (await client.notices()).notices.filter((n) => n.kind === 'wake-limit');
  assert.equal(notices.length, 1);
  assert.match(notices[0].detail, /rovnaká správa/);

  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'different text', from_project: 'q', from_topic: 'u' });
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 2);
  // Same text from another sender/topic is a different direction and rings.
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'same text', from_project: 'z', from_topic: 'z' });
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 3);
});

test('holder mode operator: blocker rings the holder and creates the notice (bob 2026-11-02), reply wakes the sender', async (t) => {
  const { inst, client, waker, adapter } = await setup(t, { holderMode: 'operator' });
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  const b = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'need you', thread_id: 'thr-a' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: 'you have mail in topic p/t' }]);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'blocker').length, 1);
  await bob.send({ reply_to: b.id, level: 'assist', text: 'here' });
  await waker.pollOnce(500);
  assert.deepEqual(adapter.calls.slice(1), [{ holder: 'thr-a', text: 'you have mail in topic thread/thr-a' }]);
  // A topic without holder cannot be rung; without an adapter nothing is rung.
  await client.registerTopic({ project: 'p', topic: 'free', subject: 'F' });
  await client.send({ project: 'p', topic: 'free', level: 'blocker', text: 'x' });
  await waker.pollOnce(500);
  assert.equal(waker.stats.skipped, 1);
  await assert.rejects(client.registerTopic({ project: 'p', topic: 'bad', subject: 'B', holder: 'h', holder_mode: 'robot' }), (e) => e.status === 400);
});

test('mail delivers without the wake module; events wait for a later consumer', async (t) => {
  const inst = makeInstance('nowake');
  const srv = await startInstance(inst);
  t.after(async () => { await srv.stop(); cleanup(inst.dir); });
  const c = makeClient(inst);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'nobody is listening' });
  const r = await c.receive({ project: 'p', topic: 't' });
  assert.equal(r.messages.length, 1);
  assert.equal(r.messages[0].text, 'nobody is listening');
  const ev = await c.events(0, 0);
  assert.equal(ev.events.length, 1);
  assert.equal(ev.events[0].holder, 'thr-1');
  assert.equal(ev.events[0].text, undefined, 'events never carry the body');
  assert.equal((await c.events('latest', 0)).next, 1);
  // A client may not create arbitrary notice kinds.
  await assert.rejects(c.createNotice({ kind: 'blocker', title: 'x', message_id: r.messages[0].id }), (e) => e.status === 400);
});

test('adapter t3 runs the configured command with <threadId> <text>; failure is reported, not thrown', async (t) => {
  const dir = tmpDir('comms-t3-');
  t.after(() => cleanup(dir));
  const logFile = path.join(dir, 'calls.log');
  const env = { ...process.env, FAKE_T3_LOG: logFile };
  const a = createT3Adapter({ command: [process.execPath, FAKE_T3], env });
  assert.equal(a.name, 't3');
  const r = await a.ring('thr-1', 'you have mail in topic p/t');
  assert.equal(r.ok, true);
  assert.deepEqual(JSON.parse(fs.readFileSync(logFile, 'utf8').trim()), ['thr-1', 'you have mail in topic p/t']);

  const failing = createT3Adapter({ command: [process.execPath, FAKE_T3], env: { ...env, FAKE_T3_EXIT: '3' } });
  assert.equal((await failing.ring('thr-1', 'x')).ok, false);
  const missing = createT3Adapter({ command: 'definitely-missing-t3-binary thread send' });
  assert.equal((await missing.ring('thr-1', 'x')).ok, false);
  assert.deepEqual(parseCommand('t3 thread send --base-dir "/x y"'), ['t3', 'thread', 'send', '--base-dir', '/x y']);
  assert.deepEqual(createT3Adapter().command, ['t3', 'thread', 'send']);
});

test('wake config: flags > env > config file > defaults', (t) => {
  const dir = tmpDir('comms-wakecfg-');
  t.after(() => cleanup(dir));
  const cfgFile = path.join(dir, 'comms.json');
  fs.writeFileSync(cfgFile, JSON.stringify({ data_dir: dir, key: 'k', wake: { command: 'fake-t3 send', limit_per_hour: 3, state_dir: path.join(dir, 'st') } }));
  const env = { HOME: process.env.HOME, PATH: process.env.PATH };
  const c1 = resolveWakeConfig({ config: cfgFile }, env);
  assert.equal(c1.command, 'fake-t3 send');
  assert.equal(c1.limitPerHour, 3);
  assert.equal(c1.stopOnRepeat, 2);
  assert.equal(c1.stateFile, path.join(dir, 'st', 'wake-state.json'));
  const c2 = resolveWakeConfig({ config: cfgFile, limitPerHour: '5' }, { ...env, WAKE_COMMAND: 'env-t3', WAKE_LIMIT_PER_HOUR: '4' });
  assert.equal(c2.command, 'env-t3');
  assert.equal(c2.limitPerHour, 5);
  const c3 = resolveWakeConfig({ dataDir: dir }, env);
  assert.deepEqual(c3.command, ['t3', 'thread', 'send']);
  assert.equal(c3.stateFile, path.join(dir, 'client', 'wake', 'wake-state.json'));
});

test('bin/wake.mjs run: rings through the command, survives a mail restart, stops on SIGTERM', async (t) => {
  const inst = makeInstance('wakeproc');
  const dir = tmpDir('comms-wakeproc-');
  const logFile = path.join(dir, 'calls.log');
  let srv = await startInstance(inst);
  const child = spawn(process.execPath, [WAKE_BIN, 'run', '--data-dir', inst.dir, '--no-start', '--command', `${process.execPath} ${FAKE_T3}`,
    '--wait-ms', '300', '--reconnect-ms', '100', '--state-dir', dir], {
    env: { PATH: process.env.PATH, HOME: process.env.HOME, FAKE_T3_LOG: logFile, COMMS_SELF_RESTART: '0' }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '';
  let err = '';
  child.stdout.on('data', (d) => { out += d; });
  child.stderr.on('data', (d) => { err += d; });
  const exited = new Promise((r) => child.on('exit', (code, sig) => r({ code, sig })));
  t.after(async () => { try { child.kill('SIGKILL'); } catch { /* gone */ } await exited; if (srv) await srv.stop(); cleanup(inst.dir, dir); });

  const stateFile = path.join(dir, 'wake-state.json');
  await waitFor(() => fs.existsSync(stateFile));
  // The module provisioned its own key `wake`.
  assert.ok(inst.store.keys().some((k) => k.sender === 'wake'));
  const c = makeClient(inst);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'one' });
  const lines = () => (fs.existsSync(logFile) ? fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean) : []);
  await waitFor(() => lines().length >= 1);
  assert.deepEqual(JSON.parse(lines()[0]), ['thr-1', 'you have mail in topic p/t']);
  // The rung thread took its turn (else the same bell again would be skipped as queued).
  await sleep(5);
  const o = await c.turn({ thread: 'thr-1', state: 'open' });
  await c.turn({ thread: 'thr-1', state: 'closed', opened_at: o.turn.opened_at });

  // Mail restarts; the module reconnects and continues from its cursor.
  await srv.stop();
  srv = null;
  await waitFor(() => err.includes('comms unavailable'));
  srv = await startInstance(inst);
  await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'two' });
  await waitFor(() => lines().length >= 2, 8000);
  assert.equal(lines().length, 2);

  child.kill('SIGTERM');
  const r = await exited;
  assert.equal(r.code, 0);
  assert.match(out, /"stopped":true/);
  assert.match(out, /"rang":2/);
});

// --- hotfix 0.2.1 / 0.3.0: expired deadline (`no reply <id>`) and keepalive ----------------

test('expired deadline: rings the sender with `no reply <id>`, never the recipient; answered meanwhile → skip', async (t) => {
  const { inst, client, waker, adapter, svc, logs } = await setup(t);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  await client.registerTopic({ project: 'q', topic: 'u', subject: 'Src', holder: 'thr-q' });
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'SECRET?', ttl: '60s', thread_id: 'thr-a', from_project: 'q', from_topic: 'u' });
  const m2 = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'main?', ttl: '60s', from_project: 'q', from_topic: 'u' });
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'later deadline', ttl: '30m', thread_id: 'thr-a' });
  assert.equal(await waker.pollOnce(500), 3, 'each assist rings the recipient once');
  adapter.calls.length = 0;
  // The service check runs every 30 s; here directly with a time past the deadline.
  svc.checkExpired(Date.now() + 61_000);
  assert.equal(await waker.pollOnce(500), 2);
  assert.deepEqual(adapter.calls, [
    { holder: 'thr-a', text: `no reply ${m.id}` },
    { holder: 'thr-q', text: `no reply ${m2.id}` },
  ]);
  assert.equal(NO_REPLY_TEMPLATE, 'no reply');
  assert.ok(!adapter.calls.some((c) => c.holder === 'thr-1'), 'the expiry does not wake the recipient (holder of p/t)');

  // Second check: nothing new. A real late reply wakes the sender with the usual template.
  svc.checkExpired(Date.now() + 61_000);
  assert.equal(await waker.pollOnce(0), 0);
  await bob.send({ reply_to: m.id, level: 'assist', text: 'late' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls[2], { holder: 'thr-a', text: `${WAKE_TEMPLATE} q/u` });

  // An expired event of a message answered (or acked) before the ring is skipped.
  const r = await waker.handle({ seq: 99, type: 'expired', message_id: m.id, level: 'assist', project: 'p', topic: 't', holder: 'thr-a' });
  assert.deepEqual([r.action, r.reason], ['skipped', 'answered']);
  await bob.ack([m2.id]);
  assert.equal((await waker.handle({ seq: 100, type: 'expired', message_id: m2.id, project: 'p', topic: 't', holder: 'thr-q' })).reason, 'answered');
  assert.equal(adapter.calls.length, 3);
  assert.ok(logs.some((l) => /skip 99 \(answered\)/.test(l)));
});

test('one queued ring per open turn of a busy holder; a new turn may be rung again; state persisted and pruned', async (t) => {
  const { client, waker, adapter } = await setup(t);
  const o1 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'two', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal(adapter.calls.length, 1, 'the second ring would queue a second turn');
  assert.equal(waker.stats.skipped, 1);
  assert.deepEqual(JSON.parse(fs.readFileSync(waker.state.file, 'utf8')).queued, { 'thr-1': o1.turn.opened_at });
  assert.equal(Object.keys(waker.state.rings).length, 1, 'a queued skip does not count against the limit');

  // The queued ring started a new turn: the next event rings again.
  await sleep(5);
  const o2 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'three', thread_id: 'thr-s3' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 2);
  assert.deepEqual(waker.state.queued, { 'thr-1': o2.turn.opened_at });
  // Closed turn: no queue mark, rings; the old mark is pruned.
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o2.turn.opened_at });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'four', thread_id: 'thr-s4' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 3);
  assert.deepEqual(waker.state.queued, {});
});

test('idle holder: the same bell again before its turn opens is skipped; after the turn opened it rings (fable audit M1)', async (t) => {
  const { client, waker, adapter } = await setup(t, { idleDedupeMs: 120_000 });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'two', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal(adapter.calls.length, 1, 'the turn of the first ring sees both messages');
  // The rung turn opened and closed: a later message rings again.
  await sleep(5);
  const o = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o.turn.opened_at });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'three', thread_id: 'thr-s3' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 2);
});

/** Keepalive setup: waker on machine m1 with a fake status adapter and a fixed T3 start time. */
async function kaSetup(t, extra = {}) {
  const adapter = keepaliveAdapter();
  const t3 = { since: null };
  const ctx = await setup(t, { adapter, machine: 'm1', keepaliveSince: async () => t3.since, ...extra });
  return { ...ctx, t3 };
}

test('keepalive: interrupted turn is rung once and closed; the next turn (after the T3 start) is not rung', async (t) => {
  const { client, waker, adapter, t3, clock } = await kaSetup(t);
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't', holder_mode: 'auto' });
  await client.turn({ thread: 'thr-run', state: 'open', machine: 'm1' });
  await client.turn({ thread: 'thr-other', state: 'open', machine: 'm2' });
  await client.turn({ thread: 'thr-done', state: 'open', machine: 'm1' });
  await client.turn({ thread: 'thr-done', state: 'closed' });
  adapter.running['thr-run'] = true;
  await sleep(5);
  t3.since = Date.now(); // T3 restarted after these turns opened

  const r1 = await waker.keepaliveOnce();
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: KEEPALIVE_TEMPLATE }]);
  assert.equal(KEEPALIVE_TEMPLATE, 'turn interrupted, continue from handover');
  assert.deepEqual(r1.map((x) => [x.thread, x.action, x.reason]).sort(), [['thr-1', 'rang', null], ['thr-other', 'skipped', 'other-machine'], ['thr-run', 'skipped', 'running']]);
  assert.ok(!adapter.statusCalls.includes('thr-other'));
  const all = (await client.turns(false)).turns;
  const rec = all.find((x) => x.thread === 'thr-1');
  assert.deepEqual([rec.state, rec.close_reason, rec.keepalive.count], ['closed', 'keepalive', 1]);

  // The rung thread starts a new turn (hook UserPromptSubmit) after the T3 start: no loop.
  await sleep(5);
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't' });
  clock.t += 5 * 60_000;
  const r2 = await waker.keepaliveOnce();
  assert.equal(adapter.calls.length, 1);
  assert.deepEqual(r2.find((x) => x.thread === 'thr-1').reason, 'after-restart');
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'turn-interrupted').length, 0);
});

test('keepalive: operator mode only creates a turn-interrupted notice and closes the turn', async (t) => {
  const { client, waker, adapter, t3 } = await kaSetup(t);
  await client.turn({ thread: 'thr-op', state: 'open', machine: 'm1', project: 'p', topic: 't', holder_mode: 'operator' });
  await sleep(5);
  t3.since = Date.now();
  const r = await waker.keepaliveOnce();
  assert.deepEqual(r.map((x) => [x.action, x.reason]), [['notified', 'operator-mode']]);
  assert.equal(adapter.calls.length, 0);
  const n = (await client.notices()).notices.filter((x) => x.kind === 'turn-interrupted');
  assert.equal(n.length, 1);
  assert.deepEqual([n[0].thread_id, n[0].project, n[0].topic], ['thr-op', 'p', 't']);
  assert.deepEqual(n[0].options.map((o) => o.id), ['ack']);
  assert.equal((await client.turns(true)).turns.length, 0);
});

test('keepalive: at most 3 attempts per hour, ≥ 60 s apart; after the 3rd failure a notice and closed', async (t) => {
  const { client, waker, adapter, t3, clock } = await kaSetup(t);
  adapter.ok = false;
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't' });
  await sleep(5);
  t3.since = Date.now();
  const step = async () => (await waker.keepaliveOnce()).find((x) => x.thread === 'thr-1');
  assert.deepEqual([(await step()).action, adapter.calls.length], ['rang', 1]);
  clock.t += 30_000;
  assert.equal((await step()).reason, 'backoff');
  assert.equal(adapter.calls.length, 1);
  clock.t += 31_000;
  assert.deepEqual([(await step()).attempt, adapter.calls.length], [2, 2]);
  clock.t += 61_000;
  const third = await step();
  assert.deepEqual([third.action, third.reason, adapter.calls.length], ['notified', 'failed', 3]);
  assert.equal((await client.turns(true)).turns.length, 0, 'closed after the 3rd failure');
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'turn-interrupted').length, 1);

  // Within the same hour a new interrupted turn of the same thread is not rung again.
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await sleep(5);
  t3.since = Date.now();
  clock.t += 10 * 60_000;
  assert.deepEqual([(await step()).reason, adapter.calls.length], ['limit', 3]);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'turn-interrupted').length, 1, 'deduplicated per thread');
  // After an hour the window is free again.
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await sleep(5);
  t3.since = Date.now();
  clock.t += 61 * 60_000;
  adapter.ok = true;
  assert.deepEqual([(await step()).action, adapter.calls.length], ['rang', 4]);
});

test('keepalive is off without since_command; unknown T3 start skips the round', async (t) => {
  const { client, waker, adapter } = await setup(t, { adapter: keepaliveAdapter(), machine: 'm1' });
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.deepEqual(await waker.keepaliveOnce(), []);
  assert.equal(adapter.statusCalls.length, 0);
  const env = { HOME: process.env.HOME, PATH: process.env.PATH };
  const dir = tmpDir('comms-kacfg-');
  t.after(() => cleanup(dir));
  const off = resolveWakeConfig({ dataDir: dir }, env);
  assert.equal(off.keepaliveSinceCommand, null);
  assert.equal(off.keepaliveIntervalMs, 60_000);
  assert.deepEqual(off.statusCommand, ['t3', 'thread', 'show', '--json']);
  const cfgFile = path.join(dir, 'c.json');
  fs.writeFileSync(cfgFile, JSON.stringify({ data_dir: dir, wake: { status_command: 'st', keepalive: { since_command: 'cfg-cmd', interval_ms: 5000 } } }));
  const on = resolveWakeConfig({ config: cfgFile }, env);
  assert.deepEqual([on.keepaliveSinceCommand, on.keepaliveIntervalMs, on.statusCommand], ['cfg-cmd', 5000, 'st']);
  assert.equal(resolveWakeConfig({ config: cfgFile }, { ...env, WAKE_KEEPALIVE_SINCE_COMMAND: 'env-cmd' }).keepaliveSinceCommand, 'env-cmd');

  const unknown = await setup(t, { adapter: keepaliveAdapter(), machine: 'm1', keepaliveSince: async () => null });
  await unknown.client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.deepEqual(await unknown.waker.keepaliveOnce(), []);
  assert.equal(unknown.adapter.statusCalls.length, 0);
});

test('T3 start time parsing (ISO, epoch, @epoch, systemctl) and since_command; adapter status via t3 thread show --json', async (t) => {
  assert.equal(parseTimestamp('2026-11-01T16:32:21Z'), Date.parse('2026-11-01T16:32:21Z'));
  assert.equal(parseTimestamp('1793550741'), 1793550741000);
  assert.equal(parseTimestamp('1793550741000\n'), 1793550741000);
  assert.equal(parseTimestamp('@1793550741'), 1793550741000);
  assert.equal(parseTimestamp('Sun 2026-11-01 16:32:21 UTC'), Date.parse('2026-11-01T16:32:21Z'));
  assert.equal(parseTimestamp(''), null);
  assert.equal(parseTimestamp('n/a'), null);
  assert.equal(parseTimestamp('garbage'), null);
  assert.deepEqual([parseDuration('90s'), parseDuration('30m'), parseDuration('4h'), parseDuration(120), parseDuration('0'), parseDuration('x')], [90, 1800, 14400, 120, 0, null]);

  assert.equal(await readSince(`"${process.execPath}" -e "console.log('2026-11-01T16:32:21Z')"`), Date.parse('2026-11-01T16:32:21Z'));
  assert.equal(await readSince(`"${process.execPath}" -e "process.exit(1)"`), null);

  const dir = tmpDir('comms-t3st-');
  t.after(() => cleanup(dir));
  const logFile = path.join(dir, 'calls.log');
  const mk = (stdout, exit = '0') => createT3Adapter({ statusCommand: [process.execPath, FAKE_T3, 'thread', 'show', '--json'], env: { ...process.env, FAKE_T3_LOG: logFile, FAKE_T3_STDOUT: stdout, FAKE_T3_EXIT: exit } });
  assert.deepEqual(await mk(JSON.stringify({ thread: { status: 'completed', activeRunId: null } })).status('thr-1'), { ok: true, running: false, status: 'completed' });
  assert.deepEqual(await mk(JSON.stringify({ thread: { status: 'running', activeRunId: 'run-7' } })).status('thr-1'), { ok: true, running: true, status: 'running' });
  assert.equal((await mk('not json').status('thr-1')).ok, false);
  assert.equal((await mk('{}', '4').status('thr-1')).ok, false);
  assert.deepEqual(JSON.parse(fs.readFileSync(logFile, 'utf8').trim().split('\n')[0]), ['thread', 'show', '--json', 'thr-1']);
});

test('run(): the keepalive loop runs next to the long-poll and stops with the signal', async (t) => {
  const { client, waker, adapter, t3 } = await kaSetup(t, { keepaliveIntervalMs: 50, waitMs: 100 });
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await sleep(5);
  t3.since = Date.now();
  const ac = new AbortController();
  const running = waker.run({ signal: ac.signal });
  await waitFor(() => adapter.calls.length >= 1);
  ac.abort();
  const stats = await running;
  assert.equal(stats.keepalive_rang, 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: KEEPALIVE_TEMPLATE }]);
});

test('ring to an archived thread bounces: the sender is rung with `undelivered <id>` once, no mail, the message stays pending', async (t) => {
  const adapter = { name: 'fake', command: ['fake'], calls: [], ring: async (holder, text) => { adapter.calls.push({ holder, text }); return holder === 'thr-1' || holder === 'thr-dead' ? { ok: false, stderr: `Thread ${holder} is archived and cannot receive messages.` } : { ok: true }; } };
  const { client, waker } = await setup(t, { adapter });
  const m = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'restart?', thread_id: 'thr-sender' });
  assert.equal(await waker.pollOnce(500), 1);
  // The bounce is an `undelivered` event → the sender's thread is rung with the exact text.
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `${WAKE_TEMPLATE} p/t` }, { holder: 'thr-sender', text: `undelivered ${m.id}` }]);
  assert.equal(UNDELIVERED_TEMPLATE, 'undelivered');
  assert.equal(ringText({ type: 'undelivered', message_id: 'x1' }), 'undelivered x1');
  const st = await client.message(m.id);
  assert.deepEqual([st.state, st.replied, st.outcome, st.undelivered_holder], ['pending', false, 'undelivered', 'thr-1']);
  assert.match(st.undelivered_reason, /archived/);
  assert.equal(((await client.receive({ thread: 'thr-sender' })).messages || []).length, 0, 'no mail from comms');
  // Once per message.
  assert.equal((await client.bounce({ message_id: m.id, holder: 'thr-1', reason: 'archived' })).reason, 'already');
  assert.equal((await client.notices()).notices.filter((n) => n.kind !== 'blocker').length, 0, 'no operator notice');

  // A failed `undelivered`/`expired` ring never bounces (only a log line).
  const m2 = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'i', thread_id: 'thr-dead' });
  const r = await waker.handle({ seq: 50, type: 'undelivered', message_id: m2.id, project: 'p', topic: 't', holder: 'thr-dead' });
  assert.deepEqual([r.action, r.ok, r.bounce], ['rang', false, undefined]);
  const r2 = await waker.handle({ seq: 51, type: 'expired', message_id: m2.id, project: 'p', topic: 't', holder: 'thr-dead' });
  assert.deepEqual([r2.action, r2.ok, r2.bounce], ['rang', false, undefined]);
  assert.equal((await client.message(m2.id)).outcome, null);
});

test('hour limit applies to expired/undelivered rings too (wake-limit notice)', async (t) => {
  const { client, waker, adapter } = await setup(t, { limitPerHour: 1 });
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'a', thread_id: 'thr-s' });
  const ev = { type: 'undelivered', message_id: m.id, project: 'p', topic: 't', holder: 'thr-s', thread_id: 'thr-s' };
  assert.equal((await waker.handle({ seq: 1, ...ev })).action, 'rang');
  const r = await waker.handle({ seq: 2, ...ev, type: 'expired' });
  assert.deepEqual([r.action, r.reason], ['limited', 'hour']);
  assert.equal(adapter.calls.length, 1);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'wake-limit').length, 1);
});

test('transient ring failure does not bounce', async (t) => {
  const { client, waker } = await setup(t, { adapter: fakeAdapter(false) });
  const m = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'x', thread_id: 'thr-sender' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal((await client.message(m.id)).replied, false);
});

test('turn records accept T3 thread ids (mcp:<uuid>) and keepalive rings them', async (t) => {
  const { client, waker, adapter, t3 } = await kaSetup(t);
  const thread = 'mcp:3c22248f-b132-4e46-8546-f99b101b4298';
  const r = await client.turn({ thread, state: 'open', machine: 'm1' });
  assert.equal(r.ok, true);
  assert.equal((await client.turns(true)).turns.map((x) => x.thread).join(), thread);
  await sleep(5);
  t3.since = Date.now();
  const out = await waker.keepaliveOnce();
  assert.deepEqual(out.map((x) => x.action), ['rang']);
  assert.equal(adapter.calls[0].holder, thread);
  await assert.rejects(client.turn({ thread: '../x', state: 'open' }), /thread/);
});

test('6: t3 status: exit 0 JSON without a thread object is an unknown state (ok:false), never running:false', async () => {
  const status = (script) => createT3Adapter({ statusCommand: [process.execPath, '-e', script] }).status('thr-1');
  const noThread = await status('console.log(JSON.stringify({ id: "x" }))');
  assert.equal(noThread.ok, false);
  assert.notEqual(noThread.running, false);
  assert.equal((await status('console.log(JSON.stringify({ thread: "x" }))')).ok, false, 'thread must be an object');
  assert.equal((await status('console.log("not json")')).ok, false);
  assert.deepEqual(await status('console.log(JSON.stringify({ thread: { status: "running", activeRunId: "r1" } }))'), { ok: true, running: true, status: 'running' });
  assert.deepEqual(await status('console.log(JSON.stringify({ thread: { status: "completed", activeRunId: null } }))'), { ok: true, running: false, status: 'completed' });
});

test('7: closing a keepalive turn closes only that turn; a newer open turn of the same thread stays open', async (t) => {
  const { client, waker } = await setup(t);
  const old = (await client.turn({ thread: 'thr-k', state: 'open', machine: 'm1' })).turn;
  await sleep(5);
  const fresh = (await client.turn({ thread: 'thr-k', state: 'open', machine: 'm1' })).turn;
  assert.ok(fresh.opened_at > old.opened_at);
  await waker.closeTurn(old);
  const open = (await client.turns(true)).turns.filter((x) => x.thread === 'thr-k');
  assert.deepEqual(open.map((x) => x.opened_at), [fresh.opened_at], 'the current turn is not closed by a close of the old one');
});

test('bob 2026-11-02 scenario: blocker into an operator-mode topic rings the holder; assist to a main thread held by an archived thread bounces to the sender at once', async (t) => {
  const adapter = fakeAdapter();
  adapter.ring = async (holder, text) => {
    adapter.calls.push({ holder, text });
    return holder === 'thr-archived' ? { ok: false, stderr: 'thread is archived' } : { ok: true };
  };
  const { inst, client, waker } = await setup(t, { holderMode: 'operator', adapter });
  // @alice/main is held by a thread that was archived with its topic.
  await client.registerTopic({ persona: 'alice', main: true, holder: 'thr-archived' });
  await client.registerTopic({ project: 'bob', topic: 'units', subject: 'U', holder: 'thr-bob' });
  const assist = await client.send({ persona: 'alice', level: 'assist', text: 'units?', thread_id: 'thr-bob', from_project: 'bob', from_topic: 'units' });
  // Blocker into p/t held in operator mode by thr-1 (the dev thread).
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'units!', thread_id: 'thr-bob', from_project: 'bob', from_topic: 'units' });
  await waker.pollOnce(500);
  await waker.pollOnce(500);
  const rings = adapter.calls.map((c) => [c.holder, c.text]);
  assert.deepEqual(rings, [
    ['thr-archived', 'you have mail in topic @alice/main'],
    ['thr-1', 'you have mail in topic p/t'],
    ['thr-bob', `${UNDELIVERED_TEMPLATE} ${assist.id}`],
  ]);
  assert.equal((await client.message(assist.id)).outcome, 'undelivered');
});

test('continuation: a closed turn with a next step is rung once after the delay; the same step again is a turn-stalled notice', async (t) => {
  const { client, waker, adapter, clock } = await kaSetup(t);
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't' });
  await client.turn({ thread: 'thr-1', state: 'closed', next: 'implement y' });
  await client.turn({ thread: 'thr-none', state: 'open', machine: 'm1' });
  await client.turn({ thread: 'thr-none', state: 'closed' });
  await client.turn({ thread: 'thr-other', state: 'open', machine: 'm2' });
  await client.turn({ thread: 'thr-other', state: 'closed', next: 'x' });
  assert.equal((await client.turns(false)).turns.find((x) => x.thread === 'thr-1').next, 'implement y');

  const early = await waker.continueOnce();
  assert.deepEqual(early.find((x) => x.thread === 'thr-1').reason, 'delay', 'an operator prompt within the delay wins');
  clock.t = Date.now() + 2 * 60_000;
  await waker.continueOnce();
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: CONTINUE_TEMPLATE }]);
  await waker.continueOnce();
  assert.equal(adapter.calls.length, 1, 'one ring per closed turn');

  // The continued turn ends again with the same next step: no progress -> notice, no ring.
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't' });
  await sleep(5);
  await client.turn({ thread: 'thr-1', state: 'closed', next: 'implement y' });
  clock.t = Date.now() + 2 * 60_000;
  const r = await waker.continueOnce();
  assert.equal(r.find((x) => x.thread === 'thr-1').reason, 'stalled');
  assert.equal(adapter.calls.length, 1);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'turn-stalled').length, 1);
  await waker.continueOnce();
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'turn-stalled').length, 1, 'one notice');

  // A new next step (progress) is rung again.
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't' });
  await sleep(5);
  await client.turn({ thread: 'thr-1', state: 'closed', next: 'release y' });
  clock.t = Date.now() + 2 * 60_000;
  await waker.continueOnce();
  assert.equal(adapter.calls.length, 2);
});
