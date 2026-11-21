// M6: wake module (P-043). Limit 6/h per pair, stop on repeat, fixed template,
// adapter t3 called with the holder id, mail delivers without wake.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeInstance, startInstance, makeClient, addKey, cleanup, sleep, tmpDir } from './helpers.mjs';
import { Waker, WAKE_TEMPLATE, KEEPALIVE_TEMPLATE, CONTINUE_TEMPLATE, NO_REPLY_TEMPLATE, UNDELIVERED_TEMPLATE, RECIPIENT_CLOSED_TEMPLATE, WAKE_TYPES, wakeText, sentText, dueText, pairKey, pairLimit, resolveWakeConfig, readSince } from '../src/wake/index.mjs';
import { acquireWakeLock, readWakeLock, lockHeld, lockFile } from '../src/wake/lock.mjs';
import { parseTimestamp, parseDuration, TURN_BUSY_MAX_MS } from '../src/common/util.mjs';
import { createT3Adapter, parseCommand, platformCommand } from '../src/adapters/t3/index.mjs';

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

/**
 * Fake T3 with thread states: `threads[id]` = { run, archived } (run: the active run id, none = not running) or 'gone'
 * (T3: no longer available). A ring into an archived or gone thread fails as T3 answers it.
 */
function t3Adapter() {
  const a = { name: 'fake', command: ['fake'], calls: [], statusCalls: [], threads: {} };
  a.ring = async (holder, text) => {
    a.calls.push({ holder, text });
    const s = a.threads[holder];
    if (s === 'gone') return { ok: false, stderr: `Thread ${holder} was not found.` };
    if (s && s.archived) return { ok: false, stderr: `Thread ${holder} is archived and cannot receive messages.` };
    return { ok: true };
  };
  a.status = async (thread) => {
    a.statusCalls.push(thread);
    const s = a.threads[thread];
    if (s === 'gone') return { ok: false, running: false, status: null, error: `Thread ${thread} is no longer available.` };
    const x = s || {};
    return { ok: true, running: !!x.run, status: x.run ? 'running' : 'completed', archived: !!x.archived, run: x.run || null };
  };
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

// --- mail model 2 (design v3 part 1e): the sender's wait is a state of the message, read by dueOnce -------------------
/** Write fields of a message on the service (a wait made due, a deadline passed) without sleeping. */
function patchMsg(svc, id, fields) {
  const m = svc.store.message(id);
  Object.assign(m, fields);
  svc.store.saveMessage(m);
  return m;
}
/** The wait of `id` is due at the fake clock minus `agoMs` (the clock is never ahead of real time, so GET due sees it). */
function makeDue({ svc, clock }, id, agoMs = 0) {
  return patchMsg(svc, id, { wait_due_at: new Date(clock.t - agoMs).toISOString() });
}
/** The pickup deadline of `id` passed at the fake clock: checkExpired writes expired_at and the due wait (or a code answer). */
function expire({ svc, clock }, id) {
  patchMsg(svc, id, { expires_at: new Date(clock.t - 1000).toISOString() });
  return svc.checkExpired(clock.t);
}
/** The service's clock is the fake clock (hours pass without sleeping): due and the wait-rang confirmation at clock.t. */
function dueByClock({ svc, waker, clock }, sender = 'alice') {
  waker.client.due = async () => svc.due(sender, clock.t);
  waker.client.waitRang = async (id) => svc.waitRang(sender, { id, rang_at: new Date(clock.t).toISOString() });
}

test('blocker rings the topic holder with the fixed template, never the body', async (t) => {
  const { client, waker, adapter } = await setup(t);
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'SECRET BODY: prevezmi tému X', thread_id: 'thr-sender' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `you have mail from alice in topic p/t` }]);
  assert.ok(!adapter.calls[0].text.includes('SECRET'));
  assert.equal(wakeText({ project: 'p', topic: 't' }), 'you have mail in topic p/t');
  assert.equal(wakeText({ holder: 'thr-9' }), 'you have mail in topic thread/thr-9');
  // the sender after the recipient (operator 2026-11-17): its topic, else its key, else its thread; never the body
  assert.equal(wakeText({ project: 'p', topic: 't', from_project: 'q', from_topic: 'u', sender: 'bob' }), 'you have mail from q/u in topic p/t');
  assert.equal(wakeText({ holder: 'thr-9', sender: 'agent bob' }), 'you have mail from agent-bob in topic thread/thr-9');
  assert.equal(wakeText({ project: 'p', topic: 't', thread_id: 'thr-x' }), 'you have mail from thread/thr-x in topic p/t');
  // the link to the sender's thread (operator 2026-11-20): only a thread of this machine, on a line of its own (cmd.exe: a space)
  const linked = { project: 'p', topic: 't', from_project: 'q', from_topic: 'u', thread_id: 'mcp:launch:a1', from_machine: 'M1' };
  assert.equal(sentText(linked, 'm1', 'linux'), 'you have mail from q/u in topic p/t\n[q/u](t3-thread://v1/mcp:launch:a1)');
  assert.equal(sentText(linked, 'm1', 'win32'), 'you have mail from q/u in topic p/t [q/u](t3-thread://v1/mcp:launch:a1)');
  for (const ev of [{ ...linked, from_machine: 'm2' }, { ...linked, from_machine: null }, { ...linked, thread_id: 'a b)' }, { ...linked, thread_id: null }]) assert.equal(sentText(ev, 'm1', 'linux'), wakeText(ev));
  assert.equal(sentText(linked, undefined, 'linux'), wakeText(linked));
  // a reply carries no from_machine: the machine of the sender's thread decides, also against from_machine
  assert.match(sentText({ ...linked, from_machine: null, thread_machine: 'm1' }, 'm1', 'linux'), /\n\[q\/u\]\(t3-thread:\/\/v1\/mcp:launch:a1\)$/);
  assert.equal(sentText({ ...linked, thread_machine: 'm2' }, 'm1', 'linux'), wakeText(linked));
  // Cursor persisted; no replay on the next poll.
  assert.equal(await waker.pollOnce(0), 0);
  assert.equal(JSON.parse(fs.readFileSync(waker.state.file, 'utf8')).cursor, 1);
});

// bob 2026-11-02: assist and steer wake the holder too, not only blocker.
test('assist and steer wake the holder; a reply to any level wakes the sender thread', async (t) => {
  const { inst, client, waker, adapter } = await setup(t);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  await client.registerTopic({ project: 'q', topic: 'u', subject: 'S2', holder: 'thr-q' });
  const a = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'help?', thread_id: 'thr-a', from_project: 'q', from_topic: 'u' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: 'you have mail from q/u in topic p/t' }]);
  adapter.calls.length = 0;

  // Model 2: the recipient picks the message up and answers with resolve (send with reply_to is 400).
  await assert.rejects(bob.send({ reply_to: a.id, level: 'assist', text: 'answer' }), (e) => e.status === 400);
  await bob.receive({ project: 'p', topic: 't' });
  await bob.resolve({ project: 'p', topic: 't', id: a.id, state: 'hotovo', text: 'answer' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-a', text: 'you have mail from p/t in topic q/u' }]);
  const s = await bob.send({ project: 'p', topic: 't', level: 'steer', text: 'stop that', thread_id: 'thr-a' });
  assert.equal(s.level, 'steer');
  adapter.calls.length = 0;
  const st = await waker.pollOnce(500);
  assert.equal(st, 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: 'you have mail from bob in topic p/t' }]);
  adapter.calls.length = 0;

  // Blocker from a main thread without a topic: the reply names the thread.
  const b = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'urgent', thread_id: 'thr-main' });
  assert.equal(await waker.pollOnce(500), 1);
  await bob.receive({ project: 'p', topic: 't', ids: b.id });
  await bob.resolve({ project: 'p', topic: 't', id: b.id, state: 'hotovo', text: 'done' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [
    { holder: 'thr-1', text: 'you have mail from alice in topic p/t' },
    { holder: 'thr-main', text: 'you have mail from p/t in topic thread/thr-main' },
  ]);
});

test('limit 6 per hour per pair of topics: above it a notice, no ring; resumes after an hour', async (t) => {
  // Only the limit: the reminder of the messages still pending after an hour is tested in wake-remind.core.test.mjs.
  const { inst, client, waker, adapter, clock } = await setup(t, { remindAfterMs: 1e12 });
  for (let n = 1; n <= 8; n++) {
    await client.send({ project: 'p', topic: 't', level: 'assist', text: `message ${n}`, from_project: 'q', from_topic: 'u' });
  }
  assert.equal(await waker.pollOnce(500), 8);
  assert.equal(adapter.calls.length, 6);
  assert.equal(waker.stats.limited, 2);
  const notices = (await client.notices()).notices.filter((n) => n.kind === 'wake-limit');
  assert.equal(notices.length, 1, 'one open notice per pair (deduplicated)');
  assert.match(notices[0].title, /Limit budenia/);
  // The inbox is read without a pickup (count): receive would pick the messages up.
  const inbox = await client.pendingMail({ project: 'p', topic: 't' });
  assert.equal(notices[0].message_id, inbox.ids[6]);
  assert.deepEqual(notices[0].options.map((o) => o.id), ['ack']);
  // Messages above the limit are still in the mailbox.
  assert.equal(inbox.total, 8);

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

test('a message above the hourly limit is held and rung once the window has room; a picked one is not rung', async (t) => {
  const { client, waker, adapter, clock } = await setup(t);
  for (let n = 1; n <= 7; n++) {
    await client.send({ project: 'p', topic: 't', level: 'assist', text: `message ${n}`, from_project: 'q', from_topic: 'u' });
    clock.t += 3 * 60 * 1000;   // past the idle dedupe: each one is a ring of its own
  }
  assert.equal(await waker.pollOnce(500), 7);
  assert.equal(adapter.calls.length, 6);
  const held = Object.values(waker.state.held);
  assert.equal(held.length, 1);
  assert.equal(held[0].held_reason, 'limit');
  const notice = (await client.notices()).notices.find((n) => n.kind === 'wake-limit');
  assert.match(notice.detail, /zazvoní, keď sa okno uvoľní/);
  // Still a full window: nothing.
  await waker.releaseHeld();
  assert.equal(adapter.calls.length, 6);
  // No new event and no new turn: the window frees and the held message rings by itself.
  clock.t += 61 * 60 * 1000;
  assert.equal(await waker.releaseHeld(), 1);
  assert.equal(adapter.calls.length, 7);
  assert.deepEqual(adapter.calls[6], { holder: 'thr-1', text: 'you have mail from q/u in topic p/t' });
  assert.equal(Object.keys(waker.state.held).length, 0);
});

test('a steer and a blocker get twice the hourly pair limit; they count in the same window', async (t) => {
  const { client, waker, adapter } = await setup(t);
  for (let n = 1; n <= 6; n++) await client.send({ project: 'p', topic: 't', level: 'assist', text: `assist ${n}`, from_project: 'q', from_topic: 'u' });
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'assist 7', from_project: 'q', from_topic: 'u' });
  await client.send({ project: 'p', topic: 't', level: 'steer', text: 'zmena smeru', from_project: 'q', from_topic: 'u' });
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 7, 'the 7th assist waits, the steer rings');
  assert.equal(waker.stats.limited, 1);
  for (let n = 1; n <= 6; n++) await client.send({ project: 'p', topic: 't', level: 'blocker', text: `blocker ${n}`, from_project: 'q', from_topic: 'u' });
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 12, 'urgent rings stop at twice the limit');
  assert.equal(waker.stats.limited, 2);
});

test('a reply to a blocker gets the urgent limit; the release time is when enough rings left the window (2026-11-18)', async (t) => {
  assert.equal(pairLimit({ type: 'reply', original_level: 'blocker' }, 6), 12);
  assert.equal(pairLimit({ type: 'reply', original_level: 'steer' }, 6), 12);
  assert.equal(pairLimit({ type: 'reply', original_level: 'assist' }, 6), 6);
  const { client, waker, clock } = await setup(t);
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'first', from_project: 'q', from_topic: 'u' });
  await waker.pollOnce(500);
  const [pair] = Object.keys(waker.state.rings);
  // 8 rings of the pair in the last hour, one a minute from 50 min ago (the incident: 8 over a limit of 6).
  const start = clock.t - 50 * 60 * 1000;
  waker.state.rings[pair] = Array.from({ length: 8 }, (_, i) => start + i * 60 * 1000);
  clock.t += 3 * 60 * 1000;
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'second', from_project: 'q', from_topic: 'u' });
  await waker.pollOnce(500);
  const notice = (await client.notices()).notices.find((n) => n.kind === 'wake-limit');
  assert.match(notice.title, /6 za hodinu/);
  // Three of the 8 rings must leave for 5 < 6: the third oldest + 1 h, not the oldest + 1 h.
  assert.ok(notice.detail.includes(new Date(start + 2 * 60 * 1000 + 60 * 60 * 1000).toISOString()), notice.detail);
});

test('a held limited message the thread picked up meanwhile is not rung', async (t) => {
  const { inst, client, waker, adapter, clock } = await setup(t);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  for (let n = 1; n <= 7; n++) {
    await client.send({ project: 'p', topic: 't', level: 'assist', text: `message ${n}`, from_project: 'q', from_topic: 'u' });
    clock.t += 3 * 60 * 1000;
  }
  await waker.pollOnce(500);
  assert.equal(adapter.calls.length, 6);
  await bob.receive({ project: 'p', topic: 't' });
  clock.t += 61 * 60 * 1000;
  assert.equal(await waker.releaseHeld(), 0);
  assert.equal(adapter.calls.length, 6);
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

// bob 2026-11-02: operator mode still rings the holder, besides creating the notice.
test('holder mode operator: blocker rings the holder and creates the notice, reply wakes the sender', async (t) => {
  const { inst, client, waker, adapter } = await setup(t, { holderMode: 'operator' });
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  const b = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'need you', thread_id: 'thr-a' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: 'you have mail from alice in topic p/t' }]);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'blocker').length, 1);
  await bob.receive({ project: 'p', topic: 't' });
  await bob.resolve({ project: 'p', topic: 't', id: b.id, state: 'hotovo', text: 'here' });
  await waker.pollOnce(500);
  assert.deepEqual(adapter.calls.slice(1), [{ holder: 'thr-a', text: 'you have mail from p/t in topic thread/thr-a' }]);
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

// Windows fixes reported from another machine (comms-public #1, #2): the T3 CLI is a .cmd outside PATH, and a restart of
// the scheduled task left a second wake running.
test('platformCommand: argv unchanged off Windows; on win32 t3 resolves to t3.cmd and runs through cmd.exe, quoted', () => {
  const argv = ['t3', 'thread', 'send', 'th-1', 'you have mail in topic app/fix'];
  assert.deepEqual(platformCommand(argv, { platform: 'linux' }), { file: 't3', args: argv.slice(1), verbatim: false });
  assert.deepEqual(platformCommand(argv, { platform: 'darwin' }).file, 't3');
  const cli = 'C:\\Users\\u x\\.t3\\bin\\t3.cmd';
  const env = { USERPROFILE: 'C:\\Users\\u x', ComSpec: 'C:\\Windows\\system32\\cmd.exe' };
  const w = platformCommand(argv, { platform: 'win32', env, exists: (f) => f === cli });
  assert.equal(w.file, 'C:\\Windows\\system32\\cmd.exe');
  assert.equal(w.verbatim, true);
  assert.deepEqual(w.args, ['/d', '/s', '/c', `""${cli}" "thread" "send" "th-1" "you have mail in topic app/fix""`]);
  const viaEnv = platformCommand(argv, { platform: 'win32', env: { ...env, T3CODE_CLI_PATH: 'D:\\t3\\t3.cmd' }, exists: () => true });
  assert.match(viaEnv.args[3], /^""D:\\t3\\t3\.cmd" /, 'T3CODE_CLI_PATH first');
  assert.equal(platformCommand(['C:\\t3\\t3.exe', 'x'], { platform: 'win32', env, exists: () => true }).file, 'C:\\t3\\t3.exe', 'an .exe runs directly');
  assert.equal(platformCommand(argv, { platform: 'win32', env, exists: () => false }).file, 't3', 'no CLI found: unchanged (the ring fails and is logged)');
  assert.match(platformCommand([cli, 'send', '100%'], { platform: 'win32', env }).error, /cmd\.exe rozvinie/);
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

test('resolveWakeConfig: wake.json from the data dir; env and --config take precedence', () => {
  const inst = makeInstance('wakejson');
  fs.writeFileSync(path.join(inst.dir, 'wake.json'), JSON.stringify({ limit_per_hour: 3, keepalive: { since_command: 'echo 0' }, self_restart: false }));
  const c = resolveWakeConfig({ dataDir: inst.dir }, {});
  assert.equal(c.limitPerHour, 3);
  assert.equal(c.keepaliveSinceCommand, 'echo 0');
  assert.equal(c.selfRestart, false);
  assert.equal(c.wakeFile, path.join(inst.dir, 'wake.json'));
  assert.equal(resolveWakeConfig({ dataDir: inst.dir }, { WAKE_LIMIT_PER_HOUR: '5' }).limitPerHour, 5);
  assert.equal(resolveWakeConfig({ dataDir: inst.dir, limitPerHour: '7' }, {}).limitPerHour, 7);
  const cfgFile = path.join(inst.dir, 'cfg.json');
  fs.writeFileSync(cfgFile, JSON.stringify({ wake: { limit_per_hour: 9 } }));
  const d = resolveWakeConfig({ dataDir: inst.dir, config: cfgFile }, {});
  assert.equal(d.limitPerHour, 9);
  assert.equal(d.keepaliveSinceCommand, null);
  assert.equal(d.wakeFile, null);
  cleanup(inst.dir);
});

test('bin/wake.mjs run: rings through the command, survives a mail restart, stops on SIGTERM', async (t) => {
  const inst = makeInstance('wakeproc');
  const dir = tmpDir('comms-wakeproc-');
  const logFile = path.join(dir, 'calls.log');
  let srv = await startInstance(inst);
  // The status probe goes to the fake T3 too (a thread without a turn record is asked whether it runs): never the real t3.
  const spawnWake = (extraEnv = {}) => spawn(process.execPath, [WAKE_BIN, 'run', '--data-dir', inst.dir, '--no-start', '--command', `${process.execPath} ${FAKE_T3}`,
    '--status-command', `${process.execPath} ${FAKE_T3} status`, '--wait-ms', '300', '--reconnect-ms', '100', '--state-dir', dir], {
    env: { PATH: process.env.PATH, HOME: process.env.HOME, FAKE_T3_LOG: logFile, FAKE_T3_STDOUT: JSON.stringify({ thread: { status: 'completed', activeRunId: null } }), COMMS_SELF_RESTART: '0', ...extraEnv }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  const child = spawnWake();
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
  // Rings only; status probes start with `status`.
  const lines = () => (fs.existsSync(logFile) ? fs.readFileSync(logFile, 'utf8').trim().split('\n').filter(Boolean).filter((l) => JSON.parse(l)[0] !== 'status') : []);
  await waitFor(() => lines().length >= 1);
  assert.deepEqual(JSON.parse(lines()[0]), ['thr-1', 'you have mail from alice in topic p/t']);
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

  // A second run of the same instance waits for the first (comms-public#1: a restarted scheduled task) and takes over
  // when it ends.
  const second = spawnWake({ WAKE_LOCK_POLL_MS: '100' });
  let err2 = '';
  second.stderr.on('data', (d) => { err2 += d; });
  const exited2 = new Promise((r) => second.on('exit', (code) => r(code)));
  t.after(async () => { try { second.kill('SIGKILL'); } catch { /* gone */ } await exited2; });
  await waitFor(() => err2.includes(`už beží (pid ${child.pid})`));

  child.kill('SIGTERM');
  const r = await exited;
  assert.equal(r.code, 0);
  assert.match(out, /"stopped":true/);
  assert.match(out, /"rang":2/);
  await waitFor(() => err2.includes('zámok budenia prevzatý'));
  second.kill('SIGTERM');
  assert.equal(await exited2, 0);
});

test('wake lock: a second run waits while the first holds a fresh lock; a dead holder is replaced; release frees it', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-lock-'));
  try {
    const a = await acquireWakeLock(dir, { heartbeatMs: 50 });
    assert.equal(readWakeLock(dir).pid, process.pid);
    a.release();
    assert.equal(readWakeLock(dir), null);
    // Another live process (the parent) holds it: a second run waits and logs once.
    fs.writeFileSync(lockFile(dir), JSON.stringify({ pid: process.ppid, heartbeat_at: new Date().toISOString() }));
    assert.equal(lockHeld(readWakeLock(dir)), true);
    const logs = [];
    const ac = new AbortController();
    const waiting = acquireWakeLock(dir, { log: (m) => logs.push(m), signal: ac.signal, pollMs: 20 });
    await new Promise((r) => setTimeout(r, 80));
    assert.match(logs.join('\n'), new RegExp(`už beží \\(pid ${process.ppid}\\)`));
    ac.abort();
    assert.equal(await waiting, null, 'aborted while waiting');
    // A stale heartbeat (holder hung or pid reused) does not block.
    fs.writeFileSync(lockFile(dir), JSON.stringify({ pid: process.ppid, heartbeat_at: '2020-01-01T00:00:00Z' }));
    const b = await acquireWakeLock(dir, { pollMs: 20 });
    assert.equal(readWakeLock(dir).pid, process.pid);
    b.release();
    assert.equal(readWakeLock(dir), null);
  } finally { fs.rmSync(dir, { recursive: true, force: true }); }
});

// --- model 2 (1e): the sender's wait after the deadline (`no reply <id>`) and keepalive -----------------------------

test('due wait after the deadline: rings the sender once with `no reply <id>`, never the recipient; wait-rang confirms (wait_n 1, next check); answered → not due', async (t) => {
  const ctx = await setup(t);
  const { inst, client, waker, adapter, svc, clock } = ctx;
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  await client.registerTopic({ project: 'q', topic: 'u', subject: 'Src', holder: 'thr-q' });
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'SECRET?', ttl: '60s', thread_id: 'thr-a', from_project: 'q', from_topic: 'u' });
  const m2 = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'main?', ttl: '60s', from_project: 'q', from_topic: 'u' });
  const m3 = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'later deadline', ttl: '30m', thread_id: 'thr-a' });
  assert.equal(await waker.pollOnce(500), 3, 'each assist rings the recipient once');
  adapter.calls.length = 0;
  assert.deepEqual(await waker.dueOnce(), [], 'nothing due before the deadline');
  // The service check runs every 30 s; here directly at the fake clock past the deadline of m and m2.
  expire(ctx, m.id);
  expire(ctx, m2.id);
  assert.ok((await client.message(m.id)).expired_at);
  const out = await waker.dueOnce();
  assert.deepEqual(out.map((x) => x.action), ['rang', 'rang']);
  assert.deepEqual(adapter.calls.sort((a, b) => a.holder.localeCompare(b.holder)), [
    { holder: 'thr-a', text: `no reply ${m.id}` },
    { holder: 'thr-q', text: `no reply ${m2.id}` },
  ]);
  assert.equal(NO_REPLY_TEMPLATE, 'no reply');
  assert.equal(dueText({ id: 'x1', wait_reason: 'no-reply' }), 'no reply x1');
  assert.ok(!adapter.calls.some((c) => c.holder === 'thr-1'), 'the expiry does not wake the recipient (holder of p/t)');
  assert.ok(!adapter.calls.some((c) => c.text.includes('SECRET')));
  for (const id of [m.id, m2.id]) {
    const st = await client.message(id);
    assert.equal(st.wait_n, 1, 'wait-rang confirmed');
    assert.equal(Date.parse(st.wait_due_at), Date.parse(st.wait_rang_at) + 3600_000 * 2, 'next check rang_at (service clock) + base (assist 1 h) × 2');
    assert.equal(st.status, 'pending', 'the message itself stays');
  }
  // Second cycle: nothing due. A real late answer (receive + resolve) wakes the sender with the usual template.
  assert.deepEqual(await waker.dueOnce(), []);
  assert.equal(adapter.calls.length, 2);
  await bob.receive({ project: 'p', topic: 't' });
  await bob.resolve({ project: 'p', topic: 't', id: m.id, state: 'hotovo', text: 'late' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls[2], { holder: 'thr-a', text: `you have mail from p/t in topic q/u` });
  // Answered or acked: no wait any more, never due; the unanswered m3 still is.
  await bob.resolve({ project: 'p', topic: 't', id: m2.id, state: 'hotovo', ack: true });
  const farDue = svc.due('alice', Date.now() + 365 * 86400_000).due.map((x) => x.id);
  assert.ok(!farDue.includes(m.id) && !farDue.includes(m2.id), 'answered/acked are not due');
  assert.ok(farDue.includes(m3.id));
  assert.equal((await client.message(m.id)).wait_due_at, null);
  // A picked message (no answer yet) is not due either before its check.
  assert.equal((await client.message(m3.id)).status, 'picked');
  assert.ok(!(await waker.dueOnce()).length);
  // Expired events are gone: an `expired`/`undelivered` event is not a ring type any more.
  assert.deepEqual(WAKE_TYPES, ['blocker', 'steer', 'assist', 'reply']);
  assert.equal((await waker.handle({ seq: 99, type: 'expired', message_id: m3.id, holder: 'thr-a' })).reason, 'unknown-type');
});

// Audit M5 2026-11-11.
test('held message (unregistered topic) past its deadline: the sender is rung with `no reply <id>` via due', async (t) => {
  const ctx = await setup(t);
  const { client, waker, adapter } = ctx;
  const m = await client.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'lost?', ttl: '60s', thread_id: 'thr-a' });
  assert.equal(m.status, 'held');
  assert.deepEqual(expire(ctx, m.id), [m.id]);
  assert.equal(await waker.pollOnce(100), 0, 'no event: the wait is read from due');
  assert.deepEqual(adapter.calls.filter((c) => c.holder === 'thr-a'), [{ holder: 'thr-a', text: `no reply ${m.id}` }]);
  assert.equal((await client.message(m.id)).wait_n, 1);
});

test('one queued ring per open turn of a busy holder; a new turn may be rung again; state persisted and pruned', async (t) => {
  const { client, waker, adapter, clock } = await setup(t);
  const o1 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'two', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal(adapter.calls.length, 1, 'the second ring would queue a second turn');
  assert.equal(waker.stats.skipped, 1);
  assert.deepEqual(JSON.parse(fs.readFileSync(waker.state.file, 'utf8')).queued, { 'thr-1': o1.turn.opened_at });
  assert.equal(Object.keys(waker.state.rings).length, 1, 'a queued skip does not count against the limit');

  // The run ended and the queued ring started a new run: the next event rings again.
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o1.turn.opened_at });
  await sleep(5);
  const o2 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  // The queued ring's turn reads both messages: the held second one then rings no more.
  await client.receive({ project: 'p', topic: 't' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'three', thread_id: 'thr-s3' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 2);
  assert.deepEqual(waker.state.queued, { 'thr-1': o2.turn.opened_at });
  // Closed run whose queued ring has not started for longer than queuedStartMs: the ring was lost, it rings; mark pruned.
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o2.turn.opened_at });
  clock.t = Date.now() + waker.opts.queuedStartMs + 1000;
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'four', thread_id: 'thr-s4' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 3);
  assert.deepEqual(waker.state.queued, {});
});

// doma-pc 2026-11-18 (ttl: 2027-05-18): the ring queued behind the run was spent by that run (steered into it); the next
// message was skipped as "queued" and dropped, read only at the operator's next prompt. It is held and rings once the
// queued mark is gone, only while it still waits.
test('a message skipped behind a queued ring is held: it rings once the mark is gone and it still waits, never twice', async (t) => {
  const { client, waker, adapter, clock } = await setup(t);
  const o1 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  const m1 = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'two', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal(adapter.calls.length, 1);
  assert.equal(Object.values(waker.state.held)[0]?.held_reason, 'queued', 'held, not dropped');
  // The run reads "one" and ends; no new run comes (the queued ring was steered into it).
  await client.receive({ project: 'p', topic: 't', id: m1.id });
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o1.turn.opened_at });
  await waker.pollOnce(50);
  assert.equal(adapter.calls.length, 1, 'within queuedStartMs the queued ring may still start');
  clock.t = Date.now() + waker.opts.queuedStartMs + 1000;
  await waker.pollOnce(50);
  assert.equal(adapter.calls.length, 2, 'the held message rings');
  assert.deepEqual(waker.state.held, {});
  await waker.pollOnce(50);
  assert.equal(adapter.calls.length, 2, 'once');
});

// wake-stale-ring 2026-11-13: a ring queued behind a long run; the operator's prompts steered into that run opened new turn
// records, the mark was dropped and a second bell queued; at the run's close a third bell went out before the queued turn
// opened. Both later bells hit mail the first queued turn had picked up ("wake: no new mail", "blocked by hook" in T3).
test('a ring queued behind a run: no second bell after a prompt steered into the run, nor right after the run closed', async (t) => {
  const { client, waker, adapter } = await setup(t);
  const o1 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 1, 'rung into the busy thread, queued');
  // The operator types into the running turn: UserPromptSubmit opens a new record without a Stop, same run.
  await sleep(5);
  const steer = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.notEqual(steer.turn.opened_at, o1.turn.opened_at);
  assert.equal(steer.turn.run_at, o1.turn.opened_at, 'a steered prompt keeps the run');
  await client.send({ project: 'p', topic: 't', level: 'steer', text: 'two', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 1, 'the queued ring has not started yet: no second bell');
  // The run ends; the queued ring starts in T3 but its UserPromptSubmit has not opened the record yet.
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: steer.turn.opened_at });
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'three', thread_id: 'thr-s3' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 1, 'the queued ring is starting: no third bell');
  // The queued ring's turn opens (a new run): later mail rings again.
  await sleep(5);
  const o2 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.equal(o2.turn.run_at, o2.turn.opened_at, 'a prompt after a Stop starts a new run');
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'four', thread_id: 'thr-s4' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 2);
});

// Fable audit M1.
test('idle holder: the same bell again before its turn opens is skipped; after the turn opened it rings', async (t) => {
  const { client, waker, adapter } = await setup(t, { idleDedupeMs: 120_000 });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  // another sender (the bell names it, operator 2026-11-17): still the same bell to the same recipient
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'two', thread_id: 'thr-s2', from_project: 'q', from_topic: 'u' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal(adapter.calls.length, 1, 'the turn of the first ring sees both messages');
  assert.equal(Object.values(waker.state.held)[0]?.held_reason, 'queued', 'held, not dropped (doma-pc 2026-11-18)');
  // The rung turn opened and closed: a later message rings again.
  await sleep(5);
  const o = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.receive({ project: 'p', topic: 't' });
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
  t3.restart = async () => { await sleep(5); t3.since = Date.now(); };
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
  await t3.restart(); // T3 restarted after these turns opened

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
  await t3.restart();
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
  await t3.restart();
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
  await t3.restart();
  clock.t += 10 * 60_000;
  assert.deepEqual([(await step()).reason, adapter.calls.length], ['limit', 3]);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'turn-interrupted').length, 1, 'deduplicated per thread');
  // After an hour the window is free again.
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await t3.restart();
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
  assert.deepEqual(await mk(JSON.stringify({ thread: { status: 'completed', activeRunId: null } })).status('thr-1'), { ok: true, running: false, status: 'completed', archived: false, run: null });
  assert.deepEqual(await mk(JSON.stringify({ thread: { status: 'running', activeRunId: 'run-7' } })).status('thr-1'), { ok: true, running: true, status: 'running', archived: false, run: 'run-7' });
  assert.deepEqual(await mk(JSON.stringify({ thread: { status: 'completed', activeRunId: null, archived: true } })).status('thr-1'), { ok: true, running: false, status: 'completed', archived: true, run: null });
  assert.equal((await mk('not json').status('thr-1')).ok, false);
  assert.equal((await mk('{}', '4').status('thr-1')).ok, false);
  assert.deepEqual(JSON.parse(fs.readFileSync(logFile, 'utf8').trim().split('\n')[0]), ['thread', 'show', '--json', 'thr-1']);
});

test('run(): the keepalive loop runs next to the long-poll and stops with the signal', async (t) => {
  const { client, waker, adapter, t3 } = await kaSetup(t, { keepaliveIntervalMs: 50, waitMs: 100 });
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await t3.restart();
  const ac = new AbortController();
  const running = waker.run({ signal: ac.signal });
  await waitFor(() => adapter.calls.length >= 1);
  ac.abort();
  const stats = await running;
  assert.equal(stats.keepalive_rang, 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: KEEPALIVE_TEMPLATE }]);
});

test('ring to an archived thread bounces: the sender gets `undelivered <id>` via due once, no mail; a picked message is not bounced', async (t) => {
  const adapter = { name: 'fake', command: ['fake'], calls: [], ring: async (holder, text) => { adapter.calls.push({ holder, text }); return holder === 'thr-1' || holder === 'thr-dead' ? { ok: false, stderr: `Thread ${holder} is archived and cannot receive messages.` } : { ok: true }; } };
  const ctx = await setup(t, { adapter });
  const { inst, client, waker, clock } = ctx;
  const m = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'restart?', thread_id: 'thr-sender' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `you have mail from alice in topic p/t` }]);
  const st = await client.message(m.id);
  assert.deepEqual([st.status, st.undelivered_holder, st.wait_reason], ['undelivered', 'thr-1', 'undelivered']);
  assert.match(st.undelivered_reason, /archived/);
  // The bounce made the sender's wait due now (real time): the next cycle rings the sender's thread with the exact text.
  clock.t = Date.now() + 1000;
  assert.equal(await waker.pollOnce(100), 0, 'no event: the wait comes from due');
  assert.deepEqual(adapter.calls.slice(1), [{ holder: 'thr-sender', text: `undelivered ${m.id}` }]);
  assert.equal(UNDELIVERED_TEMPLATE, 'undelivered');
  assert.equal(dueText({ id: 'x1', wait_reason: 'undelivered' }), 'undelivered x1');
  assert.equal((await client.message(m.id)).wait_due_at, null, 'one-shot wait: confirmed and ended');
  assert.deepEqual(await waker.dueOnce(), [], 'once per message');
  assert.equal(((await client.receive({ thread: 'thr-sender' })).messages || []).length, 0, 'no mail from comms');
  assert.equal((await client.bounce({ message_id: m.id, holder: 'thr-1', reason: 'archived' })).bounced, false, 'a final message is not bounced again');
  assert.equal((await client.notices()).notices.filter((n) => n.kind !== 'blocker').length, 0, 'no operator notice');

  // A picked message stays picked (I3): its ring fails for good, the bounce changes nothing.
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  const picked = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'read', thread_id: 'thr-s2' });
  const waiting = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'unread', thread_id: 'thr-s3' });
  await bob.receive({ project: 'p', topic: 't', ids: picked.id });
  adapter.calls.length = 0;
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal((await client.message(picked.id)).status, 'picked', 'a picked message is not bounced');
  assert.equal((await client.message(waiting.id)).status, 'undelivered');

  // A due ring that fails for good (the sender's thread archived) is confirmed, never bounced: no later turn will come.
  const m2 = await client.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'i', thread_id: 'thr-dead' });
  clock.t = Date.now();
  makeDue(ctx, m2.id);
  adapter.calls.length = 0;
  const r = (await waker.dueOnce()).filter((x) => x.id === m2.id);
  assert.deepEqual(r.map((x) => [x.action, x.reason]), [['failed', 'permanent']]);
  assert.deepEqual(adapter.calls.filter((c) => c.holder === 'thr-dead'), [{ holder: 'thr-dead', text: `no reply ${m2.id}` }]);
  const st2 = await client.message(m2.id);
  assert.deepEqual([st2.status, st2.wait_n], ['held', 1], 'confirmed, not bounced');
});

test('transient ring failure does not bounce', async (t) => {
  const { client, waker } = await setup(t, { adapter: fakeAdapter(false) });
  const m = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'x', thread_id: 'thr-sender' });
  assert.equal(await waker.pollOnce(500), 1);
  const st = await client.message(m.id);
  assert.deepEqual([st.status, st.wait_reason, st.undelivered_at], ['pending', 'no-reply', null]);
});

test('turn records accept T3 thread ids (mcp:<uuid>) and keepalive rings them', async (t) => {
  const { client, waker, adapter, t3 } = await kaSetup(t);
  const thread = 'mcp:3c22248f-b132-4e46-8546-f99b101b4298';
  const r = await client.turn({ thread, state: 'open', machine: 'm1' });
  assert.equal(r.ok, true);
  assert.equal((await client.turns(true)).turns.map((x) => x.thread).join(), thread);
  await t3.restart();
  const out = await waker.keepaliveOnce();
  assert.deepEqual(out.map((x) => x.action), ['rang']);
  assert.equal(adapter.calls[0].holder, thread);
  await assert.rejects(client.turn({ thread: '../x', state: 'open' }), /thread/);
});

// The ok:true running/completed shapes and `not json` are asserted in the T3 start time / adapter status test.
test('t3 status: exit 0 JSON without a thread object is an unknown state (ok:false), never running:false', async () => {
  const status = (script) => createT3Adapter({ statusCommand: [process.execPath, '-e', script] }).status('thr-1');
  const noThread = await status('console.log(JSON.stringify({ id: "x" }))');
  assert.equal(noThread.ok, false);
  assert.notEqual(noThread.running, false);
  assert.equal((await status('console.log(JSON.stringify({ thread: "x" }))')).ok, false, 'thread must be an object');
});

test('keepalive: closing a turn closes only that turn; a newer open turn of the same thread stays open', async (t) => {
  const { client, waker } = await setup(t);
  const old = (await client.turn({ thread: 'thr-k', state: 'open', machine: 'm1' })).turn;
  await sleep(5);
  const fresh = (await client.turn({ thread: 'thr-k', state: 'open', machine: 'm1' })).turn;
  assert.ok(fresh.opened_at > old.opened_at);
  await waker.closeTurn(old);
  const open = (await client.turns(true)).turns.filter((x) => x.thread === 'thr-k');
  assert.deepEqual(open.map((x) => x.opened_at), [fresh.opened_at], 'the current turn is not closed by a close of the old one');
});

// bob 2026-11-02 scenario.
test('operator-mode topic: blocker rings the holder; assist to a main thread held by an archived thread bounces, the sender gets `undelivered <id>` in the next cycle', async (t) => {
  const adapter = fakeAdapter();
  adapter.ring = async (holder, text) => {
    adapter.calls.push({ holder, text });
    return holder === 'thr-archived' ? { ok: false, stderr: 'thread is archived' } : { ok: true };
  };
  const { client, waker, clock } = await setup(t, { holderMode: 'operator', adapter });
  // @alice/main is held by a thread that was archived with its topic.
  await client.registerTopic({ persona: 'alice', main: true, holder: 'thr-archived' });
  await client.registerTopic({ project: 'bob', topic: 'units', subject: 'U', holder: 'thr-bob' });
  const assist = await client.send({ persona: 'alice', level: 'assist', text: 'units?', thread_id: 'thr-bob', from_project: 'bob', from_topic: 'units' });
  // Blocker into p/t held in operator mode by thr-1 (the dev thread).
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'units!', thread_id: 'thr-bob', from_project: 'bob', from_topic: 'units' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal((await client.message(assist.id)).status, 'undelivered');
  clock.t = Date.now() + 1000;
  await waker.pollOnce(100);
  assert.deepEqual(adapter.calls.map((c) => [c.holder, c.text]), [
    ['thr-archived', 'you have mail from bob/units in topic @alice/main'],
    ['thr-1', 'you have mail from bob/units in topic p/t'],
    ['thr-bob', `${UNDELIVERED_TEMPLATE} ${assist.id}`],
  ]);
  assert.equal((await client.message(assist.id)).wait_due_at, null);
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

// P-061.
test('timer: a turn closed with resume_at is rung at that time, also with the same step again', async (t) => {
  const { client, waker, adapter, clock } = await kaSetup(t);
  const at = new Date(Date.now() + 10 * 60_000).toISOString();
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't' });
  await client.turn({ thread: 'thr-1', state: 'closed', next: 'check reply', resume_at: at });
  assert.equal((await client.turns(false)).turns.find((x) => x.thread === 'thr-1').resume_at, at);
  clock.t = Date.now() + 2 * 60_000;
  assert.equal((await waker.continueOnce()).find((x) => x.thread === 'thr-1').reason, 'delay', 'not before the timer');
  clock.t = Date.parse(at) + 1000;
  await waker.continueOnce();
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: CONTINUE_TEMPLATE }]);
  // Deferred again with the same step: rung again at the new time, no stalled notice.
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', project: 'p', topic: 't' });
  await sleep(5);
  const at2 = new Date(clock.t + 5 * 60_000).toISOString();
  await client.turn({ thread: 'thr-1', state: 'closed', next: 'check reply', resume_at: at2 });
  clock.t = Date.parse(at2) + 1000;
  await waker.continueOnce();
  assert.equal(adapter.calls.length, 2);
  assert.equal((await client.notices()).notices.filter((n) => n.kind === 'turn-stalled').length, 0);
  await assert.rejects(client.turn({ thread: 'thr-1', state: 'closed', next: 'x', resume_at: 'soon' }), /resume_at/);
});

test('code answer `otázka` (recipient waits for the operator): no ring at the deadline; the sender is rung with `no reply <id>` once, after 60 h (4 h × 15)', async (t) => {
  const ctx = await setup(t);
  const { inst, client, waker, adapter, svc, clock } = ctx;
  dueByClock(ctx);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key, thread: 'thr-1' });
  const o = await bob.turn({ thread: 'thr-1', state: 'open' });
  await bob.turn({ thread: 'thr-1', waiting_operator: true, opened_at: o.turn.opened_at });
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'decide?', ttl: '60s', thread_id: 'thr-a' });
  assert.equal(await waker.pollOnce(500), 1);
  adapter.calls.length = 0;
  assert.deepEqual(expire(ctx, m.id), [], 'a code answer, not an expiry');
  const st = await client.message(m.id);
  assert.deepEqual([st.code_answer, st.status, st.wait_reason, st.expired_at], ['otázka', 'pending', 'check', null]);
  // operator 2026-11-21: until the end of the span the sender has no step (the hook line says "do not resend"): one ring.
  assert.equal(Date.parse(st.wait_due_at), clock.t + 60 * 3600_000, 'one ring at the end of the span');
  clock.t += 59 * 3600_000;
  assert.deepEqual(await waker.dueOnce(), [], 'not due before the end of the span');
  clock.t += 3600_000;
  await waker.dueOnce();
  assert.deepEqual(adapter.calls, [{ holder: 'thr-a', text: `no reply ${m.id}` }], 'the hook reads the state `otázka` from `message`');
  const after = await client.message(m.id);
  assert.deepEqual([after.wait_n, after.wait_due_at], [4, null], 'the one ring ends the wait');
  assert.ok(!svc.due('alice', clock.t).due.length);
});

test('code answer `busy` (recipient runs a turn that holds the message): the sender is rung with `no reply <id>` once, after 60 h', async (t) => {
  const ctx = await setup(t);
  const { inst, client, waker, adapter, clock } = ctx;
  dueByClock(ctx);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key, thread: 'thr-1' });
  await bob.turn({ thread: 'thr-1', state: 'open', discussion: 'open' });
  const m = await client.send({ project: 'p', topic: 't', level: 'steer', text: 'adjust', ttl: '60s', thread_id: 'thr-a' });
  await waker.pollOnce(500);
  adapter.calls.length = 0;
  expire(ctx, m.id);
  const st = await client.message(m.id);
  assert.deepEqual([st.code_answer, st.status], ['busy', 'pending']);
  assert.equal(Date.parse(st.wait_due_at), clock.t + 60 * 3600_000);
  clock.t += 60 * 3600_000 - 1000;
  assert.deepEqual(await waker.dueOnce(), []);
  clock.t += 1000;
  await waker.dueOnce();
  assert.deepEqual(adapter.calls, [{ holder: 'thr-a', text: `no reply ${m.id}` }], 'the hook reads the state `busy` from `message`');
  assert.equal((await client.message(m.id)).wait_due_at, null, 'the one ring ends the wait');
});

// D-0015, D-0022: no steer/assist ring into an open discussion with the operator; a blocker passes; the held ring goes
// out when the discussion closes.
test('discussion with the operator: steer and assist are held, a blocker rings; the held mail rings when it closes', async (t) => {
  const { client, waker, adapter } = await setup(t);
  const o = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', discussion: 'open' });
  await client.send({ project: 'p', topic: 't', level: 'steer', text: 'stop', thread_id: 'thr-s' });
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'help?', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.deepEqual(adapter.calls, [], 'no ring into the discussion');
  assert.equal(Object.keys(JSON.parse(fs.readFileSync(waker.state.file, 'utf8')).held).length, 2, 'held, persisted');
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'urgent', thread_id: 'thr-s3' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `you have mail from alice in topic p/t` }], 'a blocker passes');
  adapter.calls.length = 0;
  // The turn ends with a question to the operator: the discussion stays open, nothing rings.
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o.turn.opened_at, discussion: 'open' });
  assert.equal(await waker.pollOnce(100), 0);
  assert.deepEqual(adapter.calls, []);
  // The operator answers; that turn ends with hotovo: the discussion closes and the held mail rings.
  const o2 = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', discussion: 'open' });
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o2.turn.opened_at, discussion: 'closed' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `you have mail from alice in topic p/t` }, { holder: 'thr-1', text: `you have mail from alice in topic p/t` }]);
  assert.deepEqual(waker.state.held, {});
});

// SIGTERM during a round over many threads (T3 status ~1.2 s each) stopped wake only after the whole round, ~17 s
// (2026-11-13): the round ends at the next thread once the signal aborts.
test('continue and keepalive rounds end at the next thread when the signal aborts', async (t) => {
  const { client, waker, adapter, clock, t3 } = await kaSetup(t);
  for (const th of ['thr-a', 'thr-b', 'thr-c']) {
    await client.turn({ thread: th, state: 'open', machine: 'm1', project: 'p', topic: 't' });
    await client.turn({ thread: th, state: 'closed', next: 'step' });
  }
  for (const th of ['thr-d', 'thr-e', 'thr-f']) await client.turn({ thread: th, state: 'open', machine: 'm1', project: 'p', topic: 't' });
  await t3.restart();
  clock.t = Date.now() + 2 * 60_000;
  let ac = new AbortController();
  adapter.status = async (thread) => { adapter.statusCalls.push(thread); ac.abort(); return { ok: true, running: false, status: 'completed' }; };
  assert.equal((await waker.continueOnce(ac.signal)).length, 1, 'continue: no thread after the abort');
  assert.equal(adapter.statusCalls.length, 1);
  ac = new AbortController();
  adapter.statusCalls.length = 0;
  await waker.keepaliveOnce(ac.signal);
  assert.deepEqual(adapter.statusCalls, ['thr-d'], 'keepalive: no thread after the abort');
});

test('held ring: dropped when picked up meanwhile; released by the next poll without a `discussion-closed` event', async (t) => {
  const { inst, client, waker, adapter } = await setup(t);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key, thread: 'thr-1' });
  const o = await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', discussion: 'open' });
  const picked = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'read me', thread_id: 'thr-s' });
  await client.send({ project: 'p', topic: 't', level: 'steer', text: 'later', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 2);
  await bob.receive({ project: 'p', topic: 't', ids: picked.id });
  await client.turn({ thread: 'thr-1', state: 'closed', opened_at: o.turn.opened_at, discussion: 'closed' });
  // the event is missed (older service): the poll after it still releases the held ring
  waker.state.cursor = (await client.events('latest', 0)).next;
  assert.equal(await waker.pollOnce(100), 0);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `you have mail from alice in topic p/t` }], 'only the message not picked up rings');
  assert.deepEqual(waker.state.held, {});
});

// --- due waits (1e): restart, retry, open turn, recipient closed, own key ------------------------------------------

// keep: core
test('wake off while a wait becomes due: a waker started later (new state file) rings it once in its first cycle and confirms; the next cycle does not ring', async (t) => {
  const inst = makeInstance('wake-restart');
  const srv = await startInstance(inst);
  const stateDir = tmpDir('comms-wake-restart-');
  t.after(async () => { await srv.stop(); cleanup(inst.dir, stateDir); });
  const svc = srv.srv.service;
  const client = makeClient(inst);
  await client.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  // No wake module runs: the message is sent and its wait becomes due (here written into the message, the clock unmoved).
  const m = await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'anyone?', thread_id: 'thr-a' });
  patchMsg(svc, m.id, { wait_due_at: new Date(Date.now() - 2 * 3600_000).toISOString() });
  // Wake starts (a restart: an empty state file), with the key `wake` of the service's machine, over HTTP.
  const adapter = fakeAdapter();
  const waker = new Waker({ client: makeClient(inst, { key: addKey(inst, 'wake') }), adapter, stateFile: path.join(stateDir, 'wake-state.json'), maxEventAgeMs: 1e12 });
  assert.equal(await waker.pollOnce(100), 0, 'a new state starts at the end of the event stream (no replay)');
  assert.deepEqual(adapter.calls, [{ holder: 'thr-a', text: `no reply ${m.id}` }], 'rung once in the first cycle');
  const st = await client.message(m.id);
  assert.equal(st.wait_n, 1, 'confirmed by wait-rang');
  assert.ok(Date.parse(st.wait_due_at) > Date.now() + 25 * 60_000, 'next check rang_at + 15 min × 2');
  assert.equal(waker.stats.due_rang, 1);
  await waker.pollOnce(100);
  assert.equal(adapter.calls.filter((c) => c.holder === 'thr-a').length, 1, 'the second cycle does not ring again');
});

test('unconfirmed ring (transient adapter failure) stays due and rings in a later cycle, even when the due time is hours old', async (t) => {
  const adapter = fakeAdapter();
  adapter.ok = false;
  adapter.ring = async (holder, text) => { adapter.calls.push({ holder, text }); return adapter.ok ? { ok: true } : { ok: false, stderr: 'connection refused' }; };
  const ctx = await setup(t, { adapter });
  const { client, waker, clock } = ctx;
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'x', thread_id: 'thr-a' });
  makeDue(ctx, m.id, 3 * 3600_000);
  const r1 = await waker.dueOnce();
  assert.deepEqual(r1.map((x) => [x.action, x.reason]), [['failed', 'ring']]);
  let st = await client.message(m.id);
  assert.deepEqual([st.wait_n, st.wait_due_at], [0, new Date(clock.t - 3 * 3600_000).toISOString()], 'not confirmed: still due');
  // Within the retry pause no second attempt.
  clock.t += 30_000;
  assert.deepEqual((await waker.dueOnce()).map((x) => x.reason), ['retry']);
  assert.equal(adapter.calls.length, 1);
  // Later cycle: the ring goes through and is confirmed, regardless of the age of the due time.
  clock.t += 31_000;
  adapter.ok = true;
  assert.deepEqual((await waker.dueOnce()).map((x) => x.action), ['rang']);
  assert.deepEqual(adapter.calls.at(-1), { holder: 'thr-a', text: `no reply ${m.id}` });
  st = await client.message(m.id);
  assert.equal(st.wait_n, 1);
  assert.equal(Date.parse(st.wait_due_at), Date.parse(st.wait_rang_at) + 3600_000 * 2);
});

test('open turn of the holder: the due wait is not rung and stays due; rung after the turn closes', async (t) => {
  const ctx = await setup(t);
  const { client, waker, adapter } = ctx;
  const o = await client.turn({ thread: 'thr-a', state: 'open', machine: 'm1' });
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'x', thread_id: 'thr-a' });
  const m2 = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'y', thread_id: 'thr-a' });
  makeDue(ctx, m.id, 2000);
  makeDue(ctx, m2.id, 1000);
  assert.deepEqual((await waker.dueOnce()).map((x) => x.reason), ['running', 'running']);
  assert.equal(adapter.calls.length, 0);
  assert.equal((await client.message(m.id)).wait_n, 0, 'stays due');
  await client.turn({ thread: 'thr-a', state: 'closed', opened_at: o.turn.opened_at });
  const r = await waker.dueOnce();
  assert.deepEqual(r.map((x) => [x.id, x.action, x.reason]), [[m.id, 'rang', null], [m2.id, 'skipped', 'this-round']], 'one wait ring per thread per round');
  assert.deepEqual(adapter.calls, [{ holder: 'thr-a', text: `no reply ${m.id}` }]);
  assert.equal((await client.message(m.id)).wait_n, 1);
  assert.equal((await client.message(m2.id)).wait_n, 0);
  await waker.dueOnce();
  assert.deepEqual(adapter.calls.at(-1), { holder: 'thr-a', text: `no reply ${m2.id}` }, 'the other wait in the next round');
});

test('wait reason recipient-closed: a picked message of a closed topic rings the sender with `recipient closed <id>` once', async (t) => {
  const ctx = await setup(t);
  const { inst, client, waker, adapter, clock } = ctx;
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key });
  const m = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'x', thread_id: 'thr-a' });
  await bob.receive({ project: 'p', topic: 't' });
  const closed = await client.registerTopic({ project: 'p', topic: 't', holder: 'thr-1', close: true });
  assert.deepEqual(closed.recipient_closed, [m.id]);
  const st = await client.message(m.id);
  assert.deepEqual([st.status, st.wait_reason], ['picked', 'recipient-closed'], 'picked stays, not undelivered');
  clock.t = Date.now() + 1000;
  await waker.dueOnce();
  assert.deepEqual(adapter.calls, [{ holder: 'thr-a', text: `recipient closed ${m.id}` }]);
  assert.equal(RECIPIENT_CLOSED_TEMPLATE, 'recipient closed');
  assert.equal((await client.message(m.id)).wait_due_at, null, 'one-shot: ended by the confirmation');
  assert.deepEqual(await waker.dueOnce(), []);
});

test('a waker with a non-wake key (carol) sees and confirms only the waits of messages sent with that key', async (t) => {
  const ctx = await setup(t);
  const { inst, client, svc, clock, stateDir } = ctx;
  const carol = makeClient(inst, { key: addKey(inst, 'carol') });
  const mine = await carol.send({ project: 'p', topic: 't', level: 'assist', text: 'from carol', thread_id: 'thr-l' });
  const other = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'from alice', thread_id: 'thr-a' });
  makeDue(ctx, mine.id);
  makeDue(ctx, other.id);
  const adapter = fakeAdapter();
  const waker = new Waker({ client: carol, adapter, stateFile: path.join(stateDir, 'carol-state.json'), now: () => clock.t });
  assert.deepEqual((await carol.due()).due.map((x) => x.id), [mine.id]);
  const r = await waker.dueOnce();
  assert.deepEqual(r.map((x) => [x.id, x.action]), [[mine.id, 'rang']]);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-l', text: `no reply ${mine.id}` }]);
  assert.equal((await client.message(mine.id)).wait_n, 1);
  assert.equal((await client.message(other.id)).wait_n, 0, 'the wait of another sender is untouched');
  assert.ok(svc.due('wake').due.some((x) => x.id === other.id), 'still due for the key wake');
  await assert.rejects(carol.waitRang(other.id), (e) => e.status === 403);
});

// --- busy only for a thread that runs; an archived or gone holder is undelivered (comms-client-wake, workflow 1.8.0) --
// 2026-11-15: a steer to stop did not reach a running thread and an archived thread was listed as busy: busy came from
// an open turn record and the discussion, and a held steer/assist was returned only on a permanent ring failure.

// keep: core
test('a stale open turn record gives no busy: T3 not running → rung, nothing queued, a due wait rings; while T3 runs the thread its mail is held and rung after the run', async (t) => {
  const adapter = t3Adapter();
  const ctx = await setup(t, { adapter });
  const { client, waker } = ctx;
  // thr-1 has an open record (its Stop was lost), T3 runs nothing.
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-1', text: `you have mail from alice in topic p/t` }]);
  assert.deepEqual(waker.state.queued, {}, 'not queued behind a run nobody runs');
  await client.send({ project: 'p', topic: 't', level: 'steer', text: 'stop', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(adapter.calls.length, 2, 'the next mail rings too');

  // T3 runs run-1: the mail is held, not rung behind the run (that ring came after the mail was picked in the run and the
  // workflow hook blocked it, operator 2026-11-18); nothing is queued.
  adapter.threads['thr-1'] = { run: 'run-1' };
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'three', thread_id: 'thr-s3' });
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'four', thread_id: 'thr-s4' });
  assert.equal(await waker.pollOnce(500), 2);
  assert.equal(adapter.calls.length, 2, 'held while T3 runs the thread');
  assert.deepEqual(Object.values(waker.state.held).map((e) => e.held_reason), ['running', 'running']);
  assert.deepEqual(waker.state.queued, {});
  await waker.releaseHeld();
  assert.equal(adapter.calls.length, 2, 'still running: still held');
  // The run ended and the mail still waits: rung (both here: this fixture turns the idle dedupe off).
  adapter.threads['thr-1'] = {};
  await waker.releaseHeld();
  assert.equal(adapter.calls.length, 4, 'rung after the run');
  assert.deepEqual(waker.state.held, {});

  // A due wait of a sender with an open record: T3 decides, not the record.
  await client.turn({ thread: 'thr-a', state: 'open', machine: 'm1' });
  const m = await client.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'x', thread_id: 'thr-a' });
  const m2 = await client.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'y', thread_id: 'thr-a' });
  makeDue(ctx, m.id, 2000);
  makeDue(ctx, m2.id, 1000);
  adapter.threads['thr-a'] = { run: 'run-a' };
  assert.deepEqual((await waker.dueOnce()).map((x) => x.reason), ['running', 'running'], 'T3 runs it: waits');
  adapter.threads['thr-a'] = {};
  assert.deepEqual((await waker.dueOnce()).map((x) => [x.id, x.action]), [[m.id, 'rang'], [m2.id, 'skipped']], 'stale record: rung');
  assert.deepEqual(adapter.calls.at(-1), { holder: 'thr-a', text: `no reply ${m.id}` });
});

// keep: core
test('without the thread state an open record counts only within its deadline: past it the service writes no busy, a due wait rings, wake queues nothing', async (t) => {
  const ctx = await setup(t);   // fake adapter without `status`
  const { inst, client, waker, adapter, clock } = ctx;
  dueByClock(ctx);
  const bob = makeClient(inst, { key: inst.store.addKey('bob').key, thread: 'thr-1' });
  await bob.turn({ thread: 'thr-1', state: 'open', machine: 'm1', discussion: 'open' });
  await client.turn({ thread: 'thr-a', state: 'open', machine: 'm1' });
  // Within the deadline the open records are busy: the service writes `busy`, a due wait of thr-a waits.
  const s0 = await client.send({ project: 'p', topic: 't', level: 'steer', text: 'adjust', ttl: '60s', thread_id: 'thr-b' });
  expire(ctx, s0.id);
  assert.equal((await client.message(s0.id)).code_answer, 'busy');
  const m = await client.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'x', thread_id: 'thr-a' });
  makeDue(ctx, m.id);
  assert.deepEqual((await waker.dueOnce()).filter((x) => x.id === m.id).map((x) => x.reason), ['running']);

  // Past the deadline nothing says the threads run (interrupted, archived, Stop lost).
  clock.t += TURN_BUSY_MAX_MS + 60_000;
  assert.deepEqual((await waker.dueOnce()).filter((x) => x.id === m.id).map((x) => x.action), ['rang']);
  assert.deepEqual(adapter.calls.filter((c) => c.holder === 'thr-a'), [{ holder: 'thr-a', text: `no reply ${m.id}` }]);
  const s1 = await client.send({ project: 'p', topic: 't', level: 'steer', text: 'adjust again', ttl: '60s', thread_id: 'thr-b' });
  assert.ok(expire(ctx, s1.id).includes(s1.id), 'expired: no code answer');
  const st = await client.message(s1.id);
  assert.deepEqual([st.code_answer, st.status, st.wait_reason], [null, 'pending', 'no-reply']);
  // Wake: the stale record holds nothing for its discussion (discussionHolds): both steers and a blocker ring thr-1 as
  // an idle thread (no dedupe of the bell in this setup); no ring is queued behind its record.
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'now', thread_id: 'thr-c' });
  await waker.pollOnce(500);
  assert.deepEqual(adapter.calls.filter((c) => c.holder === 'thr-1'), Array(3).fill({ holder: 'thr-1', text: `you have mail from alice in topic p/t` }));
  assert.deepEqual(waker.state.queued, {});
  assert.deepEqual(waker.state.held, {});
});

// keep: core
// 2026-11-19: the maintainer's thread kept an open turn record with `discussion` for hours (no Stop reached the service);
// wake held its mail for the discussion for ever and the report expired `no reply`.
test('stale open discussion record: a steer/assist rings when T3 does not run the thread; held while T3 runs it, rung after the run', async (t) => {
  const adapter = t3Adapter();
  const { client, waker, clock } = await setup(t, { adapter, goneCheckMs: 0 });
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', discussion: 'open' });
  clock.t += 2 * TURN_BUSY_MAX_MS;   // the record is past its deadline
  adapter.threads['thr-1'] = {};
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'report', thread_id: 'thr-s' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(waker.state.held, {}, 'a stale record holds nothing');
  assert.equal(adapter.calls.filter((c) => c.holder === 'thr-1').length, 1, 'rung at once');

  // T3 runs the thread: the discussion holds behind an old record too; the end of the run without a Stop rings it.
  adapter.threads['thr-1'] = { run: 'run-1' };
  adapter.calls.length = 0;
  await client.send({ project: 'p', topic: 't', level: 'steer', text: 'stop', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(Object.values(waker.state.held).map((e) => e.held_reason), ['discussion']);
  assert.deepEqual(adapter.calls, [], 'no ring into the running discussion');
  adapter.threads['thr-1'] = {};
  await waker.pollOnce(100);
  assert.deepEqual(waker.state.held, {});
  assert.equal(adapter.calls.filter((c) => c.holder === 'thr-1').length, 1, 'rung after the run');
});

// keep: core
test('archived or gone holder: a steer/assist is undelivered instead of held for the discussion, the sender gets `undelivered <id>`; a topic held by another thread keeps it', async (t) => {
  const adapter = t3Adapter();
  const ctx = await setup(t, { adapter, goneCheckMs: 0 });
  const { client, waker, logs } = ctx;
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1', discussion: 'open' });
  adapter.threads['thr-1'] = { archived: true };
  const m = await client.send({ project: 'p', topic: 't', level: 'steer', text: 'stop', thread_id: 'thr-s' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(waker.state.held, {}, 'not held');
  assert.deepEqual(adapter.calls, [], 'no ring into the archived thread');
  const st = await client.message(m.id);
  assert.deepEqual([st.status, st.undelivered_holder, st.wait_reason], ['undelivered', 'thr-1', 'undelivered']);
  assert.match(st.undelivered_reason, /archived/);
  // 1e: the next cycle rings the sender once.
  assert.equal(await waker.pollOnce(100), 0);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-s', text: `${UNDELIVERED_TEMPLATE} ${m.id}` }]);
  assert.equal((await client.message(m.id)).wait_due_at, null);

  // Held while the holder discusses (T3 runs it), archived later: the next poll returns it to its sender.
  adapter.threads['thr-1'] = { run: 'run-1' };
  adapter.calls.length = 0;
  const a = await client.send({ project: 'p', topic: 't', level: 'assist', text: 'help?', thread_id: 'thr-s2' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.equal(Object.keys(waker.state.held).length, 1, 'held for the discussion');
  adapter.threads['thr-1'] = { archived: true };
  assert.equal(await waker.pollOnce(100), 0);
  assert.deepEqual(waker.state.held, {});
  assert.equal((await client.message(a.id)).status, 'undelivered');
  await waker.pollOnce(100);
  assert.deepEqual(adapter.calls, [{ holder: 'thr-s2', text: `${UNDELIVERED_TEMPLATE} ${a.id}` }]);

  // A thread T3 no longer has: the same.
  adapter.threads['thr-1'] = 'gone';
  adapter.calls.length = 0;
  const g = await client.send({ project: 'p', topic: 't', level: 'steer', text: 'gone?', thread_id: 'thr-s3' });
  await waker.pollOnce(500);
  const sg = await client.message(g.id);
  assert.deepEqual([sg.status, sg.wait_reason], ['undelivered', 'undelivered']);
  assert.match(sg.undelivered_reason, /no longer available/);
  // The topic is held by another thread now: its message stays for the new holder.
  const k = await client.send({ project: 'p', topic: 't', level: 'steer', text: 'kept', thread_id: 'thr-s4' });
  await client.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-2', takeover: true });
  await waker.pollOnce(500);
  assert.equal((await client.message(k.id)).status, 'pending', 'the new holder receives it');
  assert.ok(logs.some((l) => l.includes(`bounce ${k.id} nevrátené (holder thr-2)`)), logs.join('\n'));
  assert.ok(!adapter.calls.some((c) => c.holder === 'thr-1'), 'never rung into the gone thread');
});

// keep: core
test('a workflow thread T3 runs gets no ring behind the run (closed record too); picked in the run: none after it; past turnBusyMaxMs: rung behind the run', async (t) => {
  const adapter = t3Adapter();
  const ctx = await setup(t, { adapter });
  const { client, waker, clock } = ctx;
  // Stop wrote a closed record, T3 still runs the thread (or a bell turn started before its hook opened a record).
  await client.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  await client.turn({ thread: 'thr-1', state: 'closed', machine: 'm1' });
  adapter.threads['thr-1'] = { run: 'run-1' };
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'one', thread_id: 'thr-s' });
  assert.equal(await waker.pollOnce(500), 1);
  assert.deepEqual(adapter.calls, [], 'held while T3 runs the thread');
  // Picked in that run (the tool line showed it): no empty bell after it (operator 2026-11-18, "blocked by hook").
  await client.receive({ project: 'p', topic: 't' });
  adapter.threads['thr-1'] = {};
  await waker.releaseHeld();
  assert.deepEqual(adapter.calls, [], 'picked meanwhile: no ring');
  assert.deepEqual(waker.state.held, {});
  // A run that does not end: past turnBusyMaxMs from the first hold the ring goes behind the run, as before.
  adapter.threads['thr-1'] = { run: 'run-2' };
  await client.send({ project: 'p', topic: 't', level: 'assist', text: 'two', thread_id: 'thr-s2' });
  await waker.pollOnce(500);
  assert.deepEqual(adapter.calls, []);
  clock.t += waker.opts.turnBusyMaxMs + 1000;
  await waker.releaseHeld();
  assert.equal(adapter.calls.length, 1, 'rung behind the run after the cap');
  assert.deepEqual(waker.state.held, {});
});
