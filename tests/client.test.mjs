import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeInstance, startInstance, makeClient, cleanup, sleep, killPid, tmpDir, runCli } from './helpers.mjs';
import { probe } from '../src/server/index.mjs';
import { socketPath } from '../src/common/paths.mjs';
import { Client, resolveConfig } from '../src/client/index.mjs';

const BIN = fileURLToPath(new URL('../bin/comms.mjs', import.meta.url));

function cli(args, env = {}) {
  const r = spawnSync(process.execPath, [BIN, ...args], {
    encoding: 'utf8', env: { PATH: process.env.PATH, HOME: process.env.HOME, COMMS_SELF_RESTART: '0', ...env },
  });
  return { code: r.status, out: r.stdout, err: r.stderr, json: tryJson(r.stdout) };
}
const tryJson = (s) => { try { return JSON.parse(s); } catch { return null; } };
// Servers used together with spawnSync must run in their own process: spawnSync blocks the event loop.
const serverInfo = (dir) => { try { return tryJson(fs.readFileSync(path.join(dir, 'server.json'), 'utf8')); } catch { return null; } };

// Starts a service for the test; stopping it and removing its data is registered for the end of the test.
async function serviceUp(t) {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  return { a, sa };
}

test('client talks over socketPath and verifies instance_id', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a);
  assert.equal(c.cfg.socket, a.socket);
  assert.equal(c.cfg.url, null);
  const h = await c.ensureRunning();
  assert.equal(h.instance_id, a.instanceId);
});

test('stopped service: send goes to outbox, later flushed in order', async (t) => {
  const a = makeInstance('a');
  const outbox = path.join(tmpDir('comms-outbox-'), 'outbox');
  t.after(() => cleanup(a.dir, path.dirname(outbox)));

  // Register topic while running, then stop.
  const s1 = await startInstance(a);
  await makeClient(a).registerTopic({ project: 'p', topic: 't', subject: 'S' });
  await s1.stop();

  const offline = makeClient(a, { outbox, timeout: 500 });
  const started = Date.now();
  const r1 = await offline.sendOrQueue({ project: 'p', topic: 't', level: 'assist', text: 'first' });
  const r2 = await offline.sendOrQueue({ project: 'p', topic: 't', level: 'assist', text: 'second' });
  assert.ok(Date.now() - started < 2000, 'client must give up within the deadline');
  assert.equal(r1.queued, true);
  assert.equal(r2.queued, true);
  assert.equal(fs.readdirSync(outbox).filter((f) => f.endsWith('.json')).length, 2);
  assert.equal(fs.readdirSync(path.join(a.dir, 'messages')).length, 0);

  // Service back: next client call flushes the outbox first.
  const s2 = await startInstance(a);
  t.after(() => s2.stop());
  const online = makeClient(a, { outbox });
  const r3 = await online.sendOrQueue({ project: 'p', topic: 't', level: 'assist', text: 'third' });
  assert.equal(r3.queued, undefined);
  assert.equal(fs.readdirSync(outbox).filter((f) => f.endsWith('.json')).length, 0);
  const inbox = await online.receive({ project: 'p', topic: 't' });
  assert.deepEqual(inbox.messages.map((m) => m.text), ['first', 'second', 'third']);
});

test('client respects the 2 s deadline when the socket is dead', async (t) => {
  const a = makeInstance('a');
  t.after(() => cleanup(a.dir));
  const c = makeClient(a, { timeout: 2000 });
  const started = Date.now();
  await assert.rejects(c.receive({ project: 'p', topic: 't' }), (e) => e.code === 'UNAVAILABLE');
  assert.ok(Date.now() - started < 2500);
});

test('client starts the service on demand (CLI) and a second start is a no-op', async (t) => {
  const a = makeInstance('a');
  t.after(async () => {
    const info = serverInfo(a.dir);
    if (info) { killPid(info.pid); await sleep(100); }
    cleanup(a.dir);
  });

  const h = cli(['health', '--data-dir', a.dir]);
  assert.equal(h.code, 0, h.err);
  assert.equal(h.json.instance_id, a.instanceId);
  assert.ok(fs.existsSync(path.join(a.dir, 'server.json')));
  const pid = tryJson(fs.readFileSync(path.join(a.dir, 'server.json'), 'utf8')).pid;

  // serve again: the socket acts as the lock.
  const again = cli(['serve', '--data-dir', a.dir]);
  assert.equal(again.code, 0, again.err);
  assert.equal(again.json.already_running, true);
  assert.equal(again.json.pid, pid);

  const stop = cli(['stop', '--data-dir', a.dir]);
  assert.equal(stop.json.stopped, true);
  await sleep(100);
  const after = cli(['health', '--data-dir', a.dir, '--no-start']);
  assert.equal(after.code, 2);
});

test('count is live (new mail within 60 s is seen) and outage is reported once per window', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(() => cleanup(a.dir));
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'b' });

  const now = Date.now();
  const r1 = await c.count({ project: 'p', topic: 't' }, now);
  assert.deepEqual([r1.total, r1.blocker, r1.cached], [1, 1, false]);
  await c.send({ project: 'p', topic: 't', level: 'assist', text: 'i' });
  // Pilot: the bell for this message came 15 s after the previous count; a cached 0 hid it.
  const r2 = await makeClient(a).count({ project: 'p', topic: 't' }, now + 15_000);
  assert.deepEqual([r2.total, r2.cached], [2, false], 'mail that arrived after the last count is counted');

  await sa.stop();
  const r4 = await makeClient(a, { timeout: 300 }).count({ project: 'p', topic: 't' }, now + 130_000);
  assert.deepEqual([r4.unavailable, r4.first_report, r4.total], [true, true, 2]);
  const r5 = await makeClient(a, { timeout: 300 }).count({ project: 'p', topic: 't' }, now + 140_000);
  assert.deepEqual([r5.unavailable, r5.first_report], [true, false]);
});

test('config file supplies key, instance_id and outbox', async (t) => {
  const a = makeInstance('a');
  const cfgDir = tmpDir('comms-cfg-');
  t.after(async () => {
    const info = serverInfo(a.dir);
    if (info) { killPid(info.pid); await sleep(100); }
    cleanup(a.dir, cfgDir);
  });
  const cfgFile = path.join(cfgDir, 'comms.json');
  fs.writeFileSync(cfgFile, JSON.stringify({ data_dir: a.dir, key: a.key, instance_id: a.instanceId, outbox: path.join(cfgDir, 'outbox') }));
  const r = cli(['register-topic', '--config', cfgFile, '--project', 'p', '--topic', 't', '--subject', 'S']);
  assert.equal(r.code, 0, r.err);
  assert.equal(r.json.registered, true);
  fs.writeFileSync(cfgFile, JSON.stringify({ data_dir: a.dir, key: a.key, instance_id: 'other' }));
  const bad = cli(['health', '--config', cfgFile, '--no-start']);
  assert.equal(bad.code, 3);
});

test('queued closed turn: flushed by the next turn record, before the new open', async (t) => {
  const a = makeInstance('a');
  const outbox = path.join(tmpDir('comms-turnq-'), 'outbox');
  t.after(() => cleanup(a.dir, path.dirname(outbox)));
  const offline = makeClient(a, { outbox, timeout: 300 });
  const q1 = await offline.turn({ thread: 'thr-1', state: 'closed', opened_at: 'old-turn' });
  assert.deepEqual([q1.ok, q1.unavailable, q1.queued], [false, true, true]);
  const q2 = await offline.turn({ thread: 'thr-1', state: 'open' });
  assert.deepEqual([q2.ok, q2.unavailable, q2.queued], [false, true, undefined]);
  assert.equal(fs.readdirSync(outbox).filter((f) => f.endsWith('.json')).length, 1);

  const sa = await startInstance(a);
  t.after(() => sa.stop());
  const online = makeClient(a, { outbox });
  // The next turn record flushes the queued close first (fable audit M6), then opens the new turn.
  const open = await online.turn({ thread: 'thr-1', state: 'open', machine: 'm1' });
  assert.equal(fs.readdirSync(outbox).filter((f) => f.endsWith('.json')).length, 0, 'flushed by turn');
  const turns = (await online.turns(true)).turns;
  assert.deepEqual(turns.map((x) => [x.thread, x.opened_at]), [['thr-1', open.turn.opened_at]], 'the stale close changed nothing');
});

// --- hotfix 0.2.1: turn, message, --ttl, count ids ------------------------------------

test('CLI turn on a dead mail: { ok:false, unavailable:true }, exit 0, fast; only closed is queued', (t) => {
  const a = makeInstance('a');
  const outbox = path.join(tmpDir('comms-turn-outbox-'), 'outbox');
  t.after(() => cleanup(a.dir, path.dirname(outbox)));
  const started = Date.now();
  const r = cli(['turn', '--data-dir', a.dir, '--key', a.key, '--no-start', '--outbox', outbox, '--state', 'open', '--thread', 'thr-1', '--timeout', '5000']);
  assert.ok(Date.now() - started < 3000, 'short timeout');
  assert.equal(r.code, 0, r.err);
  assert.equal(r.json.ok, false);
  assert.equal(r.json.unavailable, true);
  assert.equal(fs.existsSync(outbox), false, 'open is never queued');
  const closed = cli(['turn', '--data-dir', a.dir, '--key', a.key, '--no-start', '--outbox', outbox, '--state', 'closed', '--thread', 'thr-1', '--opened-at', '2026-11-01T10:00:00.000Z']);
  assert.equal(closed.code, 0, closed.err);
  assert.deepEqual([closed.json.ok, closed.json.unavailable, closed.json.queued], [false, true, true]);
  const queued = fs.readdirSync(outbox).filter((f) => f.endsWith('.json'));
  assert.equal(queued.length, 1);
  const item = JSON.parse(fs.readFileSync(path.join(outbox, queued[0]), 'utf8'));
  assert.deepEqual([item.path, item.body.state, item.body.opened_at], ['/turn', 'closed', '2026-11-01T10:00:00.000Z']);
  const noThread = cli(['turn', '--data-dir', a.dir, '--key', a.key, '--no-start', '--state', 'open']);
  assert.equal(noThread.code, 1);
  assert.match(noThread.err, /--thread/);
});

test('CLI send: info is refused with the valid levels; --ttl max 5m (blocker/steer) and 30m (assist); turn and message against a running service', async (t) => {
  const a = makeInstance('a');
  t.after(async () => {
    const info = serverInfo(a.dir);
    if (info) { killPid(info.pid); await sleep(100); }
    cleanup(a.dir);
  });
  const base = ['--data-dir', a.dir, '--key', a.key];
  const info = cli(['send', ...base, '--project', 'p', '--topic', 't', '--level', 'info', '--text', 'x']);
  assert.equal(info.code, 1);
  assert.match(info.err, /zrušená/);
  assert.match(info.err, /blocker\|steer\|assist/);
  assert.match(info.err, /"status":400/);
  const tooLong = cli(['send', ...base, '--project', 'p', '--topic', 't', '--level', 'blocker', '--text', 'x', '--ttl', '30m']);
  assert.equal(tooLong.code, 1);
  assert.match(tooLong.err, /maximum 5m/);
  const noLevel = cli(['send', ...base, '--project', 'p', '--topic', 't', '--text', 'x']);
  assert.equal(noLevel.code, 1);
  assert.match(noLevel.err, /--level/);
  const bad = cli(['send', ...base, '--project', 'p', '--topic', 't', '--level', 'assist', '--text', 'x', '--ttl', 'later']);
  assert.equal(bad.code, 1);

  assert.equal(cli(['register-topic', ...base, '--project', 'p', '--topic', 't', '--subject', 'S']).code, 0);
  const sent = cli(['send', ...base, '--thread', 'thr-a', '--project', 'p', '--topic', 't', '--level', 'assist', '--text', 'help', '--ttl', '90s']);
  assert.equal(sent.code, 0, sent.err);
  assert.ok(sent.json.expires_at);
  const msg = cli(['message', ...base, '--id', sent.json.id]);
  assert.equal(msg.code, 0, msg.err);
  assert.deepEqual(msg.json, {
    id: sent.json.id, state: 'pending', level: 'assist', reply_to: null, reply_state: null, answer_state: null, answer_at: null, answer_id: null, answer_origin: null, cancels: null, cancelled_by: null, project: 'p', topic: 't', to: 'p/t', outcome: null, replied: false,
    expires_at: sent.json.expires_at, ttl_s: 90, picked_at: null, undelivered_holder: null, undelivered_reason: null,
  });

  const open = cli(['turn', ...base, '--state', 'open', '--thread', 'thr-a', '--machine', 'Box-1', '--project', 'p', '--topic', 't', '--holder-mode', 'operator']);
  assert.equal(open.code, 0, open.err);
  assert.deepEqual([open.json.ok, open.json.turn.machine, open.json.turn.holder_mode, open.json.turn.state], [true, 'box-1', 'operator', 'open']);
  const stale = cli(['turn', ...base, '--state', 'closed', '--thread', 'thr-a', '--opened-at', '2000-01-01T00:00:00.000Z']);
  assert.deepEqual(stale.json, { ok: true, stale: true });
  const closed = cli(['turn', ...base, '--state', 'closed', '--thread', 'thr-a', '--opened-at', open.json.turn.opened_at]);
  assert.equal(closed.json.turn.state, 'closed');
  const wrong = cli(['turn', ...base, '--state', 'half', '--thread', 'thr-a']);
  assert.equal(wrong.code, 1, 'a rejected turn is an error, only an outage is not');

  const count = cli(['count', ...base, '--project', 'p', '--topic', 't']);
  assert.deepEqual(count.json.ids, [sent.json.id]);
});

test('count keeps ids for an outage; an old service without ids gives none', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const m = await c.send({ project: 'p', topic: 't', level: 'assist', text: 'a' });
  const now = Date.now();
  assert.deepEqual((await c.count({ project: 'p', topic: 't' }, now)).ids, [m.id]);
  const m2 = await c.send({ project: 'p', topic: 't', level: 'assist', text: 'b' });
  const live = await makeClient(a).count({ project: 'p', topic: 't' }, now + 10_000);
  assert.deepEqual([live.cached, live.ids], [false, [m.id, m2.id]]);
  const cache = JSON.parse(fs.readFileSync(path.join(a.dir, 'client', 'count-cache.json'), 'utf8'));
  assert.deepEqual(Object.values(cache)[0].ids, [m.id, m2.id]);
  // Outage: the last answer with its ids, marked cached and unavailable.
  const down = makeClient(a);
  down.call = async () => { throw new Error('down'); };
  const off = await down.count({ project: 'p', topic: 't' }, now + 20_000);
  assert.deepEqual([off.cached, off.unavailable, off.ids], [true, true, [m.id, m2.id]]);

  // Old service: count without ids → the client reports none (the workflow falls back to total).
  const old = makeClient(a, { clientDir: tmpDir('comms-oldcount-') });
  t.after(() => cleanup(old.cfg.clientDir));
  old.call = async () => ({ total: 2, blocker: 0 });
  const r = await old.count({ project: 'p', topic: 't' }, now);
  assert.deepEqual([r.total, 'ids' in r], [2, false]);
});

test('client: a 2xx answer that is not JSON is an error (BAD_RESPONSE); the outbox item is kept', async (t) => {
  const srv = http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/html' }); res.end('<html>proxy page</html>'); });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  t.after(() => srv.close());
  const dir = tmpDir('comms-nonjson-');
  t.after(() => cleanup(dir));
  const outbox = path.join(dir, 'outbox');
  const c = new Client(resolveConfig({ url: `http://127.0.0.1:${srv.address().port}`, key: 'k', outbox, dataDir: dir, noStart: true }, {}));
  const file = c.queue('/send', { project: 'p', topic: 't', level: 'assist', text: 'x' });
  await assert.rejects(c.request('POST', '/send', { body: {} }), (e) => e.code === 'BAD_RESPONSE');
  await assert.rejects(c.flushOutbox(), (e) => e.code === 'BAD_RESPONSE');
  assert.equal(fs.existsSync(file), true, 'the queued message is not deleted');
});

test('CLI: serve forwards --config/--machine to the wake child; --wake on a running service exits non-zero', async (t) => {
  const { a, sa } = await serviceUp(t);
  // A service is already running (in this process): --wake cannot start and says so.
  const busy = await runCli(['serve', '--data-dir', a.dir, '--wake']);
  assert.notEqual(busy.code, 0);
  assert.match(busy.err, /NESPUSTILI/);
  assert.equal((await runCli(['serve', '--data-dir', a.dir])).code, 0, 'without --wake/--tcp the old answer stays');

  // Forwarding: the wake child logs the machine it got; with --config the machine comes from the config file.
  const dir = tmpDir('comms-serve-fwd-');
  const b = makeInstance('fwd');
  const cfg2 = path.join(dir, 'cfg2.json');
  fs.writeFileSync(cfg2, JSON.stringify({ data_dir: b.dir, machine: 'cfg-box' }));
  t.after(() => cleanup(dir, b.dir));
  const child = spawn(process.execPath, [BIN, 'serve', '--config', cfg2, '--wake'], { env: { PATH: process.env.PATH, HOME: process.env.HOME, COMMS_SELF_RESTART: '0', WAKE_ADAPTER: 'none' }, stdio: ['ignore', 'ignore', 'pipe'] });
  let err = '';
  child.stderr.on('data', (d) => { err += d; });
  t.after(() => { try { child.kill('SIGKILL'); } catch { /* gone */ } });
  const until = Date.now() + 10000;
  while (!/stroj cfg-box/.test(err) && Date.now() < until) await sleep(50);
  assert.match(err, /stroj cfg-box/, err);
  // Stopping the service ends wake too (README): serve waits for its child, and the child does not start a service of
  // its own when this one goes away (four wake processes from this test ran for hours, 2026-11-12).
  const exited = new Promise((r) => child.once('exit', r));
  child.kill('SIGTERM');
  await Promise.race([exited, sleep(8000)]);
  assert.match(err, /wake skončil/, err);
  await sleep(1000);
  assert.equal(await probe(socketPath(b.dir)), null, 'no service runs after the stop');
});

test('client: each call has its own timeout; a client created long ago still makes calls', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a, { timeout: 100 });
  await sleep(250);
  assert.equal((await c.ensureRunning()).instance_id, a.instanceId);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  await sleep(250);
  assert.equal((await c.notices()).notices.length, 0);
  await sleep(250);
  assert.equal((await c.events(0, 0)).events.length, 0);
});

test('CLI: message with several --id, config set-notify without argument and key remove of an unknown sender are errors', async (t) => {
  const a = makeInstance('a');
  t.after(() => cleanup(a.dir));
  const base = ['--data-dir', a.dir, '--key', a.key];
  const many = cli(['message', ...base, '--id', 'x1', '--id', 'x2']);
  assert.equal(many.code, 1);
  assert.match(many.err, /jedno --id/);
  const notify = cli(['config', 'set-notify', ...base]);
  assert.equal(notify.code, 1);
  assert.equal(JSON.parse(cli(['config', 'show', ...base]).out).notify.command, null, 'nothing was changed');
  const rm = cli(['key', 'remove', ...base, '--sender', 'nobody']);
  assert.equal(rm.code, 1);
  assert.match(rm.err, /neexistuje/);
  assert.equal(cli(['key', 'remove', ...base, '--sender', 'alice']).code, 0, 'a known sender is removed');
});

test('a managed instance (started by systemd) is never autostarted by a client', async (t) => {
  const { isManagedStart } = await import('../src/server/index.mjs');
  assert.equal(isManagedStart({ INVOCATION_ID: 'x' }, 1), true);
  assert.equal(isManagedStart({ INVOCATION_ID: 'x' }, 4242), false, 'client-spawned server inherits INVOCATION_ID but not parent PID 1');
  assert.equal(isManagedStart({ COMMS_MANAGED: '1' }, 4242), true);
  assert.equal(isManagedStart({}, 1), false);

  const a = makeInstance('managed');
  t.after(() => cleanup(a.dir));
  const sa = await startInstance(a, { managed: true });
  await sa.stop();
  assert.equal(a.store.instance().managed, true);
  const c = makeClient(a, { noStart: false, timeout: 300 });
  assert.equal(c.cfg.autostart, true);
  c.spawnServer = () => { throw new Error('autostart of a managed instance'); };
  await assert.rejects(c.ensureRunning(), (e) => e.code === 'UNAVAILABLE');
});
