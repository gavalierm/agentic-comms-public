// keep: core (5a, client of mail model 3): the outbox replayed twice is one answer; a service of another model gets
// nothing written or queued and a flush keeps the outbox; 3c order of a flush (a model 2 item without brain → rejected/);
// the obligations backup (3b); one flush of an outbox at a time (flush lock: two flushes at once, a stale lock, a lock
// still held when the wait ends).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { spawnSync } from 'node:child_process';
import { makeInstance, startInstance, makeClient, cleanup, tmpDir, runCli } from './helpers.mjs';
import { Client, resolveConfig, modelMismatchText } from '../src/client/index.mjs';

const BOX = { project: 'p', topic: 't' };
// Literals, not imports: the lock file name is a contract between the clients of one machine (sessions may run two versions).
const FLUSH_LOCK = 'flush.lock';
const FLUSH_BUSY_TEXT = 'outbox flush in progress';
const jsonFiles = (dir) => (fs.existsSync(dir) ? fs.readdirSync(dir).filter((f) => f.endsWith('.json')) : []);
const readMsg = (inst, id) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', `${id}.json`), 'utf8'));

async function setup(t) {
  const inst = makeInstance('core');
  const outbox = path.join(tmpDir('comms-core-outbox-'), 'outbox');
  const s = await startInstance(inst);
  t.after(async () => { await s.stop(); cleanup(inst.dir, path.dirname(outbox)); });
  const sender = makeClient(inst, { thread: 'thr-a', outbox });
  const recipient = makeClient(inst, { thread: 'thr-b', outbox });
  await recipient.registerTopic({ ...BOX, subject: 'S', holder: 'thr-b' });
  return { inst, s, outbox, sender, recipient };
}

/** What reaches the service, in order: the text of each send and the state of each turn record. */
function recordWrites(s) {
  const seen = [];
  const { service } = s.srv;
  for (const [name, label] of [['send', (_sender, body) => body.text], ['turn', (body) => `turn ${body.state}`]]) {
    const orig = service[name].bind(service);
    service[name] = (...a) => { seen.push(label(...a)); return orig(...a); };
  }
  return seen;
}

/** A fake service: /health answers `health`, every other request `status` with `body`. */
async function fakeService(t, health, status = 426, body = { error: 'mail model 4: update mail and workflow together', model: 4 }) {
  const seen = [];
  const srv = http.createServer((req, res) => {
    seen.push(`${req.method} ${req.url}`);
    const [code, data] = req.url === '/health' ? [200, health] : [status, body];
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  t.after(() => srv.close());
  const dir = tmpDir('comms-fake-');
  t.after(() => cleanup(dir));
  const url = `http://127.0.0.1:${srv.address().port}`;
  const client = new Client(resolveConfig({ url, key: 'k', dataDir: dir, outbox: path.join(dir, 'outbox'), noStart: true }, {}));
  return { client, seen, url, dir };
}

test('outbox replayed twice is one answer: the same client_op_id gives the original confirmation (send and resolve)', async (t) => {
  const { inst, outbox, sender, recipient } = await setup(t);
  const sent = await sender.send({ ...BOX, level: 'assist', text: 'q' });
  await recipient.receive({ ...BOX, thread: 'thr-b' });
  // The same resolve queued twice (a flush that died after the HTTP answer, then a second copy).
  const item = { ...BOX, id: sent.id, state: 'hotovo', text: 'answer', thread: 'thr-b', thread_id: 'thr-b', client_op_id: 'op-resolve-1' };
  recipient.queue('/resolve', item);
  recipient.queue('/resolve', item);
  const send = { ...BOX, level: 'assist', text: 'again', thread_id: 'thr-a', client_op_id: 'op-send-1' };
  sender.queue('/send', send);
  sender.queue('/send', send);
  const r = await recipient.flushOutbox();
  assert.equal(r.sent.length, 4);
  assert.deepEqual(r.sent.map((x) => !!x.replayed), [false, true, false, true]);
  assert.equal(jsonFiles(outbox).length, 0);
  const m = readMsg(inst, sent.id);
  assert.deepEqual([m.status, m.resolution], ['done', 'reply']);
  const replies = fs.readdirSync(path.join(inst.dir, 'messages')).map((f) => JSON.parse(fs.readFileSync(path.join(inst.dir, 'messages', f), 'utf8'))).filter((x) => x.reply_to === sent.id);
  assert.equal(replies.length, 1, 'one reply');
  const again = (await sender.receive({ ...BOX, thread: 'thr-b' })).messages.filter((x) => x.text === 'again');
  assert.equal(again.length, 1, 'one message from two queued sends');
});

test('another model: nothing is written or queued, the client fails loudly, a flush keeps the outbox (4a, 3c, I10)', async (t) => {
  // Service 0.4.x: /health has no model.
  const old = await fakeService(t, { ok: true, instance_id: null, version: '0.4.21' }, 200, { id: 'x' });
  old.client.queue('/send', { ...BOX, level: 'assist', text: 'queued before', client_op_id: 'op-1' });
  await assert.rejects(old.client.send({ ...BOX, level: 'assist', text: 'x' }), (e) => e.code === 'MODEL_MISMATCH' && e.message === modelMismatchText(null));
  await assert.rejects(old.client.resolve({ id: 'm1', state: 'hotovo' }), (e) => e.code === 'MODEL_MISMATCH');
  await assert.rejects(old.client.flushOutbox(), (e) => e.code === 'MODEL_MISMATCH');
  await assert.rejects(old.client.obligations({ thread: 'thr-a' }), (e) => e.code === 'MODEL_MISMATCH');
  assert.equal(jsonFiles(old.client.cfg.outbox).length, 1, 'only the item queued before; nothing moved to rejected/');
  assert.ok(!fs.existsSync(path.join(old.client.cfg.outbox, 'rejected')));
  assert.deepEqual(old.seen.filter((s) => !s.startsWith('GET /health')), [], 'nothing but /health was asked');
  assert.equal(modelMismatchText(null), 'comms model ? ≠ 3: update mail and workflow together');

  // A service that refuses the model with 426: an error, not an outage (no outbox item).
  const strict = await fakeService(t, { ok: true, instance_id: null, model: 3 });
  await assert.rejects(strict.client.send({ ...BOX, level: 'assist', text: 'x' }), (e) => e.code === 'MODEL_MISMATCH' && e.status === 426);
  assert.equal(jsonFiles(strict.client.cfg.outbox).length, 0);

  // CLI: exit code 3 and the line on stderr.
  const cli = await runCli(['send', '--url', old.url, '--key', 'k', '--data-dir', old.dir, '--client-dir', old.dir, '--outbox', path.join(old.dir, 'cli-outbox'), ...['--project', 'p', '--topic', 't'], '--level', 'assist', '--text', 'x']);
  assert.equal(cli.code, 3, cli.err);
  assert.match(cli.err, /comms model \? ≠ 3: update mail and workflow together/);
  assert.equal(jsonFiles(path.join(old.dir, 'cli-outbox')).length, 0);
});

test('flush (3c): 409 with a final status drops the item with `already <resolution>`; another refusal goes to rejected/', async (t) => {
  const { inst, outbox, sender, recipient } = await setup(t);
  const a = await sender.send({ ...BOX, level: 'assist', text: 'a' });
  const b = await sender.send({ ...BOX, level: 'assist', text: 'b' });
  await recipient.receive({ ...BOX, thread: 'thr-b' });
  await recipient.resolve({ ...BOX, id: a.id, state: 'hotovo', text: 'done live' });
  recipient.queue('/resolve', { ...BOX, id: a.id, state: 'hotovo', thread: 'thr-b', client_op_id: 'op-late' });
  // A resolve for a mailbox the message is not in: 403.
  recipient.queue('/resolve', { project: 'p', topic: 'other', id: b.id, state: 'hotovo', client_op_id: 'op-other' });
  // An item queued by a model 2 client (no sender brain): it cannot name its topic, so it goes to rejected/ with its address.
  fs.writeFileSync(path.join(outbox, '29991231T000000000Z-0000aaaa.json'), JSON.stringify({ path: '/send', body: { ...BOX, level: 'assist', text: 'old', client_op_id: 'op-m2' } }));
  const r = await recipient.flushOutbox();
  assert.deepEqual(r.dropped.map((x) => [x.id, x.resolution]), [[a.id, 'reply']]);
  assert.ok(r.lines.includes(`already reply ${a.id}`));
  assert.deepEqual(r.rejected.map((x) => x.status), [403, null]);
  assert.ok(r.lines.includes('rejected /send p/t (model 2 item without brain)'), r.lines.join('\n'));
  assert.equal(jsonFiles(outbox).length, 0);
  assert.equal(jsonFiles(path.join(outbox, 'rejected')).filter((f) => !f.endsWith('.error.json')).length, 2);
  assert.equal(readMsg(inst, b.id).status, 'picked', 'the refused resolve changed nothing');
});

test('obligations backup (3b): offline answer is the last live one, marked, without recent, minus ids with a queued operation', async (t) => {
  const { inst, s, outbox, sender, recipient } = await setup(t);
  const a = await sender.send({ ...BOX, level: 'assist', text: 'a' });
  const b = await sender.send({ ...BOX, level: 'assist', text: 'b' });
  await recipient.receive({ ...BOX, thread: 'thr-b' });
  const t0 = Date.now();
  const live = await recipient.obligations({ thread: 'thr-b' }, t0);
  assert.equal(live.offline, false);
  assert.deepEqual(live.owed.map((x) => x.id), [a.id, b.id]);
  const mine = await sender.obligations({ thread: 'thr-a' }, t0);
  assert.deepEqual(mine.awaiting.map((x) => x.id), [a.id, b.id]);
  await s.stop();
  const off = makeClient(inst, { thread: 'thr-b', outbox, timeout: 300 });
  const q = await off.resolve({ ...BOX, id: a.id, state: 'hotovo', text: 'later' });
  assert.equal(q.queued, true);
  const o = await off.obligations({ thread: 'thr-b' }, t0 + 90_000);
  assert.deepEqual([o.offline, o.cached, o.age_s, o.recent.length], [true, true, 90, 0]);
  assert.deepEqual(o.owed.map((x) => x.id), [b.id], 'the resolved one is gone from owed');
  const offSender = makeClient(inst, { thread: 'thr-a', outbox, timeout: 300 });
  await offSender.withdraw({ id: b.id });
  const o2 = await offSender.obligations({ thread: 'thr-a' }, t0);
  assert.deepEqual(o2.awaiting.map((x) => x.id), [a.id], 'the withdrawn one is gone from awaiting');
  // Back: the outbox goes first, then the live answer replaces the backup (server wins).
  const s2 = await startInstance(inst);
  t.after(() => s2.stop());
  const back = makeClient(inst, { thread: 'thr-b', outbox });
  const after = await back.obligations({ thread: 'thr-b' });
  assert.equal(after.offline, false);
  assert.deepEqual(after.owed, [], 'a resolved, b withdrawn');
  assert.equal(jsonFiles(outbox).length, 0);
  assert.deepEqual([readMsg(inst, a.id).resolution, readMsg(inst, b.id).resolution], ['reply', 'withdrawn']);
});

test('one flush of an outbox at a time: two flushes and a send at once in one process, then two processes: every item reaches the service once, in order, no call fails', async (t) => {
  const { inst, s, outbox, sender, recipient } = await setup(t);
  const seen = recordWrites(s);
  // Four sends and a turn record (it has no client_op_id: the service cannot replay it).
  const fill = (tag) => {
    for (const n of [1, 2, 3, 4]) sender.queue('/send', { ...BOX, level: 'assist', text: `${tag}${n}`, thread_id: 'thr-a', client_op_id: `op-${tag}${n}` });
    sender.queue('/turn', { thread: 'thr-a', state: 'closed', opened_at: 'old-turn' });
  };
  fill('a');
  const [f1, f2, live] = await Promise.all([
    sender.flushOutbox(),
    recipient.flushOutbox(),
    makeClient(inst, { thread: 'thr-a', outbox }).send({ ...BOX, level: 'assist', text: 'live' }),
  ]);
  assert.deepEqual(seen, ['a1', 'a2', 'a3', 'a4', 'turn closed', 'live'], 'once each, in order, the new write last');
  assert.deepEqual([f1.sent.length + f2.sent.length, [...f1.sent, ...f2.sent].some((x) => x.replayed)], [5, false]);
  assert.equal(live.status, 'pending');
  assert.deepEqual(fs.readdirSync(outbox), [], 'no item and no lock left');

  // Two processes (two sessions of one brain, each runs a hook).
  fill('b');
  seen.length = 0;
  const flush = ['flush', '--data-dir', inst.dir, '--key', inst.key, '--no-start', '--outbox', outbox];
  const runs = await Promise.all([runCli(flush), runCli(flush)]);
  assert.deepEqual(runs.map((r) => r.code), [0, 0], runs.map((r) => r.err).join('\n'));
  assert.deepEqual(seen, ['b1', 'b2', 'b3', 'b4', 'turn closed']);
  assert.equal(runs[0].json.sent.length + runs[1].json.sent.length, 5);
  assert.deepEqual(fs.readdirSync(outbox), []);
});

test('flush lock: a lock of a dead process or with an old heartbeat is taken over', async (t) => {
  const { outbox, sender } = await setup(t);
  const dead = spawnSync(process.execPath, ['-e', '']).pid;
  const locks = [
    { pid: dead, token: 'other', heartbeat_at: new Date().toISOString(), stale_ms: 60_000 },
    { pid: process.pid, token: 'other', heartbeat_at: new Date(Date.now() - 61_000).toISOString(), stale_ms: 60_000 },
  ];
  for (const [i, lock] of locks.entries()) {
    sender.queue('/send', { ...BOX, level: 'assist', text: `m${i}`, thread_id: 'thr-a', client_op_id: `op-stale-${i}` });
    fs.writeFileSync(path.join(outbox, FLUSH_LOCK), JSON.stringify(lock));
    const r = await sender.flushOutbox();
    assert.deepEqual([r.sent.length, r.busy], [1, undefined], `lock ${i}`);
    assert.deepEqual(fs.readdirSync(outbox), [], 'the stale lock is gone, the new one released');
  }
});

test('flush lock still held when the wait ends: a send and a closed turn are queued behind the outbox, an open turn and obligations go on, nothing overtakes', async (t) => {
  const { inst, s, outbox, sender } = await setup(t);
  const seen = recordWrites(s);
  sender.queue('/send', { ...BOX, level: 'assist', text: 'first', thread_id: 'thr-a', client_op_id: 'op-first' });
  // A live flush of another caller: this process, another token, a fresh heartbeat.
  const lockFile = path.join(outbox, FLUSH_LOCK);
  fs.writeFileSync(lockFile, JSON.stringify({ pid: process.pid, token: 'other', heartbeat_at: new Date().toISOString(), stale_ms: 60_000 }));
  const c = () => makeClient(inst, { thread: 'thr-a', outbox, timeout: 250 });
  const [w, closed, open, ob, fl] = await Promise.all([
    c().send({ ...BOX, level: 'assist', text: 'second' }),
    c().turn({ thread: 'thr-a', state: 'closed', opened_at: 'old-turn' }),
    c().turn({ thread: 'thr-a', state: 'open' }),
    c().obligations({ thread: 'thr-a' }),
    c().flushOutbox(),
  ]);
  assert.deepEqual([w.queued, w.message, typeof w.client_op_id, fs.existsSync(w.outbox_file)], [true, FLUSH_BUSY_TEXT, 'string', true]);
  assert.deepEqual([closed.ok, closed.unavailable, closed.queued, closed.error], [false, true, true, FLUSH_BUSY_TEXT]);
  assert.equal(open.turn.state, 'open', 'open is never queued');
  assert.equal(ob.offline, false);
  assert.deepEqual([fl.busy, fl.remaining, fl.sent.length], [true, 1, 0]);
  assert.deepEqual(seen, ['turn open'], 'no queued write reached the service');
  assert.equal(JSON.parse(fs.readFileSync(lockFile, 'utf8')).token, 'other', 'the live lock is not touched');
  const queued = jsonFiles(outbox).sort().map((f) => JSON.parse(fs.readFileSync(path.join(outbox, f), 'utf8')));
  assert.deepEqual([queued.length, queued[0].body.text], [3, 'first']);

  // The other flush ends: the next flush sends all three, the oldest first.
  fs.rmSync(lockFile);
  const r = await sender.flushOutbox();
  assert.deepEqual([r.sent.length, r.busy], [3, undefined]);
  assert.deepEqual([seen[1], seen.slice(2).sort()], ['first', ['second', 'turn closed']]);
  assert.deepEqual(fs.readdirSync(outbox), []);
});
