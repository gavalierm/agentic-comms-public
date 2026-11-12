import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, makeClient, rawRequest, cleanup, sleep, tmpDir } from './helpers.mjs';

// Starts a service for the test; stopping it and removing its data is registered for the end of the test.
async function serviceUp(t) {
  const a = makeInstance('a');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  return { a, sa };
}

test('blocker creates a notice; decide is executed by the service without a model', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const sent = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'stuck' });
  assert.ok(sent.notice_id);

  const open = await c.notices();
  assert.equal(open.notices.length, 1);
  assert.equal(open.notices[0].kind, 'blocker');
  assert.equal(open.notices[0].text, 'stuck');

  const d = await c.decide({ notice_id: sent.notice_id, decision: 'ack' });
  assert.equal(d.decision, 'ack');
  assert.equal((await c.notices()).notices.length, 0);
  const all = await c.notices(true);
  assert.equal(all.notices[0].state, 'decided');
  assert.equal(all.notices[0].decision, 'ack');
  // message still pending for the holder
  assert.equal((await c.receive({ project: 'p', topic: 't' })).messages.length, 1);

  await assert.rejects(c.decide({ notice_id: sent.notice_id, decision: 'ack' }), (e) => e.status === 409);
  await assert.rejects(c.decide({ notice_id: 'nope', decision: 'ack' }), (e) => e.status === 404);
});

test('legacy no-reply notice (0.2.x data) stays decidable', async (t) => {
  const { a, sa } = await serviceUp(t);
  const svc = sa.srv.service;
  svc.store.saveNotice({ id: 'old-n', kind: 'no-reply', title: 'old', state: 'open', message_id: null, options: [{ id: 'ack', label: 'ok', needs_target: false }], created_at: '2026-01-01T00:00:00Z' });
  const c = makeClient(a);
  assert.equal((await c.decide({ notice_id: 'old-n', decision: 'ack' })).decision, 'ack');
});

test('decide redirect moves a held message to a registered topic; discard drops it', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 'real', subject: 'S' });

  const m1 = await c.send({ project: 'p', topic: 'typo', level: 'assist', text: 'where am i' });
  await assert.rejects(c.decide({ notice_id: m1.notice_id, decision: 'redirect', target: 'p/missing' }), (e) => e.status === 409);
  await assert.rejects(c.decide({ notice_id: m1.notice_id, decision: 'redirect', target: 'bad target' }), (e) => e.status === 400);
  await assert.rejects(c.decide({ notice_id: m1.notice_id, decision: 'unknown' }), (e) => e.status === 400);
  const d = await c.decide({ notice_id: m1.notice_id, decision: 'redirect', target: 'p/real' });
  assert.deepEqual(d.delivered_to, { project: 'p', topic: 'real' });
  const inbox = await c.receive({ project: 'p', topic: 'real' });
  assert.equal(inbox.messages.length, 1);
  assert.equal(inbox.messages[0].id, m1.id);
  assert.equal(fs.readdirSync(path.join(a.dir, 'topics')).length, 1, 'no topic created by the service');

  const m2 = await c.send({ project: 'p', topic: 'typo', level: 'assist', text: 'drop me' });
  await c.decide({ notice_id: m2.notice_id, decision: 'discard' });
  const stored = JSON.parse(fs.readFileSync(path.join(a.dir, 'messages', `${m2.id}.json`), 'utf8'));
  assert.equal(stored.state, 'discarded');
});

test('HTML page lists notices with decision buttons and accepts form decisions', async (t) => {
  const { a, sa } = await serviceUp(t);
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  const sent = await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'Potrebujem <prístup>' });
  const held = await c.send({ project: 'p', topic: 'nowhere', level: 'assist', text: 'lost' });

  const noKey = await rawRequest(a.socket, 'GET', '/');
  assert.equal(noKey.status, 401);
  assert.match(noKey.headers['content-type'], /text\/html/);

  const page = await rawRequest(a.socket, 'GET', `/?key=${encodeURIComponent(a.key)}`);
  assert.equal(page.status, 200);
  assert.match(page.headers['content-type'], /text\/html/);
  assert.match(page.headers['set-cookie'][0], /comms_key=/);
  assert.ok(page.body.includes('Blocker pre tému p/t'));
  assert.ok(page.body.includes('Nová téma? / presmerovať: p/nowhere'));
  assert.ok(page.body.includes('Potrebujem &lt;prístup&gt;'), 'text is escaped');
  assert.ok(!page.body.includes('<prístup>'));
  assert.ok(page.body.includes(`name="notice_id" value="${sent.notice_id}"`));
  assert.ok(page.body.includes('name="decision" value="ack"'));
  assert.ok(page.body.includes('name="decision" value="redirect"'));
  assert.ok(!page.body.includes('<script'));

  // Form post (as the browser button does): redirect back, notice decided.
  const form = await rawRequest(a.socket, 'POST', '/decide', {
    body: new URLSearchParams({ key: a.key, notice_id: sent.notice_id, decision: 'ack' }).toString(),
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });
  assert.equal(form.status, 303);
  assert.match(form.headers.location, /^\/\?key=/);
  assert.equal((await c.notices()).notices.length, 1);
  assert.equal((await c.notices()).notices[0].id, held.notice_id);

  // Cookie auth works for subsequent page loads; decided notice shown under all=1.
  const withCookie = await rawRequest(a.socket, 'GET', '/?all=1', { headers: { Cookie: `comms_key=${encodeURIComponent(a.key)}` } });
  assert.equal(withCookie.status, 200);
  assert.ok(withCookie.body.includes('Rozhodnuté: ack'));

  // Form error → HTML error page, not JSON.
  const bad = await rawRequest(a.socket, 'POST', '/decide', {
    body: new URLSearchParams({ key: a.key, notice_id: held.notice_id, decision: 'redirect', target: 'p/none' }).toString(),
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });
  assert.equal(bad.status, 409);
  assert.match(bad.headers['content-type'], /text\/html/);
});

test('notify.command runs for blocker notices and receives the notice on stdin', async (t) => {
  const a = makeInstance('a');
  const out = path.join(tmpDir('comms-notify-'), 'mail.txt');
  const inst = a.store.instance();
  inst.notify = { command: `cat >> "${out}"`, to: 'operator@example.test' };
  a.store.saveInstance(inst);
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir, path.dirname(out)); });
  const c = makeClient(a);
  await c.registerTopic({ project: 'p', topic: 't', subject: 'S' });
  await c.send({ project: 'p', topic: 't', level: 'assist', text: 'quiet' });
  await c.send({ project: 'p', topic: 't', level: 'blocker', text: 'loud' });
  let mail = '';
  for (let i = 0; i < 60 && !/Možnosti:/.test(mail); i++) { await sleep(50); try { mail = fs.readFileSync(out, 'utf8'); } catch { /* not yet */ } }
  assert.match(mail, /To: operator@example.test/);
  assert.match(mail, /Subject: \[comms a\] Blocker pre tému p\/t/);
  assert.match(mail, /Úroveň: blocker/);
  assert.equal((mail.match(/Subject:/g) || []).length, 1, 'info did not notify');
});

test('TCP listener requires the bearer key; HTML page available over TCP', async (t) => {
  const a = makeInstance('a');
  const sa = await startInstance(a, { tcp: { host: '127.0.0.1', port: 0 } });
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  const { port } = sa.srv.tcp;
  assert.ok(port > 0);
  assert.equal(a.store.serverInfo().tcp.port, port);
  const url = `http://127.0.0.1:${port}`;

  const health = await fetch(`${url}/health`);
  assert.equal((await health.json()).instance_id, a.instanceId);
  const anon = await fetch(`${url}/notices`);
  assert.equal(anon.status, 401);
  const authed = await fetch(`${url}/notices`, { headers: { Authorization: `Bearer ${a.key}` } });
  assert.equal(authed.status, 200);
  const page = await fetch(`${url}/?key=${encodeURIComponent(a.key)}`);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /Žiadne otvorené oznamy/);

  // Remote client via --url, same instance_id check, no autostart.
  const remote = makeClient(a, { url, instanceId: a.instanceId });
  assert.equal(remote.cfg.autostart, false);
  assert.equal((await remote.ensureRunning()).instance_id, a.instanceId);
});
