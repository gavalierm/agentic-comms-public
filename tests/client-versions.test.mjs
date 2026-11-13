// The service records which mail client and workflow version a sender runs on a machine; `comms clients` lists it.
// A client without version headers (older release) keeps working and is recorded with nulls.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { makeInstance, startInstance, makeClient, cleanup, rawRequest } from './helpers.mjs';
import { CLIENT_VERSION } from '../src/client/index.mjs';

async function up(t) {
  const a = makeInstance('ver');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  return a;
}

test('client sends its version, the workflow version and machine; service lists them per sender and machine', async (t) => {
  const a = await up(t);
  const c = makeClient(a, { machine: 'Mac Carol', workflowVersion: 'v0.9.91' });
  await c.registerTopic({ project: 'proj', topic: 'topic-a', subject: 'Test topic' });
  const { clients } = await c.clients();
  const rec = clients.find((r) => r.sender === a.sender && r.machine === 'mac-carol');
  assert.ok(rec, JSON.stringify(clients));
  assert.equal(rec.comms_version, CLIENT_VERSION);
  assert.equal(rec.workflow_version, 'v0.9.91');
  assert.ok(rec.last_seen);
});

test('old client without version headers still works and is recorded with null versions', async (t) => {
  const a = await up(t);
  const r = await rawRequest(a.socket, 'POST', '/register-topic', { body: { project: 'proj', topic: 'topic-b', subject: 'Old' }, headers: { Authorization: `Bearer ${a.key}` } });
  assert.equal(r.status, 200, r.body);
  const c = makeClient(a, { machine: 'srv' });
  const { clients } = await c.clients();
  const old = clients.find((x) => x.machine === 'unknown');
  assert.ok(old, JSON.stringify(clients));
  assert.equal(old.comms_version, null);
  assert.equal(old.workflow_version, null);
});

test('a call without the workflow version (wake, direct CLI) keeps the recorded workflow version', async (t) => {
  const a = await up(t);
  await makeClient(a, { machine: 'm2', workflowVersion: 'v9' }).notices();
  const { clients } = await makeClient(a, { machine: 'm2' }).clients();
  const m2 = clients.filter((x) => x.machine === 'm2');
  assert.equal(m2.length, 1);
  assert.equal(m2[0].workflow_version, 'v9');
});

test('a changed workflow version replaces the record of the same sender and machine', async (t) => {
  const a = await up(t);
  await makeClient(a, { machine: 'm1', workflowVersion: 'v1' }).notices();
  await makeClient(a, { machine: 'm1', workflowVersion: 'v2' }).notices();
  const { clients } = await makeClient(a, { machine: 'm1', workflowVersion: 'v2' }).clients();
  const m1 = clients.filter((x) => x.machine === 'm1');
  assert.equal(m1.length, 1);
  assert.equal(m1[0].workflow_version, 'v2');
});
