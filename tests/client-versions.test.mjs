// The service records which mail client and workflow version a sender runs on a machine; `comms clients` lists it.
// A call without version headers keeps working and is recorded with nulls. Server side: the headers X-Comms-Client,
// X-Workflow-Version and X-Comms-Machine (src/client sends them with every request).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, cleanup, rawRequest, api } from './helpers.mjs';

async function up(t) {
  const a = makeInstance('ver');
  const sa = await startInstance(a);
  t.after(async () => { await sa.stop(); cleanup(a.dir); });
  return a;
}

/** A client that names its version, workflow version and machine in every request. */
function clientOn(a, { machine = null, comms = null, workflow = null } = {}) {
  const c = api(a);
  const headers = {
    ...(machine ? { 'X-Comms-Machine': machine } : {}),
    ...(comms ? { 'X-Comms-Client': comms } : {}),
    ...(workflow ? { 'X-Workflow-Version': workflow } : {}),
  };
  const get = async (p) => { const r = await c.call('GET', p, undefined, headers); assert.equal(r.status, 200, r.body); return r.json; };
  return {
    clients: async () => (await get('/clients')).clients,
    notices: () => get('/notices'),
    register: async (body) => { const r = await c.call('POST', '/register-topic', body, headers); assert.equal(r.status, 200, r.body); return r.json; },
  };
}

test('client sends its version, the workflow version and machine; service lists them per sender and machine', async (t) => {
  const a = await up(t);
  const c = clientOn(a, { machine: 'Mac Carol', comms: '0.5.0', workflow: 'v0.9.91' });
  await c.register({ project: 'proj', topic: 'topic-a', subject: 'Test topic' });
  const clients = await c.clients();
  const rec = clients.find((r) => r.sender === a.sender && r.machine === 'mac-carol');
  assert.ok(rec, JSON.stringify(clients));
  assert.equal(rec.comms_version, '0.5.0');
  assert.equal(rec.workflow_version, 'v0.9.91');
  assert.ok(rec.last_seen);
});

test('a call without version headers still works and is recorded with null versions; a call of another model is not recorded', async (t) => {
  const a = await up(t);
  const r = await api(a).call('POST', '/register-topic', { project: 'proj', topic: 'topic-b', subject: 'Old' });
  assert.equal(r.status, 200, r.body);
  const clients = await clientOn(a, { machine: 'srv' }).clients();
  const old = clients.find((x) => x.machine === 'unknown');
  assert.ok(old, JSON.stringify(clients));
  assert.equal(old.comms_version, null);
  assert.equal(old.workflow_version, null);
  // A model 1 client (no X-Comms-Model) is refused before anything is written (I10), also its client record.
  const before = fs.readdirSync(path.join(a.dir, 'clients')).length;
  const m1 = await rawRequest(a.socket, 'GET', '/clients', { headers: { Authorization: `Bearer ${a.key}`, 'X-Comms-Machine': 'legacy', 'X-Comms-Client': '0.4.21' } });
  assert.equal(m1.status, 426);
  assert.equal(fs.readdirSync(path.join(a.dir, 'clients')).length, before);
  assert.ok(!(await clientOn(a, { machine: 'srv' }).clients()).some((x) => x.machine === 'legacy'));
});

test('a call without the workflow version (wake, direct CLI) keeps the recorded workflow version', async (t) => {
  const a = await up(t);
  await clientOn(a, { machine: 'm2', workflow: 'v9' }).notices();
  const clients = await clientOn(a, { machine: 'm2' }).clients();
  const m2 = clients.filter((x) => x.machine === 'm2');
  assert.equal(m2.length, 1);
  assert.equal(m2[0].workflow_version, 'v9');
});

test('a changed workflow version replaces the record of the same sender and machine', async (t) => {
  const a = await up(t);
  await clientOn(a, { machine: 'm1', workflow: 'v1' }).notices();
  await clientOn(a, { machine: 'm1', workflow: 'v2' }).notices();
  const clients = await clientOn(a, { machine: 'm1', workflow: 'v2' }).clients();
  const m1 = clients.filter((x) => x.machine === 'm1');
  assert.equal(m1.length, 1);
  assert.equal(m1[0].workflow_version, 'v2');
});
