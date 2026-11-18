// keep: core
// Thread address (operator 2026-11-18, docs/MODEL.md section 4): the maintainer's thread sends a new message to a thread by
// its id (to_thread); the server takes the machine from what it knows of that thread so wake rings it there; any other
// sender is refused (403), an unknown thread is refused (404), and the recipient's answer goes back to the maintainer.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { makeInstance, startInstance, api, cleanup } from './helpers.mjs';

const MT = 'thread-maintainer';
const R = 'thread-remote';

test('to_thread: only the maintainer thread sends it; rung on the thread\'s machine; unknown thread 404; the answer reaches the maintainer', async () => {
  const inst = makeInstance('thr');
  const { stop } = await startInstance(inst);
  const a = api(inst);
  const messages = () => fs.readdirSync(path.join(inst.dir, 'messages')).filter((f) => f.endsWith('.json')).length;
  try {
    const f = path.join(inst.dir, 'instance.json');
    fs.writeFileSync(f, JSON.stringify({ ...JSON.parse(fs.readFileSync(f, 'utf8')), maintainer: { thread: MT, machine: 'myserver' } }));
    // Unknown thread: nothing to ring, nothing stored.
    await assert.rejects(a.send({ to_thread: R, level: 'assist', text: 'x', thread_id: MT, thread_machine: 'myserver' }), (e) => e.status === 404);
    assert.equal(messages(), 0);
    // The remote thread wrote the maintainer once from its machine: the server knows where it runs.
    await a.send({ maintainer: true, level: 'assist', text: 'test', thread_id: R, thread_machine: 'doma-pc' });
    const before = messages();
    await assert.rejects(a.send({ to_thread: R, level: 'assist', text: 'x', thread_id: 'thread-other', thread_machine: 'myserver' }), (e) => e.status === 403);
    await assert.rejects(a.send({ to_thread: R, level: 'assist', text: 'x', thread_id: MT, thread_machine: 'elsewhere' }), (e) => e.status === 403, 'the maintainer thread id from another machine');
    await assert.rejects(a.send({ to_thread: R, project: 'p', topic: 't', level: 'assist', text: 'x', thread_id: MT, thread_machine: 'myserver' }), (e) => e.status === 400);
    assert.equal(messages(), before, 'refused sends store nothing');
    const r = await a.send({ to_thread: R, level: 'assist', text: 'ping from the maintainer', thread_id: MT, thread_machine: 'myserver' });
    assert.equal(r.status, 'pending');
    assert.equal(r.to_thread, R);
    const ev = (await a.events(0)).find((e) => e.message_id === r.id);
    assert.equal(ev.holder, R);
    assert.equal(ev.holder_machine, 'doma-pc');
    const got = await a.receive({ thread: R });
    assert.ok(got.some((m) => m.id === r.id && /ping from the maintainer/.test(m.text)));
    await a.resolve({ id: r.id, state: 'hotovo', text: 'pong', thread: R, thread_id: R });
    const back = await a.receive({ thread: MT });
    assert.ok(back.some((m) => m.reply_to === r.id && /pong/.test(m.text)), JSON.stringify(back.map((m) => [m.id, m.reply_to])));
  } finally { await stop(); cleanup(inst.dir); }
});
