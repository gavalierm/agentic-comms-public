// Generic prod units: no server values (user, home, paths), commands relative to WorkingDirectory from the server drop-in.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

const dir = path.join(path.dirname(new URL(import.meta.url).pathname), '..', 'deploy', 'systemd');

test('deploy/systemd: generic comms-prod and wake-prod units', () => {
  for (const [unit, bin] of [['comms-prod.service', 'bin/comms.mjs serve --instance prod'], ['wake-prod.service', 'bin/wake.mjs run --instance prod --no-start']]) {
    const text = fs.readFileSync(path.join(dir, unit), 'utf8');
    assert.match(text, new RegExp(`^ExecStart=/usr/bin/node ${bin}$`, 'm'), unit);
    assert.match(text, /^Restart=always$/m, `${unit}: self-restart exits 0`);
    assert.doesNotMatch(text, /^(User|Group|WorkingDirectory|Environment)=/m, `${unit}: server values belong to a drop-in`);
    assert.doesNotMatch(text, /\/home\/|v\d+\.\d+\.\d+/, `${unit}: no home path, no version`);
  }
});
