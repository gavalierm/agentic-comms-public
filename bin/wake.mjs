#!/usr/bin/env node
// Wake module (P-043): separate process above the mail. See src/wake/cli.mjs.
// Outside systemd `run` goes through a small supervisor that starts wake again after a self-restart (src/wake/supervise.mjs).
import { fileURLToPath } from 'node:url';
import { main } from '../src/wake/cli.mjs';
import { needsSupervisor, supervise } from '../src/wake/supervise.mjs';

const argv = process.argv.slice(2);
const code = needsSupervisor(argv) ? await supervise(fileURLToPath(import.meta.url), argv) : await main(argv);
if (code !== 0) process.exit(code);
