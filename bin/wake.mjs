#!/usr/bin/env node
// Wake module (P-043): separate process above the mail. See src/wake/cli.mjs.
import { main } from '../src/wake/cli.mjs';

const code = await main(process.argv.slice(2));
if (code !== 0) process.exit(code);
