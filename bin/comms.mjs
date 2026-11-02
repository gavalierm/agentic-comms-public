#!/usr/bin/env node
// Mail (comms) client and service launcher. See src/client/cli.mjs.
import { main } from '../src/client/cli.mjs';

const code = await main(process.argv.slice(2));
if (code !== 0) process.exit(code);
