// Fake `t3 thread send` / `t3 thread show --json` for tests: appends its arguments as one
// JSON line to FAKE_T3_LOG, prints FAKE_T3_STDOUT (status JSON) and exits FAKE_T3_EXIT.
import fs from 'node:fs';
if (process.env.FAKE_T3_LOG) fs.appendFileSync(process.env.FAKE_T3_LOG, JSON.stringify(process.argv.slice(2)) + '\n');
if (process.env.FAKE_T3_STDOUT) process.stdout.write(process.env.FAKE_T3_STDOUT);
process.exit(Number(process.env.FAKE_T3_EXIT || 0));
