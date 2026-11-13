// Periodic wake job for the tests: `node job.mjs <log> [sleep-ms] [exit-code]`.
// Appends "start <epoch ms>" to <log>, sleeps, appends "end <epoch ms>", exits with the code
// (a non-zero code also writes a line to stderr, so the wake log shows the job output).
import fs from 'node:fs';

const [file, sleepMs = '0', code = '0'] = process.argv.slice(2);
fs.appendFileSync(file, `start ${Date.now()}\n`);
setTimeout(() => {
  fs.appendFileSync(file, `end ${Date.now()}\n`);
  if (Number(code) !== 0) process.stderr.write('job failed on purpose\n');
  process.exit(Number(code));
}, Number(sleepMs));
