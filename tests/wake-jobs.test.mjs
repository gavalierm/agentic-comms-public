// Periodic jobs of the wake module (D-0023, D-0024): `jobs` in wake.json run through the shell at their interval next
// to the long-poll, one run of a job at a time, with a time limit; a failing job is logged and never stops the module.
// The module is imported as a namespace, so each test fails on its own assertion where the feature is missing.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { makeInstance, startInstance, makeClient, cleanup, sleep, tmpDir } from './helpers.mjs';
import * as wake from '../src/wake/index.mjs';

const JOB = fileURLToPath(new URL('./fixtures/job.mjs', import.meta.url));
const WAKE_BIN = fileURLToPath(new URL('../bin/wake.mjs', import.meta.url));
const q = (s) => `"${s}"`;
/** Shell command of the fixture job: appends start/end lines to `log`, sleeps `ms`, exits with `code`. */
const jobCmd = (log, ms = 0, code = 0) => `${q(process.execPath)} ${q(JOB)} ${q(log)} ${ms} ${code}`;
const lines = (file) => (fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split('\n').filter(Boolean) : []);
const starts = (file) => lines(file).filter((l) => l.startsWith('start ')).map((l) => Number(l.split(' ')[1]));

async function waitFor(fn, timeout = 8000, step = 20) {
  const until = Date.now() + timeout;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > until) throw new Error('waitFor: timeout');
    await sleep(step);
  }
}

function fakeAdapter() {
  const calls = [];
  return { name: 'fake', command: ['fake'], calls, ring: async (holder, text) => { calls.push({ holder, text }); return { ok: true }; } };
}

test('run(): jobs run at their interval next to the long-poll; failing, missing and hanging commands are logged, the module keeps working', async (t) => {
  const inst = makeInstance('wakejobs');
  const srv = await startInstance(inst);
  const dir = tmpDir('comms-wakejobs-');
  // Stopped also when an assertion fails (else its job loops keep the test process alive).
  const ac = new AbortController();
  let running = null;
  t.after(async () => { ac.abort(); await running; await srv.stop(); cleanup(inst.dir, dir); });
  const client = makeClient(inst);
  await client.registerTopic({ project: 'p', topic: 't', subject: 'S', holder: 'thr-1' });
  const okLog = path.join(dir, 'ok.log');
  const failLog = path.join(dir, 'fail.log');
  const hangLog = path.join(dir, 'hang.log');
  const logs = [];
  const adapter = fakeAdapter();
  const waker = new wake.Waker({
    client: makeClient(inst), adapter, stateFile: path.join(dir, 'wake-state.json'), log: (m) => logs.push(m),
    maxEventAgeMs: 1e12, idleDedupeMs: 0, waitMs: 100, keepaliveIntervalMs: 60_000,
    // Short intervals through the parameter (the 60 s minimum applies to wake.json, see normalizeJobs).
    jobs: [
      { name: 'ok', command: jobCmd(okLog), interval_ms: 120, timeout_ms: 5000 },
      { name: 'fail', command: jobCmd(failLog, 0, 3), interval_ms: 120, timeout_ms: 5000 },
      { name: 'missing', command: 'no-such-command-for-wake-jobs', interval_ms: 120, timeout_ms: 5000 },
      // Time limit well above the start-up of node under a loaded test run, far below the job's own 10 s.
      { name: 'hang', command: jobCmd(hangLog, 10_000), interval_ms: 1500, timeout_ms: 1000 },
    ],
  });
  await waker.initCursor();
  // When the module starts each run (the child's own start line also carries its start-up time, which varies under load).
  const calls = [];
  const runJobOnce = waker.runJobOnce.bind(waker);
  waker.runJobOnce = (job, signal) => { calls.push({ name: job.name, at: Date.now() }); return runJobOnce(job, signal); };
  running = waker.run({ signal: ac.signal });

  await waitFor(() => starts(okLog).length >= 3 && starts(failLog).length >= 2 && starts(hangLog).length >= 2
    && logs.some((l) => l.startsWith('úloha hang: zlyhala')), 20_000);
  const s = calls.filter((c) => c.name === 'ok').map((c) => c.at);
  assert.ok(s.length >= 3);
  for (let i = 1; i < s.length; i++) assert.ok(s[i] - s[i - 1] >= 115, `ok runs started ${s[i] - s[i - 1]} ms apart, interval 120 ms`);
  assert.ok(logs.some((l) => /^úloha ok: ok \(\d+ ms\)$/.test(l)), logs.join('\n'));
  assert.ok(logs.some((l) => l === 'úloha fail: zlyhala (kód 3): job failed on purpose'), logs.join('\n'));
  assert.ok(logs.some((l) => l.startsWith('úloha missing: zlyhala (')), logs.join('\n'));
  assert.ok(logs.some((l) => l.startsWith('úloha hang: zlyhala (prekročila časový limit)')), logs.join('\n'));
  // The hanging run was killed at its time limit (no `end` line) and started again at the next interval.
  assert.ok(!lines(hangLog).some((l) => l.startsWith('end ')));

  // After all the failures the mail part still works: a blocker rings the holder.
  await client.send({ project: 'p', topic: 't', level: 'blocker', text: 'x' });
  await waitFor(() => adapter.calls.length >= 1, 10_000);
  assert.deepEqual(adapter.calls[0], { holder: 'thr-1', text: `${wake.WAKE_TEMPLATE} p/t` });

  // The last finished run of each job is persisted (read while running: an aborted run at the stop would overwrite it).
  const st = JSON.parse(fs.readFileSync(path.join(dir, 'wake-state.json'), 'utf8'));
  assert.equal(st.jobs.ok.ok, true);
  assert.equal(st.jobs.fail.ok, false);
  assert.equal(st.jobs.fail.code, 3);

  ac.abort();
  const stats = await running;
  assert.ok(stats.jobs_ok >= 3, JSON.stringify(stats));
  assert.ok(stats.jobs_failed >= 3, JSON.stringify(stats));
  assert.equal(stats.rang, 1);
  // Stopped with the signal: no further runs.
  const after = starts(okLog).length;
  await sleep(300);
  assert.equal(starts(okLog).length, after);
});

test('one run of a job at a time: a second start while it runs is skipped; a slow job never overlaps itself; the schedule survives a restart', async (t) => {
  const dir = tmpDir('comms-wakejobs1-');
  t.after(() => cleanup(dir));
  const stateFile = path.join(dir, 'wake-state.json');
  const logs = [];
  const waker = new wake.Waker({ client: null, stateFile, log: (m) => logs.push(m) });
  const once = path.join(dir, 'once.log');
  const job = { name: 'slow', command: jobCmd(once, 300), interval_ms: 60_000, timeout_ms: 5000 };
  const first = waker.runJobOnce(job);
  const second = await waker.runJobOnce(job);
  assert.deepEqual(second, { action: 'skipped', reason: 'running' });
  assert.ok(logs.includes('úloha slow: predchádzajúci beh ešte beží, ďalší sa nespúšťa'), logs.join('\n'));
  assert.equal((await first).action, 'ok');
  assert.equal(starts(once).length, 1);

  // Interval shorter than a run: the loop waits for the run to end, so start/end lines alternate.
  const loopLog = path.join(dir, 'loop.log');
  const fast = { name: 'overrun', command: jobCmd(loopLog, 200), interval_ms: 50, timeout_ms: 5000 };
  const ac = new AbortController();
  waker.opts.jobs = [fast];
  const loop = waker.jobsLoop(ac.signal);
  await waitFor(() => starts(loopLog).length >= 3);
  ac.abort();
  await loop;
  const seq = lines(loopLog);
  for (let i = 0; i < seq.length; i++) assert.ok(seq[i].startsWith(i % 2 ? 'end ' : 'start '), seq.join('\n'));
  for (let i = 2; i < seq.length; i += 2) assert.ok(Number(seq[i].split(' ')[1]) >= Number(seq[i - 1].split(' ')[1]), seq.join('\n'));

  // The last run is persisted: a new module on the same state does not run the job again before its interval.
  const again = new wake.Waker({ client: null, stateFile, log: () => {} });
  assert.equal(again.state.jobs.slow.ok, true);
  assert.ok(again.jobDue(job) > Date.now() + 50_000);
  assert.ok(again.jobDue({ name: 'never-ran', interval_ms: 60_000 }) <= Date.now());
});

test('wake.json jobs: valid entries normalized, invalid ones skipped with a line, short interval raised to 60 s; other keys unchanged; `wake config` shows them', (t) => {
  const inst = makeInstance('wakejobscfg');
  t.after(() => cleanup(inst.dir));
  fs.writeFileSync(path.join(inst.dir, 'wake.json'), JSON.stringify({
    keepalive: { since_command: 'echo 0' },
    jobs: [
      { name: 'issue-watch', command: 'sh watch.sh', interval_ms: 900000 },
      { name: 'too-fast', command: 'true', interval_ms: 1000, timeout_ms: 600000 },
      { name: 'defaults', command: 'true' },
      { name: 'no-command' },
      'not an object',
      { name: 'issue-watch', command: 'again' },
      { name: 'bad-interval', command: 'true', interval_ms: 'soon' },
      { command: 'nameless' },
    ],
  }));
  const c = wake.resolveWakeConfig({ dataDir: inst.dir }, {});
  assert.deepEqual(c.jobs, [
    { name: 'issue-watch', command: 'sh watch.sh', interval_ms: 900000, timeout_ms: 300000 },
    { name: 'too-fast', command: 'true', interval_ms: 60000, timeout_ms: 60000 },
    { name: 'defaults', command: 'true', interval_ms: 900000, timeout_ms: 300000 },
  ]);
  assert.deepEqual(c.jobProblems.map((p) => p.split(':')[0]), ['jobs[1] (too-fast)', 'jobs[3] (no-command)', 'jobs[4]', 'jobs[5] (issue-watch)', 'jobs[6] (bad-interval)', 'jobs[7]']);
  assert.match(c.jobProblems[0], /pod minimom 60000; použije sa 60000/);
  assert.match(c.jobProblems[1], /chýba príkaz/);
  assert.equal(c.keepaliveSinceCommand, 'echo 0');
  assert.equal(c.keepaliveIntervalMs, wake.DEFAULTS.keepaliveIntervalMs);
  assert.equal(wake.JOB_MIN_INTERVAL_MS, 60000);

  // No jobs key → none; `jobs` that is not a list → none, one line.
  fs.writeFileSync(path.join(inst.dir, 'wake.json'), JSON.stringify({ limit_per_hour: 3 }));
  assert.deepEqual(wake.resolveWakeConfig({ dataDir: inst.dir }, {}).jobs, []);
  fs.writeFileSync(path.join(inst.dir, 'wake.json'), JSON.stringify({ jobs: { name: 'x', command: 'y' } }));
  const bad = wake.resolveWakeConfig({ dataDir: inst.dir }, {});
  assert.deepEqual([bad.jobs, bad.jobProblems.length], [[], 1]);
  // The --config file: `wake.jobs`.
  const cfgFile = path.join(inst.dir, 'cfg.json');
  fs.writeFileSync(cfgFile, JSON.stringify({ data_dir: inst.dir, wake: { jobs: [{ name: 'from-config', command: 'true', interval_ms: 120000 }] } }));
  assert.deepEqual(wake.resolveWakeConfig({ config: cfgFile }, {}).jobs, [{ name: 'from-config', command: 'true', interval_ms: 120000, timeout_ms: 120000 }]);
  // `wake config` prints the resolved jobs.
  const r = spawnSync(process.execPath, [WAKE_BIN, 'config', '--config', cfgFile], { env: { PATH: process.env.PATH, HOME: process.env.HOME }, encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(JSON.parse(r.stdout).jobs, [{ name: 'from-config', command: 'true', interval_ms: 120000, timeout_ms: 120000 }]);
});

test('bin/wake.mjs run: a job from wake.json runs at start (also while the mail is down), is listed in the log and in `wake status`', async (t) => {
  const inst = makeInstance('wakejobsproc');
  const dir = tmpDir('comms-wakejobsproc-');
  const jobLog = path.join(dir, 'job.log');
  fs.writeFileSync(path.join(inst.dir, 'wake.json'), JSON.stringify({
    jobs: [{ name: 'issue-watch', command: jobCmd(jobLog), interval_ms: 60000 }, { name: 'broken' }],
  }));
  const env = { PATH: process.env.PATH, HOME: process.env.HOME, COMMS_SELF_RESTART: '0' };
  const child = spawn(process.execPath, [WAKE_BIN, 'run', '--data-dir', inst.dir, '--no-start', '--adapter', 'none', '--reconnect-ms', '100', '--state-dir', dir], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  let err = '';
  child.stderr.on('data', (d) => { err += d; });
  const exited = new Promise((r) => child.on('exit', (code) => r(code)));
  t.after(async () => { try { child.kill('SIGKILL'); } catch { /* gone */ } await exited; cleanup(inst.dir, dir); });

  await waitFor(() => starts(jobLog).length >= 1 && err.includes('úloha issue-watch: ok'));
  assert.match(err, /úloha issue-watch: každých 60000 ms \(limit 60000 ms\)/);
  assert.match(err, /úlohy: jobs\[1\] \(broken\): chýba príkaz \(command\); preskočená/);
  const st = spawnSync(process.execPath, [WAKE_BIN, 'status', '--data-dir', inst.dir, '--state-dir', dir], { env, encoding: 'utf8' });
  assert.equal(st.status, 0, st.stderr);
  const job = JSON.parse(st.stdout).jobs['issue-watch'];
  assert.equal(job.interval_ms, 60000);
  assert.equal(job.last.ok, true);
  child.kill('SIGTERM');
  assert.equal(await exited, 0);
  assert.equal(starts(jobLog).length, 1);
});
