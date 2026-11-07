// Self-restart without a service manager (comms-public#3). A new install revision or a changed wake.json ends
// `wake run`; under systemd (Restart=always) that is the restart. In a terminal (Linux, macOS, Windows), under
// launchd without KeepAlive or a Windows scheduled task nothing would start it again and wake would stop for good.
// So `bin/wake.mjs run` outside systemd runs the real wake as a child and starts it again when it exits with
// RESTART_EXIT; the child is a new node process, so it runs the new code. Kept small: the supervisor itself is the
// old version until it is restarted by hand.
import { spawn } from 'node:child_process';
import { selfRestartEnabled } from '../common/selfrestart.mjs';

export const RESTART_EXIT = 75;
export const CHILD_ENV = 'COMMS_WAKE_CHILD';

/** True when `run` should go through the supervisor: not the child itself, not systemd, self-restart on. */
export function needsSupervisor(argv, env = process.env) {
  if (argv[0] !== 'run' || argv.includes('--help')) return false;
  if (env[CHILD_ENV]) return false;
  if (env.INVOCATION_ID) return false; // systemd sets it; Restart=always restarts the exit
  if (['0', 'false', 'no', 'off'].includes(String(env.COMMS_WAKE_SUPERVISOR ?? '').toLowerCase())) return false;
  return selfRestartEnabled({ noSelfRestart: argv.includes('--no-self-restart') }, env);
}

/** Run `node <bin> ...argv` as a child until it exits with something other than RESTART_EXIT; resolves to its code. */
export function supervise(bin, argv, { env = process.env, log = (m) => process.stderr.write(`[wake] ${m}\n`), restartDelayMs = 1000 } = {}) {
  return new Promise((resolve) => {
    let child = null;
    let stopping = false;
    const forward = (sig) => { stopping = true; child?.kill(sig); };
    const onInt = () => forward('SIGINT');
    const onTerm = () => forward('SIGTERM');
    process.on('SIGINT', onInt);
    process.on('SIGTERM', onTerm);
    const start = () => {
      child = spawn(process.execPath, [bin, ...argv], { stdio: 'inherit', env: { ...env, [CHILD_ENV]: '1' } });
      child.on('exit', (code, sig) => {
        if (!stopping && code === RESTART_EXIT) {
          log('samoreštart: spúšťam budenie znova (nová revízia alebo wake.json)');
          setTimeout(() => (stopping ? done(0) : start()), restartDelayMs);
          return;
        }
        done(code ?? (sig ? 0 : 1));
      });
    };
    const done = (code) => {
      process.off('SIGINT', onInt);
      process.off('SIGTERM', onTerm);
      resolve(code);
    };
    start();
  });
}
