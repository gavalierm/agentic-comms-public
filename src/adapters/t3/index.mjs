// Wake adapter `t3`: rings a thread through the T3 CLI, `t3 thread send <threadId> <text>`
// (P-043). The command is configurable (`wake.command`, WAKE_COMMAND, --command) so
// tests and other machines may substitute it; the thread id and the fixed template
// are always the last two arguments. Without the CLI nothing is woken: the message
// waits in the mailbox for the next turn.
// `status(threadId)` (keepalive) runs `t3 thread show --json <threadId>` (configurable
// as `wake.status_command`) and reads `thread.status` and `thread.activeRunId`.
import { spawn } from 'node:child_process';

export const DEFAULT_T3_COMMAND = ['t3', 'thread', 'send'];
export const DEFAULT_T3_STATUS_COMMAND = ['t3', 'thread', 'show', '--json'];
const DEFAULT_TIMEOUT = 20_000;

/** Split a command string into argv (quotes supported) or pass an array through. */
export function parseCommand(command) {
  if (Array.isArray(command)) return command.map(String).filter(Boolean);
  const out = [];
  const re = /"([^"]*)"|'([^']*)'|(\S+)/g;
  let m;
  while ((m = re.exec(String(command || '')))) out.push(m[1] ?? m[2] ?? m[3]);
  return out;
}

/** Run argv without a shell; resolves { ok, code, stdout, stderr }, never throws. */
export function runCommand(argv, { timeout = DEFAULT_TIMEOUT, env = process.env } = {}) {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn(argv[0], argv.slice(1), { stdio: ['ignore', 'pipe', 'pipe'], env, windowsHide: true });
    } catch (e) {
      return resolve({ ok: false, code: null, stdout: '', stderr: e.message });
    }
    let stdout = '';
    let stderr = '';
    let settled = false;
    const finish = (r) => { if (!settled) { settled = true; clearTimeout(timer); resolve(r); } };
    const timer = setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* gone */ } finish({ ok: false, code: null, stdout, stderr: `${stderr}\nčasový limit ${timeout} ms`.trim() }); }, timeout);
    child.stdout.on('data', (d) => { stdout += d; });
    child.stderr.on('data', (d) => { stderr += d; });
    child.on('error', (e) => finish({ ok: false, code: null, stdout, stderr: e.message }));
    child.on('close', (code) => finish({ ok: code === 0, code, stdout: stdout.trim(), stderr: stderr.trim() }));
  });
}

export function createT3Adapter({ command = DEFAULT_T3_COMMAND, statusCommand = DEFAULT_T3_STATUS_COMMAND, timeout = DEFAULT_TIMEOUT, env = process.env } = {}) {
  const argv = parseCommand(command);
  if (!argv.length) throw new Error('adaptér t3: prázdny príkaz');
  const statusArgv = parseCommand(statusCommand);
  return {
    name: 't3',
    command: argv,
    statusCommand: statusArgv,
    /** Resolves { ok, code, stdout, stderr }; never throws. */
    ring(threadId, text) {
      return runCommand([...argv, String(threadId), String(text)], { timeout, env });
    },
    /**
     * Whether the thread runs a turn now. T3 does not mark a run cut by a restart
     * (`status: completed`, `activeRunId: null`), so `running` = active run present.
     * Resolves { ok, running, status, error? }; never throws.
     */
    async status(threadId) {
      if (!statusArgv.length) return { ok: false, running: false, status: null, error: 'prázdny príkaz stavu' };
      const r = await runCommand([...statusArgv, String(threadId)], { timeout, env });
      if (!r.ok) return { ok: false, running: false, status: null, error: r.stderr || `kód ${r.code}` };
      let j;
      try { j = JSON.parse(r.stdout); } catch { return { ok: false, running: false, status: null, error: 'výstup nie je JSON' }; }
      // Exit 0 without a thread object is an unknown state, never "not running" (keepalive would ring a busy thread).
      if (!j || typeof j.thread !== 'object' || j.thread === null) return { ok: false, running: null, status: null, error: 'výstup bez objektu thread' };
      const th = j.thread;
      return { ok: true, running: th.activeRunId != null && th.activeRunId !== '', status: th.status ?? null };
    },
  };
}
