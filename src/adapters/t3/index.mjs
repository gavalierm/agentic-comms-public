// Wake adapter `t3`: rings a thread through the T3 CLI, `t3 thread send <threadId> <text>`
// (P-043). The command is configurable (`wake.command`, WAKE_COMMAND, --command) so
// tests and other machines may substitute it; the thread id and the fixed template
// are always the last two arguments. Without the CLI nothing is woken: the message
// waits in the mailbox for the next turn.
// `status(threadId)` (keepalive) runs `t3 thread show --json <threadId>` (configurable
// as `wake.status_command`) and reads `thread.status` and `thread.activeRunId`.
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

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

/**
 * Windows (issue comms-public#2): T3 installs its CLI as `%USERPROFILE%\\.t3\\bin\\t3.cmd` and does not add it to PATH
 * (threads get only T3CODE_CLI_PATH); Node finds no `.cmd` without a shell (ENOENT) and refuses to spawn `.cmd`/`.bat`
 * without one (EINVAL, CVE-2024-27980). On win32 a bare `t3` resolves to T3CODE_CLI_PATH or that file, and a `.cmd`/`.bat`
 * runs through `cmd.exe /d /s /c` with every argument quoted; an argument cmd could expand (`"`, `%`, a line break) is
 * refused. Other platforms get argv unchanged.
 * @returns {{ file: string, args: string[], verbatim: boolean } | { error: string }}
 */
export function platformCommand(argv, { platform = process.platform, env = process.env, exists = fs.existsSync } = {}) {
  if (platform !== 'win32') return { file: argv[0], args: argv.slice(1), verbatim: false };
  let file = argv[0];
  if (file === 't3') {
    const home = env.USERPROFILE || env.HOME || '';
    const found = [env.T3CODE_CLI_PATH, home && path.win32.join(home, '.t3', 'bin', 't3.cmd')].find((f) => f && exists(f));
    if (found) file = found;
  }
  if (!/\.(cmd|bat)$/i.test(file)) return { file, args: argv.slice(1), verbatim: false };
  const all = [file, ...argv.slice(1)];
  const bad = all.find((a) => /["%\r\n]/.test(a));
  if (bad !== undefined) return { error: `argument pre ${path.win32.basename(file)} obsahuje znak, ktorý cmd.exe rozvinie: ${JSON.stringify(bad).slice(0, 80)}` };
  return { file: env.ComSpec || 'cmd.exe', args: ['/d', '/s', '/c', `"${all.map((a) => `"${a}"`).join(' ')}"`], verbatim: true };
}

/** Run argv without a shell (on Windows a .cmd through cmd.exe); resolves { ok, code, stdout, stderr }, never throws. */
export function runCommand(argv, { timeout = DEFAULT_TIMEOUT, env = process.env, platform = process.platform } = {}) {
  return new Promise((resolve) => {
    let child;
    const cmd = platformCommand(argv, { platform, env });
    if (cmd.error) return resolve({ ok: false, code: null, stdout: '', stderr: cmd.error });
    try {
      child = spawn(cmd.file, cmd.args, { stdio: ['ignore', 'pipe', 'pipe'], env, windowsHide: true, windowsVerbatimArguments: cmd.verbatim });
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
