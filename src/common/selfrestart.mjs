// Self-restart after deployment: `serve` and `wake run` remember the revision of
// their installation (<repo>/.git/HEAD, resolved to a SHA when it is a ref) and the
// mtime of watched files; when either changes they exit cleanly with code 0 and
// systemd (Restart=always) starts the new version. Without .git nothing is checked.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const SELF_RESTART_INTERVAL_MS = 30000;
const DEFAULT_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');

/** Installation root: COMMS_INSTALL_ROOT (tests) or the repository holding this file. */
export function installRoot(env = process.env) {
  return env.COMMS_INSTALL_ROOT ? path.resolve(env.COMMS_INSTALL_ROOT) : DEFAULT_ROOT;
}

/** Off by --no-self-restart, COMMS_SELF_RESTART=0 or `self_restart: false` in a config. */
export function selfRestartEnabled(opts = {}, env = process.env, ...configs) {
  if (opts.noSelfRestart) return false;
  if (env.COMMS_SELF_RESTART != null && ['0', 'false', 'no', 'off'].includes(String(env.COMMS_SELF_RESTART).toLowerCase())) return false;
  return !configs.some((c) => c && c.self_restart === false);
}

function gitDir(root) {
  const p = path.join(root, '.git');
  let st;
  try { st = fs.statSync(p); } catch { return null; }
  if (st.isDirectory()) return p;
  // Worktree / submodule: `.git` is a file "gitdir: <path>".
  const m = /^gitdir:\s*(.+)$/m.exec(fs.readFileSync(p, 'utf8'));
  return m ? path.resolve(root, m[1].trim()) : null;
}

/** Revision string "<HEAD content>[@<sha>]" or null when there is no .git. */
export function readRevision(root) {
  const dir = gitDir(root);
  if (!dir) return null;
  let head;
  try { head = fs.readFileSync(path.join(dir, 'HEAD'), 'utf8').trim(); } catch { return null; }
  const m = /^ref:\s*(.+)$/.exec(head);
  if (!m) return head;
  const ref = m[1].trim();
  let commonDir = dir;
  try { commonDir = path.resolve(dir, fs.readFileSync(path.join(dir, 'commondir'), 'utf8').trim()); } catch {}
  for (const d of [dir, commonDir]) {
    try { return `${ref}@${fs.readFileSync(path.join(d, ref), 'utf8').trim()}`; } catch {}
  }
  try {
    const line = fs.readFileSync(path.join(commonDir, 'packed-refs'), 'utf8').split('\n').find((l) => l.endsWith(` ${ref}`));
    if (line) return `${ref}@${line.split(' ')[0]}`;
  } catch {}
  return `${ref}@?`;
}

const mtime = (f) => { try { return fs.statSync(f).mtimeMs; } catch { return null; } };

/**
 * Watch the installation. Calls onChange(reason) once and stops. Returns
 * { stop(), revision } or null when there is nothing to watch.
 */
export function watchInstall({ root = installRoot(), files = [], intervalMs, log = () => {}, onChange, env = process.env } = {}) {
  const every = Number(intervalMs || env.COMMS_SELF_RESTART_INTERVAL_MS || SELF_RESTART_INTERVAL_MS);
  const revision = readRevision(root);
  if (revision == null && !files.length) return null;
  const times = files.map(mtime);
  const timer = setInterval(() => {
    let reason = null;
    if (revision != null) {
      const now = readRevision(root);
      if (now !== revision) reason = `install changed ${revision} -> ${now}`;
    }
    if (!reason) {
      const i = files.findIndex((f, k) => mtime(f) !== times[k]);
      if (i >= 0) reason = `config changed ${files[i]}`;
    }
    if (!reason) return;
    clearInterval(timer);
    log(`${reason}, exiting for restart`);
    onChange(reason);
  }, every);
  timer.unref();
  return { stop: () => clearInterval(timer), revision };
}
