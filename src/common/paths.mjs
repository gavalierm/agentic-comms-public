// Shared path resolution for server and client. No absolute paths hard-coded:
// everything derives from the user's home directory or explicit overrides.
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';

export const DEFAULT_INSTANCE = 'default';

/** Base directory holding all instances: <base>/<instance>/ */
export function baseDir(env = process.env) {
  if (env.COMMS_BASE_DIR) return env.COMMS_BASE_DIR;
  const home = os.homedir();
  switch (process.platform) {
    case 'win32':
      return path.join(env.LOCALAPPDATA || path.join(home, 'AppData', 'Local'), 'comms');
    case 'darwin':
      return path.join(home, 'Library', 'Application Support', 'comms');
    default:
      return path.join(env.XDG_DATA_HOME || path.join(home, '.local', 'share'), 'comms');
  }
}

export function instanceName(opts = {}, env = process.env) {
  return opts.instance || env.COMMS_INSTANCE || DEFAULT_INSTANCE;
}

/** Data directory of one instance (override: --data-dir / COMMS_DATA_DIR). */
export function dataDir(opts = {}, env = process.env) {
  if (opts.dataDir) return path.resolve(opts.dataDir);
  if (env.COMMS_DATA_DIR) return path.resolve(env.COMMS_DATA_DIR);
  return path.join(baseDir(env), instanceName(opts, env));
}

/** Local endpoint: unix socket inside the data dir, named pipe on Windows. */
export function socketPath(dir) {
  if (process.platform === 'win32') {
    const h = createHash('sha1').update(path.resolve(dir).toLowerCase()).digest('hex').slice(0, 16);
    return `\\\\.\\pipe\\comms-${h}`;
  }
  return path.join(dir, 'comms.sock');
}

export function clientDir(dir) {
  return path.join(dir, 'client');
}
