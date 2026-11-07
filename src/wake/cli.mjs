// CLI: `node bin/wake.mjs run [options]`. Long-running process; logs one line per
// event on stderr; stops on SIGINT/SIGTERM. `status` prints the persisted state.
import { CommsError } from '../client/index.mjs';
import { watchInstall } from '../common/selfrestart.mjs';
import { RESTART_EXIT, CHILD_ENV } from './supervise.mjs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { acquireWakeLock, readWakeLock, lockHeld } from './lock.mjs';
import { installPlan, formatPlan, applyPlan } from './install.mjs';
import { baseDir } from '../common/paths.mjs';
import { resolveWakeConfig, createWaker, WakeState, WAKE_TEMPLATE, KEEPALIVE_TEMPLATE } from './index.mjs';

const USAGE = `Použitie: wake <príkaz> [voľby]

  run       trvalý proces: odoberá udalosti pošty a budí držiteľov cez adaptér
  status    vypíše stav modulu (či beží: pid a posledný heartbeat; kurzor, zobudenia za hodinu)
  config    vypíše výslednú konfiguráciu (bez kľúča)
  install   trvalý beh pre prihláseného používateľa: Linux systemd --user, macOS LaunchAgent, Windows Plánovač úloh
            (voľby ako run; --print iba vypíše súbory a príkazy, --remove registráciu odstráni)

Voľby pošty (ako comms): --instance <meno> | --data-dir <cesta> | --config <súbor.json> | --url <http://host:port>
                         --key <kľúč> | --instance-id <id> | --timeout <ms> | --no-start
Voľby budenia:           --adapter t3|none (predvolené t3) | --command "<príkaz>" (predvolené "t3 thread send")
                         --limit-per-hour <n> (6) | --stop-on-repeat <n> (2) | --max-event-age-ms <ms> (3600000)
                         --wait-ms <ms> (25000) | --reconnect-ms <ms> (1000) | --state-dir <cesta>
                         --machine <stroj> (predvolené názov tohto stroja): budia sa iba držitelia na tomto stroji
Keepalive (vypnutý, kým nie je zadaný príkaz štartu T3):
                         --keepalive-since-command "<príkaz>": vypíše čas štartu servera T3 (ISO, epoch, formát systemctl)
                         --keepalive-interval-ms <ms> (60000) | --status-command "<príkaz>" (predvolené "t3 thread show --json")
Premenné: WAKE_MACHINE, WAKE_ADAPTER, WAKE_COMMAND, WAKE_STATUS_COMMAND, WAKE_STATE_DIR, WAKE_LIMIT_PER_HOUR, WAKE_STOP_ON_REPEAT, WAKE_MAX_EVENT_AGE_MS,
          WAKE_WAIT_MS, WAKE_RECONNECT_MS, WAKE_KEEPALIVE_SINCE_COMMAND, WAKE_KEEPALIVE_INTERVAL_MS
Súbor --config: kľúče pošty (instance, data_dir, url, key, instance_id, client_dir, machine) a "wake": { machine, adapter, command, status_command,
          limit_per_hour, stop_on_repeat, max_event_age_ms, state_dir, wait_ms, reconnect_ms, keepalive: { since_command, interval_ms } }
Bez --config/COMMS_CONFIG sa sekcia "wake" číta z <data-dir>/wake.json (rovnaké kľúče, bez obalu "wake"), iba pri štarte.
Samoreštart: run sa každých 30 s pozrie na revíziu inštalácie (.git/HEAD) a mtime wake.json; pri zmene skončí s kódom 0.
          Vypnutie: --no-self-restart | COMMS_SELF_RESTART=0 | "self_restart": false

Budiaca správa je vždy „${WAKE_TEMPLATE} <project>/<topic>“, nikdy telo správy (P-043); keepalive „${KEEPALIVE_TEMPLATE}“.
`;

const FLAGS_WITH_VALUE = new Set(['instance', 'data-dir', 'config', 'url', 'key', 'instance-id', 'timeout', 'client-dir',
  'machine', 'adapter', 'command', 'limit-per-hour', 'stop-on-repeat', 'max-event-age-ms', 'wait-ms', 'reconnect-ms', 'state-dir',
  'status-command', 'keepalive-since-command', 'keepalive-interval-ms']);
const BOOL_FLAGS = new Set(['no-start', 'help', 'no-self-restart', 'print', 'remove']);

export function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) { out._.push(a); continue; }
    const eq = a.indexOf('=');
    const name = eq > 0 ? a.slice(2, eq) : a.slice(2);
    if (BOOL_FLAGS.has(name)) { out[camel(name)] = true; continue; }
    if (!FLAGS_WITH_VALUE.has(name)) throw new CommsError('USAGE', `neznáma voľba --${name}`);
    const value = eq > 0 ? a.slice(eq + 1) : argv[++i];
    if (value === undefined) throw new CommsError('USAGE', `voľba --${name} vyžaduje hodnotu`);
    out[camel(name)] = value;
  }
  return out;
}

const camel = (s) => s.replace(/-([a-z])/g, (_, c) => c.toUpperCase());

export async function main(argv, { stdout = process.stdout, stderr = process.stderr, signal = null } = {}) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (e) {
    stderr.write(JSON.stringify({ error: e.message }) + '\n' + USAGE);
    return 1;
  }
  const cmd = args._[0];
  if (!cmd || args.help) { stdout.write(USAGE); return cmd ? 0 : 1; }
  const log = (m) => stderr.write(`[wake ${new Date().toISOString()}] ${m}\n`);

  try {
    const cfg = resolveWakeConfig(args);
    switch (cmd) {
      case 'config': {
        const { comms, ...rest } = cfg;
        stdout.write(JSON.stringify({ ...rest, comms: { ...comms, key: comms.key ? '***' : null } }, null, 2) + '\n');
        return 0;
      }
      case 'status': {
        const st = new WakeState(cfg.stateFile);
        st.prune(Date.now());
        const rings = Object.fromEntries(Object.entries(st.rings).map(([k, v]) => [k, v.length]));
        const keepalive = Object.fromEntries(Object.entries(st.keepalive).map(([k, v]) => [k, v.length]));
        const lock = readWakeLock(cfg.stateDir);
        const running = lockHeld(lock, { pid: -1 }) ? { pid: lock.pid, started_at: lock.started_at, heartbeat_at: lock.heartbeat_at } : null;
        stdout.write(JSON.stringify({ running, state_file: cfg.stateFile, cursor: st.cursor, updated_at: st.updated_at, rings_last_hour: rings, keepalive_attempts_last_hour: keepalive, adapter: cfg.adapter, command: cfg.command, keepalive_since_command: cfg.keepaliveSinceCommand }, null, 2) + '\n');
        return 0;
      }
      case 'run': {
        const waker = createWaker(cfg, { log });
        const ac = new AbortController();
        const stop = (sig) => { log(`${sig}: končím`); ac.abort(); };
        process.on('SIGINT', () => stop('SIGINT'));
        process.on('SIGTERM', () => stop('SIGTERM'));
        if (signal) signal.addEventListener('abort', () => ac.abort(), { once: true });
        log(`stroj ${cfg.machine}, adaptér ${waker.adapter ? `${waker.adapter.name} (${waker.adapter.command.join(' ')})` : 'žiadny (nič sa nebudí)'}, limit ${cfg.limitPerHour}/h, stop pri ${cfg.stopOnRepeat} rovnakých, stav ${cfg.stateFile}`);
        log(`pošta: ${cfg.comms.url || cfg.comms.socket}`);
        log(cfg.keepaliveSinceCommand ? `keepalive: každých ${cfg.keepaliveIntervalMs} ms, štart T3 z „${cfg.keepaliveSinceCommand}“` : 'keepalive vypnutý (bez keepalive.since_command)');
        // Self-restart: a new installation revision or a changed wake.json ends the process: with 0 under systemd
        // (Restart=always), with RESTART_EXIT under the supervisor of bin/wake.mjs (terminal, macOS, Windows).
        let restart = false;
        const watch = cfg.selfRestart ? watchInstall({ files: cfg.wakeFile ? [cfg.wakeFile] : [], log, onChange: () => { restart = true; ac.abort(); } }) : null;
        log(watch ? `samoreštart: revízia ${watch.revision ?? 'bez .git'}${cfg.wakeFile ? `, sleduje ${cfg.wakeFile}` : ''}` : 'samoreštart vypnutý');
        // One wake per instance (comms-public#1): a second `run` waits until the first one ends.
        const lock = await acquireWakeLock(cfg.stateDir, { log, signal: ac.signal, ...(Number(process.env.WAKE_LOCK_POLL_MS) > 0 ? { pollMs: Number(process.env.WAKE_LOCK_POLL_MS) } : {}) });
        const stats = lock ? await waker.run({ signal: ac.signal }) : { lock: 'neprevzatý' };
        lock?.release();
        watch?.stop();
        stdout.write(JSON.stringify({ stopped: true, ...stats }) + '\n');
        return restart && process.env[CHILD_ENV] ? RESTART_EXIT : 0;
      }
      case 'install': {
        // comms-public#4: one command per system instead of a hand recipe. The service runs `wake run` with the same options.
        const runArgs = argv.slice(argv.indexOf('install') + 1).filter((a) => a !== '--print' && a !== '--remove');
        if (runArgs.some((a) => a === '--key' || a.startsWith('--key='))) throw new CommsError('USAGE', 'kľúč nepatrí do súboru služby: použite --config <súbor.json> alebo keys.json inštancie');
        if (!['--instance', '--data-dir', '--config', '--url'].some((f) => runArgs.some((a) => a === f || a.startsWith(`${f}=`)))) runArgs.unshift('--instance', cfg.comms.instance);
        const plan = installPlan({
          platform: process.env.WAKE_INSTALL_PLATFORM || process.platform, instance: cfg.comms.instance, runArgs, remove: !!args.remove,
          node: process.execPath, bin: fileURLToPath(new URL('../../bin/wake.mjs', import.meta.url)),
          home: os.homedir(), base: baseDir(), env: process.env, uid: process.getuid?.() ?? 0,
        });
        if (args.print) { stdout.write(formatPlan(plan)); return 0; }
        const res = applyPlan(plan, { stopWake: () => stopRunningWake(cfg.stateDir, log) });
        stdout.write(JSON.stringify({ [args.remove ? 'removed' : 'installed']: res.ok, platform: plan.platform, files: res.ok ? plan.files.map((f) => f.path) : [], removed_files: res.ok ? plan.remove : [], notes: plan.notes, errors: res.errors }, null, 2) + '\n');
        return res.ok ? 0 : 1;
      }
      default:
        throw new CommsError('USAGE', `neznámy príkaz „${cmd}“`);
    }
  } catch (e) {
    const code = e.code === 'UNAVAILABLE' ? 2 : e.code === 'INSTANCE_MISMATCH' ? 3 : 1;
    stderr.write(JSON.stringify({ error: e.message, code: e.code || 'ERROR' }) + '\n');
    if (e.code === 'USAGE') stderr.write(USAGE);
    return code;
  }
}

/** End the wake that holds this instance's lock (Windows install: the old task's node), wait until it is gone. */
function stopRunningWake(stateDir, log) {
  const lock = readWakeLock(stateDir);
  if (!lockHeld(lock, { pid: -1 })) return;
  try { process.kill(lock.pid, 'SIGTERM'); } catch { return; }
  const alive = () => { try { process.kill(lock.pid, 0); return true; } catch { return false; } };
  const until = Date.now() + 10000;
  while (alive() && Date.now() < until) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
  log(`bežiace budenie (pid ${lock.pid}) ${alive() ? 'sa neukončilo do 10 s' : 'ukončené'}`);
}
