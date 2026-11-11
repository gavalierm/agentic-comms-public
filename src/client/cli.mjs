// CLI: `node bin/comms.mjs <command> [options]`. Output is one JSON object on
// stdout; errors are JSON on stderr. Exit codes: 0 ok, 1 rejected/usage,
// 2 comms unavailable, 3 instance mismatch.
import { Client, CommsError, resolveConfig, TURN_TIMEOUT } from './index.mjs';
import { parseDuration, readJson, LEVELS, REPLY_STATES, LEVEL_REMOVED_TEXT, MAX_TTL } from '../common/util.mjs';
import { Store } from '../server/store.mjs';
import { startServer, probe } from '../server/index.mjs';
import { socketPath } from '../common/paths.mjs';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { selfRestartEnabled, watchInstall } from '../common/selfrestart.mjs';
import { wakeStatus } from '../wake/lock.mjs';
import { resolveWakeConfig } from '../wake/index.mjs';

const WAKE_BIN = fileURLToPath(new URL('../../bin/wake.mjs', import.meta.url));

const USAGE = `Použitie: comms <príkaz> [voľby]

Služba:
  init [--instance <meno>] [--sender <meno>]      vytvorí inštanciu (dátový priečinok, instance_id, prvý kľúč)
  key add --sender <meno> | key list | key remove --sender <meno>
  config set-notify "<príkaz>" | config set-tcp <host:port> | config clear-tcp | config show
  serve [--tcp <host:port>] [--wake] [--no-self-restart]   spustí službu (lokálny socket / named pipe, voliteľne TCP); --wake spustí aj modul budenia
                                                  samoreštart: pri zmene revízie inštalácie (.git/HEAD, kontrola každých 30 s) skončí s kódom 0;
                                                  vypnutie --no-self-restart | COMMS_SELF_RESTART=0 | "self_restart": false (--config alebo instance.json)
  stop                                            zastaví bežiacu službu inštancie
  health                                          stav služby a instance_id

Správy (vyžadujú kľúč: --key, COMMS_KEY alebo "key" v --config):
  send --project <p> --topic <t> | --persona <meno> [--project <p> [--topic <t>]]
       --level blocker|steer|assist --text <text|-> [--from-project <p> --from-topic <t>] [--ttl <dĺžka>] [--cancels <id>]
  send --reply-to <id> --state hotovo|otázka|čakám [--wait-for <čo>] --text <text|->
                                                  adresa: projekt+téma, alebo persona (+projekt, +téma); persona bez
                                                  témy, ktorú vlastní, ide do hlavného vlákna persony (@<persona>/main)
                                                  --ttl: lehota na prevzatie správy (90s, 5m, 30m, sekundy; skracuje, nie predlžuje; blocker a steer najviac 5m, assist 30m)
                                                  predvolené ttl: blocker a steer 5m, assist 30m (instance.json ttl.<úroveň>). Odpoveď (--reply-to) nemá lehotu, úroveň zdedí
                                                  a nesie stav: hotovo je konečná (jedna na správu, po nej 409), otázka a čakám sú priebežné; bez --state platí hotovo.
                                                  Na odpoveď sa neodpovedá (400). --cancels <id>: nová správa ruší tvoju konečnú odpoveď <id>
  receive --project <p> --topic <t> | --persona <meno> | --thread <id> [--id <id> ...] [--ack]
                                                  čítanie tela = prevzatie (picked_at); --id číta iba zadané správy
  ack --id <id> [--id <id> ...]
  count --project <p> --topic <t> | --persona <meno> | --thread <id>  počet a ids čakajúcich správ (vždy živo; po výpadku nový pokus najskôr po 60 s)
  message --id <id>                               stav správy bez tela (state, to, outcome, replied, expires_at, ttl_s, undelivered_*)
  turn --state open|closed [--thread <id>] [--machine <stroj>] [--project <p> --topic <t>] [--holder-mode operator|auto]
       [--opened-at <iso>] [--next <krok>]        záznam ťahu vlákna pre keepalive; closed s --opened-at zatvorí iba ten ťah;
                                                  closed s --next: ďalší krok z handoveru, budenie vlákno zobudí na pokračovanie;
                                                  pri výpadku { ok:false, unavailable:true[, queued:true] }, kód 0 (closed ide do outboxu)
  notices [--all]
  decide --notice <id> --decision <id> [--target <project/topic>]
  register-topic --project <p> --topic <t> --subject <text> [--persona <meno>] [--visibility public|private]
                 [--holder <thread>] [--holder-mode operator|auto] [--holder-machine <stroj>] [--takeover]
  register-topic --persona <meno> --main [--holder <thread>] [--holder-machine <stroj>] [--takeover]   hlavné vlákno persony
  register-topic --project <p> --topic <t> --release [--holder <thread>]   uvoľní držiteľa (bez --holder alebo vlastného);
                 cudzí držiteľ sa mení iba s --takeover; zmena držiteľa bez --takeover je odmietnutá (409)
  register-topic --project <p> --topic <t> --close [--holder <thread>]   uzavrie tému (dokončená, archivovaná): nové správy 410,
                 čakajúce sa vrátia odosielateľom (undelivered); nová registrácia bez --close ju znovu otvorí
  events [--since <n>|latest] [--wait <ms>]       udalosti budenia (long-poll)
  flush                                           odošle odchádzajúcu schránku (outbox)

Spoločné voľby: --instance <meno> | --data-dir <cesta> | --config <súbor.json> | --url <http://host:port>
                --key <kľúč> | --thread <id> | --machine <stroj> | --outbox <cesta> | --timeout <ms> | --no-start
`;

const FLAGS_WITH_VALUE = new Set(['instance', 'data-dir', 'config', 'url', 'key', 'thread', 'outbox', 'timeout', 'client-dir', 'instance-id',
  'sender', 'tcp', 'project', 'topic', 'level', 'text', 'reply-to', 'wait-for', 'cancels', 'from-project', 'from-topic', 'id', 'notice', 'decision', 'target',
  'subject', 'holder', 'holder-mode', 'since', 'wait', 'persona', 'visibility', 'holder-machine', 'machine', 'ttl', 'state', 'opened-at', 'next', 'resume-at']);
const BOOL_FLAGS = new Set(['no-start', 'all', 'ack', 'help', 'json', 'wake', 'main', 'no-self-restart', 'takeover', 'release', 'close']);

export function parseArgs(argv) {
  const out = { _: [], id: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) { out._.push(a); continue; }
    const eq = a.indexOf('=');
    const name = eq > 0 ? a.slice(2, eq) : a.slice(2);
    if (BOOL_FLAGS.has(name)) { out[camel(name)] = true; continue; }
    if (!FLAGS_WITH_VALUE.has(name)) throw new CommsError('USAGE', `neznáma voľba --${name}`);
    const value = eq > 0 ? a.slice(eq + 1) : argv[++i];
    if (value === undefined) throw new CommsError('USAGE', `voľba --${name} vyžaduje hodnotu`);
    if (name === 'id') out.id.push(value); else out[camel(name)] = value;
  }
  return out;
}

const camel = (s) => s.replace(/-([a-z])/g, (_, c) => c.toUpperCase());

export async function main(argv, { stdout = process.stdout, stderr = process.stderr, stdin = process.stdin } = {}) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (e) {
    stderr.write(JSON.stringify({ error: e.message }) + '\n' + USAGE);
    return 1;
  }
  const cmd = args._[0];
  if (!cmd || args.help) { stdout.write(USAGE); return cmd ? 0 : 1; }
  const cfg = resolveConfig(args);

  try {
    const result = await run(cmd, args, cfg, { stdout, stderr, stdin });
    if (result !== undefined) stdout.write(JSON.stringify(result, null, 2) + '\n');
    return 0;
  } catch (e) {
    const code = e.code === 'UNAVAILABLE' ? 2 : e.code === 'INSTANCE_MISMATCH' ? 3 : 1;
    stderr.write(JSON.stringify({ error: e.message, code: e.code || 'ERROR', ...(e.status ? { status: e.status } : {}), ...(e.data ? { details: e.data } : {}) }) + '\n');
    if (e.code === 'USAGE') stderr.write(USAGE);
    return code;
  }
}

async function run(cmd, args, cfg, io) {
  const store = new Store(cfg.dataDir);
  const need = (...names) => {
    for (const n of names) if (args[n] == null || args[n] === '') throw new CommsError('USAGE', `chýba voľba --${n.replace(/[A-Z]/g, (c) => '-' + c.toLowerCase())}`);
  };

  switch (cmd) {
    case 'init': {
      const r = store.init({ instance: cfg.instance, sender: args.sender });
      return {
        created: r.created, instance: r.instance.instance, instance_id: r.instance.instance_id, data_dir: cfg.dataDir,
        socket: cfg.socket, ...(r.firstKey ? { sender: r.firstKey.sender, key: r.firstKey.key } : { note: 'inštancia už existovala; kľúče: comms key list' }),
      };
    }
    case 'key': {
      requireInstance(store, cfg);
      const sub = args._[1];
      if (sub === 'add') { need('sender'); const k = store.addKey(args.sender); return { sender: k.sender, key: k.key }; }
      if (sub === 'list') return { keys: store.keys() };
      if (sub === 'remove') {
        need('sender');
        if (!store.removeKey(args.sender)) throw new CommsError('NOT_FOUND', `kľúč pre odosielateľa „${args.sender}“ neexistuje`);
        return { removed: true, sender: args.sender };
      }
      throw new CommsError('USAGE', 'key add|list|remove');
    }
    case 'config': {
      requireInstance(store, cfg);
      const inst = store.instance();
      const sub = args._[1];
      if (sub === 'show') return inst;
      if (sub === 'set-notify') {
        if (args._[2] === undefined) throw new CommsError('USAGE', 'config set-notify vyžaduje príkaz v úvodzovkách');
        inst.notify = { ...(inst.notify || {}), command: args._[2] || null };
      }
      else if (sub === 'set-tcp') { inst.tcp = parseTcp(args._[2]); }
      else if (sub === 'clear-tcp') { inst.tcp = null; }
      else throw new CommsError('USAGE', 'config show|set-notify|set-tcp|clear-tcp');
      store.saveInstance(inst);
      return { saved: true, notify: inst.notify, tcp: inst.tcp, note: 'zmena platí po reštarte služby (comms stop; comms serve)' };
    }
    case 'serve': {
      requireInstance(store, cfg);
      const log = (m) => io.stderr.write(`[comms ${new Date().toISOString()}] ${m}\n`);
      let srv;
      try {
        srv = await startServer({ dataDir: cfg.dataDir, tcp: args.tcp ? parseTcp(args.tcp) : null, log });
      } catch (e) {
        if (e.code === 'ALREADY_RUNNING') {
          // The running service keeps its own options: a requested --wake/--tcp is not started, and that is an error.
          if (args.wake || args.tcp) throw new CommsError('ALREADY_RUNNING', 'služba už beží; --wake a --tcp sa NESPUSTILI (zastavte službu: comms stop)');
          io.stdout.write(JSON.stringify({ already_running: true, ...(e.health || {}) }) + '\n');
          return undefined;
        }
        throw e;
      }
      let wake = null;
      if (args.wake) {
        // Optional: run the wake module (P-043) next to the service; it stays a separate process.
        // The child does not self-restart; this process watches for it (revision and wake.json).
        // Mail options are forwarded so that the child talks to the same instance and key as this process.
        const forward = [];
        for (const name of ['config', 'machine', 'instance', 'key']) if (args[name] != null) forward.push(`--${name}`, String(args[name]));
        wake = spawn(process.execPath, [WAKE_BIN, 'run', '--data-dir', cfg.dataDir, ...forward, '--no-self-restart'], { stdio: ['ignore', 'ignore', 'inherit'], windowsHide: true });
        wake.on('exit', (code) => { log(`wake skončil (${code})`); wake = null; });
      }
      const stop = () => { if (wake) wake.kill('SIGTERM'); srv.close().then(() => process.exit(0)); };
      const cfgPath = args.config || process.env.COMMS_CONFIG || null;
      if (selfRestartEnabled(args, process.env, cfgPath ? readJson(path.resolve(cfgPath), {}) : null, store.instance())) {
        const files = args.wake && !cfgPath ? [path.join(cfg.dataDir, 'wake.json')] : [];
        const watch = watchInstall({ files, log, onChange: stop });
        log(watch ? `samoreštart: revízia ${watch.revision ?? 'bez .git'}` : 'samoreštart: bez .git, nekontroluje sa');
      }
      process.on('SIGINT', stop);
      process.on('SIGTERM', stop);
      await new Promise(() => {}); // run until signalled
      return undefined;
    }
    case 'stop': {
      requireInstance(store, cfg);
      const info = store.serverInfo();
      const h = await probe(socketPath(cfg.dataDir));
      if (!h) { store.clearServerInfo(); return { stopped: false, note: 'služba nebeží' }; }
      const pid = info ? info.pid : h.pid;
      try { process.kill(pid, 'SIGTERM'); } catch (e) { throw new CommsError('ERROR', `nepodarilo sa zastaviť proces ${pid}: ${e.message}`); }
      for (let i = 0; i < 40 && await probe(socketPath(cfg.dataDir)); i++) await new Promise((r) => setTimeout(r, 50));
      return { stopped: true, pid };
    }
  }

  // Everything below talks to the service.
  const client = new Client(cfg);
  switch (cmd) {
    case 'health':
      return client.ensureRunning();
    case 'send': {
      need('text');
      // Level is required, except for a reply (it inherits the level of the message it answers).
      if (args.level === 'info') throw new CommsError('REJECTED', LEVEL_REMOVED_TEXT, { status: 400 });
      if (args.level == null && !args.replyTo) throw new CommsError('USAGE', `chýba voľba --level (${LEVELS.join('|')})`);
      if (args.level != null && !LEVELS.includes(args.level)) throw new CommsError('REJECTED', `neznáma úroveň „${args.level}“; platné úrovne: ${LEVELS.join('|')}`, { status: 400 });
      if (!args.replyTo && !args.persona) need('project', 'topic');
      if (args.state != null && !args.replyTo) throw new CommsError('USAGE', '--state patrí iba odpovedi (--reply-to)');
      if (args.state != null && !REPLY_STATES.includes(args.state)) throw new CommsError('REJECTED', `neznámy stav odpovede „${args.state}“; platné stavy: ${REPLY_STATES.join('|')}`, { status: 400 });
      if (args.cancels != null && args.replyTo) throw new CommsError('USAGE', '--cancels patrí novej správe s úrovňou a adresou, nie odpovedi (--reply-to)');
      if (args.ttl != null && !args.replyTo) {
        // Checked here too, so that a message queued during an outage is not rejected later (the server checks the same).
        const s = parseDuration(args.ttl);
        if (s == null) throw new CommsError('REJECTED', `neplatná lehota (ttl) „${args.ttl}“: napr. 90s, 5m, 30m alebo počet sekúnd`, { status: 400 });
        if (s <= 0) throw new CommsError('REJECTED', `lehota (ttl) musí byť kladná; ${args.level} najviac ${MAX_TTL[args.level]}`, { status: 400 });
        if (s > parseDuration(MAX_TTL[args.level])) throw new CommsError('REJECTED', `lehota (ttl) „${args.ttl}“ je dlhšia ako maximum ${MAX_TTL[args.level]} pre ${args.level}`, { status: 400 });
      }
      let text = args.text;
      if (text === '-') text = await readAll(io.stdin);
      return client.sendOrQueue({
        project: args.project || null, topic: args.topic || null, persona: args.persona || null, level: args.level, text,
        reply_to: args.replyTo || null, from_project: args.fromProject || null, from_topic: args.fromTopic || null,
        ...(args.ttl != null ? { ttl: args.ttl } : {}),
        ...(args.replyTo && args.state != null ? { reply_state: args.state } : {}),
        ...(args.replyTo && args.waitFor != null ? { wait_for: args.waitFor } : {}),
        ...(args.cancels != null ? { cancels: args.cancels } : {}),
      });
    }
    case 'message':
      if (!args.id.length) throw new CommsError('USAGE', 'chýba voľba --id');
      if (args.id.length > 1) throw new CommsError('USAGE', 'message zisťuje stav jednej správy: zadajte jedno --id');
      return client.message(args.id[0]);
    case 'turn': {
      need('state');
      const thread = args.thread || cfg.threadId;
      if (!thread) throw new CommsError('USAGE', 'chýba voľba --thread (alebo COMMS_THREAD_ID / T3CODE_THREAD_ID)');
      // Short deadline; on outage only `closed` is queued (the service drops it when `opened_at` no longer matches).
      const quick = new Client({ ...cfg, timeout: Math.min(cfg.timeout, TURN_TIMEOUT) });
      return quick.turn({
        thread, state: args.state, machine: cfg.machine || null, project: args.project || null, topic: args.topic || null,
        holder_mode: args.holderMode || null, ...(args.openedAt ? { opened_at: args.openedAt } : {}), ...(args.next ? { next: args.next } : {}), ...(args.resumeAt ? { resume_at: args.resumeAt } : {}),
      });
    }
    case 'receive': {
      if (!args.thread && !args.persona) need('project', 'topic');
      await client.ensureRunning();
      try { await client.flushOutbox(); } catch { /* outbox stays, reported by flush */ }
      // `--id` limits the read (and so the pickup) to the named messages.
      const r = await client.receive({ project: args.project, topic: args.topic, thread: args.thread, persona: args.persona, ids: args.id.length ? args.id.join(',') : null });
      if (args.ack && r.messages.length) await client.ack(r.messages.map((m) => m.id));
      return r;
    }
    case 'ack':
      if (!args.id.length) throw new CommsError('USAGE', 'zadaj aspoň jedno --id');
      return client.ack(args.id);
    case 'count':
      if (!args.thread && !args.persona) need('project', 'topic');
    {
      const r = await client.count({ project: args.project, topic: args.topic, thread: args.thread, persona: args.persona });
      // comms-public#4: whether this machine's wake runs, for the session status line (null: wake never ran here).
      // The state dir as `wake run` resolves it: workflow passes its own --client-dir (outbox), wake does not.
      const wake = wakeStatus(wakeStateDir(args));
      return wake ? { ...r, wake } : r;
    }
    case 'notices':
      return client.notices(!!args.all);
    case 'decide':
      need('notice', 'decision');
      return client.decide({ notice_id: args.notice, decision: args.decision, target: args.target || null });
    case 'register-topic':
      if (args.main) need('persona'); else if (args.release || args.close) need('project', 'topic'); else need('project', 'topic', 'subject');
      return client.registerTopic({
        project: args.main ? null : args.project, topic: args.main ? null : args.topic, subject: args.subject || null,
        persona: args.persona || null, visibility: args.visibility || null, main: !!args.main,
        holder: args.holder || null, holder_mode: args.holderMode || null,
        takeover: !!args.takeover, release: !!args.release, close: !!args.close,
        // The holder runs on this machine unless told otherwise (wake rings only on the holder's machine).
        holder_machine: args.holder ? (args.holderMachine || cfg.machine || null) : null,
      });
    case 'events':
      return client.events(args.since === 'latest' ? 'latest' : Number(args.since || 0), Number(args.wait || 0));
    case 'flush': {
      await client.ensureRunning();
      return client.flushOutbox();
    }
    default:
      throw new CommsError('USAGE', `neznámy príkaz „${cmd}“`);
  }
}

function requireInstance(store, cfg) {
  if (!store.exists()) throw new CommsError('USAGE', `inštancia „${cfg.instance}“ v ${cfg.dataDir} neexistuje; spusťte „comms init --instance ${cfg.instance}“`);
}

function parseTcp(s) {
  const m = /^(?:(.*):)?(\d+)$/.exec(String(s || ''));
  if (!m) throw new CommsError('USAGE', 'TCP adresa musí byť host:port alebo port');
  return { host: m[1] || '127.0.0.1', port: Number(m[2]) };
}

function readAll(stream) {
  return new Promise((resolve, reject) => {
    let s = '';
    stream.setEncoding('utf8');
    stream.on('data', (d) => { s += d; });
    stream.on('end', () => resolve(s));
    stream.on('error', reject);
  });
}

function wakeStateDir(args) {
  const { clientDir: _own, outbox: _outbox, ...opts } = args;
  try { return resolveWakeConfig(opts).stateDir; } catch { return null; }
}
