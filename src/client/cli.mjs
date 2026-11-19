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

Správy (vyžadujú kľúč: --key, COMMS_KEY alebo "key" v --config; model pošty 3, iný model: kód 3, nič sa nezapíše):
  Model 3: téma je brain + stroj + projekt + téma. Brain a stroj klienta: --brain <id> --brain-name <meno> --machine <stroj>
  (inak COMMS_BRAIN, COMMS_BRAIN_NAME, COMMS_MACHINE alebo "brain", "brain_name", "machine" v --config); posielajú sa ako
  X-Comms-Brain, X-Comms-Brain-Name, X-Comms-Machine a sú schránkou príkazov receive, count, obligations (bez brainu iba vlákno).
  Adresa témy: [<brain>[@<stroj>]:]<projekt>/<téma> (brain = ID alebo jednoznačné meno brainu); bez brainu vlastný brain,
  inak jediná otvorená téma iného brainu (potvrdenie nesie resolved: <meno>@<stroj>), viac → 409 s ambiguous.
  send --to <adresa> | --project <p> --topic <t> | --persona <meno> [--project <p> [--topic <t>]] | --maintainer | --to-thread <id>
       --level blocker|steer|assist --text <text|-> [--from-project <p> --from-topic <t>] [--ttl <dĺžka>] [--supersede-key <kľúč>]
                                                  nová správa (odpoveď ide cez resolve); potvrdenie { id, status, wait_due_at }
                                                  adresa: projekt+téma, alebo persona (+projekt, +téma); persona bez
                                                  témy, ktorú vlastní, ide do hlavného vlákna persony (@<persona>/main)
                                                  --maintainer: vlákno T3 správcu workflow (instance.json maintainer.thread);
                                                  bez nastaveného vlákna 404, archivované/neexistujúce vlákno → undelivered
                                                  --ttl: lehota na prevzatie správy (90s, 5m, 30m, sekundy; skracuje, nie predlžuje; blocker a steer najviac 5m, assist 30m)
                                                  predvolené ttl: blocker a steer 5m, assist 30m (instance.json ttl.<úroveň>)
                                                  --supersede-key <kľúč>: nová správa s rovnakým kľúčom od toho istého odosielateľa na tú istú adresu nahradí ešte neprevzatú
                                                  (blocker s kľúčom vytvorí oznam až po neprevzatí v lehote)
  receive --project <p> --topic <t> | --persona <meno> | --thread <id> [--id <id> ...]
                                                  čakajúce správy (pending); čítanie tela = prevzatie (picked); odpoveď a steer sú tým vybavené (done/read);
                                                  --id: iba zadané správy, aj prevzaté či konečné (telo odosielateľovi a schránke, opakovateľne)
  resolve --id <id> --state hotovo|otázka|čakám [--text <text|->] [--wait-for <čo>] [--cancels <id odpovede>] [schránka]
                                                  jediné vybavenie správy: hotovo s textom = konečná odpoveď (jedna, ďalšia 409), hotovo bez textu = vybavené
                                                  bez odpovede (settled); otázka a čakám sú priebežné odpovede s textom; --cancels: nová konečná odpoveď
                                                  nahradí tvoju predchádzajúcu. Pri uzavretej téme odosielateľa: 200, answer_delivered false
  settle --id <id> [schránka]                     resolve hotovo bez textu (vybavené bez odpovede)
  ack --id <id> [--id <id> ...] [schránka]        resolve hotovo s ack: potvrdená bez odpovede, iba prevzatá (picked); po priebežnej odpovedi 409
  withdraw --id <id>                              odosielateľ stiahne svoju nekonečnú správu (done/withdrawn)
                                                  schránka: --project <p> --topic <t> | --persona <meno> | --thread <id> (predvolene vlákno klienta)
                                                  resolve, settle, ack a withdraw nesú client_op_id; pri nedostupnej pošte idú do outboxu
  obligations [--thread <id>] [--topics <p/t,...>] [--project <p>] [--persona <meno>] [--since <cursor>]
                                                  čo vlákno dlhuje a na čo čaká: inbox, owed, awaiting, recent (po --since), cursor, topics, server;
                                                  pri nedostupnej pošte posledná odpoveď s offline: true, age_s (bez id s operáciou v outboxe);
                                                  wake: stav budenia tohto stroja ako pri count (running, pid, heartbeat_at; chýba, ak tu nikdy nebežalo)
  obligations --due                               splatné čakania odosielateľov (modul wake; kľúč wake všetky, iný kľúč iba svojich správ)
  wait-adopt --id <id> --due-at <iso> [--base-s <sekundy>]
                                                  služba prevezme lokálne čakanie (budík) klienta na odpoveď: iba odosielateľ, iba nekonečná správa
  count --project <p> --topic <t> | --persona <meno> | --thread <id>  počet a ids čakajúcich správ (vždy živo; po výpadku nový pokus najskôr po 60 s)
  message --id <id> [schránka]                    stav správy (status, resolution, history, wait_*; pri held dôvod held_reason); telo iba odosielateľovi a schránke
  turn --state open|closed [--thread <id>] [--machine <stroj>] [--project <p> --topic <t>] [--holder-mode operator|auto]
       [--opened-at <iso>] [--next <krok>] [--discussion open|closed]
                                                  záznam ťahu vlákna pre keepalive; closed s --opened-at zatvorí iba ten ťah;
                                                  closed s --next: ďalší krok z handoveru, budenie vlákno zobudí na pokračovanie;
                                                  --discussion open: vlákno diskutuje s operátorom (D-0015), budenie steer/assist
                                                  pozdrží do zatvorenia diskusie, po lehote odosielateľ dostane „busy“ (otvorený ťah,
                                                  doručí sa po ťahu) alebo „otázka“ (ťah skončil otázkou); blocker prejde;
                                                  pri výpadku { ok:false, unavailable:true[, queued:true] }, kód 0 (closed ide do outboxu)
  turn --waiting-operator true|false [--thread <id>] [--opened-at <iso>]
                                                  príznak otvoreného ťahu: vlákno čaká na dialóg operátora (P-061); pošta správu preň po lehote
                                                  neoznačí „no reply“, ale odosielateľovi zapíše odpoveď „otázka“ (pôvod code); koniec ťahu príznak zruší
  notices [--all]
  decide --notice <id> --decision <id> [--target <[brain[@stroj]:]project/topic>]   potvrdenie nesie resolved: <meno>@<stroj>
  register-topic --project <p> --topic <t> --subject <text> [--persona <meno>] [--visibility public|private]
                 [--holder <thread>] [--holder-mode operator|auto] [--holder-machine <stroj>] [--takeover]
                 vyžaduje brain; najviac jedna otvorená téma (brain, projekt, téma) naprieč strojmi: registrácia na inom
                 stroji ju presunie (ten istý --holder bez --takeover, iný držiteľ iba s --takeover, inak 409);
                 potvrdenie nesie moved (ids) a moved_from (<meno>@<stroj>)
  register-topic --persona <meno> --main [--holder <thread>] [--holder-machine <stroj>] [--takeover]   hlavné vlákno persony
  register-topic --project <p> --topic <t> --release [--holder <thread>]   uvoľní držiteľa (bez --holder alebo vlastného);
                 cudzí držiteľ sa mení iba s --takeover; zmena držiteľa bez --takeover je odmietnutá (409)
  register-topic --project <p> --topic <t> --close [--holder <thread>]   uzavrie tému (dokončená, archivovaná): nové správy 410,
                 neprevzaté sa vrátia odosielateľom (undelivered), prevzaté ostávajú (odosielateľ dostane „recipient closed“);
                 nová registrácia bez --close ju znovu otvorí
  events [--since <n>|latest] [--wait <ms>]       udalosti budenia novej správy a odpovede (long-poll)
  flush                                           odošle odchádzajúcu schránku (outbox): 409 s konečným stavom zahodí („already <resolution>“),
                                                  iné odmietnutie presunie do rejected/; pri inom modeli pošty ostane celá;
                                                  jeden flush naraz: keď iný ešte beží po lehote, vráti busy: true a nič nepošle
  clients                                         verzie pošty a workflow podľa odosielateľa (persony) a stroja, ako ich služba videla
                                                  (comms_version a workflow_version null: starý klient bez verzie alebo workflow ju neposlal)

Spoločné voľby: --instance <meno> | --data-dir <cesta> | --config <súbor.json> | --url <http(s)://host[:port]>
                --key <kľúč> | --thread <id> | --brain <id> | --brain-name <meno> | --machine <stroj> | --outbox <cesta> | --timeout <ms> | --no-start
`;

const FLAGS_WITH_VALUE = new Set(['instance', 'data-dir', 'config', 'url', 'key', 'thread', 'outbox', 'timeout', 'client-dir', 'instance-id',
  'sender', 'tcp', 'project', 'topic', 'level', 'text', 'wait-for', 'cancels', 'supersede-key', 'from-project', 'from-topic', 'to-thread', 'id', 'notice', 'decision', 'target',
  'subject', 'holder', 'holder-mode', 'since', 'wait', 'persona', 'visibility', 'holder-machine', 'machine', 'ttl', 'state', 'opened-at', 'next', 'resume-at', 'waiting-operator', 'discussion',
  'topics', 'due-at', 'base-s', 'brain', 'brain-name', 'to']);
const BOOL_FLAGS = new Set(['maintainer', 'no-start', 'all', 'help', 'json', 'wake', 'main', 'no-self-restart', 'takeover', 'release', 'close', 'due']);

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

/**
 * One CLI call: the exit code; the output goes to `stdout` and `stderr`. The workflow calls it inside its hook process
 * too (agentic-flow scripts/comms.mjs clientInProcess: no process per call, two calls at once, always with --no-start),
 * so: the settings of a call (thread, brain, machine, workflow version) are read from process.env before the first await
 * (resolveConfig below; the caller sets those variables for that part only), nothing is kept between calls in this
 * module, and no command of a client ends the process.
 */
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
    const code = e.code === 'UNAVAILABLE' ? 2 : (e.code === 'INSTANCE_MISMATCH' || e.code === 'MODEL_MISMATCH') ? 3 : 1;
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
        // --no-start: the service is this process; a child that starts its own when it goes away outlives the stop.
        const forward = [];
        for (const name of ['config', 'machine', 'instance', 'key']) if (args[name] != null) forward.push(`--${name}`, String(args[name]));
        wake = spawn(process.execPath, [WAKE_BIN, 'run', '--data-dir', cfg.dataDir, ...forward, '--no-self-restart', '--no-start'], { stdio: ['ignore', 'ignore', 'inherit'], windowsHide: true });
        wake.on('exit', (code) => { log(`wake skončil (${code})`); wake = null; });
      }
      // Stopping the service ends wake too: wait for the child (SIGKILL after 5 s), then exit.
      let stopping = false;
      const stop = () => {
        if (stopping) return;
        stopping = true;
        const w = wake;
        const ended = w ? new Promise((done) => {
          const t = setTimeout(() => { try { w.kill('SIGKILL'); } catch { /* gone */ } }, 5000);
          w.once('exit', () => { clearTimeout(t); done(); });
          w.kill('SIGTERM');
        }) : null;
        Promise.all([ended, srv.close()]).then(() => process.exit(0));
      };
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
      if (args.level === 'info') throw new CommsError('REJECTED', LEVEL_REMOVED_TEXT, { status: 400 });
      need('level');
      if (!LEVELS.includes(args.level)) throw new CommsError('REJECTED', `neznáma úroveň „${args.level}“; platné úrovne: ${LEVELS.join('|')}`, { status: 400 });
      if (args.maintainer && (args.project || args.topic || args.persona || args.to)) throw new CommsError('USAGE', '--maintainer sa nekombinuje s --to, --project, --topic ani --persona');
      if (args.toThread && (args.maintainer || args.project || args.topic || args.persona || args.to)) throw new CommsError('USAGE', '--to-thread sa nekombinuje s --to, --maintainer, --project, --topic ani --persona');
      if (args.to && (args.project || args.topic || args.persona)) throw new CommsError('USAGE', '--to sa nekombinuje s --project, --topic ani --persona');
      const to = args.to ? parseAddress(args.to) : null;
      if (!args.persona && !args.maintainer && !args.toThread && !to) need('project', 'topic');
      for (const f of ['state', 'cancels', 'waitFor']) {
        if (args[f] != null) throw new CommsError('USAGE', `--${f === 'waitFor' ? 'wait-for' : f} patrí vybaveniu správy (comms resolve), nie send`);
      }
      if (args.ttl != null) {
        // Checked here too, so that a message queued during an outage is not rejected later (the server checks the same).
        const s = parseDuration(args.ttl);
        if (s == null) throw new CommsError('REJECTED', `neplatná lehota (ttl) „${args.ttl}“: napr. 90s, 5m, 30m alebo počet sekúnd`, { status: 400 });
        if (s <= 0) throw new CommsError('REJECTED', `lehota (ttl) musí byť kladná; ${args.level} najviac ${MAX_TTL[args.level]}`, { status: 400 });
        if (s > parseDuration(MAX_TTL[args.level])) throw new CommsError('REJECTED', `lehota (ttl) „${args.ttl}“ je dlhšia ako maximum ${MAX_TTL[args.level]} pre ${args.level}`, { status: 400 });
      }
      let text = args.text;
      if (text === '-') text = await readAll(io.stdin);
      return client.send({
        project: to ? to.project : args.project || null, topic: to ? to.topic : args.topic || null, persona: args.persona || null,
        ...(to && to.brain ? { brain: to.brain } : {}), ...(to && to.machine ? { machine: to.machine } : {}), ...(args.maintainer ? { maintainer: true } : {}), ...(args.toThread ? { to_thread: args.toThread } : {}), level: args.level, text,
        from_project: args.fromProject || null, from_topic: args.fromTopic || null,
        ...(args.ttl != null ? { ttl: args.ttl } : {}),
        ...(args.supersedeKey != null ? { supersede_key: args.supersedeKey } : {}),
      });
    }
    case 'message':
      if (!args.id.length) throw new CommsError('USAGE', 'chýba voľba --id');
      if (args.id.length > 1) throw new CommsError('USAGE', 'message zisťuje stav jednej správy: zadajte jedno --id');
      return client.message(args.id[0], mailboxOf(args, cfg));
    case 'resolve': {
      if (args.id.length !== 1) throw new CommsError('USAGE', 'resolve vybaví jednu správu: zadajte jedno --id');
      need('state');
      if (!REPLY_STATES.includes(args.state)) throw new CommsError('REJECTED', `neznámy stav „${args.state}“; platné stavy: ${REPLY_STATES.join('|')}`, { status: 400 });
      let text = args.text ?? null;
      if (text === '-') text = await readAll(io.stdin);
      // Checked here too, so that an item queued during an outage is not rejected later (the server checks the same).
      if ((text == null || !String(text).trim()) && args.state !== 'hotovo') throw new CommsError('USAGE', `priebežná odpoveď (${args.state}) musí mať --text`);
      if (args.cancels != null && (args.state !== 'hotovo' || text == null)) throw new CommsError('USAGE', '--cancels nahrádza konečnú odpoveď: --state hotovo a --text');
      return client.resolve({
        id: args.id[0], state: args.state, ...(text != null ? { text } : {}), ...mailboxOf(args, cfg),
        ...(args.waitFor != null ? { wait_for: args.waitFor } : {}), ...(args.cancels != null ? { cancels: args.cancels } : {}),
      });
    }
    case 'settle':
      if (args.id.length !== 1) throw new CommsError('USAGE', 'settle vybaví jednu správu: zadajte jedno --id');
      return client.resolve({ id: args.id[0], state: 'hotovo', ...mailboxOf(args, cfg) });
    case 'ack': {
      if (!args.id.length) throw new CommsError('USAGE', 'zadaj aspoň jedno --id');
      const results = [];
      for (const id of args.id) results.push(await client.resolve({ id, state: 'hotovo', ack: true, ...mailboxOf(args, cfg) }));
      return args.id.length === 1 ? results[0] : { results };
    }
    case 'withdraw':
      if (args.id.length !== 1) throw new CommsError('USAGE', 'withdraw stiahne jednu správu: zadajte jedno --id');
      return client.withdraw({ id: args.id[0] });
    case 'obligations': {
      if (args.due) return client.due();
      const q = { thread: args.thread || cfg.threadId || null, topics: args.topics || null, project: args.project || null, persona: args.persona || null, since: args.since ?? null, ...brainOf(cfg) };
      if (!q.thread && !q.topics && !q.persona) throw new CommsError('USAGE', 'obligations: zadaj --thread (alebo COMMS_THREAD_ID), --topics alebo --persona');
      // obligations replaces count for the status line (design part 2), so it carries the wake status of this machine
      // like count (comms-public#4), also with the offline backup: null (no field) when wake never ran here.
      const r = await client.obligations(q);
      const wake = wakeStatus(wakeStateDir(args));
      return wake ? { ...r, wake } : r;
    }
    case 'wait-adopt': {
      if (args.id.length !== 1) throw new CommsError('USAGE', 'wait-adopt prevezme čakanie jednej správy: zadajte jedno --id');
      need('dueAt');
      if (!Number.isFinite(Date.parse(args.dueAt))) throw new CommsError('USAGE', '--due-at musí byť čas ISO 8601');
      const base = args.baseS == null ? null : Number(args.baseS);
      if (base != null && !(Number.isInteger(base) && base > 0)) throw new CommsError('USAGE', '--base-s musí byť kladné celé číslo sekúnd');
      return client.waitAdopt({ id: args.id[0], due_at: args.dueAt, ...(base != null ? { base_s: base } : {}) });
    }
    case 'turn': {
      // `--waiting-operator true|false` without --state: the flag of the open turn (the thread waits for the operator's dialog).
      if (args.state == null && args.waitingOperator != null) {
        if (!['true', 'false'].includes(args.waitingOperator)) throw new CommsError('USAGE', '--waiting-operator je true alebo false');
      } else need('state');
      if (args.discussion != null && !['open', 'closed'].includes(args.discussion)) throw new CommsError('USAGE', '--discussion je open alebo closed');
      const thread = args.thread || cfg.threadId;
      if (!thread) throw new CommsError('USAGE', 'chýba voľba --thread (alebo COMMS_THREAD_ID / T3CODE_THREAD_ID)');
      // Short deadline; on outage only `closed` is queued (the service drops it when `opened_at` no longer matches).
      const quick = new Client({ ...cfg, timeout: Math.min(cfg.timeout, TURN_TIMEOUT) });
      return quick.turn({
        thread, ...(args.state != null ? { state: args.state } : {}), machine: cfg.machine || null, project: args.project || null, topic: args.topic || null,
        holder_mode: args.holderMode || null, ...(args.waitingOperator != null ? { waiting_operator: args.waitingOperator === 'true' } : {}), ...(args.openedAt ? { opened_at: args.openedAt } : {}), ...(args.next ? { next: args.next } : {}), ...(args.resumeAt ? { resume_at: args.resumeAt } : {}),
        ...(args.discussion != null ? { discussion: args.discussion } : {}),
      });
    }
    case 'receive': {
      if (!args.thread && !args.persona) need('project', 'topic');
      await client.ensureRunning();
      try { await client.flushOutbox(); } catch (e) { if (e.code === 'MODEL_MISMATCH' || e.code === 'INSTANCE_MISMATCH') throw e; /* outbox stays, reported by flush */ }
      const q = { project: args.project, topic: args.topic, thread: args.thread, persona: args.persona, ...brainOf(cfg) };
      // One `--id`: that message in any status (receive?id, 1d); more: only those of the pending ones.
      if (args.id.length === 1) return client.receive({ ...q, id: args.id[0] });
      return client.receive({ ...q, ids: args.id.length ? args.id.join(',') : null });
    }
    case 'count':
      if (!args.thread && !args.persona) need('project', 'topic');
    {
      const r = await client.count({ project: args.project, topic: args.topic, thread: args.thread, persona: args.persona, ...brainOf(cfg) });
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
    case 'clients':
      return client.clients();
    case 'flush': {
      await client.ensureRunning();
      return client.flushOutbox();
    }
    default:
      throw new CommsError('USAGE', `neznámy príkaz „${cmd}“`);
  }
}

/** Mailbox of resolve/settle/ack/message: project+topic, persona, else the thread (--thread or the client's thread). */
function mailboxOf(args, cfg) {
  const thread = args.thread || cfg.threadId || null;
  return { project: args.project || null, topic: args.topic || null, persona: args.persona || null, thread, ...brainOf(cfg) };
}

/** Brain and machine of a topic mailbox (model 3): this client's (--brain/--machine, COMMS_BRAIN/COMMS_MACHINE, config). */
function brainOf(cfg) {
  return { ...(cfg.brain ? { brain: cfg.brain } : {}), ...(cfg.machine ? { machine: cfg.machine } : {}) };
}

/** Topic address `[<brain>[@<machine>]:]<project>/<topic>` (model 3; brain = id or brain_name). */
export function parseAddress(s) {
  const raw = String(s || '');
  const colon = raw.lastIndexOf(':');
  const head = colon >= 0 ? raw.slice(0, colon) : null;
  const parts = raw.slice(colon + 1).split('/');
  const [brain, machine, extra] = head != null ? head.split('@') : [null, null];
  const ok = (v) => /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(v || '');
  if (parts.length !== 2 || !ok(parts[0]) || !ok(parts[1]) || (head != null && !ok(brain)) || (machine != null && !machine) || extra !== undefined) {
    throw new CommsError('USAGE', `adresa „${raw}“ musí byť [<brain>[@<stroj>]:]<projekt>/<téma>`);
  }
  return { brain: brain || null, machine: machine || null, project: parts[0], topic: parts[1] };
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
