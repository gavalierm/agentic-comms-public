// Start the mail service of one instance on its local socket / named pipe and
// optionally on TCP (bearer key required; HTTPS is left to a reverse proxy).
import http from 'node:http';
import fs from 'node:fs';
import { Store } from './store.mjs';
import { Service } from './service.mjs';
import { createHandler } from './http.mjs';
import { socketPath } from '../common/paths.mjs';
import { nowIso } from '../common/util.mjs';

/** Probe whether a service answers on the socket. Resolves health JSON or null. */
export function probe(sock, timeout = 500) {
  return new Promise((resolve) => {
    const req = http.request({ socketPath: sock, path: '/health', method: 'GET', timeout }, (res) => {
      let raw = '';
      res.on('data', (d) => { raw += d; });
      res.on('end', () => { try { resolve(JSON.parse(raw)); } catch { resolve(null); } });
    });
    req.on('timeout', () => { req.destroy(); resolve(null); });
    req.on('error', () => resolve(null));
    req.end();
  });
}

/**
 * Start the service. Returns { close(), socket, tcp, service } or throws
 * { code: 'ALREADY_RUNNING', health } if another process serves this instance.
 */
/** Started by a service manager (systemd: parent PID 1 and INVOCATION_ID; or COMMS_MANAGED=1). A server spawned by a
 * client inherits INVOCATION_ID from its caller but not parent PID 1, so it does not count. */
export function isManagedStart(env = process.env, ppid = process.ppid) {
  return env.COMMS_MANAGED === '1' || (Boolean(env.INVOCATION_ID) && ppid === 1);
}

export async function startServer({ dataDir, tcp = null, log = () => {}, managed = isManagedStart() }) {
  const store = new Store(dataDir);
  if (!store.exists()) throw new Error(`inštancia v ${dataDir} neexistuje; spusťte najprv „comms init“`);
  const sock = socketPath(dataDir);

  const running = await probe(sock);
  if (running && running.ok) {
    const err = new Error('služba už beží');
    err.code = 'ALREADY_RUNNING';
    err.health = running;
    throw err;
  }
  if (process.platform !== 'win32') {
    try { fs.unlinkSync(sock); } catch { /* no stale socket */ }
  }

  const service = new Service(store, { log });
  const handler = createHandler(service, store);
  const server = http.createServer(handler);
  server.keepAliveTimeout = 5000;

  await new Promise((resolve, reject) => {
    server.once('error', (e) => {
      if (e.code === 'EADDRINUSE') { e.code = 'ALREADY_RUNNING'; }
      reject(e);
    });
    server.listen(sock, resolve);
  });
  if (process.platform !== 'win32') {
    try { fs.chmodSync(sock, 0o600); } catch { /* best effort */ }
  }

  let tcpServer = null;
  const inst = store.instance();
  // A managed instance is never started by clients: a client autostart in the restart gap would hold the socket
  // and the managed unit would loop on already_running (myserver 2026-11-01).
  if (managed && !inst.managed) { inst.managed = true; store.saveInstance(inst); log('instance marked managed: clients will not autostart it'); }
  let tcpCfg = tcp || inst.tcp || null;
  if (tcpCfg && typeof tcpCfg.port === 'number') {
    tcpServer = http.createServer(handler);
    await new Promise((resolve, reject) => {
      tcpServer.once('error', (e) => { server.close(); reject(e); });
      tcpServer.listen(tcpCfg.port, tcpCfg.host || '127.0.0.1', resolve);
    });
    tcpCfg = { host: tcpCfg.host || '127.0.0.1', port: tcpServer.address().port };
    log(`tcp listening on ${tcpCfg.host}:${tcpCfg.port}`);
  }

  service.startTimers();
  store.saveServerInfo({ pid: process.pid, instance_id: service.instanceId, socket: sock, tcp: tcpCfg, started_at: nowIso() });
  log(`listening on ${sock} (instance ${inst.instance}, id ${service.instanceId})`);

  let closed = false;
  const close = () => new Promise((resolve) => {
    if (closed) return resolve();
    closed = true;
    service.stopTimers();
    const info = store.serverInfo();
    if (info && info.pid === process.pid) store.clearServerInfo();
    let n = tcpServer ? 2 : 1;
    const done = () => { if (--n === 0) resolve(); };
    server.close(done);
    server.closeAllConnections?.();
    if (tcpServer) { tcpServer.close(done); tcpServer.closeAllConnections?.(); }
  });

  return { close, socket: sock, tcp: tcpCfg, service, store };
}
