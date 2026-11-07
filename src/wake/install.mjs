// `wake install [--remove] [--print]` (comms-public#4): registers the permanent wake run for the logged-in user, the same
// way on every system instead of a hand recipe per OS:
//   Linux    systemd user service  ~/.config/systemd/user/agentic-wake-<instance>.service (Restart=always)
//   macOS    LaunchAgent           ~/Library/LaunchAgents/com.agentic-comms.wake.<instance>.plist (KeepAlive)
//   Windows  scheduled task        agentic-wake-<instance>, at logon, no window, loop <base>\wake-loop-<instance>.cmd
// installPlan() is pure (files to write, commands to run) so every platform is tested on any machine; applyPlan() runs it.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const NAME_RE = /^[A-Za-z0-9._-]+$/;
const xml = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
const sysq = (s) => `"${String(s).replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`; // systemd ExecStart quoting
const winq = (s) => (/[\s"&|<>^]/.test(String(s)) ? `"${String(s).replace(/"/g, '""')}"` : String(s));
const psq = (s) => `'${String(s).replace(/'/g, "''")}'`;

/** Names of the service files and units for one instance. */
export function serviceNames(instance) {
  if (!NAME_RE.test(instance)) throw new Error(`inštancia "${instance}" sa nehodí do názvu služby (povolené A-Z a-z 0-9 . _ -)`);
  return {
    unit: `agentic-wake-${instance}.service`,
    label: `com.agentic-comms.wake.${instance}`,
    task: `agentic-wake-${instance}`,
    loop: `wake-loop-${instance}.cmd`,
    log: `wake-${instance}.log`,
  };
}

/**
 * @param {object} o  platform, instance, runArgs (after `run`), node, bin, home, base (comms base dir), env, uid, remove
 * @returns {{ platform, before: Cmd[], files: {path, content}[], remove: string[], after: Cmd[], notes: string[] }}
 *   in this order; Cmd = { cmd, args, ignoreFail? } or { stopWake: true } (end the running wake by its lock pid)
 */
export function installPlan({ platform, instance, runArgs, node, bin, home, base, env = {}, uid = 0, remove = false }) {
  const n = serviceNames(instance);
  const plan = { platform, before: [], files: [], remove: [], after: [], notes: [] };
  const run = ['run', ...runArgs];
  if (platform === 'linux') {
    const dir = path.posix.join(env.XDG_CONFIG_HOME || path.posix.join(home, '.config'), 'systemd', 'user');
    const unit = path.posix.join(dir, n.unit);
    if (remove) {
      plan.before.push({ cmd: 'systemctl', args: ['--user', 'disable', '--now', n.unit], ignoreFail: true });
      plan.remove.push(unit);
      plan.after.push({ cmd: 'systemctl', args: ['--user', 'daemon-reload'], ignoreFail: true });
      return plan;
    }
    // No user manager (no login session, container): fail before writing, with the way out.
    plan.before.push({ cmd: 'systemctl', args: ['--user', 'show-environment'], hint: 'používateľský systemd nebeží (chýba prihlásenie alebo XDG_RUNTIME_DIR); prihlás sa cez login/ssh, na serveri najprv loginctl enable-linger, alebo spúšťaj wake run v termináli' });
    plan.files.push({ path: unit, content: [
      '[Unit]',
      `Description=agentic-comms: budenie vlákien T3 (inštancia ${instance})`,
      '[Service]',
      `ExecStart=${[node, bin, ...run].map(sysq).join(' ')}`,
      `Environment=${sysq(`PATH=${env.PATH || '/usr/local/bin:/usr/bin:/bin'}`)}`,
      'Restart=always',
      'RestartSec=5',
      '[Install]',
      'WantedBy=default.target',
      '',
    ].join('\n') });
    plan.after.push({ cmd: 'systemctl', args: ['--user', 'daemon-reload'] });
    plan.after.push({ cmd: 'systemctl', args: ['--user', 'enable', n.unit] });
    plan.after.push({ cmd: 'systemctl', args: ['--user', 'restart', n.unit] });
    plan.notes.push(`log: journalctl --user -u ${n.unit} -f`);
    plan.notes.push('beh aj bez prihlásenia (server): loginctl enable-linger');
    return plan;
  }
  if (platform === 'darwin') {
    const plist = path.posix.join(home, 'Library', 'LaunchAgents', `${n.label}.plist`);
    const domain = `gui/${uid}`;
    plan.before.push({ cmd: 'launchctl', args: ['bootout', `${domain}/${n.label}`], ignoreFail: true });
    if (remove) { plan.remove.push(plist); return plan; }
    const log = path.posix.join(base, n.log);
    const str = (s) => `    <string>${xml(s)}</string>`;
    plan.files.push({ path: plist, content: [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
      '<plist version="1.0">',
      '<dict>',
      `  <key>Label</key><string>${xml(n.label)}</string>`,
      '  <key>ProgramArguments</key>',
      '  <array>',
      ...[node, bin, ...run].map(str),
      '  </array>',
      '  <key>EnvironmentVariables</key>',
      `  <dict><key>PATH</key><string>${xml(env.PATH || '/usr/local/bin:/usr/bin:/bin')}</string></dict>`,
      '  <key>RunAtLoad</key><true/>',
      '  <key>KeepAlive</key><true/>',
      `  <key>StandardOutPath</key><string>${xml(log)}</string>`,
      `  <key>StandardErrorPath</key><string>${xml(log)}</string>`,
      '</dict>',
      '</plist>',
      '',
    ].join('\n') });
    plan.after.push({ cmd: 'launchctl', args: ['bootstrap', domain, plist] });
    plan.notes.push(`log: ${log}`);
    return plan;
  }
  if (platform === 'win32') {
    const loop = path.win32.join(base, n.loop);
    const log = path.win32.join(base, n.log);
    const ps = (script) => ({ cmd: 'powershell.exe', args: ['-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-Command', script] });
    // Stop the task and its loop first (Stop-ScheduledTask ends only conhost, the cmd loop would start node again, #3);
    // the wake itself is ended by its lock pid (stopWake), which also ends the supervisor above it.
    // The hand recipe of README <= v0.4.5 (task agentic-wake, loop wake-loop.cmd) is replaced too: else its loop keeps
    // starting a second wake that waits on the lock forever.
    plan.before.push({ ...ps([
      `$old = Get-ScheduledTask -TaskName 'agentic-wake' -ErrorAction SilentlyContinue | Where-Object { ($_.Actions.Arguments -join ' ') -like '*wake-loop.cmd*' }`,
      `if ($old) { Stop-ScheduledTask -TaskName 'agentic-wake'; Unregister-ScheduledTask -TaskName 'agentic-wake' -Confirm:$false }`,
      `Get-CimInstance Win32_Process -Filter "Name='cmd.exe'" | Where-Object { $_.CommandLine -like '*\\wake-loop.cmd*' } | Invoke-CimMethod -MethodName Terminate | Out-Null`,
    ].join('; ')), ignoreFail: true });
    plan.before.push({ ...ps([
      `if (Get-ScheduledTask -TaskName ${psq(n.task)} -ErrorAction SilentlyContinue) { Stop-ScheduledTask -TaskName ${psq(n.task)} }`,
      `Get-CimInstance Win32_Process -Filter "Name='cmd.exe'" | Where-Object { $_.CommandLine -like ${psq(`*${n.loop}*`)} } | Invoke-CimMethod -MethodName Terminate | Out-Null`,
    ].join('; ')), ignoreFail: true });
    plan.before.push({ stopWake: true });
    if (remove) {
      plan.before.push({ ...ps(`Unregister-ScheduledTask -TaskName ${psq(n.task)} -Confirm:$false -ErrorAction SilentlyContinue`), ignoreFail: true });
      plan.remove.push(loop);
      return plan;
    }
    plan.files.push({ path: loop, content: [
      '@echo off',
      ':loop',
      `if not exist ${winq(bin)} exit /b 0`,
      `${[node, bin, ...run].map(winq).join(' ')} >> ${winq(log)} 2>&1`,
      'timeout /t 5 /nobreak >nul',
      'goto loop',
      '',
    ].join('\r\n') });
    plan.after.push(ps([
      `$a = New-ScheduledTaskAction -Execute 'conhost.exe' -Argument ${psq(`--headless cmd.exe /d /c "${loop}"`)}`,
      '$t = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME',
      '$s = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries',
      `Register-ScheduledTask -TaskName ${psq(n.task)} -Description ${psq(`agentic-comms: budenie vlákien T3 (${n.loop}). Vypnutie: node bin\\wake.mjs install --remove`)} -Action $a -Trigger $t -Settings $s -Force | Out-Null`,
      `Start-ScheduledTask -TaskName ${psq(n.task)}`,
    ].join('; ')));
    plan.notes.push(`log: ${log}`);
    return plan;
  }
  throw new Error(`systém ${platform} nie je podporovaný; spúšťaj wake run v termináli`);
}

/** Human-readable plan for --print (no secrets: the key stays in keys.json, never on the command line). */
export function formatPlan(plan) {
  const out = [];
  for (const f of plan.files) out.push(`--- ${f.path}`, f.content.replace(/\r\n/g, '\n').trimEnd());
  for (const r of plan.remove) out.push(`--- zmazať ${r}`);
  const cmd = (c) => (c.stopWake ? '> ukončiť bežiace budenie tejto inštancie (pid zo zámku)' : `> ${[c.cmd, ...c.args].join(' ')}`);
  return [...plan.before.map(cmd), ...out, ...plan.after.map(cmd)].join('\n') + '\n';
}

/** Run the plan in order: before, files, remove, after. stopWake(): end the running wake of this instance. */
export function applyPlan(plan, { stopWake = () => {}, spawn = spawnSync } = {}) {
  const errors = [];
  const exec = (c) => {
    if (c.stopWake) { stopWake(); return; }
    const r = spawn(c.cmd, c.args, { encoding: 'utf8', windowsHide: true });
    if ((r.error || r.status !== 0) && !c.ignoreFail) errors.push(`${c.cmd} ${c.args.slice(0, 3).join(' ')}: ${c.hint || String(r.error?.message || r.stderr || `kód ${r.status}`).trim().split('\n')[0]}`);
  };
  for (const c of plan.before) exec(c);
  if (errors.length) return { ok: false, errors }; // nothing written when a precondition fails
  for (const f of plan.files) { fs.mkdirSync(path.dirname(f.path), { recursive: true }); fs.writeFileSync(f.path, f.content); }
  for (const r of plan.remove) fs.rmSync(r, { force: true });
  for (const c of plan.after) { if (errors.length) break; exec(c); }
  return { ok: !errors.length, errors };
}
