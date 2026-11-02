// Operator notification through an external command (P-046). The command is
// configured in instance.json as `notify.command` and run through the shell;
// the notice text is written to its stdin (sendmail-compatible). Without a
// command only the notices page applies. Failures are logged, never fatal.
import { spawn } from 'node:child_process';

export function notifyOperator(instance, { notice, message, log = () => {} }) {
  const command = instance && instance.notify && instance.notify.command;
  if (!command) return false;
  const to = instance.notify.to || '';
  const subject = `[comms ${instance.instance}] ${notice.title}`;
  const body = [
    to ? `To: ${to}` : null,
    `Subject: ${subject}`,
    '',
    `Oznam: ${notice.title}`,
    `Typ: ${notice.kind}`,
    `Odosielateľ: ${message.sender}${message.thread_id ? ` (vlákno ${message.thread_id})` : ''}`,
    `Úroveň: ${message.level}`,
    `Čas: ${notice.created_at}`,
    `Možnosti: ${notice.options.map((o) => o.id).join(', ')}`,
    '',
    'Rozhodnite na stránke oznamov alebo príkazom:',
    `  node bin/comms.mjs decide --notice ${notice.id} --decision <${notice.options.map((o) => o.id).join('|')}>`,
    '',
  ].filter((l) => l !== null).join('\n');

  try {
    const child = spawn(command, {
      shell: true,
      stdio: ['pipe', 'ignore', 'pipe'],
      env: {
        ...process.env,
        COMMS_NOTICE_ID: notice.id,
        COMMS_NOTICE_KIND: notice.kind,
        COMMS_NOTICE_TITLE: notice.title,
        COMMS_NOTICE_LEVEL: message.level,
        COMMS_NOTICE_PROJECT: message.project || '',
        COMMS_NOTICE_TOPIC: message.topic || '',
        COMMS_NOTIFY_SUBJECT: subject,
      },
    });
    let err = '';
    child.stderr.on('data', (d) => { err += d; });
    child.on('error', (e) => log(`notify failed: ${e.message}`));
    child.on('close', (code) => { if (code !== 0) log(`notify exited ${code}: ${err.trim()}`); });
    child.stdin.on('error', () => {});
    child.stdin.end(body);
    return true;
  } catch (e) {
    log(`notify failed: ${e.message}`);
    return false;
  }
}
