// Operator notices page: plain HTML, no scripts, no dependencies. Decision
// buttons are forms posting to /decide. Texts in Slovak (operator-facing).
import { waitingText } from '../common/util.mjs';

const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

export function renderPage({ instance, notices, pending, key, showAll }) {
  const keyField = key ? `<input type="hidden" name="key" value="${esc(key)}">` : '';
  const q = key ? `?key=${encodeURIComponent(key)}` : '?';

  const noticeRows = notices.length ? notices.map((n) => `
    <li class="notice ${esc(n.kind)} ${esc(n.state)}">
      <div class="head"><strong>${esc(n.title)}</strong> <span class="meta">${esc(n.kind)} · ${esc(n.level)} · od ${esc(n.sender)}${n.thread_id ? ` (vlákno ${esc(n.thread_id)})` : ''} · čaká ${esc(waitingText(n.waiting_since))}</span></div>
      ${n.detail ? `<div class="meta">${esc(n.detail)}</div>` : ''}
      <pre class="text">${esc(n.text)}</pre>
      ${n.state === 'open' ? `
      <form method="post" action="/decide" class="actions">
        ${keyField}
        <input type="hidden" name="notice_id" value="${esc(n.id)}">
        ${n.options.some((o) => o.needs_target) ? '<label>Cieľ (project/topic): <input name="target" placeholder="projekt/tema"></label>' : ''}
        ${n.options.map((o) => `<button type="submit" name="decision" value="${esc(o.id)}">${esc(o.label)}</button>`).join('\n        ')}
      </form>` : `<div class="meta">Rozhodnuté: ${esc(n.decision)}${n.target ? ` → ${esc(n.target)}` : ''} (${esc(n.decided_at)})</div>`}
      <div class="meta">oznam ${esc(n.id)} · správa ${esc(n.message_id)}</div>
    </li>`).join('') : '<li class="empty">Žiadne otvorené oznamy.</li>';

  const pendingRows = pending.length ? pending.map((m) => `
    <tr><td>${esc(m.project)}/${esc(m.topic)}${m.to_thread ? `<br><small>vlákno ${esc(m.to_thread)}</small>` : ''}</td><td>${esc(m.level)}</td><td>${esc(m.sender)}</td><td>${esc(waitingText(m.created_at))}</td><td><pre>${esc(m.text)}</pre></td></tr>`).join('')
    : '<tr><td colspan="5" class="empty">Žiadne čakajúce správy.</td></tr>';

  return `<!doctype html>
<html lang="sk">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pošta ${esc(instance.instance)} – oznamy</title>
<style>
  body { font: 15px/1.45 system-ui, sans-serif; margin: 1.5rem auto; max-width: 60rem; padding: 0 1rem; color: #222; }
  h1 small { color: #777; font-weight: normal; font-size: .7em; }
  ul { list-style: none; padding: 0; }
  .notice { border: 1px solid #ccc; border-left: 6px solid #999; border-radius: 6px; padding: .8rem 1rem; margin: .8rem 0; }
  .notice.blocker { border-left-color: #c33; }
  .notice.wake-limit { border-left-color: #36c; }
  .notice.unregistered-topic { border-left-color: #d90; }
  .notice.no-reply, .notice.turn-interrupted { border-left-color: #a3a; }
  .notice.decided { opacity: .6; }
  .meta { color: #666; font-size: .85em; }
  pre { white-space: pre-wrap; background: #f6f6f6; padding: .5rem; border-radius: 4px; margin: .5rem 0; }
  .actions { display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; }
  button { padding: .35rem .8rem; cursor: pointer; }
  table { border-collapse: collapse; width: 100%; }
  td, th { border-bottom: 1px solid #ddd; padding: .4rem; text-align: left; vertical-align: top; }
  .empty { color: #888; }
  nav a { margin-right: 1rem; }
</style>
</head>
<body>
<h1>Pošta <small>inštancia ${esc(instance.instance)} · ${esc(instance.instance_id)}</small></h1>
<nav><a href="/${q}">Otvorené oznamy</a><a href="/${q}&all=1">Všetky oznamy</a><a href="/health">Stav služby</a></nav>
<h2>Oznamy${showAll ? ' (všetky)' : ''}</h2>
<ul>${noticeRows}
</ul>
<h2>Čakajúce správy</h2>
<p class="meta">Pošta nezisťuje, či vlákno ešte žije; zobrazuje iba čas čakania.</p>
<table>
<tr><th>Téma</th><th>Úroveň</th><th>Od</th><th>Čaká</th><th>Text</th></tr>${pendingRows}
</table>
</body>
</html>
`;
}

export function renderError(status, message, backHref) {
  return `<!doctype html><html lang="sk"><head><meta charset="utf-8"><title>Chyba ${status}</title></head>
<body style="font-family: system-ui, sans-serif; margin: 2rem"><h1>Chyba ${status}</h1><p>${esc(message)}</p>${backHref ? `<p><a href="${esc(backHref)}">Späť</a></p>` : ''}</body></html>`;
}
