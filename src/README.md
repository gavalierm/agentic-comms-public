# src/

- `common/`: cesty podľa OS (`paths.mjs`: dátový priečinok, socket / named pipe), pomôcky (`util.mjs`: atomický zápis JSON, ID, úrovne), samoreštart (`selfrestart.mjs`: revízia `.git/HEAD`, sledovanie súborov, vypnutie).
- `server/`: `store.mjs` (súbory inštancie: správy, témy, oznamy, udalosti, kľúče), `service.mjs` (logika `send`/`receive`/`decide`/udalosti, lehoty TTL, ťahy), `http.mjs` (routing, kľúč → odosielateľ), `page.mjs` (HTML stránka oznamov), `index.mjs` (štart na sockete, voliteľne TCP).
- `client/`: `index.mjs` (`http.request` so `socketPath`, overenie `instance_id`, spustenie na požiadanie, outbox, cache počtu), `cli.mjs` (príkazy a voľby).
- `notify/`: externý príkaz `notify.command` pri `blocker`.
- `wake/`: `index.mjs` (`Waker`: odber udalostí long-pollom, pevná šablóna, limit 6/h na dvojicu tém, stop pri opakovaní, oznam `wake-limit`, keepalive prerušených ťahov, stav a kurzor, reconnect), `cli.mjs` (`run`, `status`, `config`).
- `adapters/t3/`: `ring(threadId, text)` cez `t3 thread send <threadId> <text>`, `status(threadId)` cez `t3 thread show --json <threadId>`; príkazy konfigurovateľné (`wake.command`, `wake.status_command`).

Služba zapisuje iba do svojho dátového priečinka; jedna správa = jeden súbor; `level` povinný; odosielateľ podľa kľúča, nikdy podľa tela správy.
