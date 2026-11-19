# Model pošty 2 (záväzný opis)

Tento dokument je **záväzný opis** modelu pošty 2 (D-0087, D-0089, D-0090). Opisuje skutočný kód (`src/server`, `src/client`, `src/wake`, `tests`) stavu po commitoch `cfd252d`, `2c561a1`, `43290a4`; východisko bol návrh `mail-service-design` v3, odchýlky od neho sú v časti 5.

Pravidlo D-0090: koncept pošty sa pri ďalších zmenách nesmie rozbiť. **Každá zmena pošty tento dokument najprv prečíta a zmení ako prvý** (časť 6). Keď sa kód a tento dokument rozchádzajú, je to chyba, ktorá sa opraví v kóde alebo vedome tu, nikdy sa neprehliadne.

Skratky: LC = `src/server/lifecycle.mjs`, SV = `src/server/service.mjs`, HT = `src/server/http.mjs`, MG = `src/server/migrate.mjs`, CL = `src/client/index.mjs`, CLI = `src/client/cli.mjs`, WK = `src/wake/index.mjs`, UT = `src/common/util.mjs`, LK = `src/common/lock.mjs`.

## 1. Princíp

| Pravidlo | Kde v kóde |
|---|---|
| Stav správy vedie **iba server**: jedno pole `status` (+ `resolution` pri `done`) v zázname `messages/<id>.json`; zapisuje ho iba `transition()`. | LC `transition`, SV `apply` |
| Klient (workflow aj `src/client`) **nedrží stav správ**. Drží iba outbox pre nedostupnú poštu (`send`, `resolve`, `withdraw`, `turn closed`, každý zápis s `client_op_id`) a zálohu poslednej odpovede `obligations` na čítanie (`obligations-cache.json`). Po obnove platí server bez zlučovania. | CL `write`, `flushOutbox`, `obligations` |
| Odpoveď aj vybavenie bez odpovede sú **jedna operácia** `resolve`. Čo vlákno dlhuje, na čo čaká a kedy ho zobudiť, odvodzuje server (`obligations`, `wait_due_at`). | SV `resolve`, `obligations`, `due` |
| Pošta a workflow sa **vydávajú spolu** (pošta 0.5.0 + workflow 1.8.0 = model 2; model 3 = téma brain + stroj + projekt + téma), **bez vrstvy kompatibility**: žiadne odvodené polia, aliasy ani vetvy podľa verzie druhej strany. | LC `MAIL_MODEL = 3`, CL `MAIL_MODEL = 3` |
| Nesúlad modelu **zlyhá nahlas** a nič nestratí: server odmietne požiadavku bez `X-Comms-Model: 3` kódom **426** („mail model 3 (topic = brain + machine + project + topic): update mail and workflow together“) a nič nezapíše; klient pri `/health` s iným `model` alebo pri 426 hlási `MODEL_MISMATCH` („comms model <n> ≠ 3: update mail and workflow together“), CLI skončí **kódom 3**, nič nečíta, nezapíše ani nezaradí do outboxu. | HT, CL `ensureRunning`, `request`, CLI kódy |

## 2. Životný cyklus `status`

| `status` | Význam | Konečný | `inbox` | `owed` | `awaiting` |
|---|---|---|---|---|---|
| `held` | pozdržaná: neregistrovaná alebo cudzia súkromná téma, čaká na operátora | nie | nie | nie | áno |
| `pending` | v schránke, neprevzatá (aj s `expired_at` alebo `code_answer`) | nie | **áno** | nie | áno |
| `picked` | prevzatá (telo prečítané), bez odpovede | nie | nie | áno | áno |
| `waiting` | priebežná odpoveď príjemcu `otázka` / `čakám` | nie | nie | áno | áno |
| `done` | vybavená; `resolution`: `reply`, `settled`, `acked`, `read`, `withdrawn` | áno | nie | nie | nie |
| `discarded` | zahodil operátor (`decide`) | áno | nie | nie | nie |
| `superseded` | nahradená novšou s rovnakým `supersede_key` pred prevzatím | áno | nie | nie | nie |
| `undelivered` | adresát trvalo nedosiahnuteľný a správu nikto neprevzal | áno | nie | nie | nie |

Správa s `reply_to` (odpoveď) a `steer` nie sú nikdy `owed`. Každá správa ostáva čitateľná cez `GET /receive?id=` pre odosielateľa a schránku.

**Udalosti nad `pending`, nie stavy** (zapisuje ich `checkExpired`, `status` nemenia): `expired_at` (lehota na prevzatie uplynula; správa ostáva v schránke) a `code_answer` `otázka`|`busy` s `code_answer_at` (príjemca čaká na operátora alebo beží jeho ťah; správa ostáva v schránke).

### Prechody (LC `TRANSITIONS`, jediný zapisovateľ `transition()`)

`transition(m, to, op, by)` overí riadok tabuľky (`findTransition`), inak hodí **409** s telom `{id, status, resolution, answer_state, answer_id}` a nezmení nič. Zapíše `status`, `resolution` (iba pri `done`), `status_at`, `status_by` (`thread` | `code` | `operator` | `migration`) a riadok `history[]` `{at, from, to, op, by}` (strop 20). Konečný `status` vynuluje čakanie (`wait_due_at`, `wait_reason`).

| `op` | Z | Do | `resolution` | Kto (`by`) | Vedľajší účinok (SV) |
|---|---|---|---|---|---|
| `send` | nový | `pending` / `held` | – | `thread` | `blocker`/`assist`: čakanie `no-reply` (15 / 60 min); pri `pending` udalosť úrovne; oznam `blocker` (bez `supersede_key`) alebo `unregistered-topic` (`held`) |
| `resolve` | nový | `pending` | – | `thread` | záznam odpovede (`reply_to`, `reply_state`) |
| `resolve` | `pending` | `undelivered` | – | `thread` | záznam odpovede, keď je téma odosielateľa uzavretá (`sender-closed`) alebo nie je cesta späť (`no-return-path`) |
| `decide` | `held` | `pending` / `discarded` | – | `operator` | `deliver`/`redirect` (po lehote 409), `discard`; doručenie persone ide do jej hlavného vlákna |
| `decide` | `pending` | `discarded` | – | `operator` | `discard` oznamu `blocker` / `unregistered-persona` |
| `receive` | `pending` | `picked` | – | `thread` | `picked_at`, `picked_by_thread` |
| `receive` | `pending` | `done` | `read` | `thread` | odpoveď (`reply_to`) a `steer`: prevzatím vybavené |
| `resolve` | `picked` / `waiting` | `waiting` | – | `thread` | `otázka`/`čakám` s textom; udalosť `reply` iba pri prvej odpovedi; čakanie `check` (`čakám` 60 min, `otázka` 4 h), ak bola odpoveď doručená |
| `resolve` | `pending` / `picked` / `waiting` | `done` | `reply` / `settled` | `thread` | `hotovo` s textom (udalosť `reply`) / bez textu (riadok `recent` odosielateľovi, nezvoní); oznamy zatvorené |
| `resolve` | `picked` | `done` | `acked` | `thread` | `ack: true`; iba z `picked`, z `waiting` 409; riadok `recent` odosielateľovi |
| `resolve` | `done` | `done` | `reply` | `thread` | `cancels: X` nahradí súčasnú konečnú odpoveď X (iba jej odosielateľ); X dostane `cancelled_by` |
| `withdraw` | `held` / `pending` / `picked` / `waiting` | `done` | `withdrawn` | `thread` | iba odosielateľ; oznamy zatvorené; riadok `recent` príjemcovi |
| `bounce` | `pending` / `held` | `undelivered` | – | `code` | wake: adresát archivovaný alebo neexistuje; jednorazové čakanie `undelivered` (splatné hneď); oznam zatvorený |
| `close` | `pending` / `held` | `undelivered` | – | `thread` | `register-topic close`; ako `bounce` |
| `supersede` | `pending` | `superseded` | – | `thread` | otvorený oznam prejde na novú správu |
| `repair` | `pending` / `picked` / `waiting` | `done` | `reply` | `code` | štart: odpoveď zapísaná, originál nie (I5) |
| `repair` | `picked` / `waiting` | `waiting` | – | `code` | štart: priebežná odpoveď zapísaná, originál nie |
| `migration` | bez `status` | ktorýkoľvek | podľa 4b | `migration` | iba záznam 0.4 bez `status` (MG) |

Mimo prechodov (pole správy, `status` sa nemení): `expired_at`, `code_answer` (`checkExpired`), `wait_*` (`setWait`, `waitDueNow`, `wait-rang`, `wait-adopt`), uzavretie témy príjemcu pri `picked`/`waiting` (čakanie `recipient-closed`), `answer_*` (`resolve`, `repair`).

## 3. Invarianty a testy jadra

Testy jadra sú označené `// keep: core` a nesmú sa zmazať bez náhrady (časť 6): `tests/lifecycle.core.test.mjs` (server), `tests/client.core.test.mjs` (klient), štyri testy v `tests/wake.test.mjs` (wake).

| # | Invariant (ako ho drží kód) | Test jadra (presný názov) |
|---|---|---|
| I1 | Správa má práve jeden `status` z `STATUSES`; zapisuje ho iba `transition()` podľa `TRANSITIONS`, iný prechod je 409 so súčasným stavom; konečné stavy sa nemenia okrem `done → done` (`cancels`). | lifecycle.core: `I1: every transition of table 1b passes, any other is refused with 409 and the current state`; `I1: a refused resolve over HTTP is 409 with {status, resolution, answer_state, answer_id}`; `I1: ack from picked → done (acked); from waiting → 409 with the state` |
| I2 | `inbox` = `status=pending` (aj s `expired_at`, `code_answer`); `count`, `receive`, `obligations.inbox` a zvonček pošty wake (`/count` pred zvonením) čítajú iba `pending`. | lifecycle.core: `I2: resolve hotovo with text, without text and ack → not in count, receive, inbox`; `I2: a code answer on a pending message keeps it in the inbox; receive returns it`; `I2 receive of a reply and of a steer → done (read), never in the inbox nor owed (I8)` |
| I3 | `undelivered` vzniká iba z `pending`/`held` (`bounce`, `close`, záznam odpovede pri `resolve`); `bounce` prevzatej či vybavenej nič nezmení; `bounce` témy, ktorú teraz drží iné vlákno, nič nezmení (`bounced: false`). | lifecycle.core: `I3 undelivered only from pending/held: bounce of a picked or done message changes nothing; held → undelivered closes its notice (I6)`; `I9 closing the recipient topic keeps picked/waiting/done (waits due, recipient-closed); pending → undelivered (I3)`; wake: ``archived or gone holder: a steer/assist is undelivered instead of held for the discussion, the sender gets `undelivered <id>`; a topic held by another thread keeps it`` |
| I4 | Najviac jedna konečná odpoveď: druhé `resolve hotovo` → 409 so stavom; rovnaké `client_op_id` (`send`, `resolve`, `withdraw`) → pôvodné potvrdenie s `replayed: true`; to isté `client_op_id` inej operácie → 409. | lifecycle.core: `I4: a second resolve hotovo → 409; the same client_op_id → the original confirmation`; client.core: `outbox replayed twice is one answer: the same client_op_id gives the original confirmation (send and resolve)` |
| I5 | `resolve` zapíše najprv záznam odpovede, potom zrušenú odpoveď (`cancels`), potom originál; `repair()` pri štarte (pred prvou požiadavkou) dokončí originál podľa poslednej odpovede s `reply_to`: polia `answer_*`, prechod `repair`, oznamy, `cancelled_by`, udalosť `reply`. | lifecycle.core: `I5: a reply written without its original (stop between the writes) is finished by repair at start` |
| I6 | Otvorený oznam existuje iba pre nekonečnú správu: každý konečný prechod ho zatvorí (`decision` podľa `resolution`/`status`: `answered`, `settled`, `acked`, `read`, `withdrawn`, `discarded`, `superseded`, `undelivered`; oznam, o ktorom rozhodol `decide`, nesie rozhodnutie operátora); `POST /notice` ku konečnej správe → 409. | lifecycle.core: `I6: resolve, decide and withdraw close the open notice`; `I3 undelivered only from pending/held: …` (held → undelivered zatvorí oznam) |
| I7 | `wait_due_at` je pri nekonečnej správe; jediná výnimka je jednorazové čakanie `undelivered` na `undelivered`. Konečný prechod čakanie vynuluje; nepotvrdené zvonenie ostáva splatné bez ohľadu na vek; potvrdenie zdvojí odstup; po 4 potvrdeniach čakanie končí riadkom `no answer after 4 checks <id>`; jednorazové čakanie končí po jednom zvonení. | lifecycle.core: `I7: due + wait-rang: unconfirmed stays due after 2 h; a confirmation doubles; after 4 it ends with a recent row; done is not due`; wake: `wake off while a wait becomes due: a waker started later (new state file) rings it once in its first cycle and confirms; the next cycle does not ring` |
| I8 | `owed` = `picked`/`waiting` schránok vlákna (témy, ktoré drží, `to_thread`, `topics`, `persona`) bez ohľadu na `picked_by_thread`; odpoveď ani `steer` nikdy nie sú `owed`. | lifecycle.core: `I8: owed of thread B holds a message picked by thread A of the same topic; after resolve by A it does not`; `I2 receive of a reply and of a steer → done (read), never in the inbox nor owed (I8)` |
| I9 | Uzavretie témy príjemcu nemení `picked`/`waiting` (čakanie odosielateľa splatné, `recipient-closed`); uzavretie témy odosielateľa nemení vybavenie: `resolve` → 200, `answer_delivered: false`, `reason: sender-closed`, záznam odpovede `undelivered`. | lifecycle.core: `I9 closing the recipient topic keeps picked/waiting/done (waits due, recipient-closed); pending → undelivered (I3)`; `I9: resolve when the sender closed its topic → 200, answer_delivered=false, reason sender-closed` |
| I10 | Server a klient iného modelu si nič nezapíšu: požiadavka bez `X-Comms-Model: 3` (model 3) → 426 pred akýmkoľvek zápisom (aj záznamu klienta); klient pri inom modeli nič nečíta, nezapíše ani nezaradí, `flush` nechá outbox celý (nič do `rejected/`), CLI kód 3. | lifecycle.core: `I10: a request without X-Comms-Model: 3 → 426, nothing written`; client.core: `another model: nothing is written or queued, the client fails loudly, a flush keeps the outbox (4a, 3c, I10)` |
| I11 | Najviac jedna otvorená téma (`brain`, `project`, `topic`) naprieč strojmi; prevzatie na inom stroji ju presunie: starý záznam `closed` s `moved_to`, nekonečné správy jeho schránky na novom kľúči s nezmeneným `status`; odpoveď na správu zo starého kľúča ide po `moved_to`; rovnaká adresa `projekt/téma` v dvoch brainoch bez `brain` je 409 `ambiguous`, nič nezapíše. | lifecycle.core: `I11: takeover on another machine moves the open messages and closes the old record; a reply follows moved_to`; `I11: the same project/topic in two brains without brain is 409 ambiguous; own brain first`; `I11: a stop between the writes of a move is finished by repair at start`; `I11: a legacy record (brain null) is claimed only by its holder or when it has no open messages; a reply to from_brain null never goes to a guessed brain` |

Ďalšie testy jadra (nie invariant I, ale súčasť modelu):

| Oblasť | Test jadra |
|---|---|
| telo iba odosielateľovi a schránke (1d) | lifecycle.core: `1d: GET /message and receive?id without a right to the mailbox give no body` |
| migrácia 2→3 (schema 3) | lifecycle.core: `schema 3: migration of a schema-2 sample keys topics and messages by brain and machine, verifies, a second start changes nothing; a stop in the middle leaves schema 2` |
| migrácia 4b | lifecycle.core: `4b: migration of a 0.4 sample (bounced_at, replies without answer_state, busy, steer) → expected status; second start changes nothing`; `4b: verification catches a planted error: the service does not start and names the ids` |
| outbox po obnove (3c) | client.core: ``flush (3c): 409 with a final status drops the item with `already <resolution>`; another refusal goes to rejected/`` |
| jeden flush outboxu naraz (zámok `flush.lock`) | client.core: `one flush of an outbox at a time: two flushes and a send at once in one process, then two processes: every item reaches the service once, in order, no call fails`; `flush lock: a lock of a dead process or with an old heartbeat is taken over`; `flush lock still held when the wait ends: a send and a closed turn are queued behind the outbox, an open turn and obligations go on, nothing overtakes` |
| záloha `obligations` (3b) | client.core: `obligations backup (3b): offline answer is the last live one, marked, without recent, minus ids with a queued operation` |
| busy iba pre bežiace vlákno | wake: `a stale open turn record gives no busy: T3 not running → rung, nothing queued, a due wait rings; while T3 runs the thread its mail is held and rung after the run`; `a workflow thread T3 runs gets no ring behind the run (closed record too); picked in the run: none after it; past turnBusyMaxMs: rung behind the run`; `without the thread state an open record counts only within its deadline: past it the service writes no busy, a due wait rings, wake queues nothing` |

Overenie invariantov nad celým priečinkom (`verifyInvariants`: platný `status`, `resolution` iba pri `done`, žiadne pole 0.4, žiadna odpoveď ani `steer` v `owed`, žiadne `undelivered` s `picked_at`, žiadne čakanie na konečnej okrem `undelivered`, žiadny otvorený oznam pri konečnej) beží iba pri migrácii 4b (MG). Pri bežnom štarte beží iba `repair()` (I5); komentár v LC „and at every start“ kód nespĺňa.

## 4. API, CLI a budenie

### HTTP (HT, SV)

Každá požiadavka okrem `GET /health` a stránky operátora (`GET /`, formuláre `__form`) nesie `X-Comms-Model: 3`, inak 426 `{error, model: 3, got}`. Model 3: `X-Comms-Brain`, `X-Comms-Brain-Name` a `X-Comms-Machine` sú brain a stroj volajúceho (predvolená schránka, odosielateľ, registrácia). Kľúč: `Authorization: Bearer`, `?key=`, cookie `comms_key`; odosielateľ je vždy podľa kľúča.

| Operácia | Vstup | Potvrdenie | Chyby |
|---|---|---|---|
| `GET /health` | – | `{ok, instance_id, instance, pid, time, version, model: 3}` | – |
| `POST /send` | adresa (`project`+`topic`, `persona` [+`project` [+`topic`]], `maintainer: true` alebo `to_thread`), `level`, `text`, `ttl`, `supersede_key`, `thread_id`, `from_project`/`from_topic`, `client_op_id` | `{id, status, level, notice_id, expires_at, wait_due_at, project_known?, warning?}` | 400 (`reply_to`, `reply_state`, `cancels` patria `resolve`; `info`), 410 uzavretá téma |
| `GET /receive` | schránka (`project`+`topic`, `persona`, `thread`), `ids?` | iba `pending`; čítanie = prevzatie (`picked`, odpoveď a `steer` `done/read`) | 400 |
| `GET /receive?id=` | `id`, schránka | správa v akomkoľvek stave; schránka čakajúcu prevezme | 403 nie odosielateľ ani schránka, 404 |
| `POST /resolve` | `id`, `state` `hotovo`/`otázka`/`čakám`, `text?`, `ack?`, `cancels?`, `wait_for?`, schránka, `client_op_id` | `{id, status, resolution, answer_state, answer_id, answer_delivered, reason?}` | 400 (odpoveď na odpoveď, vlastná správa, priebežná bez textu), 403 mimo schránky, 404, 409 so stavom |
| `POST /withdraw` | `id`, `client_op_id` | `{id, status: done, resolution: withdrawn}` | 403 nie odosielateľ, 409 konečná |
| `GET /message?id=` | `id`, schránka | metadáta, `status`, `history`, `wait_*`; `text` iba odosielateľovi a schránke | 404 |
| `GET /obligations` | `thread`, `topics`, `project`, `persona`, `since` | `{inbox, owed, awaiting, recent, cursor, topics, server}` | 400 bez `thread`, `topics` aj `persona` |
| `GET /obligations?due=1` | – | `{due: [{id, level, status, wait_due_at, wait_n, wait_reason, holder, holder_machine, …}], server}` | – |
| `POST /wait-rang` | `id`, `rang_at?` (bez neho hodiny servera) | `{id, status, wait_due_at, wait_n, wait_reason}` | 403, 404, 409 konečná bez čakania |
| `POST /wait-adopt` | `id`, `due_at`, `base_s?` | `{id, status, wait_due_at, wait_base_s, wait_n, wait_reason}` | 400 odpoveď, 403 nie odosielateľ, 409 konečná |
| `POST /register-topic` (`close: true`) | téma, `holder` | `{closed, undelivered: [ids], recipient_closed: [ids]}` | 404, 409 |
| `POST /bounce` | `message_id`, `holder`, `reason` | `{bounced, status, reason?}` | 404 |
| ostatné | `count`, `notices`, `decide`, `turn`, `turns`, `clients`, `notice`, `events` | nad `status` | |

**Adresa vlákna (`to_thread`, operátor 2026-11-18):** novú správu vláknu podľa ID (nie odpoveď) smie poslať iba vlákno správcu: `thread_id` odosielateľa = `instance.json` `maintainer.thread` a, keď ho inštancia uvádza, `thread_machine` = `maintainer.machine`, inak 403. Cieľové vlákno musí server poznať: stroj vezme zo záznamu ťahu vlákna (`turns`), inak z jeho najnovšej odoslanej správy (`thread_machine`); bez stroja 404 (budenie by nevedelo, kde zazvoniť, a cudzí stroj by správu vrátil ako `undelivered`). Správa ide do schránky vlákna (`to_thread`, `to_thread_machine`) ako správa správcovi; odpoveď (`resolve`) ide späť vláknu správcu. Kombinácia s `project`, `topic`, `persona` či `maintainer` je 400. `thread_id` je deklarácia klienta (rovnaká dôvera ako kľúč; kľúče vydáva operátor). CLI: `send --to-thread <id>`, workflow `comms.mjs send --to thread/<id>`.

Zrušené bez náhrady (D-0089): `POST /ack`, `POST /settle`, `send` s `reply_to`, polia `state`, `acked_at`, `outcome`, `bounced_at`, udalosti `expired` a `undelivered`.

`client_op_id`: 1–100 znakov `[A-Za-z0-9._:-]`; uložené v `ops[]` správy (strop 10) s pôvodným potvrdením; `send` sa hľadá v indexe `client_op_id → id`.

### CLI (CLI) a klient (CL)

| Príkaz | Operácia |
|---|---|
| `send` | `POST /send` (bez `--reply-to`) |
| `receive [--id]` | `GET /receive`, `GET /receive?id=` |
| `resolve --id --state [--text] [--cancels] [--wait-for]` | `POST /resolve` |
| `settle --id` | `resolve` `hotovo` bez textu |
| `ack --id …` | `resolve` `hotovo` s `ack: true` |
| `withdraw --id` | `POST /withdraw` |
| `obligations […]` | `GET /obligations`; navyše `wake` = stav budenia tohto stroja (`running`, `pid`, `heartbeat_at`; chýba, kde nikdy nebežalo) |
| `obligations --due` | `GET /obligations?due=1` |
| `wait-adopt --id --due-at [--base-s]` | `POST /wait-adopt` |
| `message`, `count`, `flush`, `turn`, `register-topic`, … | ako v README |

Kódy CLI: 0 ok, 1 odmietnuté alebo použitie, 2 `comms unavailable`, 3 iná inštancia alebo iný model.

Outbox (CL `write`, `flushOutbox`): zápis ide do outboxu **iba** pri `UNAVAILABLE`; 426, 4xx a iná inštancia zlyhajú hneď. `flush` najprv overí model (iný → nič sa nepohne), potom v poradí: 2xx → hotovo; 409 s konečným `status` → zahodí s riadkom `already <resolution> <id>`; iné 4xx → `rejected/` + `.error.json`; výpadok, 426, 5xx → stop, zvyšok ostáva.

Jeden flush naraz (CL `flushOutbox`, LK `acquireTokenLock`): outbox posiela najviac jeden flush naraz, medzi procesmi aj v jednom procese (dve relácie jedného brainu, dve volania klienta v procese hooku). Dva súbežné flushe by každú položku poslali dvakrát (`turn` nemá `client_op_id`) a stratili poradie.

- Zámok je súbor `flush.lock` v priečinku outboxu (nie `*.json`, takže nie je položka). Vzniká výlučným vytvorením (`wx`) a drží `{pid, token, heartbeat_at, stale_ms}`. Vlastníkom je `token` jedného získania, nie `pid`: dva klienty jedného procesu sa vylučujú tiež.
- Držiteľ obnoví `heartbeat_at` pred každou položkou a zámok zmaže na konci (`finally`). Keď zámok už nenesie jeho `token`, flush zastaví (`busy: true`); zvyšok pošle nový držiteľ.
- Zámok mŕtveho `pid` alebo s `heartbeat_at` starším ako `stale_ms` držiteľa prevezme ďalší flush. `stale_ms` = 3 × `timeout` držiteľa, najmenej 10 s; jedna položka trvá najviac `timeout`.
- Druhý flush čaká (opakovanie po 25 ms) najviac svoj `timeout`, potom číta outbox znova: zvyčajne je prázdny a nič nejde dvakrát.
- Čakanie uplynie a iný flush ešte beží: flush nič nepošle a vráti `busy: true` s `remaining`. Volajúci zaradené zápisy nepredbehne: `write` (`send`, `resolve`, `withdraw`) zaradí svoju položku ako pri výpadku (`queued: true`, `message: outbox flush in progress`); `turn` `closed` rovnako (`queued: true`, `error: outbox flush in progress`); `turn` `open` (nikdy sa nezaraďuje) a `obligations` (čítanie) pokračujú bez vlastného flushu.
- Bežiaci flush číta outbox po každom prechode znova: položku zaradenú počas neho pošle sám. Položku zaradenú po jeho poslednom čítaní pošle ďalší flush.
- Súbor, ktorý pri mazaní alebo presune do `rejected/` už neexistuje, nie je chyba: dokončil ho iný flush.

Záloha (CL `obligations`): živá odpoveď (po `flush`) prepíše `obligations-cache.json` pre ten istý dotaz; pri výpadku posledná živá odpoveď s `offline: true`, `cached`, `age_s`, bez `recent`, bez `id` s operáciou v outboxe (z `inbox` každá, z `owed` `resolve`, z `awaiting` `withdraw`).

### `obligations`

| Časť | Obsah |
|---|---|
| `inbox` | `{n, ids: [{id, level, code_answer, reply_to, reply_state, expired_at}]}` = `pending` schránok vlákna |
| `owed` | `picked`/`waiting` schránok vlákna bez odpovedí a `steer`: `{id, level, from_project, from_topic, sender, status, picked_at, picked_by_thread, answer_state}` |
| `awaiting` | nekonečné správy odoslané vláknom (`thread_id`) alebo jeho témami (`from_*`), bez odpovedí: `{id, to, level, status, answer_state, code_answer, wait_due_at, wait_n, wait_reason}` |
| `recent` | riadky `{seq, at, kind, id, level, line}` za kurzorom: `settled <id>`, `acked <id>` (odosielateľovi), `withdrawn <id>` (príjemcovi), `no answer after 4 checks <id>`; kľúč záznamu `threads/<kľúč>.json` je vlákno, inak `topic:<p>/<t>` (`for_topic`) |
| `cursor` | najvyššie `seq`; `since=<cursor>` zmaže staršie riadky a uloží kurzor vlákna |
| `topics` | `{project, topic, registered, state, holder, holder_mode, closed_at}` |
| `server` | `{version, model, instance_id}` |

### Budenie je stav, nie udalosť

Čakanie odosielateľa sú polia správy: `wait_due_at`, `wait_n`, `wait_base_s`, `wait_reason`, `wait_rang_at`. Vzniká iba pri ceste späť (`thread_id` alebo `from_project`+`from_topic`) a nikdy pri odpovedi.

| Kedy (SV) | `wait_reason` | Splatné | Základ `wait_base_s` |
|---|---|---|---|
| `send` `blocker` / `assist` | `no-reply` | `created_at + základ` | 15 min / 60 min (`steer` čakanie pri `send` nemá) |
| lehota uplynula bez prevzatia (`checkExpired`, aj `held`) | `no-reply` | hneď (skoršie splatné ostáva) | podľa úrovne |
| kódová odpoveď `otázka`/`busy` | `check` | `teraz + 4 h` | 4 h |
| `resolve` `čakám` / `otázka` (doručená odpoveď) | `check` | `teraz + základ`, `wait_n = 0` | 60 min / 4 h |
| `bounce`, `close` (`→ undelivered`) | `undelivered` | hneď, jednorazovo | – |
| uzavretie témy príjemcu s `picked`/`waiting` | `recipient-closed` | hneď, jednorazovo | – |
| konečný prechod | – | vynulované (okrem `undelivered` vyššie) | – |

- **`obligations?due`**: správy s `wait_due_at <= teraz`; kľúč `wake` vidí všetky, iný kľúč iba správy, ktoré poslal (`mayRing`); `holder` = vlákno odosielateľa, inak držiteľ jeho zdrojovej témy.
- **`wait-rang`**: `wait_rang_at`; jednorazové čakanie končí (`wait_due_at = null`); inak `wait_n += 1` a `wait_due_at = rang_at + wait_base_s × 2^wait_n` (**zdvojenie**); pri `wait_n = 4` (**strop**, `WAIT_MAX_RINGS`) `wait_due_at = null` a riadok `no answer after 4 checks <id>` do `recent` odosielateľa; správa ostáva `owed` u príjemcu. Potvrdenie čakania, ktoré v `rang_at` ešte nie je splatné, nič nezmení.
- **Wake (WK `dueOnce`)** v každom cykle pred long-pollom (aj hneď po štarte): pri položke na svojom stroji preskočí bez potvrdenia (`other-machine`, `turns-unknown`, `this-round` = najviac jedno zvonenie čakania na vlákno v kole, `retry` = 60 s po zlyhanom zvončeku, `running` = vlákno je busy), inak zazvoní presný text a potvrdí `wait-rang` bez `rang_at` (hodiny servera). Trvalé zlyhanie zvončeka (archivované, neexistuje) sa potvrdí, lebo ďalší ťah nepríde. Nepotvrdené ostáva splatné.
- **Texty zvončekov** (P-047, nikdy telo): `no reply <id>` (`no-reply`, `check`), `undelivered <id>`, `recipient closed <id>`; nová pošta a odpoveď `you have mail from <odosielateľ> in topic <project>/<topic>` (najprv od koho, potom komu; bez témy `thread/<id>`; bez známeho odosielateľa `you have mail in topic <project>/<topic>`; odosielateľ je jeho téma `<project>/<topic>`, inak jeho kľúč, inak `thread/<id>`). Udalosti (`events`) sú iba `blocker`, `steer`, `assist`, `reply`, `discussion-closed`.

### Busy iba pre bežiace vlákno

| Kde | Pravidlo |
|---|---|
| server, kódová odpoveď (SV `recipientCodeAnswer`) | `otázka`: otvorený ťah s `waiting_operator`, alebo pri `steer`/`assist` zatvorený ťah s `discussion`. `busy`: `steer`/`assist`, otvorený ťah s `discussion` bez `waiting_operator` **a** záznam ťahu mladší ako `TURN_BUSY_MAX_MS` = 60 min od posledného zápisu (`opened_at`, `discussion_at`, `waiting_operator_at`; UT `turnFresh`). Starší záznam `busy` nedá a správa uplynie (`no-reply`). `blocker` diskusiou prejde. |
| wake (WK `threadNow`) | busy = otvorený záznam ťahu **a** vlákno nie je preč **a** (T3 `status` známy ? T3 ho vykonáva : záznam mladší ako 60 min). Zaradený zvonček platí iba za behom `run_at` a tým istým behom T3 (`activeRunId`); iný beh alebo vlákno, ktoré nebeží, značku zruší. |
| wake, zvonček busy vláknu (WK `handle`, `releaseHeld`) | Vlákno, ktoré T3 práve vykonáva (`status` známy a `running`; s otvoreným či zatvoreným záznamom ťahu aj bez neho; T3 sa pýta pri každej udalosti známeho držiteľa), zvonček nedostane: wake ho pozdrží (`held_reason: running`) a zazvoní, až keď ho T3 nevykonáva, a iba keď je správa stále `pending` (prevzatá, vybavená, stiahnutá či nedoručená nezvoní). Strop: po `turnBusyMaxMs` = 60 min od prvého pozdržania (`running_since`) zvonček zazvoní aj do bežiaceho vlákna, zaradený za beh ako doteraz (beh, ktorý neskončí, poštu nezadrží). Stav T3 sa v jednom kole uvoľňovania pýta raz na vlákno. Kým taký zvonček čaká, long-poll trvá najviac `runningCheckMs` = 5 s, takže zvonček príde do ~5 s po konci behu. Neznámy T3 stav: vlákno so záznamom ťahu dostane zvonček zaradený za beh (značka `queued`, ako doteraz; hook workflow prázdny zvonček zavrie bez volania modelu); pozdržaný zvonček sa pri neznámom stave uvoľní, keď záznam ťahu nie je otvorený alebo je starší ako 60 min (`turnFresh`), vlákno bez záznamu hneď (stratený zvonček je horší ako dvojitý). Archivované či chýbajúce vlákno → `undelivered` (nižšie). |
| wake, správa za zaradeným zvončekom (WK `handle`, `releaseHeld`) | Správa pre vlákno, ktoré už má zvonček zaradený za beh (`queued`), alebo pre nečinné vlákno zazvonené tým istým zvončekom pred chvíľou (`idleDedupeMs`), nezvoní hneď, lebo ten ťah ju uvidí; wake ju však **pozdrží** (`held_reason: queued`), nezahodí. Uvoľní ju, keď značka `queued` zanikne (`pruneQueued`: iný beh alebo `queuedStartMs` po zatvorení behu; ako v `handle` aj otvorený záznam, ktorý T3 nevykonáva, či iný beh T3) a nečaká zvonček toho istého textu (`bellKey`) z posledných `idleDedupeMs` bez otvoreného ťahu; potom zazvoní iba správa stále `pending`. Kým taká správa čaká, long-poll trvá najviac `runningCheckMs`. Opakovanie textu (`stopOnRepeat`) sa jej počíta pri uvoľnení; započítaná je vopred iba správa pozdržaná nad limitom (`counted`) a pripomienka. Nie zahodenie so `skip queued`, lebo zvonček zaradený do bežiaceho behu (steer) ten beh už spotreboval a správa po ňom ostala bez zvončeka, prečítaná až pri ďalšom pokyne operátora (doma-pc 2026-11-18, kolo 3 testu ping/pong). |
| wake, limit budenia (WK `handle`, `pairLimit`) | Dvojica tém (`pairKey`, oba smery) dostane najviac `limitPerHour` = 6 zvonení za hodinu; `blocker`, `steer` a **odpoveď na `blocker` či `steer`** (`type: reply` s `original_level`) 2× toľko, lebo odosielateľ blockera čaká zablokovaný. Nad limitom sa zvonček pozdrží (`held_reason: limit`) a zazvoní, keď počet zvonení v okne klesne pod limit; čas uvoľnenia v logu a v upozornení operátora = (n − limit + 1)-té najstaršie zvonenie v okne + 1 h, názov upozornenia uvádza skutočný limit. Rovnaký text `stopOnRepeat`-krát za sebou je slučka: zastaví sa bez uvoľnenia. |

### Identita témy: brain, stroj, projekt, téma (model 3, operátor 2026-11-18)

Operátor 2026-11-18: „skôr by mali rozlišovať aj brain aj stroj“ (stroj je súčasť identity, nie iba údaj držiteľa). Dva brainy s rovnakým menom projektu zdieľali jednu schránku `projekt/téma`; hlásenie o zdieľanom skille nevedelo adresovať vlákno, ktoré skill vyvíja. Posúdenie proti kódu (sonnet, 25 bodov) je zapracované.

**Kľúč a jeho zdroje**
- Kľúč témy je `brain` + `machine` + `project` + `topic`. Súbor `topics/<sha1 kľúča, 16 znakov>.json`, identita je obsah (`saveTopic` porovná všetky štyri polia; nie mená spojené `__`, lebo tie nie sú jednoznačné). Záznam nesie aj `brain_name` (čitateľný názov adresára brainu, nie identita).
- `brain`: `brain_id` v `policy.json` brainu (zapíše ho `brain init`, náhodná hodnota), inak prvých 12 znakov najstaršieho koreňového commitu gitu brainu (rovnaké vo všetkých klonoch, iné pre iný brain; plytký klon alebo brain bez commitu: chyba, nie odhad). Kópia brainu so zdieľanou históriou musí mať vlastné `brain_id`. Jedna funkcia workflow (`brainId`), výsledok pre session uložený.
- `machine`: jeden zdroj, `machineName()` workflow v tvare, ktorý normalizuje server (`normalizeMachine`). Workflow ho dá klientovi (`COMMS_MACHINE`) a pri inštalácii wake (`wake.machine`); záznam témy, ťahu, hlavička `X-Comms-Machine` a wake majú ten istý stroj. Kontrola workflow (`doctor`) hlási rozdiel stroja wake a klienta.
- Klient posiela `X-Comms-Brain` a `X-Comms-Brain-Name` (z nastavení workflow). Brain je povinný iba pri adresovaní alebo registrácii témy (`project`+`topic`) z brainu; požiadavky iba s vláknom (`to_thread`, `turn`, správca, `--maintainer`) ho nepotrebujú. Odosielateľ bez brainu (vlákno správcu) má `from_brain: null` a krok 3 rozlíšenia preskočí „vlastný brain“.

**Jedna otvorená téma naprieč strojmi (I11) a presun**
- Najviac jeden otvorený záznam (`brain`, `project`, `topic`). Registrácia na inom stroji pri otvorenom zázname iného stroja: ten istý `holder` (premenovaný stroj, vlákno pokračuje inde) presunie bez `takeover`; iný držiteľ bez `takeover` 409 (ako dnes) a nič sa nepresunie; s `takeover` alebo bez držiteľa starého záznamu presun.
- Presun: (1) zápis nového záznamu, (2) presun každej nekonečnej správy schránky (`held`, `pending`, `picked`, `waiting`: `to_machine`, história `op: move`, `status` sa nemení; idempotentné po správe), (3) riadky `recent` na nový kľúč, (4) starý záznam `closed` s `moved_to` ako posledný zápis. `repair()` pri štarte dokončí presun každej nekonečnej správy, ktorej kľúč ukazuje na záznam s `moved_to`.
- Presunutá `pending` správa vydá udalosť pre nového držiteľa (zvonček na novom stroji); wake starého stroja zahodí položky `held`, ktorých správa má iný `to_machine`.
- `moved_to` sa sleduje najviac 5 krokmi; registrácia, ktorá záznam znova otvorí, `moved_to` zmaže. Jediný rozlišovač `resolveKey()` (kľúč → otvorený záznam po `moved_to`) používajú všetky vyhľadania záznamu (`senderHolder`, `recipientThread`, `bounce`, `makeReply`, `due`, `replyEvent`, `afterDelivery`, `mailboxOwner`, `decide`).
- Metódy služby sú synchrónne v jednom procese (presun cez viac súborov nesmie mať prekladanie); tak ostanú.

**Adresy a porovnania**
- Správa: príjemca `to_brain`, `to_machine` + `project`, `topic`; odosielateľ `from_brain`, `from_machine` + `from_project`, `from_topic`. Odpoveď ide na adresu odosielateľa cez `resolveKey()`.
- Každé porovnanie adresy je na celom kľúči (`brain`, `machine`, `project`, `topic`): `store.topic`, `inMailbox`, `obligations` (`boxes`, `inBox`, `fromMe`), `supersede` (rovnaká adresa), `closeTopicMail`, `pending`, `senderHolder`, `recipientThread`, `bounce`, `makeReply`, `recipientCodeAnswer`. Porovnanie iba `project`/`topic` je chyba. Kľúč riadkov `recent` je `topic:<brain>/<project>/<topic>` (bez stroja, presun ich zachová).
- Udalosti (`eventBase`) nesú `brain`, `machine`, `from_brain`, `from_machine`. Dotaz schránky (`receive`, `count`, `obligations?topics=`) nesie `brain` a `machine` ako parametre (hlavička je iba predvolená hodnota); položka `projekt/téma` bez nich znamená brain a stroj požiadavky. Wake počíta s hodnotami udalosti, nikdy so svojimi.
- **Rozlíšenie adresy** pri `send` a `decide --target` (`project` + `topic`, voliteľne `brain` = ID alebo jednoznačné `brain_name`, `machine`): (1) `brain` aj `machine`: tá téma; (2) `brain` bez `machine`: otvorená téma brainu (I11); (3) bez `brain`: otvorená téma vlastného brainu odosielateľa, inak jediná otvorená téma (`project`, `topic`) iného brainu (odpoveď nesie `resolved: <brain_name>@<machine>`), viac ako jedna → 409 `ambiguous` so zoznamom `<brain_name> (<brain>)@<machine>` a nič sa nezapíše; žiadna → neregistrovaná téma (`held` + oznam) s `to_brain` = brain odosielateľa a `to_machine: null`. `projectKnown` počíta iba brain odosielateľa. Zápis v CLI a workflow: `[<brain>[@<stroj>]:]<projekt>/<téma>`; holé `<téma>` ostáva vlastný projekt a brain. Adresou odpovede je ID správy (`--reply-to`), nie štítok zvončeka.
- `held` správa sa doručí pri registrácii témy tým istým rozlíšením (`deliver`); oznam a stránka operátora ukazujú `<brain_name>@<machine>`.
- Persona: `@p/main` sa rozlišuje menom persony a jej brainom; správa persone bez registrovaného main má `to_brain: null` a prevezme ju prvá registrácia main. Main z druhého stroja: ten istý držiteľ presun, iný 409 bez `takeover` (ako I11).
- `to_thread` sa presunom nemení; odpoveď s `to_thread` aj témou platí, keď sedí aspoň jedno. Záznam ťahu (`turns/<vlákno>`) nesie `brain` iba ako štítok, nie identitu. Zvonček sa nemení (`you have mail from <od> in topic <projekt>/<téma>`, regex workflow); wake `endpoint` (`pairKey`, `directionKey`) je `brain/project/topic` bez stroja.

**Workflow**
- `registerTopic` registruje pod `topic.project`, keď ho téma má (D-0035), inak pod projektom session. Rozlíšenie témy v inom brainu podľa adresárov (`topicElsewhere`, `topicInOtherBrain`) sa ruší; platí rozlíšenie servera (kroky 1 až 3).
- `developed_in` zdieľaného skillu je `<brain>:<projekt>/<téma>`.
- Kľúče záloh klienta (`count`, `obligations`) obsahujú `brain` a `machine`.
- Outbox: telo zaradenej položky nesie `from_brain`, `from_machine` a cieľový brain z času zaradenia; položku bez brainu (model 2) flush neodošle, presunie ju do `rejected/` s adresou v riadku.

**Staršie záznamy a migrácia (schema 3)**
- Migrácia 2→3 je samostatný krok s vlastnou zálohou (`.bak-2-<čas>`), zápisom záznamov tém (`brain: null`, `machine: holder_machine` alebo `unknown`) a správ (`to_brain`/`from_brain: null`, `to_machine` zo záznamu témy), overením (každá nekonečná správa má kľúč existujúceho záznamu, najviac jeden otvorený záznam na `brain`, `project`, `topic`) a `meta.json` ako posledným zápisom; druhý štart nič nezmení, stop uprostred nechá schema 2.
- Záznam s `brain: null` je staršia téma. Registrácia modelom 3 ho prevezme (doplní `brain`, `machine`, presun ako pri I11, starý záznam `moved_to`), iba keď registruje jeho `holder`, alebo keď nemá nekonečné správy. Inak vznikne nový záznam, starý ostane a operátor dostane oznam na rozhodnutie (`decide`), lebo by inak prevzal poštu cudzieho brainu. Kým starý záznam existuje, rozlíšenie bez `brain` ho počíta ako tému iného brainu (krok 3).
- Odpoveď na správu s `from_brain: null`: cez `moved_to` starého záznamu, inak starý záznam ako `brain: null`, inak `undelivered` (`no-return-path`), nikdy doručenie do brainu odhadom.
- `MAIL_MODEL = 3` v LC aj CL; pošta a workflow sa vydajú spolu; klient modelu 2 dostane 426 a nič nezapíše (I10).

### Archivované či chýbajúce vlákno = `undelivered`

Wake pri T3 `status` `archived` alebo trvalej chybe (`PERMANENT_RING_FAILURE`: archived, not found, does not exist …) nezvoní ani nepozdrží (diskusia, bežiace vlákno) a volá `POST /bounce`: neprevzatá správa → `undelivered` a jednorazové čakanie odosielateľa → zvonček `undelivered <id>`. Server `bounce` odmietne (`bounced: false`), keď je správa prevzatá alebo vybavená (I3) alebo keď tému teraz drží iné vlákno (dostane ju nový držiteľ). Pozdržaná správa, ktorej držiteľa medzitým archivovali, prejde rovnako pri ďalšom kole (kontrola najviac raz za `goneCheckMs` 60 s).

## 5. Odchýlky implementácie od návrhu v3

| # | Odchýlka | Commit | Prečo |
|---|---|---|---|
| 1 | Migrácia 4b: `answer_origin=code` s `answer_id` sa číta ako odpoveď; kódová odpoveď na správe `acked` → `done/acked` (nie `picked`/`pending`). | `cfd252d` | P14 na kópii prod dát (986 záznamov): 0.4 nechal `code` pod skutočnou odpoveďou; poradie tabuľky dávalo 20 falošných `owed`, po oprave 6, všetky skutočné. |
| 2 | Prechody nad rámec 1b: záznam odpovede `nový → pending` a `pending → undelivered` (`resolve`), `decide pending → discarded`, `repair` (dva riadky), `supersede`, `close` ako vlastná `op`. | `cfd252d` | Každý zápis `status` musí ísť cez `transition()` (I1); cesty, ktoré návrh opísal iba slovom, dostali riadok tabuľky. |
| 3 | Pri uzavretej téme odosielateľa vznikne záznam odpovede v stave `undelivered` (čitateľný cez `receive --id`), originál `done` s `answer_delivered: false`. | `cfd252d` | Odpoveď sa nestratí ani pri nedoručení; návrh mal iba príznak. |
| 4 | Výnimka z I7: `undelivered` nesie jednorazové čakanie `undelivered`; `undelivered` a `recipient-closed` po jednom zvonení končia (bez zdvojenia a počítadla). | `cfd252d` | 1b žiada „čakanie splatné hneď“ pri prechode do konečného stavu, čo I7 doslova vylučuje; zvonček sa nesmie stratiť, a správa sama nepokročí, rozhodne odosielateľ. |
| 5 | Lehota platí aj pre `held` (`expired_at`, čakanie `no-reply`); kódová odpoveď iba pri `pending`. | `cfd252d` | Zachované správanie 0.3.10: odosielateľ pozdržanej správy sa dozvie, že ju nikto nevzal. |
| 6 | Riadky `recent` aj pre tému (`topic:<p>/<t>`), keď odosielateľ nemá vlákno. | `cfd252d` | Odosielateľ bez vlákna (skript so zdrojovou témou) by inak riadok nedostal. |
| 7 | `X-Comms-Model` nevyžaduje `GET /health` a stránka operátora. | `cfd252d` | Klient musí model najprv prečítať; prehliadač formulára hlavičku nepošle. |
| 8 | `send` odmietne aj `reply_state` a `cancels`; `resolve` vlastnej správy a odpovede je 400. | `cfd252d` | Odpoveď ide iba cez `resolve`; odpoveď na vlastnú správu by sa vrátila do toho istého vlákna. |
| 9 | `obligations?due` a `wait-rang` prijmú každý kľúč: `wake` všetky čakania, iný kľúč iba svojich správ (inak 403). | `2c561a1` | Wake na inom stroji (persona cez TCP) budí iba vlákna svojej persony. |
| 10 | Wake neposiela `rang_at`; čas potvrdenia určí server. | `2c561a1` | Wake s oneskorenými hodinami potvrdil pred splatnosťou, server potvrdenie ignoroval a čakanie sa neposunulo. |
| 11 | Najviac jedno zvonenie čakania na vlákno v kole, opakovanie zlyhaného po 60 s, trvalé zlyhanie sa potvrdí. | `2c561a1` | Bez nich búrka zvončekov alebo nekonečné zvonenie archivovanému vláknu. |
| 12 | Pilot vyberá poštu podľa `commsModel` workflow (scripts/comms-pair.mjs): workflow 1.8.0 (model 2) dostane dev poštu modelu 2. | `2c561a1` | Bez vrstvy kompatibility; pilot 2026-11-15 (workflow 4dd75da, pošta 9babdc1) overil P1–P4, P6, P8, P11, P15, P16, steer počas ťahu, zvonenie a undelivered pre archivované vlákno. |
| 13 | `busy` iba pre bežiace vlákno: server do 60 min od zápisu záznamu ťahu, wake podľa T3 `status`; zaradený zvonček viazaný na beh T3. | `43290a4` | Zastaraný otvorený záznam (prerušený ťah, archivované vlákno, stratený Stop) dával `busy` navždy a `steer` nedošiel. |
| 14 | Archivovaný alebo chýbajúci držiteľ: bez pozdržania a zvonenia, `bounce` → `undelivered` so zvončekom odosielateľovi; `bounce` nechá správu, keď tému drží iné vlákno. | `43290a4` | Pozdržanie za diskusiou archivovaného držiteľa nikdy neskončilo a správa uviazla. |
| 15 | CLI `obligations` vracia `wake` (stav budenia stroja); CLI `wait-adopt`. | `43290a4` | `obligations` nahrádza `count` v stavovom riadku, ktorý stav budenia ukazoval (comms-public#4); `wait-adopt` potrebuje workflow 1.8.0 (4b krok 5). |
| 16 | Model 3: nový záznam pri presune nesie `moved_from` (kľúč zdroja); `repair()` dokončí presun, keď zdroj ešte nie je `closed` s `moved_to` (kroky 2–4), iba smerom k novšiemu záznamu (`updated_at` zdroja je menší), a potom aj každú nekonečnú správu, ktorej kľúč ukazuje na záznam s `moved_to`. Zdroj zatvorený presunom stratí `moved_from`. | model 3 (dev) | `moved_to` je posledný zápis presunu, takže stop medzi zápismi ho ešte nemá; bez `moved_from` by `repair()` nedokončený presun nenašiel a ostali by dva otvorené záznamy (I11). Návrat B→A zastavený uprostred necháva B otvorené s `moved_from` A; bez podmienky novšieho záznamu by repair dokončil aj „A→B“ a oba záznamy by skončili `closed` s `moved_to` na seba (review 2026-11-18). Podmienka „`moved_from` zdroja neukazuje na cieľ“ z review sa nepoužila: práve B (zdroj návratu) ju má a návrat by sa nedokončil. |
| 17 | Model 3: schránka bez stroja (bez parametra `machine` aj bez `X-Comms-Machine`) je otvorený záznam brainu (I11), inak `unknown`; prázdny parameter `brain=` / `machine=` znamená presne kľúč bez nich (wake pri udalosti bez stroja). Registrácia bez stroja (bez `machine`, `holder_machine` aj hlavičky) ponechá otvorený záznam brainu na jeho stroji (bez presunu), `unknown` iba bez otvoreného záznamu. Kľúčový stroj registrácie je `machine`, inak `holder_machine`, inak hlavička. Presun na iný stroj bez `holder_machine` dá držiteľovi stroj nového záznamu. | model 3 (dev) | Požiadavka bez stroja (surové HTTP, staršie skripty) by inak nemala kľúč; `holder_machine` je stroj, kde držiteľ beží a číta schránku. |
| 18 | Model 3: `resolveKey()` kľúča bez vlastného záznamu (bez stroja alebo so strojom, ktorý záznam nemá) alebo s vlastným záznamom `closed` bez `moved_to` vráti otvorený záznam toho istého (`brain`, `project`, `topic`), inak ten záznam, inak najnovší z nich, a potom ide po `moved_to`. | model 3 (dev) | Kľúč odosielateľa nesie stroj hlavičky; migrované správy a odosielatelia bez stroja ho nemajú. V rámci jedného brainu je otvorený záznam jednoznačný (I11), takže to nie je odhad brainu; `from_brain: null` bez záznamu ostáva `no-return-path`. |
| 19 | Model 3: oznam staršej témy je druh `legacy-topic` s rozhodnutiami `move` (pošta staršieho záznamu prejde do nového, ako presun I11) a `ack` (nechať); jeden otvorený oznam na starší záznam (`dedupe_key`). Prevzatie staršieho záznamu (jeho držiteľ alebo bez nekonečných správ) nevyžaduje `takeover`. | model 3 (dev) | Návrh hovorí iba „oznam na rozhodnutie (`decide`)“; starší záznam nemá brain, takže kontrola držiteľa ako pri I11 by registráciu bez nekonečných správ zablokovala, čo návrh výslovne povoľuje. |
| 20 | Model 3: adresa persony s projektom a témou (`persona` + `project` + `topic`) nevolá krok 3 s 409: platí otvorená téma, ktorú persona vlastní, vo vlastnom brainu odosielateľa, inak v jedinom brainu, inak hlavné vlákno persony. Hlavné vlákno persony (`@p/main`) sa rozlišuje krokmi 1–3 (pri dvoch brainoch 409 `ambiguous`). | model 3 (dev) | Adresa persony bola vždy doručiteľná (inak do main); 409 by ju zmenilo na odmietnutie. |
| 21 | Model 3: `resolved` vracia `send` iba v kroku 3 pre tému iného brainu; `decide` (`deliver`, `redirect`) ho vracia vždy. Krok 3 bez otvorenej témy vráti uzavretý záznam vlastného brainu, inak najnovší uzavretý (bez `moved_to`) iného brainu, a `send` odpovie 410 ako doteraz. | model 3 (dev) | Operátor má vidieť, kam správa išla; 410 pri uzavretej téme ostáva správanie modelu 2. |
| 22 | Model 3: pozdržaná správa nesie `address` (`brain`, `machine` od odosielateľa); `decide deliver` rozlíši znova `address` + `project`/`topic` s `from_brain` odosielateľa, `redirect` rozlíši cieľ `[<brain>[@<stroj>]:]<projekt>/<téma>` s brainom operátora (`X-Comms-Brain`, zvyčajne žiadny). Starý záznam po presune má prázdneho držiteľa. | model 3 (dev) | „Doručí sa tým istým rozlíšením“ potrebuje pôvodnú adresu (`to_brain` je pri pozdržanej správe iba predvolený brain odosielateľa). |
| 23 | Model 3, klient: značku modelu 3 (`from_brain`, `from_machine`, pri `resolve` aj `brain` a `machine` schránky) pridá `queue()` pri zaradení; server berie výslovné `null` týchto polí ako „bez brainu/stroja“, nie hlavičku klienta, ktorý `flush` robí; `flush` presunie do `rejected/` položku `send`/`resolve`/`withdraw` bez kľúča `from_brain` s riadkom `rejected <cesta> <adresa> (model 2 item without brain)`. CLI `send` má cieľ v `--to [<brain>[@<stroj>]:]<projekt>/<téma>`; `--brain`, `--brain-name`, `--machine` sú brain a stroj klienta (hlavičky, schránka `receive`/`count`/`obligations`). | model 3 (dev) | `--machine` už bol stroj klienta; ten istý príznak ako cieľ by zmenil aj `from_machine`. |
| 24 | Model 3: kľúč riadkov `recent` témy bez brainu je `topic:/<projekt>/<téma>` (prázdny brain); migrácia 2→3 premenuje `topic:<p>/<t>`. `for_topic` riadku je `<brain>/<projekt>/<téma>`. Záznam ťahu nesie `brain` (štítok). `obligations.topics[]` nesie `brain`, `brain_name`, `machine`, `moved_to`. | model 3 (dev) | Riadky starších tém by sa po migrácii inak stratili. |
| 25 | Migrácia 2→3: správa bez záznamu svojej témy (napr. odpoveď do neregistrovanej témy odosielateľa) dostane `to_machine: null` a schránka témy ju nenájde; prevezme ju iba prvá registrácia `@p/main` (pošta persone). Overenie kontroluje iba správy so strojom. | model 3 (dev) | Bez záznamu niet stroja ani brainu; pridelenie by bolo odhadom (časť 4, staršie záznamy). |

## 6. Postup zmeny modelu

1. **Najprv tento dokument**: zmena prechodu, invariantu, poľa stavu, operácie API alebo budenia sa zapíše sem (časti 2–4) skôr ako do kódu, v tej istej téme.
2. **Nezávislé posúdenie** návrhu zmeny iným modelom **proti kódu** (nie iba proti návrhu) pred implementáciou; výhrady sa zapracujú sem.
3. **Test jadra**: nový či zmenený prechod (`TRANSITIONS`) alebo invariant má test jadra (`// keep: core`, názov nesie číslo invariantu alebo časti); test jadra sa maže iba s náhradou a zoznamom v commite.
4. **Pilot ako regresia**: pred vydaním pilotný scenár pošty (skill `pilot-sync`, scenáre P1–P16 návrhu v3 časť 5b, ktorých sa zmena týka).
5. **Jedno vydanie**: zmena rozhrania ide do pošty aj workflow v tom istom vydaní (P-073); zmena, ktorú druhá strana nevie čítať, zvýši `MAIL_MODEL` v LC aj CL a text nesúladu.
6. **Odchýlka** od tohto dokumentu, ktorú nájde implementácia, sa zapíše do časti 5 s commitom a dôvodom.

## 7. Čo nerobiť (chyby pôvodného modelu a návrhu v1)

- Nie stav správy aj v klientovi (`picked_assist`, `picked_at`, `reply_wait`, budíky `for:<id>`, mail wakery), lebo sa rozišiel so serverom: vybavená správa čakala v schránke, Stop pripomínal vybavenú, budík zvonil po vybavení.
- Nie tri nezávislé osi stavu na serveri (`state`, `answer_*`, `outcome`), lebo z nich vznikali nemožné kombinácie (prevzatá a vybavená správa dostala `undelivered`).
- Nie jednorazové udalosti budenia (`expired`, `undelivered`, preskočenie udalostí starších ako 60 min), lebo zvonček pri vypnutom alebo spadnutom wake sa stratil; čakanie je pole správy potvrdené `wait-rang`.
- Nie vrstva kompatibility (odvodené `state`/`outcome`, aliasy `ack`/`settle`, vetvy podľa verzie, režim legacy), lebo drží dva modely naraz a zdvojuje testy; nesúlad zlyhá nahlas (426, kód 3).
- Nie busy zo zastaraného záznamu ťahu, lebo záznam prerušeného či archivovaného vlákna ostal otvorený a `steer` sa zaradil za beh, ktorý nikdy neprišiel (steer nedošiel).
- Nie správa vláknu iného stroja iba ako odpoveď, lebo správca nemal ako začať test ani pokyn vláknu, ktoré nedrží tému (operátor 2026-11-18: „nové správy mu máš vedieť poslať“); adresa vlákna `to_thread` patrí iba vláknu správcu.
- Nie limit odpovede na `blocker` ako bežnej správy, lebo odpoveď, na ktorú odosielateľ zablokovaný čakal, ležala 36 min za limitom 6 za hodinu, kým samotný blocker mal 12 (projekt 2026-11-18).
- Nie zvonček zaradený za beh vlákna, ktoré T3 práve vykonáva (výnimky: neznámy stav T3, strop 60 min), lebo prišiel až po tom, čo vlákno poštu v tom behu prevzalo, a operátor videl prázdny zvonček ako „UserPromptSubmit operation blocked by hook“ (operátor 2026-11-18).
- Nie kódová odpoveď (`otázka`/`busy`) ako vybavenie mimo schránky, lebo neprevzatú správu by už nikto neprevzal (stratená správa); `code_answer` je pole nad `pending`.
- Nie odpoveď a ack ako dve operácie klienta, lebo výpadok medzi nimi nechal správu nevybavenú; `resolve` je jedna operácia s `client_op_id`.
