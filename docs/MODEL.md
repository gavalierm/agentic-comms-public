# Model pošty 2 (záväzný opis)

Tento dokument je **záväzný opis** modelu pošty 2 (D-0087, D-0089, D-0090). Opisuje skutočný kód (`src/server`, `src/client`, `src/wake`, `tests`) stavu po commitoch `cfd252d`, `2c561a1`, `43290a4`; východisko bol návrh `mail-service-design` v3, odchýlky od neho sú v časti 5.

Pravidlo D-0090: koncept pošty sa pri ďalších zmenách nesmie rozbiť. **Každá zmena pošty tento dokument najprv prečíta a zmení ako prvý** (časť 6). Keď sa kód a tento dokument rozchádzajú, je to chyba, ktorá sa opraví v kóde alebo vedome tu, nikdy sa neprehliadne.

Skratky: LC = `src/server/lifecycle.mjs`, SV = `src/server/service.mjs`, HT = `src/server/http.mjs`, MG = `src/server/migrate.mjs`, CL = `src/client/index.mjs`, CLI = `src/client/cli.mjs`, WK = `src/wake/index.mjs`, UT = `src/common/util.mjs`.

## 1. Princíp

| Pravidlo | Kde v kóde |
|---|---|
| Stav správy vedie **iba server**: jedno pole `status` (+ `resolution` pri `done`) v zázname `messages/<id>.json`; zapisuje ho iba `transition()`. | LC `transition`, SV `apply` |
| Klient (workflow aj `src/client`) **nedrží stav správ**. Drží iba outbox pre nedostupnú poštu (`send`, `resolve`, `withdraw`, `turn closed`, každý zápis s `client_op_id`) a zálohu poslednej odpovede `obligations` na čítanie (`obligations-cache.json`). Po obnove platí server bez zlučovania. | CL `write`, `flushOutbox`, `obligations` |
| Odpoveď aj vybavenie bez odpovede sú **jedna operácia** `resolve`. Čo vlákno dlhuje, na čo čaká a kedy ho zobudiť, odvodzuje server (`obligations`, `wait_due_at`). | SV `resolve`, `obligations`, `due` |
| Pošta a workflow sa **vydávajú spolu** (pošta 0.5.0 + workflow 1.8.0 = model 2), **bez vrstvy kompatibility**: žiadne odvodené polia, aliasy ani vetvy podľa verzie druhej strany. | LC `MAIL_MODEL = 2`, CL `MAIL_MODEL = 2` |
| Nesúlad modelu **zlyhá nahlas** a nič nestratí: server odmietne požiadavku bez `X-Comms-Model: 2` kódom **426** („mail model 2 (comms 0.5.0) and workflow 1.8.0 go together“) a nič nezapíše; klient pri `/health` s iným `model` alebo pri 426 hlási `MODEL_MISMATCH` („comms model <n> ≠ 2: update mail and workflow together“), CLI skončí **kódom 3**, nič nečíta, nezapíše ani nezaradí do outboxu. | HT, CL `ensureRunning`, `request`, CLI kódy |

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
| I10 | Server a klient iného modelu si nič nezapíšu: požiadavka bez `X-Comms-Model: 2` → 426 pred akýmkoľvek zápisom (aj záznamu klienta); klient pri inom modeli nič nečíta, nezapíše ani nezaradí, `flush` nechá outbox celý (nič do `rejected/`), CLI kód 3. | lifecycle.core: `I10: a request without X-Comms-Model: 2 → 426, nothing written`; client.core: `another model: nothing is written or queued, the client fails loudly, a flush keeps the outbox (4a, 3c, I10)` |

Ďalšie testy jadra (nie invariant I, ale súčasť modelu):

| Oblasť | Test jadra |
|---|---|
| telo iba odosielateľovi a schránke (1d) | lifecycle.core: `1d: GET /message and receive?id without a right to the mailbox give no body` |
| migrácia 4b | lifecycle.core: `4b: migration of a 0.4 sample (bounced_at, replies without answer_state, busy, steer) → expected status; second start changes nothing`; `4b: verification catches a planted error: the service does not start and names the ids` |
| outbox po obnove (3c) | client.core: ``flush (3c): 409 with a final status drops the item with `already <resolution>`; another refusal goes to rejected/`` |
| záloha `obligations` (3b) | client.core: `obligations backup (3b): offline answer is the last live one, marked, without recent, minus ids with a queued operation` |
| busy iba pre bežiace vlákno | wake: `a stale open turn record gives no busy: T3 not running → rung, nothing queued, a due wait rings; a ring queued behind run-1 does not stop the mail of run-2`; `without the thread state an open record counts only within its deadline: past it the service writes no busy, a due wait rings, wake queues nothing` |

Overenie invariantov nad celým priečinkom (`verifyInvariants`: platný `status`, `resolution` iba pri `done`, žiadne pole 0.4, žiadna odpoveď ani `steer` v `owed`, žiadne `undelivered` s `picked_at`, žiadne čakanie na konečnej okrem `undelivered`, žiadny otvorený oznam pri konečnej) beží iba pri migrácii 4b (MG). Pri bežnom štarte beží iba `repair()` (I5); komentár v LC „and at every start“ kód nespĺňa.

## 4. API, CLI a budenie

### HTTP (HT, SV)

Každá požiadavka okrem `GET /health` a stránky operátora (`GET /`, formuláre `__form`) nesie `X-Comms-Model: 2`, inak 426 `{error, model: 2, got}`. Kľúč: `Authorization: Bearer`, `?key=`, cookie `comms_key`; odosielateľ je vždy podľa kľúča.

| Operácia | Vstup | Potvrdenie | Chyby |
|---|---|---|---|
| `GET /health` | – | `{ok, instance_id, instance, pid, time, version, model: 2}` | – |
| `POST /send` | adresa (`project`+`topic` alebo `persona` [+`project` [+`topic`]]), `level`, `text`, `ttl`, `supersede_key`, `thread_id`, `from_project`/`from_topic`, `client_op_id` | `{id, status, level, notice_id, expires_at, wait_due_at, project_known?, warning?}` | 400 (`reply_to`, `reply_state`, `cancels` patria `resolve`; `info`), 410 uzavretá téma |
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

### Archivované či chýbajúce vlákno = `undelivered`

Wake pri T3 `status` `archived` alebo trvalej chybe (`PERMANENT_RING_FAILURE`: archived, not found, does not exist …) nezvoní ani nepozdrží (diskusia) a volá `POST /bounce`: neprevzatá správa → `undelivered` a jednorazové čakanie odosielateľa → zvonček `undelivered <id>`. Server `bounce` odmietne (`bounced: false`), keď je správa prevzatá alebo vybavená (I3) alebo keď tému teraz drží iné vlákno (dostane ju nový držiteľ). Pozdržaná správa, ktorej držiteľa medzitým archivovali, prejde rovnako pri ďalšom kole (kontrola najviac raz za `goneCheckMs` 60 s).

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
- Nie kódová odpoveď (`otázka`/`busy`) ako vybavenie mimo schránky, lebo neprevzatú správu by už nikto neprevzal (stratená správa); `code_answer` je pole nad `pending`.
- Nie odpoveď a ack ako dve operácie klienta, lebo výpadok medzi nimi nechal správu nevybavenú; `resolve` je jedna operácia s `client_op_id`.
