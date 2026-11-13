# agentic-comms: pošta a budenie

Samostatná služba pre správy medzi témami a agentmi (P-041 až P-047 v `agentic-flow/docs/PRINCIPLES.md`) a oddelený modul budenia (P-043). Nezávislá od harnessu aj od gitu. Iba Node.js 22+, bez závislostí (P-034). Stav: M4 hotové (služba, klient, oznamy, notifikácia), M6 hotové (modul `wake`, adaptér `t3`); M5 (napojenie workflow) prebieha v `agentic-flow`. Verzia 0.2.1: lehoty `assist`/`blocker` (TTL, systémová odpoveď od `comms` pri uplynutí), záznam ťahov a keepalive po reštarte T3. Verzia 0.2.2: samoreštart po nasadení a konfigurácia budenia z `<data-dir>/wake.json`. Verzia 0.3.8: pokračovanie ťahu s ďalším krokom (`turn --next`, oznam `turn-stalled`). Verzia 0.3.9: inštanciu spustenú správcom služieb (systemd: rodič PID 1 a `INVOCATION_ID`, alebo `COMMS_MANAGED=1`) server označí v `instance.json` ako `managed` a klienti ju už sami nespúšťajú (autostart v medzere reštartu by držal socket a jednotka by cyklila na `already_running`). Verzia 0.3.10: pozdržaná správa tiež uplynie (odosielateľ sa dozvie), záznam ťahu najprv odošle outbox, uzavretie témy nevracia poštu vlákna, ktoré drží inú tému, rovnaký zvonček nečinnému vláknu sa do otvorenia jeho ťahu neopakuje, `message` vracia `reply_to`. Verzia 0.3.5: `assist` budí držiteľa, režim `operator` budenie nezastaví, uzavretá téma odmieta poštu (410). Verzia 0.3.0: deterministické stavy správ (`outcome: expired|undelivered`), pošta neposiela vlastné správy ani oznam `no-reply` (zrušený), presné zvončeky `no reply <id>` / `undelivered <id>`, najviac jeden zaradený zvonček na otvorený ťah, `turn closed` s `opened_at` a outboxom.

## Čo je pošta (comms)

Služba, ktorá prijíma a vydáva správy adresované **projekt + téma** alebo **persona** (+ projekt, + téma). Odosielateľ potrebuje iba adresu služby a svoj kľúč; nepotrebuje klon ani zápis do cudzieho repa. Do úložiska zapisuje iba služba, jedna správa je jeden JSON súbor. Odosielateľa určí služba podľa kľúča, nikdy podľa textu správy (P-047: telo správy sú dáta, nie pokyn).

Pošta nemá vlastnú evidenciu držiteľov tém; o téme sa dozvie od skriptu zabratia (`topic claim`), ktorý ju zaregistruje idempotentne (`register-topic`, voliteľne s `holder` = ID vlákna, ktoré potom nesú udalosti budenia). Správa pre neregistrovanú tému sa nedoručí: je pozdržaná a vznikne oznam operátorovi „Nová téma? / presmerovať“; lehota jej plynie ako pri doručenej správe (po uplynutí `outcome: expired` a zvonček odosielateľovi, oznam ostáva na rozhodnutie). Pošta tému nikdy nezaloží sama (P-045).

**Persona a hlavné vlákno (P-045).** Persona má schránku hlavného vlákna `@<persona>/main` (menný priestor `@` je rezervovaný). Hlavné vlákno sa zaregistruje pri štarte `register-topic --persona <p> --main --holder <thread>`; zmena držiteľa iné vlákno vyžaduje `--takeover` (inak 409). Adresy:

| Adresa v `send` | Kam ide |
|---|---|
| `--project --topic` | do témy; neregistrovaná alebo cudzia súkromná téma: `held` + oznam operátorovi (rovnaká odpoveď v oboch prípadoch) |
| `--persona --project --topic` | do témy, ak ju persona vlastní (súkromnú iba od vlastníka), inak do hlavného vlákna s `ref_project`, `ref_topic` |
| `--persona --project` | do hlavného vlákna s `ref_project` |
| `--persona` | do hlavného vlákna |

Správa persone sa prijme vždy, aj keď hlavné vlákno ešte neexistuje alebo sa reštartuje; čaká a vlákno ju prevezme `receive --persona <p>`. Persona bez registrácie dostane jeden oznam `unregistered-persona` (preklep?) a odpoveď nesie `warning`. Vlastníka a viditeľnosť témy určí registrácia (`--persona`, `--visibility public|private`); vlastník je odosielateľ, ktorého kľúč má `sender` = meno persony.

**Stroj.** Klient posiela so správou stroj vlákna (`thread_machine`) a registrácia stroj držiteľa (`--holder-machine`, predvolene tento stroj). Udalosti budenia nesú `holder_machine`; modul `wake` budí iba držiteľov na svojom stroji (P-043). Názov stroja: `--machine`, `COMMS_MACHINE`, `machine` v `--config`, inak `os.hostname()` malými písmenami.

## Inštalácia a spustenie (pre laika)

Potrebujete iba **Node.js 22 alebo novší** (`node --version`) a **Git**.

Verzie (P-051): `main` = stabilný stav, vydania sú tagy `vX.Y.Z`, vývoj v branchi `dev`. Produkčná inštancia beží z klonu na tagu, testovacia (pilot) z vlastného klonu na `dev`; každá má vlastné `init`, kľúče a proces budenia. Aktualizácia produkcie: `git fetch --tags && git checkout v<nová-verzia>` a reštart služieb; testu: `git pull --ff-only` na `dev`.

### Lokálne (Linux, macOS, Windows)

```sh
git clone https://github.com/gavalierm/agentic-comms-public.git agentic-comms
cd agentic-comms
git checkout "$(git tag -l 'v*' --sort=-v:refname | head -1)"   # produkcia: najnovší tag vydania; test (pilot): branch dev
node bin/comms.mjs init --instance pilot --sender <vaše-meno>
```

`init` vytvorí dátový priečinok inštancie, `instance_id` a prvý kľúč; kľúč si odložte (vytlačí sa iba raz, neskôr `comms key list`). Dátový priečinok je:

| OS | Priečinok |
|---|---|
| Linux | `~/.local/share/comms/<instance>` |
| macOS | `~/Library/Application Support/comms/<instance>` |
| Windows | `%LOCALAPPDATA%\comms\<instance>` |

Prepis: `--data-dir <cesta>` alebo premenná `COMMS_DATA_DIR` (celá cesta inštancie), prípadne `COMMS_BASE_DIR` (nadradený priečinok).

Službu **netreba spúšťať ručne**: každý príkaz klienta (`health`, `send`, `receive` …) overí, či beží, a ak nie, spustí ju na pozadí. Lokálne počúva iba na unix sockete `<data-dir>/comms.sock` (Linux/macOS) alebo na named pipe `\\.\pipe\comms-<hash>` (Windows); žiadny sieťový port, žiadny dialóg firewallu. Socket je zároveň zámok: druhé spustenie iba oznámi `already_running`.

```sh
node bin/comms.mjs health --instance pilot              # spustí službu, ak nebeží, a vráti instance_id
node bin/comms.mjs serve --instance pilot               # ručné spustenie v popredí (Ctrl+C ukončí)
node bin/comms.mjs stop --instance pilot                # zastaví službu
```

Pre každý ďalší projekt alebo agenta vytvorte vlastný kľúč: `node bin/comms.mjs key add --instance pilot --sender <meno>`. Kľúč pilotného projektu zapíšte do `<brain>/local/comms.json` (necommitované), napríklad:

```json
{ "instance": "pilot", "key": "<kľúč>", "instance_id": "<id z init>", "outbox": "<brain>/local/outbox" }
```

a klienta volajte s `--config <brain>/local/comms.json` (alebo `COMMS_CONFIG`). Klient porovná `instance_id` služby s očakávaným a pri nezhode skončí chybou (kód 3).

Stránka oznamov je HTML a potrebuje TCP (prehliadač nevie otvoriť unix socket). Zapnite ho iba na localhost:

```sh
node bin/comms.mjs config set-tcp 127.0.0.1:7680 --instance pilot
node bin/comms.mjs stop --instance pilot && node bin/comms.mjs health --instance pilot
```

Potom otvorte `http://127.0.0.1:7680/?key=<kľúč>` (kľúč sa uloží do cookie). Bez TCP sú oznamy dostupné príkazom `comms notices`.

### Na serveri (Linux, trvalý beh, prístup zvonku)

1. Rovnaká inštalácia a `init` ako lokálne, pod účtom, ktorý službu bude prevádzkovať.
2. TCP na localhost: `node bin/comms.mjs config set-tcp 127.0.0.1:7680 --instance pilot`. Služba vyžaduje pri každej požiadavke kľúč (`Authorization: Bearer <kľúč>`), okrem `/health`.
3. HTTPS a prístup zvonku rieši **reverzná proxy** (nginx, Caddy …) smerujúca na `127.0.0.1:7680`. Pošta sama HTTPS nerieši.
4. Trvalý beh: `systemd` služba používateľa, vzor pre novú inštaláciu `~/.config/systemd/user/<služba pošty>.service` (existujúce nasadenie má vlastné jednotky, pozri jeho záchranný návod):

   ```ini
   [Unit]
   Description=agentic comms (pilot)
   [Service]
   ExecStart=/usr/bin/node <klon agentic-comms>/bin/comms.mjs serve --instance pilot
   Restart=always
   [Install]
   WantedBy=default.target
   ```

   `systemctl --user enable --now comms` (a `loginctl enable-linger <user>`, aby bežala aj bez prihlásenia).

   **Systémové jednotky prod:** `deploy/systemd/comms-prod.service` a `deploy/systemd/wake-prod.service` sú všeobecné (cesty `bin/…` relatívne k `WorkingDirectory`). Hodnoty servera (`User`, `Group`, `WorkingDirectory` = klon comms, `HOME`, pri budení `T3CODE_HOME`, `PATH` s `t3`, `After=` jednotky T3) dodá drop-in `<jednotka>.service.d/*.conf` z repa servera.

   **Nasadenie novej verzie:** stačí prepnúť klon (napríklad `git -C <klon agentic-comms> checkout <tag>`); služba sa do 30 s reštartuje sama. Služba aj `wake run` si pri štarte zapamätajú revíziu inštalácie (obsah `.git/HEAD`, pri refe aj SHA) a každých 30 s ju porovnajú. Pri zmene zalogujú `install changed <old> -> <new>, exiting for restart`, čisto sa ukončia (zatvoria socket, zapíšu stav wake) s kódom 0 a systemd ich spustí v novej verzii; preto `Restart=always` (s `on-failure` by sa po kóde 0 nespustili). Bez `.git` (inštalácia z balíka) sa nekontroluje nič. Vypnutie: `--no-self-restart`, `COMMS_SELF_RESTART=0` alebo `"self_restart": false` (v `--config`; pri službe aj v `instance.json`, pri `wake` aj vo `wake.json`).
5. Vzdialený klient (iný stroj) používa `--url https://posta.example/` alebo `COMMS_URL` plus `--key` a `--instance-id` (alebo konfiguračný súbor s `url`, `key`, `instance_id`). Pri `--url` klient službu nespúšťa.
6. E-mail pri `blocker`: `node bin/comms.mjs config set-notify "sendmail -t" --instance pilot`. Príkaz dostane na stdin text s hlavičkami `To:` (z `notify.to` v `instance.json`) a `Subject:` a v premenných `COMMS_NOTICE_*` údaje oznamu. Bez príkazu platí iba stránka oznamov.

Zmeny `config` platia po reštarte služby (`stop` a znovu `serve`/`health`).

### Windows

Node 22 LTS a Git for Windows, príkazy rovnaké (`node bin\comms.mjs …`). PowerShell: najnovší tag `git checkout (git tag -l 'v*' --sort=-v:refname | Select-Object -First 1)`. Lokálny endpoint je named pipe `\\.\pipe\comms-<hash>`, nie port: dialóg firewallu sa neobjaví (overené na Windows 11, Node 24, v0.4.2: pri autoštarte služby, `wake run` ani pri samoreštarte nevzniklo pravidlo firewallu a node nepočúva na TCP porte).

## Rozhranie klienta

`node bin/comms.mjs <príkaz> [voľby]`. Výstup je jeden JSON objekt na stdout; chyba je JSON `{ "error", "code" }` na stderr. Návratové kódy: `0` ok, `1` odmietnuté alebo chybné použitie, `2` `comms unavailable`, `3` iná inštancia.

Spoločné voľby (prednosť: voľba > premenná > `--config` > predvolené): `--instance` (`COMMS_INSTANCE`), `--data-dir` (`COMMS_DATA_DIR`), `--config` (`COMMS_CONFIG`, JSON s kľúčmi `instance`, `data_dir`, `url`, `key`, `instance_id`, `outbox`, `client_dir`, `thread_id`, `machine`, `timeout`, `autostart`), `--url` (`COMMS_URL`), `--key` (`COMMS_KEY`), `--instance-id` (`COMMS_INSTANCE_ID`), `--thread` (`COMMS_THREAD_ID`, inak `T3CODE_THREAD_ID`), `--outbox` (`COMMS_OUTBOX`), `--timeout <ms>` (predvolené 2000), `--no-start`.

| Príkaz | Voľby | Výstup |
|---|---|---|
| `init` | `--instance`, `--sender` | `{ created, instance, instance_id, data_dir, socket, sender, key }` |
| `key add\|list\|remove` | `--sender` | kľúče inštancie (zapisuje priamo do `keys.json`) |
| `config show\|set-notify "<príkaz>"\|set-tcp <host:port>\|clear-tcp` | | konfigurácia inštancie |
| `serve` | `--tcp <host:port>` | beží v popredí; ak už beží: `{ already_running: true, instance_id, pid }` |
| `stop` | | `{ stopped, pid }` |
| `health` | | `{ ok, instance_id, instance, pid, time }` (spustí službu, ak nebeží) |
| `send` | `--project --topic` alebo `--persona [--project [--topic]]`, `--level blocker\|steer\|assist --text <text\|->` (povinné okrem odpovede; odpoveď zdedí úroveň, nižšia sa povýši; `info` je zrušený, chyba 400 s platnými úrovňami) `[--from-project --from-topic] [--ttl <dĺžka>] [--cancels <id>] [--supersede-key <kľúč>]`; odpoveď: `--reply-to <id> --state hotovo\|otázka\|čakám [--wait-for <čo>] --text` | `{ id, state: pending\|held, level, reply_state?, level_raised_from?, notice_id, expires_at, project_known?, warning? }` (`project_known` iba pri `held`: projekt má aspoň jednu otvorenú registrovanú tému); pri výpadku `{ queued: true, outbox_file, message: "comms unavailable" }` |
| `receive` | `--project --topic`, `--persona <p>` alebo `--thread <id>`, `[--id <id> ...]`, `[--ack]` | `{ messages: [{ id, project, topic, persona, ref_project, ref_topic, to_thread, level, text, sender, thread_id, from_project, from_topic, reply_to, reply_state, wait_for, cancels, cancelled_by, created_at, state, picked_at }] }`; čítanie tela = prevzatie (`picked_at`, iba prvé čítanie); `--id` číta len zadané správy |
| `ack` | `--id <id> …` | `{ acked: [ids], already: [ids] }` – správa prestane byť čakajúca; neznáme id → 404 (nič sa neuzná), už uznané sú v `already` |
| `count` | `--project --topic`, `--persona` alebo `--thread` | `{ total, blocker, ids, cached, unavailable, first_report? }`; `ids` = čakajúce správy podľa `created_at`, `items` = `[{ id, level, reply_to, reply_state, cancels, answer_state, answer_at, to_thread }]` pre triedenie bez tela (počítanie nič neprevezme, vždy živý dotaz, od 0.3.11 bez cache úspešnej odpovede; pri výpadku posledná známa hodnota a nový pokus najskôr po 60 s; starší server `ids` nevracia) |
| `message` | `--id <id>` | `{ id, state, level, reply_to, reply_state, answer_state, answer_at, answer_id, answer_origin, cancels, cancelled_by, project, topic, to, outcome, replied, expires_at, ttl_s, picked_at, undelivered_holder, undelivered_reason }` – stav správy bez tela; `to` = `<project>/<topic>` alebo `thread:<id>`, `outcome` = `null` \| `expired` (lehota uplynula bez odpovede) \| `undelivered` (adresáta sa nedalo zobudiť natrvalo), `replied` = existuje odpoveď, `answer_state` = jej stav (`hotovo` konečná, `otázka`/`čakám` priebežná; `null` bez odpovede) s časom `answer_at`, `answer_origin` = `reply` (skutočná odpoveď) alebo `code` (pošta zapísala `otázka`, lebo adresát čaká na dialóg operátora; `answer_id` je vtedy `null`), `cancelled_by` = ID správy, ktorá túto odpoveď zrušila; `reply_to` = ID správy, na ktorú táto odpovedá (inak `null`); `state` ostáva `pending`, kým adresát správu nevyzdvihne |
| `turn` | `--state open\|closed [--thread <id>] [--machine <stroj>] [--project <p> --topic <t>] [--holder-mode operator\|auto] [--opened-at <iso>] [--next <krok>] [--discussion open\|closed]` | `{ ok, turn: { thread, machine, project, topic, holder_mode, state, opened_at, closed_at, close_reason, next, keepalive: { count, last_at } } }`; `closed` s `--next` uloží ďalší krok z handoveru (najviac 300 znakov) pre pokračovanie; `closed` s `--opened-at` (hodnota `turn.opened_at` z odpovede na `open`) zatvorí iba ten ťah, inak `{ ok: true, stale: true }` a záznam sa nemení; lehota najviac 1 s, pri výpadku `{ ok: false, unavailable: true }` s kódom 0, `closed` navyše ide do outboxu (`queued: true`), `open` nie; `--waiting-operator true\|false` (bez `--state`, voliteľne s `--opened-at`) nastaví príznak otvoreného ťahu „vlákno čaká na dialóg operátora“ (P-061); ťah bez otvoreného záznamu alebo s iným `opened_at` vráti `{ ok: true, ignored: true }`; `--discussion open\|closed` (D-0015, D-0022) zapíše, či vlákno diskutuje s operátorom: `open` pri ťahu od operátora alebo po ťahu so stavom `otázka`, `closed` s ťahom so stavom `hotovo`/`čakám`; diskusia je otvorená pri otvorenom ťahu s `discussion` alebo `waiting_operator` a pri zatvorenom s `discussion` (zatvorenie keepalive ju ponechá); bez voľby (starší workflow) iba počas dialógu; koniec diskusie vyšle udalosť `discussion-closed` |
| `notices` | `--all` | `{ notices: [{ id, kind, title, state, message_id, project, topic, level, sender, thread_id, options: [{ id, label, needs_target }], text, waiting_since, decision, … }] }` |
| `decide` | `--notice <id> --decision <id> [--target project/topic]` | `{ notice_id, decision, delivered_to? }`; pozdržanú správu po lehote `deliver` ani `redirect` nedoručí: chyba 409 s `details: { message_id, expired_at, decision }`, oznam ostane otvorený na `discard` |
| `register-topic` | `--project --topic --subject [--persona <p>] [--visibility public\|private] [--holder <thread>] [--holder-mode operator\|auto] [--holder-machine <stroj>] [--takeover]`, alebo `--persona <p> --main [--holder …]`; uvoľnenie držiteľa: `--release [--holder <thread>]`; uzavretie (téma dokončená a archivovaná): `--close [--holder <thread>]` | `{ registered, created, topic }` (+ `bounced` pri `--close`) (409: iný držiteľ bez `takeover`; 403: persona registruje iba jej kľúč) |
| `events` | `--since <seq>\|latest --wait <ms>` | `{ events: [{ seq, type: blocker\|reply, project, topic, holder, holder_mode, message_id, sender, thread_id, from_project, from_topic, text_hash, … }], next }` (long-poll pre modul budenia; `latest` vráti iba aktuálne `next`) |
| `flush` | | `{ sent, rejected, remaining }` odošle outbox |

Sémantika:

- `send` bez `--reply-to` ide na `project/topic`; `--from-project/--from-topic` a `--thread` hovoria, kam majú prísť odpovede. `send --reply-to <id>` ide do vlákna (`to_thread`) a témy pôvodného odosielateľa; neregistrovaná téma sa pri odpovedi nekontroluje. Odpoveď nikdy neznižuje naliehavosť (od 0.3.7): nižšiu úroveň než má pôvodná správa služba zvýši na jej úroveň (odpoveď vráti `level` a `level_raised_from`); odpoveď nemá lehotu (`expires_at: null`) a nevytvára oznam operátorovi. Odpoveď nesie stav `reply_state`: `hotovo` (konečná), `otázka` (rozhoduje operátor adresáta) alebo `čakám` (adresát čaká na udalosť, `wait_for`); odpoveď bez stavu (starší klient) je `hotovo`. Na správu je jedna konečná odpoveď: ďalšiu odpoveď po `hotovo` služba odmietne (409), odpoveď na odpoveď tiež (400); nová požiadavka je nová správa s úrovňou. Zmena konečnej odpovede je nová správa s `cancels: <id odpovede>` (`--cancels`); služba zrušenej odpovedi zapíše `cancelled_by` a zrušiť ju smie iba jej odosielateľ (inak 403).
- `receive` je nedeštruktívne (triáž iba číta); správa prestane čakať po `ack` (alebo `receive --ack`). Výnimka: konečná odpoveď (`hotovo`) sa potvrdí sama pri prevzatí (`receive`, `state: acked`), samostatné `ack` netreba (starý `ack` vráti `already`); priebežná (`otázka`, `čakám`) čaká na `ack`.
- `supersede_key` (`--supersede-key`, nová správa bez `--reply-to`, najviac 200 znakov): správa toho istého odosielateľa na tú istú adresu s rovnakým kľúčom nahradí ešte neprevzatú (aj po lehote): stará dostane `state: acked`, `outcome: superseded`, `superseded_by`, v schránke čaká iba nová. Oznam `blocker` správy s kľúčom vznikne až keď ju do lehoty nikto neprevzal (`checkExpired`), jeden na reťaz nahradených správ; blocker bez kľúča ho vytvára pri doručení ako doteraz. Prevzatá správa sa nenahrádza.
- Lehota (TTL, P-042): lehota je na PREVZATIE správy (čítanie tela: `receive --id` alebo `receive`, záznam `picked_at`), nie na odpoveď. Každá správa okrem odpovede ju má: `blocker` a `steer` 5 min, `assist` 30 min; `--ttl 90s|5m|30m|<sekundy>` môže lehotu skrátiť, nie predĺžiť (nad maximom je chyba 400, `0` tiež); odpoveď (`--reply-to`) lehotu nemá. Bez `--ttl` platí `ttl` v `instance.json` (`"ttl": { "blocker": "5m", "steer": "5m", "assist": "30m" }`, to sú aj predvolené hodnoty a maximá; `0` alebo neplatná hodnota → predvolená, nad maximom → maximum, záznam v logu; zmena platí po reštarte služby). Služba kontroluje lehoty každých 30 s a pri štarte (lehota prežije reštart). Správa uplynie, ak nebola prevzatá (`picked_at` najneskôr v lehote), je stále `pending`, nemá odpoveď (`--reply-to` na ňu) ani výsledok (`outcome`); potom dostane `outcome: expired` a `expired_at` a služba vyšle **jednu udalosť `expired`** pre odosielateľa (`holder` = jeho vlákno `thread_id` s `thread_machine`, inak držiteľ jeho `from_project/from_topic`). Služba pritom nepošle žiadnu správu ani oznam operátorovi (oznam `no-reply` z 0.2.x je zrušený; staré otvorené oznamy ostávajú rozhodnuteľné). Bez cesty späť (bez vlákna aj zdrojovej témy s držiteľom) iba záznam v logu. Výnimka (od 0.4.9, P-061 V1): ak má adresátovo vlákno otvorený ťah s príznakom `waiting_operator` (`turn --waiting-operator true`, nastaví hook pri dialógu `AskUserQuestion`, zruší ho PostToolUse, koniec ťahu alebo keepalive), správa neuplynie: služba jej zapíše `answer_state: otázka`, `answer_origin: code` a vyšle tú istú udalosť `expired` s `answer_state: otázka`; modul budenia ju pri takom stave zvoní (nepreskočí ako `answered`) a odosielateľ z `message --id` vidí stav `otázka`, nie „no reply“. Príznak je súčasť záznamu otvoreného ťahu: po páde ťahu ho zatvorenie ťahu (keepalive) zruší. Odpoveď, ktorá ostane v stave `otázka` bez konečnej odpovede 24 h, vyvolá jeden oznam operátorovi (`question-open`, `dedupe_key: question:<id>`). Pôvodná správa ostáva v schránke adresáta (`state: pending`); neskorá skutočná odpoveď odosielateľa zobudí bežnou udalosťou `reply` (overené testom `levels`/`wake`). Rovnako odpovie služba pri `steer` a `assist`, ak adresát diskutuje s operátorom (`turn --discussion open`, D-0015, D-0022); `blocker` diskusiou prejde a uplynie ako doteraz.
- Nedoručiteľná správa (`POST /bounce`, volá modul budenia, keď sa adresát nedá zobudiť natrvalo: vlákno archivované alebo neexistuje): raz na správu `outcome: undelivered`, `undelivered_at`, `undelivered_holder`, `undelivered_reason` a jedna udalosť `undelivered` pre odosielateľa (holder ako pri `expired`); žiadna správa, žiadny oznam, správa ostáva `pending`. Správy od `comms` (staré dáta 0.2.x) sa nevracajú. Nedoručiteľná správa už neuplynie.
- Ťahy (keepalive): hook workflow volá `turn --state open` na začiatku ťahu a `turn --state closed` na konci. Záznam je jeden na vlákno; nové `open` začne nový ťah s novým `opened_at`.
- Pozdržaná správa (`state: held`, neregistrovaná téma alebo cudzia súkromná téma): odpoveď na `send` nesie `project_known` (D-0024): `true`, keď projekt správy má aspoň jednu otvorenú registrovanú tému (aj súkromnú), inak `false` (žiadna alebo iba uzavreté). Hodnota závisí iba od projektu, nie od témy, takže neprezradí, či téma existuje (P-045): neznáma aj cudzia súkromná téma toho istého projektu dostanú rovnakú odpoveď. Odosielateľ tak rozlíši novú tému známeho projektu od chybného projektu.
- Oznamy: `unregistered-topic` (s `project`, `topic` a `message_id` pozdržanej správy) s rozhodnutiami `redirect` (vyžaduje `--target`), `deliver` (téma medzitým zaregistrovaná; pri cudzej súkromnej téme do hlavného vlákna vlastníka), `discard`; `redirect` aj `deliver` správu po lehote odmietnu s 409 (jedna kontrola pre každé rozhodnutie okrem `discard`), lebo odosielateľ už dostal `no reply <id>` a mohol ju poslať znova alebo nahlásiť inak (issue): doručenie či presmerovanie by ju zdvojilo, oznam ostane otvorený na `discard`; `blocker` s `ack`, `discard`; `unregistered-persona` s `ack`, `discard` (uzavrie ho aj registrácia hlavného vlákna); `wake-limit` (vytvára modul budenia) s `ack`; `turn-interrupted` (vytvára modul budenia pri keepalive, jeden otvorený na vlákno) s `ack`; `turn-stalled` (vytvára modul budenia pri pokračovaní bez pokroku) s `ack`. Rozhodnutie vykoná služba, model sa nespúšťa (P-046).
- Udalosti budenia: `blocker` a `assist` do témy (s `holder`, `holder_mode` a `holder_machine` z registrácie), `reply` na `blocker`/`assist` (holder = vlákno pôvodného odosielateľa, inak držiteľ jeho témy; nesie `reply_state`; prechod medzi priebežnými stavmi udalosť nevyšle), `expired` a `undelivered` (`{ type, message_id, level, project, topic, holder, holder_machine, holder_mode }` + `sender`, `thread_id`, `from_project`, `from_topic`; holder = odosielateľ správy), `discussion-closed` (`{ type, holder, holder_machine, project, topic }`, vlákno skončilo diskusiu s operátorom). Udalosť nesie nanajvýš hash textu (`text_hash`), nikdy telo. Modul `wake` ich odoberá cez `events --since --wait`.

HTTP rozhranie (pre iné klienty): `GET /health`, `POST /send`, `GET /receive?project&topic&thread`, `GET /count`, `POST /ack`, `GET /notices?all=1`, `POST /decide` (JSON alebo HTML formulár), `POST /register-topic`, `GET /events?since&wait`, `GET /message?id=<id>`, `POST /turn` (`{ thread, state: open|closed, machine, project, topic, holder_mode, reason, opened_at }` alebo `{ thread, attempt: true }` = pokus keepalive), `GET /turns?open=1`, `POST /notice` (`kind: wake-limit` s `message_id`, `title`, `detail`, `dedupe_key`; `kind: turn-interrupted` s `thread`, `title`, `detail`, `project`, `topic`, dedup na vlákno), `GET /` (stránka). Kľúč v `Authorization: Bearer`, `?key=` alebo cookie `comms_key`.

## Úrovne správ (P-042)

Určuje ich odosielateľ podľa toho, ako ho blokuje odpoveď:

- `blocker`: odosielateľ je zablokovaný, adresát koná hneď; odpoveď je povinná; lehota 5 min na prevzatie (predvolene aj maximum 5 min),
- `steer`: adresát má hneď zastaviť alebo zmeniť prácu na adresovanej téme; relevantnosť je daná adresou; lehota na prevzatie 5 min (maximum 5 min); odpoveď je nepovinná; budí držiteľa témy (bez oznamu),
- `assist`: odosielateľ pokračuje; adresát správu prečíta a pracuje na nej pri voľnom bode, odpovie stavom (`hotovo` s výsledkom, `otázka`, `čakám`); lehota na prevzatie 30 min (maximum 30 min); bez prevzatia do lehoty pošta odosielateľovi odpovie (`comms`, `no reply <id>`) a tým ho zobudí,
- `info` je zrušený (staré správy sa čítajú ako `assist` bez lehoty).

## Inštancie a lokálny socket

Na jednom stroji môže bežať viac nezávislých pôšt. Každá inštancia má vlastný dátový priečinok, vlastné kľúče, vlastný `instance_id` (klient ho overí) a vlastný lokálny socket: unix socket na Linux/macOS, named pipe na Windows, v priečinku používateľa, nie spoločný port. Klient používa `http.request` so `socketPath` (globálny `fetch` unix socket nepodporuje). Medzi inštanciami komunikácia nie je.

Lokálne sa služba spúšťa **na požiadanie**: klient overí, či beží, inak ju spustí. Na serveri beží trvalo za HTTPS (reverzná proxy). Pri nedostupnosti služby klient uloží správu do outboxu (`--outbox`, workflow použije `<brain>/local/outbox/`) a odošle ju pri najbližšom úspešnom spojení (`send`, `receive`, `flush`, `turn`: záznam ťahu najprv odošle outbox, aby zaradené `closed` predbehlo nový `open`); rovnako zaradí aj `turn --state closed` (`POST /turn`); klient čaká najviac 2 s a pri výpadku iba oznámi „comms unavailable“.

## Oznamy operátorovi (P-046)

Oznamy nie sú správy do vlákna. Služba ich zobrazí na **stránke oznamov** (HTML bez skriptov, tá istá služba, tlačidlá rozhodnutí volajú `decide`) a v `comms notices`. Pri `blocker` pošle notifikáciu cez konfigurovateľný externý príkaz `notify.command` (napríklad sendmail-kompatibilný); bez neho platí iba stránka oznamov. Žiadne npm závislosti. Pošta nezisťuje zánik vlákna; čakajúce správy iba zobrazí s časom čakania.

## Budenie (wake, P-043)

Samostatný proces `node bin/wake.mjs run`. Odoberá od pošty udalosti budenia (`events`, long-poll) a volá adaptér `ring(holder, text)`:

- `blocker` do témy budí jej držiteľa (`holder` z `register-topic`),
- odpoveď (`--reply-to`) na ktorúkoľvek úroveň budí vlákno pôvodného odosielateľa,
- `expired` (uplynutá lehota `assist`/`blocker`) budí odosielateľa, ale iba ak správa medzičasom nemá odpoveď a je stále `pending` (kontrola `message --id`; inak preskočené `answered`),
- `undelivered` (adresáta sa nedalo zobudiť natrvalo) budí odosielateľa,
- `steer` budí držiteľa témy pri odoslaní; `reply` na `steer` budí odosielateľa.

Zvončeky (celý prompt je presne tento text, nikdy telo správy):

| Zvonček | Kedy |
|---|---|
| `you have mail in topic <project>/<topic>` (bez témy `you have mail in topic thread/<id>`) | `blocker` pre držiteľa, odpoveď na `blocker`/`assist` pre odosielateľa (prvý priebežný stav a `hotovo`) |
| `no reply <message_id>` | lehota uplynula bez odpovede (udalosť `expired`) |
| `undelivered <message_id>` | adresáta sa nedalo zobudiť natrvalo (udalosť `undelivered`) |
| `turn interrupted, continue from handover` | keepalive |
| `turn ended with a next step, continue from handover` | pokračovanie (od 0.3.8) |

Šablóny exportuje `src/wake/index.mjs` (`WAKE_TEMPLATE`, `NO_REPLY_TEMPLATE = 'no reply'`, `UNDELIVERED_TEMPLATE = 'undelivered'`, `KEEPALIVE_TEMPLATE`). Workflow (`agentic-flow`, `policy.json`) rozpozná ťah iba presnou zhodou celého promptu a odmietne v ňom citlivé kroky (P-047).

Trvalé zlyhanie zvončeka pri `blocker`/`reply` (vlákno archivované alebo neexistuje) volá `bounce` → udalosť `undelivered` pre odosielateľa. Zlyhanie zvončeka `expired`/`undelivered` sa iba zaloguje (nevracia sa).

Diskusia s operátorom (D-0015, D-0022): `steer` a `assist` pre držiteľa, ktorý diskutuje s operátorom (záznam ťahu, `turn --discussion`, `waiting_operator`), modul nezvoní; udalosť si uloží (`wake-state.json`, `held`, najviac 200 a 24 h) a zazvoní ju, keď diskusia skončí (udalosť `discussion-closed`, inak pri ďalšom kole long-pollu, napríklad pri staršej službe). Správu medzičasom prevzatú, zodpovedanú alebo vrátenú nezvoní. `blocker` diskusiou prejde a zvoní ako doteraz. Bez načítaných ťahov sa zvoní ako doteraz.

Bežiace vlákno: T3 doručí zvonček bežiacemu vláknu ako jeho ďalší ťah. Aby sa nepálili zbytočné ťahy, modul pred zvonením načíta otvorené ťahy (`turns`); ak má holder otvorený ťah, na ktorý už jeden zvonček zaradil (`wake-state.json`, `queued[holder] = opened_at`), preskočí (`queued`, bez oznamu, nepočíta sa do limitu). Nový ťah (nové `opened_at`) môže dostať ďalší zvonček; záznamy bez aktuálneho otvoreného ťahu sa pri každej kontrole odstránia. Nečinné vlákno (bez otvoreného ťahu): rovnaký zvonček do 120 s (`idleDedupeMs`) po predošlom sa preskočí (`queued`), kým sa ťah vlákna neotvorí; ťah z prvého zvončeka vidí aj novú poštu. Bez načítaných ťahov sa zvoní vždy.

Limity (platia pre všetky typy udalostí): najviac **6 zobudení za hodinu na dvojicu tém** (oba smery spolu) a **zastavenie pri 2 rovnakých správach za sebou** (rovnaký odosielateľ → rovnaká téma, rovnaký text). Nad limit sa nič nebudí, správa ostáva v schránke a vznikne oznam `wake-limit` (jeden otvorený na dvojicu; po `ack` a uplynutí hodiny sa budenie obnoví). Zlyhanie adaptéra (T3 nebeží, vlákno neexistuje) sa iba zaloguje; správa počká na ďalší ťah.

Pošta funguje úplne bez budenia: každá správa sa doručí pri najbližšom ťahu adresáta, nech ho spustí ktokoľvek. Udalosti sa ukladajú, modul po štarte pokračuje od svojho kurzora; pri prvom štarte začne od konca (históriu nebudí) a udalosti staršie ako hodinu preskočí.

Režim držiteľa (P-037) budenie nezastaví: `blocker` aj `assist` budia držiteľa aj v režime `operator` (pri `blocker` vznikne navyše oznam operátorovi); držiteľ v ťahu budenia odpovie a to, čo patrí operátorovi, mu ohlási. Odpoveď budí vždy. Zvonček na archivované alebo neexistujúce vlákno vráti `blocker`/`assist`/odpoveď odosielateľovi (`undelivered <id>`).

Uzavretá téma (`register-topic --close`, robí ju `done --topic`): nová správa aj odpoveď do nej dostane hneď 410 s časom uzavretia (nikto ju nedrží); správy, ktoré v nej ešte čakajú, sa vrátia odosielateľom (`undelivered`). Adresa persony na uzavretú tému ide do hlavného vlákna persony. Nová registrácia bez `--close` tému znovu otvorí.

**Pokračovanie (od 0.3.8, P-026).** Ťah zatvorený s ďalším krokom (`turn --state closed --next`, hook Stop workflow ho pošle, keď handover držanej témy má ďalší krok) budenie po 60 s (`continueDelayMs`) zazvoní textom `turn ended with a next step, continue from handover`, raz na zatvorený ťah, v každom režime držiteľa a nezávisle od `keepalive.since_command`. Prompt operátora v tej lehote otvorí nový ťah a má prednosť; bežiace vlákno sa nebudí. Keď vlákno po zazvonení skončí znova s rovnakým ďalším krokom (bez pokroku), budenie nezvoní a vytvorí jeden oznam `turn-stalled`. Slučku tak drží kód, nie agent.

**Keepalive (obnovenie ťahu po reštarte T3).** Reštart servera T3 ukončí bežiace ťahy a T3 ich neoznačí (`status: completed`, `activeRunId: null`). Keepalive beží v tom istom procese každých 60 s (`keepalive.interval_ms`) a je **vypnutý, kým nie je nastavený `keepalive.since_command`**: príkaz (cez shell), ktorý vypíše čas posledného štartu servera T3 (ISO, epoch, `@epoch` alebo `systemctl show -p ActiveEnterTimestamp --value <služba>`; pri inom pásme ako UTC použite `TZ=UTC systemctl …` alebo `--timestamp=unix`). Pre každý otvorený ťah tohto stroja (`turn.machine` = `--machine`) otvorený pred štartom T3 sa opýta adaptéra `status` (`t3 thread show --json <vlákno>`); ak vlákno nemá aktívny beh:

- `holder_mode` `operator`: iba oznam `turn-interrupted` a ťah sa zatvorí,
- inak zvonček pevnou vetou **`turn interrupted, continue from handover`**; po úspechu sa ťah zatvorí (nový ťah má nový `opened_at`, takže sa znova nebudí). Najviac 3 pokusy za hodinu na vlákno, medzi pokusmi aspoň 60 s; po 3. neúspechu (alebo pri vyčerpanom limite) oznam `turn-interrupted` a zatvorenie.

### Spustenie pre laika

Modul musí bežať **na stroji, kde je T3 (`t3` CLI)**, lebo volá `t3 thread send <vlákno> "<šablóna>"`. Overte: `t3 thread send --help`. Windows: T3 dáva CLI ako `%USERPROFILE%\.t3\bin\t3.cmd` a nepridá ho do PATH; budenie ho od v0.4.3 nájde samo (`T3CODE_CLI_PATH`, potom tento súbor) a spustí cez `cmd.exe`, `command` vo `wake.json` netreba meniť. Ručné overenie: `& "$env:USERPROFILE\.t3\bin\t3.cmd" thread send --help`.

**Lokálne (Linux, macOS, Windows), pošta na tom istom stroji:**

```sh
node bin/wake.mjs run --instance pilot
```

Kľúč netreba: modul si v `keys.json` inštancie nájde alebo vytvorí kľúč odosielateľa `wake`. Beží, kým ho neukončíte (Ctrl+C); každé zobudenie vypíše jeden riadok na stderr. Mimo systemd (terminál, launchd na macOS, Plánovač úloh na Windowse) beží `wake run` od v0.4.5 ako malý dozorný proces nad samotným budením: po samoreštarte (nový tag, zmena `wake.json`) vypíše `samoreštart: spúšťam budenie znova …` a spustí budenie v novej verzii; predtým budenie po aktualizácii skončilo a nikto ho znova nespustil. Pid v `status` je pid budenia; jeho ukončenie (aj Ctrl+C) zastaví oboje. Pod systemd (`INVOCATION_ID`) dozor nebeží, reštart robí `Restart=always`. Vypnutie dozoru: `COMMS_WAKE_SUPERVISOR=0`. Ak pošta nebeží, spustí ju (ako klient), pri jej výpadku sa pripája znova (1 s … 30 s). Stav je v `<data-dir>/client/wake/wake-state.json`, zámok bežiaceho budenia v `wake.lock` vedľa neho (`node bin/wake.mjs status --instance pilot`).

Alternatíva: spustiť poštu spolu s budením jedným príkazom `node bin/comms.mjs serve --instance pilot --wake` (ukončenie služby ukončí aj budenie: služba počká na jeho koniec, najviac 5 s; budenie si vtedy samo službu nespúšťa; samoreštart vrátane zmeny `wake.json` vtedy stráži služba).

**Trvalý beh (Linux, macOS, Windows), od v0.4.6 jedným príkazom:**

```sh
node bin/wake.mjs install --instance pilot      # registrácia a spustenie
node bin/wake.mjs install --instance pilot --print   # iba vypíše súbory a príkazy, nič nezmení
node bin/wake.mjs install --instance pilot --remove  # zastavenie a odstránenie registrácie
```

Služba spúšťa `wake run` s rovnakými voľbami ako `install` (bez `--instance`, `--data-dir`, `--config` a `--url` doplní `--instance` z konfigurácie); `--key` na príkazovom riadku odmietne, kľúč patrí do `keys.json` inštancie alebo do `--config`. Opakované `install` (napr. po presune klonu alebo zmene volieb) registráciu prepíše a budenie reštartuje. Výsledok je JSON (`installed`, súbory, `notes` s cestou k logu, `errors`).

| systém | čo vznikne | log |
| --- | --- | --- |
| Linux | `systemd` služba používateľa `~/.config/systemd/user/agentic-wake-<inštancia>.service` (`Restart=always`), `enable` a `restart` | `journalctl --user -u agentic-wake-<inštancia> -f` |
| macOS | LaunchAgent `~/Library/LaunchAgents/com.agentic-comms.wake.<inštancia>.plist` (`RunAtLoad`, `KeepAlive`), `launchctl bootstrap` | `<base>/wake-<inštancia>.log` |
| Windows | úloha Plánovača `agentic-wake-<inštancia>`: pri prihlásení, bez okna (`conhost --headless`), bez časového limitu, jedna inštancia; slučka `<base>\wake-loop-<inštancia>.cmd` | `<base>\wake-<inštancia>.log` |

`<base>` je priečinok inštancií: `%LOCALAPPDATA%\comms` (Windows), `~/Library/Application Support/comms` (macOS), alebo `COMMS_BASE_DIR`. `PATH` služby je `PATH` príkazu `install`, preto ho spúšťajte v termináli, kde funguje `t3` (Windows: `t3.cmd` nájde budenie samo).

- **Linux:** bez bežiaceho používateľského systemd (napr. bez prihlásenia, chýba `XDG_RUNTIME_DIR`) `install` skončí chybou a nič nezapíše. Na serveri, kde sa nikto neprihlasuje, najprv `loginctl enable-linger`, inak služba skončí s odhlásením. Ručne napísanú službu budenia z návodu do v0.4.5 najprv vypnite (`systemctl --user disable --now <služba budenia>` a zmažte jej `.service`), inak bežia dve (druhá čaká na zámok).
- **Windows:** `install` pred registráciou zastaví úlohu tejto inštancie, jej slučku a bežiace budenie (pid zo zámku); ručne založenú úlohu `agentic-wake` so slučkou `wake-loop.cmd` z návodu do v0.4.5 zastaví a odregistruje tiež. Súbor `wake-loop.cmd` a starý `wake.log` môžete potom zmazať. `Stop-ScheduledTask` sám nestačí (ukončí iba `conhost.exe`, slučka `cmd.exe` spustí `node` znova); na zastavenie použite `install --remove`, na reštart `install`.

Pod systemd (`INVOCATION_ID`) reštartuje budenie po samoreštarte `Restart=always`, pod launchd a Plánovačom dozorný proces `wake run`; slučka na Windowse je poistka pri páde `node` a pri zmene samotného dozoru. Vo Windowse beží jedna reťaz `conhost → cmd → node → node` (dozor a budenie) a `status` ukazuje jeden `running`.

Stavový riadok session workflow ukáže `wake: off`, keď budenie na stroji už bežalo (`wake-state.json` existuje) a teraz nebeží; `comms count` to vracia v poli `wake` (`running`, `pid`, `heartbeat_at`), na stroji, kde budenie nikdy nebežalo, pole chýba.

**Pošta na inom stroji (T3 lokálne):** `node bin/wake.mjs run --url https://posta.example/ --key <kľúč> --instance-id <id>` (alebo rovnaký `--config` JSON ako klient). Pri `--url` je kľúč povinný a modul poštu nespúšťa.

**Windows:** rovnaký príkaz v PowerShell (`node bin\wake.mjs run --instance pilot`) v otvorenom termináli, alebo trvalý beh `node bin\wake.mjs install --instance pilot` (vyššie).

Na jednej inštancii beží vždy jedno budenie (od v0.4.3): druhé `wake run` vypíše `budenie tejto inštancie už beží (pid …); čakám, kým skončí` a prevezme prácu až po skončení prvého. Či budenie beží: `node bin\wake.mjs status --instance pilot`, pole `running` (pid a posledný heartbeat, obnovuje sa každých 30 s), `null` = nebeží.

Budenie beží bez okna: stav overíte `wake.mjs status` (pole `running`), výpis je v `%LOCALAPPDATA%\comms\wake-<inštancia>.log`. Zlyhaný zvonček (napr. presunutý T3) je iba v logu.

**Odinštalovanie na Windowse** (čo inštalácia necháva mimo repozitárov):

- úloha Plánovača: `node bin\wake.mjs install --instance <inštancia> --remove` (zastaví slučku aj budenie a odregistruje úlohu);
- `%LOCALAPPDATA%\comms\` (dáta inštancií `<instance>\` vrátane `keys.json` a `wake.json`, `wake-loop-<inštancia>.cmd`, `wake-<inštancia>.log`);
- pravidlá v `~/.gitconfig`, ak ste ich pri inštalácii pridali (napr. `credential` pre GitHub, `core.autocrlf`);
- klony `agentic-comms` a workflow (`%USERPROFILE%\.claude\skills\workflow`).

**Odinštalovanie na Linuxe a macOS:**

- budenie: `node bin/wake.mjs install --instance <inštancia> --remove`, alebo ukončiť `wake run` (Ctrl+C alebo `kill <pid>` z `status`, pole `running`); služby z ručného návodu: pri systemd `systemctl --user disable --now <služba budenia> <služba pošty>` a zmazať ich `.service` v `~/.config/systemd/user/`; pri launchd `launchctl bootout gui/$(id -u) <plist>` a zmazať plist v `~/Library/LaunchAgents/`;
- dáta inštancií (vrátane `keys.json` a `wake.json`): `${XDG_DATA_HOME:-~/.local/share}/comms/` (Linux), `~/Library/Application Support/comms/` (macOS), alebo `COMMS_BASE_DIR`, ak ste ho nastavili;
- klony `agentic-comms` a workflow (`~/.claude/skills/workflow`).

### Rozhranie a konfigurácia

`node bin/wake.mjs run|status|config [voľby]`. Prednosť: voľba > premenná > `--config` > `<data-dir>/wake.json` > predvolené.

**`wake.json`:** bez `--config` a `COMMS_CONFIG` sa sekcia `wake` číta zo súboru `<data-dir>/wake.json` (rovnaké kľúče ako v tabuľke, bez obalu `wake`), napríklad `{ "keepalive": { "since_command": "systemctl --user show -p ActiveEnterTimestamp --value t3" }, "jobs": [{ "name": "issue-watch", "command": "sh issues/watch.sh", "interval_ms": 900000 }] }`. Nastavenie budenia tak nepotrebuje zmenu systemd jednotky. Súbor sa číta pri štarte; `wake run` však sleduje jeho mtime v tej istej 30 s kontrole a pri zmene (aj vzniku či zmazaní) sa ukončí a spustí ho s novou konfiguráciou systemd (kód 0) alebo mimo systemd dozorný proces `wake run`. S `--config` sa `wake.json` ignoruje.

| Voľba | Premenná | Kľúč v `--config` (`wake.*`) | Predvolené |
|---|---|---|---|
| `--machine <stroj>` | `WAKE_MACHINE` | `machine` | `machine` klienta, inak `os.hostname()`; budia sa iba držitelia na tomto stroji, udalosť bez stroja budí každý modul |
| `--adapter t3\|none` | `WAKE_ADAPTER` | `adapter` | `t3` (`none` = nič sa nebudí, iba log) |
| `--command "<príkaz>"` | `WAKE_COMMAND` | `command` (reťazec alebo pole) | `t3 thread send` (dostane `<thread> <text>` ako posledné dva argumenty) |
| `--limit-per-hour <n>` | `WAKE_LIMIT_PER_HOUR` | `limit_per_hour` | 6 |
| `--stop-on-repeat <n>` | `WAKE_STOP_ON_REPEAT` | `stop_on_repeat` | 2 |
| `--max-event-age-ms <ms>` | `WAKE_MAX_EVENT_AGE_MS` | `max_event_age_ms` | 3 600 000 |
| `--wait-ms <ms>` | `WAKE_WAIT_MS` | `wait_ms` | 25 000 (dĺžka long-pollu) |
| `--reconnect-ms <ms>` | `WAKE_RECONNECT_MS` | `reconnect_ms` | 1 000 (prvý pokus; zdvojuje sa do 30 s) |
| `--state-dir <cesta>` | `WAKE_STATE_DIR` | `state_dir` | `<data-dir>/client/wake` |
| `--status-command "<príkaz>"` | `WAKE_STATUS_COMMAND` | `status_command` | `t3 thread show --json` (dostane `<thread>` ako posledný argument; číta `thread.status`, `thread.activeRunId`) |
| `--keepalive-since-command "<príkaz>"` | `WAKE_KEEPALIVE_SINCE_COMMAND` | `keepalive.since_command` | žiadny = keepalive vypnutý |
| `--keepalive-interval-ms <ms>` | `WAKE_KEEPALIVE_INTERVAL_MS` | `keepalive.interval_ms` | 60 000 |
| – | – | `jobs` | žiadne (periodické úlohy, pozri nižšie) |

**Periodické úlohy (`jobs`, D-0023, D-0024).** `wake run` spúšťa príkazy zo zoznamu `jobs` (iba zo súboru: `wake.json` alebo `wake.jobs` v `--config`, bez voľby a premennej) vedľa long-pollu a keepalive, nezávisle od dostupnosti pošty. Položka: `name` (povinné, písmená, číslice, `.`, `_`, `-`, najviac 64 znakov, jedinečné), `command` (povinné, reťazec pre shell), `interval_ms` (predvolené 900 000; menej ako 60 000 sa zvýši na 60 000 s riadkom v logu), `timeout_ms` (predvolené 300 000, najviac `interval_ms`). Neplatná položka sa preskočí s riadkom v logu pri štarte, ostatné bežia. Príkaz sa spúšťa cez shell ako `keepalive.since_command`; po časovom limite alebo pri ukončení budenia sa ukončí celá skupina procesov. Úloha beží prvýkrát pri štarte (alebo keď od jej posledného štartu uplynul interval; čas posledného behu je v stave `wake-state.json` a prežije reštart), potom raz za `interval_ms`; jeden beh úlohy naraz (beh dlhší ako interval posunie ďalší o interval od konca). Výsledok každého behu je jeden riadok v logu (`úloha <meno>: ok (<ms> ms)` alebo `úloha <meno>: zlyhala (kód N): <koniec výstupu>`); zlyhanie budenie nikdy nezastaví. `wake config` vypíše `jobs` aj `jobProblems`, `wake status` posledný beh každej úlohy. Zmena `wake.json` (aj `jobs`) reštartuje `wake run` (samoreštart).

Voľby pošty sú rovnaké ako pri klientovi (`--instance`, `--data-dir`, `--config`, `--url`, `--key`, `--instance-id`, `--timeout`, `--no-start`). Návratové kódy ako klient (`3` pri inej inštancii; `run` sa pri nej ukončí).

Adaptéry: pilot má iba `t3` (`src/adapters/t3/`). `headless` a `notify` po pilote.

## Dátový priečinok inštancie

```
instance.json   instance_id, názov, notify.command, tcp, ttl (lehoty blocker/steer/assist)
keys.json       kľúče (sender ↔ key), iba pre čítanie používateľom (0600)
server.json     pid a socket bežiacej služby (existuje iba počas behu)
messages/       jedna správa = jeden súbor <id>.json (state: pending | held | acked | discarded; ttl_s, expires_at, outcome: expired | undelivered, expired_at, undelivered_at/_holder/_reason)
topics/         registrované témy <project>__<topic>.json
notices/        oznamy operátorovi
events/         udalosti budenia (poradové číslo seq)
turns/          ťahy vlákien <thread>.json (keepalive)
client/         lokálny stav klienta: outbox/ (predvolené), count-cache.json, wake/wake-state.json (modul budenia)
comms.sock      unix socket (Linux/macOS)
```

## Štruktúra repa

```
bin/        comms.mjs (klient, `init`, `serve [--wake]`, `stop`), wake.mjs (`run`, `status`, `config`)
src/common/ cesty podľa OS, pomôcky
src/server/ služba: store (súbory), service (logika), http (routing, kľúče), page (HTML), index (socket, TCP)
src/client/ klient: http.request so socketPath, instance_id, outbox, cache, spustenie na požiadanie; cli
src/wake/   modul budenia: Waker (udalosti, limity, šablóna, stav), cli
src/adapters/t3/  adaptér budenia cez T3 CLI (`t3 thread send`, stav `t3 thread show --json`)
src/notify/ notifikácia operátora (externý príkaz `notify.command`)
tests/      node --test (`npm test`), dočasné priečinky v /tmp
```
