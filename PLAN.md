# Plán

Implementácia pošty a budenia sa riadi míľnikmi v `agentic-flow/PLAN.md`:

- **M4** služba a klient (`send`, `receive`, `notices`, `decide`, `register-topic`, inštancie, socket/named pipe, outbox, spustenie na požiadanie, stránka oznamov, adaptér e-mail) – hotové,
- **M5** napojenie workflow (triáž, skill `comms`, stavový riadok) – prebieha v `agentic-flow`, tu iba stabilné rozhranie klienta,
- **M6** modul `wake`, limity, adaptér `t3` – hotové (zostáva ručné overenie na stroji s T3, bod 8 nasadenia).

- **Hotfix 0.2.2** samoreštart po nasadení (revízia `.git/HEAD`, mtime `wake.json`, kód 0 pre `Restart=always`) a `<data-dir>/wake.json` – hotové (zostáva overenie v pilote: prepnutie stable klonu na tag).
- **Hotfix 0.2.1** doručenie a budenie (zmluva s `agentic-flow`): lehoty `assist`/`blocker` so systémovou odpoveďou `comms`, `count.ids`, `message`, `turn`, keepalive – hotové (zostáva overenie v pilote: TTL 2 min, súbeh, reštart T3).

Testy (`npm test` (`node --test "tests/**/*.test.mjs"`)): `server.test.mjs`, `client.test.mjs`, `notices.test.mjs` (M4), `wake.test.mjs` (M6), `persona.test.mjs` (persony a stroj). Smoke test body 5, 19–23 v `agentic-flow/PLAN.md` časť (e).

Zámerne mimo pilotu: HTTPS na serveri, adaptéry `headless` a `notify`, externé klienty (task manager).

## Rozhodnutia pri implementácii

M4 (agent, 2026-11-01), vždy najjednoduchší variant pri nejasnom zadaní:

1. **Doplnené operácie `ack`, `count`, `events`, `flush`, `stop`, `key`, `config`.** `receive` je nedeštruktívne (triáž iba číta, P-044); správa prestane čakať až po `ack`. `count` dáva stavovému riadku lacný počet s cache. `events` je long-poll pre modul budenia (M6). Ostatné sú správa inštancie.
2. **Blocker vždy vytvorí oznam.** Pošta nepozná stav témy (`working`/`free`) ani režim držiteľa, preto oznam `blocker` vzniká pri každom `blocker` do registrovanej témy; rozhodnutia `ack`/`discard`. Jemnejšie pravidlo (iba `working` v režime `operator`) môže doplniť M5 cez `register-topic`.
3. **Držiteľ pri registrácii.** `register-topic` prijíma voliteľné `holder` (ID vlákna) od `topic claim`; udalosti `blocker` ho nesú, aby `wake` nemusel čítať brain. Pošta ostáva bez vlastnej evidencie držiteľov (len odkladá, čo jej skript povie).
4. **Odpovede.** `send --reply-to <id>` sa doručí do vlákna pôvodného odosielateľa (`to_thread`) a do jeho `from_project/from_topic`, ak ich uviedol; `receive --thread <id>` ich vráti. Pre odpoveď sa registrácia témy nekontroluje (hlavné vlákna nemajú adresu, P-045). Udalosť `reply` vzniká pri odpovedi na `blocker` aj `assist`.
5. **Neregistrovaná téma.** Správa sa uloží so stavom `held` a oznamom; samotná registrácia témy ju nedoručí, doručí až rozhodnutie `deliver` alebo `redirect` (cieľ musí byť zaregistrovaný). Pošta tému nikdy nezaloží.
6. **Kľúče v `keys.json` v čitateľnom tvare** (0600 v priečinku používateľa), nie hash: operátor si ich vie vypísať (`key list`) a pridať bez bežiacej služby. Odosielateľ = `sender` kľúča; telo správy pole `sender` ignoruje.
7. **Autentizácia.** Každá požiadavka okrem `/health` vyžaduje kľúč (bearer, `?key=`, cookie alebo pole formulára). Rovnako na sockete aj TCP; na sockete je navyše ochranou právo 0600.
8. **Stránka oznamov iba cez TCP** (prehliadač nevie unix socket): `config set-tcp 127.0.0.1:<port>`; HTTPS a prístup zvonku rieši reverzná proxy. Stránka je HTML bez skriptov, tlačidlá sú formuláre `POST /decide` s presmerovaním 303.
9. **Notifikácia** iba pri oznamoch s úrovňou `blocker` (vrátane `blocker` na neregistrovanú tému), cez `spawn(command, { shell: true })` s textom na stdin a premennými `COMMS_NOTICE_*`. Zlyhanie sa iba zaloguje.
10. **Outbox a cache klienta.** Predvolené miesto `<data-dir>/client/outbox/` a `<data-dir>/client/count-cache.json` (prepis `--outbox`, `COMMS_OUTBOX`, `client_dir` v konfigu; workflow použije `<brain>/local/outbox/`). Outbox sa odosiela pred `send`/`receive` a pri `flush`; správa odmietnutá službou (4xx) sa presunie do `outbox/rejected/`.
11. **Spustenie na požiadanie.** Klient spustí `node bin/comms.mjs serve --data-dir <dir>` odpojene (`detached`, `unref`) a čaká na `/health` do uplynutia lehoty 2 s. Socket je zámok: druhý `serve` dostane `already_running`. Mŕtvy socket súbor sa pred počúvaním zmaže. Pri `--url` sa nespúšťa nič.
12. **Windows named pipe** `\\.\pipe\comms-<sha1(data-dir)>`; nedá sa umiestniť do priečinka, názov odvodený z dátového priečinka inštancie. Netestované na Windows (smoke test bod 5).
13. **Konfiguračný súbor klienta** (`--config`, `COMMS_CONFIG`) s kľúčmi `instance`, `data_dir`, `url`, `key`, `instance_id`, `outbox`, `client_dir`, `thread_id`, `timeout`, `autostart`; určený pre `<brain>/local/comms.json`. ID vlákna sa inak berie z `COMMS_THREAD_ID` alebo `T3CODE_THREAD_ID`.
14. **Pozdržané a vybavené správy sa nemažú**; stav je v súbore (`held`, `acked`, `discarded`). Rotáciu/čistenie rieši neskorší míľnik.

M6 (agent, 2026-11-01):

1. **Šablóna budenia po anglicky**: `you have mail in topic <project>/<topic>`, lebo workflow (`agentic-flow/policy/policy.json`, `wake_template: "you have mail in topic"`, hook `user-prompt-submit.mjs`) rozpoznáva tento prefix bez ohľadu na veľkosť písmen a nastavuje `turn_source: wake` (P-047). Pri odpovedi vláknu, ktoré nemá tému (hlavné vlákno), je text `you have mail in topic thread/<id>`. **Požiadavka pre workflow (M5):** ťah so šablónou spracovať triážou (`comms receive --project --topic`, pri `thread/<id>` `comms receive --thread <id>`); `topic claim` posielať `register-topic` s `--holder <thread>` a `--holder-mode <mode>` z `holder.json`, inak sa budí vždy.
2. **Udalosti nesú kontext, nie telo.** Udalosť `blocker`/`reply` nesie `project`, `topic`, `holder`, `holder_mode`, `sender`, `thread_id`, `from_project`, `from_topic`, `message_id` a `text_hash` (sha1, 16 znakov). Hash stačí na „2 rovnaké správy za sebou“ bez toho, aby telo opustilo poštu. Pri odpovedi bez `thread_id` pôvodného odosielateľa sa použije držiteľ jeho témy.
3. **Dvojica tém a opakovanie.** Limit 6/h sa počíta na neusporiadanú dvojicu adries (adresa = `project/topic`, inak `thread:<id>`, inak `sender:<meno>`), teda oba smery výmeny spolu. Opakovanie sa sleduje na usporiadaný smer (odosielateľ → adresát): druhá rovnaká správa za sebou sa nebudí; iná správa počítadlo vynuluje. Oba limity sa vyhodnocujú až po preskočeniach (bez držiteľa, zastarané, režim `operator`), aby tie nespotrebúvali limit. Zlyhanie adaptéra sa do limitu počíta (bráni búšeniu na T3).
4. **Oznam nad limit cez poštu.** Nový druh oznamu `wake-limit` (rozhodnutie `ack`) vytvára modul operáciou `POST /notice` (`comms` klient `createNotice`). Služba povolí iba tento druh a dedupuje: jeden otvorený oznam na `dedupe_key` (dvojica alebo smer). Pri `blocker` správe ide aj notifikácia `notify.command` (existujúce pravidlo služby). Pošta sama žiadne limity nevedie.
5. **Režim držiteľa.** `register-topic` prijíma `holder_mode` (`operator`|`auto`, uložené iba s `holder`). Pošta nepozná stav `working`/`free`, preto tému s držiteľom považuje za pracujúcu: pri `operator` sa `blocker` nebudí (operátor má oznam `blocker` zo služby), pri `auto` alebo bez údaja sa budí. Odpoveď budí vždy (odosielateľ čaká). Pokračovanie v režime `auto` iba pri „Ďalší krok“ v handoveri (P-026) rieši workflow pri spracovaní ťahu, nie pošta.
6. **Kurzor a história.** Stav modulu (`cursor`, zobudenia za hodinu na dvojicu, posledný hash na smer) je v `<data-dir>/client/wake/wake-state.json` (`--state-dir`). Prvý štart začne od konca prúdu (`events --since latest`), aby nebudil históriu; po výpadku pokračuje od kurzora, udalosti staršie ako `max_event_age_ms` (hodina) preskočí ako zastarané. Pri `INSTANCE_MISMATCH` proces skončí (kód 3); pri výpadku pošty čaká 1 s … 30 s a pripája sa znova; lokálne poštu spustí ako každý klient (`--no-start` vypne).
7. **Kľúč modulu.** Lokálne si modul vezme alebo vytvorí v `keys.json` kľúč odosielateľa `wake` (zápis mimo služby, rovnako ako `comms key add`); pri `--url` je kľúč povinný.
8. **Adaptér `t3`** spúšťa `t3 thread send <threadId> <text>` bez shellu, s limitom 20 s; `--command`/`WAKE_COMMAND`/`wake.command` nahradí príkaz (testy používajú `tests/fixtures/fake-t3.mjs`). `--adapter none` iba loguje. Overenie na skutočnom vlákne je ručný krok nasadenia (bod 8 v `agentic-flow/PLAN.md`), v testoch sa skutočné `t3` nevolá.
9. **`comms serve --wake`** voliteľne spustí modul ako podproces služby (ostáva samostatným procesom; ukončenie služby ho ukončí). Predvolený spôsob je samostatný `wake run`.
10. **Oprava nestabilného testu** (outbox: poradie `first`, `second`): ID správ sú v rámci milisekundy monotónne (počítadlo) a `receive` triedi podľa `created_at`, potom podľa `id`.

Persony a stroj (agent (delegované operátorom), 2026-11-01; P-041, P-043, P-045):

1. **Schránka hlavného vlákna persony** je obyčajný záznam témy `@<persona>/main`. `NAME_RE` vyžaduje na začiatku písmeno alebo číslicu, preto `@` žiadny projekt nemá. Registráciu robí hlavné vlákno samo (`--main`), posledná vyhráva; brain ju nepozná, takže `--force` netreba.
2. **Persona sa prijme vždy.** `toPersonaMain` uloží správu ako `pending` bez ohľadu na registráciu; neregistrovaná persona dostane jeden otvorený oznam `unregistered-persona` (dedup na personu), registrácia `--main` ho uzavrie (`decision: registered`).
3. **Vlastník a viditeľnosť** sú polia registrácie (`persona`, `visibility`). Cudzia správa do súkromnej témy dostane tú istú odpoveď ako neregistrovaná téma (`held` + `notice_id`), aby sa existencia nedala zistiť; `deliver` ju pošle do hlavného vlákna vlastníka. Vlastník = `sender` kľúča (kľúč persony sa volá menom persony).
4. **Stroj** sa normalizuje (`normalizeMachine`: malé písmená, `[a-z0-9.-]`). Registrácia s `holder` bez `holder_machine` cez CLI doplní stroj klienta; cez API ostane `null` a budí sa všade (spätná kompatibilita). Odpoveď budí vlákno pôvodného odosielateľa na stroji z jeho `thread_machine`.
5. **Filter v `wake`** je pred limitmi (`skip('other-machine')`), takže cudzie udalosti nespotrebúvajú limit ani nebúšia na T3.
6. Testy: `tests/persona.test.mjs`.

Hotfix 0.2.1 (agent, 2026-11-01; príčina: `assist` bez lehoty, nikto neobnoví prerušený ťah; P-042, P-043, P-046):

1. **Lehota pri odoslaní.** `expires_at` = `created_at` + TTL, aj pre pozdržanú správu (uplynie až po doručení, lebo kontroluje iba `pending`). TTL majú aj odpovede úrovne `assist`/`blocker`. Predvolené `ttl` v `instance.json` (služba ho číta pri štarte); CLI odmietne `info --ttl` už pred odoslaním, aby sa nedostalo do outboxu.
2. **Kontrola lehôt** v službe každých 30 s a hneď pri štarte; odpoveď = ľubovoľná správa s `reply_to` na ňu. `expired_at` sa zapíše raz, preto druhé uplynutie ani druhý oznam nevznikne.
3. **Uplynutie = systémová odpoveď, nie udalosť `expired`** (oprava návrhu: zobudený odosielateľ by inak vo svojej schránke nič nenašiel). Služba odpovie cez `send` ako odosielateľ `comms` (`info`, `reply_to`, anglický text bez tela originálu); `afterDelivery` vyšle bežnú udalosť `reply`, takže `wake` nemá pre lehoty žiadnu vlastnú logiku. Originál potom má odpoveď a znova neuplynie; neskorá skutočná odpoveď budí znova (prijaté). Bez vlákna aj zdrojovej témy iba oznam `no-reply`, ten vzniká vždy. `GET /message?id=` (query namiesto `/message/<id>`) ostáva na kontrolu stavu.
4. **Ťahy** sú jeden záznam na vlákno v `turns/`; `open` prepíše záznam (nový `opened_at`, `keepalive.count` 0), `closed` ponechá `opened_at` a zapíše `close_reason`. Pokus keepalive sa zaznamená `POST /turn { attempt: true }`. Limit 3/h a odklad 60 s drží `wake` vo svojom stave (`keepalive` vo `wake-state.json`) naprieč ťahmi vlákna.
5. **Keepalive iba pre ťahy so strojom tohto modulu**; ťah bez stroja sa nebudí (mohol bežať na inom T3). Vlákno „nebeží“ = `activeRunId` prázdne. Nezistený stav (`status` zlyhá) alebo neznámy štart T3 kolo preskočí. Po úspešnom zazvonení aj pri ozname sa ťah zatvorí s dôvodom `keepalive`. Keepalive má vlastného klienta (lehota nezávislá od long-pollu).
6. **Oznam `turn-interrupted`** nemá správu (`message_id: null`, `thread_id` = vlákno); `POST /notice` ho deduplikuje na vlákno (`dedupe_key: thread:<id>`). `no-reply` vytvára iba služba.

Hotfix 0.2.2 (agent, 2026-11-01; príčina: po prepnutí klonu bežala stará verzia, kým niekto nereštartoval jednotky):

1. **Revízia** = obsah `.git/HEAD`; pri refe `ref@SHA` (súbor refu, inak `packed-refs`; `.git` ako súbor `gitdir:` sa tiež číta). Bez `.git` sa nekontroluje nič. Koreň je repo modulu, v testoch `COMMS_INSTALL_ROOT`; interval 30 s, v testoch `COMMS_SELF_RESTART_INTERVAL_MS`.
2. **Ukončenie** cez existujúcu cestu zastavenia: služba volá to isté ako pri SIGTERM (zastaví dieťa `wake`, `srv.close()`, kód 0); `wake run` zruší `AbortController`, `run()` zapíše stav a vráti 0. Časovač je `unref`, nedrží proces.
3. **Služba s `--wake`** spúšťa dieťa s `--no-self-restart` a `wake.json` sleduje sama, aby sa dieťa po zmene neukončilo bez opätovného spustenia.
4. **`wake.json`** platí iba bez `--config`/`COMMS_CONFIG` (prednosť voľby > env > `--config` > `wake.json`); kľúče bez obalu `wake`. Sleduje sa mtime vrátane vzniku a zmazania súboru.
5. **Vypnutie** `self_restart: false` sa hľadá v `--config` (najvyššia úroveň), pri službe aj v `instance.json`, pri `wake` aj v sekcii `wake`/`wake.json`.
6. Testy: `tests/selfrestart.test.mjs`; existujúce testy spúšťajúce procesy majú `COMMS_SELF_RESTART=0`.


Hotfix 0.3.0 (agent, 2026-11-01; príčina: odpovede od `comms` a oznam `no-reply` pálili ťahy a zaťažovali operátora; zvonček `you have mail` pri uplynutí bol nepresný; bežiacemu vláknu sa zaraďovalo viac ťahov; neskoré `closed` zatváralo nový ťah):

1. **Uplynutie** (`checkExpired`): `outcome: expired`, `expired_at`, jedna udalosť `expired` pre odosielateľa (vlákno, inak držiteľ zdrojovej témy); žiadna správa, žiadny oznam (`no-reply` zrušený, staré oznamy sa rozhodujú podľa uložených `options`). Bez cesty späť iba log. Stav pre adresáta ostáva `pending`.
2. **Vrátenie** (`POST /bounce`): `outcome: undelivered`, `undelivered_at/_holder/_reason`, udalosť `undelivered`; raz na správu (aj staré `bounced_at`), zaznamená sa aj bez cesty späť; správy od `comms` sa nevracajú. Správa s výsledkom už neuplynie a výsledok sa neprepisuje.
3. **`GET /message`**: `{ id, state, level, project, topic, to, outcome, replied, expires_at, ttl_s, undelivered_holder, undelivered_reason }`.
4. **`POST /turn closed` s `opened_at`**: nezhoda (aj bez záznamu) → `{ ok: true, stale: true }`, záznam sa nemení. Klient pri výpadku zaradí `closed` do outboxu (`/turn`), `open` nie.
5. **Wake**: typy `blocker|reply|expired|undelivered`; texty `you have mail in topic …`, `no reply <id>`, `undelivered <id>`; `expired` sa pred zvonením overí cez `message` (odpoveď alebo nie `pending` → `answered`); `bounce` iba pri `blocker`/`reply`; najviac jeden zaradený zvonček na beh ťahu (`queued` vo wake-state podľa `run_at`, prune podľa záznamov ťahov a `queuedStartMs` po zatvorení behu), preskočenie sa nepočíta do limitu; detekcia opakovania iba pre udalosti s `text_hash`.
