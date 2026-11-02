# bin/

- `comms.mjs`: CLI klient (`send`, `receive`, `ack`, `count`, `message`, `turn`, `notices`, `decide`, `register-topic`, `events`, `flush`, `health`) a správa služby (`init`, `key`, `config`, `serve [--wake]`, `stop`). Rozhranie je v `README.md` repa.
- `wake.mjs`: modul budenia, samostatný proces: `run` (trvalý beh, predvolený adaptér `t3`), `status`, `config`. Rozhranie v `README.md` repa.

Všetko Node, bez závislostí (P-034). Žiadne absolútne cesty; dátový priečinok inštancie sa odvodí z priečinka používateľa (`os.homedir()`) alebo z `--data-dir` / `COMMS_DATA_DIR`.
