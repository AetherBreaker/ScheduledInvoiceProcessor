# E2E suite

Proves the SAS and RYO pickup/dropoff cycle end to end against Docker stand-ins for the production
servers and the *testing* Google Sheet. Design: `docs/superpowers/specs/2026-08-24-e2e-baseline-test-suite-design.md`.

| Production | Stand-in |
|---|---|
| SFT holding FTP (Pure-FTPd) | `stilliard/pure-ftpd` on 127.0.0.1:21 |
| SAS SFTP (Files.com) | `drakkan/sftpgo` on 127.0.0.1:2022 |
| RYO SFTP (Bitvise) | `drakkan/sftpgo` on 127.0.0.1:2222 |
| Google Sheet | the testing spreadsheet (live) |

## Running in CI

`.github/workflows/e2e.yml` runs on pushes to `main` and `test/**` and via *Run workflow* (no `pull_request` trigger: the repo is public and the job needs secrets). Required repository secrets:

- `SFTPYPI_USERNAME`, `SFTPYPI_PASSWORD` — internal package index (for `aeth-ext`); the workflow
  surfaces these to `uv` as `UV_INDEX_SFTPYPI_USERNAME` / `UV_INDEX_SFTPYPI_PASSWORD`
- `E2E_DB_KEY_JSON` — service-account key JSON (must have edit access to the testing sheet)
- `E2E_DATABASE_ID`, `E2E_DATABASE_BASE_SCHEDULE_ID`, `E2E_DATABASE_ORDER_LOG_ID` — testing sheet id and tab gids

## Running locally (best effort)

```bash
export E2E_DB_KEY_JSON="$(cat path/to/db-key.json)"
export E2E_DATABASE_ID=... E2E_DATABASE_BASE_SCHEDULE_ID=... E2E_DATABASE_ORDER_LOG_ID=...
docker compose -f tests/docker/compose.yaml up -d --wait
uv run pytest tests/e2e -v --no-cov
docker compose -f tests/docker/compose.yaml down -v
```

The suite refuses to run within an hour of the Sunday 00:00 (US/Eastern) week flip.

## Local-run quirks

- The app's paramiko client auto-loads `~/.ssh` keys; SFTPGo drops the session after a failed
  public-key attempt, so locally run pytest with an empty profile:

  ```bash
  mkdir -p .cache/e2e-home
  USERPROFILE="$(pwd)/.cache/e2e-home" HOME="$(pwd)/.cache/e2e-home" uv run pytest tests/e2e -v --no-cov
  ```

  CI runners have no keys and don't need this.
- `.env` values may be quoted; when extracting `E2E_*` from it, strip quotes and CRs, e.g.:

  ```bash
  E2E_DATABASE_ID="$(grep -E '^DATABASE_ID=' .env | head -1 | cut -d= -f2- | tr -d '\r"')"
  ```
- The testing sheet's `store` column has a `"SFT"000` number format; the harness reads with
  `UNFORMATTED_VALUE` so this is transparent, but any new helper must do the same.
- A Windows-only `faulthandler` "access violation" dump can appear on stderr during FTP login;
  the tests still pass. Not seen on Linux so far.

## What the suite deliberately does not cover

Failure paths, the APScheduler wiring, Coremark, and vendor-exact server software (Files.com / Bitvise).
