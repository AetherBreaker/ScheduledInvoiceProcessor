# E2E baseline test suite — design

Written 2026-08-24. Establishes an end-to-end test suite on `main` (aeth-ext 6.3.1) that proves the
production order pickup/dropoff cycle works, so that the later migration to aeth-ext v8.0.0 can be
verified against the same suite.

## Goal and non-goals

**Goal:** a CI-run guarantee that the SAS and RYO supplier flows work end to end — vendor pickup,
staging on the SFT holding FTP, preprocessing, dropoff, and Google Sheet bookkeeping — against
servers that behave like production.

**Non-goals (explicit):**
- Not exhaustive. No failure-condition tests, minimal unit tests.
- Scheduler is not exercised (deferred; see "Future widening").
- Coremark is not covered (not live in production; not wired into `startup.py`).
- Local runnability is best-effort only. GitHub Actions is the target.
- No bug fixes on `main` as part of this work. Known issues are left as-is and characterised by the
  suite only where they surface: Coremark filename regex `{2}` (`suppliers/coremark.py:85`),
  `_dropoff_files` early return before draining `_file_dropoff_queue` (`suppliers/__init__.py:427`).

## Production topology being simulated

| Role | Production server | Software (banner-grabbed 2026-08-24) | Test stand-in |
|---|---|---|---|
| SFT holding ("waiting") FTP | `178.63.233.44:21` | Pure-FTPd `[privsep] [TLS]` | **Pure-FTPd** in Docker (exact software) |
| SAS vendor SFTP | `sassftp.sasinc.com:22` | Files.com (SaaS, not self-hostable) | **SFTPGo** in Docker |
| RYO vendor SFTP | `sftp.ryodist.com:2222` | Bitvise SSH Server 9.66 (Windows-only) | **SFTPGo** in Docker |
| Database | Google Sheet | gspread over Sheets API | **live testing spreadsheet** |

SFTPGo was chosen over OpenSSH-based images because the production vendors allow a writable root
(OpenSSH `sftp-server` chroot does not) and enforce per-user connection ceilings, which SFTPGo
replicates via `max_sessions`. Fidelity to Files.com/Bitvise specifically is deferred (would need a
Files.com sandbox / a Windows runner).

## Flow under test

Per supplier, mirroring each module's `main()` (`suppliers/sas.py:121-171`, `suppliers/ryo.py:431-500`):

1. `DatabaseCache().refresh_cache()`
2. `register_pickup` for every seeded schedule row
3. `pickup_files`
4. `register_dropoff` for every seeded row
5. `dropoff_files`
6. `submit_queued_writes_to_pool` / flush between stages, as `main()` does

Run with `__debug__ == True` and `USE_TESTING_FOLDERS=True`, so:
- vendor-side archive after pickup is *simulated* (no rename/delete on the vendor server);
- SFT-side paths are prefixed with `/Testing` (`/Testing/Waiting/<S>`, `/Testing/Waiting/<S>/Archive`,
  `/Testing/Processed/<S>`, `/Testing/<S>`). Vendor pickup paths are unchanged
  (`/Fastrax Invoices`, `/RYOtoSFT`) and exist as real directories on the SFTPGo containers.

## Test scenarios

`tests/e2e/`:

1. `test_sas_cycle` — N SAS files, each renamed through `/Testing/Waiting/SAS` → `/Testing/Processed/SAS`
   → `/Testing/SAS` (SAS has no merge step).
2. `test_ryo_cycle` — N RYO files for one customer, picked up into `/Testing/Waiting/RYO`, merged into
   a single `{customer_id}_{inv1-inv2-…}_{max_timestamp}.txt` under `/Testing/RYO`, originals in
   `/Testing/Waiting/RYO/Archive`.
3. `test_both_suppliers_same_process` — SAS and RYO processors constructed in one process sharing the
   `waiting_ftp` adapter, full cycle for both. This is what production does and where the
   `SingletonType` / shared-adapter interactions live.

Assertions per scenario:
- after pickup: file present in `/Testing/Waiting/<S>`; vendor original still present;
  `invoice_grabbed` ticked on the seeded rows;
- after dropoff: expected file(s) present in `/Testing/<S>` with expected names (for RYO, the merged
  header line matches the template layout); `invoice_applied` ticked;
- Processing Log: the tail contains one success row per invoice per stage for this run.

## Fixtures and data

- `tests/fixtures/templates/` — sanitized SAS and RYO invoice files derived from vendor archive
  samples (pulled read-only). Real customer numbers, invoice numbers and amounts are replaced with
  synthetic values; layout (fixed-width SAS header `ASAS␣{7}…`, pipe-delimited RYO header) is preserved
  exactly, since the app's header regexes parse them.
- `tests/e2e/generator.py` — builds N invoice files per supplier with filenames and headers valid for
  the current Sun–Sat window and given `customer_id`/`invoice_num`s, and uploads them to the vendor
  pickup dir. Both suppliers use `checks_date_in_filename = True`, so filename timestamps are what
  matter; mtime is not relied upon.
- `tests/e2e/sheet.py` — gspread helper: seed rows into `Current Week` (pickup/dropoff time = now,
  store/customer ids matching the generated files), delete exactly those rows on teardown, read the
  Processing Log tail. Fails fast if the current time is within a guard window of the Sunday
  00:00 week flip, so a run can never straddle it.
- `tests/e2e/conftest.py` — session fixture that writes a temporary `persisted_data/` tree
  (`secrets/{sft_creds,sas_ftp_creds,ryo_ftp_creds,db-key}.json` pointing at the Docker servers and
  the CI-provided service account) and sets env (`PERSISTED_DIR_LOC` pointing at that tree,
  `USE_TESTING_FOLDERS=True`, `DATABASE_ID`, `DATABASE_BASE_SCHEDULE_ID`, `DATABASE_ORDER_LOG_ID`,
  and a dummy `ALERTS_EMAIL_PWD`, which aeth-ext's `BaseSettings` requires with no default)
  **before** any `scheduled_invoice_processor` import, since `SETTINGS`, `CWD`, creds and the
  `/Testing` path mutation are all evaluated at import time. Creates the `/Testing/...` tree on
  Pure-FTPd and the vendor dirs on SFTPGo at session start; empties them at session end.

## Docker environment

`tests/docker/compose.yaml`:
- `sft-ftp`: Pure-FTPd, one user, passive port range published, home = holding root.
- `sas-sftp`: SFTPGo, one user, home containing `Fastrax Invoices/Archive`, `max_sessions` set.
- `ryo-sftp`: SFTPGo, one user, published on 2222, home containing `RYOtoSFT/Archive`, `max_sessions` set.

Users/passwords are fixed test values; ports are fixed and written into the generated creds JSONs.

## Changes to `src/` and project config on `main`

- `pyproject.toml`: `aeth-ext[sftp, async]>=6.2.2,<7` (CI guard; the win32 editable source is a
  development convenience and is unaffected); add `pytest-cov`, `pytest-asyncio` to the dev group
  (the existing `[tool.pytest.ini_options]` already assumes both).
- Application code: no changes unless the two-suppliers scenario needs a singleton reset hook; if
  so, that is the only change and it is behaviour-preserving.

## CI

`.github/workflows/e2e.yml`:
- trigger: `pull_request`, `workflow_dispatch`;
- `concurrency: e2e-sheet` with `cancel-in-progress: false` — the live sheet is a shared resource;
- steps: checkout → `docker compose -f tests/docker/compose.yaml up -d --wait` → set up `uv` with
  the SFTPyPI index credentials secret → `uv sync` → `uv run pytest tests/e2e` → `compose down`;
- secrets: `SFTPYPI_USERNAME`/`SFTPYPI_PASSWORD`, `TEST_DB_KEY_JSON`, `TEST_DATABASE_ID`,
  `TEST_DATABASE_BASE_SCHEDULE_ID`, `TEST_DATABASE_ORDER_LOG_ID`.

## Migration use

1. Suite passes on `main` pinned `<7` → merged.
2. The aeth-ext v8 migration branch rebases onto it, lifts the cap, adapts `src/`.
3. The same suite is the acceptance gate; test code must not import `aeth_ext` directly so it stays
   valid across both versions.

## Future widening (not in this spec)

- Option A scheduler coverage: import `startup`, start `AsyncIOScheduler`, force jobs via
  `job.modify(next_run_time=now)`.
- Coremark cycle once it goes live.
- Vendor-exact software job on a Windows runner (Bitvise, IIS FTP) and/or a Files.com sandbox.
