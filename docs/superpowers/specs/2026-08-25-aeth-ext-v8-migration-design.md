# aeth-ext v8.0.0 migration — design

Written 2026-08-25. Resolves the open questions in `.claude/plans/2026-08-13-shutdown-and-ftp-master-plan.md`
into a buildable design, verified against aeth_ext 8.0.0 source (`../aeth_ext`, branch `main`, HEAD `ffff2fbd`)
rather than the plan's snapshot. The e2e suite from
`docs/superpowers/specs/2026-08-24-e2e-baseline-test-suite-design.md` is the acceptance gate.

## Goal and hard constraints

**Goal:** `scheduled-invoice-processor` runs on `aeth-ext[sftp, async]>=8.0.0` with the same production
behaviour proven by the e2e suite, and adopts what v8 ships in two phases: **Phase 1** — pooled FTP with `SecretStr` credentials
(Pillars B/C), the minimal fatal-exception-trail adoption that unblocks the pin (A4), durable queue state
(A2), and the before/after drag race; **Phase 2** — the shutdown lifecycle (A3), specced separately once the
drag-race numbers say whether an in-flight wave of pooled transfers can finish inside the shutdown budget.

**Constraints (binding on every task):**
- Work happens on branch `chore/update-to-aeth-ext-v8` and is pushed to **PR #10 only. The PR is never
  merged by the implementer or the controller** — Jacob reviews the final diff and merges himself.
- `docker/Dockerfile` is **not modified** (it stays pinned to the pre-v8 image until Jacob has everything
  ready to deploy; the `fonts-dejavu-core` block stays).
- Test code never imports `aeth_ext`. The e2e suite's assertions are not weakened.
- Coremark is migrated mechanically (its module must import) but stays unwired and untested.
- `__debug__` semantics stay as they are (testing folders, simulated vendor archive, no-op fatal decorators
  under `__debug__`).

## aeth_ext 8.0.0 facts this design relies on (verified 2026-08-25)

| Surface | Fact | Source |
|---|---|---|
| `aeth_ext.ftp` | only re-exports `create_ftp_adapter(credentials, **kw)`; everything else from its submodule | `ftp/__init__.py:1-17` |
| `create_ftp_adapter` | kwargs: `max_connections=16`, `chunk_size=8192`, `pbar=None`, `tzinfo=SETTINGS.tz`, `container_cls=None`, `container_cvar=None`, `keepalive_interval=None`, `acquire_timeout=30.0` (+`channels_per_transport=4` for SFTP); returns `FTPAdapter`/`SFTPAdapter` | `ftp/factory.py:39-91` |
| `FTPCredentials` | `host, username, password: SecretStr, port=21, use_tls=False, verify_tls=True, protect_data_channel=None, passive_mode=True, connect_timeout=None` | `ftp/credentials.py:24-59` |
| `SFTPCredentials` | `host, username, port=22, password: SecretStr\|None, private_key_path, private_key_passphrase, host_key_policy: "auto_add"\|"reject" = "reject", known_hosts_path, connect_timeout` | `ftp/credentials.py:62-91` |
| SFTP connector | `look_for_keys=False, allow_agent=False` — no `~/.ssh` auto-load | `ftp/sftp_connector.py:88-92` |
| pool | `start_session()` → `AdaptedFTP`/`AdaptedSFTP` context manager; `test_connection()`, `close()`; `pbar` is a plain public attribute; each pool registers its own THREADED shutdown teardown | `ftp/pool/base.py:302,492-528,588-607` |
| session API | same method names as v6: `upload_file(remote_path, callback, file_size, task_msg="")`, `download_file(remote_path, callback, task_msg="")`, `transfer_file(source_remote_path, dest_remote_path, other, task_msg="", callback=None, mem_stream=None) -> bool`, `get_size`, `rename`, `remove`, `listdir -> Iterator[ListDirResult(filename, modified_time)]`, `makedir`, `test_connection(logit=False)` | `ftp/session.py:179-292` |
| old API | `aeth_ext.ftp.adapter`, `FTPProtocol`/`SFTPProtocol`, `get_conn_handler` — **gone** | grep, `ftp/credentials.py:2` |
| `handle_fatal_exc_sync` | decorator with **no parameters**; no-op under `__debug__` unless module is `__main__` | `errors/err_handling.py:231-248` |
| shutdown | `SHUTDOWN` awaitable with `.kind`; `ShutdownKind{RUNNING,GRACEFUL,FATAL,FORCED}`; `ShutdownPhase{INTERRUPT,THREADED}`; `register_for_shutdown(cb, *, phase, priority=0, required=False)` with `cb(trails: tuple[ExceptionTrail, ...]) -> None` (bound methods held by `WeakMethod`); `get_current_fatal_trails()`; budgets 7 s / 1 s / 0 s; `install_shutdown_signal_handlers()` no-op under `__debug__` | `errors/shutdown.py:76-97,244-269,287-295,362-376,424-480,687-712` |
| trail | `build_exception_trail(exc, *, walk_chain=True, walk_groups=True)`; `ExceptionTrail.matches(*patterns) -> tuple[TrailEntry, ...]` with `*`/`**` dot-segment globs | `errors/exception_trail.py:374-433` |
| unchanged imports | `aeth_ext.settings.BaseSettings`, `types.abc.SingletonType`, `types.StrEnum`, `utils.{today,get_now,get_last_sat,get_next_sat}`, `monitoring.{run_heartbeat_async,send_heartbeat}`, `rich.progress.Progress`, `logging.setup.BaseLoggingConfig`, `logging.bases.TaggedLogRecord`, `monkey_patcher.MonkeyPatcher`, `errors.send_alert_email.send_alert_email`, `aeth_ext.initialize` | verified present |

## Sequencing

**Phase 1 (this spec's implementation plan)**, ordered so the e2e suite goes green as early as possible:

1. **B2 + C2** — pooled FTP with per-vendor credentials
2. **A4 (minimal)** — decorator fix, `err_handling.py` deleted, interim origin check inlined in `main()`
   (app imports and runs on v8 after steps 1–2)
3. **README cleanup** — e2e README no longer needs the `HOME`/`USERPROFILE` workaround
4. **A2** — durable queue state (+ `atexit` safety net)
5. **Drag race after-run** — same harness on v8; results recorded here. **This is the decision gate for Phase 2.**

**Phase 2 (separate spec, after step 5)**: A3 shutdown lifecycle — see "Phase 2 — deferred" below.

## B2 + C2 — pooled FTP, credentials grouped by vendor

- `src/scheduled_invoice_processor/ftp_configs.py` is **deleted**.
- Each vendor module owns its credentials loader:
  - `suppliers/sas.py`: `def load_credentials() -> SFTPCredentials` reading `SETTINGS.sas_ftp_creds_file`
    (`HOSTNAME`, `PORT` default 22, `USER`, `PWD` → `password=SecretStr(...)`, `host_key_policy="auto_add"`).
  - `suppliers/ryo.py`: same shape over `SETTINGS.ryo_ftp_creds_file`.
  - `suppliers/coremark.py`: `def load_credentials() -> FTPCredentials` over `SETTINGS.coremark_ftp_creds_file`
    (`HOST`, `PORT` default 21, `USER`, `PWD`).
  - `suppliers/__init__.py`: `def load_sft_credentials() -> FTPCredentials` over `SETTINGS.sft_website_creds_file`
    (the holding server is shared, so it lives with `waiting_ftp` on the base class).
  - Loaders return the value object and bind the raw JSON to nothing longer-lived than the call — the
    plaintext-password-in-a-class-attribute surface (`creds`, `pickup_ftp_creds`) is removed; that is C2.
- Adapter construction stays where it is today, at class level, via the factory:
  - base: `waiting_ftp: FTPAdapter = create_ftp_adapter(load_sft_credentials(), container_cls="SupplierProcessorBase")`
  - SAS/RYO: `vendor_ftp: SFTPAdapter = create_ftp_adapter(load_credentials(), container_cls="SASProcessor")`
  - Coremark: `vendor_ftp: FTPAdapter = create_ftp_adapter(load_credentials(), container_cls="CoremarkProcessor")`
  - `__init__` keeps injecting the progress bar: `self.waiting_ftp.pbar = pbar` / `vendor_ftp.pbar = pbar`.
  - Defaults are kept (`max_connections=16`, no keep-alive, `acquire_timeout=30`); the pool's ceiling
    discovery adapts to Files.com/Bitvise session caps. Jobs run every 10 minutes, so idle keep-alive would
    only churn connections.
- Every `start_session()` call site is unchanged. Type hints move: `from aeth_ext.ftp.session import AdaptedFTP, AdaptedSFTP`
  (TYPE_CHECKING), `from aeth_ext.ftp.pool.ftp_adapter import FTPAdapter`, `from aeth_ext.ftp.pool.sftp_adapter import SFTPAdapter`,
  `from aeth_ext.ftp.errors import ServerNotAvailableError` where still referenced.
- `pickup_ftp_creds` class attributes are removed from all three processors (nothing reads them).
- The `if __name__ == "__main__":` smoke script in `ftp_configs.py` is deleted with the module (the e2e
  suite supersedes it).

## A4 (minimal, Phase 1) — unblock the pin bump

- `scheduler_config.py`: `@handle_fatal_exc_sync` with no arguments (the kwarg raises `TypeError` at
  decoration on v8); the `extract_exc_details` import goes.
- `src/scheduled_invoice_processor/err_handling.py` is **deleted** — a module for one function is not worth
  keeping. `typing_custom.FatalDetails` goes with it.
- `startup.main()`'s existing post-`await SHUTDOWN` block is kept **structurally as-is for Phase 1**
  (including the `sleep(600)`/`.errored` heuristic — its fate is a Phase 2 decision), with exactly one
  substitution: every `get_last_fatal_details()["is_database_origin"]` read becomes an inline check over
  `get_current_fatal_trails()` from `aeth_ext.errors.shutdown`:
  `any(trail.matches("scheduled_invoice_processor.database", "**.gspread.**", "**.google.oauth2.**") for trail in trails)`.
  The `exception_type`/`exception_message` log fields are replaced by `trail.origin` of the first matching
  trail. This is the master plan's own "step 1: unblock the pin bump" shape.
- Unit test: build real trails with `build_exception_trail` from an exception raised inside
  `scheduled_invoice_processor.database` and from one raised elsewhere; assert the inline expression (extracted
  into a tiny private helper in `startup.py` only if needed for testability — the spec does not require one).

## A2 — queue state durable on every change

- New `SupplierProcessorBase._persist_queues() -> None` (sync): for each of the four queue dicts, dump JSON
  with the existing `_queue_ta`, write to `<backup_file>.tmp`, `os.replace` onto the real path. Atomic on
  both platforms; a crash mid-write leaves the previous file intact, so the existing quarantine path only
  ever sees genuine corruption.
- Called at the end of every mutating block, all of which already hold `self._lock`:
  `_register_pickup`, `_register_dropoff`, `_pickup_files` (after queue moves), `_preprocess_off_thread`
  (after the queue swap), `_dropoff_files` (after pops), `_clean_stale_queue_entries` (replacing the
  post-hoc `to_thread(self._save_backups)`).
- Removed: `__del__`, `save_queue_backups_off_thread`, the `_save_backups` method, and the
  `save_queue_backups_off_thread` cron job in `startup.py`. `_load_queue_backups` is unchanged.
- Belt and braces: `__init__` registers `atexit.register(self._persist_queues_at_exit)` once per processor
  instance (they are singletons, so once per process per supplier). `_persist_queues_at_exit()` tries
  `self._lock.acquire(timeout=1.0)`; on success it calls `_persist_queues()` and releases; on timeout it
  logs a warning and calls `_persist_queues()` anyway — a possibly mid-mutation snapshot beats losing the
  last change, and the atomic write still guarantees a parseable file. Replaces `__del__`, which CPython
  does not guarantee to run for module-level singletons.
- The write is a few KB and happens a handful of times per 10-minute cycle; doing it inline under the lock
  is simpler and safer than a debounced writer (rejected: reintroduces a loss window and another thread
  for A3 to coordinate).
- Unit test on a temp `PERSISTED_DIR_LOC`: after a mutation the file reflects it immediately; a patched
  `os.replace` that raises leaves the original file unchanged and the `.tmp` file cleaned up or ignored by
  the loader; `_persist_queues_at_exit()` writes when the lock is free and still writes (with a warning) when
  the lock is held by another thread.

## Phase 2 — deferred: A3 shutdown lifecycle

Built in Phase 2 — see `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-phase2-shutdown-design.md`
(decision: THREADED callbacks + crash-safe queue transitions, no wait-for-wave; the "bounded by Docker's stop
grace" premise was wrong because aeth_ext's threaded pass ends by raising `KeyboardInterrupt` on the main
thread) and `docs/superpowers/plans/2026-08-25-aeth-ext-v8-migration-phase2.md`. The `await SHUTDOWN` regression
noted below is closed there.

## e2e README cleanup

`tests/e2e/README.md`'s "Local-run quirks" loses the `HOME`/`USERPROFILE` workaround (v8's connector never
loads `~/.ssh`), keeps the other notes. No test code changes are expected; if the suite needs any, they are
their own reviewed task and must not weaken assertions.

## Drag race — before/after connection pooling

- Harness: today's throwaway is committed as `scripts/benchmarks/dragrace_ryo.py` (outside pytest
  `testpaths`), with `scripts/benchmarks/dragrace_before.json` beside it. It mirrors `suppliers.ryo.main()`
  with `perf_counter` per stage, times each `_transfer_file_vend_to_main`, runs against the **real** RYO
  server with `USE_TESTING_FOLDERS=True`, the testing sheet and `__debug__` on (vendor archive simulated), then
  resets the rows it ticked and clears `/Testing/RYO`, `/Testing/Waiting/RYO[/Archive]`, `/Testing/Processed/RYO`.
- Baseline (aeth-ext 6.3.1, 2026-08-25 00:37, 7 current-week files): per-file mean **5.35 s**, max 5.45 s;
  `pickup_files` 10.8 s; `dropoff_files` 24.0 s; whole cycle 40.5 s.
- After-run (aeth-ext 8.0.0, 2026-08-25 02:05, the same 7 current-week files): per-file mean **4.68 s**
  (−12.5 %), max 5.34 s; `pickup_files` 10.1 s; `dropoff_files` **3.4 s** (was 24.0 s — the pooled holding-FTP
  renames no longer pay a connect/login per file); whole cycle 19.7 s (was 40.5 s). Raw:
  `scripts/benchmarks/dragrace_after.json`. Per-file time is dominated by the vendor server's per-transfer
  cost (SFTP open + stream + close on Bitvise), which pooling does not remove.
- Phase 2 decision input: the wave numbers above were measured against the wrong constraint (see the Phase 2
  spec §1). Decision taken 2026-08-25: no wait-for-wave; callbacks plus the F1–F7 ordering fixes.
- Running it needs the real credentials and writes to the testing sheet/holding tree — it is a manual
  task, not CI.

## Out of scope / explicitly deferred

- `docker/Dockerfile` (stays pre-v8 until deployment is ready).
- Scheduler-level e2e coverage (option A), Coremark tests, TODO #11 logging filter, treating `USER`/`HOSTNAME`
  as secrets, batching in-flight job cancellation on shutdown.
- Known bugs left as-is: Coremark filename regex `{2}` (`suppliers/coremark.py:85`); `_dropoff_files`
  early-return before draining `_file_dropoff_queue` (`suppliers/__init__.py:427`).
