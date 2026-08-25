# Master plan: migrating to aeth_ext v8.0.0

Written 2026-08-13 as an investigation into a routine "upgrade to aeth_ext v7" task. Rewritten
2026-08-24, now scoped as the actual working plan for the full migration to aeth_ext v8.0.0 — reviewed
directly against the diff of aeth_ext's `v8.0.0-dev` branch vs. its `main`, ahead of that branch
merging. Three pillars, each pairing aeth_ext-side work (done) with `scheduled-invoice-processor`-side
adoption work (not yet started):

- **Pillar A — fatal-exception-origin trail + graceful shutdown lifecycle.**
- **Pillar B — FTP connection-pooling performance.**
- **Pillar C — secret redaction (`SecretStr` credential typing).**

This repo's migration branch `chore/update-to-aeth-ext-v8` pins `aeth-ext[sftp, async]>=8.0.0` (`pyproject.toml`). Everything below is what's still needed to move that pin to a real
`8.0.0` release and actually adopt what it ships, not just avoid breaking on import.

## Decisions made 2026-08-25 (supersede the open questions below)

The buildable design lives in `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md`,
verified against aeth_ext 8.0.0 source. Resolutions, in the order the questions appear below:

- **Pin**: this branch is on `>=8.0.0` (locked 8.0.0); `main` is capped `<7` and carries the e2e gate
  suite (`tests/e2e`, PR #11, merged 2026-08-25). **PR #10 is never merged by an agent** — Jacob reviews.
- **A2**: full atomic save (`.tmp` + `os.replace`) after every mutation, inline under the existing lock;
  four files stay separate; cron job, `__del__` and `save_queue_backups_off_thread` removed.
- **A3**: `main()` returns after `await SHUTDOWN`; teardown = two THREADED callbacks (freeze scheduler at
  priority -10; required final Sheets flush via a new sync `DatabaseCache.flush_queued_writes()`).
  `sleep(600)`/`.errored` heuristic dropped with no replacement. Orphaned in-flight jobs accepted.
- **A4**: `err_handling.py` becomes `is_database_origin(trails)` over `ExceptionTrail.matches(...)`;
  `_last_fatal_details`/`get_last_fatal_details` are deleted (their only consumer was the retired
  `main()` block), not kept.
- **B2**: like-for-like at call sites, real pool underneath (`create_ftp_adapter`, defaults). `ftp_configs.py`
  is deleted; each vendor module owns its credentials loader; the SFT holding creds live in
  `suppliers/__init__.py`. `SFTPCredentials(host_key_policy="auto_add")` to match today's `AutoAddPolicy`.
- **C2**: satisfied by B2 (SecretStr value objects, raw dicts gone); `USER`/`HOSTNAME` not treated as secrets.
- **Dockerfile**: **not touched** — stays pinned pre-v8 until deployment is ready (Jacob, 2026-08-25).
- **Drag race**: baseline on 6.3.1 = 5.35 s mean per file (7 files, real RYO server, testing folders);
  after-run is the last plan task with the same harness (`scripts/benchmarks/dragrace_ryo.py`).

## How this started

A routine v7 upgrade turned up a real race condition: `startup.py::main()` drives its own
fatal-shutdown teardown (pause scheduler, sleep up to 600s, flush Google Sheets writes, `sys.exit(1)`)
directly after `await SHUTDOWN`, while aeth_ext's shutdown system *also* drives teardown independently
on a daemon thread and force-exits the main thread via `interrupt_main()` once its own, much shorter
budget is up — two uncoordinated teardown sequences on the same process, aeth_ext's finishing first
essentially always. This app predates and inspired aeth_ext's shutdown system, so fixing this is a real
redesign opportunity, not a compatibility patch.

Investigating that surfaced two more independent threads: aeth_ext TODO.md item #6
(`extract_details_callable` standardization — this repo was its only real production caller, via
`err_handling.py`'s `_is_database_origin_exception`), and FTP transfer performance (a 2026-08-13
profiling spike confirmed transfers within a batch already run fully concurrently — 16-way,
thread-pool-limited, 77 files in ~36s wall time — but `_transfer_file_vend_to_main` opens a **fresh**
vendor+waiting connection per file instead of once per batch, the single biggest fixable cost).

All three of those are now folded into Pillars A and B above. Pillar C (secret redaction) was not part
of the original investigation — it's aeth_ext work that landed independently on the same `v8.0.0-dev`
branch, discovered during the 2026-08-24 review, and is included here because it changes how this
repo's own FTP credentials should be typed once Pillar B is adopted (see A/B/C's overlap note under
Pillar C).

## What's already shipped on aeth_ext `v8.0.0-dev` (reviewed 2026-08-24)

Version bumped 7.0.0 → 8.0.0. Not yet merged to aeth_ext `main` as of this writing, but the diff is
final enough to plan against. Load-bearing facts for everything below:

- **`_BUDGETS` unchanged**: GRACEFUL=7s, FATAL=1s, FORCED=0s, still racing Docker's ~10s SIGKILL grace
  period.
- **`register_for_shutdown` callbacks are no longer zero-arg.** Every callback, every phase, now takes
  exactly one positional argument: `tuple[ExceptionTrail, ...]` (aliased `ShutdownCallback` in
  `shutdown.py`) — every fatal exception's trail accumulated so far this process, empty if none has
  occurred. aeth_ext's own `FTPConnectionPoolBase._shutdown_teardown` (`aeth_ext/ftp/pool/base.py`) is
  the shipped reference: `def _shutdown_teardown(self, trails: tuple[ExceptionTrail, ...]) -> None`.
- **`aeth_ext.errors.exception_trail`** is new: `build_exception_trail(exc)` walks an exception's
  traceback plus (by default) its `__cause__`/`__context__` chain and `BaseExceptionGroup` members into
  an `ExceptionTrail` — an ordered, deduplicated, origin-first tuple of `TrailEntry(module, category,
  file)`, `category` one of `OriginCategory.{FIRST_PARTY, THIRD_PARTY, STDLIB, UNPACKAGED}`.
  `ExceptionTrail.matches(*patterns)` (dot-segment globs — `*` one segment, `**` zero or more) returns
  every entry matching any pattern; this is the direct replacement for hand-rolled path-substring
  matching. **`_handle_fatal` now calls this automatically for every fatal exception** — there is no
  callback-hook parameter to opt into it. The result is exposed two ways: a new module-level
  `get_current_fatal_trails() -> tuple[ExceptionTrail, ...]` getter (`aeth_ext.errors.shutdown`), and
  as the argument every `register_for_shutdown` callback now receives. The old
  `extract_details_callable`/any renamed equivalent **does not exist** —
  `handle_fatal_exc_sync`/`handle_fatal_exc_async` dropped the keyword-argument/factory form entirely
  (confirmed: the `@overload` pairs, the kwarg, and the `testing_details_extractor` test stub are all
  gone; TODO.md item #6 is deleted, not checked off). A known, deferred edge case:
  aeth_ext TODO.md #9 — a single-file entrypoint installed loose directly under
  `site-packages`/`dist-packages` gets misclassified `THIRD_PARTY` instead of `FIRST_PARTY`; almost
  certainly doesn't apply to this repo's packaged entrypoint, worth a one-line sanity check rather than
  a blocker.
- **FTP got a full package restructure, not an incremental patch.** The old `aeth_ext.ftp.adapter`
  module (566 lines) is deleted outright. Pooling, opt-in keep-alive, server connection-ceiling
  discovery/re-probing, and validated reuse across `start_session()` calls are implemented across new
  `aeth_ext.ftp.pool.*`, `credentials.py`, `factory.py`, and `session.py` modules.
  **Heads-up (from the user, 2026-08-24, not yet visible in the diff reviewed here): the current
  `v8.0.0-dev` top-level convenience re-exports from `aeth_ext/ftp/__init__.py`
  (`create_ftp_adapter`, `FTPAdapter`, `SFTPAdapter`, `AdaptedFTP`, `AdaptedSFTP`, `HandleProvider`) are
  themselves about to be removed in a follow-up change, in favor of a more verbose but explicit surface
  — consumers importing directly from the specific submodule each name actually lives in
  (`aeth_ext.ftp.factory.create_ftp_adapter`, `aeth_ext.ftp.credentials.{FTPCredentials,SFTPCredentials}`,
  `aeth_ext.ftp.pool.ftp_adapter.FTPAdapter`, `aeth_ext.ftp.pool.sftp_adapter.SFTPAdapter`,
  `aeth_ext.ftp.session.{AdaptedFTP,AdaptedSFTP}`), rather than one flat `aeth_ext.ftp` namespace.**
  Pillar B's adoption work below should be designed against the explicit submodule paths, not the
  top-level package, since the latter is a moving target right now.
- **Secret redaction landed via `pydantic.SecretStr`.** aeth_ext's own credential settings
  (`alerts_email_pwd`, `alerts_pushover_token`, `alerts_pushover_user_key`, `alerts_healthcheck_pingkey`
  in `settings.py`) are now `SecretStr`/`SecretStr | None`, threaded through with unwrap-at-last-moment
  discipline in `utils.py::batch_send_emails`, `send_alert_email.py`, `send_alert_push.py`, and
  `monitoring/heartbeat.py`/`ping.py` (raw values never bound to a name longer than the expression that
  needs them, so `show_locals=True` traceback rendering — deliberately kept on — can't dump one from a
  lingering local/attribute). `aeth_ext.ftp.credentials.FTPCredentials`/`SFTPCredentials` already type
  `password`/`private_key_passphrase` as `SecretStr` too. Deliberately deferred, not blocking: TODO.md
  #11, a universal logging-filter backstop for secrets that reach `logger.*()` through an untraced path
  — a defense-in-depth item, not a known bug.
- **A concrete, low-risk cleanup this migration unlocks**: aeth_ext's `docker/Dockerfile` removes a
  `fonts-dejavu-core` apt install that was only needed because aeth_ext <8.0 rendered fatal-exception
  tracebacks to PNG via `resvg_py` with no font available on `bookworm-slim`. **This repo's own
  `docker/Dockerfile` carries the identical temporary workaround** (commit `0856e8c`, 2026-08-20,
  copied verbatim from aeth_ext's then-current Dockerfile) — safe to delete once the pin actually moves
  to 8.0+, since 8.0 bundles its own font.
- **aeth_ext deleted its own plan docs once implemented**, including its copy of this master plan
  (`.claude/plans/2026-08-13-exception-trail-design.md`, `-ftp-connection-pooling-design.md`/`-plan.md`,
  and the master plan itself are all gone on `v8.0.0-dev`) — alongside a same-day commit (`06b69c5`)
  adding an explicit warning to aeth_ext's own `CLAUDE.md` against trusting plan docs as current intent,
  after one there was caught contradicting a live instruction. Practical effect: **this file is now the
  only surviving copy** of this master plan; treat it the same way, as a snapshot that needs
  re-verifying against real code before being trusted, not as ground truth by default.

## Pillar A: fatal-exception-origin trail + graceful shutdown lifecycle

### A1 — `aeth_ext.errors.exception_trail` + shutdown-callback redesign (aeth_ext) — done

Shipped on `v8.0.0-dev` as described above. Nothing left to do here; A3/A4 below consume it.

### A2 — queue backups persist on every change, not just ~10min cron cadence (scheduled-invoice-processor)

**Not yet specced — brainstorm from scratch when picked up.** Blocks A3 (below); independent of
everything else.

**Problem**: `SupplierProcessorBase._save_backups()` (`suppliers/__init__.py`) only runs on a 10-minute
cron tick (`save_queue_backups_off_thread`, scheduled in `startup.py`) and in `__del__` (unreliable —
CPython doesn't guarantee `__del__` runs at interpreter exit for objects still referenced by
module-level singletons, and it calls the *synchronous* `_save_backups()` directly, racing the
`_lock` — an `aiologic.Lock`, cross-thread/cross-loop — against any concurrently running async holder of
that lock). Up to 10 minutes of queue-state changes (new pickups/dropoffs, files matched/transferred,
moves between `_file_pickup_queue`/`_file_waiting_queue`/`_file_preprocess_queue`/`_file_dropoff_queue`)
can be lost on an abrupt shutdown.

**Direction to start from, not a locked design**: persist on every mutation instead of on a timer. The
four queue dicts are mutated at several call sites across `_register_pickup`, `_register_dropoff`,
`_pickup_files`, `_preprocess_files`, `_dropoff_files`, `clean_stale_queue_entries` — all already under
`self._lock`. Open questions for whoever picks this up: does "every change" mean a full
`_save_backups()` call after every mutation (simplest, but re-serializes and rewrites all 4 files on
every tiny change — check whether that's cheap enough given pydantic's `dump_json` + full file
rewrite), or a debounced/batched write (more complex, reintroduces a small loss window); should the
four separate JSON files stay separate or could one combined write reduce I/O; does this move off the
`to_thread` dispatch pattern `save_queue_backups_off_thread` uses today.

### A3 — shutdown lifecycle redesign (scheduled-invoice-processor)

**Not yet specced — brainstorm from scratch when picked up.** Depends on A2 being *implemented*, not
just specced — the budget/durability reasoning below assumes queue backups are already durable on
every change.

**Core shape**: register scheduler-pause, queue-backup-flush (redundant with A2's always-current
backups once that lands, so may end up a no-op/thin confirmation rather than real work), and
`DatabaseCache.submit_queued_writes_to_pool` as `register_for_shutdown` THREADED-phase callbacks
(default priority, ahead of aeth_ext's own `LOGGING_TRANSPORT_PRIORITY=1000` transport teardown). Every
one of these must be written against the real shipped signature — `Callable[[tuple[ExceptionTrail,
...]], None]`, one positional arg — not a zero-arg callback; none of scheduler-pause/backup-flush/
`submit_queued_writes_to_pool` have an obvious use for the trail itself, so in practice this is a thin
`def _cb(_trails: tuple[ExceptionTrail, ...]) -> None:` shim around each. `main()`'s current
post-`await SHUTDOWN` block (scheduler pause, `sleep(600)`, Sheets flush, `sys.exit(1)`) is retired —
`await SHUTDOWN` becomes a signal to stop scheduling new work, not a place to drive teardown, since
teardown now lives in registered callbacks aeth_ext's ladder times/budgets/coordinates directly instead
of racing it on the same thread.

**Do NOT build a bounded-wait-on-FTP mechanism.** Investigated and rejected 2026-08-13: real per-file
transfer times (~5-7s each, confirmed by the profiling spike, and that's *before* Pillar B's pooling
removes the ~1-3s handshake cost — actual data-transfer time will still often approach or exceed the
7s GRACEFUL budget for anything but small files) mean a wait long enough to reliably catch an in-flight
transfer eats most/all of the GRACEFUL budget by itself, leaving little room for the guaranteed Sheets
flush. Lean entirely on A2 (durable-on-every-change queue state) to make abandoning in-flight FTP work
cheap to recover from; treat the guaranteed `DatabaseCache` flush as the only thing shutdown actively
waits on. Checkoff ordering already makes this safe: a per-key Google Sheets checkoff
(`schedule.check_box(...)`, `suppliers/__init__.py`) is queued into `DatabaseCache`'s write buffers only
*after* all files for that key finish transferring, so interrupting mid-batch forfeits (silently
redoes) only the not-yet-finished keys — it never corrupts anything, and finished keys are already
durable in `DatabaseCache`'s buffers before any Sheets flush.

**The `sleep(600)`/`.errored` heuristic needs its own fresh discussion when this is picked up.** Per the
original author (2026-08-13), it existed mainly to give the scheduler time for other non-errored
suppliers' in-flight cron jobs to "trickle down" naturally — a low-effort substitute for real
per-processor in-flight-work tracking, not because interrupting mid-transfer is unsafe (it isn't, per
the checkoff-ordering finding above). Once A2 lands, most of the original motivation (protecting
unpersisted queue state) evaporates; whether *any* replacement wait is still warranted should be
re-litigated fresh, not assumed away.

**Also unresolved, needs investigation when picked up**: `OrderProcessingScheduler.shutdown(wait=False)`
does not cancel or await in-flight jobs (asyncio tasks / `run_in_executor` futures already dispatched
via `CustomAsyncIOExecutor._do_submit_job`) — they become orphaned if the event loop is then closed.
Whether the redesign needs to explicitly gather/cancel `_pending_futures` with a bounded timeout, versus
accepting orphaned in-flight jobs as an acceptable cost (consistent with the FTP-abandonment conclusion
above), was flagged but never resolved.

### A4 — adopt the fatal-exception-origin trail in `err_handling.py` (scheduled-invoice-processor)

**Not yet specced, but urgent — not deferrable to a full brainstorming pass.**
`scheduler_config.py`'s `CustomAsyncIOExecutor._do_submit_job` currently decorates its callback with
`@handle_fatal_exc_sync(extract_details_callable=extract_exc_details)`. On v8.0.0, `handle_fatal_exc_sync`
takes no keyword arguments at all — this call site raises `TypeError:
handle_fatal_exc_sync() got an unexpected keyword argument 'extract_details_callable'` immediately at
decoration time. **The moment this repo's aeth_ext pin moves past v7, this is a hard runtime break.**

Current logic to preserve, relocated: `err_handling.py`'s `_is_database_origin_exception`
(hand-rolled frame-walking against `_DATABASE_FATAL_PATH_MARKERS = ("\\src\\database\\", "/gspread/",
"/google/oauth2/", ...)`) becomes a call to `ExceptionTrail.matches(...)` with module-glob patterns,
e.g. `trail.matches("scheduled_invoice_processor.database", "**.gspread.**", "**.google.oauth2.**")`.
The `_last_fatal_details` module-level dict and `get_last_fatal_details()` stay exactly as they are —
only the extraction *mechanism* changes.

**Migration shape**:

1. **Unblock the pin bump (small, can happen immediately)**: drop the
   `extract_details_callable=extract_exc_details` kwarg from `scheduler_config.py`'s decorator call.
   Move the (rewritten) database-origin check to read from `aeth_ext.errors.shutdown.get_current_fatal_trails()`
   instead of being invoked synchronously inside the decorator — the simplest landing spot today is
   right where `startup.py` already handles the post-`await SHUTDOWN` fatal path, since that code
   already only runs for a fatal shutdown.
2. **Clean version (naturally lands with A3)**: once shutdown teardown is callback-registered, move
   this check into a registered callback that receives `trails: tuple[ExceptionTrail, ...]` directly
   instead of calling `get_current_fatal_trails()` itself.

Also worth a one-line sanity check when this is implemented: aeth_ext TODO.md #9's single-file-entrypoint
misclassification (see "What's already shipped" above) — confirm it doesn't apply to this repo's
packaged entrypoint rather than assuming so.

## Pillar B: FTP connection-pooling performance

### B1 — connection pooling, keep-alive, ceiling discovery (aeth_ext) — done

Shipped on `v8.0.0-dev` as described above — everything the 2026-08-13 profiling spike called for
(reuse a connection across a batch instead of dialing fresh per file) is implemented internally to the
new pool.

### B2 — adopt the pooled FTP API in this repo (scheduled-invoice-processor)

**Not yet specced, and urgent — this is a hard break, not an enhancement.** This repo currently does:

```python
from aeth_ext.ftp.adapter import FTPAdapter   # suppliers/__init__.py, ftp_configs.py (__main__)
from aeth_ext.ftp.adapter import AdaptedFTP   # suppliers/__init__.py (TYPE_CHECKING), ftp_configs.py
```

(also referenced from `ryo.py`, `coremark.py`, `sas.py`) and constructs its pooled waiting-side adapter
directly: `FTPAdapter(SFTFTPClient, container_cls="SupplierProcessorBase")`. The entire
`aeth_ext.ftp.adapter` module is deleted on v8.0.0 — this import fails outright, `ImportError`, the
moment the pin moves to v8.

This repo's own FTP credentials are also a mismatch with the new shape: `ftp_configs.py`'s
`SFTFTPClient`/`CoremarkFTPClient`/`SASSFTPClient`/`RYOSFTPClient` each load a raw JSON creds file
(`SETTINGS.*_ftp_creds_file`) into a plain `dict` (`self.creds["PWD"]`, `self.creds["USER"]`, ...) and
pass the raw password straight into `FTP.login()`/`SSHClient.connect()`. aeth_ext's new
`aeth_ext.ftp.credentials.FTPCredentials`/`SFTPCredentials` value objects already type `password`/
`private_key_passphrase` as `SecretStr` (see Pillar C) — adopting them here isn't just an API-compat
fix, it's also this repo's most direct opportunity to close the same class of leak Pillar C addresses
upstream (a raw password sitting in `self.creds`, a long-lived instance attribute, is exactly the
lingering-secret-variable shape aeth_ext's audit flagged and fixed for its *own* credential fields).

**Design against the explicit submodule paths, not `aeth_ext.ftp`'s top-level package** — per the
"Heads-up" note above, the current top-level re-exports (`create_ftp_adapter`, `FTPAdapter`, etc.) are
already slated for removal in a follow-up aeth_ext change in favor of importing each name from where it
actually lives (`aeth_ext.ftp.factory`, `aeth_ext.ftp.credentials`, `aeth_ext.ftp.pool.ftp_adapter`,
`aeth_ext.ftp.pool.sftp_adapter`, `aeth_ext.ftp.session`). Re-verify the exact shape against aeth_ext's
actual code at implementation time rather than trusting this paragraph — this is exactly the kind of
detail that goes stale fastest.

**Worth deciding when picked up**: whether to do a like-for-like swap (minimum change to unblock the
pin bump) or use this as the occasion to actually realize Pillar B's original motivation in this repo —
reusing one connection across a batch instead of dialing fresh per file in `_transfer_file_vend_to_main`
(see "How this started" above). The new pool already does exactly that internally; a naive swap that
still constructs a fresh adapter per file would forfeit most of the performance win the pooling was
built for.

**Also unblocks a cleanup, not a blocker**: this repo's `docker/Dockerfile` carries a
`fonts-dejavu-core` install that's a direct copy of a now-obsolete aeth_ext workaround (see "What's
already shipped" above) — delete it once the pin is actually on 8.0+.

## Pillar C: secret redaction (`SecretStr` credential typing)

### C1 — `SecretStr` for aeth_ext's own credential settings (aeth_ext) — done

Shipped on `v8.0.0-dev`. Summary of what aeth_ext did, useful as the reference pattern for C2:
`alerts_email_pwd`, `alerts_pushover_token`, `alerts_pushover_user_key`, `alerts_healthcheck_pingkey`
(`settings.py`) became `SecretStr`/`SecretStr | None`; every call site that used to hold the unwrapped
value in a local or instance attribute for longer than the single expression that needed it was
restructured to unwrap only inline (e.g. `batch_send_emails`'s `smtp_password.get_secret_value()`
directly in the `server.login(...)` call, not bound to a name beforehand; `HeartbeatThread`'s
`self._pingkey` — previously a long-lived plain-`str` attribute — now stays `SecretStr` and unwraps only
inline in the one f-string that builds the ping URL). Deliberately *not* done, and out of scope for
this repo to wait on: TODO.md #11 (a universal logging-filter backstop) and disabling
`show_locals=True` traceback rendering (kept on intentionally, for debugging value) — the discipline
above is what protects against `show_locals` leaking a secret, not a config toggle.

### C2 — apply the same discipline to this repo's own plaintext secrets (scheduled-invoice-processor)

**Not yet specced.** This repo's own settings (`environment_settings.py`) don't store secrets as
pydantic string fields directly — `google_api_key_file`, `sft_website_creds_file`,
`sas_ftp_creds_file`, `ryo_ftp_creds_file`, `coremark_ftp_creds_file` are all `Path`s to JSON files, not
`str`/`SecretStr` settings fields. The actual leak surface is downstream, in `ftp_configs.py`: each of
`SFTFTPClient`/`CoremarkFTPClient`/`SASSFTPClient`/`RYOSFTPClient` reads its creds file into a plain
`dict` at class-definition time (`creds = loads(SETTINGS.*_creds_file.read_text())`) and keeps the raw
password (`creds["PWD"]`) sitting there for the process's entire lifetime — a class attribute, not even
a transient local — before passing it straight into `FTP.login()`/`SSHClient.connect()`. If any
exception with a live traceback through `get_conn_handler()` ever reaches `err_handling.py`'s
`show_locals=True` rendering, `self.creds` (or the class-level `creds` dict) is exactly the kind of
long-lived container aeth_ext's own audit (C1) flagged as the worse case, worse than a short-lived
local.

**Natural point to fix this**: folds directly into B2 (Pillar B's adoption of `FTPCredentials`/
`SFTPCredentials`, which already type `password`/`private_key_passphrase` as `SecretStr`) — migrating
`ftp_configs.py`'s four client classes off raw creds dicts and onto those value objects gets this
repo's FTP passwords `SecretStr`-wrapped for free as part of that work, rather than needing a separate
pass. Anything left over after that (e.g. whether `USER`/`HOSTNAME` values in the same JSON files
deserve the same treatment, or whether there's a lingering-variable case elsewhere in this repo's own
exception-handling paths worth auditing the way C1's "step 2" did for aeth_ext) is open — brainstorm
from scratch when this is picked up, using C1's finished pattern as the reference, not a locked design.
