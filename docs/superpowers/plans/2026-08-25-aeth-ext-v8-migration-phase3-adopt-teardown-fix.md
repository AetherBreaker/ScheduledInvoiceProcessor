# aeth_ext v8 Migration — Phase 3: adopt the aeth_ext teardown-completion fix

**Status:** done 2026-08-25 (pending push / PR #10 update). aeth_ext **8.0.1** shipped **option C** of the
issue write-up: `SHUTDOWN_COMPLETE` (`ShutdownCompletion`, waitable via `is_set`/`wait`/`await`, set in
`_run_threaded_pass` once every callback has run or been skipped, immediately before the exit nudge) **plus**
`_join_pass_at_exit`, an `atexit` join of the still-daemon pass thread registered by `run_shutdown`. Awaiting
`SHUTDOWN_COMPLETE` also *declares a tail*: the nudge is held until the GRACEFUL budget (7 s from the request —
not Docker's 30 s) or completion + 0.25 s, whichever is later, and skipped once the main thread has finished.
`SHUTDOWN`'s docstring now says resolution ≠ teardown done. Lifecycle:
`await SHUTDOWN` → tail → `await SHUTDOWN_COMPLETE` → return.

**Goal:** Remove the app-side workaround for the aeth_ext gap (the `await sleep(20)` park at the tail of
`startup.main()`) and replace it with the library's lifecycle, so that "the required Sheets flush completes
before the process exits" is guaranteed by aeth_ext rather than by a timer we picked.

**Spec:** `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-phase2-shutdown-design.md` §2.1–2.2 (the
"`main()` waits for `SHUTDOWN_COMPLETE`" bullet is this phase's outcome).

## What was done

| Task | Commit | Outcome |
|---|---|---|
| 1. Bump to `aeth-ext>=8.0.1`, refresh `uv.lock`, confirm contract | `d674193` | 44/44 unit on 8.0.1; option C confirmed by reading `shutdown.py` |
| 2. `await SHUTDOWN_COMPLETE` replaces the park; comments, `shutdown_hooks` docstring, spec §2.2 updated; `run_app()` unchanged except its comment | `5742ada` | reviewed (opus, adversarial): mechanism correct, "with fixes" = doc accuracy |
| 2b. Review fixes: nudge window also spans `asyncio.run()`'s close (in-flight FTP joins); unbounded wait is deliberate; dead issue link | `be9b148` | — |
| 3. Proof | — | see below |
| 4. Close-out | this commit | option branches pruned from this plan |

The exit-code mapping in `__main__.run_app()` (`except KeyboardInterrupt` → `sys.exit(exit_code_for_shutdown(...))`)
stays: aeth_ext decides *when* the process exits, the app maps *how* to a code. The `except` is now the
exceptional path (tail or `Runner.close()` outran the budget), not the normal one.

## Task 3 — proof (2026-08-25, local, docker compose fixtures + testing sheet)

- **e2e:** 9/10 on the first run — `test_ryo_cycle` failed with pure-ftpd `425 Unable to identify the local data
  socket: Address already in use`; passed alone on rerun. Fixture limit (10 passive ports `30000-30009`, parallel
  RYO wave right after earlier tests' sockets in TIME_WAIT), not a regression; `425` is also not in
  `TRANSIENT_TRANSFER_ERROR_STRINGS` so it is not retried. Follow-up: widen `FTP_PASSIVE_PORTS` in
  `tests/docker/compose.yaml` and/or add `"425"` to the transient list. All other 9 green, including `test_sas_cycle`.
- **Graceful stop, idle, `-O`, real `startup.main()`** (harness: `DatabaseCache.flush_queued_writes` wrapped with a
  1.5 s delay to widen the race window; `CTRL_BREAK_EVENT` sent 3 s after "Boot Done" — Windows' SIGTERM stand-in,
  aeth_ext registers `SIGBREAK`):

  | event | t (s from boot) |
  |---|---|
  | signal sent | ~15.15 |
  | `flush_queued_writes` entered (shutdown thread) | 15.178 |
  | flush returned | 16.678 |
  | `main()` returned | **16.686** (8 ms *after* the flush) |
  | interpreter `atexit` | 16.798 |

  `SHUTDOWN_COMPLETE.is_set()` = True at exit; **no `KeyboardInterrupt`** (nudge skipped because `main()` returned
  within its window); kind GRACEFUL; exit code **0**; aeth_ext banner "teardown complete; 5 run, 0 skipped"; 1.9 s
  wall from signal to exit. Under the Phase 2 code before the park, `main()` would have returned at ~15.2 s, mid-flush.
- **FATAL exit code:** not driven end-to-end (no safe way to raise a fatal in the running app without a code hook);
  covered by `tests/unit/test_exit_code.py` (mapping) and aeth_ext's own `tests/errors/test_shutdown.py`
  (required callbacks run under the 1 s FATAL budget; declared tail gets ≥ 0.25 s). The graceful run exercises the
  same path with a larger budget.

## Follow-ups (not blocking)

- e2e fixture passive-port range / `425` retry (above).
- Phase 2 ledger deferred minors (see `.superpowers/sdd/2026-08-25-aeth-ext-v8-migration-phase2/progress.md`),
  notably: RYO `_middle_archive_file` cleanup not gated on the persist result the way pickup is; Pyright noise in
  the new unit tests.
