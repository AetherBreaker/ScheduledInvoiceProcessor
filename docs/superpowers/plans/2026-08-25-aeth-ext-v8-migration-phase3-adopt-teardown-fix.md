# aeth_ext v8 Migration — Phase 3: adopt the aeth_ext teardown-completion fix

**Status:** blocked on aeth_ext. Do not start until the fix for
`aeth_ext/ISSUE-shutdown-required-callbacks-race-interpreter-exit.md` has landed on aeth_ext `main` and been
released (v8.0.1 or v8.1.0).

**Goal:** Remove the app-side workaround for the aeth_ext gap (the `await sleep(20)` park at the tail of
`startup.main()`) and replace it with whatever lifecycle aeth_ext ships, so that "the required Sheets flush
completes before the process exits" is guaranteed by the library rather than by a timer we picked.

**Why this is its own phase:** the exact steps depend on the shape aeth_ext chooses — see the issue doc's §4:
(A) a new `TEARDOWN_COMPLETE` awaitable, (B) a non-daemon shutdown thread, or (C) both. The plan below is
written per option; delete the branches that don't apply once the fix is known.

**Spec:** `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-phase2-shutdown-design.md` §2.1–2.2 — the
"main() parks until the nudge" bullet is what this phase retires.

## Global Constraints

- Branch off `main` after Phase 2 (PR #10) has merged; bump `aeth-ext` in `pyproject.toml` / `uv.lock` to the
  release that carries the fix (Linux extra from the internal index, Windows editable path unchanged).
- The exit-code mapping in `__main__.run_app()` (`except KeyboardInterrupt` → `sys.exit(exit_code_for_shutdown(...))`)
  stays regardless of option: aeth_ext decides *when* the process exits, the app maps *how* to a code.
- Unit suite (`uv run pytest tests/unit`) and e2e (`tests/e2e`, docker compose) remain the gates; ruff clean.

## Tasks

### Task 1: Bump aeth_ext and confirm the new contract

- Bump the dependency; `uv sync`; run the unit suite (expect 44+ green, nothing else changed).
- Read the released `aeth_ext/errors/shutdown.py` and record in this file which option shipped and the exact
  names (e.g. `TEARDOWN_COMPLETE`, its `__await__`/`wait`/`is_set` surface, and whether the shutdown thread is
  still `daemon=True`).

### Task 2: Replace the park

- **Option A (awaitable):** in `startup.main()`, replace
  `with suppress(CancelledError): await sleep(20)` with `await TEARDOWN_COMPLETE` (import from
  `aeth_ext.errors.shutdown`). Keep it under `suppress(CancelledError)` only if the awaitable can raise it on
  the nudge; otherwise plain. Delete the `sleep` import if unused. Rewrite the comment: "aeth_ext signals when
  every shutdown callback has run; the nudge follows."
- **Option B (non-daemon thread):** delete the park entirely; `main()` returns after its tail. Comment: "the
  interpreter joins aeth_ext's non-daemon shutdown thread before `atexit`, so the required flush cannot race
  exit." Verify `run_app()`'s `except KeyboardInterrupt` is still reachable (the nudge may now land during
  interpreter shutdown instead) and that the exit code is still what `exit_code_for_shutdown` returns — if the
  nudge no longer reaches `run_app`, move the mapping to an `atexit` hook or accept that the code comes from the
  interpreter's own handling, and document which.
- **Option C:** do A; B is the safety net and needs no app change.
- Update `shutdown_hooks.py`'s module docstring and spec §2.2 ("`main()` parks until the nudge" bullet) to
  describe the new mechanism; note in the spec that the Phase 2 park was a workaround and cite the aeth_ext issue.

### Task 3: Prove it

- Unit: if option A, a test that `main()`'s tail awaits `TEARDOWN_COMPLETE` is hard to write without booting
  the app; instead assert the import and the absence of `sleep(20)` via a small source-level test, or skip
  unit coverage and rely on the next step. Do not fake the awaitable.
- Manual (the check that matters, same as Phase 2 Task 7 step 3): with the e2e compose fixtures up, run
  `python -O -m scheduled_invoice_processor`, wait for "Boot Done", `docker stop` / SIGTERM it while idle, and
  confirm the log shows `scheduler paused` and `final Google Sheets flush completed` (or "no queued … to
  flush") **before** the process exits, exit code 0. Repeat with a forced fatal → exit code 1. Record both
  timings here.
- e2e suite green.

### Task 4: Close out

- Remove this file's option branches that did not apply; commit; PR titled "chore: adopt aeth_ext <version>
  teardown-completion fix; drop the shutdown park".
- If aeth_ext also changed `SHUTDOWN`'s docstring or added a lint/deprecation for "returning on `await SHUTDOWN`",
  address any warning it emits.
