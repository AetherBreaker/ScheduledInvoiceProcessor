# aeth_ext v8.0.0 migration — Phase 2: shutdown lifecycle (A3) and crash-safe queue transitions

**Date:** 2026-08-25
**Branch:** `chore/update-to-aeth-ext-v8` (on top of Phase 1, PR #10)
**Supersedes:** the "Phase 2 — deferred" section of
`docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md` and the "Phase 1 outcome and Phase 2
inputs" section of `.claude/plans/2026-08-13-shutdown-and-ftp-master-plan.md`.

## 1. Decision: no wait-for-wave; callbacks + crash-safe job ordering

Phase 1 recorded two candidate shapes for A3 and left the choice open. Both were measured against the wrong
constraint. This is what aeth_ext 8.0.0 actually does (`aeth_ext/errors/shutdown.py`):

1. `run_shutdown()` runs the INTERRUPT pass inline, starts the daemon thread `aeth-ext-shutdown` for the
   THREADED pass and returns immediately.
2. The threaded pass runs callbacks in priority order against the budget (GRACEFUL 7 s / FATAL 1 s /
   FORCED 0 s). The skip test is `budget exhausted and not reg.required` — `required=True` callbacks run
   regardless of budget.
3. When the callbacks are done it calls `_attempt_early_exit()` → `interrupt_main()`, which simulates SIGINT
   on the main thread; `_handle_shutdown_signal` sees `_exit_nudge_sent` and defers to
   `signal.default_int_handler` → **`KeyboardInterrupt` is raised on the main thread**. Without aeth_ext's
   handler (non-`-O` runs) Python's stock handler raises it anyway.

So `main()`'s code after `await SHUTDOWN` is **not** bounded by Docker's stop grace; it is bounded by how
long the threaded pass takes, which with only the pools' teardown and the logging transport registered is
well under a second. A 20 s `asyncio.wait` in `main()` would be interrupted almost immediately. The
"controller's recommendation" in the master plan is therefore unbuildable as written and is dropped.

The remaining honest version of a wait (a `required=True` THREADED callback that blocks on the in-flight
wave via a cross-loop future) is not worth building either:

- jobs fire every 10 min and a whole cycle is ~20 s, so a shutdown lands mid-wave ≈3 % of the time, and
  shutdowns themselves are rare;
- the loss when it does is bounded work-redo, **provided** the queue transitions are crash-safe — which
  §3 shows they currently are not, so the real work is there, not in a wait;
- the FATAL case argues against waiting at all (the in-flight job is usually the broken one).

**Decision:** the spec sketch ("callbacks") shape, plus the regression fix, plus §3's ordering fixes.

## 2. Shutdown lifecycle (A3)

### 2.1 What runs where

| Concern | Where | Why |
|---|---|---|
| Stop new jobs starting | THREADED callback `freeze_scheduler`, `priority=-10` | `AsyncIOScheduler.pause()` is thread-safe (`wakeup` goes through `call_soon_threadsafe`). `shutdown()` is **not** called here: `AsyncIOExecutor.shutdown` cancels asyncio tasks from a foreign thread. |
| Flush queued Google Sheets writes | THREADED callback `final_sheets_flush`, `priority=0`, `required=True` | Queued writes live only in memory — unlike the queue backups (A2) they *are* lost on a kill. Runs on aeth_ext's shutdown thread, concurrently with the tail of `main()`, and is guaranteed to finish before the nudge that ends the run. Skipped when any fatal trail is database-origin (A4's `trail_is_database_origin`). Calls a new sync `DatabaseCache.flush_queued_writes()` which wraps the existing thread-safe `_api_write()` (`aiologic.Lock` is usable from plain threads). |
| Cancel heartbeat, stop scheduler | `main()` after `await SHUTDOWN` | Best-effort: the nudge may pre-empt it; both are harmless to lose (the nudge cancels every task anyway). |
| Exit code | `run_app()` in `__main__.py` | `run(main())` is wrapped in `except KeyboardInterrupt` (the nudge), then `sys.exit(exit_code_for_shutdown(SHUTDOWN.kind))`: `0` for GRACEFUL / not-requested, `1` for FATAL or FORCED. `main()` no longer calls `sys.exit`. |
| Queue backups | nothing new | A2: persisted on every mutation and once more at `atexit`. |

The `sleep(600)` / `.errored` heuristic and every "Fatal shutdown:" log line in `main()` are deleted. This
also closes the **known regression** carried out of Phase 1 (`await SHUTDOWN` resolves on every kind, so
`docker stop` currently runs the fatal path, sleeps up to 10 min and exits 1).

Callbacks are registered from a new module `scheduled_invoice_processor/shutdown_hooks.py` by
`register_shutdown_hooks(scheduler, cache)`, called from `main()` once `bootstrap_runtime` has returned
(there is nothing to freeze or flush before that). Registration happens **after** `aeth_ext.initialize()`
(already the case: `run_app()` initialises before `run(main())`).

### 2.2 What happens on each kind of stop

- **`docker stop` / SIGTERM (GRACEFUL):** callbacks freeze the scheduler and flush the sheet; the nudge
  cancels the in-flight job task at its current `await`; `asyncio.Runner.close()` joins the default
  executor, so every in-flight `to_thread` body (transfer, rename, archive, upload) **runs to completion**;
  `atexit` persists the queues; exit 0. What is lost: the post-`gather` bookkeeping of the cancelled job.
- **FATAL (`_handle_fatal`):** same, with a 1 s budget for non-required callbacks; the flush still runs
  (required) unless the trail is database-origin; exit 1.
- **`main()` parks until the nudge:** `run_shutdown()` requests the shutdown and starts the threaded pass in
  the same synchronous stretch (inside the signal handler / `_handle_fatal`), so by the time the `await SHUTDOWN`
  waiter wakes the thread has *started* — but not *finished*: the flush is a Sheets HTTP round trip, while
  `main()`'s tail is milliseconds. `main()`'s tail therefore runs alongside the callbacks, not after them.
  After cancelling the heartbeat and stopping the scheduler, `main()` therefore `await sleep(20)`s: without it,
  an idle `docker stop` would let the interpreter exit in milliseconds while the required Sheets flush was
  mid-HTTP-request. The park is bounded so a nudge that never arrives cannot outlast the 30 s grace, and it
  never delays a FATAL stop — the nudge arrives after the 1 s FATAL budget and ends the park early.
- **SIGKILL (Docker grace exhausted / OOM):** threads die mid-body; no `atexit`. The last persisted queue
  state is whatever the last `_persist_queues()` wrote.

### 2.3 Docker

The exit sequence is callbacks (≤7 s) + executor join (an in-flight vendor transfer is ~5 s, a wave of
renames a few seconds) + `atexit`. That can exceed Docker's **default 10 s** grace, and a SIGKILL during the
join loses the `atexit` save. `docker/compose.yaml` therefore gains `stop_grace_period: 30s` — it is
required, not insurance. Rolling updates stay **off** for this service in Coolify: it is single-instance
with queue files on a bind mount, and two overlapping instances sharing those files is a worse failure than
20 s of downtime.

## 3. Idempotency audit: can a stop at the wrong point strand state?

Method: for each job, list every durable side effect (remote file, queue backup, sheet cell) in the order it
is made, and ask what a reboot does from each gap. "Graceful-reachable" means the gap can be hit by the nudge
(main-thread cancellation while threads complete); "SIGKILL-only" means only a hard kill mid-thread reaches
it. Boot runs `_clean_stale_queue_entries`, which moves a pickup entry to *waiting* when the sheet already
says `invoice_grabbed`/`manually_moved`, and drops any entry whose sheet row says `invoice_applied`.

### F1 — `_pickup_files`: vendor archive before the queue commit (SIGKILL-only, strands)

Order today: vendor→waiting copies → `check_box(invoice_grabbed)` (cache) → **vendor archive renames** →
queue pickup→waiting → persist.

- Graceful: cancellation anywhere after the copies leaves the sheet flushed (§2.1) and the queue in
  pickup; boot's clean-stale moves it to waiting because the sheet says grabbed. Recovers.
- SIGKILL after the archive renames, before persist, with the sheet write not yet flushed: files gone from
  the vendor folder, sheet says not grabbed, queue says pickup. Next pickup matches nothing, forever
  ("No files matched"), until the entry goes stale. **Manual intervention.**

Fix: commit first, archive last — after the copies: `check_box` → move queue entries → `_persist_queues()`
→ *then* `await gather(*archive_futures)`. A stop after the commit leaves an already-copied file in the
vendor folder. Nothing ever archives it: the entry is in waiting so it is never re-matched and there is no
sweeper, so the copy is permanent but inert clutter. Also reset `pickup_success` when `file_names` is re-matched, so a
persisted `True` from a previous partial run cannot satisfy `all(...)` for a smaller re-match (F6).

### F2 — base `_preprocess_files` (SAS) and `_dropoff_files`: non-idempotent renames (graceful-reachable, strands)

Both do `waiting_ftp.rename(src, dst)` per file on threads, then post-`gather` bookkeeping. The nudge
cancels the task during `await gather`; the renames complete; the queue is persisted with the *old*
folder. On reboot the rename is retried with a source that no longer exists → exception → `*_success[idx]
= False` → the entry sits in the preprocess (or dropoff) queue until stale; the file sits in the target
folder. **Manual intervention, and reachable by a plain `docker stop`.** (`_transfer_file_main_to_main`
swallows the exception and logs; it never marks success.)

Fix: make the rename idempotent in `_transfer_file_main_to_main`: if `rename` raises and the destination
exists with size > 0 while the source is absent, treat it as already moved (log at INFO, set success). A
destination that exists *alongside* a still-present source is left as the failure it is (never overwrite).

### F3 — `_preprocess_off_thread` (RYO and Coremark): queue commit before the merged-file upload (SIGKILL-only, strands)

Order today: download originals → merge locally → **commit** (dropoff queue gets the merged name and the
post-processing folder, preprocess entry popped, persist) → archive originals → delete local originals →
**upload merged file** → delete local merged.

SIGKILL between commit and upload: the dropoff queue references a file that does not exist on the holding
FTP; the originals may already be archived. On reboot `_dropoff_files` never drains it (F7), and even with
F7 fixed the dropoff rename fails forever. **Manual intervention.** Graceful is safe only because the
executor join lets the whole thread body finish — which is exactly what the 30 s grace (§2.3) protects.

Fix: reorder to download → merge → **upload merged** → commit → archive originals → delete locals. A stop
before the commit re-runs preprocessing from intact originals and overwrites the uploaded merged file
(SFTP `put` overwrites); a stop after it leaves the dropoff entry valid, with at worst un-archived originals
lingering in the pre-processing folder (logged, harmless: nothing re-matches them). Only `RYOProcessor` is
changed: Coremark's byte-identical copy stays untouched because Coremark is unwired and not deployed
(decision 2026-08-25); it gets the same reorder if and when it goes live.

### F4 — `_dropoff_files` post-`gather` (graceful-reachable, recovers with F2)

Renames post-processing→destination complete; cancellation before `check_box(invoice_applied)` / pop /
persist. With F2's guard the reboot rename reports "already moved", the box is ticked, the entry is popped.
A cancellation after the `check_box` but before persist is recovered by clean-stale (sheet says applied).

### F5 — `_register_pickup` / `_register_dropoff` (safe)

Each mutates the queues and persists inside one lock-held synchronous block with no `await` between
mutation and persist; a SIGKILL sees either the previous or the next backup (A2's atomic replace).

### F7 — `_dropoff_files` early return (known bug, now load-bearing)

`if not self._file_preprocess_queue: return` at the top means a non-empty dropoff queue with an empty
preprocess queue is never drained — precisely the state F3 leaves behind, and the state a graceful stop
during dropoff leaves behind whenever preprocessing had already finished. Phase 1 listed it as "left
as-is"; Phase 2 fixes it: return only when *both* queues are empty.

### Summary

| Finding | Reachable by | Today | After Phase 2 |
|---|---|---|---|
| F1 pickup archive before commit | SIGKILL | stranded | recovers |
| F2 SAS preprocess / all dropoff renames | `docker stop` | **stranded** | recovers |
| F3 RYO/Coremark commit before upload | SIGKILL | stranded | recovers |
| F4 dropoff bookkeeping | `docker stop` | stranded (via F2) | recovers |
| F5 register | — | safe | safe |
| F7 dropoff never drains | `docker stop` | stranded | recovers |

The Phase 1 claim "a doubled wave is idempotent" was true for the transfers and false for the queue
transitions around them. With F1–F3 and F7 fixed, every gap either re-runs from intact inputs or resumes
from a committed state; no gap requires a human.

## 4. Tests

Unit (`tests/unit`, network-free; the existing `processor` fixture pattern from `test_queue_persistence.py`
with pool/session fakes):

- `test_shutdown_hooks.py`: freeze pauses (and never shuts down) the scheduler; flush runs `flush_queued_writes`
  when no database-origin trail exists, skips when one does, swallows and logs a flush failure; hooks are
  registered with the expected phase/priority/required.
- `test_exit_code.py`: `exit_code_for_shutdown` mapping for every `ShutdownKind` and for "not requested".
- `test_transfer_idempotency.py`: rename raises + dest present + source absent → success; rename raises +
  dest present + source present → failure; rename raises + dest absent → failure.
- `test_job_ordering.py`: pickup persists the queue move before any vendor archive call; preprocess uploads
  the merged file before the queue commit and archives after; a non-empty dropoff queue with an empty
  preprocess queue is drained.

e2e (`tests/e2e`, docker compose) stays the acceptance gate and is run unchanged at the end; the two cycle
tests exercise every reordered path.

## 5. Out of scope

- Cancelling in-flight jobs in batch on FATAL; any wait-for-wave.
- `docker/Dockerfile` version bump (still gated on this phase landing).
- Coremark filename regex `{2}`; TODO #11; scheduler-level e2e.
