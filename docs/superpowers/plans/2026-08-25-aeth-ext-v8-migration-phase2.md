# aeth_ext v8.0.0 Migration — Phase 2 (Shutdown Lifecycle + Crash-Safe Queue Transitions) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `docker stop`, FATAL and SIGKILL all leave the processor in a state the next boot resumes from without a human: register aeth_ext v8 shutdown callbacks (freeze scheduler, flush sheet), fix the `await SHUTDOWN` regression and exit-code mapping, and reorder the three job paths whose queue transitions can currently strand an entry.

**Architecture:** Two THREADED `register_for_shutdown` callbacks in a new `shutdown_hooks.py` do the only work that must precede interpreter exit; `main()` returns after `await SHUTDOWN` and `run_app()` maps `SHUTDOWN.kind` to an exit code. In `suppliers/__init__.py`, every queue transition is committed (persisted) *after* the remote side effect it describes and *before* any cleanup that would destroy the inputs a re-run needs; the holding-FTP rename becomes idempotent so a re-run after a completed rename succeeds. No wait-for-wave.

**Tech Stack:** Python 3.14, aeth_ext 8.0.0 (`aeth_ext.errors.shutdown`), APScheduler `AsyncIOScheduler`, pytest + pytest-asyncio (unit suite is network-free; e2e via docker compose).

**Spec:** `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-phase2-shutdown-design.md` — read §2 (lifecycle) and §3 (findings F1–F7) before starting; every task below cites the finding it closes.

## Global Constraints

- Branch: `chore/update-to-aeth-ext-v8`, on top of Phase 1 (PR #10). Do not touch `docker/Dockerfile`.
- aeth_ext pinned at `>=8.0.0`; callbacks use `aeth_ext.errors.shutdown.register_for_shutdown(callback, *, phase, priority=0, required=False)`; the callback takes exactly one positional argument (a tuple of `ExceptionTrail`).
- THREADED callbacks may block and log. Never call `scheduler.shutdown()` from a callback (it cancels asyncio tasks from a foreign thread).
- Never overwrite a remote file by rename; the idempotency guard only accepts "destination present **and** source absent".
- Unit tests live in `tests/unit`, are network-free, and use the `processor` fixture pattern from `tests/unit/test_queue_persistence.py` (real processor, filesystem redirected to `tmp_path`, `DatabaseCache` stubbed with `SimpleNamespace`). Run them with `uv run pytest tests/unit -v`.
- Run `uv run ruff check src tests && uv run ruff format --check src tests` before every commit (2-space indentation, existing style).
- Commit after every task with the message given; do not squash.

---

### Task 1: Shutdown hooks module + sync sheet flush (closes spec §2.1 callbacks)

**Files:**
- Create: `src/scheduled_invoice_processor/shutdown_hooks.py`
- Modify: `src/scheduled_invoice_processor/database.py` (add `flush_queued_writes` next to `submit_queued_writes_to_pool`, ~line 317)
- Test: `tests/unit/test_shutdown_hooks.py`

**Interfaces:**
- Consumes: `aeth_ext.errors.shutdown.register_for_shutdown`, `ShutdownPhase`; `scheduled_invoice_processor.database.trail_is_database_origin` (Phase 1, A4); `DatabaseCache._api_write()` (sync, guarded by `aiologic.Lock`s that work from plain threads).
- Produces:
  - `DatabaseCache.flush_queued_writes(self) -> bool` — sync; calls `_api_write()` if anything is queued; returns whether it wrote.
  - `shutdown_hooks.freeze_scheduler(scheduler) -> Callable[[tuple[ExceptionTrail, ...]], None]`
  - `shutdown_hooks.final_sheets_flush(cache) -> Callable[[tuple[ExceptionTrail, ...]], None]`
  - `shutdown_hooks.register_shutdown_hooks(scheduler, cache) -> None`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_shutdown_hooks.py
"""Shutdown callbacks: freeze the scheduler, flush queued sheet writes, skip the flush on a database-origin fatal."""

# Standard library imports
import logging
from types import SimpleNamespace
from typing import Any

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.shutdown_hooks as hooks
from aeth_ext.errors.shutdown import ShutdownPhase


class _Scheduler:
  def __init__(self) -> None:
    self.calls: list[str] = []

  def pause(self) -> None:
    self.calls.append("pause")

  def shutdown(self, wait: bool = True) -> None:
    self.calls.append("shutdown")


class _Cache:
  def __init__(self, fail: bool = False) -> None:
    self.flushed = 0
    self.fail = fail

  def flush_queued_writes(self) -> bool:
    if self.fail:
      raise RuntimeError("sheets down")
    self.flushed += 1
    return True


def test_freeze_pauses_and_never_shuts_down() -> None:
  scheduler = _Scheduler()
  hooks.freeze_scheduler(scheduler)(())
  assert scheduler.calls == ["pause"]


def test_freeze_swallows_and_logs_pause_failure(caplog: pytest.LogCaptureFixture) -> None:
  scheduler = SimpleNamespace(pause=lambda: (_ for _ in ()).throw(RuntimeError("boom")))
  with caplog.at_level(logging.ERROR):
    hooks.freeze_scheduler(scheduler)(())
  assert "pause" in caplog.text.lower()


def test_flush_runs_when_no_database_origin_trail(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.setattr(hooks, "trail_is_database_origin", lambda trail: False)
  cache = _Cache()
  hooks.final_sheets_flush(cache)((object(),))  # type: ignore[arg-type]
  assert cache.flushed == 1


def test_flush_skipped_when_a_database_origin_trail_exists(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
  monkeypatch.setattr(hooks, "trail_is_database_origin", lambda trail: True)
  cache = _Cache()
  with caplog.at_level(logging.WARNING):
    hooks.final_sheets_flush(cache)((SimpleNamespace(origin=SimpleNamespace(module="m", file="f")),))  # type: ignore[arg-type]
  assert cache.flushed == 0
  assert "skipping final google sheets flush" in caplog.text.lower()


def test_flush_swallows_and_logs_failure(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
  monkeypatch.setattr(hooks, "trail_is_database_origin", lambda trail: False)
  with caplog.at_level(logging.ERROR):
    hooks.final_sheets_flush(_Cache(fail=True))(())
  assert "flush failed" in caplog.text.lower()


def test_register_shutdown_hooks_registers_freeze_then_required_flush(monkeypatch: pytest.MonkeyPatch) -> None:
  registered: list[dict[str, Any]] = []

  def fake_register(callback: Any, *, phase: ShutdownPhase, priority: int = 0, required: bool = False) -> None:
    registered.append({"callback": callback, "phase": phase, "priority": priority, "required": required})

  monkeypatch.setattr(hooks, "register_for_shutdown", fake_register)
  scheduler, cache = _Scheduler(), _Cache()
  hooks.register_shutdown_hooks(scheduler, cache)  # type: ignore[arg-type]

  assert [r["phase"] for r in registered] == [ShutdownPhase.THREADED, ShutdownPhase.THREADED]
  assert [r["priority"] for r in registered] == [-10, 0]
  assert [r["required"] for r in registered] == [False, True]
  registered[0]["callback"](())
  registered[1]["callback"](())
  assert scheduler.calls == ["pause"]
  assert cache.flushed == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_shutdown_hooks.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scheduled_invoice_processor.shutdown_hooks'`

- [ ] **Step 3: Add `DatabaseCache.flush_queued_writes`**

In `src/scheduled_invoice_processor/database.py`, directly above `async def submit_queued_writes_to_pool`:

```python
  def flush_queued_writes(self) -> bool:
    """Synchronously write every queued Sheets update. Safe from any thread: `_api_write` takes `aiologic` locks,
    which work from plain threads as well as coroutines. Used by the shutdown callback, which runs on aeth_ext's
    shutdown thread, not the event loop. Returns whether anything was written."""
    if not (
      self.queued_values_raw_updates
      or self.queued_values_user_entered_updates
      or self.queued_before_write_update_requests
      or self.queued_after_write_update_requests
    ):
      return False
    self._api_write()
    return True
```

- [ ] **Step 4: Create `shutdown_hooks.py`**

```python
# src/scheduled_invoice_processor/shutdown_hooks.py
"""aeth_ext v8 shutdown callbacks.

Both run on aeth_ext's shutdown thread (`ShutdownPhase.THREADED`) *before* it nudges the main thread to exit with
`interrupt_main()`, so they are the only application code guaranteed to run on every kind of stop. Anything after
`await SHUTDOWN` in `startup.main()` is best-effort: the nudge can pre-empt it.

Rules for this phase: may block and log; must not `scheduler.shutdown()` (that cancels asyncio tasks from a
foreign thread); `required=True` callbacks run even after the budget is exhausted.
"""

# Standard library imports
from collections.abc import Callable
from logging import getLogger
from typing import TYPE_CHECKING

# First party imports
from aeth_ext.errors.shutdown import ShutdownPhase, register_for_shutdown
from scheduled_invoice_processor.database import trail_is_database_origin

if TYPE_CHECKING:
  # First party imports
  from aeth_ext.errors.exception_trail import ExceptionTrail
  from scheduled_invoice_processor.database import DatabaseCache
  from scheduled_invoice_processor.scheduler_config import OrderProcessingScheduler

logger = getLogger(__name__)

type ShutdownCallback = Callable[[tuple["ExceptionTrail", ...]], None]

FREEZE_SCHEDULER_PRIORITY = -10
"""Runs first: no new job may start while the sheet is being flushed."""

FINAL_SHEETS_FLUSH_PRIORITY = 0


def freeze_scheduler(scheduler: "OrderProcessingScheduler") -> ShutdownCallback:
  """Stop new jobs from starting. `AsyncIOScheduler.pause()` is thread-safe (its wakeup goes through
  `call_soon_threadsafe`); `shutdown()` is not called here on purpose."""

  def _freeze(_trails: tuple["ExceptionTrail", ...]) -> None:
    try:
      scheduler.pause()
      logger.warning("Shutdown: scheduler paused; no new jobs will start")
    except Exception:
      logger.exception("Shutdown: failed to pause the scheduler")

  return _freeze


def final_sheets_flush(cache: "DatabaseCache") -> ShutdownCallback:
  """Write the in-memory Sheets update queue. Skipped when a fatal error originated inside the database interface
  (A4): the write would only fail again."""

  def _flush(trails: tuple["ExceptionTrail", ...]) -> None:
    database_origin_trail = next((trail for trail in trails if trail_is_database_origin(trail)), None)
    if database_origin_trail is not None:
      logger.warning(
        "Shutdown: skipping final Google Sheets flush because a fatal error originated in the database interface (origin=%s in %s)",
        database_origin_trail.origin.module,
        database_origin_trail.origin.file,
      )
      return
    try:
      if cache.flush_queued_writes():
        logger.warning("Shutdown: final Google Sheets flush completed")
      else:
        logger.info("Shutdown: no queued Google Sheets writes to flush")
    except Exception:
      logger.exception("Shutdown: final Google Sheets flush failed")

  return _flush


def register_shutdown_hooks(scheduler: "OrderProcessingScheduler", cache: "DatabaseCache") -> None:
  register_for_shutdown(freeze_scheduler(scheduler), phase=ShutdownPhase.THREADED, priority=FREEZE_SCHEDULER_PRIORITY)
  register_for_shutdown(final_sheets_flush(cache), phase=ShutdownPhase.THREADED, priority=FINAL_SHEETS_FLUSH_PRIORITY, required=True)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_shutdown_hooks.py -v`
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add src/scheduled_invoice_processor/shutdown_hooks.py src/scheduled_invoice_processor/database.py tests/unit/test_shutdown_hooks.py
git commit -m "feat(shutdown): THREADED callbacks freeze the scheduler and flush queued sheet writes; sync DatabaseCache.flush_queued_writes"
```

---

### Task 2: `main()` post-shutdown block, exit-code mapping, Docker grace (closes the Phase 1 regression; spec §2.1–2.3)

**Files:**
- Modify: `src/scheduled_invoice_processor/startup.py:272-390` (`main()`), plus a new function near the top-level helpers
- Modify: `src/scheduled_invoice_processor/__main__.py`
- Modify: `docker/compose.yaml`
- Test: `tests/unit/test_exit_code.py`

**Interfaces:**
- Consumes: `shutdown_hooks.register_shutdown_hooks` (Task 1); `aeth_ext.errors.shutdown.SHUTDOWN`, `ShutdownKind`.
- Produces: `startup.exit_code_for_shutdown(kind: ShutdownKind) -> int`. `SHUTDOWN.kind` is never `None`: it returns `ShutdownKind.RUNNING` (0) when nothing was requested (`aeth_ext/errors/shutdown.py` ~line 137).

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_exit_code.py
"""Process exit code follows the shutdown kind: 0 for running/graceful, 1 for fatal/forced."""

# Third party imports
import pytest

# First party imports
from aeth_ext.errors.shutdown import ShutdownKind
from scheduled_invoice_processor.startup import exit_code_for_shutdown


@pytest.mark.parametrize(
  ("kind", "expected"),
  [
    (ShutdownKind.RUNNING, 0),
    (ShutdownKind.GRACEFUL, 0),
    (ShutdownKind.FATAL, 1),
    (ShutdownKind.FORCED, 1),
  ],
)
def test_exit_code_for_shutdown(kind: ShutdownKind, expected: int) -> None:
  assert exit_code_for_shutdown(kind) == expected
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_exit_code.py -v`
Expected: FAIL with `ImportError: cannot import name 'exit_code_for_shutdown'`

- [ ] **Step 3: Rewrite the post-`await SHUTDOWN` block in `main()`**

In `src/scheduled_invoice_processor/startup.py`, change the imports:

```python
from aeth_ext.errors.shutdown import SHUTDOWN, ShutdownKind
```

(drop `get_current_fatal_trails`, `sys`, `sleep`, and `trail_is_database_origin` from the imports if nothing else in the module uses them — `sleep` is still used elsewhere only if grep says so). Change the signature `async def main() -> NoReturn:` to `async def main() -> None:` and delete the `NoReturn` import under `TYPE_CHECKING` if now unused.

Replace everything from `with RICH_CONSOLE.status("Application is running."):` (line 351) through `sys.exit(1)` (line 390) with:

```python
    register_shutdown_hooks(scheduler, cache)

    with RICH_CONSOLE.status("Application is running."):
      await SHUTDOWN

    # Everything below is best-effort: aeth_ext's threaded pass ends by raising KeyboardInterrupt on this
    # thread (`_attempt_early_exit`), which can land before any of it runs. The work that *must* happen
    # (pause the scheduler, flush the sheet) is in shutdown_hooks and has already run by then; queue backups
    # are persisted on every change and once more at atexit (A2).
    logger.warning("Shutdown requested (%s); stopping", SHUTDOWN.kind.name)
    periodic_heartbeat_task.cancel()
    with suppress(CancelledError):
      await periodic_heartbeat_task
    try:
      scheduler.shutdown(wait=False)
    except Exception:
      logger.exception("Shutdown: failed to stop the scheduler cleanly")
```

Add the import at the top of the module: `from scheduled_invoice_processor.shutdown_hooks import register_shutdown_hooks`.

Add this module-level function directly above `async def main()`:

```python
def exit_code_for_shutdown(kind: ShutdownKind) -> int:
  """0 for RUNNING (never requested) or GRACEFUL, 1 for FATAL or FORCED. `ShutdownKind` is an IntEnum ordered by
  severity. Kept out of `main()` so it is testable and so `main()` never calls `sys.exit` itself."""
  return 1 if kind >= ShutdownKind.FATAL else 0
```

The imports `sys`, `sleep`, `get_current_fatal_trails`, `trail_is_database_origin` and the `TYPE_CHECKING`-only `NoReturn` are used nowhere else in `startup.py` (verified 2026-08-25) — remove all five.

- [ ] **Step 4: Map the exit code in `run_app()`**

Replace the body of `run_app()` in `src/scheduled_invoice_processor/__main__.py`:

```python
def run_app() -> None:
  """Run the main application loop and exit with a code that reflects how it stopped."""
  initialize(asyncio=True, logging="socket")

  # Standard library imports
  import sys
  from asyncio import run

  # First party imports
  from aeth_ext.errors.shutdown import SHUTDOWN
  from scheduled_invoice_processor.startup import exit_code_for_shutdown, main

  try:
    run(main())
  except KeyboardInterrupt:
    # aeth_ext's threaded shutdown pass ends by simulating SIGINT on the main thread so the interpreter
    # unwinds normally (atexit still runs). Not an error; the kind below says how we stopped.
    pass
  sys.exit(exit_code_for_shutdown(SHUTDOWN.kind))
```

- [ ] **Step 5: Set the Docker stop grace**

In `docker/compose.yaml`, under `scheduled-invoice-processor:` after `restart: no`, add:

```yaml
    # Shutdown = aeth_ext callbacks (<=7 s) + join of in-flight FTP threads (~5 s per vendor transfer)
    # + atexit queue save. Docker's default 10 s can SIGKILL during the join and lose the final save.
    stop_grace_period: 30s
```

- [ ] **Step 6: Run the unit suite**

Run: `uv run pytest tests/unit -v`
Expected: all pass (previous 21 + Task 1's 6 + 4 here)

- [ ] **Step 7: Boot smoke check**

Run: `uv run python -c "from scheduled_invoice_processor.startup import main, exit_code_for_shutdown; print('ok')"`
Expected: `ok` (import-time errors from the rewrite surface here rather than in the container).

- [ ] **Step 8: Commit**

```bash
git add src/scheduled_invoice_processor/startup.py src/scheduled_invoice_processor/__main__.py docker/compose.yaml tests/unit/test_exit_code.py
git commit -m "fix(shutdown): branch on SHUTDOWN.kind; drop sleep(600)/.errored heuristic; exit code mapped in run_app; 30 s stop grace"
```

---

### Task 3: Idempotent holding-FTP rename (closes F2, F4)

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/__init__.py:625-677` (`_transfer_file_main_to_main`)
- Test: `tests/unit/test_transfer_idempotency.py`

**Interfaces:**
- Consumes: `AdapterBase.rename`, `AdapterBase.get_size` (raise `(*ftplib.all_errors, OSError)` on a missing path).
- Produces: `_transfer_file_main_to_main` sets `getattr(file_meta, success_attr)[idx] = True` when the destination already holds the file and the source is gone.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_transfer_idempotency.py
"""A holding-FTP rename that already happened (e.g. before a stop mid-wave) is reported as success on re-run."""

# Standard library imports
import atexit
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sas import SASProcessor

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Iterator


class _FakeClient:
  """`rename` always fails; `get_size` answers from `sizes` and raises OSError for anything else."""

  def __init__(self, sizes: dict[str, int]) -> None:
    self.sizes = sizes
    self.renames: list[tuple[str, str]] = []

  def rename(self, old: str, new: str) -> None:
    self.renames.append((old, new))
    raise OSError("rename failed")

  def get_size(self, path: str) -> int:
    if path not in self.sizes:
      raise OSError(f"no such file {path}")
    return self.sizes[path]


class _FakePool:
  def __init__(self, client: _FakeClient) -> None:
    self.client = client

  @contextmanager
  def start_session(self) -> "Iterator[_FakeClient]":
    yield self.client

  def test_connection(self, logit: bool = False) -> bool:
    return True


class _FakePbar:
  def update(self, *args: object, **kwargs: object) -> None:
    pass


def _drop_singleton() -> None:
  if "__shared_instance__" in SASProcessor.__dict__:
    delattr(SASProcessor, "__shared_instance__")


@pytest.fixture
def processor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> "Iterator[SASProcessor]":
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", SimpleNamespace)
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(SASProcessor, "_file_queue_backup_folder", tmp_path / "queue_backups")
  monkeypatch.setattr(SASProcessor, "_corrupted_queue_backup_folder", tmp_path / "queue_backups" / "corrupted")
  monkeypatch.setattr(SASProcessor, "log_file_loc", tmp_path / "logs")
  _drop_singleton()
  proc = SASProcessor()
  proc.pbar = _FakePbar()  # type: ignore[assignment]
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton()


def _meta() -> FileRegisterData:
  now = datetime.now(SETTINGS.tz)
  return FileRegisterData(
    storenum=9001,
    customer_id="900100",
    pickup_date=now,
    dropoff_date=now + timedelta(days=1),
    file_pattern=re.compile(r".*"),
    _current_week=True,
    _waiting_folder=PurePosixPath("/Waiting/SAS"),
    _local_copy_folder=Path("unit-test-local"),
    file_names={0: "inv.txt"},
  )


SEND = PurePosixPath("/Waiting/SAS/inv.txt")
RECV = PurePosixPath("/Waiting/SAS/Processed/inv.txt")


def _run(processor: SASProcessor, sizes: dict[str, int]) -> tuple[FileRegisterData, _FakeClient]:
  client = _FakeClient(sizes)
  processor.__class__.waiting_ftp = _FakePool(client)  # type: ignore[assignment]
  meta = _meta()
  processor._transfer_file_main_to_main(
    send_path=SEND, recv_path=RECV, move_files_task=0, file_meta=meta, idx=0, key="k", success_attr="preprocess_success"
  )
  return meta, client


def test_already_moved_counts_as_success(processor: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, client = _run(processor, {RECV.as_posix(): 128})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: True}
  assert client.renames == [(SEND.as_posix(), RECV.as_posix())]


def test_destination_present_but_source_still_there_is_a_failure(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _ = _run(processor, {RECV.as_posix(): 128, SEND.as_posix(): 128})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: False}


def test_destination_absent_is_a_failure(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _ = _run(processor, {})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: False}


def test_empty_destination_is_a_failure(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _ = _run(processor, {RECV.as_posix(): 0})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: False}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_transfer_idempotency.py -v`
Expected: `test_already_moved_counts_as_success` FAILS (`preprocess_success == {}` — today the exception path never sets the flag); the three failure cases also fail for the same reason (`{}` != `{0: False}`).

- [ ] **Step 3: Implement the guard**

Replace `_transfer_file_main_to_main` in `src/scheduled_invoice_processor/suppliers/__init__.py` with:

```python
  def _transfer_file_main_to_main(  # noqa: PLR0917
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: TaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    success_attr: str,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    """Move a file within the holding FTP. Idempotent: if the rename fails because it already happened (a stop
    mid-wave lets the rename thread finish but never runs the bookkeeping that records it), the move is reported
    as a success so the re-run can advance the queue instead of stranding the entry."""
    local_logger = adapted_logger or logger
    result = StatusCode.UNKNOWN
    if self.errored:
      local_logger.warning("%s: Disabled due to error state. Skipping transfer of files within main FTP", self.__class__.__name__)
      return False
    success = False
    try:
      with self.waiting_ftp.start_session() as client:
        try:
          client.rename(send_path.as_posix(), recv_path.as_posix())
        except (*all_errors, OSError) as rename_error:
          if self._already_moved(client, send_path, recv_path):
            local_logger.info(
              "%s: [yellow]%s[/] was already moved to [yellow]%s[/] by an earlier run; treating as success",
              self.__class__.__name__,
              send_path,
              recv_path,
              extra={"markup": True},
            )
            success = True
          else:
            raise rename_error
        else:
          # Verify file was moved successfully
          try:
            client.get_size(recv_path.as_posix())
            success = True
            local_logger.info(
              "%s: Moved [yellow]%s[/] to [yellow]%s[/]",
              self.__class__.__name__,
              send_path,
              recv_path,
              extra={"markup": True},
            )
          except (*all_errors, OSError) as e:
            local_logger.warning("%s: Failed to verify move of %s", self.__class__.__name__, send_path.name, exc_info=e)
      result = StatusCode.SUCCESS if success else StatusCode.FAILURE
      getattr(file_meta, success_attr)[idx] = success
      self.pbar.update(move_files_task, advance=1, refresh=True)
      if log_action_handler is not None:
        log_action_handler(key, result, file_meta)
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception:
      local_logger.exception(
        "%s: Error moving\n[yellow]%s[/] to\n[yellow]%s[/]",
        self.__class__.__name__,
        send_path,
        recv_path,
        extra={"markup": True},
      )
      getattr(file_meta, success_attr)[idx] = False
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)
    return success

  @staticmethod
  def _already_moved(client: AdapterBase, send_path: PurePosixPath, recv_path: PurePosixPath) -> bool:
    """True only when the destination holds a non-empty file *and* the source is gone. A destination beside a
    still-present source is a genuine conflict and is never treated as done (nothing here ever overwrites)."""
    try:
      recv_size = client.get_size(recv_path.as_posix())
    except (*all_errors, OSError):
      return False
    if not recv_size:
      return False
    try:
      client.get_size(send_path.as_posix())
    except (*all_errors, OSError):
      return True
    return False
```

`AdapterBase` is already imported under `TYPE_CHECKING`; since `_already_moved` only uses it in an annotation, that is enough (the module has `from __future__ import annotations` semantics via string annotations already used elsewhere — if the linter complains, quote the annotation).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_transfer_idempotency.py tests/unit/test_queue_persistence.py -v`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git add src/scheduled_invoice_processor/suppliers/__init__.py tests/unit/test_transfer_idempotency.py
git commit -m "fix(ftp): treat an already-completed holding-FTP rename as success so a re-run after a stop mid-wave advances the queue"
```

---

### Task 4: `_dropoff_files` drains a non-empty dropoff queue (closes F7)

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/__init__.py:447-460` (top of `_dropoff_files`)
- Test: `tests/unit/test_job_ordering.py` (created here; Tasks 5 and 6 add to it)

**Interfaces:**
- Consumes: the `processor`/`_FakePool`/`_FakePbar` fixtures — copy them from `tests/unit/test_transfer_idempotency.py` into this file's top (the plan repeats them below so the file is self-contained).
- Produces: `_dropoff_files` returns early only when both `_file_preprocess_queue` and `_file_dropoff_queue` are empty.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_job_ordering.py
"""Queue transitions are committed after the remote side effect they describe and before any cleanup a re-run
would need (spec §3, findings F1/F3/F7)."""

# Standard library imports
import atexit
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sas import SASProcessor

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Iterator


class _FakeClient:
  def __init__(self) -> None:
    self.listing: list[Any] = []
    self.uploads: list[str] = []

  def listdir(self, path: str) -> list[Any]:
    return self.listing

  def upload_file(self, remote_path: str, callback: Any, file_size: int, task_msg: str = "") -> None:
    self.uploads.append(remote_path)

  def get_size(self, path: str) -> int:
    return 1

  def rename(self, old: str, new: str) -> None:
    pass


class _FakePool:
  def __init__(self, client: _FakeClient) -> None:
    self.client = client

  @contextmanager
  def start_session(self) -> "Iterator[_FakeClient]":
    yield self.client

  def test_connection(self, logit: bool = False) -> bool:
    return True


class _FakePbar:
  @contextmanager
  def add_task(self, *args: object, **kwargs: object) -> "Iterator[int]":
    yield 0

  def update(self, *args: object, **kwargs: object) -> None:
    pass


def _drop_singleton(cls: type) -> None:
  if "__shared_instance__" in cls.__dict__:
    delattr(cls, "__shared_instance__")


@pytest.fixture
def sas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> "Iterator[SASProcessor]":
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", SimpleNamespace)
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(SASProcessor, "_file_queue_backup_folder", tmp_path / "queue_backups")
  monkeypatch.setattr(SASProcessor, "_corrupted_queue_backup_folder", tmp_path / "queue_backups" / "corrupted")
  monkeypatch.setattr(SASProcessor, "log_file_loc", tmp_path / "logs")
  client = _FakeClient()
  monkeypatch.setattr(SASProcessor, "waiting_ftp", _FakePool(client))
  monkeypatch.setattr(SASProcessor, "vendor_ftp", _FakePool(client))
  _drop_singleton(SASProcessor)
  proc = SASProcessor()
  proc.pbar = _FakePbar()  # type: ignore[assignment]
  proc.cache = SimpleNamespace(  # type: ignore[assignment]
    schedule=SimpleNamespace(check_box=AsyncMock()),
    prev_week_schedule=SimpleNamespace(check_box=AsyncMock()),
  )
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton(SASProcessor)


def _meta(waiting_folder: str, file_names: dict[int, str] | None = None) -> FileRegisterData:
  now = datetime.now(SETTINGS.tz)
  return FileRegisterData(
    storenum=9001,
    customer_id="900100",
    pickup_date=now,
    dropoff_date=now + timedelta(days=1),
    file_pattern=re.compile(r"^inv.*\.txt$"),
    _current_week=True,
    _waiting_folder=PurePosixPath(waiting_folder),
    _local_copy_folder=Path("unit-test-local"),
    file_names=file_names or {0: "inv.txt"},
  )


# ---- F7: a dropoff queue left behind by a stop is drained even when nothing is waiting to be preprocessed ----


async def test_dropoff_drains_when_only_dropoff_queue_is_populated(sas: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  meta = _meta("/Waiting/SAS/Processed")
  sas._file_dropoff_queue["k"] = meta

  async def no_preprocess(*args: object, **kwargs: object) -> None:
    pass

  def fake_move(*, file_meta: FileRegisterData, idx: int, success_attr: str, **kwargs: object) -> bool:
    getattr(file_meta, success_attr)[idx] = True
    return True

  monkeypatch.setattr(sas, "_preprocess_files", no_preprocess)
  monkeypatch.setattr(sas, "_transfer_file_main_to_main", fake_move)

  await sas._dropoff_files()

  assert "k" not in sas._file_dropoff_queue
  sas.cache.schedule.check_box.assert_awaited_once()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_job_ordering.py -v`
Expected: FAIL — `assert "k" not in sas._file_dropoff_queue` (the early return skipped the drain; `check_box` not awaited).

- [ ] **Step 3: Fix the early return**

In `_dropoff_files` (`suppliers/__init__.py`, ~line 455) replace

```python
    if not self._file_preprocess_queue:
      return
```

with

```python
    if not self._file_preprocess_queue and not self._file_dropoff_queue:
      return
```

and, after `await self._preprocess_files()` and the `errored` check, replace the "No files to drop off after preprocessing step" `error` branch with:

```python
    if not self._file_dropoff_queue:
      local_logger.warning(
        "%s: No files to drop off after preprocessing step (preprocessing did not advance any entry this run)",
        self.__class__.__name__,
      )
      return
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_job_ordering.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add src/scheduled_invoice_processor/suppliers/__init__.py tests/unit/test_job_ordering.py
git commit -m "fix(dropoff): drain a populated dropoff queue even when the preprocess queue is empty"
```

---

### Task 5: Pickup commits the queue before archiving vendor files (closes F1, F6)

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/__init__.py:1058-1161` (`_pickup_files`, from the matching loop to the end)
- Test: `tests/unit/test_job_ordering.py` (append)

**Interfaces:**
- Consumes: Task 4's fixtures.
- Produces: `_pickup_files` order = copies → `check_box` → queue move → `_persist_queues()` → vendor archive; `pickup_success` is reset whenever `file_names` is re-matched.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_job_ordering.py`:

```python
# ---- F1: the pickup→waiting move is persisted before any vendor archive rename ----


async def test_pickup_persists_queue_move_before_vendor_archive(sas: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  events: list[str] = []
  meta = _meta("/Waiting/SAS")
  meta.pickup_success = {0: True, 1: True}  # stale flags from an earlier partial run (F6)
  sas._file_pickup_queue["k"] = meta
  sas.waiting_ftp.client.listing = [SimpleNamespace(filename="inv.txt", modified_time=datetime.now(SETTINGS.tz))]
  monkeypatch.setattr(SASProcessor, "pickup_archive_ftp_folder", PurePosixPath("/RYO/Archive"), raising=False)

  def fake_copy(*, file_meta: FileRegisterData, idx: int, success_attr: str, **kwargs: object) -> bool:
    events.append("copy")
    getattr(file_meta, success_attr)[idx] = True
    return True

  def fake_archive(**kwargs: object) -> None:
    events.append("archive")
    assert "k" in sas._file_waiting_queue, "archive ran before the queue move was committed"

  real_persist = sas._persist_queues

  def recording_persist() -> None:
    events.append("persist")
    real_persist()

  monkeypatch.setattr(sas, "_transfer_file_vend_to_main", fake_copy)
  monkeypatch.setattr(sas, "_vendor_archive_file", fake_archive)
  monkeypatch.setattr(sas, "_persist_queues", recording_persist)

  await sas._pickup_files()

  assert events == ["copy", "persist", "archive"]
  assert "k" in sas._file_waiting_queue and "k" not in sas._file_pickup_queue
  assert meta.pickup_success == {0: True}, "stale success flags from a previous match must be cleared on re-match"
  sas.cache.schedule.check_box.assert_awaited_once()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_job_ordering.py::test_pickup_persists_queue_move_before_vendor_archive -v`
Expected: FAIL — either the assertion inside `fake_archive` (archive ran first) or `events == ['copy', 'archive', 'persist']`.

- [ ] **Step 3: Reorder `_pickup_files`**

In the matching loop, where `file_meta.file_names = {idx: m.string for idx, m in enumerate(matched_files)}` is set, add directly after it:

```python
          file_meta.pickup_success = {}
          file_meta.invoice_nums = {}
```

Replace everything after `await gather(*dl_futures)` (from `archive_futures = []` to the final `self._persist_queues()`) with:

```python
      # Commit first, clean up last. Once the copies have landed, the durable facts are the sheet tick and the
      # queue move; the vendor-side archive only removes inputs a re-run would need. A stop between the commit and
      # the archive leaves an already-copied file in the vendor folder, which `_vendor_archive_file` reconciles
      # on the next run; a stop before the commit re-runs the copies from intact inputs.
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if all(file_meta.pickup_success.values()):
          items_to_advance[key] = file_meta
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule
          local_logger.info(
            "%s: %s: Checking off %s_%s invoice_grabbed",
            self.__class__.__name__,
            key,
            self.supplier_name,
            file_meta.storenum,
          )
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

      for key, item in items_to_advance.items():
        self._file_waiting_queue[key] = item
        self._file_pickup_queue.pop(key)
        local_logger.info("%s: %s: Moved %s to waiting queue", self.__class__.__name__, key, item.storenum)

      self._persist_queues()

      if self.pickup_archive_ftp_folder is not None:
        archive_futures = [
          to_thread(
            self._vendor_archive_file,
            source_folder=self.pickup_ftp_folder,
            remote_file=filename,
            archive_folder=self.pickup_archive_ftp_folder,
            adapted_logger=adapted_logger,
            debug=__debug__,
          )
          for file_meta in items_to_advance.values()
          for filename in file_meta.file_names.values()
        ]
        await gather(*archive_futures)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_job_ordering.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/scheduled_invoice_processor/suppliers/__init__.py tests/unit/test_job_ordering.py
git commit -m "fix(pickup): persist the pickup->waiting move before archiving vendor files; reset success flags on re-match"
```

---

### Task 6: Preprocess uploads the merged file before committing the queue (closes F3)

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/ryo.py:209-280` (`RYOProcessor._preprocess_off_thread`, rewritten in place)
- Do NOT touch: `src/scheduled_invoice_processor/suppliers/coremark.py` — Coremark is unwired and not deployed (decision 2026-08-25); its identical-but-old `_preprocess_off_thread` stays as-is and gets the same treatment if/when it goes live.
- Test: `tests/unit/test_job_ordering.py` (append)

**Interfaces:**
- Consumes: `RYOProcessor._create_new_merged_file(key, old_file_meta, adapted_logger) -> FileRegisterData` (untouched); `self._middle_archive_file`; `self._persist_lock`.
- Produces: `RYOProcessor._preprocess_off_thread(self, key, old_file_meta, adapted_logger=None) -> tuple[SupplierQueueKey, FileRegisterData]` — same signature as today, new order: merge → upload → commit → archive originals → delete locals.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_job_ordering.py`:

```python
# ---- F3: the merged file is on the holding FTP before the dropoff queue says it is ----

# First party imports
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor  # noqa: E402


@pytest.fixture
def ryo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> "Iterator[RYOProcessor]":
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", SimpleNamespace)
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(RYOProcessor, "_file_queue_backup_folder", tmp_path / "queue_backups")
  monkeypatch.setattr(RYOProcessor, "_corrupted_queue_backup_folder", tmp_path / "queue_backups" / "corrupted")
  monkeypatch.setattr(RYOProcessor, "log_file_loc", tmp_path / "logs")
  client = _FakeClient()
  monkeypatch.setattr(RYOProcessor, "waiting_ftp", _FakePool(client))
  monkeypatch.setattr(RYOProcessor, "vendor_ftp", _FakePool(client))
  _drop_singleton(RYOProcessor)
  proc = RYOProcessor()
  proc.pbar = _FakePbar()  # type: ignore[assignment]
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton(RYOProcessor)


def test_preprocess_uploads_before_commit_and_archives_after(ryo: RYOProcessor, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
  events: list[str] = []
  old = _meta("/Waiting/RYO", {0: "inv-a.txt", 1: "inv-b.txt"})
  ryo._file_preprocess_queue["k"] = old

  merged_dir = tmp_path / "merged"
  merged_dir.mkdir()
  (merged_dir / "merged.txt").write_text("merged")
  new = FileRegisterData(
    storenum=old.storenum,
    customer_id=old.customer_id,
    pickup_date=old.pickup_date,
    dropoff_date=old.dropoff_date,
    file_pattern=old.file_pattern,
    _current_week=True,
    _waiting_folder=ryo.post_processing_waiting_folder,
    _local_copy_folder=merged_dir,
    file_names={0: "merged.txt"},
  )

  monkeypatch.setattr(ryo, "_create_new_merged_file", lambda key, old_file_meta, adapted_logger=None: new)

  def fake_archive(**kwargs: object) -> None:
    events.append("archive")
    assert "k" in ryo._file_dropoff_queue, "originals archived before the queue commit"

  real_upload = ryo.waiting_ftp.client.upload_file

  def recording_upload(remote_path: str, callback: Any, file_size: int, task_msg: str = "") -> None:
    events.append("upload")
    assert "k" not in ryo._file_dropoff_queue, "queue committed before the merged file was uploaded"
    real_upload(remote_path, callback, file_size, task_msg)

  real_persist = ryo._persist_queues

  def recording_persist() -> None:
    events.append("persist")
    real_persist()

  monkeypatch.setattr(ryo, "_middle_archive_file", fake_archive)
  monkeypatch.setattr(ryo.waiting_ftp.client, "upload_file", recording_upload)
  monkeypatch.setattr(ryo, "_persist_queues", recording_persist)

  key, result = ryo._preprocess_off_thread("k", old)

  assert (key, result) == ("k", new)
  assert events == ["upload", "persist", "archive", "archive"]
  assert ryo.waiting_ftp.client.uploads == [(ryo.post_processing_waiting_folder / "merged.txt").as_posix()]
  assert "k" in ryo._file_dropoff_queue and "k" not in ryo._file_preprocess_queue
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_job_ordering.py::test_preprocess_uploads_before_commit_and_archives_after -v`
Expected: FAIL — the assertion inside `recording_upload` (today the queue is committed first), or `events == ['persist', 'archive', 'archive', 'upload']`.

- [ ] **Step 3: Rewrite `RYOProcessor._preprocess_off_thread` in place**

In `src/scheduled_invoice_processor/suppliers/ryo.py`, replace the whole method (lines 209–280) with:

```python
  def _preprocess_off_thread(
    self,
    key: SupplierQueueKey,
    old_file_meta: FileRegisterData,
    adapted_logger: LoggerAdapter[Any] | None = None,
  ) -> tuple[SupplierQueueKey, FileRegisterData]:
    """Merge one entry's files and move it from the preprocess queue to the dropoff queue.

    Ordering is the whole point: **upload → commit → cleanup**. The merged file must exist on the holding FTP
    before the dropoff queue claims it does, and the originals must survive until the commit so a stop before it
    re-runs from intact inputs (the re-upload overwrites). A stop after the commit at worst leaves un-archived
    originals in the pre-processing folder, which nothing re-matches.
    """
    try:
      local_logger = adapted_logger or logger

      new_file_meta = self._create_new_merged_file(key, old_file_meta, adapted_logger)
      local_logger.info(
        "%s: %s: Created merged file at location [yellow]%s[/]",
        self.__class__.__name__,
        key,
        new_file_meta.local_copy_loc[0].without_cwd(),
        extra={"markup": True},
      )

      # TODO Upload the original invoice files to a shared store specific google drive

      # 1. Upload the merged file to the post-processing waiting folder.
      for new_file_loc in new_file_meta.local_copy_loc.values():
        send_path = self.post_processing_waiting_folder / new_file_loc.name
        with new_file_loc.open("rb") as f, self.waiting_ftp.start_session() as waiting_client:
          waiting_client.upload_file(
            send_path.as_posix(), callback=f.read, file_size=new_file_loc.stat().st_size, task_msg=f"Uploading {send_path.name}"
          )
        local_logger.info("%s: %s: Uploaded merged file to remote location %s", self.__class__.__name__, key, send_path)

      # 2. Commit: the dropoff queue now describes a file that really exists.
      with self._persist_lock:
        self._file_dropoff_queue[key] = new_file_meta
        old_file_meta = self._file_preprocess_queue.pop(key)
        self._persist_queues()
      local_logger.info("%s: %s: Updated queues", self.__class__.__name__, key)

      # 3. Cleanup: archive the originals on the holding FTP, delete local copies.
      for remote_file_loc in old_file_meta.remote_file_locs.values():
        self._middle_archive_file(
          source_folder=self.pre_processing_waiting_folder,
          remote_file=remote_file_loc.name,
          archive_folder=self.pre_processing_archive_folder,
          adapted_logger=adapted_logger,
        )

      for local_file_loc in (*old_file_meta.local_copy_loc.values(), *new_file_meta.local_copy_loc.values()):
        try:
          local_file_loc.unlink()
          local_logger.info("%s: %s: Deleted local file %s", self.__class__.__name__, key, local_file_loc.without_cwd())
        except Exception:
          local_logger.exception("%s: %s: Failed to delete local file %s", self.__class__.__name__, key, local_file_loc.without_cwd())

      return key, new_file_meta
    except Exception:
      logger.exception("%s: %s: Unexpected error in preprocessing off thread", self.__class__.__name__, key)
      raise
```

The method body is otherwise the existing code with the blocks reordered — diff it against the old version to confirm only ordering changed (plus the merged local file joining the cleanup loop). `coremark.py` is not modified.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit -v`
Expected: all pass (3 in `test_job_ordering.py`)

- [ ] **Step 5: Commit**

```bash
git add src/scheduled_invoice_processor/suppliers/ryo.py tests/unit/test_job_ordering.py
git commit -m "fix(preprocess): RYO uploads the merged file before committing the dropoff queue and archives originals after"
```

---

### Task 7: Full verification, adversarial review, and docs

**Review bar (Jacob, 2026-08-25):** this PR carries the shutdown rewrite *and* the hot-path reorders together for
schedule reasons, so the review must be adversarial, not confirmatory. Before Step 1, run a dedicated review pass
over the whole Phase 2 diff (`git diff fd34f9e..HEAD -- src`) with the `superpowers:requesting-code-review` skill,
and separately walk each of these by hand, writing the answer into the PR description:

1. For every `await` in `_pickup_files`, `_preprocess_files`, `_dropoff_files` and `_preprocess_off_thread`: if
   the task is cancelled *here*, what is on disk (queue backups), on the holding FTP, on the vendor FTP, and in the
   sheet cache — and does the next boot resume from it without a human? Tabulate; any "no" is a blocker.
2. Confirm the nudge ordering empirically (Step 3 below): the log must show `scheduler paused` and the flush line
   *before* the interpreter unwinds, on both a SIGTERM and a fatal.
3. Confirm `_already_moved` can never return True while the source exists (read `AdapterBase.get_size` for both
   the FTP and SFTP adapters in `aeth_ext/ftp/session.py` and note what each raises for a missing path; if either
   returns `None` instead of raising, the guard must treat `None` as "absent" and the test must cover it).
4. Confirm no caller depended on `main()` being `NoReturn` or on `sys.exit` inside it (`grep -rn "main()" src tests`).

**Files:**
- Modify: `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md` (the "Phase 2 — deferred" section and the "Phase 2 decision input" bullet under "Drag race")
- Modify: `.claude/plans/2026-08-13-shutdown-and-ftp-master-plan.md` ("Phase 1 outcome and Phase 2 inputs")
- Modify: `tests/e2e/README.md` only if the e2e run below needs a note

- [ ] **Step 1: Lint and unit suite**

Run: `uv run ruff check src tests && uv run ruff format --check src tests && uv run pytest tests/unit -v`
Expected: clean; all unit tests pass.

- [ ] **Step 2: e2e suite (docker compose; acceptance gate)**

Follow `tests/e2e/README.md` to bring up the compose fixtures, then: `uv run pytest tests/e2e -v`
Expected: 10 passed. The RYO and SAS cycle tests exercise every reordered path (pickup commit-before-archive, preprocess upload-before-commit, dropoff drain). If `test_sheet.py::test_seed_read_delete_roundtrip` fails with a Sheets 429 write-quota error, re-run it once — that failure was seen on CI on a docs-only commit and is quota, not code.

- [ ] **Step 3: Manual graceful-stop check (no real credentials needed beyond the e2e fixtures)**

With the e2e compose fixtures up, start the app with `PYTHONOPTIMIZE=1` so aeth_ext's signal handlers are live (`uv run python -O -m scheduled_invoice_processor` with the e2e env), wait for "Boot Done", send SIGTERM (or Ctrl-C once), and confirm in the log: `Shutdown: scheduler paused`, then either `final Google Sheets flush completed` or `no queued Google Sheets writes to flush`, then the process exits with code 0 (`echo $?` / `$LASTEXITCODE`). Then trigger a fatal by any means the e2e harness offers (or a deliberate `raise` behind a debug flag) and confirm exit code 1.

- [ ] **Step 4: Update the two Phase 1 docs**

In `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md`, replace the body of "## Phase 2 — deferred: A3 shutdown lifecycle" with one paragraph:

```markdown
Built in Phase 2 — see `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-phase2-shutdown-design.md`
(decision: THREADED callbacks + crash-safe queue transitions, no wait-for-wave; the "bounded by Docker's stop
grace" premise was wrong because aeth_ext's threaded pass ends by raising `KeyboardInterrupt` on the main
thread) and `docs/superpowers/plans/2026-08-25-aeth-ext-v8-migration-phase2.md`. The `await SHUTDOWN` regression
noted below is closed there.
```

and in the "Drag race" section replace the "Phase 2 decision input" bullet with:

```markdown
- Phase 2 decision input: the wave numbers above were measured against the wrong constraint (see the Phase 2
  spec §1). Decision taken 2026-08-25: no wait-for-wave; callbacks plus the F1–F7 ordering fixes.
```

In `.claude/plans/2026-08-13-shutdown-and-ftp-master-plan.md`, add directly under the "### The decision to make (A3)" heading:

```markdown
**Resolved 2026-08-25 — see `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-phase2-shutdown-design.md`.**
Both shapes below measured the wave against the wrong budget: aeth_ext's threaded pass ends with
`_attempt_early_exit()` → `interrupt_main()` → `KeyboardInterrupt` on the main thread, so post-`await SHUTDOWN`
code in `main()` is bounded by the callback pass, not Docker. Shape 1 (callbacks) was chosen; shape 2 is
unbuildable as written. The "doubled wave is idempotent" claim was audited and found false for three queue
transitions (F1–F3) and one drain bug (F7); all fixed in Phase 2.
```

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md .claude/plans/2026-08-13-shutdown-and-ftp-master-plan.md
git commit -m "docs: record the Phase 2 A3 decision and the idempotency audit outcome in the Phase 1 docs"
```

- [ ] **Step 6: Push and update PR #10**

`git push` and edit PR #10's description to list Phase 2's commits under a "Phase 2" heading (`gh pr edit 10 --body-file -`). Do not merge — Jacob reviews.
