# Standard library imports
from asyncio import CancelledError, Event, Future, Task, create_task, get_running_loop, sleep
from contextlib import nullcontext, suppress
from typing import TYPE_CHECKING, Any

# First party imports
from aeth_ext.errors.shutdown import ShutdownKind
from scheduled_invoice_processor import startup

if TYPE_CHECKING:
  # Third party imports
  import pytest


class _Shutdown:
  def __init__(self, future: Future[None]) -> None:
    self._future = future
    self.kind = ShutdownKind.GRACEFUL

  def __await__(self):
    return self._future.__await__()


class _Scheduler:
  def __init__(self, in_flight: set[Future[Any]]) -> None:
    self.calls: list[str] = []
    self._in_flight = in_flight

  def add_job(self, *_args: Any, **_kwargs: Any) -> None:
    pass

  def start(self) -> None:
    self.calls.append("start")

  def print_jobs(self) -> None:
    pass

  def in_flight_jobs(self) -> set[Future[Any]]:
    return set(self._in_flight)

  def shutdown(self, wait: bool = True) -> None:
    self.calls.append("shutdown")
    assert wait is False
    for fut in self._in_flight:
      fut.cancel()


class _Cache:
  def __init__(self, events: list[str]) -> None:
    self._events = events

  async def refresh_cache(self) -> None:
    pass

  async def submit_queued_writes_to_pool(self) -> None:
    self._events.append("flush")


async def _commit_block(gate: Event, events: list[str]) -> None:
  try:
    await sleep(3600)
  except CancelledError:
    events.append("job_cancelled")
    await gate.wait()
    events.append("job_done")


async def test_catch_up_flush_waits_for_in_flight_jobs_and_shutdown_complete(monkeypatch: pytest.MonkeyPatch) -> None:
  loop = get_running_loop()
  events: list[str] = []
  shutdown_requested: Future[None] = loop.create_future()
  shutdown_complete: Future[None] = loop.create_future()
  job_gate = Event()
  job = create_task(_commit_block(job_gate, events))
  scheduler = _Scheduler({job})
  cache = _Cache(events)

  async def _bootstrap(_pbar: Any) -> _Cache:
    return cache

  async def _noop(*_args: Any, **_kwargs: Any) -> None:
    pass

  async def _heartbeat(*_args: Any, **_kwargs: Any) -> None:
    await sleep(3600)

  monkeypatch.setattr(startup, "SHUTDOWN", _Shutdown(shutdown_requested))
  monkeypatch.setattr(startup, "SHUTDOWN_COMPLETE", shutdown_complete)
  monkeypatch.setattr(startup, "scheduler", scheduler)
  monkeypatch.setattr(startup, "bootstrap_runtime", _bootstrap)
  monkeypatch.setattr(startup, "register_shutdown_hooks", lambda *_a, **_k: None)
  monkeypatch.setattr(startup, "send_heartbeat", lambda *_a, **_k: None)
  monkeypatch.setattr(startup, "run_heartbeat_async", _heartbeat)
  monkeypatch.setattr(startup, "_run_debug_code", _noop)
  monkeypatch.setattr(startup, "get_current_fatal_trails", tuple)
  monkeypatch.setattr(startup, "Progress", lambda *_a, **_k: nullcontext())
  monkeypatch.setattr(startup.RICH_CONSOLE, "status", lambda *_a, **_k: nullcontext())

  main_task = create_task(startup.main())
  try:
    await _drive(
      main_task=main_task,
      scheduler=scheduler,
      events=events,
      shutdown_requested=shutdown_requested,
      shutdown_complete=shutdown_complete,
      job_gate=job_gate,
    )
  finally:
    # Never leave main() (or the stub heartbeat) running into the session loop's teardown.
    for task in (main_task, job):
      task.cancel()
    for task in (main_task, job):
      with suppress(CancelledError):
        await task


async def _drive(
  *,
  main_task: Task[None],
  scheduler: _Scheduler,
  events: list[str],
  shutdown_requested: Future[None],
  shutdown_complete: Future[None],
  job_gate: Event,
) -> None:
  await sleep(0)
  assert "start" in scheduler.calls
  assert "flush" not in events

  # Shutdown requested: jobs get cancelled but the flush must wait for them to actually stop.
  shutdown_requested.set_result(None)
  for _ in range(5):
    await sleep(0)
  assert scheduler.calls == ["start", "shutdown"]
  assert "job_cancelled" in events
  assert "flush" not in events

  # Job finishes, but the threaded shutdown pass is still running: still no flush.
  job_gate.set()
  for _ in range(5):
    await sleep(0)
  assert "job_done" in events
  assert "flush" not in events
  assert not main_task.done()

  # Both gates released: the catch-up flush runs exactly once, last.
  shutdown_complete.set_result(None)
  await main_task
  assert events == ["job_cancelled", "job_done", "flush"]
