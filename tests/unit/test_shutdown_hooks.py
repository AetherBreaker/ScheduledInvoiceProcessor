"""Shutdown callbacks: freeze the scheduler, flush queued sheet writes, skip the flush on a database-origin fatal."""

# Standard library imports
import logging
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

# First party imports
import scheduled_invoice_processor.shutdown_hooks as hooks
from aeth_ext.errors.shutdown import ShutdownPhase

if TYPE_CHECKING:
  # Third party imports
  import pytest


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
