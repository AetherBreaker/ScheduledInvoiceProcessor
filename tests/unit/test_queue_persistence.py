"""Queue backups are written atomically on every change, and once more at interpreter exit."""

# Standard library imports
import atexit
import json
import logging
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

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


def _drop_singleton() -> None:
  if "__shared_instance__" in SASProcessor.__dict__:
    delattr(SASProcessor, "__shared_instance__")


@pytest.fixture
def backup_dir(tmp_path: Path) -> Path:
  return tmp_path / "queue_backups"


@pytest.fixture
def processor(tmp_path: Path, backup_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[SASProcessor]:
  """A real SASProcessor with its filesystem redirected to tmp_path and its DatabaseCache stubbed (no network)."""
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", SimpleNamespace)
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(SASProcessor, "_file_queue_backup_folder", backup_dir)
  monkeypatch.setattr(SASProcessor, "_corrupted_queue_backup_folder", backup_dir / "corrupted")
  monkeypatch.setattr(SASProcessor, "log_file_loc", tmp_path / "logs")
  _drop_singleton()
  proc = SASProcessor()
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton()


def _entry(days_from_now: int = 7) -> FileRegisterData:
  now = datetime.now(SETTINGS.tz)
  return FileRegisterData(
    storenum=9001,
    customer_id="900100",
    pickup_date=now + timedelta(days=days_from_now),
    dropoff_date=now + timedelta(days=days_from_now + 1),
    file_pattern=re.compile(r"^unit-test-.*\.txt$"),
    _current_week=True,
    _waiting_folder=PurePosixPath("/Waiting/SAS"),
    _local_copy_folder=Path("unit-test-local"),
  )


def _read(path: Path) -> dict[str, Any]:
  return json.loads(path.read_text())


def test_persist_writes_all_four_queues_and_leaves_no_tmp(processor: SASProcessor, backup_dir: Path) -> None:
  processor._file_pickup_queue["p"] = _entry()
  processor._file_waiting_queue["w"] = _entry()
  processor._file_preprocess_queue["pp"] = _entry()
  processor._file_dropoff_queue["d"] = _entry()

  processor._persist_queues()

  assert set(_read(processor.pickup_queue_backup_file)) == {"p"}
  assert set(_read(processor.waiting_queue_backup_file)) == {"w"}
  assert set(_read(processor.preprocess_queue_backup_file)) == {"pp"}
  assert set(_read(processor.dropoff_queue_backup_file)) == {"d"}
  assert not list(backup_dir.glob("*.tmp"))


def test_persist_reflects_each_change_immediately(processor: SASProcessor) -> None:
  processor._file_pickup_queue["first"] = _entry()
  processor._persist_queues()
  assert set(_read(processor.pickup_queue_backup_file)) == {"first"}

  processor._file_pickup_queue.pop("first")
  processor._file_pickup_queue["second"] = _entry()
  processor._persist_queues()
  assert set(_read(processor.pickup_queue_backup_file)) == {"second"}


def test_failed_replace_leaves_previous_file_intact(processor: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  processor._file_pickup_queue["kept"] = _entry()
  processor._persist_queues()
  before = processor.pickup_queue_backup_file.read_text()

  def _boom(src: Any, dst: Any) -> None:
    raise OSError("simulated replace failure")

  monkeypatch.setattr(suppliers_mod, "replace", _boom)
  processor._file_pickup_queue["lost"] = _entry()
  with pytest.raises(OSError, match="simulated replace failure"):
    processor._persist_queues()

  assert processor.pickup_queue_backup_file.read_text() == before


def test_loader_ignores_stale_tmp_files(
  processor: SASProcessor, tmp_path: Path, backup_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
  processor._file_pickup_queue["real"] = _entry()
  processor._persist_queues()
  stale = backup_dir / f"{processor.pickup_queue_backup_file.name}.tmp"
  stale.write_text("{ this is not json")

  atexit.unregister(processor._persist_queues_at_exit)
  _drop_singleton()
  reloaded = SASProcessor()
  try:
    assert set(reloaded._file_pickup_queue) == {"real"}
    assert not list((backup_dir / "corrupted").glob("*"))
  finally:
    atexit.unregister(reloaded._persist_queues_at_exit)


def test_at_exit_persists_when_lock_is_free(processor: SASProcessor, caplog: pytest.LogCaptureFixture) -> None:
  processor._file_dropoff_queue["d"] = _entry()
  with caplog.at_level(logging.WARNING):
    processor._persist_queues_at_exit()
  assert set(_read(processor.dropoff_queue_backup_file)) == {"d"}
  assert "still held" not in caplog.text
  assert not processor._lock.locked()


def test_at_exit_still_persists_with_warning_when_lock_is_held(processor: SASProcessor, caplog: pytest.LogCaptureFixture) -> None:
  processor._file_dropoff_queue["d"] = _entry()
  holder_ready = threading.Event()
  release = threading.Event()

  def _hold() -> None:
    with processor._lock:
      holder_ready.set()
      release.wait(timeout=10)

  holder = threading.Thread(target=_hold, daemon=True)
  holder.start()
  assert holder_ready.wait(timeout=5)
  try:
    with caplog.at_level(logging.WARNING):
      processor._persist_queues_at_exit()
  finally:
    release.set()
    holder.join(timeout=5)

  assert set(_read(processor.dropoff_queue_backup_file)) == {"d"}
  assert "still held" in caplog.text
  assert not processor._lock.locked()


def test_at_exit_handler_is_registered_on_construction(processor: SASProcessor) -> None:
  # atexit._ncallbacks() is CPython's count of registered handlers; construction (in the fixture) registered exactly one
  # bound method, so unregistering it drops the count by one. Re-register so the fixture teardown stays symmetric.
  before = atexit._ncallbacks()
  atexit.unregister(processor._persist_queues_at_exit)
  after = atexit._ncallbacks()
  atexit.register(processor._persist_queues_at_exit)
  assert before - after == 1


async def test_clean_stale_entries_persists_under_lock(processor: SASProcessor) -> None:
  processor._file_pickup_queue["stale"] = _entry(days_from_now=-30)
  processor._persist_queues()
  assert set(_read(processor.pickup_queue_backup_file)) == {"stale"}

  await processor.clean_stale_queue_entries()

  assert processor._file_pickup_queue == {}
  assert _read(processor.pickup_queue_backup_file) == {}


def test_concurrent_off_thread_persists_are_serialised(processor: SASProcessor) -> None:
  """`_persist_lock` must serialise concurrent OS-thread mutation-plus-persist, the way `_preprocess_off_thread`
  workers do, against sibling workers and against plain `_persist_queues()` callers alike."""
  errors: list[BaseException] = []

  def _mutate_and_persist(key: str) -> None:
    try:
      with processor._persist_lock:
        processor._file_dropoff_queue[key] = _entry()
        processor._persist_queues()
    except BaseException as e:  # noqa: BLE001
      errors.append(e)

  def _persist_only() -> None:
    try:
      processor._persist_queues()
    except BaseException as e:  # noqa: BLE001
      errors.append(e)

  mutators = [threading.Thread(target=_mutate_and_persist, args=(f"k{i}",)) for i in range(8)]
  persisters = [threading.Thread(target=_persist_only) for _ in range(8)]
  threads = mutators + persisters

  for t in threads:
    t.start()
  for t in threads:
    t.join(timeout=10)

  assert errors == []
  assert set(_read(processor.dropoff_queue_backup_file)) == {f"k{i}" for i in range(8)}
  assert not list(processor._file_queue_backup_folder.glob("*.tmp"))


def test_legacy_save_paths_are_gone() -> None:
  assert not hasattr(SASProcessor, "_save_backups")
  assert not hasattr(SASProcessor, "save_queue_backups_off_thread")
  assert "__del__" not in suppliers_mod.SupplierProcessorBase.__dict__
