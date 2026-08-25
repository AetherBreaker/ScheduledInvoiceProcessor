"""Queue transitions are committed after the remote side effect they describe and before any cleanup a re-run
would need (spec §3, findings F1/F3/F7)."""

# This file drives the private job methods and queues by design.
# pyright: reportPrivateUsage=false

# Standard library imports
import atexit
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.monkey_patches import Patches
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
from scheduled_invoice_processor.suppliers.sas import SASProcessor

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Generator

  # First party imports
  from scheduled_invoice_processor.suppliers import SupplierProcessorBase


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
  def start_session(self) -> Generator[_FakeClient]:
    yield self.client

  def test_connection(self, logit: bool = False) -> bool:
    return True


class _FakePbar:
  @contextmanager
  def add_task(self, *args: object, **kwargs: object) -> Generator[int]:
    yield 0

  def update(self, *args: object, **kwargs: object) -> None:
    pass


def _drop_singleton(cls: type) -> None:
  if "__shared_instance__" in cls.__dict__:
    delattr(cls, "__shared_instance__")


def _client(proc: SupplierProcessorBase) -> _FakeClient:
  """The fixtures install one `_FakePool` (sharing one client) as both pools; the class attribute is typed as the
  real adapter, so reach the fake through a cast."""
  return cast("_FakePool", proc.waiting_ftp).client


def _check_box(proc: SupplierProcessorBase) -> AsyncMock:
  return cast("AsyncMock", proc.cache.schedule.check_box)


def _fake_cache() -> Any:
  return SimpleNamespace(
    schedule=SimpleNamespace(check_box=AsyncMock()),
    prev_week_schedule=SimpleNamespace(check_box=AsyncMock()),
    order_log=SimpleNamespace(log_action=AsyncMock()),
  )


@pytest.fixture
def sas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[SASProcessor]:
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
  proc.pbar = cast("Any", _FakePbar())
  proc.cache = _fake_cache()
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
  _check_box(sas).assert_awaited_once()


# ---- F1: the pickup->waiting move is persisted before any vendor archive rename ----


async def test_pickup_persists_queue_move_before_vendor_archive(sas: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  events: list[str] = []
  meta = _meta("/Waiting/SAS")
  meta.pickup_success = {0: True, 1: True}  # stale flags from an earlier partial run (F6)
  sas._file_pickup_queue["k"] = meta
  _client(sas).listing = [SimpleNamespace(filename="inv.txt", modified_time=datetime.now(SETTINGS.tz))]

  def fake_copy(*, file_meta: FileRegisterData, idx: int, success_attr: str, **kwargs: object) -> bool:
    events.append("copy")
    getattr(file_meta, success_attr)[idx] = True
    return True

  def fake_archive(**kwargs: object) -> None:
    events.append("archive")
    assert "k" in sas._file_waiting_queue, "archive ran before the queue move was committed"

  real_persist = sas._persist_queues

  def recording_persist() -> bool:
    events.append("persist")
    return real_persist()

  monkeypatch.setattr(sas, "_transfer_file_vend_to_main", fake_copy)
  monkeypatch.setattr(sas, "_vendor_archive_file", fake_archive)
  monkeypatch.setattr(sas, "_persist_queues", recording_persist)

  await sas._pickup_files()

  assert events == ["copy", "persist", "archive"]
  assert "k" in sas._file_waiting_queue and "k" not in sas._file_pickup_queue
  assert meta.pickup_success == {0: True}, "stale success flags from a previous match must be cleared on re-match"
  _check_box(sas).assert_awaited_once()


async def test_pickup_does_not_advance_when_no_transfer_recorded_a_result(sas: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  events: list[str] = []
  meta = _meta("/Waiting/SAS")
  meta.pickup_success = {0: True, 1: True}  # stale flags from an earlier partial run (F6)
  sas._file_pickup_queue["k"] = meta
  _client(sas).listing = [SimpleNamespace(filename="inv.txt", modified_time=datetime.now(SETTINGS.tz))]

  def fake_copy(*, file_meta: FileRegisterData, idx: int, success_attr: str, **kwargs: object) -> bool:
    # Simulates the `self.errored` early return in `_transfer_file_vend_to_main`: no flag is written.
    return False

  def fake_archive(**kwargs: object) -> None:
    events.append("archive")

  real_persist = sas._persist_queues

  def recording_persist() -> bool:
    events.append("persist")
    return real_persist()

  monkeypatch.setattr(sas, "_transfer_file_vend_to_main", fake_copy)
  monkeypatch.setattr(sas, "_vendor_archive_file", fake_archive)
  monkeypatch.setattr(sas, "_persist_queues", recording_persist)

  await sas._pickup_files()

  assert "k" in sas._file_pickup_queue
  assert "k" not in sas._file_waiting_queue
  _check_box(sas).assert_not_awaited()
  assert "archive" not in events


# ---- F3: the merged file is on the holding FTP before the dropoff queue says it is ----


@pytest.fixture
def ryo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[RYOProcessor]:
  if not hasattr(Path, "without_cwd"):
    Patches.patch_the_monkey()

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
  proc.pbar = cast("Any", _FakePbar())
  proc.cache = _fake_cache()
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton(RYOProcessor)


def test_preprocess_uploads_before_commit_and_archives_after(
  ryo: RYOProcessor, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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

  real_upload = _client(ryo).upload_file

  def recording_upload(remote_path: str, callback: Any, file_size: int, task_msg: str = "") -> None:
    events.append("upload")
    assert "k" not in ryo._file_dropoff_queue, "queue committed before the merged file was uploaded"
    real_upload(remote_path, callback, file_size, task_msg)

  real_persist = ryo._persist_queues

  def recording_persist() -> bool:
    events.append("persist")
    return real_persist()

  monkeypatch.setattr(ryo, "_middle_archive_file", fake_archive)
  monkeypatch.setattr(_client(ryo), "upload_file", recording_upload)
  monkeypatch.setattr(ryo, "_persist_queues", recording_persist)

  key, result = ryo._preprocess_off_thread("k", old)

  assert (key, result) == ("k", new)
  assert events == ["upload", "persist", "archive", "archive"]
  assert _client(ryo).uploads == [(ryo.post_processing_waiting_folder / "merged.txt").as_posix()]
  assert "k" in ryo._file_dropoff_queue and "k" not in ryo._file_preprocess_queue


def test_preprocess_skips_original_archive_when_persist_fails(
  ryo: RYOProcessor, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
  """Same gate as pickup: a queue backup that could not be written leaves the originals where the on-disk ledger
  (still 'preprocess') would look for them on the next run."""
  events: list[str] = []
  old = _meta("/Waiting/RYO", {0: "inv-a.txt"})
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
  monkeypatch.setattr(ryo, "_middle_archive_file", lambda **kwargs: events.append("archive"))
  monkeypatch.setattr(ryo, "_persist_queues", lambda: False)

  key, result = ryo._preprocess_off_thread("k", old)

  assert (key, result) == ("k", new)
  assert "archive" not in events
  assert _client(ryo).uploads == [(ryo.post_processing_waiting_folder / "merged.txt").as_posix()]
  assert "k" in ryo._file_dropoff_queue and "k" not in ryo._file_preprocess_queue


# ---- C2: the sheet tick happens while the dropoff entry is still in the queue ----


async def test_dropoff_ticks_invoice_applied_before_popping_the_entry(sas: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  meta = _meta("/Waiting/SAS/Processed")
  sas._file_dropoff_queue["k"] = meta

  async def no_preprocess(*args: object, **kwargs: object) -> None:
    pass

  def fake_move(*, file_meta: FileRegisterData, idx: int, success_attr: str, **kwargs: object) -> bool:
    getattr(file_meta, success_attr)[idx] = True
    return True

  async def assert_still_queued(*args: object, **kwargs: object) -> None:
    assert "k" in sas._file_dropoff_queue, "entry was popped before invoice_applied was ticked"

  check_box = AsyncMock(side_effect=assert_still_queued)
  monkeypatch.setattr(sas.cache.schedule, "check_box", check_box)
  monkeypatch.setattr(sas, "_preprocess_files", no_preprocess)
  monkeypatch.setattr(sas, "_transfer_file_main_to_main", fake_move)

  await sas._dropoff_files()

  assert "k" not in sas._file_dropoff_queue
  check_box.assert_awaited_once()


async def test_dropoff_pops_and_persists_after_every_tick_with_no_await_between(
  sas: SASProcessor, monkeypatch: pytest.MonkeyPatch
) -> None:
  """All ticks are awaited first; then every popped entry and the ledger write happen in one synchronous stretch,
  so a cancellation can never leave an in-memory pop unpersisted."""
  sas._file_dropoff_queue["a"] = _meta("/Waiting/SAS/Processed")
  sas._file_dropoff_queue["b"] = _meta("/Waiting/SAS/Processed")
  events: list[str] = []

  async def no_preprocess(*args: object, **kwargs: object) -> None:
    pass

  def fake_move(*, file_meta: FileRegisterData, idx: int, success_attr: str, **kwargs: object) -> bool:
    getattr(file_meta, success_attr)[idx] = True
    return True

  async def tick(*args: object, **kwargs: object) -> None:
    events.append(f"tick:{sorted(sas._file_dropoff_queue)}")

  real_persist = sas._persist_queues

  def recording_persist() -> bool:
    events.append(f"persist:{sorted(sas._file_dropoff_queue)}")
    return real_persist()

  monkeypatch.setattr(sas.cache.schedule, "check_box", AsyncMock(side_effect=tick))
  monkeypatch.setattr(sas, "_preprocess_files", no_preprocess)
  monkeypatch.setattr(sas, "_transfer_file_main_to_main", fake_move)
  monkeypatch.setattr(sas, "_persist_queues", recording_persist)

  await sas._dropoff_files()

  # Both ticks saw both entries still queued; the single persist saw neither.
  assert events == ["tick:['a', 'b']", "tick:['a', 'b']", "persist:[]"]


# ---- Ledger promotion: a failed queue-backup write gates the vendor archive ----


async def test_pickup_skips_vendor_archive_when_persist_fails(sas: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  events: list[str] = []
  meta = _meta("/Waiting/SAS")
  sas._file_pickup_queue["k"] = meta
  _client(sas).listing = [SimpleNamespace(filename="inv.txt", modified_time=datetime.now(SETTINGS.tz))]

  def fake_copy(*, file_meta: FileRegisterData, idx: int, success_attr: str, **kwargs: object) -> bool:
    getattr(file_meta, success_attr)[idx] = True
    return True

  def fake_archive(**kwargs: object) -> None:
    events.append("archive")

  monkeypatch.setattr(sas, "_transfer_file_vend_to_main", fake_copy)
  monkeypatch.setattr(sas, "_vendor_archive_file", fake_archive)
  monkeypatch.setattr(sas, "_persist_queues", lambda: False)

  await sas._pickup_files()

  assert "archive" not in events
  assert "k" in sas._file_waiting_queue and "k" not in sas._file_pickup_queue
