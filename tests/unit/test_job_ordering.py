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
  def start_session(self) -> Iterator[_FakeClient]:
    yield self.client

  def test_connection(self, logit: bool = False) -> bool:
    return True


class _FakePbar:
  @contextmanager
  def add_task(self, *args: object, **kwargs: object) -> Iterator[int]:
    yield 0

  def update(self, *args: object, **kwargs: object) -> None:
    pass


def _drop_singleton(cls: type) -> None:
  if "__shared_instance__" in cls.__dict__:
    delattr(cls, "__shared_instance__")


@pytest.fixture
def sas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[SASProcessor]:
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
    order_log=SimpleNamespace(log_action=AsyncMock()),
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
