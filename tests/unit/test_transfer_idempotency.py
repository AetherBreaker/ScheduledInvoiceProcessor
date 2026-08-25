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
  def start_session(self) -> Iterator[_FakeClient]:
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
def processor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[SASProcessor]:
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
