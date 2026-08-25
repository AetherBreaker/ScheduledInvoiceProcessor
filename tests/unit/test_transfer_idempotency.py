"""A holding-FTP rename that already happened (e.g. before a stop mid-wave) is reported as success on re-run."""

# This file tests a private method by design.
# pyright: reportPrivateUsage=false

# Standard library imports
import atexit
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from ftplib import error_perm
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from aeth_ext.rich.progress import TaskID
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sas import SASProcessor

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Generator

  # First party imports
  from aeth_ext.rich.progress import Progress


class _FakeClient:
  """`get_size` answers from `sizes` and raises FileNotFoundError for anything else.

  A `sizes` value may be an exception instance, which is raised instead of returned -- used to drive the
  source-probe error paths of `_already_moved`.

  `rename` raises OSError unless `fail_rename` is False, in which case it succeeds and is merely recorded.
  """

  def __init__(self, sizes: dict[str, int | BaseException], fail_rename: bool = True) -> None:
    self.sizes = sizes
    self.fail_rename = fail_rename
    self.renames: list[tuple[str, str]] = []
    self.get_size_calls: list[str] = []

  def rename(self, old: str, new: str) -> None:
    self.renames.append((old, new))
    if self.fail_rename:
      raise OSError("rename failed")

  def get_size(self, path: str) -> int:
    self.get_size_calls.append(path)
    if path not in self.sizes:
      raise FileNotFoundError(f"no such file {path}")
    size = self.sizes[path]
    if isinstance(size, BaseException):
      raise size
    return size


class _FakePool:
  def __init__(self, client: _FakeClient) -> None:
    self.client = client

  @contextmanager
  def start_session(self) -> Generator[_FakeClient]:
    yield self.client

  def test_connection(self, logit: bool = False) -> bool:
    return True


class _FakePbar:
  def __init__(self, raise_on_update: bool = False) -> None:
    self.raise_on_update = raise_on_update
    self.advances = 0

  def update(self, *args: object, **kwargs: object) -> None:
    self.advances += 1
    if self.raise_on_update:
      raise RuntimeError("progress bar exploded")


def _drop_singleton() -> None:
  if "__shared_instance__" in SASProcessor.__dict__:
    delattr(SASProcessor, "__shared_instance__")


@pytest.fixture
def processor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[SASProcessor]:
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", SimpleNamespace)
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(SASProcessor, "_file_queue_backup_folder", tmp_path / "queue_backups")
  monkeypatch.setattr(SASProcessor, "_corrupted_queue_backup_folder", tmp_path / "queue_backups" / "corrupted")
  monkeypatch.setattr(SASProcessor, "log_file_loc", tmp_path / "logs")
  _drop_singleton()
  proc = SASProcessor()
  proc.pbar = _FakePbar()
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

# TaskID only ever calls `prog_instance.remove_task`; a namespace with that one method stands in for Progress.
_MOVE_FILES_TASK = TaskID(0, cast("Progress", SimpleNamespace(remove_task=lambda *args: None)), remove=False)


def _run(
  processor: SASProcessor, sizes: dict[str, int | BaseException], fail_rename: bool = True
) -> tuple[FileRegisterData, _FakeClient, bool]:
  client = _FakeClient(sizes, fail_rename=fail_rename)
  processor.__class__.waiting_ftp = _FakePool(client)  # type: ignore[assignment]
  meta = _meta()
  result = processor._transfer_file_main_to_main(
    send_path=SEND,
    recv_path=RECV,
    move_files_task=_MOVE_FILES_TASK,
    file_meta=meta,
    idx=0,
    key="k",
    success_attr="preprocess_success",
  )
  return meta, client, result


def test_rename_succeeds_is_reported_as_success(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, client, result = _run(processor, {RECV.as_posix(): 128}, fail_rename=False)
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: True}
  assert client.renames == [(SEND.as_posix(), RECV.as_posix())]
  assert client.get_size_calls == [RECV.as_posix()]
  assert result is True


def test_already_moved_counts_as_success(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, client, result = _run(processor, {RECV.as_posix(): 128})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: True}
  assert client.renames == [(SEND.as_posix(), RECV.as_posix())]
  assert result is True


def test_destination_present_but_source_still_there_is_a_failure(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _, result = _run(processor, {RECV.as_posix(): 128, SEND.as_posix(): 128})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: False}
  assert result is False


def test_destination_absent_is_a_failure(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _, result = _run(processor, {})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: False}
  assert result is False


def test_empty_destination_is_a_failure(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _, result = _run(processor, {RECV.as_posix(): 0})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: False}
  assert result is False


def test_source_probe_permission_error_is_not_absence(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _, result = _run(processor, {RECV.as_posix(): 128, SEND.as_posix(): PermissionError("permission denied")})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: False}
  assert result is False


def test_source_probe_550_perm_error_is_absence(processor: SASProcessor) -> None:
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _, result = _run(processor, {RECV.as_posix(): 128, SEND.as_posix(): error_perm("550 No such file")})
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: True}
  assert result is True


@pytest.mark.parametrize(
  ("sizes", "fail_rename", "expected"),
  [
    ({RECV.as_posix(): 128}, False, True),  # plain success
    ({}, True, False),  # rename failed, destination absent
  ],
)
def test_progress_advances_exactly_once_per_move_even_if_the_bar_raises(
  processor: SASProcessor, sizes: dict[str, int | BaseException], fail_rename: bool, expected: bool
) -> None:
  """A progress-bar failure must neither flip a successful move to failure nor double-advance the bar."""
  pbar = _FakePbar(raise_on_update=True)
  processor.pbar = pbar  # type: ignore[assignment]
  original_pool = SASProcessor.waiting_ftp
  try:
    meta, _, result = _run(processor, sizes, fail_rename=fail_rename)
  finally:
    SASProcessor.waiting_ftp = original_pool
  assert meta.preprocess_success == {0: expected}
  assert result is expected
  assert pbar.advances == 1
