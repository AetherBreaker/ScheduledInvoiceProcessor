"""Unit tests for the SFT warehouse supplier (same-server pickup, header-dated window, RYO-style merge)."""

# This file tests private methods by design.
# pyright: reportPrivateUsage=false

# Future imports
from __future__ import annotations

# Standard library imports
import atexit
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from unittest.mock import AsyncMock, patch

# Third party imports
import pytest
from aeth_ext.rich.progress import TaskID

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.monkey_patches import Patches
from scheduled_invoice_processor.suppliers import SupplierProcessorBase
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sft import SFTProcessor
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Callable, Generator, Iterator
  from io import BytesIO

  # Third party imports
  from aeth_ext.rich.progress import Progress

SAMPLE_HEADER = "SFT017|13842|49273|6/19/2025 9:46:46 AM"
PADDED_HEADER = "SFT017|13842|49273|06/19/2025 09:46:46 AM"


def _drop_singleton() -> None:
  if "__shared_instance__" in SFTProcessor.__dict__:
    delattr(SFTProcessor, "__shared_instance__")


@pytest.fixture
def processor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[SFTProcessor]:
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", SimpleNamespace)
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(SFTProcessor, "_file_queue_backup_folder", tmp_path / "queue_backups")
  monkeypatch.setattr(SFTProcessor, "_corrupted_queue_backup_folder", tmp_path / "queue_backups" / "corrupted")
  monkeypatch.setattr(SFTProcessor, "log_file_loc", tmp_path / "logs")
  _drop_singleton()
  proc = SFTProcessor()
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton()


@pytest.fixture
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
  """Freeze datetime.now() to a time within the 2025-06-08 to 2025-06-22 window."""
  mock_now = datetime(2025, 6, 19, 12, 0, tzinfo=SETTINGS.tz)
  with patch("scheduled_invoice_processor.suppliers.file_register_data.datetime") as mock_dt:
    mock_dt.now.return_value = mock_now
    # Allow datetime constructor to work normally
    mock_dt.side_effect = datetime
    yield


def test_enum_has_sft() -> None:
  assert SuppliersEnum.SFT == "SFT"
  assert SFTProcessor.supplier_name is SuppliersEnum.SFT


def test_vendor_ftp_is_the_waiting_ftp() -> None:
  assert SFTProcessor.vendor_ftp is SFTProcessor.waiting_ftp


@pytest.mark.parametrize("header", [SAMPLE_HEADER, PADDED_HEADER])
def test_header_pattern_matches_sample(header: str) -> None:
  match = SFTProcessor.invoice_num_pattern.match(header)
  assert match is not None
  assert match.group("customer_num") == "SFT017"
  assert match.group("invoice_num") == "13842"
  assert match.group("po_num") == "49273"


def test_header_pattern_rejects_garbage() -> None:
  assert SFTProcessor.invoice_num_pattern.match("850661003182|Boveda 62%|5|5|4.930000") is None


@pytest.mark.parametrize(
  ("name", "expected"),
  [
    ("SFT017_13842.edi", True),
    ("SFT017_13842-13843.edi", True),
    ("SFT017_13842.txt", False),
    ("SFT018_13842.edi", False),
    ("SFT017_13842_20250619094646000000.edi", False),
  ],
)
def test_filename_pattern(processor: SFTProcessor, name: str, expected: bool) -> None:
  now = datetime.now(SETTINGS.tz)
  pattern = processor.assemble_filename_pattern("SFT017", now, now, current_week=True)
  assert (pattern.match(name) is not None) is expected


def test_merged_filename_format() -> None:
  assert SFTProcessor.file_name_format.format(customer_id="SFT017", invoice_num="13842-13843") == "SFT017_13842-13843.edi"


def _meta(pickup: datetime, dropoff: datetime, current_week: bool = True, names: dict[int, str] | None = None) -> FileRegisterData:
  return FileRegisterData(
    storenum=17,
    customer_id="SFT017",
    pickup_date=pickup,
    dropoff_date=dropoff,
    file_pattern=re.compile(r"^SFT017_(?P<invoice_num>[\d\-]+)\.edi$"),
    _current_week=current_week,
    _waiting_folder=PurePosixPath("/TODO_SFT/Waiting"),
    _local_copy_folder=Path("unit-test-local"),
    file_names=names or {},
  )


class _FakeClient:
  """`files` is the remote filesystem: path -> bytes. `transfer_file` copies an entry, `download_file` streams
  one to `callback`, `rename` moves one, and `listdir` yields the entries whose parent is the queried folder."""

  def __init__(
    self,
    files: dict[str, bytes],
    fail_transfer_paths: set[str] | None = None,
    mtimes: dict[str, datetime] | None = None,
  ) -> None:
    self.files = dict(files)
    self.fail_transfer_paths: set[str] = set(fail_transfer_paths or ())
    """Source paths whose transfer raises. Mutable so a test can let a second run succeed where the first failed."""
    self.mtimes = dict(mtimes or {})
    """Per-path mtimes for `listdir`; anything not listed reports `_STALE_MTIME`. This is what the base pickup
    gates on, so it is what decides which files a test's entry picks up."""
    self.renames: list[tuple[str, str]] = []
    self.downloads: list[str] = []
    self.transfers: list[tuple[str, str]] = []

  def listdir(self, path: str) -> Iterator[SimpleNamespace]:
    for remote in list(self.files):
      remote_path = PurePosixPath(remote)
      if remote_path.parent.as_posix() == path:
        yield SimpleNamespace(filename=remote_path.name, modified_time=self.mtimes.get(remote, _STALE_MTIME))

  def rename(self, old: str, new: str) -> None:
    self.renames.append((old, new))
    self.files[new] = self.files.pop(old)

  def transfer_file(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: _FakeClient,
    task_msg: str = "",
    mem_stream: BytesIO | None = None,
  ) -> bool:
    """The base pickup's transfer: stream source -> destination. For SFT `other` is this same fake, because
    `vendor_ftp` and `waiting_ftp` are one pool."""
    self.transfers.append((source_remote_path, dest_remote_path))
    if source_remote_path in self.fail_transfer_paths:
      raise OSError("transfer failed")
    data = self.files[source_remote_path]
    other.files[dest_remote_path] = data
    if mem_stream is not None:
      mem_stream.write(data)
    return True

  def get_size(self, path: str) -> int:
    if path not in self.files:
      raise FileNotFoundError(f"no such file {path}")
    return len(self.files[path])

  def download_file(self, remote_path: str, callback: Callable[[bytes], None], task_msg: str = "") -> None:
    self.downloads.append(remote_path)
    callback(self.files[remote_path])


class _FakePool:
  def __init__(self, client: _FakeClient) -> None:
    self.client = client

  @contextmanager
  def start_session(self) -> Generator[_FakeClient]:
    yield self.client

  def test_connection(self, logit: bool = False) -> bool:
    return True


class _FakePbar:
  def __init__(self) -> None:
    self.advances = 0

  def update(self, *args: object, **kwargs: object) -> None:
    self.advances += 1

  @contextmanager
  def add_task(self, *args: object, **kwargs: object) -> Generator[TaskID]:
    yield _MOVE_FILES_TASK


# TaskID only ever calls `prog_instance.remove_task`; a namespace with that one method stands in for Progress.
_MOVE_FILES_TASK = TaskID(0, cast("Progress", SimpleNamespace(remove_task=lambda *args: None)), remove=False)

SAMPLE_FILE = (SAMPLE_HEADER + "\r\n850661003182|Boveda 62%|5|5|4.930000\r\n").encode()
OUT_OF_WINDOW_FILE = b"SFT017|13900|49300|1/2/2020 9:00:00 AM\r\nX|Y|1|1|1.000000\r\n"

_STALE_MTIME = datetime(2000, 1, 1, tzinfo=SETTINGS.tz)
"""Default mtime for the fake server: outside every pickup window this suite uses."""

SECOND_FILE = b"SFT017|13843|49274|6/20/2025 8:00:00 AM\r\nG100137|Dome Pipe Small|4|4|1.000000\r\n"

# Wednesday of the sample invoice's week.
PICKUP_DATE = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)


def _wire(processor: SFTProcessor, client: _FakeClient, monkeypatch: pytest.MonkeyPatch) -> None:
  pool = _FakePool(client)
  monkeypatch.setattr(SFTProcessor, "vendor_ftp", pool)
  monkeypatch.setattr(SFTProcessor, "waiting_ftp", pool)
  processor.pbar = _FakePbar()  # pyright: ignore[reportAttributeAccessIssue]


class _FakeSchedule:
  def __init__(self) -> None:
    self.checked: list[tuple[tuple[str, int], str]] = []

  async def check_box(self, key: tuple[str, int], column: str) -> None:
    self.checked.append((key, column))


def _stub_cache(processor: SFTProcessor) -> _FakeSchedule:
  """`processor.cache` stand-in. `order_log`: the base `_pickup_files` is wrapped by `@log_actions`, which logs
  every outcome through `self.cache.order_log.log_action` regardless of what the test cares about."""
  schedule = _FakeSchedule()
  processor.cache = SimpleNamespace(  # pyright: ignore[reportAttributeAccessIssue]
    schedule=schedule, prev_week_schedule=schedule, order_log=SimpleNamespace(log_action=AsyncMock())
  )
  return schedule


def _register(processor: SFTProcessor, meta: FileRegisterData) -> str:
  key = processor.assemble_queue_key(meta.storenum, meta.customer_id, meta.pickup_date)
  processor._file_pickup_queue[key] = meta
  return key


def _record_archives(monkeypatch: pytest.MonkeyPatch) -> list[tuple[PurePosixPath, str, PurePosixPath]]:
  """`_vendor_archive_file` is base-class code and skips the move entirely under `__debug__`; record the calls
  the pickup makes instead, which is the part SFT is responsible for wiring up."""
  archived: list[tuple[PurePosixPath, str, PurePosixPath]] = []
  monkeypatch.setattr(
    SFTProcessor,
    "_vendor_archive_file",
    lambda self, source_folder, remote_file, archive_folder, **kwargs: archived.append((source_folder, remote_file, archive_folder)),
  )
  return archived


def test_pickup_is_the_base_implementation() -> None:
  """SFT adds no pickup logic of its own: `vendor_ftp` being the holding pool is the whole trick, and the date
  gate is the base's mtime branch until the warehouse export puts a timestamp in the filename."""
  assert SFTProcessor._pickup_files is SupplierProcessorBase._pickup_files
  assert SFTProcessor.checks_date_in_filename is False
  assert SFTProcessor.vendor_ftp is SFTProcessor.waiting_ftp


async def test_pickup_transfers_the_files_whose_mtime_is_in_window(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  """End to end through the inherited `_pickup_files`: pickup folder -> waiting folder on the one pool."""
  # `processor.pickup_ftp_folder`, not a literal: `tests/unit/conftest.py` forces USE_TESTING_FOLDERS=True, so
  # the class attributes carry a "/Testing" prefix.
  pickup_folder = processor.pickup_ftp_folder
  waiting_folder = processor.pre_processing_waiting_folder
  in_window = (pickup_folder / "SFT017_13842.edi").as_posix()
  stale = (pickup_folder / "SFT017_13900.edi").as_posix()
  other_customer = (pickup_folder / "SFT018_13842.edi").as_posix()

  client = _FakeClient(
    {in_window: SAMPLE_FILE, stale: OUT_OF_WINDOW_FILE, other_customer: SAMPLE_FILE},
    mtimes={in_window: PICKUP_DATE, other_customer: PICKUP_DATE},
  )
  _wire(processor, client, monkeypatch)
  schedule = _stub_cache(processor)
  archived = _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, PICKUP_DATE + timedelta(days=1))
  meta._waiting_folder = waiting_folder
  key = _register(processor, meta)

  await processor._pickup_files()

  # The stale mtime is out of window and the other customer's filename does not match the entry's pattern.
  assert client.transfers == [(in_window, (waiting_folder / "SFT017_13842.edi").as_posix())]
  assert client.files[(waiting_folder / "SFT017_13842.edi").as_posix()] == SAMPLE_FILE
  # The source outlives the copy: it is the archive wave, after the commit, that removes it.
  assert in_window in client.files
  assert stale in client.files
  assert other_customer in client.files
  assert meta.file_names == {0: "SFT017_13842.edi"}
  # The invoice number still comes out of the transferred bytes, via the header pattern.
  assert meta.invoice_nums == {0: "13842"}
  assert meta.pickup_success == {0: True}
  assert key in processor._file_waiting_queue
  assert key not in processor._file_pickup_queue
  assert schedule.checked == [(("SFT", 17), "invoice_grabbed")]
  assert archived == [(pickup_folder, "SFT017_13842.edi", processor.pickup_archive_ftp_folder)]


async def test_pickup_with_no_in_window_files_leaves_queue_untouched(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  pickup_folder = processor.pickup_ftp_folder
  client = _FakeClient({(pickup_folder / "SFT017_13900.edi").as_posix(): OUT_OF_WINDOW_FILE})
  _wire(processor, client, monkeypatch)
  schedule = _stub_cache(processor)
  archived = _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, PICKUP_DATE + timedelta(days=1))
  key = _register(processor, meta)

  await processor._pickup_files()

  assert key in processor._file_pickup_queue
  assert client.transfers == []
  assert schedule.checked == []
  assert archived == []


async def test_pickup_partial_failure_is_recovered_on_the_next_run(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  """One file of an entry copies, the other fails: the entry must not advance, and a re-run must finish it.

  Pickup copies rather than moves, so both sources are still in the pickup folder for run two and the re-copy
  overwrites what run one already wrote. Note the shape of run one: a non-transient transfer error is re-raised
  by `_transfer_file_vend_to_main` and `_pickup_files` gathers without `return_exceptions`, so the whole wave
  aborts and `@log_actions` latches `errored`. That is the base's behaviour for every supplier.
  """
  pickup_folder = processor.pickup_ftp_folder
  waiting_folder = processor.pre_processing_waiting_folder
  a_pickup = (pickup_folder / "SFT017_13842.edi").as_posix()
  b_pickup = (pickup_folder / "SFT017_13843.edi").as_posix()
  a_waiting = (waiting_folder / "SFT017_13842.edi").as_posix()
  b_waiting = (waiting_folder / "SFT017_13843.edi").as_posix()

  client = _FakeClient(
    {a_pickup: SAMPLE_FILE, b_pickup: SECOND_FILE},
    fail_transfer_paths={b_pickup},
    mtimes={a_pickup: PICKUP_DATE, b_pickup: PICKUP_DATE},
  )
  _wire(processor, client, monkeypatch)
  schedule = _stub_cache(processor)
  archived = _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, PICKUP_DATE + timedelta(days=1))
  meta._waiting_folder = waiting_folder
  key = _register(processor, meta)

  with pytest.raises(OSError, match="transfer failed"):
    await processor._pickup_files()

  assert a_waiting in client.files
  assert b_waiting not in client.files
  assert client.files[a_pickup] == SAMPLE_FILE
  assert client.files[b_pickup] == SECOND_FILE
  assert key in processor._file_pickup_queue
  assert key not in processor._file_waiting_queue
  assert schedule.checked == []
  assert archived == []

  client.fail_transfer_paths.clear()
  # `@log_actions` latches `errored` on the way out and only `__init__` clears it, so "the next run" is a restart.
  processor.errored = False

  await processor._pickup_files()

  assert client.files[a_waiting] == SAMPLE_FILE
  assert client.files[b_waiting] == SECOND_FILE
  assert set(meta.file_names.values()) == {"SFT017_13842.edi", "SFT017_13843.edi"}
  assert sorted(meta.invoice_nums.values()) == ["13842", "13843"]
  assert meta.pickup_success == {0: True, 1: True}
  assert key in processor._file_waiting_queue
  assert key not in processor._file_pickup_queue
  assert schedule.checked == [(("SFT", 17), "invoice_grabbed")]
  # A was copied twice; the second copy overwrites the first, which is what makes the re-run safe.
  assert [src for src, _ in client.transfers].count(a_pickup) == 2
  assert sorted(name for _, name, _ in archived) == ["SFT017_13842.edi", "SFT017_13843.edi"]


def test_create_new_merged_file(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  # `_create_new_merged_file` calls `Path.without_cwd`, patched onto `pathlib.PurePath` by `Patches.patch_the_monkey`
  # (applied at real-app startup, not by this test suite's conftest); mirror `test_job_ordering.py`'s `ryo` fixture.
  if not hasattr(Path, "without_cwd"):
    Patches.patch_the_monkey()

  waiting_folder = processor.pre_processing_waiting_folder
  client = _FakeClient(
    {
      (waiting_folder / "SFT017_13842.edi").as_posix(): SAMPLE_FILE,
      (waiting_folder / "SFT017_13843.edi").as_posix(): SECOND_FILE,
    }
  )
  _wire(processor, client, monkeypatch)
  now = datetime.now(SETTINGS.tz)
  meta = _meta(now, now, names={0: "SFT017_13842.edi", 1: "SFT017_13843.edi"})
  meta._waiting_folder = waiting_folder
  meta._local_copy_folder = processor.local_pre_processing_folder

  new_meta = processor._create_new_merged_file("k", meta)

  assert new_meta.file_names == {0: "SFT017_13842-13843.edi"}
  assert new_meta.invoice_nums == {0: "13842-13843"}
  merged = (processor.local_post_processing_folder / "SFT017_13842-13843.edi").read_bytes()
  lines = merged.split(b"\r\n")
  # Header keeps the earliest invoice date, verbatim in the source's unpadded format (not re-serialised through
  # strftime, which would emit "06/19/2025 09:46:46 AM"); body is the concatenation of both bodies.
  assert lines[0] == b"SFT017|13842-13843|49273|6/19/2025 9:46:46 AM"
  assert lines[1] == b"850661003182|Boveda 62%|5|5|4.930000"
  assert lines[2] == b"G100137|Dome Pipe Small|4|4|1.000000"
  assert new_meta.file_pattern.match("SFT017_13842-13843.edi") is not None


def test_sft_is_an_expected_supplier(monkeypatch: pytest.MonkeyPatch) -> None:
  # startup.py builds `supplier_register` by probing connections at import time; stub that out.
  monkeypatch.setattr(SFTProcessor, "check_connections", classmethod(lambda cls: False))
  # First party imports
  from scheduled_invoice_processor.startup import expected_suppliers

  assert expected_suppliers[SuppliersEnum.SFT] is SFTProcessor
