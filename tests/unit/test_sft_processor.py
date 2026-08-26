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

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from aeth_ext.rich.progress import TaskID
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.monkey_patches import Patches
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sft import SFTProcessor
from scheduled_invoice_processor.typing_custom.enums import StatusCode, SuppliersEnum

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Callable, Generator, Iterator

  # First party imports
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


def test_parse_header_date_sample(processor: SFTProcessor) -> None:
  parsed = processor.parse_header_date(SAMPLE_HEADER)
  assert parsed == datetime(2025, 6, 19, 9, 46, 46, tzinfo=SETTINGS.tz)
  assert parsed is not None
  assert parsed.tzinfo is SETTINGS.tz


def test_parse_header_date_rejects_non_header(processor: SFTProcessor) -> None:
  assert processor.parse_header_date("850661003182|Boveda 62%|5|5|4.930000") is None


def test_parse_header_date_rejects_impossible_date(processor: SFTProcessor) -> None:
  assert processor.parse_header_date("SFT017|1|1|13/45/2025 9:46:46 AM") is None


def test_header_date_in_window_current_week(processor: SFTProcessor, frozen_now: None) -> None:
  # A Wednesday. current_week=True gives 2-week window: Sun 2025-06-08 00:00 through Sat 2025-06-21 23:59:59.
  pickup = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
  meta = _meta(pickup, pickup + timedelta(days=1))
  # In-window: Jun 19 Thu, Jun 08 Sun, Jun 14 Sat
  assert processor.header_date_in_window(meta, datetime(2025, 6, 19, 9, 46, 46, tzinfo=SETTINGS.tz))
  assert processor.header_date_in_window(meta, datetime(2025, 6, 8, 0, 0, 0, tzinfo=SETTINGS.tz))
  assert processor.header_date_in_window(meta, datetime(2025, 6, 14, 23, 59, 59, tzinfo=SETTINGS.tz))
  # Out-of-window: Jun 07 Sat, Jun 22 Sun
  assert not processor.header_date_in_window(meta, datetime(2025, 6, 7, 23, 59, 59, tzinfo=SETTINGS.tz))
  assert not processor.header_date_in_window(meta, datetime(2025, 6, 22, 0, 0, 0, tzinfo=SETTINGS.tz))


def test_header_date_in_window_previous_week(processor: SFTProcessor, frozen_now: None) -> None:
  # A Wednesday with current_week=False (window has passed). Window is previous week: Sun 2025-06-15 through Sat 2025-06-14 (empty/past window).
  pickup = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
  meta = _meta(pickup, pickup + timedelta(days=1), current_week=False)
  # Window is Sun 2025-06-15 00:00 through Sat 2025-06-14 23:59:59 (backward, so nothing matches)
  assert not processor.header_date_in_window(meta, datetime(2025, 6, 19, 9, 46, 46, tzinfo=SETTINGS.tz))


class _FakeClient:
  """`files` is the remote filesystem: path -> bytes. `rename` moves an entry (or raises when `fail_rename`).
  `listdir` yields entries whose parent is the queried folder. `download_file` streams the bytes to `callback`."""

  def __init__(self, files: dict[str, bytes], fail_rename: bool = False) -> None:
    self.files = dict(files)
    self.fail_rename = fail_rename
    self.renames: list[tuple[str, str]] = []
    self.downloads: list[str] = []

  def listdir(self, path: str) -> Iterator[SimpleNamespace]:
    for remote in list(self.files):
      remote_path = PurePosixPath(remote)
      if remote_path.parent.as_posix() == path:
        yield SimpleNamespace(filename=remote_path.name, modified_time=datetime(2000, 1, 1, tzinfo=SETTINGS.tz))

  def rename(self, old: str, new: str) -> None:
    self.renames.append((old, new))
    if self.fail_rename:
      raise OSError("rename failed")
    self.files[new] = self.files.pop(old)

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
PICKUP = PurePosixPath("/TODO_SFT/Pickup/SFT017_13842.edi")
WAITING = PurePosixPath("/TODO_SFT/Waiting/SFT017_13842.edi")


def _wire(processor: SFTProcessor, client: _FakeClient, monkeypatch: pytest.MonkeyPatch) -> None:
  pool = _FakePool(client)
  monkeypatch.setattr(SFTProcessor, "vendor_ftp", pool)
  monkeypatch.setattr(SFTProcessor, "waiting_ftp", pool)
  processor.pbar = _FakePbar()  # pyright: ignore[reportAttributeAccessIssue]


def _transfer(processor: SFTProcessor, meta: FileRegisterData, log: list) -> bool:
  return processor._transfer_file_same_server(
    send_path=PICKUP,
    recv_path=WAITING,
    move_files_task=_MOVE_FILES_TASK,
    file_meta=meta,
    idx=0,
    key="k",
    file_bytes=SAMPLE_FILE,
    log_action_handler=lambda key, status, fm: log.append(status),
  )


def test_same_server_transfer_renames_and_extracts_invoice(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  client = _FakeClient({PICKUP.as_posix(): SAMPLE_FILE})
  _wire(processor, client, monkeypatch)
  now = datetime.now(SETTINGS.tz)
  meta = _meta(now, now, names={0: "SFT017_13842.edi"})
  log: list = []

  assert _transfer(processor, meta, log) is True
  assert client.renames == [(PICKUP.as_posix(), WAITING.as_posix())]
  assert WAITING.as_posix() in client.files
  assert meta.pickup_success == {0: True}
  assert meta.invoice_nums == {0: "13842"}
  assert log == [StatusCode.SUCCESS]
  assert processor.pbar.advances == 1  # pyright: ignore[reportAttributeAccessIssue]


def test_same_server_transfer_already_moved_is_success(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  # Source already gone, destination present: an earlier run did the rename before it could record it.
  client = _FakeClient({WAITING.as_posix(): SAMPLE_FILE}, fail_rename=True)
  _wire(processor, client, monkeypatch)
  now = datetime.now(SETTINGS.tz)
  meta = _meta(now, now, names={0: "SFT017_13842.edi"})
  log: list = []

  assert _transfer(processor, meta, log) is True
  assert meta.pickup_success == {0: True}
  assert meta.invoice_nums == {0: "13842"}
  assert log == [StatusCode.SUCCESS]


def test_same_server_transfer_failure_is_recorded_not_raised(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  client = _FakeClient({PICKUP.as_posix(): SAMPLE_FILE}, fail_rename=True)
  _wire(processor, client, monkeypatch)
  now = datetime.now(SETTINGS.tz)
  meta = _meta(now, now, names={0: "SFT017_13842.edi"})
  log: list = []

  assert _transfer(processor, meta, log) is False
  assert meta.pickup_success == {0: False}
  assert log == [StatusCode.FAILURE]
  assert processor.pbar.advances == 1  # pyright: ignore[reportAttributeAccessIssue]


OUT_OF_WINDOW_FILE = b"SFT017|13900|49300|1/2/2020 9:00:00 AM\r\nX|Y|1|1|1.000000\r\n"
NO_HEADER_FILE = b"850661003182|Boveda 62%|5|5|4.930000\r\n"


class _FakeSchedule:
  def __init__(self) -> None:
    self.checked: list[tuple[tuple[str, int], str]] = []

  async def check_box(self, key: tuple[str, int], column: str) -> None:
    self.checked.append((key, column))


def _register(processor: SFTProcessor, meta: FileRegisterData) -> str:
  key = processor.assemble_queue_key(meta.storenum, meta.customer_id, meta.pickup_date)
  processor._file_pickup_queue[key] = meta
  return key


async def test_pickup_keeps_only_in_window_files(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  # Header date in the sample is 2025-06-19 (Thursday). Pickup on Wednesday 2025-06-18, current week.
  # `frozen_now` pins `datetime.now()` inside that window: `FileRegisterData.current_week` is a live property
  # comparing the pickup/dropoff window to the real clock, not the stored `_current_week` flag alone.
  # `processor.pickup_ftp_folder`, not a hardcoded "/TODO_SFT/..." literal: `tests/unit/conftest.py` forces
  # USE_TESTING_FOLDERS=True, so the class attribute actually resolves to "/Testing/TODO_SFT/Pickup".
  pickup_folder = processor.pickup_ftp_folder
  pickup = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
  client = _FakeClient(
    {
      (pickup_folder / "SFT017_13842.edi").as_posix(): SAMPLE_FILE,
      (pickup_folder / "SFT017_13900.edi").as_posix(): OUT_OF_WINDOW_FILE,
      (pickup_folder / "SFT017_13901.edi").as_posix(): NO_HEADER_FILE,
      (pickup_folder / "SFT018_13842.edi").as_posix(): SAMPLE_FILE,
    }
  )
  _wire(processor, client, monkeypatch)
  schedule = _FakeSchedule()
  # `order_log`: `_pickup_files` is wrapped by `@log_actions`, which logs every outcome through
  # `self.cache.order_log.log_action` regardless of what the test cares about.
  processor.cache = SimpleNamespace(  # pyright: ignore[reportAttributeAccessIssue]
    schedule=schedule, prev_week_schedule=schedule, order_log=SimpleNamespace(log_action=AsyncMock())
  )
  meta = _meta(pickup, pickup + timedelta(days=1))
  key = _register(processor, meta)

  await processor._pickup_files()

  # Only the in-window SFT017 file moved; the others stay exactly where they were.
  assert "/TODO_SFT/Waiting/SFT017_13842.edi" in client.files
  assert (pickup_folder / "SFT017_13900.edi").as_posix() in client.files
  assert (pickup_folder / "SFT017_13901.edi").as_posix() in client.files
  assert (pickup_folder / "SFT018_13842.edi").as_posix() in client.files
  # Non-matching filename was never downloaded; the two SFT017 rejects were (header needed to decide).
  assert (pickup_folder / "SFT018_13842.edi").as_posix() not in client.downloads
  assert meta.file_names == {0: "SFT017_13842.edi"}
  assert meta.invoice_nums == {0: "13842"}
  assert key in processor._file_waiting_queue
  assert key not in processor._file_pickup_queue
  assert schedule.checked == [(("SFT", 17), "invoice_grabbed")]
  # The rename is the removal: nothing is left in the pickup folder and nothing is written to the archive.
  assert (pickup_folder / "SFT017_13842.edi").as_posix() not in client.files
  assert (processor.pickup_archive_ftp_folder / "SFT017_13842.edi").as_posix() not in client.files  # pyright: ignore[reportOptionalOperand]


async def test_pickup_with_no_in_window_files_leaves_queue_untouched(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  pickup_folder = processor.pickup_ftp_folder
  pickup = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
  client = _FakeClient({(pickup_folder / "SFT017_13900.edi").as_posix(): OUT_OF_WINDOW_FILE})
  _wire(processor, client, monkeypatch)
  schedule = _FakeSchedule()
  # `order_log`: `_pickup_files` is wrapped by `@log_actions`, which logs every outcome through
  # `self.cache.order_log.log_action` regardless of what the test cares about.
  processor.cache = SimpleNamespace(  # pyright: ignore[reportAttributeAccessIssue]
    schedule=schedule, prev_week_schedule=schedule, order_log=SimpleNamespace(log_action=AsyncMock())
  )
  meta = _meta(pickup, pickup + timedelta(days=1))
  key = _register(processor, meta)

  await processor._pickup_files()

  assert key in processor._file_pickup_queue
  assert client.renames == []
  assert schedule.checked == []


SECOND_FILE = b"SFT017|13843|49274|6/20/2025 8:00:00 AM\r\nG100137|Dome Pipe Small|4|4|1.000000\r\n"


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
  # Header keeps the earliest invoice date; body is the concatenation of both bodies.
  assert lines[0] == b"SFT017|13842-13843|49273|06/19/2025 09:46:46 AM"
  assert lines[1] == b"850661003182|Boveda 62%|5|5|4.930000"
  assert lines[2] == b"G100137|Dome Pipe Small|4|4|1.000000"
  assert new_meta.file_pattern.match("SFT017_13842-13843.edi") is not None


def test_sft_is_an_expected_supplier(monkeypatch: pytest.MonkeyPatch) -> None:
  # startup.py builds `supplier_register` by probing connections at import time; stub that out.
  monkeypatch.setattr(SFTProcessor, "check_connections", classmethod(lambda cls: False))
  # First party imports
  from scheduled_invoice_processor.startup import expected_suppliers

  assert expected_suppliers[SuppliersEnum.SFT] is SFTProcessor
