"""Unit tests for the SFT warehouse supplier (same-server pickup, filename-dated window, RYO-style merge)."""

# This file tests private methods by design.
# pyright: reportPrivateUsage=false

# Standard library imports
import atexit
import logging
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
from scheduled_invoice_processor.suppliers import SupplierProcessorBase
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sft import SFTProcessor
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Callable, Generator, Iterator
  from io import BytesIO
  from re import Pattern
  from typing import Any

  # First party imports
  from aeth_ext.rich.progress import Progress

SAMPLE_HEADER = "SFT017|13842|49273|6/19/2025 9:46:46 AM"
PADDED_HEADER = "SFT017|13842|49273|06/19/2025 09:46:46 AM"

# Wednesday of the sample invoice's week: Sun 2025-06-15 .. Sat 2025-06-21.
PICKUP_DATE = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
DROPOFF_DATE = PICKUP_DATE + timedelta(days=1)

# The warehouse export's filename timestamp is YYYYMMDDHHMMSS -- no microseconds, unlike RYO and SAS.
IN_WEEK_TS = "20250619094646"
"""SAMPLE_HEADER's invoice date, as the export writes it into the filename."""
LAST_WEEK_TS = "20250612090000"
"""Thursday of the week before: outside the entry's window."""

FILE_A = f"SFT017_13842_{IN_WEEK_TS}.edi"
FILE_B = f"SFT017_13843_{IN_WEEK_TS}.edi"
LAST_WEEK_FILE = f"SFT017_13800_{LAST_WEEK_TS}.edi"


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


def _pattern(start: datetime, end: datetime, current_week: bool = True) -> Pattern[str]:
  """The real pattern, exactly as `register_pickup` builds it. With `checks_date_in_filename` the pattern *is*
  the pickup window, so nothing in this suite hand-rolls one."""
  return SFTProcessor.assemble_filename_pattern(cast("Any", None), "SFT017", start, end, current_week)


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


# --- Filename pattern --------------------------------------------------------------------------------------


def test_filename_pattern_carries_the_date_groups_the_base_reads() -> None:
  """`_date_from_filename_match` wants `year`/`month`/`day` (and optionally the time); the pattern supplies them."""
  match = _pattern(PICKUP_DATE, DROPOFF_DATE).match(FILE_A)

  assert match is not None
  assert match.group("invoice_num") == "13842"
  assert match.group("timestamp") == IN_WEEK_TS
  assert tuple(match.group(g) for g in ("year", "month", "day", "hour", "minute", "second")) == (
    "2025",
    "06",
    "19",
    "09",
    "46",
    "46",
  )
  assert SupplierProcessorBase._date_from_filename_match(match) == datetime(2025, 6, 19, 9, 46, 46, tzinfo=SETTINGS.tz)


@pytest.mark.parametrize(
  ("name", "expected"),
  [
    (FILE_A, True),
    (f"SFT017_13842-13843_{IN_WEEK_TS}.edi", True),  # merged-style invoice numbers still match
    ("SFT017_13842_20250615000000.edi", True),  # Sunday 00:00:00, the first accepted instant
    ("SFT017_13842_20250621235959.edi", True),  # Saturday 23:59:59, the last accepted instant
    ("SFT017_13842_20250614235959.edi", False),  # Saturday of the week before
    ("SFT017_13842_20250622000000.edi", False),  # Sunday of the week after
    ("SFT017_13842.edi", False),  # the export's pre-timestamp shape
    ("SFT017_13842_20250619094646000000.edi", False),  # RYO/SAS-style microseconds
    (f"SFT017_13842_{IN_WEEK_TS}.txt", False),
    (f"SFT018_13842_{IN_WEEK_TS}.edi", False),
  ],
)
def test_filename_pattern(name: str, expected: bool) -> None:
  assert (_pattern(PICKUP_DATE, DROPOFF_DATE).match(name) is not None) is expected


def test_filename_pattern_shifts_back_a_week_for_a_previous_week_entry() -> None:
  """A previous-week entry gets the week before -- Sun 06-08 .. Sat 06-14 -- still exactly one week wide."""
  pattern = _pattern(PICKUP_DATE, DROPOFF_DATE, current_week=False)

  assert pattern.match(LAST_WEEK_FILE) is not None
  assert pattern.match("SFT017_13800_20250608000000.edi") is not None
  assert pattern.match("SFT017_13800_20250614235959.edi") is not None
  assert pattern.match("SFT017_13800_20250607235959.edi") is None
  assert pattern.match(FILE_A) is None


def test_month_straddling_week_admits_cross_products_that_the_diagnostic_reports(
  processor: SFTProcessor, caplog: pytest.LogCaptureFixture
) -> None:
  """The year/month/day alternations match independently. For Sun 2025-08-31 .. Sat 2025-09-06 that is
  `(08|09)` x `(31|01|...|06)`, which also admits 08-01 .. 08-06 and 09-31. Those strays are *accepted* by the
  pickup, and are exactly what the `[OUTSIDE_WEEK_PICKUP]` probe is there to report."""
  wednesday = datetime(2025, 9, 3, 12, 0, tzinfo=SETTINGS.tz)
  pattern = _pattern(wednesday, wednesday)

  assert pattern.match("SFT017_1_20250831120000.edi") is not None
  assert pattern.match("SFT017_1_20250906235959.edi") is not None
  assert pattern.match("SFT017_1_20250807120000.edi") is None
  stray = pattern.match("SFT017_1_20250801120000.edi")
  assert stray is not None

  meta = _meta(wednesday, wednesday)
  with caplog.at_level("WARNING"):
    flagged = processor._warn_if_outside_week(
      meta, SupplierProcessorBase._date_from_filename_match(stray), stray.string, logging.getLogger(__name__)
    )

  assert flagged is True
  assert SupplierProcessorBase.OUTSIDE_WEEK_LOG_TAG in caplog.text


def test_merged_filename_format() -> None:
  assert SFTProcessor.file_name_format.format(customer_id="SFT017", invoice_num="13842-13843") == "SFT017_13842-13843.edi"


def _meta(pickup: datetime, dropoff: datetime, current_week: bool = True, names: dict[int, str] | None = None) -> FileRegisterData:
  return FileRegisterData(
    storenum=17,
    customer_id="SFT017",
    pickup_date=pickup,
    dropoff_date=dropoff,
    file_pattern=_pattern(pickup, dropoff, current_week),
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
    """Per-path mtimes for `listdir`; anything not listed reports `_STALE_MTIME`. SFT is filename-dated, so the
    pickup never consults these -- tests set them deliberately *against* the filename to prove that."""
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
"""Default mtime for the fake server: outside every window this suite uses, which for SFT must not matter."""

LAST_WEEK_MTIME = datetime(2025, 6, 12, 9, 0, tzinfo=SETTINGS.tz)

SECOND_FILE = b"SFT017|13843|49274|6/20/2025 8:00:00 AM\r\nG100137|Dome Pipe Small|4|4|1.000000\r\n"


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


def test_testing_folders_leave_the_vendor_side_alone() -> None:
  """`tests/unit/conftest.py` forces USE_TESTING_FOLDERS=True, so this asserts the live prefixing.

  The pickup folder is the vendor side of the transfer, and `/Testing` is only ever about not writing into
  production holding folders -- SAS and RYO prefix exactly the four holding-side paths and read the vendor's real
  pickup folder. SFT's vendor happens to share the server, which does not make its pickup folder a holding
  folder: prefixing it just points the listing at a directory nobody created, and the run dies on
  `550 Can't check for file existence`.
  """
  assert SFTProcessor.pickup_ftp_folder == PurePosixPath("/SFT_Invoice_Pickup")
  assert SFTProcessor.pickup_archive_ftp_folder == PurePosixPath("/SFT_Invoice_Pickup/Archive")
  # The holding-side folders are still redirected, which is the whole point of the flag.
  for attr in (
    "pre_processing_waiting_folder",
    "pre_processing_archive_folder",
    "post_processing_waiting_folder",
    "destination_ftp_folder",
  ):
    assert getattr(SFTProcessor, attr).is_relative_to("/Testing"), attr


def test_pickup_is_the_base_implementation() -> None:
  """SFT adds no pickup logic of its own: `vendor_ftp` being the holding pool is the whole trick, and the date
  gate is the base's filename branch, fed by `assemble_filename_pattern`. The mtime branch -- and with it
  `_mtime_pickup_window` -- is never reached, so SFT no longer overrides it."""
  assert SFTProcessor._pickup_files is SupplierProcessorBase._pickup_files
  assert SFTProcessor.checks_date_in_filename is True
  assert "_mtime_pickup_window" not in SFTProcessor.__dict__
  assert SFTProcessor.vendor_ftp is SFTProcessor.waiting_ftp


async def test_pickup_transfers_the_files_whose_filename_date_is_in_window(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  """End to end through the inherited `_pickup_files`: pickup folder -> waiting folder on the one pool."""
  # `processor.pickup_ftp_folder`, not a literal: `tests/unit/conftest.py` forces USE_TESTING_FOLDERS=True, so
  # the class attributes carry a "/Testing" prefix.
  pickup_folder = processor.pickup_ftp_folder
  waiting_folder = processor.pre_processing_waiting_folder
  in_window = (pickup_folder / FILE_A).as_posix()
  last_week = (pickup_folder / LAST_WEEK_FILE).as_posix()
  untimestamped = (pickup_folder / "SFT017_13842.edi").as_posix()
  other_customer = (pickup_folder / f"SFT018_13842_{IN_WEEK_TS}.edi").as_posix()

  # mtimes are set against the filenames on purpose: the in-window file reports the stale default, and last
  # week's file reports a fresh one. Neither may matter.
  client = _FakeClient(
    {in_window: SAMPLE_FILE, last_week: OUT_OF_WINDOW_FILE, untimestamped: SAMPLE_FILE, other_customer: SAMPLE_FILE},
    mtimes={last_week: PICKUP_DATE, untimestamped: PICKUP_DATE, other_customer: PICKUP_DATE},
  )
  _wire(processor, client, monkeypatch)
  schedule = _stub_cache(processor)
  archived = _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, DROPOFF_DATE)
  meta._waiting_folder = waiting_folder
  key = _register(processor, meta)

  await processor._pickup_files()

  # Last week's timestamp is outside the pattern's window, the old shape has no timestamp at all, and the other
  # customer's name does not match the entry's pattern.
  assert client.transfers == [(in_window, (waiting_folder / FILE_A).as_posix())]
  assert client.files[(waiting_folder / FILE_A).as_posix()] == SAMPLE_FILE
  # The source outlives the copy: it is the archive wave, after the commit, that removes it.
  assert in_window in client.files
  assert last_week in client.files
  assert untimestamped in client.files
  assert other_customer in client.files
  assert meta.file_names == {0: FILE_A}
  # The invoice number still comes out of the transferred bytes, via the header pattern.
  assert meta.invoice_nums == {0: "13842"}
  assert meta.pickup_success == {0: True}
  assert key in processor._file_waiting_queue
  assert key not in processor._file_pickup_queue
  assert schedule.checked == [(("SFT", 17), "invoice_grabbed")]
  assert archived == [(pickup_folder, FILE_A, processor.pickup_archive_ftp_folder)]


async def test_pickup_with_no_in_window_files_leaves_queue_untouched(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  pickup_folder = processor.pickup_ftp_folder
  client = _FakeClient({(pickup_folder / LAST_WEEK_FILE).as_posix(): OUT_OF_WINDOW_FILE})
  _wire(processor, client, monkeypatch)
  schedule = _stub_cache(processor)
  archived = _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, DROPOFF_DATE)
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
  a_pickup = (pickup_folder / FILE_A).as_posix()
  b_pickup = (pickup_folder / FILE_B).as_posix()
  a_waiting = (waiting_folder / FILE_A).as_posix()
  b_waiting = (waiting_folder / FILE_B).as_posix()

  client = _FakeClient({a_pickup: SAMPLE_FILE, b_pickup: SECOND_FILE}, fail_transfer_paths={b_pickup})
  _wire(processor, client, monkeypatch)
  schedule = _stub_cache(processor)
  archived = _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, DROPOFF_DATE)
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
  assert set(meta.file_names.values()) == {FILE_A, FILE_B}
  assert sorted(meta.invoice_nums.values()) == ["13842", "13843"]
  assert meta.pickup_success == {0: True, 1: True}
  assert key in processor._file_waiting_queue
  assert key not in processor._file_pickup_queue
  assert schedule.checked == [(("SFT", 17), "invoice_grabbed")]
  # A was copied twice; the second copy overwrites the first, which is what makes the re-run safe.
  assert [src for src, _ in client.transfers].count(a_pickup) == 2
  assert sorted(name for _, name, _ in archived) == [FILE_A, FILE_B]


def test_create_new_merged_file(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  # `_create_new_merged_file` calls `Path.without_cwd`, patched onto `pathlib.PurePath` by `Patches.patch_the_monkey`
  # (applied at real-app startup, not by this test suite's conftest); mirror `test_job_ordering.py`'s `ryo` fixture.
  if not hasattr(Path, "without_cwd"):
    Patches.patch_the_monkey()

  waiting_folder = processor.pre_processing_waiting_folder
  client = _FakeClient(
    {
      (waiting_folder / FILE_A).as_posix(): SAMPLE_FILE,
      (waiting_folder / FILE_B).as_posix(): SECOND_FILE,
    }
  )
  _wire(processor, client, monkeypatch)
  meta = _meta(PICKUP_DATE, DROPOFF_DATE, names={0: FILE_A, 1: FILE_B})
  meta._waiting_folder = waiting_folder
  meta._local_copy_folder = processor.local_pre_processing_folder

  new_meta = processor._create_new_merged_file("k", meta)

  # The merged name keeps `file_name_format`'s timestamp-less shape. Nothing re-matches it against the pickup
  # pattern -- that only ever runs over the vendor pickup folder -- so it does not need to carry a timestamp.
  assert new_meta.file_names == {0: "SFT017_13842-13843.edi"}
  assert new_meta.invoice_nums == {0: "13842-13843"}
  merged = (processor.local_post_processing_folder / "SFT017_13842-13843.edi").read_bytes()
  lines = merged.split(b"\r\n")
  # Header keeps the earliest invoice date, verbatim in the source's unpadded format (not re-serialised through
  # strftime, which would emit "06/19/2025 09:46:46 AM"); body is the concatenation of both bodies.
  assert lines[0] == b"SFT017|13842-13843|49273|6/19/2025 9:46:46 AM"
  assert lines[1] == b"850661003182|Boveda 62%|5|5|4.930000"
  assert lines[2] == b"G100137|Dome Pipe Small|4|4|1.000000"


def test_sft_is_an_expected_supplier(monkeypatch: pytest.MonkeyPatch) -> None:
  # startup.py builds `supplier_register` by probing connections at import time; stub that out.
  monkeypatch.setattr(SFTProcessor, "check_connections", classmethod(lambda cls: False))
  # First party imports
  from scheduled_invoice_processor.startup import expected_suppliers

  assert expected_suppliers[SuppliersEnum.SFT] is SFTProcessor


# --- Strict one-week pickup window -------------------------------------------------------------------------
# The entry's week for PICKUP_DATE (Wed 2025-06-18) / dropoff 2025-06-19 is Sun 06-15 .. Sat 06-21. The window
# is the filename pattern itself: a name outside it never matches, so the base never has anything to accept.


@pytest.mark.parametrize(
  ("timestamp", "accepted"),
  [
    ("20250614235959", False),  # Saturday of the week before
    ("20250615000000", True),  # Sunday 00:00, the first accepted instant
    ("20250621235959", True),  # Saturday night, the last accepted instant
    ("20250622000000", False),  # Sunday of the week after
  ],
)
async def test_pickup_accepts_only_the_current_weeks_filenames(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None, timestamp: str, accepted: bool
) -> None:
  """End to end through the inherited `_pickup_files`, one candidate at a time, at each window edge. Every
  candidate carries an in-week mtime, which must not rescue an out-of-week name."""
  pickup_folder = processor.pickup_ftp_folder
  waiting_folder = processor.pre_processing_waiting_folder
  candidate = (pickup_folder / f"SFT017_13842_{timestamp}.edi").as_posix()

  client = _FakeClient({candidate: SAMPLE_FILE}, mtimes={candidate: PICKUP_DATE})
  _wire(processor, client, monkeypatch)
  _stub_cache(processor)
  _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, DROPOFF_DATE)
  meta._waiting_folder = waiting_folder
  key = _register(processor, meta)

  await processor._pickup_files()

  assert bool(client.transfers) is accepted
  assert (key in processor._file_waiting_queue) is accepted


@pytest.mark.parametrize(
  ("name", "mtime", "accepted"),
  [
    (FILE_A, LAST_WEEK_MTIME, True),  # in-week name, stale mtime: the name wins
    (FILE_A, _STALE_MTIME, True),  # in-week name, an mtime from 2000: still the name
    (LAST_WEEK_FILE, PICKUP_DATE, False),  # last week's name, fresh mtime: the name still wins
  ],
)
async def test_pickup_dates_by_filename_not_mtime(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None, *, name: str, mtime: datetime, accepted: bool
) -> None:
  """The reason for the filename timestamp: an mtime on the warehouse server is whatever the last person to touch
  the file left it as, so it must carry no weight at all."""
  pickup_folder = processor.pickup_ftp_folder
  candidate = (pickup_folder / name).as_posix()

  client = _FakeClient({candidate: SAMPLE_FILE}, mtimes={candidate: mtime})
  _wire(processor, client, monkeypatch)
  _stub_cache(processor)
  _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, DROPOFF_DATE)
  meta._waiting_folder = processor.pre_processing_waiting_folder
  key = _register(processor, meta)

  await processor._pickup_files()

  assert bool(client.transfers) is accepted
  assert (key in processor._file_waiting_queue) is accepted


async def test_pickup_leaves_last_weeks_file_in_the_vendor_folder(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None
) -> None:
  """The regression: a file the warehouse exported last week must not be collected, copied, or archived."""
  pickup_folder = processor.pickup_ftp_folder
  waiting_folder = processor.pre_processing_waiting_folder
  this_week = (pickup_folder / FILE_A).as_posix()
  last_week = (pickup_folder / LAST_WEEK_FILE).as_posix()

  client = _FakeClient({this_week: SAMPLE_FILE, last_week: OUT_OF_WINDOW_FILE})
  _wire(processor, client, monkeypatch)
  _stub_cache(processor)
  archived = _record_archives(monkeypatch)
  meta = _meta(PICKUP_DATE, DROPOFF_DATE)
  meta._waiting_folder = waiting_folder
  _register(processor, meta)

  await processor._pickup_files()

  assert client.transfers == [(this_week, (waiting_folder / FILE_A).as_posix())]
  assert meta.file_names == {0: FILE_A}
  assert (waiting_folder / LAST_WEEK_FILE).as_posix() not in client.files
  assert archived == [(pickup_folder, FILE_A, processor.pickup_archive_ftp_folder)]


async def test_pickup_never_warns_about_an_outside_week_file(
  processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch, frozen_now: None, caplog: pytest.LogCaptureFixture
) -> None:
  """The probe only reports *accepted* files, and an out-of-week name is never accepted -- it never matches."""
  pickup_folder = processor.pickup_ftp_folder
  last_week = (pickup_folder / LAST_WEEK_FILE).as_posix()

  client = _FakeClient({last_week: OUT_OF_WINDOW_FILE}, mtimes={last_week: PICKUP_DATE})
  _wire(processor, client, monkeypatch)
  _stub_cache(processor)
  _record_archives(monkeypatch)
  _register(processor, _meta(PICKUP_DATE, DROPOFF_DATE))

  with caplog.at_level("WARNING"):
    await processor._pickup_files()

  assert SupplierProcessorBase.OUTSIDE_WEEK_LOG_TAG not in caplog.text
