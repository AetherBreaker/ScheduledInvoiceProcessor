# SFT Warehouse Supplier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `SFT` supplier whose invoices already live on the SFT FTP server, so pickup is a header-checked rename instead of a cross-server copy, with RYO-style invoice merging.

**Architecture:** New `SFTProcessor(SupplierProcessorBase)` in `suppliers/sft.py`. `vendor_ftp` aliases the shared `waiting_ftp` adapter. `_pickup_files` is overridden to download each filename-matched candidate, read the header line, keep only files whose header date is inside the schedule window, then rename them into the waiting folder. Merge logic is copied from `RYOProcessor` with the file timestamp taken from the header date (SFT filenames carry none).

**Tech Stack:** Python 3.13, pydantic dataclasses, `aeth_ext.ftp` adapters, pytest (`asyncio_mode = auto`), `uv run pytest`.

**Spec:** `docs/superpowers/specs/2026-08-26-sft-supplier-design.md`

## Global Constraints

- Two-space indentation, ruff/pyright clean (project config in `pyproject.toml`); run `uv run ruff check src tests` before each commit.
- Run tests with `uv run pytest tests/unit -q` (unit conftest bootstraps a network-free environment; no real FTP is needed).
- No new credentials file and no `Settings` change — `vendor_ftp = SupplierProcessorBase.waiting_ftp`.
- FTP folder paths are **placeholders** under a loud banner comment; they must not be silently "fixed".
- Header regex must accept the non-padded sample `SFT017|13842|49273|6/19/2025 9:46:46 AM`.
- Merge logic is **copied** from RYO, not extracted into a mixin.
- Commit messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

## File map

| File | Responsibility |
|---|---|
| `src/scheduled_invoice_processor/typing_custom/enums.py` | Add `SuppliersEnum.SFT`. |
| `src/scheduled_invoice_processor/suppliers/sft.py` (new) | `SFTProcessor`: attributes, filename pattern, header parsing, same-server transfer, `_pickup_files` override, merge logic, testing-folder prefixing, debug `main()`. |
| `src/scheduled_invoice_processor/startup.py` | Register `SFTProcessor` in `expected_suppliers`. |
| `tests/unit/test_sft_processor.py` (new) | Fakes (`_FakeClient`, `_FakePool`, `_FakePbar`), `processor` fixture, all SFT unit tests. Grows across Tasks 1–5. |
| `tests/e2e/test_sft_cycle.py` (new) | Skipped cycle test; unskip when real folder paths exist. |

---

### Task 1: Enum + `SFTProcessor` skeleton with filename and header patterns

**Files:**
- Modify: `src/scheduled_invoice_processor/typing_custom/enums.py:8-11`
- Create: `src/scheduled_invoice_processor/suppliers/sft.py`
- Create: `tests/unit/test_sft_processor.py`

**Interfaces:**
- Produces: `SFTProcessor` with class attrs `invoice_num_pattern: Pattern[str]`, `header_date_format = "%m/%d/%Y %I:%M:%S %p"`, `header_format`, `file_name_format`, six folder attrs, and `assemble_filename_pattern(customer_id, start_date, end_date, current_week) -> Pattern[str]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_sft_processor.py`:

```python
"""Unit tests for the SFT warehouse supplier (same-server pickup, header-dated window, RYO-style merge)."""

# This file tests private methods by design.
# pyright: reportPrivateUsage=false

# Standard library imports
import atexit
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
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
from scheduled_invoice_processor.suppliers.sft import SFTProcessor
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Generator, Iterator

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov`
Expected: collection error — `ModuleNotFoundError: No module named 'scheduled_invoice_processor.suppliers.sft'`.

- [ ] **Step 3: Add the enum member**

In `src/scheduled_invoice_processor/typing_custom/enums.py` change:

```python
class SuppliersEnum(StrEnum):
  SAS = "SAS"
  RYO = "RYO"
  COREMARK = "COREMARK"
  SFT = "SFT"
```

- [ ] **Step 4: Create the skeleton `sft.py`**

Create `src/scheduled_invoice_processor/suppliers/sft.py`:

```python
# Standard library imports
from contextvars import ContextVar
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

# Local folder imports
from . import SupplierProcessorBase

if TYPE_CHECKING:
  # Standard library imports
  from datetime import datetime
  from re import Pattern

  # First party imports
  from scheduled_invoice_processor.typing_custom import CustomerID

logger = getLogger(__name__)


class SFTProcessor(SupplierProcessorBase):
  """SFT's own warehouse. The vendor side *is* the holding FTP, so pickup is a header-checked rename.

  Date windows are decided from the header line's date, never the filename (it has none) and never mtime (a
  human touching the file poisons it).
  """

  # Same server as the holding FTP: one adapter, no separate credentials.
  vendor_ftp = SupplierProcessorBase.waiting_ftp

  queue_backup_prefix: str = "sft"

  supplier_name: SuppliersEnum = SuppliersEnum.SFT

  # Header: SFT017|13842|49273|6/19/2025 9:46:46 AM  (month/day/hour are NOT zero-padded)
  invoice_num_pattern: Pattern[str] = compile(  # pyright: ignore[reportIncompatibleVariableOverride]
    r"^(?P<customer_num>[^|]+)\|"
    r"(?P<invoice_num>\d+)\|"
    r"(?P<po_num>[^|]*)\|"
    r"(?P<invoice_date>\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [AP]M)$"
  )
  header_date_format = "%m/%d/%Y %I:%M:%S %p"

  header_format = "{customer_num}|{invoice_num}|{po_num}|{invoice_date}"
  file_name_format = "{customer_id}_{invoice_num}.edi"

  # The override of `_pickup_files` decides the window from the header; this flag only matters to the base
  # implementation, which SFT does not use for pickup.
  checks_date_in_filename: bool = False

  # ===========================================================================================================
  # !!! PLACEHOLDER FTP PATHS — MUST BE REPLACED BEFORE THIS SUPPLIER IS ENABLED IN PRODUCTION !!!
  # The real pickup/dropoff locations on the SFT FTP server have not been decided yet. Every path below is a
  # stand-in. Search for "TODO_SFT" to find them all. Do NOT ship with these values.
  # ===========================================================================================================
  pickup_ftp_folder = PurePosixPath("/TODO_SFT/Pickup")
  pickup_archive_ftp_folder = PurePosixPath("/TODO_SFT/Pickup/Archive")
  pre_processing_waiting_folder = PurePosixPath("/TODO_SFT/Waiting")
  pre_processing_archive_folder = PurePosixPath("/TODO_SFT/Waiting/Archive")
  post_processing_waiting_folder = PurePosixPath("/TODO_SFT/Processed")
  destination_ftp_folder = PurePosixPath("/TODO_SFT/Destination")
  # ===========================================================================================================

  identifier_prefix = "SFT"
  log_file_loc = SupplierProcessorBase.log_file_loc / supplier_name
  ctx_var_identifier = ContextVar("sft_log_identifier", default=None)
  ctx_var_log_loc = ContextVar("sft_log_loc", default=log_file_loc)

  def __post_init__(self) -> None:
    self.local_pre_processing_folder = self.job_holding_folder / "SFT_files" / "pre_processing"
    self.local_post_processing_folder = self.job_holding_folder / "SFT_files" / "post_processing"
    self.local_pre_processing_folder.mkdir(exist_ok=True, parents=True)
    self.local_post_processing_folder.mkdir(exist_ok=True, parents=True)

  @override
  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern[str]:
    # No date in the filename; `[\d\-]+` so a merged `SFT017_13842-13843.edi` still matches.
    return compile(rf"^{customer_id}_(?P<invoice_num>[\d\-]+)\.edi$")


if __debug__ and SETTINGS.use_testing_folders:
  # Every folder lives on the SFT server here (SAS/RYO only prefix the four holding-side folders because their
  # pickup folders are on the vendor's server).
  for attr_name in [
    "pickup_ftp_folder",
    "pickup_archive_ftp_folder",
    "pre_processing_waiting_folder",
    "pre_processing_archive_folder",
    "post_processing_waiting_folder",
    "destination_ftp_folder",
  ]:
    orig_attr: PurePosixPath = getattr(SFTProcessor, attr_name)
    new_val = PurePosixPath("/Testing") / orig_attr.relative_to("/")
    setattr(SFTProcessor, attr_name, new_val)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov`
Expected: all PASS.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check src tests
git add src/scheduled_invoice_processor/typing_custom/enums.py src/scheduled_invoice_processor/suppliers/sft.py tests/unit/test_sft_processor.py
git commit -m "feat(sft): add SFTProcessor skeleton with header and filename patterns

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Header date parsing and window check

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/sft.py`
- Modify: `tests/unit/test_sft_processor.py`

**Interfaces:**
- Produces:
  - `SFTProcessor.parse_header_date(self, first_line: str) -> datetime | None` — tz-aware in `SETTINGS.tz`, `None` if the line does not match `invoice_num_pattern` or the date fails to parse.
  - `SFTProcessor.header_date_in_window(self, file_meta: FileRegisterData, header_date: datetime) -> bool` — the same Sun–Sat window arithmetic as the base class's mtime branch.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_sft_processor.py`:

```python
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
  assert parsed.tzinfo is SETTINGS.tz


def test_parse_header_date_rejects_non_header(processor: SFTProcessor) -> None:
  assert processor.parse_header_date("850661003182|Boveda 62%|5|5|4.930000") is None


def test_parse_header_date_rejects_impossible_date(processor: SFTProcessor) -> None:
  assert processor.parse_header_date("SFT017|1|1|13/45/2025 9:46:46 AM") is None


def test_header_date_in_window_current_week(processor: SFTProcessor) -> None:
  # A Wednesday. Current-week window: previous Sunday 00:00 through Saturday 23:59:59.
  pickup = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
  meta = _meta(pickup, pickup + timedelta(days=1))
  assert processor.header_date_in_window(meta, datetime(2025, 6, 19, 9, 46, 46, tzinfo=SETTINGS.tz))
  assert processor.header_date_in_window(meta, datetime(2025, 6, 15, 0, 0, 0, tzinfo=SETTINGS.tz))
  assert not processor.header_date_in_window(meta, datetime(2025, 6, 14, 23, 59, 59, tzinfo=SETTINGS.tz))
  assert not processor.header_date_in_window(meta, datetime(2025, 6, 22, 0, 0, 0, tzinfo=SETTINGS.tz))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov -k "header_date"`
Expected: FAIL with `AttributeError: 'SFTProcessor' object has no attribute 'parse_header_date'`.

- [ ] **Step 3: Implement the two helpers**

In `sft.py`, add to the imports:

```python
from datetime import datetime  # move out of TYPE_CHECKING — it is used at runtime now

# Third party imports
from dateutil.relativedelta import SA, SU, relativedelta
```

(remove `from datetime import datetime` from the `TYPE_CHECKING` block, and add `from .file_register_data import FileRegisterData` to the `TYPE_CHECKING` block.)

Add these methods to `SFTProcessor` after `assemble_filename_pattern`:

```python
  def parse_header_date(self, first_line: str) -> datetime | None:
    """Header date localised to `SETTINGS.tz` (the header carries no offset), or None if the line is not a header."""
    match = self.invoice_num_pattern.match(first_line.strip())
    if match is None:
      return None
    try:
      return datetime.strptime(match.group("invoice_date"), self.header_date_format).replace(tzinfo=SETTINGS.tz)  # noqa: DTZ007
    except ValueError:
      return None

  def header_date_in_window(self, file_meta: FileRegisterData, header_date: datetime) -> bool:
    """Same Sun-Sat window the base class applies to mtimes, applied to the header date instead."""
    current_week = file_meta.current_week
    start_date = (
      file_meta.pickup_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0, microsecond=0)
    ) - relativedelta(weeks=1 if current_week else 0)
    end_date = (
      file_meta.dropoff_date + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59, microsecond=999999)
    ) - relativedelta(weeks=0 if current_week else 1)
    return start_date <= header_date < end_date
```

Note: `FileRegisterData.current_week` is a property that returns `False` once the window has passed, which is the behaviour the base class relies on too — do not "simplify" it to the `_current_week` field.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov`
Expected: all PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check src tests
git add src/scheduled_invoice_processor/suppliers/sft.py tests/unit/test_sft_processor.py
git commit -m "feat(sft): parse header date and decide the pickup window from it

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Same-server transfer (rename + invoice number from downloaded bytes)

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/sft.py`
- Modify: `tests/unit/test_sft_processor.py`

**Interfaces:**
- Consumes: `SupplierProcessorBase._already_moved(client, send_path, recv_path, adapted_logger)`, `extract_invoice_num(bytestream, file_meta, idx, adapted_logger)`, `_advance_progress(move_files_task)`.
- Produces: `SFTProcessor._transfer_file_same_server(self, send_path, recv_path, move_files_task, file_meta, idx, key, file_bytes: bytes, adapted_logger=None, log_action_handler=None) -> bool`. Same contract as `_transfer_file_vend_to_main`: sets `file_meta.pickup_success[idx]`, advances progress once, calls `log_action_handler(key, StatusCode, file_meta)`. Never raises.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_sft_processor.py`:

```python
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

  def download_file(self, remote_path: str, callback, task_msg: str = "") -> None:  # noqa: ANN001
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
  assert log == ["success"]
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
  assert log == ["success"]


def test_same_server_transfer_failure_is_recorded_not_raised(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  client = _FakeClient({PICKUP.as_posix(): SAMPLE_FILE}, fail_rename=True)
  _wire(processor, client, monkeypatch)
  now = datetime.now(SETTINGS.tz)
  meta = _meta(now, now, names={0: "SFT017_13842.edi"})
  log: list = []

  assert _transfer(processor, meta, log) is False
  assert meta.pickup_success == {0: False}
  assert log == ["failure"]
  assert processor.pbar.advances == 1  # pyright: ignore[reportAttributeAccessIssue]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov -k same_server`
Expected: FAIL with `AttributeError: ... has no attribute '_transfer_file_same_server'`.

- [ ] **Step 3: Implement `_transfer_file_same_server`**

Add to `sft.py` imports:

```python
from ftplib import all_errors
from io import BytesIO
```

and to the `TYPE_CHECKING` block:

```python
  from logging import LoggerAdapter
  from typing import Any

  from aeth_ext.rich.progress import TaskID
  from scheduled_invoice_processor.typing_custom.enums import StatusCode  # noqa: F401  (runtime import below)

  # Local folder imports
  from .log_action import LogActionHandlerType
```

Add the runtime import `from scheduled_invoice_processor.typing_custom.enums import StatusCode, SuppliersEnum` (replace the existing `SuppliersEnum`-only import; drop the `TYPE_CHECKING` line for `StatusCode` if ruff complains about duplication).

Add the method to `SFTProcessor`:

```python
  def _transfer_file_same_server(  # noqa: PLR0917
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: TaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    file_bytes: bytes,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ) -> bool:
    """Pickup for a vendor that shares the holding FTP: rename in place, then take the invoice number from the
    bytes already downloaded for the header check. Idempotent like `_transfer_file_main_to_main`: a rename that
    already happened on an earlier, interrupted run is reported as success. Never raises; the outcome lands in
    `file_meta.pickup_success[idx]` and the log-action handler."""
    local_logger = adapted_logger or logger
    success = False
    if self.errored:
      local_logger.warning("%s: Disabled due to error state. Skipping same-server transfer", self.__class__.__name__)
      file_meta.pickup_success[idx] = False
      self._advance_progress(move_files_task)
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)
      return False
    try:
      with self.vendor_ftp.start_session() as client:
        try:
          client.rename(send_path.as_posix(), recv_path.as_posix())
        except (*all_errors, OSError):
          if self._already_moved(client, send_path, recv_path, local_logger):
            local_logger.info(
              "%s: [yellow]%s[/] was already moved to [yellow]%s[/] by an earlier run; treating as success",
              self.__class__.__name__,
              send_path,
              recv_path,
              extra={"markup": True},
            )
            success = True
          else:
            raise
        else:
          try:
            client.get_size(recv_path.as_posix())
            success = True
            local_logger.info(
              "%s: Moved [yellow]%s[/] to [yellow]%s[/]",
              self.__class__.__name__,
              send_path,
              recv_path,
              extra={"markup": True},
            )
          except (*all_errors, OSError) as e:
            local_logger.warning("%s: Failed to verify move of %s", self.__class__.__name__, send_path.name, exc_info=e)
      file_meta.pickup_success[idx] = success
      if success:
        self.extract_invoice_num(BytesIO(file_bytes), file_meta, idx, adapted_logger=adapted_logger)
      self._advance_progress(move_files_task)
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.SUCCESS if success else StatusCode.FAILURE, file_meta)
    except Exception:
      success = False
      local_logger.exception(
        "%s: Error moving\n[yellow]%s[/] to\n[yellow]%s[/]",
        self.__class__.__name__,
        send_path,
        recv_path,
        extra={"markup": True},
      )
      file_meta.pickup_success[idx] = False
      self._advance_progress(move_files_task)
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)
    return success
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov`
Expected: all PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check src tests
git add src/scheduled_invoice_processor/suppliers/sft.py tests/unit/test_sft_processor.py
git commit -m "feat(sft): same-server pickup transfer via idempotent rename

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `_pickup_files` override (download, header filter, rename, commit, archive)

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/sft.py`
- Modify: `tests/unit/test_sft_processor.py`

**Interfaces:**
- Consumes: `parse_header_date`, `header_date_in_window`, `_transfer_file_same_server` (Tasks 2–3); base `_vendor_archive_file`, `_persist_queues`, `_file_pickup_queue`, `_file_waiting_queue`, `cache.schedule.check_box`, `DatabaseScheduleColumns.invoice_grabbed`.
- Produces: `SFTProcessor._pickup_files(self, adapted_logger=None, log_action_handler=None)` decorated exactly like the base; `SFTProcessor._download_candidate(self, remote_path: PurePosixPath, adapted_logger=None) -> bytes | None`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_sft_processor.py`:

```python
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


async def test_pickup_keeps_only_in_window_files(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  # Header date in the sample is 2025-06-19 (Thursday). Pickup on Wednesday 2025-06-18, current week.
  pickup = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
  client = _FakeClient(
    {
      "/TODO_SFT/Pickup/SFT017_13842.edi": SAMPLE_FILE,
      "/TODO_SFT/Pickup/SFT017_13900.edi": OUT_OF_WINDOW_FILE,
      "/TODO_SFT/Pickup/SFT017_13901.edi": NO_HEADER_FILE,
      "/TODO_SFT/Pickup/SFT018_13842.edi": SAMPLE_FILE,
    }
  )
  _wire(processor, client, monkeypatch)
  schedule = _FakeSchedule()
  processor.cache = SimpleNamespace(schedule=schedule, prev_week_schedule=schedule)  # pyright: ignore[reportAttributeAccessIssue]
  meta = _meta(pickup, pickup + timedelta(days=1))
  key = _register(processor, meta)

  await processor._pickup_files()

  # Only the in-window SFT017 file moved; the others stay exactly where they were.
  assert "/TODO_SFT/Waiting/SFT017_13842.edi" in client.files
  assert "/TODO_SFT/Pickup/SFT017_13900.edi" in client.files
  assert "/TODO_SFT/Pickup/SFT017_13901.edi" in client.files
  assert "/TODO_SFT/Pickup/SFT018_13842.edi" in client.files
  # Non-matching filename was never downloaded; the two SFT017 rejects were (header needed to decide).
  assert "/TODO_SFT/Pickup/SFT018_13842.edi" not in client.downloads
  assert meta.file_names == {0: "SFT017_13842.edi"}
  assert meta.invoice_nums == {0: "13842"}
  assert key in processor._file_waiting_queue
  assert key not in processor._file_pickup_queue
  assert schedule.checked == [(("SFT", 17), "invoice_grabbed")]
  # The rename is the removal: nothing is left in the pickup folder and nothing is written to the archive.
  assert "/TODO_SFT/Pickup/SFT017_13842.edi" not in client.files
  assert "/TODO_SFT/Pickup/Archive/SFT017_13842.edi" not in client.files


async def test_pickup_with_no_in_window_files_leaves_queue_untouched(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  pickup = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)
  client = _FakeClient({"/TODO_SFT/Pickup/SFT017_13900.edi": OUT_OF_WINDOW_FILE})
  _wire(processor, client, monkeypatch)
  schedule = _FakeSchedule()
  processor.cache = SimpleNamespace(schedule=schedule, prev_week_schedule=schedule)  # pyright: ignore[reportAttributeAccessIssue]
  meta = _meta(pickup, pickup + timedelta(days=1))
  key = _register(processor, meta)

  await processor._pickup_files()

  assert key in processor._file_pickup_queue
  assert client.renames == []
  assert schedule.checked == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov -k pickup`
Expected: FAIL — the base `_pickup_files` runs and calls `client.transfer_file`, which `_FakeClient` lacks (`AttributeError`), or asserts on `client.files` fail.

- [ ] **Step 3: Implement the override**

Add to `sft.py` imports:

```python
from asyncio import gather, to_thread
from time import sleep

from scheduled_invoice_processor.logging_config import add_log_context
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

from .log_action import log_actions
```

(Check `DatabaseScheduleColumns` is importable from `scheduled_invoice_processor.typing_custom.dataframe_column_names`; it is what `suppliers/__init__.py` uses — copy its import line verbatim.)

Add these methods to `SFTProcessor`:

```python
  def _download_candidate(self, remote_path: PurePosixPath, adapted_logger: LoggerAdapter[Any] | None = None) -> bytes | None:
    """Fetch a filename-matched file so its header can be inspected. Transient errors retry with the base
    backoff; anything else is logged and the file is skipped for this run (it stays in the pickup folder)."""
    local_logger = adapted_logger or logger
    for attempt in range(1, self._transient_transfer_retries + 2):
      try:
        buffer = BytesIO()
        with self.vendor_ftp.start_session() as client:
          client.download_file(remote_path.as_posix(), callback=buffer.write, task_msg=f"Reading {remote_path.name} (Attempt {attempt})")
        return buffer.getvalue()
      except Exception as e:
        if self._is_transient_transfer_error(e) and attempt <= self._transient_transfer_retries:
          backoff_seconds = 2 ** (attempt - 1)
          local_logger.warning(
            "%s: Transient read failure for %s on attempt %s of %s. Retrying in %s seconds",
            self.__class__.__name__,
            remote_path.name,
            attempt,
            self._transient_transfer_retries + 1,
            backoff_seconds,
            exc_info=e,
          )
          sleep(backoff_seconds)
          continue
        local_logger.exception("%s: Could not read %s for header inspection; skipping this run", self.__class__.__name__, remote_path)
        return None
    return None

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP)
  @override
  async def _pickup_files(  # noqa: C901, PLR0912
    self,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    """Copy of the base pickup with two substitutions: the date window is decided from each candidate's header
    line (downloaded up front), and the transfer is a same-server rename."""
    local_logger = adapted_logger or logger
    if not self._file_pickup_queue:
      return
    if not self.vendor_ftp.test_connection():
      local_logger.warning("%s: Aborting pickup_files due to offline FTP server", self.__class__.__name__)
      return

    async with self._lock:
      with self.vendor_ftp.start_session() as client:
        remote_names = [entry.filename for entry in client.listdir(self.pickup_ftp_folder.as_posix())]

      # 1. Filename match, then download every candidate once; the bytes serve both the header check and the
      #    invoice-number extraction after the rename.
      candidates: dict[str, list[str]] = {}
      for key, file_meta in self._file_pickup_queue.items():
        names = [name for name in remote_names if file_meta.file_pattern.match(name)]
        if names:
          candidates[key] = names
        else:
          local_logger.warning(
            "%s: %s: No files matched with pattern %s", self.__class__.__name__, key, file_meta.file_pattern.pattern
          )

      unique_names = sorted({name for names in candidates.values() for name in names})
      downloaded = dict(
        zip(
          unique_names,
          await gather(*(to_thread(self._download_candidate, self.pickup_ftp_folder / name, adapted_logger) for name in unique_names)),
          strict=True,
        )
      )

      # 2. Keep only files whose header date is inside the entry's window.
      items_to_dl: dict[str, FileRegisterData] = {}
      kept_bytes: dict[str, dict[int, bytes]] = {}
      for key, names in candidates.items():
        file_meta = self._file_pickup_queue[key]
        kept: list[tuple[str, bytes]] = []
        for name in names:
          data = downloaded.get(name)
          if data is None:
            continue
          first_line = data.splitlines()[0].decode("utf-8", errors="ignore") if data else ""
          header_date = self.parse_header_date(first_line)
          if header_date is None:
            local_logger.warning("%s: %s: %s has no parseable header line; leaving in place", self.__class__.__name__, key, name)
            continue
          if not self.header_date_in_window(file_meta, header_date):
            local_logger.warning(
              "%s: %s: %s header date %s is outside the pickup window; leaving in place",
              self.__class__.__name__,
              key,
              name,
              header_date.isoformat(),
            )
            continue
          kept.append((name, data))

        if kept:
          file_meta.file_names = {idx: name for idx, (name, _) in enumerate(kept)}
          file_meta.pickup_success = {}
          file_meta.invoice_nums = {}
          items_to_dl[key] = file_meta
          kept_bytes[key] = {idx: data for idx, (_, data) in enumerate(kept)}
          if log_action_handler is not None:
            log_action_handler(key, StatusCode.UNKNOWN, file_meta)
          local_logger.info("%s: %s: Matched %s files for: %s", self.__class__.__name__, key, len(kept), file_meta.storenum)

      # 3. Rename kept files into the waiting folder.
      with self.pbar.add_task("Transferring Files", total=sum(len(v.file_names) for v in items_to_dl.values())) as move_files_task:
        futures = []
        for key, file_meta in items_to_dl.items():
          remote_file_locs = file_meta.remote_file_locs
          futures.extend(
            to_thread(
              self._transfer_file_same_server,
              send_path=(self.pickup_ftp_folder / filename),
              recv_path=remote_file_locs[idx],
              move_files_task=move_files_task,
              file_meta=file_meta,
              idx=idx,
              key=key,
              file_bytes=kept_bytes[key][idx],
              adapted_logger=adapted_logger,
              log_action_handler=log_action_handler,
            )
            for idx, filename in file_meta.file_names.items()
          )
        await gather(*futures)

      # 4. Commit first, clean up last (same ordering rationale as the base class).
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if file_meta.pickup_success and all(file_meta.pickup_success.values()):
          items_to_advance[key] = file_meta
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule
          local_logger.info(
            "%s: %s: Checking off %s_%s invoice_grabbed", self.__class__.__name__, key, self.supplier_name, file_meta.storenum
          )
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

      for key, item in items_to_advance.items():
        self._file_waiting_queue[key] = item
        self._file_pickup_queue.pop(key)
        local_logger.info("%s: %s: Moved %s to waiting queue", self.__class__.__name__, key, item.storenum)

      persisted = self._persist_queues()

      if not persisted:
        local_logger.warning(
          "%s: Queue backup could not be written; a restart from the stale backup will re-match the moved files "
          "from the waiting folder via the idempotent rename",
          self.__class__.__name__,
        )
      # No vendor-side archive step: the rename *is* the removal from the pickup folder. `pickup_archive_ftp_folder`
      # stays declared to satisfy the base class's attribute contract but SFT never writes to it.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov`
Expected: all PASS. If `_FakeClient.listdir` is consumed while `rename` mutates `files`, the `list(self.files)` snapshot in the fake handles it.

- [ ] **Step 5: Run the whole unit suite**

Run: `uv run pytest tests/unit -q --no-cov`
Expected: all PASS (in particular `test_queue_persistence.py` and `test_transfer_idempotency.py` still green — the singleton drop in the fixture must not leak).

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check src tests
git add src/scheduled_invoice_processor/suppliers/sft.py tests/unit/test_sft_processor.py
git commit -m "feat(sft): header-gated same-server pickup

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Merge logic (copied from RYO, timestamp from header)

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/sft.py`
- Modify: `tests/unit/test_sft_processor.py`
- Reference (read only): `src/scheduled_invoice_processor/suppliers/ryo.py:146-446`

**Interfaces:**
- Consumes: `parse_header_date` (Task 2), `FileRegisterData`, base `_middle_archive_file`, `_persist_lock`, `_file_preprocess_queue`, `_file_dropoff_queue`.
- Produces: `_preprocess_files`, `_preprocess_off_thread`, `_create_new_merged_file(key, old_file_meta, adapted_logger) -> FileRegisterData` on `SFTProcessor`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_sft_processor.py`:

```python
SECOND_FILE = b"SFT017|13843|49274|6/20/2025 8:00:00 AM\r\nG100137|Dome Pipe Small|4|4|1.000000\r\n"


def test_create_new_merged_file(processor: SFTProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  client = _FakeClient(
    {
      "/TODO_SFT/Waiting/SFT017_13842.edi": SAMPLE_FILE,
      "/TODO_SFT/Waiting/SFT017_13843.edi": SECOND_FILE,
    }
  )
  _wire(processor, client, monkeypatch)
  now = datetime.now(SETTINGS.tz)
  meta = _meta(now, now, names={0: "SFT017_13842.edi", 1: "SFT017_13843.edi"})
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov -k merged`
Expected: FAIL with `AttributeError: ... has no attribute '_create_new_merged_file'`.

- [ ] **Step 3: Copy the three merge methods from RYO**

Copy `_preprocess_files`, `_preprocess_off_thread`, and `_create_new_merged_file` from `ryo.py` lines 146–446 into `SFTProcessor` **verbatim**, including the two decorators on `_preprocess_files`:

```python
  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED, log_subfolder=LogActionEnum.FILE_PREPROCESSED)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED)
  async def _preprocess_files(  # noqa: C901
```

Add the imports RYO needs that `sft.py` does not yet have:

```python
from asyncio import as_completed, gather, to_thread
from hashlib import file_digest

from .file_register_data import FileRegisterData   # runtime import now (constructed in _create_new_merged_file)
```

and in `TYPE_CHECKING`: `from collections.abc import Coroutine`, `from pathlib import Path`, `from scheduled_invoice_processor.typing_custom import CustomerID, SupplierQueueKey`.

Then make exactly these three edits inside the copied `_create_new_merged_file`:

**(a)** RYO derives the per-file timestamp from the filename. SFT filenames have none. Replace

```python
        filename_match = old_file_meta.file_pattern.match(file.name)
        assert filename_match is not None
        file_extracted_timestamp = filename_match.group("timestamp")

        file_timestamp = datetime.strptime(file_extracted_timestamp, "%Y%m%d%H%M%S%f")  # noqa: DTZ007

        match = self.invoice_num_pattern.match(first_line)
```

with

```python
        match = self.invoice_num_pattern.match(first_line)
```

and delete the line `found_timestamps.add(file_timestamp)` and the `found_timestamps = set()` declaration — nothing consumes them once edit (c) removes the timestamp from the output filename.

**(b)** The duplicate-hash check in RYO compares a digest *object* against a set of hex *strings*, so it never fires. In the SFT copy, write it correctly:

```python
      with file.open("rb") as fb:
        digest = file_digest(fb, "sha256").hexdigest()
        if digest in file_hashes:
          local_logger.error("%s: %s: Duplicate file hash found for file %s: %s", self.__class__.__name__, key, file.name, digest)
          continue
        file_hashes.add(digest)
```

**(c)** The output filename has no timestamp. Replace

```python
    new_file_name = self.file_name_format.format(
      customer_id=found_values["customer_num"] or "unknown_customer",
      invoice_num=invoice_num_result,
      timestamp=max(found_timestamps).strftime("%Y%m%d%H%M%S%f"),
    )
```

with

```python
    new_file_name = self.file_name_format.format(
      customer_id=found_values["customer_num"] or "unknown_customer",
      invoice_num=invoice_num_result,
    )
```

Keep the `header_invoiced_dates` handling: it parses with `"%m/%d/%Y %I:%M:%S %p"`, which accepts the non-padded input, and re-emits zero-padded, which is what the test asserts.

Everything else — `_preprocess_files`, `_preprocess_off_thread`, upload → commit → archive → local cleanup ordering — stays byte-for-byte RYO.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov`
Expected: all PASS.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check src tests
git add src/scheduled_invoice_processor/suppliers/sft.py tests/unit/test_sft_processor.py
git commit -m "feat(sft): RYO-style invoice merging with header-derived timestamps

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Wire into startup, debug `main()`, skipped e2e test

**Files:**
- Modify: `src/scheduled_invoice_processor/startup.py:23-43`
- Modify: `src/scheduled_invoice_processor/suppliers/sft.py` (append `main()`)
- Create: `tests/e2e/test_sft_cycle.py`
- Modify: `docs/superpowers/specs/2026-08-26-sft-supplier-design.md` (Pending inputs table)

**Interfaces:**
- Consumes: `SFTProcessor` (Tasks 1–5), `expected_suppliers` dict in `startup.py`.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_sft_processor.py`:

```python
def test_sft_is_an_expected_supplier(monkeypatch: pytest.MonkeyPatch) -> None:
  # startup.py builds `supplier_register` by probing connections at import time; stub that out.
  monkeypatch.setattr(SFTProcessor, "check_connections", classmethod(lambda cls: False))
  # First party imports
  from scheduled_invoice_processor.startup import expected_suppliers

  assert expected_suppliers[SuppliersEnum.SFT] is SFTProcessor
```

If importing `startup` in the unit environment fails for reasons unrelated to SFT (it imports the scheduler and probes SAS/RYO), check `tests/unit/test_job_ordering.py` and `test_main_lifecycle.py` for how they already import `startup` and copy that setup verbatim into this test instead.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_sft_processor.py -q --no-cov -k expected_supplier`
Expected: FAIL with `KeyError: <SuppliersEnum.SFT: 'SFT'>`.

- [ ] **Step 3: Register the supplier**

In `src/scheduled_invoice_processor/startup.py`:

```python
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
from scheduled_invoice_processor.suppliers.sas import SASProcessor
from scheduled_invoice_processor.suppliers.sft import SFTProcessor
```

```python
expected_suppliers: dict[SuppliersEnum, type[SupplierProcessorBase]] = {
  SuppliersEnum.SAS: SASProcessor,
  SuppliersEnum.RYO: RYOProcessor,
  SuppliersEnum.SFT: SFTProcessor,
}
```

- [ ] **Step 4: Add the debug `main()` to `sft.py`**

Append at the end of `sft.py` (after the testing-folders block), mirroring `sas.py`:

```python
async def main():
  # Third party imports
  from rich import get_console

  # First party imports
  from aeth_ext.rich.progress import Progress
  from scheduled_invoice_processor.database import DatabaseCache

  cache = DatabaseCache()
  await cache.refresh_cache()
  now = datetime.now(SETTINGS.tz)

  with Progress(console=get_console(), auto_refresh=False) as pbar:
    sft = SFTProcessor(pbar)
    orders = [order async for order in cache.schedule.walk_typed_rows() if order.supplier == SuppliersEnum.SFT]

    for order in orders:
      await sft.register_pickup(
        storenum=order.store, customer_id=order.customer, pickup_date=now, dropoff_date=now, current_week=True
      )
    await cache.submit_queued_writes_to_pool()

    await sft.pickup_files()
    await cache.submit_queued_writes_to_pool()

    for order in orders:
      await sft.register_dropoff(
        storenum=order.store, customer_id=order.customer, pickup_date=now, dropoff_date=now, current_week=True
      )
    await cache.submit_queued_writes_to_pool()

    await sft.dropoff_files()
    await cache.submit_queued_writes_to_pool()


if __name__ == "__main__":
  # Standard library imports
  from asyncio import run

  run(main())
```

- [ ] **Step 5: Add the skipped e2e cycle test**

Create `tests/e2e/test_sft_cycle.py`:

```python
"""SFT warehouse cycle. Blocked on the real FTP folder paths (see the TODO_SFT banner in suppliers/sft.py)."""

# Third party imports
import pytest

pytestmark = pytest.mark.skip(reason="SFT FTP folder paths are placeholders (TODO_SFT); unskip once real paths are set")


async def test_sft_cycle() -> None:
  # When unskipping: mirror tests/e2e/test_ryo_cycle.py using only `sft_box` (the vendor side is the same
  # server), seed two .edi files built from testing_files/SFT017_13842.edi with header dates inside the
  # current week, and assert the merged `SFT017_<a>-<b>.edi` lands in `destination_ftp_folder`.
  raise AssertionError("unreachable while skipped")
```

- [ ] **Step 6: Run the full unit suite and lint**

Run: `uv run pytest tests/unit -q --no-cov` — Expected: all PASS.
Run: `uv run pytest tests/e2e/test_sft_cycle.py -q --no-cov` — Expected: `1 skipped`.
Run: `uv run ruff check src tests` — Expected: clean.
Run: `uv run pyright src/scheduled_invoice_processor/suppliers/sft.py` — Expected: no new errors beyond what `ryo.py` already reports.

- [ ] **Step 7: Update the spec's pending-inputs table and commit**

In `docs/superpowers/specs/2026-08-26-sft-supplier-design.md`, in the "Pending inputs" table, change the folder-paths row's "Where it lands" cell to `` `sft.py` — search `TODO_SFT` `` and add a row: `| Unskip e2e cycle test | Jacob | tests/e2e/test_sft_cycle.py |`.

```bash
git add src/scheduled_invoice_processor/startup.py src/scheduled_invoice_processor/suppliers/sft.py tests/unit/test_sft_processor.py tests/e2e/test_sft_cycle.py docs/superpowers/specs/2026-08-26-sft-supplier-design.md
git commit -m "feat(sft): register SFT supplier at startup; add debug main and skipped e2e cycle

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Not in this plan (by decision)

- Extracting merge logic into a shared mixin (revisit after SFT is stable).
- Any change to `CoremarkProcessor` or its absence from `expected_suppliers`.
- The RYO duplicate-hash bug noted in Task 5(b) is fixed only in the SFT copy; RYO is untouched.
- Real FTP folder paths and schedule-sheet `SFT` rows — supplied by Jacob later.
