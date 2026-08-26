"""Unit tests for the SFT warehouse supplier (same-server pickup, header-dated window, RYO-style merge)."""

# This file tests private methods by design.
# pyright: reportPrivateUsage=false

# Future imports
from __future__ import annotations

# Standard library imports
import atexit
import re
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import patch

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sft import SFTProcessor
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Generator

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
def frozen_now(monkeypatch: pytest.MonkeyPatch) -> None:
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
