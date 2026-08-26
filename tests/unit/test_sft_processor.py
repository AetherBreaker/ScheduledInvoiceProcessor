"""Unit tests for the SFT warehouse supplier (same-server pickup, header-dated window, RYO-style merge)."""

# This file tests private methods by design.
# pyright: reportPrivateUsage=false

# Future imports
from __future__ import annotations

# Standard library imports
import atexit
from datetime import datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers.sft import SFTProcessor
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Generator
  from pathlib import Path

  # First party imports

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
