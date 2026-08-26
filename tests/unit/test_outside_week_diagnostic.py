"""The pickup accept windows are two weeks wide in places; this log-only probe reports when the extra week is used."""

# This file tests private methods by design.
# pyright: reportPrivateUsage=false

# Standard library imports
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, cast

# Third party imports
import pytest

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers import SupplierProcessorBase
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData

# Wednesday; strict week = Sun 2025-06-15 00:00 .. Sat 2025-06-21 23:59:59.999999
PICKUP = datetime(2025, 6, 18, 12, 0, tzinfo=SETTINGS.tz)


def _meta() -> FileRegisterData:
  return FileRegisterData(
    storenum=17,
    customer_id="SFT017",
    pickup_date=PICKUP,
    dropoff_date=PICKUP + timedelta(days=1),
    file_pattern=re.compile(r".*"),
    _current_week=True,
    _waiting_folder=PurePosixPath("/Waiting/X"),
    _local_copy_folder=Path("unit-test-local"),
  )


def _probe(file_date: datetime | None, caplog: pytest.LogCaptureFixture) -> bool:
  # Only `self.__class__.__name__` and the class attribute are used; a bare namespace stands in for a processor.
  fake_self = SimpleNamespace(OUTSIDE_WEEK_LOG_TAG=SupplierProcessorBase.OUTSIDE_WEEK_LOG_TAG)
  with caplog.at_level(logging.WARNING):
    return SupplierProcessorBase._warn_if_outside_week(cast("Any", fake_self), _meta(), file_date, "inv.txt", logging.getLogger("probe"))


@pytest.mark.parametrize(
  "file_date",
  [
    datetime(2025, 6, 15, 0, 0, 0, tzinfo=SETTINGS.tz),
    datetime(2025, 6, 19, 9, 46, 46, tzinfo=SETTINGS.tz),
    datetime(2025, 6, 21, 23, 59, 59, tzinfo=SETTINGS.tz),
  ],
)
def test_inside_week_is_silent(file_date: datetime, caplog: pytest.LogCaptureFixture) -> None:
  assert _probe(file_date, caplog) is False
  assert SupplierProcessorBase.OUTSIDE_WEEK_LOG_TAG not in caplog.text


@pytest.mark.parametrize(
  "file_date",
  [
    datetime(2025, 6, 14, 23, 59, 59, tzinfo=SETTINGS.tz),  # last week (the extra week SAS/mtime accept)
    datetime(2025, 6, 22, 0, 0, 0, tzinfo=SETTINGS.tz),  # next week
  ],
)
def test_outside_week_warns_with_signature(file_date: datetime, caplog: pytest.LogCaptureFixture) -> None:
  assert _probe(file_date, caplog) is True
  assert "[OUTSIDE_WEEK_PICKUP] store 17: accepted inv.txt dated " in caplog.text
  assert caplog.records[-1].levelno == logging.WARNING


def test_unknown_date_is_silent(caplog: pytest.LogCaptureFixture) -> None:
  assert _probe(None, caplog) is False
  assert caplog.text == ""


def test_date_from_filename_match_uses_year_month_day_groups() -> None:
  sas_like = re.compile(r"^EF\d+_(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})\d{6}\.TXT$")
  match = sas_like.match("EF900100_20250619094646123456.TXT")
  assert match is not None
  assert SupplierProcessorBase._date_from_filename_match(match) == datetime(2025, 6, 19, 9, 46, 46, tzinfo=SETTINGS.tz)

  no_date = re.compile(r"^CV(?P<customer>\d+)\.TXT$").match("CV123.TXT")
  assert no_date is not None
  assert SupplierProcessorBase._date_from_filename_match(no_date) is None

  bad_date = sas_like.match("EF900100_20251345094646123456.TXT")
  assert bad_date is not None
  assert SupplierProcessorBase._date_from_filename_match(bad_date) is None
