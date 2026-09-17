# This file drives the private cache view by design.
# pyright: reportPrivateUsage=false

# Standard library imports
from datetime import datetime
from typing import Any

# Third party imports
import pytest
from aiorwlock import RWLock

# First party imports
from scheduled_invoice_processor.database import CacheViewOrderLog
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum


class _StubCore:
  # Only the members log_action -> append_row -> correct_format reach for.
  def __init__(self) -> None:
    self._read_write_lock = RWLock()

  async def queue_db_api_before_write_update(self, request: Any) -> None:
    return None

  async def queue_db_api_after_write_update(self, request: Any) -> None:
    return None

  async def queue_db_api_values_raw_update(self, data: Any) -> None:
    return None

  async def queue_db_api_values_user_entered_update(self, data: Any) -> None:
    return None


async def test_reprocessing_an_invoice_raises_however_the_other_columns_differ() -> None:
  # Guards the defect this test was written for: `status` and `action_datetime` had been added to
  # DatabaseOrderLogColumns.__index_items__, which made every row's key unique and silently disabled the
  # duplicate check for months. The second call differs in status, action_datetime and week_end_date, so
  # returning any of those three to the index lets the duplicate through and fails this test.
  view = CacheViewOrderLog([], _StubCore(), sheet_id=0)  # pyright: ignore[reportArgumentType]

  entry = {
    "supplier": SuppliersEnum.RYO,
    "store": 40,
    "invoice_num": "59720",
    "customer": "9893028027",
    "action": LogActionEnum.FILE_PICKED_UP,
    "status": StatusCode.SUCCESS,
    "action_datetime": datetime(2026, 9, 9, 12, 32, 4, tzinfo=SETTINGS.tz),
    "week_end_date": datetime(2026, 9, 12, 18, 0, tzinfo=SETTINGS.tz),
    "note": "first pickup",
  }
  await view.log_action(**entry)

  reprocessed = entry | {
    "status": StatusCode.FAILURE,
    "action_datetime": datetime(2026, 9, 15, 18, 2, 29, tzinfo=SETTINGS.tz),
    "week_end_date": datetime(2026, 9, 19, 18, 0, tzinfo=SETTINGS.tz),
    "note": "same invoice, a week later",
  }

  with pytest.raises(IndexError, match="already logged for invoice 59720"):
    await view.log_action(**reprocessed)
