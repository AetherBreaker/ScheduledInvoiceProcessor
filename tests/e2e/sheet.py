"""gspread access to the TESTING spreadsheet. Only touches rows whose store number is in the reserved e2e set."""

# Standard library imports
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Third party imports
import gspread

SCHEDULE_TAB = "Current Week"
LOG_TAB = "Processing Log"
STORE_COL = 2  # 1-based column index of `store` in both tabs
TZ = ZoneInfo("US/Eastern")


def _store_of(row: list[object]) -> int | None:
  """Store number from a raw row, or None if the cell is blank / not numeric."""
  if len(row) < STORE_COL:
    return None
  try:
    return int(float(str(row[STORE_COL - 1]).strip()))
  except ValueError:
    return None


class SheetHarness:
  def __init__(self, key_file: Path, spreadsheet_id: str) -> None:
    self._client = gspread.service_account(filename=str(key_file))
    self._book = self._client.open_by_key(spreadsheet_id)

  # --- schedule rows -------------------------------------------------------------------------------------------------

  def seed_orders(self, supplier: str, orders: Iterable[tuple[int, str]]) -> None:
    rows = [
      [supplier, store, customer, "TX", "Monday", "Monday 6:00AM", "Monday 8:00AM", False, False, False]
      for store, customer in orders
    ]
    self._book.worksheet(SCHEDULE_TAB).append_rows(rows, value_input_option=gspread.utils.ValueInputOption.raw)

  def delete_orders(self, stores: Iterable[int]) -> None:
    wanted = {int(s) for s in stores}
    ws = self._book.worksheet(SCHEDULE_TAB)
    values = ws.get_all_values(value_render_option=gspread.utils.ValueRenderOption.unformatted)
    # row numbers are 1-based; skip header; delete bottom-up so indices stay valid
    for row_number in sorted(
      (i + 1 for i, row in enumerate(values) if i > 0 and _store_of(row) in wanted),
      reverse=True,
    ):
      ws.delete_rows(row_number)

  def schedule_flags(self, store: int) -> tuple[bool, bool]:
    ws = self._book.worksheet(SCHEDULE_TAB)
    for row in ws.get_all_values(value_render_option=gspread.utils.ValueRenderOption.unformatted)[1:]:
      if len(row) >= 9 and _store_of(row) == store:
        return str(row[7]).strip().upper() == "TRUE", str(row[8]).strip().upper() == "TRUE"
    raise AssertionError(f"store {store} not found in '{SCHEDULE_TAB}'")

  # --- processing log ------------------------------------------------------------------------------------------------

  def log_rows(self, stores: Iterable[int]) -> list[dict[str, str]]:
    wanted = {int(s) for s in stores}
    values = self._book.worksheet(LOG_TAB).get_all_values(value_render_option=gspread.utils.ValueRenderOption.unformatted)
    header, body = values[0], values[1:]
    return [
      {str(h): str(v) for h, v in zip(header, row, strict=False)} for row in body if _store_of(row) in wanted
    ]

  # --- guards --------------------------------------------------------------------------------------------------------

  @staticmethod
  def assert_not_near_week_flip() -> None:
    now = datetime.now(TZ)
    if (now.weekday() == 5 and now.hour >= 23) or (now.weekday() == 6 and now.hour < 1):
      raise AssertionError("Refusing to run e2e within an hour of the Sunday 00:00 week flip (US/Eastern)")
