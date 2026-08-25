"""gspread access to the TESTING spreadsheet. Only touches rows whose store number is in the reserved e2e set."""

# Standard library imports
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

# Third party imports
import gspread

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Iterable
  from pathlib import Path

SCHEDULE_TAB = "Current Week"
LOG_TAB = "Processing Log"
STORE_COL = 2  # 1-based column index of `store` in both tabs
TZ = ZoneInfo("US/Eastern")


_SHEETS_SERIAL_EPOCH = datetime(1899, 12, 30)  # noqa: DTZ001


def _serial_to_datetime(value: str) -> datetime | None:
  """Google Sheets' UNFORMATTED_VALUE returns a bare serial day-number (days since 1899-12-30, tz-naive
  wall-clock) for any cell it auto-detected as a date/time -- including `action_datetime`, even though the
  app writes it as an ISO-8601 string: Sheets silently reinterprets the ISO text on write. Assume the
  wall-clock reading is in TZ, matching the app's own SETTINGS.tz."""
  try:
    serial = float(value)
  except ValueError:
    return None
  return (_SHEETS_SERIAL_EPOCH + timedelta(days=serial)).replace(tzinfo=TZ)


def _at_or_after(value: object, since: datetime) -> bool:
  """True if `value` parses to a datetime >= since; not-parseable values do not match."""
  if value is None:
    return False
  text = str(value)
  try:
    parsed = datetime.fromisoformat(text)
  except ValueError:
    maybe = _serial_to_datetime(text)
    if maybe is None:
      return False
    parsed = maybe
  return parsed >= since


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
      [supplier, store, customer, "TX", "Monday", "Monday 6:00AM", "Monday 8:00AM", False, False, False] for store, customer in orders
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

  def log_rows(self, stores: Iterable[int], since: datetime | None = None) -> list[dict[str, str]]:
    wanted = {int(s) for s in stores}
    values = self._book.worksheet(LOG_TAB).get_all_values(value_render_option=gspread.utils.ValueRenderOption.unformatted)
    header, body = values[0], values[1:]
    rows = [{str(h): str(v) for h, v in zip(header, row, strict=False)} for row in body if _store_of(row) in wanted]
    if since is None:
      return rows
    return [row for row in rows if _at_or_after(row.get("action_datetime"), since)]

  # --- guards --------------------------------------------------------------------------------------------------------

  @staticmethod
  def assert_not_near_week_flip() -> None:
    now = datetime.now(TZ)
    if (now.weekday() == 5 and now.hour >= 23) or (now.weekday() == 6 and now.hour < 1):
      raise AssertionError("Refusing to run e2e within an hour of the Sunday 00:00 week flip (US/Eastern)")
