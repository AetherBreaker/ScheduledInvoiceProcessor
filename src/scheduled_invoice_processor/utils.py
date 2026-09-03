"""R1C1-notation cell and range helpers that allow unbounded indexes."""

# Standard library imports
from re import compile
from typing import TYPE_CHECKING

# Third party imports
from gspread import IncorrectCellLabel

if TYPE_CHECKING:
  type IntOrInf = int | float


R1C1_ADDR_ROW_COL_RE = compile(r"([rR](?P<rownum>[1-9]\d*))?([cC](?P<colnum>[1-9]\d*))?$")


def _r1c1_to_rowcol_unbounded(label: str) -> tuple[IntOrInf, IntOrInf]:
  """Translates a cell's address in R1C1 notation to a tuple of integers.

  :returns: a tuple containing `row` and `column` numbers. Both indexed
      from 1 (one).
  :rtype: tuple
  """
  match = R1C1_ADDR_ROW_COL_RE.match(label)
  if not match:
    raise IncorrectCellLabel(label)

  col, row = match.group("colnum", "rownum")

  col = int(col) if col is not None else float("inf")
  row = int(row) if row is not None else float("inf")
  return (row, col)


def r1c1_range_to_grid_range(name: str, sheet_id: int | None = None) -> dict[str, int]:
  """Converts a range defined in R1C1 notation to a dict representing a `GridRange`_.

  All indexes are zero-based. Indexes are half open, e.g the start
  index is inclusive and the end index is exclusive: [startIndex, endIndex).

  Missing indexes indicate the range is unbounded on that side.

  .. _GridRange: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange

  """
  start_label, _, end_label = name.partition(":")

  start_row_index, start_column_index = _r1c1_to_rowcol_unbounded(start_label)

  end_row_index, end_column_index = _r1c1_to_rowcol_unbounded(end_label or start_label)

  if start_row_index > end_row_index:
    start_row_index, end_row_index = end_row_index, start_row_index

  if start_column_index > end_column_index:
    start_column_index, end_column_index = end_column_index, start_column_index

  grid_range = {
    "startRowIndex": start_row_index - 1,
    "endRowIndex": end_row_index,
    "startColumnIndex": start_column_index - 1,
    "endColumnIndex": end_column_index,
  }

  filtered_grid_range: dict[str, int] = {key: value for (key, value) in grid_range.items() if isinstance(value, int)}

  if sheet_id is not None:
    filtered_grid_range["sheetId"] = sheet_id

  return filtered_grid_range
