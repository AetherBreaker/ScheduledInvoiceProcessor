if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from datetime import datetime, timedelta
from os import sep, walk
from os.path import abspath, basename
from re import compile

from dateutil.relativedelta import SA, relativedelta
from dateutil.utils import today as _today
from gspread import IncorrectCellLabel
from typing_custom import IntOrInf
from typing_custom.custom_path import CustomPath

shift = timedelta()

# if __debug__:
#   # if debugging, calculate the difference between now and last saturday at 11:58pm
#   now = datetime.now()
#   last_saturday = now + relativedelta(weekday=SA(-1), hour=23, minute=58, second=0, microsecond=0)
#   shift = last_saturday - now


def today(tzinfo=None):
  """
  Returns a :py:class:`datetime` representing the current day at midnight

  :param tzinfo:
      The time zone to attach (also used to determine the current day).

  :return:
      A :py:class:`datetime.datetime` object representing the current day
      at midnight.
  """

  result = _today(tzinfo=tzinfo)

  result += shift

  return result


def get_now(tzinfo=None):
  """
  Returns a :py:class:`datetime` representing the current date and time

  :param tzinfo:
      The time zone to attach (also used to determine the current date and time).

  :return:
      A :py:class:`datetime.datetime` object representing the current date and time.
  """

  result = datetime.now(tz=tzinfo)

  result += shift

  return result


def get_last_sat(dt: datetime | None = None, tzinfo=None):
  now = get_now(tzinfo=tzinfo) if dt is None else dt
  return now + relativedelta(weekday=SA(-1))


def get_next_sat(dt: datetime | None = None, tzinfo=None):
  now = get_now(tzinfo=tzinfo) if dt is None else dt
  return now + relativedelta(weekday=SA(+1))


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
  """Converts a range defined in R1C1 notation to a dict representing
  a `GridRange`_.

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


def print_directory_tree(root_dir: CustomPath):
  print(f"Current Working Directory: {abspath(root_dir)}")
  for each_dir_path, each_dir_name, dir_files in walk(root_dir):
    base = basename(each_dir_path)
    if base in {".git", "__pycache__", "venv", "env", ".venv"}:
      continue
    level = each_dir_path.replace(str(root_dir), "").count(sep)
    indent = " " * 4 * level
    print(f"{indent}[{basename(each_dir_path)}/]")
    sub_indent = " " * 4 * (level + 1)
    for f in dir_files:
      print(f"{sub_indent}{f}")
