"""Type aliases and TypedDicts for Google Sheets API payloads."""

# Standard library imports
from logging import getLogger
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Mapping, Sequence
  from typing import Any, NotRequired

  # Third party imports
  from gspread.utils import DateTimeOption, Dimension, ValueInputOption, ValueRenderOption

logger = getLogger(__name__)

type SupplierQueueKey = str

type StoreNum = int
type CustomerID = str
type InvoiceNum = str


type SheetsValue = int | float | str
type SheetsRangeName = str
type SheetsRangeUpdateValues = Sequence[Sequence[SheetsValue]]

type Request = "AppendDimension" | Mapping[str, Any]


class ValueRange(TypedDict):
  """Sheets API `ValueRange` body."""

  range: NotRequired[SheetsRangeName]
  majorDimension: Dimension
  values: SheetsRangeUpdateValues


class ValuesBatchUpdateBody(TypedDict):
  """Body of a `values.batchUpdate` request."""

  valueInputOption: ValueInputOption
  includeValuesInResponse: bool
  responseValueRenderOption: ValueRenderOption
  responseDateTimeRenderOption: DateTimeOption
  data: list[ValueRange]


class BatchUpdateBody(TypedDict):
  """Body of a `spreadsheets.batchUpdate` request."""

  requests: list[Request]


class AppendDimension(TypedDict):
  """`appendDimension` request payload."""

  sheetId: int
  dimension: Dimension
  length: int


class GridRange(TypedDict):
  """Sheets API `GridRange`; a missing index means the range is unbounded on that side."""

  sheetId: int
  startRowIndex: NotRequired[int]
  endRowIndex: NotRequired[int]
  startColumnIndex: NotRequired[int]
  endColumnIndex: NotRequired[int]


class CellFormat(TypedDict):
  """A range plus the cell format to apply to it."""

  range: GridRange
  format: Mapping[str, Any]
