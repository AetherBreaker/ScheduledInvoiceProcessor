if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from collections.abc import Mapping
from logging import getLogger
from typing import Any, NotRequired, TypedDict

from gspread.utils import DateTimeOption, Dimension, ValueInputOption, ValueRenderOption

logger = getLogger(__name__)

type SupplierQueueKey = str

type StoreNum = int
type CustomerID = str
type InvoiceNum = str


type SheetsValue = int | float | str
type SheetsRangeName = str
type SheetsRangeUpdateValues = list[list[SheetsValue]]

type Request = "AppendDimension" | Mapping[str, Any]


type IntOrInf = int | float


class ValueRange(TypedDict):
  range: NotRequired[SheetsRangeName]
  majorDimension: Dimension
  values: SheetsRangeUpdateValues


class ValuesBatchUpdateBody(TypedDict):
  valueInputOption: ValueInputOption
  includeValuesInResponse: bool
  responseValueRenderOption: ValueRenderOption
  responseDateTimeRenderOption: DateTimeOption
  data: list[ValueRange]


class BatchUpdateBody(TypedDict):
  requests: list[Request]


class AppendDimension(TypedDict):
  sheetId: int
  dimension: Dimension
  length: int


class GridRange(TypedDict):
  sheetId: int
  startRowIndex: NotRequired[int]
  endRowIndex: NotRequired[int]
  startColumnIndex: NotRequired[int]
  endColumnIndex: NotRequired[int]


class CellFormat(TypedDict):
  range: GridRange
  format: Mapping[str, Any]
