import contextlib

if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import get_running_loop, sleep, to_thread
from collections.abc import AsyncIterator, Sequence
from copy import deepcopy
from logging import getLogger
from typing import Any, Literal, Optional, overload

from aiologic import Lock
from aiorwlock import RWLock
from dateutil.relativedelta import SA, relativedelta
from environment_init_vars import SETTINGS
from google.oauth2.service_account import Credentials
from gspread import Client, authorize
from gspread.http_client import BackOffHTTPClient
from gspread.utils import DateTimeOption, Dimension, ValueInputOption, ValueRenderOption, finditem
from pandas import DataFrame, Series
from pydantic import TypeAdapter
from typing_custom import InvoiceNum, ValueRange, ValuesBatchUpdateBody
from typing_custom.abc import SingletonType
from typing_custom.dataframe_column_names import (
  ColNameEnum,
  DatabaseOrderLogColumns,
  DatabaseOrderLogIndex,
  DatabaseScheduleColumns,
  DatabaseScheduleIndex,
)
from utils import today
from validation import CustomBaseModel
from validation.apply_model import build_typed_dataframe
from validation.models.db_entries import (
  ORDER_LOG_TYPE_ADAPTERS,
  SCHEDULE_TYPE_ADAPTERS,
  OrderLogDBEntryModel,
  ScheduledOrderDBEntryModel,
)

logger = getLogger(__name__)


DEFAULT_SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]


class DatabaseCache(metaclass=SingletonType):
  _database_id = SETTINGS.database_id

  _tab_id_schedule_base_sheet = SETTINGS.database_base_schedule_id

  _creds = Credentials.from_service_account_file(SETTINGS.googe_api_key_file, scopes=DEFAULT_SCOPES)

  _schedule_tab_range = f"'Current Week'!R2C1:C{len(DatabaseScheduleColumns.all_columns())}"
  _prev_week_schedule_tab_range = f"'Previous Week'!R2C1:C{len(DatabaseScheduleColumns.all_columns())}"
  _order_log_tab_range = f"'Processed Orders'!R2C1:C{len(DatabaseOrderLogColumns.all_columns())}"

  _schedule_range_format_single = "'Current Week'!{cell}"
  _prev_week_schedule_range_format_single = "'Previous Week'!{cell}"
  _order_log_range_format_single = "'Processed Orders'!{cell}"

  _schedule_range_format = "'Current Week'!{start}:{end}"
  _prev_week_schedule_range_format = "'Previous Week'!{start}:{end}"
  _order_log_range_format = "'Processed Orders'!{start}:{end}"

  _db_write_body_base = ValuesBatchUpdateBody(
    valueInputOption=ValueInputOption.raw,
    includeValuesInResponse=False,
    responseValueRenderOption=ValueRenderOption.unformatted,
    responseDateTimeRenderOption=DateTimeOption.formatted_string,
    data=[],
  )

  reauth_interval = 2700
  api_call_min_interval = 1.1

  schedule: "CacheViewschedule"
  prev_week_schedule: "CacheViewschedule"
  order_log: "CacheViewOrderLog"

  def __init__(self) -> None:
    self.schedule: CacheViewschedule
    self.prev_week_schedule: CacheViewschedule
    self.order_log: CacheViewOrderLog

    self._read_write_lock = RWLock()
    # self._read_write_lock = RWLock(fast=True)

    self._api_write_lock = Lock()
    self._update_queue_lock = Lock()

    self._client = None
    self._client_last_auth_time = None

    self._api_last_call_time = None

    self._update_body: Optional[ValuesBatchUpdateBody] = None

    self.loop = get_running_loop()

    self.update_db_header()

  @property
  def client(self) -> Client:
    now = self.loop.time()
    if self._client_last_auth_time is None or ((self._client_last_auth_time + self.reauth_interval) < now):
      self._client = authorize(self._creds, http_client=BackOffHTTPClient)
      self._client_last_auth_time = self.loop.time()
    return self._client  # type: ignore

  @property
  def queued_updates(self) -> list[ValueRange]:
    if self._update_body is None:
      self._update_body = deepcopy(self._db_write_body_base)
    return self._update_body["data"]

  @property
  def get_week_ending_name(self) -> str:
    last_saturday = today() + relativedelta(weekday=SA(-2))
    return f"Week Ending {last_saturday.year:0>4}/{last_saturday.month:0>2}/{last_saturday.day:0>2}"

  async def queue_db_api_update(self, value: ValueRange) -> None:
    async with self._update_queue_lock:
      if self._update_body is None:
        self._update_body = deepcopy(self._db_write_body_base)
      self._update_body["data"].append(value)

  def update_db_header(self) -> None:
    body = deepcopy(self._db_write_body_base)

    body["data"].append(
      ValueRange(
        range=self._schedule_range_format.format(start="R1C1", end=f"R1C{len(DatabaseScheduleColumns.all_columns())}"),
        majorDimension=Dimension.rows,
        values=[DatabaseScheduleColumns.all_columns()],  # type: ignore
      )
    )
    body["data"].append(
      ValueRange(
        range=self._prev_week_schedule_range_format.format(start="R1C1", end=f"R1C{len(DatabaseScheduleColumns.all_columns())}"),
        majorDimension=Dimension.rows,
        values=[DatabaseScheduleColumns.all_columns()],  # type: ignore
      )
    )
    body["data"].append(
      ValueRange(
        range=self._order_log_range_format.format(start="R1C1", end=f"R1C{len(DatabaseOrderLogColumns.all_columns())}"),
        majorDimension=Dimension.rows,
        values=[DatabaseOrderLogColumns.all_columns()],  # type: ignore
      )
    )

    self.client.http_client.values_batch_update(id=self._database_id, body=body)

  async def wait_for_api(self) -> None:
    if self._api_last_call_time is None:
      self._api_last_call_time = self.loop.time()
      return

    delta = self.loop.time() - self._api_last_call_time
    if delta >= self.api_call_min_interval:
      self._api_last_call_time = self.loop.time()
      return

    await sleep(self.api_call_min_interval - delta)
    self._api_last_call_time = self.loop.time()
    return

  async def refresh_cache(self) -> None:
    async with self._api_write_lock, self._read_write_lock.writer_lock:
      await self.wait_for_api()

      result = self.client.http_client.values_batch_get(
        id=self._database_id,
        ranges=[self._schedule_tab_range, self._prev_week_schedule_tab_range, self._order_log_tab_range],
        params={
          "majorDimension": Dimension.rows,
          "valueRenderOption": ValueRenderOption.unformatted,
          "dateTimeRenderOption": DateTimeOption.formatted_string,
        },
      )

      schedule_data: list[list[str | int | float]] = result["valueRanges"][0]["values"]
      self.schedule = CacheViewschedule(
        raw_data=schedule_data, columns=DatabaseScheduleColumns, types_model=ScheduledOrderDBEntryModel, cache_core=self
      )

      prev_week_schedule_data: list[list[str | int | float]] = result["valueRanges"][1]["values"]
      self.prev_week_schedule = CacheViewschedule(
        raw_data=prev_week_schedule_data,
        columns=DatabaseScheduleColumns,
        types_model=ScheduledOrderDBEntryModel,
        cache_core=self,
      )

      self.prev_week_schedule._cache.loc[:, DatabaseScheduleColumns.invoice_application_time] = (
        self.prev_week_schedule._cache.loc[:, DatabaseScheduleColumns.invoice_application_time].map(
          lambda x: x - relativedelta(weeks=1) if x else x
        )
      )  # type: ignore
      self.prev_week_schedule._cache.loc[:, DatabaseScheduleColumns.invoice_pickup_time] = self.prev_week_schedule._cache.loc[
        :, DatabaseScheduleColumns.invoice_pickup_time
      ].map(lambda x: x - relativedelta(weeks=1) if x else x)  # type: ignore

      try:
        order_log_data: list[list[str | int | float]] = result["valueRanges"][2]["values"]
        self.order_log = CacheViewOrderLog(
          raw_data=order_log_data,
          columns=DatabaseOrderLogColumns,
          types_model=OrderLogDBEntryModel,
          cache_core=self,
        )
      except KeyError:
        self.order_log = CacheViewOrderLog(
          raw_data=[], columns=DatabaseOrderLogColumns, types_model=OrderLogDBEntryModel, cache_core=self
        )

      # self.schedule._cache.to_csv("debug_schedule.csv")
      # self.prev_week_schedule._cache.to_csv("debug_prev_week_schedule.csv")
      # self.order_log._cache.to_csv("debug_order_log.csv")
      # pass

  async def submit_queued_writes_to_pool(self) -> None:
    if not self.queued_updates:
      return

    await to_thread(self._api_write)

  def _api_write(self):
    with self._api_write_lock, self._update_queue_lock:
      self.client.http_client.values_batch_update(
        id=self._database_id,
        body=self._update_body,
      )

      self._update_body = None  # Reset the update body after writing

  async def flip_to_new_week(self):
    await self.submit_queued_writes_to_pool()

    async with self._api_write_lock, self._update_queue_lock:
      await self.wait_for_api()

      metadat = self.client.http_client.fetch_sheet_metadata(self._database_id)

      current_week_sheet = finditem(lambda x: x["properties"]["title"] == "Current Week", metadat["sheets"])
      previous_week_sheet = finditem(lambda x: x["properties"]["title"] == "Previous Week", metadat["sheets"])

      all_sheet_ids = [int(s["properties"]["sheetId"]) for s in metadat["sheets"]]

      new_sheet_id = max(all_sheet_ids) + 1

      with contextlib.suppress(StopIteration):
        finditem(lambda x: x["properties"]["title"] == self.get_week_ending_name, metadat["sheets"])
        logger.warning(
          "Attempted to flip week more than once in the same week."
          "\n Or a sheet exists with the same name as the most recent week ending name."
        )
        return  # already done this week
      request_body = {
        "requests": [
          {
            "updateSheetProperties": {
              "properties": {
                "sheetId": previous_week_sheet["properties"]["sheetId"],
                "title": self.get_week_ending_name,
                "hidden": True,
              },
              "fields": "title,hidden",
            }
          },
          {
            "updateSheetProperties": {
              "properties": {
                "sheetId": current_week_sheet["properties"]["sheetId"],
                "title": "Previous Week",
              },
              "fields": "title",
            }
          },
          {
            "duplicateSheet": {
              "sourceSheetId": self._tab_id_schedule_base_sheet,
              "insertSheetIndex": len(metadat["sheets"]),
              "newSheetId": new_sheet_id,
              "newSheetName": "Current Week",
            }
          },
          {
            "addProtectedRange": {
              "protectedRange": {
                "range": {
                  "sheetId": new_sheet_id,
                  "startRowIndex": 0,
                  "startColumnIndex": 0,
                },
                "description": None,
                "warningOnly": False,
                "requestingUserCanEdit": True,
                "editors": {
                  "users": [
                    "aetherbreaker7777@gmail.com",
                    "scheduling-service@order-scheduling-processor.iam.gserviceaccount.com",
                  ],
                  "groups": [],
                },
              }
            }
          },
        ]
      }

      self.client.http_client.batch_update(self._database_id, request_body)

    await self.refresh_cache()


class CacheViewBase[ModelT: CustomBaseModel]:
  _range_format: str
  _range_format_single: str
  _field_type_adapters: dict[str, TypeAdapter]

  def __init__(
    self,
    raw_data: list[list[str | int | float]],
    columns: type[ColNameEnum],
    types_model: type[ModelT],
    cache_core: DatabaseCache,
  ) -> None:
    self._cache = build_typed_dataframe(data=raw_data, columns=columns, types_model=types_model)  # type: ignore
    self._cache_index = columns.__index_items__
    self._columns = columns
    self._core = cache_core
    self._model = types_model

  async def __aenter__(self) -> DataFrame:
    await self._core._read_write_lock.reader_lock.acquire()
    return self._cache

  async def __aexit__(self, exc_type, exc_value, traceback) -> None:
    self._core._read_write_lock.reader_lock.release()

  async def read_typed_row(self, index, re_validate: bool = False) -> ModelT:
    async with self._core._read_write_lock.reader_lock:
      row = self._cache.iloc[await self.get_rownum(index)]

    if re_validate:
      return self._model(**row)
    else:
      return self._model.model_construct(**row)  # type: ignore

  async def walk_typed_rows(self, re_validate: bool = False) -> AsyncIterator[ModelT]:
    async with self._core._read_write_lock.reader_lock:
      for _, row in self._cache.iterrows():
        if re_validate:
          yield self._model(**row)
        else:
          yield self._model.model_construct(**row)  # type: ignore

  async def read_value(self, index, col, validate: bool = False) -> tuple[TypeAdapter[Any], Any] | Any:
    async with self._core._read_write_lock.reader_lock:
      value = self._cache.iat[await self.get_rownum(index), self._cache.columns.get_loc(col)]  # type: ignore
      if validate:
        ta = self._field_type_adapters.get(col)
        if ta is None:
          raise RuntimeError("how the fuck")
        return (ta, value)
      return value

  async def process_index(self, index):
    return index

  async def get_rownum(self, index) -> int:
    # default process index. Assume single index
    async with self._core._read_write_lock.reader_lock:
      # locate the row number of index
      row_number = self._cache.index.get_loc(await self.process_index(index))

    if isinstance(row_number, slice):
      row_number = int(row_number.stop - row_number.start)
    if (not isinstance(row_number, int)) or (0 < row_number < 1):
      raise IndexError("Index provided is either a partial index, or otherwise fails to return a single row")

    return row_number

  async def write_value(self, index, column, value: Any, ta: TypeAdapter) -> None:
    row_number = await self.get_rownum(index)

    value = ta.dump_python(value)
    sheets_value = ta.dump_python(value, mode="json")

    async with self._core._read_write_lock.writer_lock:
      self._cache.at[index, column] = value

    row_number += 2  # add one to account for gsheets header

    # get column index

    await self._core.queue_db_api_update(
      ValueRange(
        range=self._range_format_single.format(cell=f"R{row_number}C{self._columns.get_enum_index(column) + 1}"),
        majorDimension=Dimension.rows,
        values=[[sheets_value]],
      )
    )

  async def update_row(self, index, values: Sequence[Any] | ModelT) -> None:
    row_number = await self.get_rownum(index)

    if isinstance(values, self._model):
      row = Series(values.model_dump(), dtype=object)
      sheets_row = Series(values.model_dump(mode="json"), dtype=object)
    elif isinstance(values, Sequence):
      row = Series(values, dtype=object, index=self._columns.all_columns())
      sheets_row = row.copy()
    else:
      raise TypeError(f"{type(values)} does not match the expected type of Sequence[Any] or {self._model}")

    async with self._core._read_write_lock.writer_lock:
      self._cache.iloc[await self.get_rownum(index), :] = row

    row_number += 2  # add one to account for gsheets header

    # get column index

    update_data = ValueRange(
      range=self._range_format.format(start=f"R{row_number}C1", end=f"C{len(self._columns)}"),
      majorDimension=Dimension.rows,
      values=[sheets_row.tolist()],  # type: ignore
    )

    await self._core.queue_db_api_update(update_data)

  async def append_row(self, values: ModelT) -> None:
    index = (
      tuple(getattr(values, col) for col in self._cache_index)
      if len(self._cache_index) > 1
      else getattr(values, self._cache_index[0])
    )

    row = Series(values.model_dump(), dtype=object)
    sheets_row = Series(values.model_dump(mode="json"), dtype=object).tolist()

    async with self._core._read_write_lock.writer_lock:
      self._cache.loc[index, :] = row

    await self._core.queue_db_api_update(
      ValueRange(
        range=self._range_format.format(start=f"R{len(self._cache) + 1}C1", end=f"C{len(self._columns)}"),
        majorDimension=Dimension.rows,
        values=[sheets_row],
      )
    )

  async def check_exists(self, index) -> bool:
    async with self._core._read_write_lock.reader_lock:
      return index in self._cache.index


class CacheViewschedule(CacheViewBase[ScheduledOrderDBEntryModel]):
  _range_format_single = "Current Week!{cell}"
  _range_format = "Current Week!{start}:{end}"
  _field_type_adapters = SCHEDULE_TYPE_ADAPTERS

  async def read_typed_row(self, index: DatabaseScheduleIndex, re_validate: bool = False) -> ScheduledOrderDBEntryModel:
    return await super().read_typed_row(index, re_validate)

  async def walk_typed_rows(self, re_validate: bool = False) -> AsyncIterator[ScheduledOrderDBEntryModel]:
    async for item in super().walk_typed_rows(re_validate):
      yield item

  @overload
  async def read_value(
    self, index: DatabaseScheduleIndex, col: DatabaseScheduleColumns, validate: Literal[False] = False
  ) -> Any: ...

  @overload
  async def read_value(
    self, index: DatabaseScheduleIndex, col: DatabaseScheduleColumns, validate: Literal[True]
  ) -> tuple[TypeAdapter[Any], Any]: ...

  async def read_value(
    self, index: DatabaseScheduleIndex, col: DatabaseScheduleColumns, validate: bool = False
  ) -> tuple[TypeAdapter[Any] | Any] | Any:
    return await super().read_value(index, col, validate)

  async def process_index(self, index: DatabaseScheduleIndex) -> DatabaseScheduleIndex:
    return await super().process_index(index)

  async def get_rownum(self, index: DatabaseScheduleIndex) -> int:
    return await super().get_rownum(index)

  async def write_value(self, index: DatabaseScheduleIndex, column: DatabaseScheduleColumns, value: Any, ta: TypeAdapter) -> None:
    return await super().write_value(index, column, value, ta)

  async def update_row(self, index: DatabaseScheduleIndex, values: Sequence[Any] | ScheduledOrderDBEntryModel) -> None:
    return await super().update_row(index, values)

  async def append_row(self, values: ScheduledOrderDBEntryModel) -> None:
    return await super().append_row(values)

  async def check_exists(self, index: DatabaseScheduleIndex) -> bool:
    return await super().check_exists(index)

  async def check_box(
    self,
    idx: DatabaseScheduleIndex,
    col: Literal[DatabaseScheduleColumns.invoice_grabbed, DatabaseScheduleColumns.invoice_applied],
  ) -> None:
    await self.write_value(
      index=idx,
      column=col,
      value=True,
      ta=self._field_type_adapters[col],
    )

  async def uncheck_box(
    self,
    idx: DatabaseScheduleIndex,
    col: Literal[DatabaseScheduleColumns.invoice_grabbed, DatabaseScheduleColumns.invoice_applied],
  ) -> None:
    await self.write_value(
      index=idx,
      column=col,
      value=False,
      ta=self._field_type_adapters[col],
    )

  async def check_toggled(
    self,
    idx: DatabaseScheduleIndex,
    col: Literal[DatabaseScheduleColumns.invoice_grabbed, DatabaseScheduleColumns.invoice_applied],
  ) -> bool:
    return await self.read_value(idx, col)


class CacheViewOrderLog(CacheViewBase[OrderLogDBEntryModel]):
  _range_format_single = "'Processed Orders'!{cell}"
  _range_format = "'Processed Orders'!{start}:{end}"
  _field_type_adapters = ORDER_LOG_TYPE_ADAPTERS

  async def read_typed_row(self, index: DatabaseOrderLogIndex, re_validate: bool = False) -> OrderLogDBEntryModel:
    return await super().read_typed_row(index, re_validate)

  @overload
  async def read_value(
    self, index: DatabaseOrderLogIndex, col: DatabaseOrderLogColumns, validate: Literal[False] = False
  ) -> Any: ...

  @overload
  async def read_value(
    self, index: DatabaseOrderLogIndex, col: DatabaseOrderLogColumns, validate: Literal[True]
  ) -> tuple[TypeAdapter[Any], Any]: ...

  async def read_value(
    self, index: DatabaseOrderLogIndex, col: DatabaseOrderLogColumns, validate: bool = False
  ) -> tuple[TypeAdapter[Any] | Any] | Any:
    return await super().read_value(index, col, validate)

  async def process_index(self, index: DatabaseOrderLogIndex) -> DatabaseOrderLogIndex:
    return await super().process_index(index)

  async def get_rownum(self, index: DatabaseOrderLogIndex) -> int:
    return await super().get_rownum(index)

  async def write_value(self, index: DatabaseOrderLogIndex, column: DatabaseOrderLogColumns, value: Any, ta: TypeAdapter) -> None:
    return await super().write_value(index, column, value, ta)

  async def update_row(self, index: DatabaseOrderLogIndex, values: Sequence[Any] | OrderLogDBEntryModel) -> None:
    return await super().update_row(index, values)

  async def append_row(self, values: OrderLogDBEntryModel) -> None:
    return await super().append_row(values)

  async def check_exists(self, index: InvoiceNum) -> bool:
    async with self._core._read_write_lock.reader_lock:
      char_uids = self._cache.index.levels[0]  # type: ignore

      return index in char_uids
