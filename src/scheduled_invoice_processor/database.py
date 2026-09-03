# pyright: reportPrivateUsage=false
"""Google Sheets-backed cache of the schedule and processing-log tabs, with batched, queued writes."""

# Standard library imports
from abc import ABC
from asyncio import get_running_loop, sleep, to_thread
from collections.abc import Sequence
from contextlib import suppress
from copy import deepcopy
from logging import getLogger
from typing import TYPE_CHECKING, cast, overload

# Third party imports
from aiologic import Lock
from aiorwlock import RWLock
from dateutil.relativedelta import SA, relativedelta
from google.oauth2.service_account import Credentials
from gspread import Client, authorize
from gspread.http_client import BackOffHTTPClient
from gspread.utils import DateTimeOption, Dimension, ValueInputOption, ValueRenderOption, finditem
from pandas import Series, to_numeric

# First party imports
from aeth_ext.types.abc import SingletonType
from aeth_ext.utils import today

# Local folder imports
from .environment_init_vars import SETTINGS
from .typing_custom import AppendDimension, BatchUpdateBody, ValueRange, ValuesBatchUpdateBody
from .typing_custom.dataframe_column_names import DatabaseOrderLogColumns, DatabaseScheduleColumns
from .validation.apply_model import build_typed_dataframe
from .validation.models.db_entries import (
  ORDER_LOG_TYPE_ADAPTERS,
  SCHEDULE_TYPE_ADAPTERS,
  OrderLogDBEntryModel,
  ScheduledOrderDBEntryModel,
)

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import AsyncIterator
  from datetime import datetime
  from types import TracebackType
  from typing import Any, Literal

  # Third party imports
  from pandas import DataFrame
  from pydantic import TypeAdapter

  # Local folder imports
  from .typing_custom import CustomerID, InvoiceNum, Request, StoreNum
  from .typing_custom.dataframe_column_names import ColNameEnum, DatabaseOrderLogIndex, DatabaseScheduleIndex  # noqa: F401
  from .typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum
  from .validation import CustomBaseModel

logger = getLogger(__name__)


DEFAULT_SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]


class DatabaseCache(metaclass=SingletonType):
  """Singleton Google Sheets cache for the schedule and processing-log tabs.

  Reads go through `_read_write_lock`; Sheets writes are never issued directly but queued into four
  batch bodies (before-write structural, RAW values, USER_ENTERED values, after-write formatting) that
  `api_write` flushes in that order. `_api_write_lock` serialises API traffic, `_db_write_queue_lock`
  guards the queue bodies, the client is re-authorised every `reauth_interval` seconds and calls are
  spaced at least `api_call_min_interval` seconds apart.
  """

  _database_id = SETTINGS.database_id

  _tab_id_schedule_base_sheet = SETTINGS.database_base_schedule_id

  _creds = Credentials.from_service_account_file(SETTINGS.google_api_key_file, scopes=DEFAULT_SCOPES)

  _schedule_tab_range = f"'Current Week'!R2C1:C{len(DatabaseScheduleColumns.all_columns())}"
  _prev_week_schedule_tab_range = f"'Previous Week'!R2C1:C{len(DatabaseScheduleColumns.all_columns())}"
  _order_log_tab_range = f"'Processing Log'!R2C1:C{len(DatabaseOrderLogColumns.all_columns())}"

  _schedule_range_format_single = "'Current Week'!{cell}"
  _prev_week_schedule_range_format_single = "'Previous Week'!{cell}"
  _order_log_range_format_single = "'Processing Log'!{cell}"

  _schedule_range_format = "'Current Week'!{start}:{end}"
  _prev_week_schedule_range_format = "'Previous Week'!{start}:{end}"
  _order_log_range_format = "'Processing Log'!{start}:{end}"

  _db_values_batch_update_body_base = ValuesBatchUpdateBody(
    valueInputOption=ValueInputOption.raw,
    includeValuesInResponse=False,
    responseValueRenderOption=ValueRenderOption.unformatted,
    responseDateTimeRenderOption=DateTimeOption.formatted_string,
    data=[],
  )

  _db_values_batch_update_body_base_userentered = ValuesBatchUpdateBody(
    valueInputOption=ValueInputOption.user_entered,
    includeValuesInResponse=False,
    responseValueRenderOption=ValueRenderOption.unformatted,
    responseDateTimeRenderOption=DateTimeOption.formatted_string,
    data=[],
  )

  _db_batch_update_body_base = BatchUpdateBody(
    requests=[],
  )

  reauth_interval = 2700
  api_call_min_interval = 1.1

  schedule: CacheViewSchedule
  prev_week_schedule: CacheViewSchedule
  order_log: CacheViewOrderLog

  def __init__(self) -> None:
    """Create the locks and empty write queues, then push the header row to every tab."""
    self.schedule: CacheViewSchedule
    self.prev_week_schedule: CacheViewSchedule
    self.order_log: CacheViewOrderLog

    self._read_write_lock = RWLock()
    # self._read_write_lock = RWLock(fast=True)

    self._api_write_lock = Lock()
    self._db_write_queue_lock = Lock()

    self._client = None
    self._client_last_auth_time = None

    self._api_last_call_time = None

    self._values_batch_update_raw_body: ValuesBatchUpdateBody | None = None
    self._values_batch_update_user_entered_body: ValuesBatchUpdateBody | None = None
    self._before_write_update_body: BatchUpdateBody | None = None
    self._after_write_update_body: BatchUpdateBody | None = None

    self.loop = get_running_loop()

    self.update_db_header()
    super().__init__()

  @property
  def client(self) -> Client:
    """Authorised gspread client, re-created once `reauth_interval` seconds have passed since the last auth."""
    now = self.loop.time()
    if self._client is None or self._client_last_auth_time is None or ((self._client_last_auth_time + self.reauth_interval) < now):
      self._client = authorize(self._creds, http_client=BackOffHTTPClient)
      self._client_last_auth_time = self.loop.time()
    return self._client

  @property
  def queued_values_raw_updates(self) -> list[ValueRange]:
    """Pending RAW-input value ranges, creating the batch body on first access."""
    if self._values_batch_update_raw_body is None:
      self._values_batch_update_raw_body = deepcopy(self._db_values_batch_update_body_base)
    return self._values_batch_update_raw_body["data"]

  @property
  def queued_values_user_entered_updates(self) -> list[ValueRange]:
    """Pending USER_ENTERED value ranges, creating the batch body on first access."""
    if self._values_batch_update_user_entered_body is None:
      self._values_batch_update_user_entered_body = deepcopy(self._db_values_batch_update_body_base_userentered)
    return self._values_batch_update_user_entered_body["data"]

  @property
  def queued_before_write_update_requests(self) -> list[Request]:
    """Pending structural requests flushed before the value writes, creating the batch body on first access."""
    if self._before_write_update_body is None:
      self._before_write_update_body = deepcopy(self._db_batch_update_body_base)
    return self._before_write_update_body["requests"]

  @property
  def queued_after_write_update_requests(self) -> list[Request]:
    """Pending formatting requests flushed after the value writes, creating the batch body on first access."""
    if self._after_write_update_body is None:
      self._after_write_update_body = deepcopy(self._db_batch_update_body_base)
    return self._after_write_update_body["requests"]

  @property
  def get_week_ending_name(self) -> str:
    """Tab title naming the second most recent Saturday (today inclusive) as the week-ending date."""
    last_saturday = today(tzinfo=SETTINGS.tz) + relativedelta(weekday=SA(-2))
    return f"Week Ending {last_saturday.year:0>4}/{last_saturday.month:0>2}/{last_saturday.day:0>2}"

  async def queue_db_api_values_raw_update(self, value: ValueRange) -> None:
    """Append a RAW-input value range to the write queue."""
    async with self._db_write_queue_lock:
      if self._values_batch_update_raw_body is None:
        self._values_batch_update_raw_body = deepcopy(self._db_values_batch_update_body_base)
      self._values_batch_update_raw_body["data"].append(value)

  async def queue_db_api_values_user_entered_update(self, value: ValueRange) -> None:
    """Append a USER_ENTERED value range to the write queue."""
    async with self._db_write_queue_lock:
      if self._values_batch_update_user_entered_body is None:
        self._values_batch_update_user_entered_body = deepcopy(self._db_values_batch_update_body_base_userentered)
      self._values_batch_update_user_entered_body["data"].append(value)

  async def queue_db_api_before_write_update(self, request: Request) -> None:
    """Queue a structural request to run before the queued value writes."""
    async with self._db_write_queue_lock:
      if self._before_write_update_body is None:
        self._before_write_update_body = deepcopy(self._db_batch_update_body_base)
      self._before_write_update_body["requests"].append(request)

  async def queue_db_api_after_write_update(self, format_request: Request) -> None:
    """Queue a formatting request to run after the queued value writes."""
    async with self._db_write_queue_lock:
      if self._after_write_update_body is None:
        self._after_write_update_body = deepcopy(self._db_batch_update_body_base)
      self._after_write_update_body["requests"].append(format_request)

  def update_db_header(self) -> None:
    """Synchronously write the column header row to all three tabs."""
    body = deepcopy(self._db_values_batch_update_body_base)

    body["data"].append(
      ValueRange(
        range=self._schedule_range_format.format(start="R1C1", end=f"R1C{len(DatabaseScheduleColumns.all_columns())}"),
        majorDimension=Dimension.rows,
        values=[DatabaseScheduleColumns.all_columns()],
      )
    )
    body["data"].append(
      ValueRange(
        range=self._prev_week_schedule_range_format.format(start="R1C1", end=f"R1C{len(DatabaseScheduleColumns.all_columns())}"),
        majorDimension=Dimension.rows,
        values=[DatabaseScheduleColumns.all_columns()],
      )
    )
    body["data"].append(
      ValueRange(
        range=self._order_log_range_format.format(start="R1C1", end=f"R1C{len(DatabaseOrderLogColumns.all_columns())}"),
        majorDimension=Dimension.rows,
        values=[DatabaseOrderLogColumns.all_columns()],
      )
    )

    self.client.http_client.values_batch_update(id=self._database_id, body=body)

  async def wait_for_api(self) -> None:
    """Sleep until at least `api_call_min_interval` seconds have passed since the last recorded API call."""
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
    """Re-read all three tabs and rebuild the cache views.

    Held under both the API write lock and the writer lock so neither a flush nor a reader can observe a
    half-built cache. Previous Week dropoff/pickup times are shifted back one week. A Processing Log with
    no data rows omits `values` from the response, which yields an empty view.
    """
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
      self.schedule = CacheViewSchedule(
        raw_data=schedule_data,
        cache_core=self,
        sheet_id=None,
      )

      prev_week_schedule_data: list[list[str | int | float]] = result["valueRanges"][1]["values"]
      self.prev_week_schedule = CacheViewSchedule(
        raw_data=prev_week_schedule_data,
        cache_core=self,
        sheet_id=None,
      )

      self.prev_week_schedule._cache.loc[:, DatabaseScheduleColumns.invoice_dropoff_time] = self.prev_week_schedule._cache.loc[
        :, DatabaseScheduleColumns.invoice_dropoff_time
      ].map(lambda x: x - relativedelta(weeks=1) if x else x)  # pyright: ignore[reportOperatorIssue]
      self.prev_week_schedule._cache.loc[:, DatabaseScheduleColumns.invoice_pickup_time] = self.prev_week_schedule._cache.loc[
        :, DatabaseScheduleColumns.invoice_pickup_time
      ].map(lambda x: x - relativedelta(weeks=1) if x else x)  # pyright: ignore[reportOperatorIssue]

      try:
        order_log_data: list[list[str | int | float]] = result["valueRanges"][2]["values"]
        self.order_log = CacheViewOrderLog(
          raw_data=order_log_data,
          cache_core=self,
          sheet_id=SETTINGS.database_order_log_id,
        )
      except KeyError:
        self.order_log = CacheViewOrderLog(
          raw_data=[],
          cache_core=self,
          sheet_id=SETTINGS.database_order_log_id,
        )

      # self._original_sizes = {
      #   self.schedule._range_format: (len(self.schedule._cache), len(self.schedule.columns)),
      #   self.prev_week_schedule._range_format: (
      #     len(self.prev_week_schedule._cache),
      #     len(self.prev_week_schedule.columns),
      #   ),
      #   self.order_log._range_format: (len(self.order_log._cache), len(self.order_log.columns)),
      # }

      # self.schedule._cache.to_csv("debug_schedule.csv")
      # self.prev_week_schedule._cache.to_csv("debug_prev_week_schedule.csv")
      # self.order_log._cache.to_csv("debug_order_log.csv")
      # pass

  async def submit_queued_writes_to_pool(self) -> None:
    """Flush queued writes on a worker thread if any queue is non-empty."""
    if (
      self.queued_values_raw_updates
      or self.queued_values_user_entered_updates
      or self.queued_before_write_update_requests
      or self.queued_after_write_update_requests
    ):
      await to_thread(self.api_write)

  async def has_pending_writes(self) -> bool:
    """Whether any write queue holds entries, checked under the queue lock."""
    async with self._db_write_queue_lock:
      has_raw = self._values_batch_update_raw_body is not None and bool(self._values_batch_update_raw_body["data"])
      has_user_entered = self._values_batch_update_user_entered_body is not None and bool(
        self._values_batch_update_user_entered_body["data"]
      )
      has_before = self._before_write_update_body is not None and bool(self._before_write_update_body["requests"])
      has_after = self._after_write_update_body is not None and bool(self._after_write_update_body["requests"])
      return has_raw or has_user_entered or has_before or has_after

  def api_write(self):
    """Flush the queues in order: before-write structural, RAW values, USER_ENTERED values, after-write formatting.

    Blocking; runs under both locks so queueing pauses until the flush completes, and each body is reset only
    after its call succeeds. Exceptions are logged here because this runs off the main thread.
    """
    try:
      with self._api_write_lock, self._db_write_queue_lock:
        if self.queued_before_write_update_requests:
          self.client.http_client.batch_update(
            id=self._database_id,
            body=self._before_write_update_body,
          )

          self._before_write_update_body = None  # Reset the update body after writing

        if self.queued_values_raw_updates:
          self.client.http_client.values_batch_update(
            id=self._database_id,
            body=self._values_batch_update_raw_body,
          )

          self._values_batch_update_raw_body = None  # Reset the update body after writing

        if self.queued_values_user_entered_updates:
          self.client.http_client.values_batch_update(
            id=self._database_id,
            body=self._values_batch_update_user_entered_body,
          )

          self._values_batch_update_user_entered_body = None  # Reset the update body after writing

        if self.queued_after_write_update_requests:
          self.client.http_client.batch_update(
            id=self._database_id,
            body=self._after_write_update_body,
          )

          self._after_write_update_body = None  # Reset the update body after writing
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception:
      logger.exception("Error during API write")
      raise

  async def flip_to_new_week(self):
    """Rotate the schedule tabs at week end.

    Flushes pending writes, hides Previous Week under its week-ending name, renames Current Week to Previous
    Week, duplicates the base template as the new protected Current Week, then refreshes the cache. A no-op
    (with a warning) when a tab already carries this week's ending name.
    """
    await self.submit_queued_writes_to_pool()

    async with self._api_write_lock, self._db_write_queue_lock:
      await self.wait_for_api()

      metadat = self.client.http_client.fetch_sheet_metadata(self._database_id)

      current_week_sheet = finditem(lambda x: x["properties"]["title"] == "Current Week", metadat["sheets"])
      previous_week_sheet = finditem(lambda x: x["properties"]["title"] == "Previous Week", metadat["sheets"])

      all_sheet_ids = [int(s["properties"]["sheetId"]) for s in metadat["sheets"]]

      new_sheet_id = max(all_sheet_ids) + 1

      with suppress(StopIteration):
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


class CacheViewBase[ModelT: CustomBaseModel, ColsT: ColNameEnum, IndexT: tuple[Any, ...] | Any](ABC):
  """Typed DataFrame view over one tab, sharing the core's reader/writer lock.

  Entering as an async context manager holds the reader lock and yields the raw DataFrame. Writes update
  the cache under the writer lock and queue the matching Sheets update on the core.
  """

  _range_format: str
  _range_format_single: str
  _field_type_adapters: dict[str, TypeAdapter[Any]]
  columns: type[ColsT]
  model: type[ModelT]

  def __init__(self, raw_data: list[list[str | int | float]], cache_core: DatabaseCache, sheet_id: int | None) -> None:
    """Build the typed DataFrame from raw tab rows."""
    self._cache = build_typed_dataframe(data=raw_data, columns=self.columns, types_model=self.model)
    self._cache_index = self.columns.__index_items__
    self._core = cache_core
    self._sheet_id = sheet_id
    super().__init__()

  async def __aenter__(self) -> DataFrame:
    """Acquire the shared reader lock and return the cached DataFrame."""
    await self._core._read_write_lock.reader_lock.acquire()
    return self._cache

  async def __aexit__(
    self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
  ) -> None:
    """Release the shared reader lock."""
    self._core._read_write_lock.reader_lock.release()

  async def read_typed_row(self, index: IndexT, re_validate: bool = False) -> ModelT:
    """Return the row at `index` as a model, validating only when `re_validate` is set."""
    async with self._core._read_write_lock.reader_lock:
      row = self._cache.iloc[await self.get_rownum(index)]

    return self.model(**row) if re_validate else self.model.model_construct(**row)

  async def walk_typed_rows(self, re_validate: bool = False) -> AsyncIterator[ModelT]:
    """Yield every cached row as a model while holding the reader lock."""
    async with self._core._read_write_lock.reader_lock:
      for _, row in self._cache.iterrows():
        if re_validate:
          yield self.model(**row)
        else:
          yield self.model.model_construct(**row)

  @overload
  async def read_value(self, index: IndexT, col: ColsT, validate: Literal[False] = False) -> Any: ...

  @overload
  async def read_value(self, index: IndexT, col: ColsT, validate: Literal[True]) -> tuple[TypeAdapter[Any], Any]: ...

  async def read_value(self, index: IndexT, col: ColsT, validate: bool = False) -> tuple[TypeAdapter[Any], Any] | Any:
    """Read one cell, paired with its column's type adapter when `validate` is set."""
    async with self._core._read_write_lock.reader_lock:
      value = self._cache.iat[await self.get_rownum(index), self._cache.columns.get_loc(col)]  # type: ignore
      if validate:
        ta = self._field_type_adapters.get(col)
        if ta is None:
          raise RuntimeError("how the fuck")
        return (ta, value)
      return value

  async def process_index(self, index: IndexT) -> IndexT:
    """Hook for subclasses to normalise an index before lookup; identity by default."""
    return index

  async def get_rownum(self, index: IndexT) -> int:
    """Resolve `index` to a single positional row number, raising IndexError for partial or multi-row matches."""
    # default process index. Assume single index
    async with self._core._read_write_lock.reader_lock:
      # locate the row number of index
      row_number = self._cache.index.get_loc(await self.process_index(index))

    if isinstance(row_number, slice):
      row_number = int(row_number.stop - row_number.start)
    if (not isinstance(row_number, int)) or (0 < row_number < 1):
      raise IndexError("Index provided is either a partial index, or otherwise fails to return a single row")

    return row_number

  async def write_value(self, index: IndexT, column: ColsT, value: Any, ta: TypeAdapter[Any]) -> None:
    """Write one cell to the cache and queue the RAW Sheets update for it."""
    row_number = await self.get_rownum(index)

    value = ta.dump_python(value)
    sheets_value = ta.dump_python(value, mode="json")

    async with self._core._read_write_lock.writer_lock:
      self._cache.at[index, column] = value

    row_number += 2  # add one to account for gsheets header

    # get column index

    await self._core.queue_db_api_values_raw_update(
      ValueRange(
        range=self._range_format_single.format(cell=f"R{row_number}C{self.columns.get_enum_index(column) + 1}"),
        majorDimension=Dimension.rows,
        values=[[sheets_value]],
      )
    )

  async def update_row(self, index: IndexT, values: Sequence[Any] | ModelT, raw: bool = True) -> None:
    """Replace a whole row from a sequence or model and queue the Sheets update as RAW or USER_ENTERED."""
    row_number = await self.get_rownum(index)

    if isinstance(values, self.model):
      row = Series(values.model_dump(), dtype=object)
      sheets_row = Series(values.model_dump(mode="json"), dtype=object)
    elif isinstance(values, Sequence):
      row = Series(values, dtype=object, index=self.columns.all_columns())
      sheets_row = row.copy()
    else:
      raise TypeError(f"{type(values)} does not match the expected type of Sequence[Any] or {self.model}")

    async with self._core._read_write_lock.writer_lock:
      self._cache.iloc[await self.get_rownum(index), :] = row

    row_number += 2  # add one to account for gsheets header

    # get column index

    update_data = ValueRange(
      range=self._range_format.format(start=f"R{row_number}C1", end=f"C{len(self.columns)}"),
      majorDimension=Dimension.rows,
      values=[sheets_row.tolist()],
    )

    if raw:
      await self._core.queue_db_api_values_raw_update(update_data)
    else:
      await self._core.queue_db_api_values_user_entered_update(update_data)

  async def append_row(self, values: ModelT, raw: bool = True) -> None:
    """Add a row to the cache and queue an appendDimension followed by the row write; requires a sheet id."""
    if self._sheet_id is None:
      raise RuntimeError("This cache view does not support appending rows")
    assert len(self._cache_index) > 0, "Cache view must have at least one column in index to append rows"
    index = (
      tuple(getattr(values, col) for col in self._cache_index) if len(self._cache_index) > 1 else getattr(values, self._cache_index[0])
    )

    row = Series(values.model_dump(), dtype=object)
    sheets_row = Series(values.model_dump(mode="json"), dtype=object)

    async with self._core._read_write_lock.writer_lock:
      self._cache.loc[index, :] = row

    row_number = await self.get_rownum(cast("IndexT", index))
    row_number += 2  # add one to account for gsheets header

    # get column index

    update_data = ValueRange(
      range=self._range_format.format(start=f"R{row_number}C1", end=f"C{len(self.columns)}"),
      majorDimension=Dimension.rows,
      values=[sheets_row.tolist()],
    )

    await self._core.queue_db_api_before_write_update(
      {
        "appendDimension": AppendDimension(
          sheetId=self._sheet_id,
          dimension=Dimension.rows,
          length=1,
        )
      }
    )

    if raw:
      await self._core.queue_db_api_values_raw_update(update_data)
    else:
      await self._core.queue_db_api_values_user_entered_update(update_data)

  async def check_exists(self, index: IndexT) -> bool:
    """Whether `index` is present in the cache."""
    async with self._core._read_write_lock.reader_lock:
      return index in self._cache.index


class CacheViewSchedule(CacheViewBase["ScheduledOrderDBEntryModel", "DatabaseScheduleColumns", "DatabaseScheduleIndex"]):
  """View over the Current Week schedule tab."""

  _range_format_single = "Current Week!{cell}"
  _range_format = "Current Week!{start}:{end}"
  _field_type_adapters = SCHEDULE_TYPE_ADAPTERS
  columns = DatabaseScheduleColumns
  model = ScheduledOrderDBEntryModel

  async def check_box(
    self, idx: DatabaseScheduleIndex, col: Literal[DatabaseScheduleColumns.invoice_grabbed, DatabaseScheduleColumns.invoice_applied]
  ) -> None:
    """Set a checkbox column to True for the row at `idx`."""
    await self.write_value(index=idx, column=col, value=True, ta=self._field_type_adapters[col])

  async def uncheck_box(
    self, idx: DatabaseScheduleIndex, col: Literal[DatabaseScheduleColumns.invoice_grabbed, DatabaseScheduleColumns.invoice_applied]
  ) -> None:
    """Set a checkbox column to False for the row at `idx`."""
    await self.write_value(index=idx, column=col, value=False, ta=self._field_type_adapters[col])

  async def check_toggled(
    self,
    idx: DatabaseScheduleIndex,
    col: Literal[
      DatabaseScheduleColumns.invoice_grabbed, DatabaseScheduleColumns.invoice_applied, DatabaseScheduleColumns.manually_moved
    ],
  ) -> bool:
    """Return the current state of a checkbox column for the row at `idx`."""
    return await self.read_value(idx, col)


class CacheViewOrderLog(CacheViewBase["OrderLogDBEntryModel", "DatabaseOrderLogColumns", "DatabaseOrderLogIndex"]):
  """View over the Processing Log tab; supports appending rows and fixing their formatting."""

  _range_format_single = "'Processing Log'!{cell}"
  _range_format = "'Processing Log'!{start}:{end}"
  _field_type_adapters = ORDER_LOG_TYPE_ADAPTERS
  columns = DatabaseOrderLogColumns
  model = OrderLogDBEntryModel

  async def log_action(  # noqa: PLR0917
    self,
    supplier: SuppliersEnum,
    store: StoreNum | None,
    invoice_num: InvoiceNum | None,
    customer: CustomerID | None,
    action: LogActionEnum,
    status: StatusCode,
    action_datetime: datetime,
    week_end_date: datetime | None,
    note: str | None = None,
  ) -> None:
    """Validate and append a processing-log entry, then queue its format fixes.

    A missing invoice number is replaced with a unique negative one below the lowest existing; a duplicate
    index raises IndexError.
    """
    if invoice_num is None:
      invoice_num = str(min(int(to_numeric(self._cache.loc[:, self.columns.invoice_number], errors="coerce").min()), 0) - 1)
    new_entry = {
      "supplier": supplier,
      "store": store,
      "invoice_number": invoice_num,
      "customer": customer,
      "action": action,
      "status": status,
      "action_datetime": action_datetime,
      "week_end_date": week_end_date,
      "notes": note,
    }

    validated_entry = self.model(**new_entry)

    # assemble new index
    idx = tuple(getattr(validated_entry, col) for col in self._cache_index)

    # ensure this index doesn't already exist
    result = await self.check_exists(idx)

    if result:
      raise IndexError(f"Error appending new log to processing logs: Index already exists! {idx}", idx, validated_entry)

    await self.append_row(validated_entry, raw=False)
    await self.correct_format()

  async def correct_format(self):
    """Queue after-write requests restoring number formats on the store, invoice/customer and datetime columns and re-applying the basic filter."""
    if self._sheet_id is None:
      raise RuntimeError("This cache view does not support format updates")
    storenum_format = {
      "repeatCell": {
        "range": {"sheetId": self._sheet_id, "startRowIndex": 1, "startColumnIndex": 1, "endColumnIndex": 2},
        "cell": {
          "userEnteredFormat": {
            "numberFormat": {
              "type": "NUMBER",
              # "pattern": '"SFT"000',
            }
          },
        },
        "fields": "userEnteredFormat(numberFormat)",
      }
    }

    invoice_num_and_customer_format = {
      "repeatCell": {
        "range": {"sheetId": self._sheet_id, "startRowIndex": 1, "startColumnIndex": 2, "endColumnIndex": 4},
        "cell": {
          "userEnteredFormat": {"numberFormat": {"type": "TEXT"}},
        },
        "fields": "userEnteredFormat(numberFormat)",
      }
    }
    datetime_format = {
      "repeatCell": {
        "range": {"sheetId": self._sheet_id, "startRowIndex": 1, "startColumnIndex": 6, "endColumnIndex": 8},
        "cell": {
          "userEnteredFormat": {"numberFormat": {"type": "DATE_TIME"}},
        },
        "fields": "userEnteredFormat(numberFormat)",
      }
    }

    filter_update_request = {
      "setBasicFilter": {"filter": {"range": {"sheetId": self._sheet_id, "startRowIndex": 0, "startColumnIndex": 0}}}
    }

    await self._core.queue_db_api_after_write_update(storenum_format)
    await self._core.queue_db_api_after_write_update(invoice_num_and_customer_format)
    await self._core.queue_db_api_after_write_update(datetime_format)
    await self._core.queue_db_api_after_write_update(filter_update_request)
