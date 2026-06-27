# pyright: reportUnnecessaryIsInstance=false
# pyright: reportUnreachable=false
# Standard library imports
from datetime import datetime
from inspect import get_annotations
from logging import getLogger
from re import compile
from typing import Annotated, TypeAliasType

# Third party imports
from dateutil.relativedelta import FR, MO, SA, SU, TH, TU, WE, relativedelta
from pydantic import BeforeValidator, TypeAdapter

# First party imports
from aeth_ext.utils import today
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom import CustomerID, InvoiceNum, StoreNum  # noqa: TC001
from scheduled_invoice_processor.typing_custom.enums import (  # noqa: TC001
  LogActionEnum,
  StateEnum,
  StatusCode,
  SuppliersEnum,
  WeekdayEnum,
)
from scheduled_invoice_processor.validation import PYDANTIC_CONFIG, CustomBaseModel, CustomRootModel

logger = getLogger(__name__)


BASE_TIMESTAMP = datetime(
  year=1899,
  month=12,
  day=30,
  tzinfo=SETTINGS.tz,
)


weekday_lookup = {
  "Monday": MO,
  "Tuesday": TU,
  "Wednesday": WE,
  "Thursday": TH,
  "Friday": FR,
  "Saturday": SA,
  "Sunday": SU,
}

TIMESTAMP_PATTERN = compile(r"(?P<Weekday>\w*?) (?P<Hour>\d{1,2}):(?P<Minute>\d{2})(?P<Period>AM|PM)")


def process_formatted_time_pattern_str(target_time: str) -> datetime:
  if isinstance(target_time, (datetime, int, float)):
    raise ValueError("Expected a string for time pattern, got datetime")
  match = TIMESTAMP_PATTERN.match(target_time) if target_time else None

  if not match:
    raise ValueError(f"Time string '{target_time}' does not match expected format 'Weekday HH:MM(AM/PM)'")
  now = today(tzinfo=SETTINGS.tz)

  next_sunday = now + relativedelta(weekday=SU)

  weekday = weekday_lookup.get(match.group("Weekday"))
  if not weekday:
    raise ValueError(f"Invalid weekday: {match.group('Weekday')}")

  hour = int(match.group("Hour"))
  period = match.group("Period")
  # Convert 12-hour format to 24-hour format
  if period == "PM" and hour != 12:  # noqa: PLR2004
    hour += 12
  elif period == "AM" and hour == 12:  # noqa: PLR2004
    hour = 0
  minute = int(match.group("Minute"))

  result = now + relativedelta(weekday=weekday(+1), hour=hour, minute=minute)
  if result >= next_sunday:
    result -= relativedelta(weeks=1)

  return result


class ScheduleValidationError(ValueError):
  pass


class ScheduledOrderDBEntryModel(CustomBaseModel):
  supplier: SuppliersEnum
  store: StoreNum
  customer: CustomerID
  state: StateEnum
  expected_delivery_day: Annotated[WeekdayEnum, BeforeValidator(str.strip), BeforeValidator(str.title)]
  invoice_pickup_time: Annotated[datetime, BeforeValidator(process_formatted_time_pattern_str)]
  invoice_dropoff_time: Annotated[datetime, BeforeValidator(process_formatted_time_pattern_str)]
  invoice_grabbed: bool = False
  invoice_applied: bool = False
  manually_moved: bool = False


def remove_tz_info_if_aware(dt: datetime) -> datetime:
  if not isinstance(dt, datetime):
    raise ValueError("Expected a datetime object")
  if dt.tzinfo is not None:
    dt = dt.replace(tzinfo=None)
  return dt


def init_generic_datetime_str(dt: str) -> datetime:
  if not isinstance(dt, str):
    raise ValueError("Expected a string for datetime initialization")
  try:
    return datetime.strptime(dt, "%m/%d/%Y %H:%M:%S")  # noqa: DTZ007
  except ValueError as e:
    raise ValueError(f"Invalid datetime format: {dt}. Expected 'MM/DD/YYYY HH:MM:SS'") from e


class OrderLogDBEntryModel(CustomBaseModel):
  supplier: SuppliersEnum | None
  store: StoreNum | None
  invoice_number: InvoiceNum | None = None
  customer: CustomerID | None
  action: LogActionEnum | None
  status: StatusCode | None
  action_datetime: (
    Annotated[datetime, BeforeValidator(process_formatted_time_pattern_str)]
    | Annotated[datetime, BeforeValidator(remove_tz_info_if_aware)]
    | Annotated[datetime, BeforeValidator(init_generic_datetime_str)]
  ) | None
  week_end_date: (
    Annotated[datetime, BeforeValidator(process_formatted_time_pattern_str)]
    | Annotated[datetime, BeforeValidator(remove_tz_info_if_aware)]
    | Annotated[datetime, BeforeValidator(init_generic_datetime_str)]
  ) | None
  notes: str | None = None


SCHEDULE_TYPE_ADAPTERS = {}
for field, fieldinf in get_annotations(ScheduledOrderDBEntryModel).items():
  try:
    SCHEDULE_TYPE_ADAPTERS[field] = TypeAdapter(
      fieldinf,
      config=None
      if issubclass(fieldinf.__value__ if type(fieldinf) is TypeAliasType else fieldinf, (CustomBaseModel, CustomRootModel))
      else PYDANTIC_CONFIG,
    )
  except Exception:
    SCHEDULE_TYPE_ADAPTERS[field] = TypeAdapter(fieldinf, config=PYDANTIC_CONFIG)

ORDER_LOG_TYPE_ADAPTERS = {}
for field, fieldinf in get_annotations(OrderLogDBEntryModel).items():
  try:
    ORDER_LOG_TYPE_ADAPTERS[field] = TypeAdapter(
      fieldinf,
      config=None
      if issubclass(fieldinf.__value__ if type(fieldinf) is TypeAliasType else fieldinf, (CustomBaseModel, CustomRootModel))
      else PYDANTIC_CONFIG,
    )
  except Exception:
    ORDER_LOG_TYPE_ADAPTERS[field] = TypeAdapter(fieldinf, config=PYDANTIC_CONFIG)
