if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from enum import StrEnum
from logging import getLogger
from typing import Any, Self

from typing_custom import CustomerID, InvoiceNum, StoreNum
from typing_custom.enums import LogActionEnum, SuppliersEnum

logger = getLogger(__name__)


class ColNameEnum(StrEnum):
  __exclude__: list[str] = []
  __init_include__: list[str] = []
  __index_items__: list[str] = []

  @classmethod
  def ordered_column_names(cls, *columns: str) -> list[str]:
    columns_list = [str(column) for column in columns]
    return [str(column) for column in cls if str(column) in columns_list]

  @classmethod
  def all_columns(cls) -> list[str]:
    return [str(column) for column in cls if str(column) not in cls.__exclude__ and not str(column).startswith("_")]

  @classmethod
  def err_reporting_columns(cls) -> list[str]:
    """
    Return all columns that are not excluded and do not start with an underscore.
    This is used for error reporting.
    """
    return [
      "err_field_name",
      "err_reason",
      *[str(column) for column in cls if str(column) not in cls.__exclude__ and not str(column).startswith("_")],
    ]

  @classmethod
  def init_columns(cls) -> list[str]:
    if not cls.__init_include__:
      return cls.all_columns()
    return [str(column) for column in cls if str(column) in cls.__init_include__ and not str(column).startswith("_")]

  @classmethod
  def testing_columns(cls) -> list[str]:
    return [str(column) for column in cls if str(column) not in cls.__exclude__]

  @classmethod
  def true_all_columns(cls) -> list[str]:
    return [str(column) for column in cls]

  @classmethod
  def get_enum_index(cls, value: Self) -> int:
    return list(cls).index(value)

  @staticmethod
  def _generate_next_value_(name, start, count, last_values) -> Any:
    """
    Return the member name.
    """
    return name


class DatabaseScheduleColumns(ColNameEnum):
  __index_items__ = ["supplier", "store"]

  supplier = "supplier"
  store = "store"
  customer = "customer"
  state = "state"
  expected_delivery_day = "expected_delivery_day"
  invoice_pickup_time = "invoice_pickup_time"
  invoice_dropoff_time = "invoice_dropoff_time"
  invoice_grabbed = "invoice_grabbed"
  invoice_applied = "invoice_applied"


type DatabaseScheduleIndex = tuple[SuppliersEnum, StoreNum]


class DatabaseOrderLogColumns(ColNameEnum):
  __index_items__ = ["supplier", "store", "invoice_number", "customer", "action", "status", "action_datetime"]

  supplier = "supplier"
  store = "store"
  invoice_number = "invoice_number"
  customer = "customer"
  action = "action"
  status = "status"
  action_datetime = "action_datetime"
  week_end_date = "week_end_date"
  notes = "notes"


type DatabaseOrderLogIndex = (
  tuple[SuppliersEnum, StoreNum, InvoiceNum, CustomerID, LogActionEnum]
  | tuple[SuppliersEnum, StoreNum, InvoiceNum, slice, LogActionEnum]
  | tuple[SuppliersEnum, slice, InvoiceNum, CustomerID, LogActionEnum]
)
