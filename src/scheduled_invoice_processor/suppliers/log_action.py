# Standard library imports
from datetime import datetime
from functools import partial, wraps
from itertools import zip_longest
from typing import TYPE_CHECKING

# First party imports
from aeth_ext.utils import get_last_sat, get_next_sat
from scheduled_invoice_processor.environment_init_vars import HOST_NAME, SETTINGS
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Awaitable, Callable
  from logging import LoggerAdapter

  # First party imports
  from scheduled_invoice_processor.typing_custom import SupplierQueueKey

  # Local folder imports
  from . import SupplierProcessorBase
  from .file_register_data import FileRegisterData

type LogActionHandlerType = Callable[[SupplierQueueKey, StatusCode, "FileRegisterData"], None]


def log_action_handler(
  queue_key: SupplierQueueKey,
  status: StatusCode,
  file_meta: FileRegisterData,
  items_to_log: dict[SupplierQueueKey, tuple[StatusCode, FileRegisterData]],
) -> None:
  """Handles logging of actions for a given supplier queue key, status, and file metadata."""
  items_to_log[queue_key] = (status, file_meta)
  pass


def log_actions[**TP, TR](
  action_identifier_prefix: LogActionEnum,
) -> Callable[
  [Callable[TP, Awaitable[TR]]],
  Callable[TP, Awaitable[TR]],
]:
  def decorator(
    func: Callable[TP, Awaitable[TR]],
  ) -> Callable[TP, Awaitable[TR]]:
    @wraps(func)
    async def wrapper(*args: TP.args, **kwargs: TP.kwargs) -> TR:
      self_obj: SupplierProcessorBase = args[0]  # type: ignore
      adapted_logger: LoggerAdapter = kwargs["adapted_logger"]  # type: ignore

      # identifier = self_obj.ctx_var_identifier.get()
      log_file_loc = self_obj.ctx_var_log_loc.get()

      # log_file_loc = log_file_loc / f"{identifier}.txt" if identifier is not None and log_file_loc is not None else None

      items_to_log: dict[SupplierQueueKey, tuple[StatusCode, FileRegisterData]] = {}

      handler = partial(log_action_handler, items_to_log=items_to_log)

      note = f"https://{HOST_NAME}/{'/'.join(log_file_loc.parts[4:])}" if log_file_loc is not None else "Nothing logged"

      kwargs["log_action_handler"] = handler

      try:
        result = await func(*args, **kwargs)
      except Exception as e:
        adapted_logger.exception("Exception in %s", func.__name__)
        self_obj.errored = True
        await self_obj.cache.order_log.log_action(
          supplier=self_obj.supplier_name,
          store=kwargs.get("storenum"),  # type: ignore
          invoice_num=None,
          customer=kwargs.get("customer_id"),  # type: ignore
          action=action_identifier_prefix,
          status=StatusCode.FAILURE,
          action_datetime=datetime.now(SETTINGS.tz),
          week_end_date=None,
          note=note if log_file_loc is not None and log_file_loc.exists() else "Nothing logged",
        )
        raise e

      for status, file_meta in items_to_log.values():
        if status == StatusCode.SKIPPED:
          continue  # Skip logging for items marked as SKIPPED
        params = {
          "supplier": self_obj.supplier_name,
          "store": file_meta.storenum,
          "invoice_num": "Not yet known",
          "customer": file_meta.customer_id,
          "action": action_identifier_prefix,
          "status": status,
          "action_datetime": datetime.now(SETTINGS.tz),
          "week_end_date": (get_next_sat(file_meta.pickup_date) if file_meta.current_week else get_last_sat(file_meta.pickup_date)),
          "note": note if log_file_loc is not None and log_file_loc.exists() else "Nothing logged",
        }
        if file_meta.invoice_nums:
          for _, invoice_num in zip_longest(file_meta.file_names, file_meta.invoice_nums.values(), fillvalue=""):
            params["invoice_num"] = invoice_num
            await self_obj.cache.order_log.log_action(**params)
        else:
          await self_obj.cache.order_log.log_action(**params)

      return result

    return wrapper

  return decorator
