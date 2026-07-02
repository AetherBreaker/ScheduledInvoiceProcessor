# pyright: reportImportCycles=false
# Standard library imports
import logging
from datetime import datetime
from functools import wraps
from queue import Queue
from secrets import token_urlsafe
from typing import TYPE_CHECKING, Literal, override

# First party imports
from aeth_ext.logging.bases import CustomTimedRotatingFileHandler, NamedLogRecord
from aeth_ext.logging.config import BaseLoggingConfig, QueueCatchall, get_preferred_logrecord_formatter

# Local folder imports
from .environment_init_vars import SETTINGS

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Awaitable, Callable, Sequence

  # Third party imports
  from rich.console import Console

  # Local folder imports
  from .suppliers import SupplierProcessorBase
  from .typing_custom.enums import LogActionEnum

SCHEDULER_LOG_LOC = SETTINGS.log_loc_folder / "scheduler_logs"
APSCHEDULER_DEBUG_LOG_LOC = SETTINGS.log_loc_folder / "scheduler_debug.txt"
APSCHEDULER_INFO_LOG_LOC = SETTINGS.log_loc_folder / "scheduler.txt"


class ContextFilter(logging.Filter):
  def __init__(self, identifier: str):
    super().__init__()
    self.identifier = identifier

  @override
  def filter(self, record: NamedLogRecord) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
    try:
      return record.ctx.get() == self.identifier  # pyright: ignore[reportAttributeAccessIssue]
    except AttributeError:
      return False


class LoggingConfig(BaseLoggingConfig):
  @override
  @classmethod
  def configure_logging_main(
    cls,
    rich_console: Console,
    project_name: str,
    logging_type: Literal["daily", "per_run"] = "daily",
    logging_base_name: str | None = None,
    default_max_width: int | None = None,
    timestamp_format: str = "%b, %d %a %I:%M %p",
    log_to_console: bool | Literal["rich"] = "rich",
    queue_console_handler: bool = False,
    logging_queues: Sequence[QueueCatchall] | None = None,
  ):
    super().configure_logging_main(
      rich_console=rich_console,
      project_name=project_name,
      logging_type=logging_type,
      logging_base_name=logging_base_name,
      default_max_width=default_max_width,
      timestamp_format=timestamp_format,
      log_to_console=log_to_console,
      queue_console_handler=queue_console_handler,
      logging_queues=logging_queues,
    )

    # Standard library imports
    import atexit
    from logging.handlers import QueueHandler, QueueListener

    SCHEDULER_LOG_LOC.mkdir(exist_ok=True, parents=True)

    root = logging.getLogger()

    scheduler = logging.getLogger("apscheduler")
    scheduler.propagate = False

    scheduler_debug_handler = CustomTimedRotatingFileHandler(APSCHEDULER_DEBUG_LOG_LOC, when="midnight", backupCount=14, delay=True)
    scheduler_info_handler = CustomTimedRotatingFileHandler(APSCHEDULER_INFO_LOG_LOC, when="midnight", backupCount=14, delay=True)

    scheduler_debug_handler.setLevel(logging.DEBUG)
    scheduler_info_handler.setLevel(logging.INFO)

    formatter = get_preferred_logrecord_formatter()

    scheduler_debug_handler.setFormatter(formatter)
    scheduler_info_handler.setFormatter(formatter)

    log_queue = Queue(-1)
    scheduler_log_queue = Queue(-1)

    queue_handler = QueueHandler(log_queue)
    scheduler_queue_handler = QueueHandler(scheduler_log_queue)

    scheduler_queue_listener = QueueListener(
      scheduler_log_queue,
      scheduler_debug_handler,
      scheduler_info_handler,
      respect_handler_level=True,
    )

    root.addHandler(queue_handler)
    scheduler.addHandler(scheduler_queue_handler)

    scheduler_queue_listener.start()

    atexit.register(scheduler_queue_listener.stop)


def add_log_context[**TP, TR](
  action_identifier_prefix: LogActionEnum,
  log_subfolder: str | None = None,
) -> Callable[
  [Callable[TP, Awaitable[TR]]],
  Callable[TP, Awaitable[TR]],
]:

  def add_log_context_under(
    func: Callable[TP, Awaitable[TR]],
  ) -> Callable[TP, Awaitable[TR]]:

    file_formatter = get_preferred_logrecord_formatter()

    @wraps(func)
    async def add_log_context_wrapper(*args: TP.args, **kwargs: TP.kwargs) -> TR:
      self_obj: SupplierProcessorBase = args[0]  # type: ignore

      set_ctx_var_identifier = self_obj.ctx_var_identifier
      set_ctx_var_log_loc = self_obj.ctx_var_log_loc

      unique_id = "".join([c for c in token_urlsafe(10) if c.isalnum()])
      now = datetime.now(tz=SETTINGS.tz).strftime("%Y%m%d_%H%M%S%f")

      identifier = f"{self_obj.identifier_prefix}_{action_identifier_prefix}_{now}_{unique_id}"

      log_loc_final = self_obj.log_file_loc
      if log_subfolder is not None:
        log_loc_final = log_loc_final / log_subfolder
      log_loc_final.mkdir(exist_ok=True, parents=True)

      log_file_loc = log_loc_final / f"{identifier}.txt"

      with set_ctx_var_identifier.set(identifier), set_ctx_var_log_loc.set(log_file_loc):
        logger = logging.getLogger(func.__module__)
        adapted_logger = logging.LoggerAdapter(
          logging.getLogger(func.__module__), extra={"ctx": set_ctx_var_identifier}, merge_extra=True
        )

        context_file_handler = logging.FileHandler(log_file_loc)

        context_file_handler.addFilter(ContextFilter(identifier))
        context_file_handler.setFormatter(file_formatter)
        logger.addHandler(context_file_handler)

        kwargs["adapted_logger"] = adapted_logger

        try:
          result = await func(*args, **kwargs)
        finally:
          context_file_handler.close()
          logger.removeHandler(context_file_handler)

          # check if log_file_loc is blank
          if log_file_loc.stat().st_size == 0:
            log_file_loc.unlink()

          # check if the log file contains the word "error" or "warning" (case insensitive) and unlink it if it does not
          if log_file_loc.exists():
            with log_file_loc.open() as f:
              contents = f.read().lower()
            if "error" not in contents and "warning" not in contents:
              log_file_loc.unlink()

      return result

    return add_log_context_wrapper

  return add_log_context_under
