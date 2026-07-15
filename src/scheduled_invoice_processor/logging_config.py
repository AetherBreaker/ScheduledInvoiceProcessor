# pyright: reportImportCycles=false
# Standard library imports
import logging
from datetime import datetime
from functools import wraps
from secrets import token_urlsafe
from typing import TYPE_CHECKING, override

# First party imports
from aeth_ext.logging.bases import TaggedLogRecord
from aeth_ext.logging.setup import BaseLoggingConfig, get_preferred_logrecord_formatter

# Local folder imports
from .environment_init_vars import SETTINGS

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Awaitable, Callable

  # Local folder imports
  from .suppliers import SupplierProcessorBase
  from .typing_custom.enums import LogActionEnum


class ContextFilter(logging.Filter):
  def __init__(self, identifier: str):
    super().__init__()
    self.identifier = identifier

  @override
  def filter(self, record: TaggedLogRecord) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
    try:
      return record.ctx.get() == self.identifier  # pyright: ignore[reportAttributeAccessIssue]
    except AttributeError:
      return False


class LoggingConfig(BaseLoggingConfig):
  """Project logging configuration.

  All customization lives in the TOML override files shipped next to this
  module (discovered via the directory of ``__main__``):

  - ``logging_config.toml`` - local-mode apscheduler file split.
  - ``remote_logging_config.toml`` - the same split applied server-side by the
    central log server (merged into the remote config sent in the socket
    handshake).

  ``override_mode = "merge"`` merges those files onto the packaged aeth_ext
  defaults instead of replacing them.
  """

  override_mode = "merge"


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
