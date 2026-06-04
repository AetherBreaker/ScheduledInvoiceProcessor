from __future__ import annotations

import logging
from datetime import datetime
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from queue import Queue
from secrets import token_urlsafe
from sys import platform
from time import gmtime, localtime, strftime, time
from typing import TYPE_CHECKING

from environment_init_vars import SETTINGS
from rich.console import Console
from rich.logging import RichHandler

if TYPE_CHECKING:
  from collections.abc import Awaitable, Callable

  from rich.console import ConsoleRenderable
  from rich.traceback import Traceback
  from suppliers import SupplierProcessorBase
  from typing_custom.enums import LogActionEnum


RICH_CONSOLE = Console(
  width=None if platform == "win32" else 160,
  log_time=platform == "win32",
)


PROJECT_NAME = "ScheduledInvoiceProcessor"
LOGGING_BASE_NAME = "ScheduledInvoiceProcessor"

DEFAULT_MAX_WIDTH = 36


LOG_LOC_FOLDER = SETTINGS.persisted_dir_loc / "logs"
DEBUG_LOG_LOC = LOG_LOC_FOLDER / f"{LOGGING_BASE_NAME}_debug.txt"
INFO_LOG_LOC = LOG_LOC_FOLDER / f"{LOGGING_BASE_NAME}.txt"

SCHEDULER_LOG_LOC = LOG_LOC_FOLDER / "scheduler_logs"
APSCHEDULER_DEBUG_LOG_LOC = SCHEDULER_LOG_LOC / "scheduler_debug.txt"
APSCHEDULER_INFO_LOG_LOC = SCHEDULER_LOG_LOC / "scheduler.txt"


MAX_WIDTH_FILE = LOG_LOC_FOLDER / "max_width.txt"

LOGGING_TIMESTAMP_FORMAT = "%b, %d %a %I:%M %p"


class FixedRichHandler(RichHandler):
  def render(
    self,
    *,
    record: logging.LogRecord,
    traceback: Traceback | None,
    message_renderable: ConsoleRenderable,
  ) -> ConsoleRenderable:
    """Render log for display.

    Args:
        record (LogRecord): logging Record.
        traceback (Traceback | None): Traceback instance or None for no Traceback.
        message_renderable (ConsoleRenderable): Renderable (typically Text) containing log message contents.

    Returns:
        ConsoleRenderable: Renderable to display log.
    """

    pathpath = Path(record.pathname)

    if "site-packages" in pathpath.parts:
      libname_index = pathpath.parts.index("site-packages") + 1
    elif PROJECT_NAME in pathpath.parts:
      libname_index = pathpath.parts.index(PROJECT_NAME)
    elif "src" in pathpath.parts:
      libname_index = pathpath.parts.index("src")
    elif "Lib" in pathpath.parts:
      libname_index = pathpath.parts.index("Lib") + 1
    else:
      libname_index = 0

    path = ".".join(pathpath.parts[libname_index:])
    if "src." in path:
      path = path.split("src.", 1)[1]

    level = self.get_level_text(record)
    time_format = None if self.formatter is None else self.formatter.datefmt
    log_time = datetime.fromtimestamp(record.created)

    return self._log_render(
      self.console,
      [message_renderable, traceback] if traceback else [message_renderable],
      log_time=log_time,
      time_format=time_format,
      level=level,
      path=path,
      line_no=record.lineno,
      link_path=record.pathname if self.enable_link_path else None,
    )


class FixedLogRecord(logging.LogRecord):
  def __init__(self, *args, **kwargs):
    global DEFAULT_MAX_WIDTH
    pathpath = Path(args[2])

    if "site-packages" in pathpath.parts:
      libname_index = pathpath.parts.index("site-packages") + 1
      libname = pathpath.parts[libname_index]
    elif PROJECT_NAME in pathpath.parts:
      libname_index = pathpath.parts.index(PROJECT_NAME)
      libname = pathpath.parts[libname_index]
    elif "src" in pathpath.parts:
      libname_index = pathpath.parts.index("src")
      libname = pathpath.parts[libname_index]
    elif "Lib" in pathpath.parts:
      libname_index = pathpath.parts.index("Lib") + 1
      libname = pathpath.parts[libname_index]
    else:
      libname_index = 0
      libname = PROJECT_NAME

    libpath = ".".join(pathpath.parts[libname_index:])

    length = len(libpath)

    if length > DEFAULT_MAX_WIDTH:
      DEFAULT_MAX_WIDTH = length
      with MAX_WIDTH_FILE.open("w") as f:
        f.write(str(DEFAULT_MAX_WIDTH))

    self.libname = libname
    if "src." in libpath:
      libpath = libpath.split("src.", 1)[1]

    self.libpath = libpath

    super().__init__(*args, **kwargs)


class FixedFormatter(logging.Formatter):
  default_msec_format = None

  def formatTime(self, record, datefmt=None):
    """
    Return the creation time of the specified LogRecord as formatted text.

    This method should be called from format() by a formatter which
    wants to make use of a formatted time. This method can be overridden
    in formatters to provide for any specific requirement, but the
    basic behaviour is as follows: if datefmt (a string) is specified,
    it is used with time.strftime() to format the creation time of the
    record. Otherwise, an ISO8601-like (or RFC 3339-like) format is used.
    The resulting string is returned. This function uses a user-configurable
    function to convert the creation time to a tuple. By default,
    time.localtime() is used; to change this for a particular formatter
    instance, set the 'converter' attribute to a function with the same
    signature as time.localtime() or time.gmtime(). To change it for all
    formatters, for example if you want all logging times to be shown in GMT,
    set the 'converter' attribute in the Formatter class.
    """
    dt = datetime.fromtimestamp(record.created)
    if datefmt:
      s = dt.strftime(datefmt)
    else:
      s = dt.strftime(self.default_time_format)
      if self.default_msec_format:
        s = self.default_msec_format % (s, record.msecs)
    return s


class ContextFilter(logging.Filter):
  def __init__(self, identifier: str):
    super().__init__()
    self.identifier = identifier

  def filter(self, record):
    try:
      return record.ctx.get() == self.identifier  # pyright: ignore[reportAttributeAccessIssue]
    except AttributeError:
      return False


class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
  def doRollover(self):
    """
    do a rollover; in this case, a date/time stamp is appended to the filename
    when the rollover happens.  However, you want the file to be named for the
    start of the interval, not the current time.  If there is a backup count,
    then we have to get a list of matching filenames, sort them and remove
    the one with the oldest suffix.
    """
    base_path = Path(self.baseFilename)
    # get the time that this sequence started at and make it a TimeTuple
    currentTime = int(time())
    t = self.rolloverAt - self.interval
    if self.utc:
      timeTuple = gmtime(t)
    else:
      timeTuple = localtime(t)
      dstNow = localtime(currentTime)[-1]
      dstThen = timeTuple[-1]
      if dstNow != dstThen:
        addend = 3600 if dstNow else -3600
        timeTuple = localtime(t + addend)
    dfn = base_path.with_name(self.rotation_filename(f"{base_path.stem}.{strftime(self.suffix, timeTuple)}{base_path.suffix}"))
    if dfn.exists():
      # Already rolled over.
      return

    if self.stream:
      self.stream.close()
      self.stream = None  # type: ignore
    self.rotate(self.baseFilename, str(dfn))
    if self.backupCount > 0:
      for s in self.getFilesToDelete():
        Path(s).unlink()
    if not self.delay:
      self.stream = self._open()
    self.rolloverAt = self.computeRollover(currentTime)


FILE_FORMATTER = FixedFormatter(
  fmt=f"{{libpath: <{DEFAULT_MAX_WIDTH}}} | [{{asctime}}] | {{levelname: >8}} | {{message}}",
  datefmt=LOGGING_TIMESTAMP_FORMAT,
  style="{",
)


ROOT = logging.getLogger()
ROOT.setLevel(logging.DEBUG if __debug__ else logging.INFO)

logging.setLogRecordFactory(FixedLogRecord)

paramiko = logging.getLogger("paramiko")
paramiko.setLevel(logging.WARNING)


def configure_logging():
  import atexit
  from logging.handlers import QueueHandler, QueueListener

  from rich.traceback import install

  install(show_locals=True)

  LOG_LOC_FOLDER.mkdir(exist_ok=True, parents=True)
  SCHEDULER_LOG_LOC.mkdir(exist_ok=True, parents=True)

  scheduler = logging.getLogger("apscheduler")
  scheduler.propagate = False

  debug_file_handler = CustomTimedRotatingFileHandler(DEBUG_LOG_LOC, when="midnight", backupCount=14, delay=True)
  info_file_handler = CustomTimedRotatingFileHandler(INFO_LOG_LOC, when="midnight", backupCount=14, delay=True)
  scheduler_debug_handler = CustomTimedRotatingFileHandler(APSCHEDULER_DEBUG_LOG_LOC, when="midnight", backupCount=14, delay=True)
  scheduler_info_handler = CustomTimedRotatingFileHandler(APSCHEDULER_INFO_LOG_LOC, when="midnight", backupCount=14, delay=True)
  console_info_handler = FixedRichHandler(
    level=logging.DEBUG if __debug__ else logging.INFO,
    show_time=platform == "win32",
    console=RICH_CONSOLE,
    rich_tracebacks=True,
    log_time_format=LOGGING_TIMESTAMP_FORMAT,
    # tracebacks_show_locals=True,
  )

  debug_file_handler.setLevel(logging.DEBUG)
  info_file_handler.setLevel(logging.INFO)
  console_info_handler.setLevel(logging.INFO)
  scheduler_debug_handler.setLevel(logging.DEBUG)
  scheduler_info_handler.setLevel(logging.INFO)

  debug_file_handler.setFormatter(FILE_FORMATTER)
  info_file_handler.setFormatter(FILE_FORMATTER)
  scheduler_debug_handler.setFormatter(FILE_FORMATTER)
  scheduler_info_handler.setFormatter(FILE_FORMATTER)

  log_queue = Queue(-1)
  scheduler_log_queue = Queue(-1)

  queue_handler = QueueHandler(log_queue)
  scheduler_queue_handler = QueueHandler(scheduler_log_queue)

  queue_listener = QueueListener(
    log_queue,
    debug_file_handler,
    info_file_handler,
    respect_handler_level=True,
  )
  scheduler_queue_listener = QueueListener(
    scheduler_log_queue,
    scheduler_debug_handler,
    scheduler_info_handler,
    respect_handler_level=True,
  )

  ROOT.addHandler(queue_handler)
  ROOT.addHandler(console_info_handler)
  scheduler.addHandler(scheduler_queue_handler)

  queue_listener.start()
  scheduler_queue_listener.start()

  atexit.register(queue_listener.stop)
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

    @wraps(func)
    async def add_log_context_wrapper(*args: TP.args, **kwargs: TP.kwargs) -> TR:
      self_obj: "SupplierProcessorBase" = args[0]  # type: ignore

      set_ctx_var_identifier = self_obj.ctx_var_identifier
      set_ctx_var_log_loc = self_obj.ctx_var_log_loc

      unique_id = "".join([c for c in token_urlsafe(10) if c.isalnum()])
      now = datetime.now().strftime("%Y%m%d_%H%M%S%f")

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
        context_file_handler.setFormatter(FILE_FORMATTER)
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
