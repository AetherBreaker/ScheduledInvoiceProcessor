import atexit
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime
from functools import wraps
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler, TimedRotatingFileHandler
from queue import Queue
from secrets import token_urlsafe
from sys import platform
from time import gmtime, localtime, strftime, time
from typing import TYPE_CHECKING, Literal

from rich.console import Console, ConsoleRenderable
from rich.logging import RichHandler
from rich.traceback import Traceback
from typing_custom.custom_path import CustomPath
from typing_custom.enums import LogActionEnum

if TYPE_CHECKING:
  from supplier_processors import SupplierProcessorBase

# RICH_CONSOLE = Console()
RICH_CONSOLE = Console(
  width=None if platform == "win32" else 175,
  # force_terminal=True,
  log_time=platform == "win32",
)


PROJECT_NAME = "ScheduledOrderMiddleman"

max_width = 20

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

    pathpath = CustomPath(record.pathname)

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
    global max_width
    pathpath = CustomPath(args[2])

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

    if length > max_width:
      max_width = length
      with open("max_width.txt", "w") as f:
        f.write(str(max_width))

    self.libname = libname
    self.libpath = libpath

    super().__init__(*args, **kwargs)


class FixedFormatter(logging.Formatter):
  default_msec_format = None
  converter = datetime.fromtimestamp  # type: ignore

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
    dt = self.converter(record.created)
    if datefmt:
      s = dt.strftime(datefmt)  # type: ignore
    else:
      s = dt.strftime(self.default_time_format)  # type: ignore
      if self.default_msec_format:
        s = self.default_msec_format % (s, record.msecs)
    return s


class ContextFilter(logging.Filter):
  def __init__(self, identifier: str):
    super().__init__()
    self.identifier = identifier

  def filter(self, record):
    return record.ctx == self.identifier  # pyright: ignore[reportAttributeAccessIssue]


class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
  def doRollover(self):
    """
    do a rollover; in this case, a date/time stamp is appended to the filename
    when the rollover happens.  However, you want the file to be named for the
    start of the interval, not the current time.  If there is a backup count,
    then we have to get a list of matching filenames, sort them and remove
    the one with the oldest suffix.
    """
    base_path = CustomPath(self.baseFilename)
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
        CustomPath(s).unlink()
    if not self.delay:
      self.stream = self._open()
    self.rolloverAt = self.computeRollover(currentTime)


FILE_FORMATTER = FixedFormatter(
  fmt=f"{{libpath: <{max_width}}} | [{{asctime}}] {{levelname: >8}} | {{message}}",
  datefmt=LOGGING_TIMESTAMP_FORMAT,
  style="{",
)


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


CWD = CustomPath.cwd()

LOGGING_BASE_NAME = "ScheduledOrderMiddleman"

LOG_LOC_FOLDER = CWD / ".." / "logs" if CWD.name == "src" else CWD / "logs"
LOG_LOC_FOLDER.mkdir(exist_ok=True)

print("LOOK RIGHT HERE THIS IS WHERE YOU NEED TO LOOK AAAAAAAAAAAAAAAAAAAAAAA")
print(LOG_LOC_FOLDER)

DEBUG_LOG_LOC = LOG_LOC_FOLDER / f"{LOGGING_BASE_NAME}_debug.txt"
INFO_LOG_LOC = LOG_LOC_FOLDER / f"{LOGGING_BASE_NAME}.txt"


LOGGING_TYPE: Literal["daily", "per_run"] = "daily"


daily_debug_handler = CustomTimedRotatingFileHandler(DEBUG_LOG_LOC, when="midnight", backupCount=14, delay=True)
daily_info_handler = CustomTimedRotatingFileHandler(INFO_LOG_LOC, when="midnight", backupCount=14, delay=True)

per_run_debug_handler = RotatingFileHandler(DEBUG_LOG_LOC, maxBytes=0, backupCount=30, delay=True)
per_run_info_handler = RotatingFileHandler(INFO_LOG_LOC, maxBytes=0, backupCount=30, delay=True)

if LOGGING_TYPE == "per_run":
  per_run_debug_handler.doRollover()
  per_run_info_handler.doRollover()


def configure_logging():
  logging.setLogRecordFactory(FixedLogRecord)

  paramiko = logging.getLogger("paramiko")
  paramiko.setLevel(logging.WARNING)

  root = logging.getLogger()
  root.setLevel(logging.DEBUG if __debug__ else logging.INFO)
  # root.setLevel(logging.DEBUG)

  debugging_file_handler = daily_debug_handler if LOGGING_TYPE == "daily" else per_run_debug_handler
  debugging_file_handler.setLevel(logging.DEBUG)

  info_file_handler = daily_info_handler if LOGGING_TYPE == "daily" else per_run_info_handler
  info_file_handler.setLevel(logging.INFO)

  # console_error_handler = logging.StreamHandler(sys.stderr)
  # console_error_handler.setLevel(logging.ERROR)
  # console_info_handler = logging.StreamHandler(sys.stdout)
  # console_info_handler.setLevel(logging.INFO)

  console_info_handler = FixedRichHandler(
    # level=logging.DEBUG if __debug__ else logging.INFO,
    show_time=platform == "win32",
    console=RICH_CONSOLE,
    rich_tracebacks=True,
    log_time_format=LOGGING_TIMESTAMP_FORMAT,
  )

  console_info_handler.setLevel(logging.INFO)

  debugging_file_handler.setFormatter(FILE_FORMATTER)
  info_file_handler.setFormatter(FILE_FORMATTER)
  # console_error_handler.setFormatter(formatter)
  # console_info_handler.setFormatter(formatter)

  log_queue = Queue(-1)

  queue_handler = QueueHandler(log_queue)

  queue_listener = QueueListener(
    log_queue,
    debugging_file_handler,
    info_file_handler,
    respect_handler_level=True,
  )

  # root.addHandler(debugging_file_handler)
  # root.addHandler(info_file_handler)
  root.addHandler(queue_handler)
  # root.addHandler(console_error_handler)
  root.addHandler(console_info_handler)

  queue_listener.start()

  atexit.register(queue_listener.stop)

  # if __debug__:
  #   console_debug_handler = FixedRichHandler(
  #     level=logging.DEBUG,
  #     console=RICH_CONSOLE,
  #     rich_tracebacks=True,
  #     log_time_format=logging_timestamp_fmt,
  #   )
  #   # console_debug_handler.setFormatter(formatter)
  #   root.addHandler(console_debug_handler)
