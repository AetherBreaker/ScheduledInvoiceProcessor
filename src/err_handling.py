from __future__ import annotations

from asyncio import CancelledError
from functools import wraps
from io import StringIO
from logging import getLogger
from traceback import extract_tb
from typing import TYPE_CHECKING

from environment_init_vars import FATAL_EVENT
from rich.console import Console
from send_alert_email import send_alert_email

if TYPE_CHECKING:
  from collections.abc import Callable

  from typing_custom import FatalDetails

logger = getLogger(__name__)


_DATABASE_FATAL_PATH_MARKERS = (
  "\\src\\database\\",
  "/src/database/",
  "\\gspread\\",
  "/gspread/",
  "\\google\\oauth2\\",
  "/google/oauth2/",
)


_last_fatal_details: FatalDetails = {
  "is_database_origin": False,
  "exception_type": None,
  "exception_message": None,
}


def _is_database_origin_exception(exc: BaseException) -> bool:
  stack: list[BaseException] = [exc]
  seen: set[int] = set()

  while stack:
    current = stack.pop()
    current_id = id(current)
    if current_id in seen:
      continue
    seen.add(current_id)

    tb = current.__traceback__
    if tb is not None:
      for frame in extract_tb(tb):
        filename = frame.filename.lower().replace("\\\\", "/")
        if any(marker in filename for marker in _DATABASE_FATAL_PATH_MARKERS):
          return True

    if current.__cause__ is not None:
      stack.append(current.__cause__)
    if current.__context__ is not None:
      stack.append(current.__context__)

  return False


def get_last_fatal_details() -> FatalDetails:
  return _last_fatal_details


def handle_fatal_exc[**TP, TR](func: Callable[TP, TR]) -> Callable[TP, TR | None]:
  if not __debug__:

    @wraps(func)
    def wrapper(*args: TP.args, **kwargs: TP.kwargs) -> TR | None:
      try:
        return func(*args, **kwargs)
      except CancelledError:
        pass
        raise  # raise whatever to make the type checker happy about return values
      except BaseException as e:
        if isinstance(e, CancelledError):
          raise
        _last_fatal_details["is_database_origin"] = _is_database_origin_exception(e)
        _last_fatal_details["exception_type"] = type(e).__name__
        _last_fatal_details["exception_message"] = str(e)
        logger.critical(f"Fatal exception in {func.__qualname__}: {e}", exc_info=True)
        # _exit(1)  # Exit with non-zero code to indicate failure to Coolify

        strio = StringIO()

        tmp = Console(force_terminal=False, force_interactive=False, color_system=None, width=100, markup=False, file=strio)

        with tmp.capture() as capture:
          tmp.print_exception(show_locals=True)
        content = capture.get()

        send_alert_email(f"Fatal exception in {func.__qualname__}", f"{e}:\n\n{content}")
        FATAL_EVENT.set()
        return None

    return wrapper
  else:
    return func
