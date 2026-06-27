# Standard library imports
from logging import getLogger
from traceback import extract_tb
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  # Local folder imports
  from .typing_custom import FatalDetails

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


def extract_exc_details(exc: BaseException):
  _last_fatal_details["is_database_origin"] = _is_database_origin_exception(exc)
  _last_fatal_details["exception_type"] = type(exc).__name__
  _last_fatal_details["exception_message"] = str(exc)
