# Standard library imports
from os import environ, getcwd
from pathlib import PurePath

environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "false")


def without_cwd(self: PurePath) -> str:
  cwd = getcwd()
  return str(self)[len(cwd) :] if str(self).startswith(cwd) else str(self)


PurePath.without_cwd = without_cwd

# Standard library imports
from sys import platform  # noqa: E402

if platform in ("win32", "cygwin", "cli"):
  # Third party imports
  from winloop import new_event_loop
else:
  # if we're on apple or linux do this instead
  # Third party imports
  from uvloop import new_event_loop  # type: ignore
# Standard library imports
from asyncio import set_event_loop  # noqa: E402

set_event_loop(new_event_loop())
