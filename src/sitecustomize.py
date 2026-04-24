from os import environ, getcwd
from pathlib import PurePath

environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "false")


def without_cwd(self) -> str:
  cwd = getcwd()
  return str(self)[len(cwd) :] if str(self).startswith(cwd) else str(self)


PurePath.without_cwd = without_cwd
from logging_config import configure_logging  # noqa: E402

configure_logging()
