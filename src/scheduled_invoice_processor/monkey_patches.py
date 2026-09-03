"""Runtime patches applied to stdlib classes."""

# Standard library imports
from os import getcwd
from typing import TYPE_CHECKING

# First party imports
from aeth_ext.monkey_patcher import MonkeyPatcher

if TYPE_CHECKING:
  # Standard library imports
  from pathlib import PurePath


def without_cwd(self: PurePath) -> str:
  """The path as a string with the current working directory prefix stripped."""
  cwd = getcwd()
  return str(self).removeprefix(cwd)


class Patches(MonkeyPatcher):
  """Patch set applied through `MonkeyPatcher`."""

  @staticmethod
  def patch_the_monkey() -> None:
    """Attach `without_cwd` to `PurePath`."""
    # Standard library imports
    from pathlib import PurePath

    PurePath.without_cwd = without_cwd
