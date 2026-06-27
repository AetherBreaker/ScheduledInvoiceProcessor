# Standard library imports
from os import getcwd
from typing import TYPE_CHECKING

# First party imports
from aeth_ext.monkey_patcher import MonkeyPatcher

if TYPE_CHECKING:
  # Standard library imports
  from pathlib import PurePath


def without_cwd(self: PurePath) -> str:
  cwd = getcwd()
  return str(self)[len(cwd) :] if str(self).startswith(cwd) else str(self)


class Patches(MonkeyPatcher):
  @staticmethod
  def patch_the_monkey() -> None:
    # Standard library imports
    from pathlib import PurePath

    PurePath.without_cwd = without_cwd
