from pathlib import Path, PosixPath, PurePath
from typing import Any, Final

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema

cwd = Path.cwd()


class CustomPurePath(PurePath):
  def __init__(self, *args: str) -> None:
    super().__init__(*args)

    # create a string representation of self with the current working directory removed from the start of the path, if it exists
    cwd_str = str(cwd)
    self_str = str(self)
    self.without_cwd: Final = self_str[len(cwd_str) :] if self_str.startswith(cwd_str) else self_str

  @classmethod
  def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
    return handler(PurePath)


# class PurePosixPath(PurePosixPath):
#   def __init__(self, *args: str) -> None:
#     super().__init__(*args)

#     # create a string representation of self with the current working directory removed from the start of the path, if it exists
#     cwd_str = str(CWD)
#     self_str = str(self)
#     self.without_cwd: Final = self_str[len(cwd_str) :] if self_str.startswith(cwd_str) else self_str

# @classmethod
# def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
#   return handler(PurePath)


class CustomPath(Path):
  def __init__(self, *args: str) -> None:
    super().__init__(*args)

    # create a string representation of self with the current working directory removed from the start of the path, if it exists
    cwd_str = str(cwd)
    self_str = str(self)
    self.without_cwd: Final = self_str[len(cwd_str) :] if self_str.startswith(cwd_str) else self_str

  @classmethod
  def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
    return handler(Path)


class CustomPosixPath(PosixPath):
  def __init__(self, *args: str) -> None:
    super().__init__(*args)

    # create a string representation of self with the current working directory removed from the start of the path, if it exists
    cwd_str = str(cwd)
    self_str = str(self)
    self.without_cwd: Final = self_str[len(cwd_str) :] if self_str.startswith(cwd_str) else self_str

  @classmethod
  def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
    return handler(PosixPath)
