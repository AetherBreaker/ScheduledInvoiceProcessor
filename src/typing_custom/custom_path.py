import os
from pathlib import Path, PosixPath, PurePath, PurePosixPath, PureWindowsPath, UnsupportedOperation, WindowsPath
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
  def __new__(cls, *args, **kwargs):
    if cls is CustomPath:
      cls = CustomWindowsPath if os.name == "nt" else CustomPosixPath
    # Normalize backslashes to forward slashes on POSIX so paths
    # serialized on Windows can be deserialized in Docker/Linux
    if os.name != "nt" and args:
      args = tuple(str(a).replace("\\", "/") for a in args)
    return super().__new__(cls, *args, **kwargs)  # type: ignore

  def __init__(self, *args: str) -> None:
    if os.name != "nt" and args:
      args = tuple(str(a).replace("\\", "/") for a in args)
    super().__init__(*args)

    # create a string representation of self with the current working directory removed from the start of the path, if it exists
    cwd_str = str(cwd)
    self_str = str(self)
    self.without_cwd: Final = self_str[len(cwd_str) :] if self_str.startswith(cwd_str) else self_str

  @classmethod
  def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
    return handler(Path)


class CustomWindowsPath(CustomPath, PureWindowsPath):
  """Path subclass for Windows systems.

  On a Windows system, instantiating a Path should return this object.
  """

  __slots__ = ()

  if os.name != "nt":

    def __new__(cls, *args, **kwargs):
      raise UnsupportedOperation(f"cannot instantiate {cls.__name__!r} on your system")

  @classmethod
  def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
    return handler(WindowsPath)


class CustomPosixPath(CustomPath, PurePosixPath):
  """Path subclass for non-Windows systems.

  On a POSIX system, instantiating a Path should return this object.
  """

  __slots__ = ()

  if os.name == "nt":

    def __new__(cls, *args, **kwargs):
      raise UnsupportedOperation(f"cannot instantiate {cls.__name__!r} on your system")

  @classmethod
  def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
    return handler(PosixPath)
