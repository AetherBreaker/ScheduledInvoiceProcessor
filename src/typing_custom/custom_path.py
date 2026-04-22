# from pathlib import Path, PosixPath, PurePosixPath, PureWindowsPath, UnsupportedOperation, WindowsPath
# from typing import Any, Final

# from pydantic import GetCoreSchemaHandler
# from pydantic_core import CoreSchema, core_schema


# cwd = getcwd()


# class CustomPurePath(PurePath):
#   def __init__(self, *args: str) -> None:
#     super().__init__(*args)

#     # create a string representation of self with the current working directory removed from the start of the path, if it exists
#     self_str = str(self)
#     self.without_cwd: Final = self_str[len(cwd) :] if self_str.startswith(cwd) else self_str

#   @classmethod
#   def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
#     return core_schema.chain_schema(
#       [
#         handler(PurePath),
#         core_schema.no_info_plain_validator_function(cls),
#       ]
#     )


# class CustomPurePosixPath(PurePosixPath):
#   def __init__(self, *args: str) -> None:
#     super().__init__(*args)

#     # create a string representation of self with the current working directory removed from the start of the path, if it exists
#     self_str = str(self)
#     self.without_cwd: Final = self_str[len(cwd) :] if self_str.startswith(cwd) else self_str


# @classmethod
# def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
#   return core_schema.chain_schema(
#     [
#       handler(PurePosixPath),
#       core_schema.no_info_plain_validator_function(cls),
#     ]
#   )


# class CustomPath(Path):
#   def __new__(cls, *args, **kwargs):
#     if cls is CustomPath:
#       cls = CustomWindowsPath if os.name == "nt" else CustomPosixPath
#     # Normalize backslashes to forward slashes on POSIX so paths
#     # serialized on Windows can be deserialized in Docker/Linux
#     if os.name != "nt" and args:
#       args = tuple(str(a).replace("\\", "/") for a in args)
#     return super().__new__(cls, *args, **kwargs)  # type: ignore

#   def __init__(self, *args: str) -> None:
#     if os.name != "nt" and args:
#       args = tuple(str(a).replace("\\", "/") for a in args)
#     super().__init__(*args)

#     # create a string representation of self with the current working directory removed from the start of the path, if it exists
#     self_str = str(self)
#     self.without_cwd: Final = self_str[len(cwd) :] if self_str.startswith(cwd) else self_str

#   @classmethod
#   def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
#     return core_schema.chain_schema(
#       [
#         handler(Path),
#         core_schema.no_info_plain_validator_function(cls),
#       ]
#     )


# class CustomWindowsPath(CustomPath, PureWindowsPath):
#   """Path subclass for Windows systems.

#   On a Windows system, instantiating a Path should return this object.
#   """

#   __slots__ = ()

#   if os.name != "nt":

#     def __new__(cls, *args, **kwargs):
#       raise UnsupportedOperation(f"cannot instantiate {cls.__name__!r} on your system")

#   @classmethod
#   def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
#     return core_schema.chain_schema(
#       [
#         handler(WindowsPath),
#         core_schema.no_info_plain_validator_function(cls),
#       ]
#     )


# class CustomPosixPath(CustomPath, PurePosixPath):
#   """Path subclass for non-Windows systems.

#   On a POSIX system, instantiating a Path should return this object.
#   """

#   __slots__ = ()

#   if os.name == "nt":

#     def __new__(cls, *args, **kwargs):
#       raise UnsupportedOperation(f"cannot instantiate {cls.__name__!r} on your system")

#   @classmethod
#   def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
#     return core_schema.chain_schema(
#       [
#         handler(PosixPath),
#         core_schema.no_info_plain_validator_function(cls),
#       ]
#     )
