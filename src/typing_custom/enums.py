# Standard library imports
from enum import StrEnum, auto
from typing import TYPE_CHECKING, override

if TYPE_CHECKING:
  # Standard library imports
  from typing import Any


class CustomStrEnum(StrEnum):
  """
  Custom string enum that returns the member name as the value.
  """

  @override
  @staticmethod
  def _generate_next_value_(name: str, start: int, count: int, last_values: list[Any]) -> Any:
    """
    Return the member name.
    """
    return name


class SuppliersEnum(CustomStrEnum):
  SAS = "SAS"
  RYO = "RYO"
  COREMARK = "COREMARK"


class StateEnum(CustomStrEnum):
  AL = auto()
  AK = auto()
  AZ = auto()
  AR = auto()
  CA = auto()
  CO = auto()
  CT = auto()
  DE = auto()
  FL = auto()
  GA = auto()
  HI = auto()
  ID = auto()
  IL = auto()
  IN = auto()
  IA = auto()
  KS = auto()
  KY = auto()
  LA = auto()
  ME = auto()
  MD = auto()
  MA = auto()
  MI = auto()
  MN = auto()
  MS = auto()
  MO = auto()
  MT = auto()
  NE = auto()
  NV = auto()
  NH = auto()
  NJ = auto()
  NM = auto()
  NY = auto()
  NC = auto()
  ND = auto()
  OH = auto()
  OK = auto()
  OR = auto()
  PA = auto()
  RI = auto()
  SC = auto()
  SD = auto()
  TN = auto()
  TX = auto()
  UT = auto()
  VT = auto()
  VA = auto()
  WA = auto()
  WV = auto()
  WI = auto()
  WY = auto()
  DC = auto()
  AS = auto()
  GU = auto()
  MP = auto()
  PR = auto()
  UM = auto()
  VI = auto()


class WeekdayEnum(CustomStrEnum):
  Monday = auto()
  Tuesday = auto()
  Wednesday = auto()
  Thursday = auto()
  Friday = auto()
  Saturday = auto()
  Sunday = auto()


class LogActionEnum(CustomStrEnum):
  REGISTERED_PICKUP = auto()
  FILE_PICKED_UP = auto()
  FILE_PREPROCESSED = auto()
  REGISTERED_DROPOFF = auto()
  FILE_DROPPED_OFF = auto()


class StatusCode(CustomStrEnum):
  UNKNOWN = auto()
  FAILURE = auto()
  SUCCESS = auto()
  SKIPPED = auto()
