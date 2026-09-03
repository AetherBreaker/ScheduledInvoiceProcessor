"""String enums shared by the schedule, order log, and processors."""

# Standard library imports
from enum import auto

# First party imports
from aeth_ext.types import StrEnum


class SuppliersEnum(StrEnum):
  """Supported suppliers."""

  SAS = "SAS"
  RYO = "RYO"
  COREMARK = "COREMARK"
  SFT = "SFT"


class StateEnum(StrEnum):
  """US state and territory postal codes."""

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


class WeekdayEnum(StrEnum):
  """Day-of-week names as written in the schedule sheet."""

  Monday = auto()
  Tuesday = auto()
  Wednesday = auto()
  Thursday = auto()
  Friday = auto()
  Saturday = auto()
  Sunday = auto()


class LogActionEnum(StrEnum):
  """Order-log action stages, from pickup registration through dropoff."""

  REGISTERED_PICKUP = auto()
  FILE_PICKED_UP = auto()
  FILE_PREPROCESSED = auto()
  REGISTERED_DROPOFF = auto()
  FILE_DROPPED_OFF = auto()


class StatusCode(StrEnum):
  """Outcome recorded for an order-log action."""

  UNKNOWN = auto()
  FAILURE = auto()
  SUCCESS = auto()
  SKIPPED = auto()
