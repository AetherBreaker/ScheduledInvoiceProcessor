from __future__ import annotations

from datetime import datetime
from pathlib import Path, PurePosixPath
from re import Pattern

from dateutil.relativedelta import SU, relativedelta
from environment_init_vars import TZ
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass
from typing_custom import CustomerID, StoreNum


@dataclass
class FileRegisterData:
  __pydantic_config__ = ConfigDict(
    populate_by_name=True,
    use_enum_values=True,
    validate_default=True,
    validate_assignment=True,
    coerce_numbers_to_str=True,
  )

  storenum: StoreNum
  customer_id: CustomerID
  pickup_date: datetime
  dropoff_date: datetime
  file_pattern: Pattern[str]
  _current_week: bool
  _waiting_folder: PurePosixPath
  _local_copy_folder: Path

  file_names: dict[int, str] = Field(default_factory=dict)
  invoice_nums: dict[int, str] = Field(default_factory=dict)
  pickup_success: dict[int, bool] = Field(default_factory=dict)
  preprocess_success: dict[int, bool] = Field(default_factory=dict)
  dropoff_success: dict[int, bool] = Field(default_factory=dict)

  @property
  def current_week(self) -> bool:
    if self._current_week:
      now = datetime.now(TZ)
      window_start = self.pickup_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0)
      window_end = self.dropoff_date + relativedelta(weekday=SU(+1), hour=0, minute=0, second=0)

      return window_start <= now < window_end
    return False

  @property
  def remote_file_locs(self) -> dict[int, PurePosixPath]:
    return {idx: self._waiting_folder / name for idx, name in self.file_names.items()}

  @property
  def local_copy_loc(self) -> dict[int, Path]:
    return {idx: self._local_copy_folder / name for idx, name in self.file_names.items()}

  @property
  def stale(self) -> bool:
    return datetime.now(TZ) > (self.dropoff_date + relativedelta(weekday=SU(+1), hour=0, minute=0, second=0))
