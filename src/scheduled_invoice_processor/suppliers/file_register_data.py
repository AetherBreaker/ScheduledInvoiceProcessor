"""The per-entry record carried through the supplier queues."""

# Standard library imports
from datetime import datetime
from pathlib import Path, PurePosixPath
from re import Pattern

# Third party imports
from dateutil.relativedelta import SU, relativedelta
from pydantic import ConfigDict, Field
from pydantic.dataclasses import dataclass

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom import CustomerID, StoreNum


class FixRuffTC001:
  """Empty base named for the ruff rule (TC001) it works around."""


@dataclass
class FileRegisterData(FixRuffTC001):
  """One schedule entry's matched files and per-file success flags as it moves through the supplier queues."""

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
    """True while now is inside the Sunday-to-Sunday span around the pickup and dropoff dates; always False for a prior-week entry."""
    if self._current_week:
      now = datetime.now(SETTINGS.tz)
      window_start = self.pickup_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0)
      window_end = self.dropoff_date + relativedelta(weekday=SU(+1), hour=0, minute=0, second=0)

      return window_start <= now < window_end
    return False

  @property
  def remote_file_locs(self) -> dict[int, PurePosixPath]:
    """Each file's path in the waiting folder, keyed by index."""
    return {idx: self._waiting_folder / name for idx, name in self.file_names.items()}

  @property
  def local_copy_loc(self) -> dict[int, Path]:
    """Each file's path in the local copy folder, keyed by index."""
    return {idx: self._local_copy_folder / name for idx, name in self.file_names.items()}

  @property
  def stale(self) -> bool:
    """True once the Sunday after the dropoff date has passed."""
    return datetime.now(SETTINGS.tz) > (self.dropoff_date + relativedelta(weekday=SU(+1), hour=0, minute=0, second=0))
