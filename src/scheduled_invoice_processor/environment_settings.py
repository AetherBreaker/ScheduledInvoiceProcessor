"""Environment-backed settings for the processor."""

# Standard library imports
from logging import getLogger
from typing import TYPE_CHECKING, Annotated

# Third party imports
from pydantic import Field

# First party imports
from aeth_ext.settings import BaseSettings

if TYPE_CHECKING:
  # Standard library imports
  from pathlib import Path

logger = getLogger(__name__)


class Settings(BaseSettings):
  """Environment variables and credential-file locations for the processor."""

  database_id: Annotated[str, Field(alias="DATABASE_ID")]
  database_base_schedule_id: Annotated[int, Field(alias="DATABASE_BASE_SCHEDULE_ID")]
  database_order_log_id: Annotated[int, Field(alias="DATABASE_ORDER_LOG_ID")]

  database_refresh_interval: Annotated[int, Field(alias="DATABASE_REFRESH_INTERVAL")] = 3600
  database_write_interval: Annotated[int, Field(alias="DATABASE_WRITE_INTERVAL")] = 60

  use_testing_folders: Annotated[bool, Field(alias="USE_TESTING_FOLDERS")] = False

  @property
  def google_api_key_file(self) -> Path:
    """Google API key file at `secrets/db-key.json`."""
    return self._creds_file_reusable("Google API key file not found at expected location", "secrets", "db-key.json")

  @property
  def sft_website_creds_file(self) -> Path:
    """SFT website credentials at `secrets/sft_creds.json`."""
    return self._creds_file_reusable("SFT website creds file not found at expected location", "secrets", "sft_creds.json")

  @property
  def sas_ftp_creds_file(self) -> Path:
    """SAS FTP credentials at `secrets/sas_ftp_creds.json`."""
    return self._creds_file_reusable("SAS FTP creds file not found at expected location", "secrets", "sas_ftp_creds.json")

  @property
  def ryo_ftp_creds_file(self) -> Path:
    """RYO FTP credentials at `secrets/ryo_ftp_creds.json`."""
    return self._creds_file_reusable("RYO FTP creds file not found at expected location", "secrets", "ryo_ftp_creds.json")

  @property
  def coremark_ftp_creds_file(self) -> Path:
    """Coremark FTP credentials at `secrets/coremark_ftp_creds.json`."""
    return self._creds_file_reusable("Coremark FTP creds file not found at expected location", "secrets", "coremark_ftp_creds.json")
