# Standard library imports
from logging import getLogger
from typing import TYPE_CHECKING, Annotated

# Third party imports
from aeth_ext.settings import BaseSettings
from pydantic import Field

if TYPE_CHECKING:
  # Standard library imports
  from pathlib import Path

logger = getLogger(__name__)


class Settings(BaseSettings):
  database_id: Annotated[str, Field(alias="DATABASE_ID")]
  database_base_schedule_id: Annotated[int, Field(alias="DATABASE_BASE_SCHEDULE_ID")]
  database_order_log_id: Annotated[int, Field(alias="DATABASE_ORDER_LOG_ID")]

  database_refresh_interval: Annotated[int, Field(alias="DATABASE_REFRESH_INTERVAL")] = 3600
  database_write_interval: Annotated[int, Field(alias="DATABASE_WRITE_INTERVAL")] = 60

  use_testing_folders: Annotated[bool, Field(alias="USE_TESTING_FOLDERS")] = False

  file_serve_public_domain: Annotated[str, Field(alias="FILE_SERVE_PUBLIC_DOMAIN")] = "som.sweetfiretobacco.com"
  file_serve_host: Annotated[str, Field(alias="FILE_SERVE_HOST")] = "localhost"
  file_serve_port: Annotated[int, Field(alias="FILE_SERVE_PORT")] = 8080

  @property
  def google_api_key_file(self) -> Path:
    return self.creds_file_reusable("Google API key file not found at expected location", "secrets", "db-key.json")

  @property
  def sft_website_creds_file(self) -> Path:
    return self.creds_file_reusable("SFT website creds file not found at expected location", "secrets", "sft_creds.json")

  @property
  def sas_ftp_creds_file(self) -> Path:
    return self.creds_file_reusable("SAS FTP creds file not found at expected location", "secrets", "sas_ftp_creds.json")

  @property
  def ryo_ftp_creds_file(self) -> Path:
    return self.creds_file_reusable("RYO FTP creds file not found at expected location", "secrets", "ryo_ftp_creds.json")

  @property
  def coremark_ftp_creds_file(self) -> Path:
    return self.creds_file_reusable("Coremark FTP creds file not found at expected location", "secrets", "coremark_ftp_creds.json")
