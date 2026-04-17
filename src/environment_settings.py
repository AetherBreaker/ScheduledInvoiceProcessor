import os
import sys
from logging import getLogger
from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_custom.custom_path import CustomPath

# from pydantic.networks import NameEmail

logger = getLogger(__name__)

os.environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "false")


CWD = CustomPath(__file__).parent if getattr(sys, "frozen", False) else CustomPath.cwd()

testing = False


class Settings(BaseSettings):
  model_config = (
    SettingsConfigDict(
      env_file=CWD / "testing.env",
      env_file_encoding="utf-8",
      env_ignore_empty=True,
    )
    if __debug__
    else SettingsConfigDict()
  )

  persisted_dir_loc: Annotated[CustomPath, Field(alias="PERSISTED_DIR_LOC")] = (
    CWD / "persisted_data" if __debug__ else CustomPath("/app/persisted_data")
  )

  database_id: Annotated[str, Field(alias="DATABASE_ID")]
  database_base_schedule_id: Annotated[int, Field(alias="DATABASE_BASE_SCHEDULE_ID")]
  database_order_log_id: Annotated[int, Field(alias="DATABASE_ORDER_LOG_ID")]

  database_refresh_interval: Annotated[int, Field(alias="DATABASE_REFRESH_INTERVAL")] = 3600
  database_write_interval: Annotated[int, Field(alias="DATABASE_WRITE_INTERVAL")] = 60

  file_serve_public_domain: Annotated[str, Field(alias="FILE_SERVE_PUBLIC_DOMAIN")] = "som.sweetfiretobacco.com"
  file_serve_host: Annotated[str, Field(alias="FILE_SERVE_HOST")] = "localhost"
  file_serve_port: Annotated[int, Field(alias="FILE_SERVE_PORT")] = 8080

  alerts_email: Annotated[str, Field(alias="ALERTS_EMAIL")] = "info@sweetfiretobacco.com"
  alerts_email_pwd: Annotated[str, Field(alias="ALERTS_EMAIL_PWD")]
  alerts_recipients: Annotated[set[str], Field(alias="ALERTS_RECIPIENTS")] = set()

  def creds_file_reusable(self, err_msg: str, *expected_path_parts: str) -> CustomPath:
    fp = self.persisted_dir_loc.joinpath(*expected_path_parts)
    if not fp.exists() or not fp.is_file():
      raise FileNotFoundError(f"{err_msg}: {fp}")
    return fp

  @property
  def google_api_key_file(self) -> CustomPath:
    return self.creds_file_reusable("Google API key file not found at expected location", "secrets", "db-key.json")

  @property
  def sft_website_creds_file(self) -> CustomPath:
    return self.creds_file_reusable("SFT website creds file not found at expected location", "secrets", "sft_creds.json")

  @property
  def sas_ftp_creds_file(self) -> CustomPath:
    return self.creds_file_reusable("SAS FTP creds file not found at expected location", "secrets", "sas_ftp_creds.json")

  @property
  def ryo_ftp_creds_file(self) -> CustomPath:
    return self.creds_file_reusable("RYO FTP creds file not found at expected location", "secrets", "ryo_ftp_creds.json")

  @property
  def coremark_ftp_creds_file(self) -> CustomPath:
    return self.creds_file_reusable("Coremark FTP creds file not found at expected location", "secrets", "coremark_ftp_creds.json")
