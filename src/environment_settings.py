if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import os
import sys
from logging import getLogger
from pathlib import Path
from typing import Annotated

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = getLogger(__name__)

os.environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "false")


CWD = Path(__file__).parent if getattr(sys, "frozen", False) else Path.cwd()

testing = False


class Settings(BaseSettings):
  model_config = (
    SettingsConfigDict(
      env_file=CWD / ("testing.env" if __debug__ and testing else "production.env"),
      env_file_encoding="utf-8",
      env_ignore_empty=True,
    )
    if __debug__
    else SettingsConfigDict()
  )

  database_id: Annotated[str, Field(alias="DATABASE_ID")]
  database_base_schedule_id: Annotated[str, Field(alias="DATABASE_BASE_SCHEDULE_ID")]
  database_order_log_id: Annotated[str, Field(alias="DATABASE_ORDER_LOG_ID")]

  database_refresh_interval: Annotated[int, Field(alias="DATABASE_REFRESH_INTERVAL")] = 3600
  database_write_interval: Annotated[int, Field(alias="DATABASE_WRITE_INTERVAL")] = 60

  googe_api_key_file: Path
  sft_website_creds_file: Path
  sas_ftp_creds_file: Path
