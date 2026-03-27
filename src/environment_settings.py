if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import os
import sys
from logging import getLogger
from pathlib import Path
from typing import Annotated

from pydantic import Field
from pydantic.networks import NameEmail
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = getLogger(__name__)

os.environ.setdefault("PYDANTIC_ERRORS_INCLUDE_URL", "false")


CWD = Path(__file__).parent if getattr(sys, "frozen", False) else Path.cwd()

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

  database_id: Annotated[str, Field(alias="DATABASE_ID")]
  database_base_schedule_id: Annotated[int, Field(alias="DATABASE_BASE_SCHEDULE_ID")]
  database_order_log_id: Annotated[int, Field(alias="DATABASE_ORDER_LOG_ID")]

  database_refresh_interval: Annotated[int, Field(alias="DATABASE_REFRESH_INTERVAL")] = 3600
  database_write_interval: Annotated[int, Field(alias="DATABASE_WRITE_INTERVAL")] = 60

  googe_api_key_file: Annotated[Path, Field(alias="GOOGLE_API_KEY_FILE")]
  sft_website_creds_file: Annotated[Path, Field(alias="SFT_WEBSITE_CREDS_FILE")]
  sas_ftp_creds_file: Annotated[Path, Field(alias="SAS_FTP_CREDS_FILE")]
  ryo_ftp_creds_file: Annotated[Path, Field(alias="RYO_FTP_CREDS_FILE")]

  file_serve_public_domain: Annotated[str, Field(alias="FILE_SERVE_PUBLIC_DOMAIN")] = "som.sweetfiretobacco.com"
  file_serve_host: Annotated[str, Field(alias="FILE_SERVE_HOST")] = "localhost"
  file_serve_port: Annotated[int, Field(alias="FILE_SERVE_PORT")] = 8080

  alerts_email: Annotated[str, Field(alias="ALERTS_EMAIL")] = "info@sweetfiretobacco.com"
  alerts_email_pwd: Annotated[str, Field(alias="ALERTS_EMAIL_PWD")]
  alerts_recipients: Annotated[set[NameEmail], Field(alias="ALERTS_RECIPIENTS")] = set()

  # @field_validator("*", mode="wrap", check_fields=False)
  # @classmethod
  # def log_failed_field_validations(cls, data: str, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> Any:
  #   results = None

  #   print(data)

  #   try:
  #     results = handler(data)
  #   except ValidationError as e:
  #     raise e

  #   return data if results is None else results

  # @model_validator(mode="wrap")
  # @classmethod
  # def log_failed_validation(cls, data: Any, handler: ModelWrapValidatorHandler[Self], info: ValidationInfo) -> Self:
  #   results = None
  #   print(data)
  #   try:
  #     results = handler(data)
  #   except ValidationError as e:
  #     if cls.__name__ != "ScheduledOrderDBEntryModel":
  #       raise e

  #     for err in e.errors():
  #       if err["type"] != "missing":
  #         raise e
  #   return results  # type: ignore
