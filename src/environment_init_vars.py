# sourcery skip: raise-from-previous-error
if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import sys
from logging import getLogger
from zoneinfo import ZoneInfo

from environment_settings import Settings
from typing_custom.custom_path import CustomPath

logger = getLogger(__name__)


# Settings
SETTINGS = Settings()  # type: ignore

# Folder paths
CWD = CustomPath.cwd()

SPEC_CWD = CustomPath(__file__).parent if getattr(sys, "frozen", False) else CustomPath.cwd()

# Support environment variable overrides for Docker secrets
# In Docker, these will be set to /run/secrets/<secret_name>


if SETTINGS.googe_api_key_file.stem != "db-key":
  raise FileNotFoundError(f"Google API key file name must be 'db-key', got: {SETTINGS.googe_api_key_file.stem}")
if not SETTINGS.googe_api_key_file.exists():
  raise FileNotFoundError(
    f"Google API key file not found at: {SETTINGS.googe_api_key_file}\n"
    "Please create a service account key in the Google Cloud Console "
    "and save it as 'db-key' in the current directory.\n"
    "For Docker: ensure secrets are properly mounted in docker-compose.yml"
  )

if SETTINGS.sft_website_creds_file.stem != "sft_creds":
  raise FileNotFoundError(f"SFT website creds file name must be 'sft_creds', got: {SETTINGS.sft_website_creds_file.stem}")
if not SETTINGS.sft_website_creds_file.exists():
  raise FileNotFoundError(
    f"SFT website creds file not found at: {SETTINGS.sft_website_creds_file}\n"
    "Please create the creds file and save it as 'sft_creds' in the current directory.\n"
    "For Docker: ensure secrets are properly mounted in docker-compose.yml"
  )

if SETTINGS.sas_ftp_creds_file.stem != "sas_ftp_creds":
  raise FileNotFoundError(f"SAS FTP creds file name must be 'sas_ftp_creds', got: {SETTINGS.sas_ftp_creds_file.stem}")
if not SETTINGS.sas_ftp_creds_file.exists():
  raise FileNotFoundError(
    f"SAS FTP creds file not found at: {SETTINGS.sas_ftp_creds_file}\n"
    "Please create the creds file and save it as 'sas_ftp_creds' in the current directory.\n"
    "For Docker: ensure secrets are properly mounted in docker-compose.yml"
  )

if SETTINGS.ryo_ftp_creds_file.stem != "ryo_ftp_creds":
  raise FileNotFoundError(f"SAS FTP creds file name must be 'ryo_ftp_creds', got: {SETTINGS.sas_ftp_creds_file.stem}")
if not SETTINGS.ryo_ftp_creds_file.exists():
  raise FileNotFoundError(
    f"SAS FTP creds file not found at: {SETTINGS.ryo_ftp_creds_file}\n"
    "Please create the creds file and save it as 'ryo_ftp_creds' in the current directory.\n"
    "For Docker: ensure secrets are properly mounted in docker-compose.yml"
  )


TZ = ZoneInfo("US/Eastern")
