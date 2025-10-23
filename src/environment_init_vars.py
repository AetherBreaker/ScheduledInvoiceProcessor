# sourcery skip: raise-from-previous-error
if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

import os
import sys
from logging import getLogger
from pathlib import Path
from zoneinfo import ZoneInfo

from environment_settings import Settings

logger = getLogger(__name__)


# Settings
SETTINGS = Settings()  # type: ignore

# Folder paths
CWD = Path.cwd()

SPEC_CWD = Path(__file__).parent if getattr(sys, "frozen", False) else Path.cwd()

# Support environment variable overrides for Docker secrets
# In Docker, these will be set to /run/secrets/<secret_name>

if SETTINGS.googe_api_key_file.name != "db-key.json":
  raise FileNotFoundError(f"Google API key file name must be 'db-key.json', got: {SETTINGS.googe_api_key_file.name}")
if not SETTINGS.googe_api_key_file.exists():
  raise FileNotFoundError(
    f"Google API key file not found at: {SETTINGS.googe_api_key_file}\n"
    "Please create a service account key in the Google Cloud Console "
    "and save it as 'db-key.json' in the current directory.\n"
    "For Docker: ensure secrets are properly mounted in docker-compose.yml"
  )

if SETTINGS.sft_website_creds_file.name != "sft_creds.json":
  raise FileNotFoundError(f"SFT website creds file name must be 'sft_creds.json', got: {SETTINGS.sft_website_creds_file.name}")
if not SETTINGS.sft_website_creds_file.exists():
  raise FileNotFoundError(
    f"SFT website creds file not found at: {SETTINGS.sft_website_creds_file}\n"
    "Please create the creds file and save it as 'sft_creds.json' in the current directory.\n"
    "For Docker: ensure secrets are properly mounted in docker-compose.yml"
  )

if SETTINGS.sas_ftp_creds_file.name != "sas_ftp_creds.json":
  raise FileNotFoundError(f"SAS FTP creds file name must be 'sas_ftp_creds.json', got: {SETTINGS.sas_ftp_creds_file.name}")
if not SETTINGS.sas_ftp_creds_file.exists():
  raise FileNotFoundError(
    f"SAS FTP creds file not found at: {SETTINGS.sas_ftp_creds_file}\n"
    "Please create the creds file and save it as 'sas_ftp_creds.json' in the current directory.\n"
    "For Docker: ensure secrets are properly mounted in docker-compose.yml"
  )


SFT_WEBSITE_CREDS_FILE = Path(os.getenv("SFT_WEBSITE_CREDS_FILE", SPEC_CWD / "sft_creds.json"))


TZ = ZoneInfo("US/Eastern")
