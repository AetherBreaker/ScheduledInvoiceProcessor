from __future__ import annotations

import os
import sys
from logging import getLogger
from pathlib import Path
from zoneinfo import ZoneInfo

from aiologic import Event
from environment_settings import Settings

logger = getLogger(__name__)

if os.name != "nt" and hasattr(os, "geteuid") and os.geteuid() == 0:
  logger.warning("Process is running as root on a Unix system. This is not recommended for production.")


# Settings
SETTINGS = Settings.model_validate({})

# Folder paths
CWD = Path.cwd()

SPEC_CWD = Path(__file__).parent if getattr(sys, "frozen", False) else Path.cwd()

FATAL_EVENT = Event()

TZ = ZoneInfo("US/Eastern")

HOST_NAME = (
  f"{SETTINGS.file_serve_host}:{SETTINGS.file_serve_port}"
  if SETTINGS.file_serve_public_domain is None
  else SETTINGS.file_serve_public_domain
)
