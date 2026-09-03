"""Process-wide settings and paths resolved once at import."""

# Standard library imports
import os
from logging import getLogger
from pathlib import Path

# Local folder imports
from .environment_settings import Settings

logger = getLogger(__name__)

if os.name != "nt" and hasattr(os, "geteuid") and os.geteuid() == 0:
  logger.warning("Process is running as root on a Unix system. This is not recommended for production.")  # pyright: ignore[reportUnreachable]


# Settings
SETTINGS = Settings.model_validate({})

# Folder paths
CWD = Path.cwd()


HOST_NAME = "unavailable"
