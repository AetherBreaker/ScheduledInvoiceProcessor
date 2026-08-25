# Standard library imports
from sys import platform

# Third party imports
from rich.console import Console

# First party imports
from aeth_ext import initialize

RICH_CONSOLE = Console(
  width=None if platform == "win32" else 165,
  log_time=platform == "win32",
)
PROJECT_NAME = "scheduled-invoice-processor"
HEARTBEAT_SLUG = "scheduled-invoice-processor"


def run_app() -> None:
  """Run the main application loop and exit with a code that reflects how it stopped."""
  # initialize(asyncio=True, logging=True)
  initialize(asyncio=True, logging="socket")

  # Standard library imports
  import sys
  from asyncio import run

  # First party imports
  from aeth_ext.errors.shutdown import SHUTDOWN
  from scheduled_invoice_processor.startup import exit_code_for_shutdown, main

  try:
    run(main())
  except KeyboardInterrupt:
    # aeth_ext's exit nudge (simulated SIGINT). Normally main() returns on its own after awaiting
    # SHUTDOWN_COMPLETE and the nudge is skipped; it only lands here if main()'s tail overran the
    # shutdown budget. Not an error either way; the kind below says how we stopped.
    pass
  sys.exit(exit_code_for_shutdown(SHUTDOWN.kind))


if __name__ == "__main__":
  run_app()
