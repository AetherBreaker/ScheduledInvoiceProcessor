"""aeth_ext v8 shutdown callbacks.

Both run on aeth_ext's shutdown thread (`ShutdownPhase.THREADED`), concurrently with the tail of `startup.main()`
(the pass is started in the same synchronous stretch that requests the shutdown, so it is running — not
finished — when the `await SHUTDOWN` waiter wakes). `main()` therefore ends with `await SHUTDOWN_COMPLETE`
(aeth_ext >= 8.0.1), which resolves once every callback has run or been skipped; only then does it return.
aeth_ext also joins the pass at interpreter exit as a safety net for a main that returns early.

Rules for this phase: may block and log; must not `scheduler.shutdown()` (that cancels asyncio tasks from a
foreign thread); `required=True` callbacks run even after the budget is exhausted.
"""

# Standard library imports
from collections.abc import Callable
from logging import getLogger
from typing import TYPE_CHECKING

# First party imports
from aeth_ext.errors.shutdown import ShutdownPhase, register_for_shutdown
from scheduled_invoice_processor.database import trail_is_database_origin

if TYPE_CHECKING:
  # First party imports
  from aeth_ext.errors.exception_trail import ExceptionTrail
  from scheduled_invoice_processor.database import DatabaseCache
  from scheduled_invoice_processor.scheduler_config import OrderProcessingScheduler

logger = getLogger(__name__)

type ShutdownCallback = Callable[[tuple["ExceptionTrail", ...]], None]

FREEZE_SCHEDULER_PRIORITY = -10
"""Runs first: no new job may start while the sheet is being flushed."""

FINAL_SHEETS_FLUSH_PRIORITY = 0


def freeze_scheduler(scheduler: OrderProcessingScheduler) -> ShutdownCallback:
  """Stop new jobs from starting. `AsyncIOScheduler.pause()` is thread-safe (its wakeup goes through
  `call_soon_threadsafe`); `shutdown()` is not called here on purpose."""

  def _freeze(_trails: tuple[ExceptionTrail, ...]) -> None:
    try:
      scheduler.pause()
      logger.warning("Shutdown: scheduler paused; no new jobs will start")
    except Exception:
      logger.exception("Shutdown: failed to pause the scheduler")

  return _freeze


def final_sheets_flush(cache: DatabaseCache) -> ShutdownCallback:
  """Write the in-memory Sheets update queue. Skipped when a fatal error originated inside the database interface
  (A4): the write would only fail again."""

  def _flush(trails: tuple[ExceptionTrail, ...]) -> None:
    database_origin_trail = next((trail for trail in trails if trail_is_database_origin(trail)), None)
    if database_origin_trail is not None:
      logger.warning(
        "Shutdown: skipping final Google Sheets flush because a fatal error originated in the database interface (origin=%s in %s)",
        database_origin_trail.origin.module,
        database_origin_trail.origin.file,
      )
      return
    try:
      if cache.flush_queued_writes():
        logger.warning("Shutdown: final Google Sheets flush completed")
      else:
        logger.info("Shutdown: no queued Google Sheets writes to flush")
    except Exception:
      logger.exception("Shutdown: final Google Sheets flush failed")

  return _flush


# The callbacks are deliberately closures, not bound methods: `register_for_shutdown` holds bound methods only via
# a `WeakMethod` (`aeth_ext/errors/shutdown.py` ~462-468), so a method registration whose instance is not otherwise
# referenced would be silently dropped before the shutdown pass ran.
def register_shutdown_hooks(scheduler: OrderProcessingScheduler, cache: DatabaseCache) -> None:
  register_for_shutdown(freeze_scheduler(scheduler), phase=ShutdownPhase.THREADED, priority=FREEZE_SCHEDULER_PRIORITY)
  register_for_shutdown(final_sheets_flush(cache), phase=ShutdownPhase.THREADED, priority=FINAL_SHEETS_FLUSH_PRIORITY, required=True)
