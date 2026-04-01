if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import run
from datetime import datetime
from logging import getLogger
from pathlib import PosixPath, PurePosixPath
from typing import NoReturn

from aiohttp.web import Application, AppRunner, TCPSite
from apscheduler.triggers.cron import CronTrigger
from database.cache import DatabaseCache
from dateutil.relativedelta import SA, relativedelta
from environment_init_vars import CWD, FATAL_EVENT, SETTINGS
from err_handling import get_last_fatal_details
from logging_config import RICH_CONSOLE
from rich_custom import LiveCustom
from scheduler_config import OrderProcessingScheduler
from supplier_processors import SupplierProcessorBase
from supplier_processors.ryo import RYOProcessor
from supplier_processors.sas import SASProcessor
from typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

logger = getLogger(__name__)


# Heartbeat file for health checks
HEARTBEAT_FILE = PurePosixPath("/app/src/logs/heartbeat.txt") if __debug__ else PosixPath("/app/src/logs/heartbeat.txt")

if not __debug__:

  def write_heartbeat():
    """Write current timestamp to heartbeat file for health monitoring."""
    try:
      HEARTBEAT_FILE.write_text(datetime.now().isoformat())  # type: ignore
    except Exception as e:
      logger.error(f"Failed to write heartbeat: {e}")
else:

  def write_heartbeat():
    pass


expected_suppliers: dict[SuppliersEnum, type[SupplierProcessorBase]] = {
  SuppliersEnum.SAS: SASProcessor,
  SuppliersEnum.RYO: RYOProcessor,
}


supplier_register: dict[SuppliersEnum, type[SupplierProcessorBase]] = {
  supplier: processor for supplier, processor in expected_suppliers.items() if processor.check_connections()
}


scheduler = OrderProcessingScheduler.init_scheduler()


async def bootstrap_runtime(live: LiveCustom) -> DatabaseCache:
  try:
    cache = DatabaseCache()
  except Exception:
    logger.critical("Failed to initialize database cache during startup.", exc_info=True)
    raise

  for processor in supplier_register.values():
    processor(live.pbar)

  try:
    await cache.refresh_cache()
  except Exception:
    logger.critical("Initial cache refresh failed during startup.", exc_info=True)
    raise

  try:
    await reschedule_all_tasks()
  except Exception:
    logger.critical("Initial task schedule failed during startup.", exc_info=True)
    raise

  return cache


async def reschedule_all_tasks():
  cache = DatabaseCache()
  current_week = cache.schedule
  previous_week = cache.prev_week_schedule

  for supplier, processor in supplier_register.items():
    scheduler.add_job(
      processor().pickup_files,
      CronTrigger(minute="2-59/5"),
      id=f"{supplier}_pickup_files",
      replace_existing=True,
    )
    scheduler.add_job(
      processor().dropoff_files,
      CronTrigger(minute="4-59/5"),
      id=f"{supplier}_dropoff_files",
      replace_existing=True,
    )

    scheduler.add_job(
      processor().save_queue_backups_off_thread,
      CronTrigger(minute="*/5"),
      id=f"{supplier}_save_queue_backups",
      replace_existing=True,
    )

    scheduler.add_job(
      processor().cleanup_stale_queue_entries,
      CronTrigger(hour=3, minute=0),
      id=f"{supplier}_cleanup_stale_queue_entries",
      replace_existing=True,
    )

  async for order in current_week.walk_typed_rows():
    if not order.customer or not order.store:
      continue
    scheduler.add_job(
      supplier_register[order.supplier]().register_pickup,
      CronTrigger(
        minute="1-59/5",
        start_date=order.invoice_pickup_time,
        end_date=order.invoice_dropoff_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_dropoff_time,
        "current_week": True,
      },
      id=f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
      replace_existing=True,
      jobstore="order_processing",
    )

    scheduler.add_job(
      supplier_register[order.supplier]().register_dropoff,
      CronTrigger(
        minute="3-59/5",
        start_date=order.invoice_dropoff_time,
        end_date=order.invoice_dropoff_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_dropoff_time,
        "current_week": True,
      },
      id=f"{order.supplier}_register_dropoff_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
      replace_existing=True,
      jobstore="order_processing",
    )

  async for order in previous_week.walk_typed_rows():
    if not order.customer or not order.store:
      continue
    scheduler.add_job(
      supplier_register[order.supplier]().register_pickup,
      CronTrigger(
        minute="1-59/5",
        start_date=order.invoice_pickup_time,
        end_date=order.invoice_dropoff_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_dropoff_time,
        "current_week": False,
      },
      id=f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
      replace_existing=True,
      jobstore="order_processing",
    )

    scheduler.add_job(
      supplier_register[order.supplier]().register_dropoff,
      CronTrigger(
        minute="3-59/5",
        start_date=order.invoice_dropoff_time,
        end_date=order.invoice_dropoff_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_dropoff_time,
        "current_week": False,
      },
      id=f"{order.supplier}_register_dropoff_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
      replace_existing=True,
      jobstore="order_processing",
    )

  scheduler.print_jobs()


async def flip_week():
  scheduler.pause()
  scheduler.remove_all_jobs("order_processing")
  cache = DatabaseCache()
  await cache.flip_to_new_week()

  await reschedule_all_tasks()

  scheduler.print_jobs()

  scheduler.resume()


async def main() -> NoReturn:  # sourcery skip: remove-empty-nested-block
  RICH_CONSOLE.rule("[bold red]Booting...[/]", style="bold red")
  with LiveCustom(refresh_per_second=10, console=RICH_CONSOLE) as live:
    cache = await bootstrap_runtime(live)

    scheduler.add_job(
      cache.refresh_cache,
      CronTrigger(minute="*/30"),
      id="refresh_cache",
      replace_existing=True,
    )

    scheduler.add_job(
      cache.submit_queued_writes_to_pool,
      CronTrigger(second="*/30"),
      id="submit_queued_writes_to_pool",
      replace_existing=True,
    )

    scheduler.add_job(
      reschedule_all_tasks,
      CronTrigger(
        hour=5,
      ),
      id="reschedule_all_tasks",
      replace_existing=True,
    )

    scheduler.add_job(
      flip_week,
      CronTrigger(
        day_of_week="sun",
        hour=0,
        minute=0,
        second=0,
      ),
      id="flip_week",
      replace_existing=True,
    )

    scheduler.add_job(
      scheduler.print_jobs,
      CronTrigger(minute="*/1"),
      id="print_jobs",
      replace_existing=True,
    )

    # Heartbeat job - writes timestamp every minute for health monitoring
    scheduler.add_job(
      write_heartbeat,
      CronTrigger(minute="*/1"),
      id="heartbeat",
      replace_existing=True,
    )

    scheduler.start()

    # Write initial heartbeat on startup
    write_heartbeat()

    scheduler.print_jobs()

    app = Application()
    app.router.add_static("/", CWD / "logs", show_index=True, follow_symlinks=True, append_version=True)
    runner = AppRunner(app)
    await runner.setup()
    site = TCPSite(runner, SETTINGS.file_serve_host, SETTINGS.file_serve_port)
    await site.start()

    if __debug__:
      pass

      # await cache.order_log.log_action(
      #   supplier=SuppliersEnum.RYO,
      #   store=None,
      #   invoice_num=None,
      #   customer=None,
      #   action=LogActionEnum.FILE_DROPPED_OFF,
      #   status=StatusCode.FAILURE,
      #   action_datetime=datetime(2026, 4, 1, 10, 4, 2, 903824),
      #   week_end_date=None,
      #   note="Nothing logged",
      # )
      # pass

      # scheduler.print_jobs()

      # global TESTING_THIS_WEEK

      # TESTING_THIS_WEEK.clear()
      # TESTING_THIS_WEEK.append(True)

      # await flip_week()

      # scheduler.print_jobs()

      # await reschedule_all_tasks()

      # scheduler.print_jobs()

    RICH_CONSOLE.rule("[bold red]Boot Done[/]", style="bold red")
    with RICH_CONSOLE.status("Application is running."):
      await FATAL_EVENT

      try:
        logger.warning("Fatal shutdown: stopping scheduler to freeze application state")
        scheduler.pause()
        scheduler.shutdown(wait=False)
      except Exception as e:
        logger.error(f"Fatal shutdown: failed to stop scheduler cleanly: {e}", exc_info=True)

      fatal_details = get_last_fatal_details()

      if fatal_details["is_database_origin"]:
        logger.warning(
          "Fatal shutdown: skipping final Google Sheets flush because fatal error originated in database interface"
          f" (type={fatal_details['exception_type']}, message={fatal_details['exception_message']})"
        )
      else:
        try:
          if await cache.has_pending_writes():
            logger.warning("Fatal shutdown: attempting final Google Sheets flush of queued writes")
            await cache.submit_queued_writes_to_pool()
            logger.warning("Fatal shutdown: final Google Sheets flush completed")
        except Exception as e:
          logger.error(f"Fatal shutdown: final Google Sheets flush failed: {e}", exc_info=True)

      exit(1)

  raise RuntimeError("How did we get here? The main function should never exit normally.")


if __name__ == "__main__":
  from sys import platform

  if platform in ("win32", "cygwin", "cli"):
    from winloop import run
  else:
    # if we're on apple or linux do this instead
    from uvloop import run  # type: ignore
  run(main())
