if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import Event, run
from datetime import datetime, timedelta
from logging import getLogger
from pathlib import PosixPath, PurePosixPath

from aiohttp.web import Application, AppRunner, TCPSite
from apscheduler.triggers.cron import CronTrigger
from database.cache import DatabaseCache
from dateutil.relativedelta import SA, relativedelta
from environment_init_vars import CWD, SETTINGS
from logging_config import RICH_CONSOLE
from rich_custom import LiveCustom
from scheduler_config import OrderProcessingScheduler
from supplier_processors import SupplierProcessorBase
from supplier_processors.ryo import RYOProcessor
from supplier_processors.sas import SASProcessor
from typing_custom.enums import SuppliersEnum

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


def test_exception():
  logger.info("Running test_exception to verify error handling in the scheduler.")
  raise ValueError("This is a test exception for verifying error handling in the scheduler.")


async def test_async_exception():
  logger.info("Running test_async_exception to verify error handling in the scheduler (async).")
  raise ValueError("This is a test exception for verifying error handling in the scheduler (async).")


async def main():  # sourcery skip: remove-empty-nested-block
  RICH_CONSOLE.rule("[bold red]Booting...[/]", style="bold red")
  try:
    with LiveCustom(refresh_per_second=10, console=RICH_CONSOLE) as live:
      cache = DatabaseCache()

      for processor in supplier_register.values():
        processor(live.pbar)

      await cache.refresh_cache()
      await reschedule_all_tasks()

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

      # trigger 2 minutes after startup to allow scheduler to initialize
      now = datetime.now()
      first_run_time = now.replace(second=0, microsecond=0) + timedelta(minutes=2)
      scheduler.add_job(
        test_exception,
        next_run_time=first_run_time,
        id="test_exception",
        replace_existing=True,
      )

      scheduler.add_job(
        test_async_exception,
        next_run_time=first_run_time + timedelta(minutes=1),
        id="test_async_exception",
        replace_existing=True,
      )

      if __debug__:
        pass

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
        await Event().wait()

  except Exception as e:
    logger.exception(f"Fatal error in main: {e}")
    exit(1)  # Exit with non-zero code to indicate failure to Coolify


if __name__ == "__main__":
  from sys import platform

  if platform in ("win32", "cygwin", "cli"):
    from winloop import run
  else:
    # if we're on apple or linux do this instead
    from uvloop import run  # type: ignore
  run(main())
