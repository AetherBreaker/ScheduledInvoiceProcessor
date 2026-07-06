if __name__ == "__main__":
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
  PROJECT_NAME = "ScheduledInvoiceProcessor"
  HOST = "log-serve"
  PORT = 9020

  initialize(asyncio=True, logging="socket")
else:
  # Third party imports
  from rich import get_console

  RICH_CONSOLE = get_console()

# Standard library imports
import sys
from asyncio import run, sleep
from datetime import datetime
from logging import getLogger
from typing import TYPE_CHECKING

# Third party imports
from aiohttp.web import Application, AppRunner, FileResponse, Request, TCPSite
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from dateutil.relativedelta import SA, relativedelta

# First party imports
from aeth_ext.errors.err_handling import FATAL_EVENT
from aeth_ext.rich.progress import Progress
from scheduled_invoice_processor.database import DatabaseCache
from scheduled_invoice_processor.environment_init_vars import CWD, SETTINGS
from scheduled_invoice_processor.err_handling import get_last_fatal_details
from scheduled_invoice_processor.scheduler_config import OrderProcessingScheduler
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
from scheduled_invoice_processor.suppliers.sas import SASProcessor
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # Standard library imports
  from typing import NoReturn

  # First party imports
  from scheduled_invoice_processor.suppliers import SupplierProcessorBase

logger = getLogger(__name__)


if not __debug__:
  # Heartbeat file for health checks
  HEARTBEAT_FILE = SETTINGS.log_loc_folder / "heartbeat.txt"

  def write_heartbeat():
    """Write current timestamp to heartbeat file for health monitoring."""
    try:
      HEARTBEAT_FILE.write_text(datetime.now(tz=SETTINGS.tz).isoformat())
    except Exception:
      logger.exception("Failed to write heartbeat: %s")
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


FAVICON_PATH = CWD / "favicon.ico"


async def bootstrap_runtime(pbar: Progress) -> DatabaseCache:
  try:
    cache = DatabaseCache()
  except Exception:
    logger.critical("Failed to initialize database cache during startup.", exc_info=True)
    raise

  try:
    await cache.refresh_cache()
  except Exception:
    logger.critical("Initial cache refresh failed during startup.", exc_info=True)
    raise

  for processor in supplier_register.values():
    p = processor(pbar)
    await p.clean_stale_queue_entries()

  try:
    await reschedule_all_tasks()
  except Exception:
    logger.critical("Initial task schedule failed during startup.", exc_info=True)
    raise

  return cache


async def reschedule_all_tasks():
  cache = DatabaseCache()
  current_week = cache.schedule
  # previous_week = cache.prev_week_schedule

  scheduler.remove_all_jobs("order_processing")  # Clear all order processing jobs for a clean reset

  for supplier, processor in supplier_register.items():
    scheduler.add_job(
      processor().pickup_files,
      CronTrigger(minute="2-59/10"),
      id=f"{supplier}_pickup_files",
      replace_existing=True,
    )
    scheduler.add_job(
      processor().dropoff_files,
      CronTrigger(minute="6-59/10"),
      id=f"{supplier}_dropoff_files",
      replace_existing=True,
    )

    scheduler.add_job(
      processor().save_queue_backups_off_thread,
      CronTrigger(minute="8-59/10"),
      id=f"{supplier}_save_queue_backups",
      replace_existing=True,
    )

    scheduler.add_job(
      processor().clean_stale_queue_entries,
      CronTrigger(hour=3, minute=0),
      id=f"{supplier}_cleanup_stale_queue_entries",
      replace_existing=True,
    )

  current_week_orders = [order async for order in current_week.walk_typed_rows()]

  for order in current_week_orders:
    if not order.customer or not order.store or order.supplier not in supplier_register:
      continue
    picked_up = await cache.schedule.check_toggled((order.supplier, order.store), DatabaseScheduleColumns.invoice_grabbed)
    applied = await cache.schedule.check_toggled((order.supplier, order.store), DatabaseScheduleColumns.invoice_applied)
    manually_moved = await cache.schedule.check_toggled((order.supplier, order.store), DatabaseScheduleColumns.manually_moved)

    reg_pickup_job_id = f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}"
    reg_dropoff_job_id = (
      f"{order.supplier}_register_dropoff_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}"
    )

    processor = supplier_register[order.supplier]()

    if not picked_up and not manually_moved:
      scheduler.add_job(
        processor.register_pickup,
        CronTrigger(
          minute="0-59/10",
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
        id=reg_pickup_job_id,
        replace_existing=True,
        jobstore="order_processing",
      )
      logger.info("Scheduled %s.register_pickup for order: %s", processor.__class__.__name__, reg_pickup_job_id)
    else:
      try:
        scheduler.remove_job(reg_pickup_job_id, jobstore="order_processing")
      except JobLookupError:
        pass
      else:
        logger.info("Removed register_pickup because order was marked as picked up: %s", reg_pickup_job_id)
    if not applied and not manually_moved:
      scheduler.add_job(
        processor.register_dropoff,
        CronTrigger(
          minute="4-59/10",
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
        id=reg_dropoff_job_id,
        replace_existing=True,
        jobstore="order_processing",
      )
      logger.info("Scheduled %s.register_dropoff for order: %s", processor.__class__.__name__, reg_dropoff_job_id)
    else:
      try:
        scheduler.remove_job(reg_dropoff_job_id, jobstore="order_processing")
      except JobLookupError:
        pass
      else:
        logger.info("Removed register_dropoff because order was marked as applied: %s", reg_dropoff_job_id)

  # previous_week_orders = [order async for order in previous_week.walk_typed_rows()]
  # for order in previous_week_orders:
  #   if not order.customer or not order.store or order.supplier not in supplier_register:
  #     continue
  #   picked_up = await previous_week.check_toggled((order.supplier, order.store), DatabaseScheduleColumns.invoice_grabbed)
  #   applied = await previous_week.check_toggled((order.supplier, order.store), DatabaseScheduleColumns.invoice_applied)
  #   manually_moved = await previous_week.check_toggled((order.supplier, order.store), DatabaseScheduleColumns.manually_moved)

  #   reg_pickup_job_id = f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}"
  #   reg_dropoff_job_id = (
  #     f"{order.supplier}_register_dropoff_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}"
  #   )

  #   processor = supplier_register[order.supplier]()

  #   if not picked_up and not manually_moved:
  #     scheduler.add_job(
  #       processor.register_pickup,
  #       CronTrigger(
  #         minute="0-59/10",
  #         start_date=order.invoice_pickup_time,
  #         end_date=order.invoice_dropoff_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
  #       ),
  #       kwargs={
  #         "storenum": order.store,
  #         "customer_id": order.customer,
  #         "pickup_date": order.invoice_pickup_time,
  #         "dropoff_date": order.invoice_dropoff_time,
  #         "current_week": False,
  #       },
  #       id=reg_pickup_job_id,
  #       replace_existing=True,
  #       jobstore="order_processing",
  #     )
  #     logger.info(f"Removed register_pickup because order was marked as picked up: {reg_pickup_job_id}")
  #   else:
  #     try:
  #       scheduler.remove_job(reg_pickup_job_id, jobstore="order_processing")
  #     except JobLookupError:
  #       pass
  #     else:
  #       logger.info(f"Removed register_pickup because order was marked as picked up: {reg_pickup_job_id}")

  #   if not applied and not manually_moved:
  #     scheduler.add_job(
  #       processor.register_dropoff,
  #       CronTrigger(
  #         minute="4-59/10",
  #         start_date=order.invoice_dropoff_time,
  #         end_date=order.invoice_dropoff_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
  #       ),
  #       kwargs={
  #         "storenum": order.store,
  #         "customer_id": order.customer,
  #         "pickup_date": order.invoice_pickup_time,
  #         "dropoff_date": order.invoice_dropoff_time,
  #         "current_week": False,
  #       },
  #       id=reg_dropoff_job_id,
  #       replace_existing=True,
  #       jobstore="order_processing",
  #     )
  #     logger.info(f"Scheduled {processor.__class__.__name__}.register_dropoff for order: {reg_dropoff_job_id}")
  #   else:
  #     try:
  #       scheduler.remove_job(reg_dropoff_job_id, jobstore="order_processing")
  #     except JobLookupError:
  #       pass
  #     else:
  #       logger.info(f"Removed register_dropoff because order was marked as applied: {reg_dropoff_job_id}")

  # scheduler.print_jobs()


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
  with Progress(console=RICH_CONSOLE, auto_refresh=False) as pbar:
    cache = await bootstrap_runtime(pbar)

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

    async def favicon(request: Request):
      return FileResponse(FAVICON_PATH)

    app.router.add_get("/favicon.ico", favicon)
    app.router.add_static("/", SETTINGS.log_loc_folder, show_index=True, follow_symlinks=True, append_version=True)
    runner = AppRunner(app)
    await runner.setup()
    site = TCPSite(runner, SETTINGS.file_serve_host, SETTINGS.file_serve_port)
    await site.start()

    if __debug__:
      pass
      # # force run immediately for testing
      # # Standard library imports
      # from asyncio import create_task, gather

      # orders = [order async for order in cache.schedule.walk_typed_rows()]

      # register_pickup_tasks = []
      # for order in orders:
      #   if order.supplier not in supplier_register:
      #     continue
      #   task = create_task(
      #     supplier_register[order.supplier]().register_pickup(
      #       storenum=order.store,
      #       customer_id=order.customer,
      #       pickup_date=order.invoice_pickup_time,
      #       dropoff_date=order.invoice_dropoff_time,
      #       current_week=True,
      #     )
      #   )
      #   register_pickup_tasks.append(task)
      # await gather(*register_pickup_tasks)

      # pickup_tasks = []
      # for processor in supplier_register.values():
      #   task = create_task(processor().pickup_files())
      #   pickup_tasks.append(task)
      # await gather(*pickup_tasks)

      # register_dropoff_tasks = []
      # for order in orders:
      #   if order.supplier not in supplier_register:
      #     continue
      #   task = create_task(
      #     supplier_register[order.supplier]().register_dropoff(
      #       storenum=order.store,
      #       customer_id=order.customer,
      #       pickup_date=order.invoice_pickup_time,
      #       dropoff_date=order.invoice_dropoff_time,
      #       current_week=True,
      #     )
      #   )
      #   register_dropoff_tasks.append(task)
      # await gather(*register_dropoff_tasks)

      # dropoff_tasks = []
      # for processor in supplier_register.values():
      #   task = create_task(processor().dropoff_files())
      #   dropoff_tasks.append(task)
      # await gather(*dropoff_tasks)

      # await cache.submit_queued_writes_to_pool()

    RICH_CONSOLE.rule("[bold red]Boot Done[/]", style="bold red")
    with RICH_CONSOLE.status("Application is running."):
      await FATAL_EVENT
      fatal_details = get_last_fatal_details()

      if not fatal_details["is_database_origin"] and any(processor().errored for processor in supplier_register.values()):
        await sleep(
          600
        )  # Sleep for 10 minutes to allow pending operations from non-error-state processors to flush through before exiting

      try:
        logger.warning("Fatal shutdown: stopping scheduler to freeze application state")
        scheduler.pause()
        scheduler.shutdown(wait=False)
      except Exception:
        logger.exception("Fatal shutdown: failed to stop scheduler cleanly")

      fatal_details = get_last_fatal_details()

      if fatal_details["is_database_origin"]:
        logger.warning(
          "Fatal shutdown: skipping final Google Sheets flush because fatal error originated in database interface (type=%s, message=%s)",
          fatal_details["exception_type"],
          fatal_details["exception_message"],
        )
      else:
        try:
          if await cache.has_pending_writes():
            logger.warning("Fatal shutdown: attempting final Google Sheets flush of queued writes")
            await cache.submit_queued_writes_to_pool()
            logger.warning("Fatal shutdown: final Google Sheets flush completed")
        except Exception:
          logger.exception("Fatal shutdown: final Google Sheets flush failed")

      sys.exit(1)


if __name__ == "__main__":
  run(main())
