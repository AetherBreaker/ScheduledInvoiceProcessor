# Standard library imports
from asyncio import CancelledError, create_task
from contextlib import suppress
from datetime import datetime
from logging import INFO, WARNING, getLogger
from typing import TYPE_CHECKING

# Third party imports
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from dateutil.relativedelta import SA, relativedelta
from rich import get_console

# First party imports
from aeth_ext.errors.shutdown import SHUTDOWN, SHUTDOWN_COMPLETE, ShutdownKind
from aeth_ext.monitoring import run_heartbeat_async, send_heartbeat
from aeth_ext.rich.progress import Progress
from scheduled_invoice_processor.database import DatabaseCache
from scheduled_invoice_processor.environment_init_vars import CWD, SETTINGS
from scheduled_invoice_processor.scheduler_config import OrderProcessingScheduler
from scheduled_invoice_processor.shutdown_hooks import register_shutdown_hooks
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
from scheduled_invoice_processor.suppliers.sas import SASProcessor
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # First party imports
  from scheduled_invoice_processor.suppliers import SupplierProcessorBase

logger = getLogger(__name__)

RICH_CONSOLE = get_console()

HEARTBEAT_FILE = SETTINGS.log_loc_folder / "heartbeat.txt"
FAVICON_PATH = CWD / "favicon.ico"


expected_suppliers: dict[SuppliersEnum, type[SupplierProcessorBase]] = {
  SuppliersEnum.SAS: SASProcessor,
  SuppliersEnum.RYO: RYOProcessor,
}


supplier_register: dict[SuppliersEnum, type[SupplierProcessorBase]] = {
  supplier: processor for supplier, processor in expected_suppliers.items() if processor.check_connections()
}


scheduler = OrderProcessingScheduler.init_scheduler()


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
      CronTrigger(minute="2-59/10", timezone=SETTINGS.tz),
      id=f"{supplier}_pickup_files",
      replace_existing=True,
    )
    scheduler.add_job(
      processor().dropoff_files,
      CronTrigger(minute="6-59/10", timezone=SETTINGS.tz),
      id=f"{supplier}_dropoff_files",
      replace_existing=True,
    )

    scheduler.add_job(
      processor().clean_stale_queue_entries,
      CronTrigger(hour=3, minute=0, timezone=SETTINGS.tz),
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
          timezone=SETTINGS.tz,
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
          timezone=SETTINGS.tz,
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
  #         timezone=SETTINGS.tz,
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
  #         timezone=SETTINGS.tz,
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


def exit_code_for_shutdown(kind: ShutdownKind) -> int:
  """0 for RUNNING (never requested) or GRACEFUL, 1 for FATAL or FORCED. `ShutdownKind` is an IntEnum ordered by
  severity. Kept out of `main()` so it is testable and so `main()` never calls `sys.exit` itself."""
  return 1 if kind >= ShutdownKind.FATAL else 0


async def main() -> None:
  RICH_CONSOLE.rule("[bold red]Booting...[/]", style="bold red")

  send_heartbeat(
    HEARTBEAT_FILE,
    ping_url=SETTINGS.alerts_healthcheck_ping_url,
    pingkey=SETTINGS.alerts_healthcheck_pingkey,
    tz=SETTINGS.tz,
    start=True,
  )

  with Progress(console=RICH_CONSOLE, auto_refresh=False) as pbar:
    cache = await bootstrap_runtime(pbar)

    # Registered as early as there is something to freeze/flush -- before any job is added and before
    # `scheduler.start()`, so a shutdown requested during the rest of boot still gets the sheet flush.
    register_shutdown_hooks(scheduler, cache)

    scheduler.add_job(
      cache.refresh_cache,
      CronTrigger(minute="*/30", timezone=SETTINGS.tz),
      id="refresh_cache",
      replace_existing=True,
    )

    scheduler.add_job(
      cache.submit_queued_writes_to_pool,
      CronTrigger(second="*/30", timezone=SETTINGS.tz),
      id="submit_queued_writes_to_pool",
      replace_existing=True,
    )

    await _run_debug_code(cache)

    scheduler.add_job(
      reschedule_all_tasks,
      CronTrigger(hour=5, timezone=SETTINGS.tz),
      id="reschedule_all_tasks",
      replace_existing=True,
    )

    scheduler.add_job(
      flip_week,
      CronTrigger(day_of_week="sun", hour=0, minute=0, second=0, timezone=SETTINGS.tz),
      id="flip_week",
      replace_existing=True,
    )

    scheduler.add_job(
      scheduler.print_jobs,
      CronTrigger(minute="*/1", timezone=SETTINGS.tz),
      id="print_jobs",
      replace_existing=True,
    )

    # send_start=False: the boot-time send_heartbeat() call above already sent
    # the one "start" ping for this run.
    periodic_heartbeat_task = create_task(
      run_heartbeat_async(
        HEARTBEAT_FILE,
        ping_url=SETTINGS.alerts_healthcheck_ping_url,
        pingkey=SETTINGS.alerts_healthcheck_pingkey,
        send_start=False,
        tz=SETTINGS.tz,
      )
    )

    # # Heartbeat job - writes timestamp every minute for health monitoring
    # scheduler.add_job(
    #   write_heartbeat,
    #   CronTrigger(minute="*/1", timezone=SETTINGS.tz),
    #   id="heartbeat",
    #   replace_existing=True,
    # )

    scheduler.start()

    # Write initial heartbeat on startup
    # write_heartbeat()

    scheduler.print_jobs()

    RICH_CONSOLE.rule("[bold red]Boot Done[/]", style="bold red")

    with RICH_CONSOLE.status("Application is running."):
      await SHUTDOWN

    # `await SHUTDOWN` resolves when the shutdown is *requested*; aeth_ext's threaded pass (which runs the
    # shutdown_hooks callbacks: pause the scheduler, flush the sheet) has started but not finished. This
    # tail runs alongside it. Queue backups are persisted on every change and once more at atexit (A2).
    logger.log(INFO if SHUTDOWN.kind is ShutdownKind.GRACEFUL else WARNING, "Shutdown requested (%s); stopping", SHUTDOWN.kind.name)
    periodic_heartbeat_task.cancel()
    with suppress(CancelledError):
      await periodic_heartbeat_task
    try:
      scheduler.shutdown(wait=False)
    except Exception:
      logger.exception("Shutdown: failed to stop the scheduler cleanly")

    # aeth_ext 8.0.1 lifecycle: wait for the threaded pass to finish before returning, so the required
    # Sheets flush can never race interpreter exit. Awaiting also declares "a tail follows", which makes
    # aeth_ext hold its exit nudge until the GRACEFUL budget (7 s from the request) or a short grace
    # after completion, whichever is later, and skip it once the main thread has finished -- so the
    # normal path exits via run_app's sys.exit, not via KeyboardInterrupt. The window also covers
    # asyncio.run()'s own close (which joins in-flight to_thread FTP transfers, ~5 s each): if that
    # outlasts the window the nudge lands inside Runner.close(); run_app still catches it, the worker
    # threads are joined at interpreter finalisation, atexit persists the queues, exit code is correct.
    # Deliberately unbounded (Phase 2 parked for at most 20 s): a wedged required flush hangs here until
    # Docker's 30 s grace, exactly as aeth_ext's own exit-time join would.
    await SHUTDOWN_COMPLETE


async def _run_debug_code(cache: DatabaseCache) -> None:
  if __debug__:
    # force run immediately for testing
    # Standard library imports
    from asyncio import create_task, gather

    orders = [order async for order in cache.schedule.walk_typed_rows()]

    register_pickup_tasks = []
    now = datetime.now(tz=SETTINGS.tz)
    for order in orders:
      if order.supplier not in supplier_register:
        continue
      if order.invoice_dropoff_time < now:
        task = create_task(
          supplier_register[order.supplier]().register_pickup(
            storenum=order.store,
            customer_id=order.customer,
            pickup_date=order.invoice_pickup_time,
            dropoff_date=order.invoice_dropoff_time,
            current_week=True,
          )
        )
        register_pickup_tasks.append(task)
    await gather(*register_pickup_tasks)

    pickup_tasks = []
    for processor in supplier_register.values():
      task = create_task(processor().pickup_files())
      pickup_tasks.append(task)
    await gather(*pickup_tasks)

    register_dropoff_tasks = []
    for order in orders:
      if order.supplier not in supplier_register:
        continue
      if order.invoice_dropoff_time < now:
        task = create_task(
          supplier_register[order.supplier]().register_dropoff(
            storenum=order.store,
            customer_id=order.customer,
            pickup_date=order.invoice_pickup_time,
            dropoff_date=order.invoice_dropoff_time,
            current_week=True,
          )
        )
        register_dropoff_tasks.append(task)
    await gather(*register_dropoff_tasks)

    dropoff_tasks = []
    for processor in supplier_register.values():
      task = create_task(processor().dropoff_files())
      dropoff_tasks.append(task)
    await gather(*dropoff_tasks)

    await cache.submit_queued_writes_to_pool()
