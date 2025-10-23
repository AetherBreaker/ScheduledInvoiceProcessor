if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import Event, run
from logging import getLogger

from apscheduler.triggers.cron import CronTrigger
from database.cache import DatabaseCache
from dateutil.relativedelta import SA, relativedelta
from logging_config import RICH_CONSOLE
from rich_custom import LiveCustom
from scheduler_config import OrderProcessingScheduler
from supplier_processors.sas import SASProcessor
from typing_custom.enums import SuppliersEnum

logger = getLogger(__name__)


supplier_register = {
  SuppliersEnum.SAS: SASProcessor,
  # SuppliersEnum.RYO: ...,
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
        end_date=order.invoice_application_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_application_time,
        "current_week": True,
      },
      id=f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
      replace_existing=True,
      jobstore="order_processing",
    )

    scheduler.add_job(
      supplier_register[order.supplier]().register_application,
      CronTrigger(
        minute="3-59/5",
        start_date=order.invoice_application_time,
        end_date=order.invoice_application_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_application_time,
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
        end_date=order.invoice_application_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_application_time,
        "current_week": False,
      },
      id=f"{order.supplier}_register_pickup_{order.store:0>3}_{order.customer}_{order.invoice_pickup_time.isoformat()}",
      replace_existing=True,
      jobstore="order_processing",
    )

    scheduler.add_job(
      supplier_register[order.supplier]().register_application,
      CronTrigger(
        minute="3-59/5",
        start_date=order.invoice_application_time,
        end_date=order.invoice_application_time + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59),
      ),
      kwargs={
        "storenum": order.store,
        "customer_id": order.customer,
        "pickup_date": order.invoice_pickup_time,
        "dropoff_date": order.invoice_application_time,
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


async def main():  # sourcery skip: remove-empty-nested-block
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

    scheduler.start()

    scheduler.print_jobs()

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

    await Event().wait()


if __name__ == "__main__":
  run(main())
