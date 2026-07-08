# pyright: reportPrivateUsage=false
# Standard library imports
from datetime import datetime
from logging import getLogger
from typing import TYPE_CHECKING, ClassVar

# Third party imports
from apscheduler.jobstores.base import JobLookupError
from apscheduler.triggers.cron import CronTrigger
from dateutil.relativedelta import SA, relativedelta

# First party imports
from aeth_ext.command_server.base import CommandServerBase
from aeth_ext.command_server.decorators import command
from scheduled_invoice_processor.database import DatabaseCache
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

if TYPE_CHECKING:
  # First party imports
  from scheduled_invoice_processor.scheduler_config import OrderProcessingScheduler
  from scheduled_invoice_processor.suppliers import SupplierProcessorBase

logger = getLogger(__name__)


def _parse_dt(raw: str) -> datetime:
  """Parse an ISO 8601 string into a timezone-aware datetime in the app timezone."""
  dt = datetime.fromisoformat(raw)
  if dt.tzinfo is None:
    dt = dt.replace(tzinfo=SETTINGS.tz)
  return dt


class InvoiceProcessorCommandServer(CommandServerBase):
  """Command server exposing operational controls for the invoice processor.

  Spun up during the program's async startup sequence.  Commands mutate shared
  runtime state (the supplier processor queues, the scheduler, and the Google
  Sheets-backed database cache) and are therefore carefully interleaved with
  the 10-minute scheduler cycle by acquiring the same per-supplier lock that
  every processor step holds.
  """

  program_name: ClassVar[str] = "ScheduledInvoiceProcessor"
  port: ClassVar[int] = 9010

  def __init__(
    self,
    scheduler: "OrderProcessingScheduler",  # noqa: UP037
    supplier_register: dict[SuppliersEnum, "type[SupplierProcessorBase]"],  # noqa: UP037
  ) -> None:
    super().__init__()
    self._scheduler = scheduler
    self._supplier_register = supplier_register

  @command(
    description=(
      "Change the scheduled pickup and/or dropoff time for an invoice. Updates the "
      "database, any in-flight queue entry, and the scheduler jobs, safely interleaved "
      "with the processing cycle. Set persist_to_base_sheet to also apply the change to "
      "the base template so it survives future week flips."
    )
  )
  async def reschedule_invoice(
    self,
    supplier: str,
    store: int,
    customer: str,
    old_pickup_time: str,
    new_pickup_time: str | None = None,
    new_dropoff_time: str | None = None,
    persist_to_base_sheet: bool = False,
  ) -> str:
    if new_pickup_time is None and new_dropoff_time is None:
      raise ValueError("At least one of new_pickup_time or new_dropoff_time must be provided")

    try:
      supplier_enum = SuppliersEnum(supplier)
    except ValueError:
      raise ValueError(f"Unknown supplier {supplier!r}; expected one of {[s.value for s in SuppliersEnum]}") from None

    if supplier_enum not in self._supplier_register:
      raise ValueError(f"Supplier {supplier_enum.value!r} is not active/registered in this instance")

    old_pickup_dt = _parse_dt(old_pickup_time)
    new_pickup_dt = _parse_dt(new_pickup_time) if new_pickup_time is not None else None
    new_dropoff_dt = _parse_dt(new_dropoff_time) if new_dropoff_time is not None else None

    processor = self._supplier_register[supplier_enum]()
    cache = DatabaseCache()
    changes: list[str] = []

    async with processor._lock:
      # Current DB values fill in whichever time is not being changed.
      current_pickup_dt = await cache.schedule.read_value((supplier_enum, store), DatabaseScheduleColumns.invoice_pickup_time)
      current_dropoff_dt = await cache.schedule.read_value((supplier_enum, store), DatabaseScheduleColumns.invoice_dropoff_time)

      effective_new_pickup = new_pickup_dt if new_pickup_dt is not None else current_pickup_dt
      effective_new_dropoff = new_dropoff_dt if new_dropoff_dt is not None else current_dropoff_dt

      picked_up = await cache.schedule.check_toggled((supplier_enum, store), DatabaseScheduleColumns.invoice_grabbed)
      applied = await cache.schedule.check_toggled((supplier_enum, store), DatabaseScheduleColumns.invoice_applied)
      manually_moved = await cache.schedule.check_toggled((supplier_enum, store), DatabaseScheduleColumns.manually_moved)

      # 1. Relocate any in-flight queue entry, even if the file is mid-transit.
      queue_hit = self._update_queue_entry(
        processor, store, customer, old_pickup_dt, effective_new_pickup, effective_new_dropoff, new_pickup_dt, new_dropoff_dt
      )
      changes.append(f"queue entry {'updated' if queue_hit else 'not found (skipped)'}")

      # 2. Update the live "Current Week" database rows.
      if new_pickup_dt is not None:
        await cache.schedule.write_value(
          (supplier_enum, store),
          DatabaseScheduleColumns.invoice_pickup_time,
          new_pickup_dt,
          cache.schedule._field_type_adapters[DatabaseScheduleColumns.invoice_pickup_time],
        )
        changes.append(f"pickup -> {new_pickup_dt.isoformat()}")
      if new_dropoff_dt is not None:
        await cache.schedule.write_value(
          (supplier_enum, store),
          DatabaseScheduleColumns.invoice_dropoff_time,
          new_dropoff_dt,
          cache.schedule._field_type_adapters[DatabaseScheduleColumns.invoice_dropoff_time],
        )
        changes.append(f"dropoff -> {new_dropoff_dt.isoformat()}")

      # 3. Re-point the scheduler jobs at the new times.
      self._reschedule_jobs(
        processor,
        supplier_enum,
        store,
        customer,
        old_pickup_dt,
        effective_new_pickup,
        effective_new_dropoff,
        picked_up=picked_up,
        applied=applied,
        manually_moved=manually_moved,
      )
      changes.append("scheduler jobs refreshed")

      # 4. Optionally persist the change to the base template sheet.
      if persist_to_base_sheet:
        base_view = await cache.get_base_sheet_view()
        try:
          if new_pickup_dt is not None:
            await base_view.write_value(
              (supplier_enum, store),
              DatabaseScheduleColumns.invoice_pickup_time,
              new_pickup_dt,
              base_view._field_type_adapters[DatabaseScheduleColumns.invoice_pickup_time],
            )
          if new_dropoff_dt is not None:
            await base_view.write_value(
              (supplier_enum, store),
              DatabaseScheduleColumns.invoice_dropoff_time,
              new_dropoff_dt,
              base_view._field_type_adapters[DatabaseScheduleColumns.invoice_dropoff_time],
            )
        finally:
          base_view.freeze()
        changes.append("persisted to base sheet")

    summary = f"Rescheduled {supplier_enum.value} store {store} customer {customer}: " + "; ".join(changes)
    logger.info(summary)
    return summary

  def _update_queue_entry(
    self,
    processor: "SupplierProcessorBase",  # noqa: UP037
    store: int,
    customer: str,
    old_pickup_dt: datetime,
    effective_new_pickup: datetime,
    effective_new_dropoff: datetime,
    new_pickup_dt: datetime | None,
    new_dropoff_dt: datetime | None,
  ) -> bool:
    """Move/mutate a matching queue entry across all four processor queues.

    Returns ``True`` if an entry was found and updated.  Must be called while
    holding ``processor._lock``.
    """
    old_key = processor.assemble_queue_key(store, customer, old_pickup_dt)
    new_key = processor.assemble_queue_key(store, customer, effective_new_pickup)

    queues = (
      processor._file_pickup_queue,
      processor._file_preprocess_queue,
      processor._file_waiting_queue,
      processor._file_dropoff_queue,
    )
    for queue in queues:
      entry = queue.get(old_key)
      if entry is None:
        continue
      del queue[old_key]
      if new_pickup_dt is not None:
        entry.pickup_date = new_pickup_dt
      if new_dropoff_dt is not None:
        entry.dropoff_date = new_dropoff_dt
      entry.file_pattern = processor.assemble_filename_pattern(
        entry.customer_id, effective_new_pickup, effective_new_dropoff, entry._current_week
      )
      queue[new_key] = entry
      return True
    return False

  def _reschedule_jobs(
    self,
    processor: "SupplierProcessorBase",  # noqa: UP037
    supplier_enum: SuppliersEnum,
    store: int,
    customer: str,
    old_pickup_dt: datetime,
    effective_new_pickup: datetime,
    effective_new_dropoff: datetime,
    *,
    picked_up: bool,
    applied: bool,
    manually_moved: bool,
  ) -> None:
    """Remove the old register jobs and re-add them at the new times.

    Mirrors the gating in ``reschedule_all_tasks``: jobs are only (re-)added
    while the corresponding step is still outstanding and the order has not
    been manually moved.  Both job ids key on the pickup time, matching the
    rest of the system.
    """
    end_of_window = effective_new_dropoff + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59)

    old_pickup_iso = old_pickup_dt.isoformat()
    for kind in ("register_pickup", "register_dropoff"):
      old_job_id = f"{supplier_enum}_{kind}_{store:0>3}_{customer}_{old_pickup_iso}"
      try:
        self._scheduler.remove_job(old_job_id, jobstore="order_processing")
      except JobLookupError:
        pass

    new_pickup_iso = effective_new_pickup.isoformat()
    common_kwargs = {
      "storenum": store,
      "customer_id": customer,
      "pickup_date": effective_new_pickup,
      "dropoff_date": effective_new_dropoff,
      "current_week": True,
    }

    if not picked_up and not manually_moved:
      self._scheduler.add_job(
        processor.register_pickup,
        CronTrigger(minute="0-59/10", start_date=effective_new_pickup, end_date=end_of_window),
        kwargs=common_kwargs,
        id=f"{supplier_enum}_register_pickup_{store:0>3}_{customer}_{new_pickup_iso}",
        replace_existing=True,
        jobstore="order_processing",
      )

    if not applied and not manually_moved:
      self._scheduler.add_job(
        processor.register_dropoff,
        CronTrigger(minute="4-59/10", start_date=effective_new_dropoff, end_date=end_of_window),
        kwargs=common_kwargs,
        id=f"{supplier_enum}_register_dropoff_{store:0>3}_{customer}_{new_pickup_iso}",
        replace_existing=True,
        jobstore="order_processing",
      )
