# Standard library imports
from contextvars import ContextVar
from datetime import datetime
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# Third party imports
from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule

# First party imports
from scheduled_invoice_processor.environment_init_vars import CWD, SETTINGS
from scheduled_invoice_processor.ftp_configs import FTPAdapter, SASSFTPClient
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

# Local folder imports
from . import SupplierProcessorBase

if TYPE_CHECKING:
  # Standard library imports
  from re import Pattern

  # First party imports
  from aeth_ext.rich.progress import Progress
  from scheduled_invoice_processor.ftp_configs import AdaptedSFTP
  from scheduled_invoice_processor.typing_custom import CustomerID

logger = getLogger(__name__)


class SASProcessor(SupplierProcessorBase):
  vendor_ftp: FTPAdapter[AdaptedSFTP] = FTPAdapter(SASSFTPClient, container_cls="SASProcessor")

  queue_backup_prefix: str = "sas"

  supplier_name: SuppliersEnum = SuppliersEnum.SAS

  invoice_num_pattern = compile(
    r"^ASAS       "
    r"(?P<invoice_num>\d{6})"
    r"(?P<invoice_date>\d{6})"
    r"\+"
    r"(?P<invoice_total>\d{9})"
    r"(?P<customer_num>\d{6})\s*$"
  )

  pickup_ftp_creds: dict[str, str] = loads(SETTINGS.sas_ftp_creds_file.read_text())

  checks_date_in_filename = True

  pickup_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices")
  pickup_archive_ftp_folder = PurePosixPath("/Fastrax Invoices/Archive")
  pre_processing_waiting_folder = PurePosixPath("/Waiting/SAS")
  pre_processing_archive_folder = PurePosixPath("/Waiting/SAS/Archive")
  post_processing_waiting_folder = PurePosixPath("/Processed/SAS")
  destination_ftp_folder = PurePosixPath("/SAS")

  local_pre_processing_folder = CWD / "SAS_files" / "pre_processing"
  local_post_processing_folder = CWD / "SAS_files" / "post_processing"

  identifier_prefix: str = "SAS"
  log_file_loc = SupplierProcessorBase.log_file_loc / supplier_name
  ctx_var_identifier = ContextVar("sas_log_identifier", default=None)
  ctx_var_log_loc = ContextVar("sas_log_loc", default=log_file_loc)

  def __init__(self, pbar: Progress = None) -> None:  # type: ignore
    if pbar is not None:  # pyright: ignore[reportUnnecessaryComparison]
      self.vendor_ftp.pbar = pbar
    super().__init__(pbar)

  @override
  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern[str]:
    dates = list(
      rrule(
        DAILY,
        dtstart=(start_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0, microsecond=0))
        - relativedelta(weeks=1 if current_week else 0),
        until=(end_date + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59, microsecond=999999))
        - relativedelta(weeks=0 if current_week else 1),
      )
    )

    years = {str(date.year) for date in dates}
    months = {f"{date.month:02d}" for date in dates}
    days = {f"{date.day:02d}" for date in dates}

    years_part = "|".join(years)
    months_part = "|".join(months)
    days_part = "|".join(days)

    pattern = (
      rf"^EF{customer_id}_"
      r"(?P<timestamp>"
      rf"(?P<year>{years_part})"
      rf"(?P<month>{months_part})"
      rf"(?P<day>{days_part})"
      r"(?P<hour>\d{2})"
      r"(?P<minute>\d{2})"
      r"(?P<second>\d{2})"
      r"(?P<microsecond>\d{6})"
      r")\.TXT$"
    )
    return compile(pattern)


if __debug__ and SETTINGS.use_testing_folders:
  for attr_name in [
    "pre_processing_waiting_folder",
    "pre_processing_archive_folder",
    "post_processing_waiting_folder",
    "destination_ftp_folder",
  ]:
    # prepend /Testing to each of the FTP folder paths for testing
    orig_attr: PurePosixPath = getattr(SASProcessor, attr_name)
    new_val = PurePosixPath("/Testing") / orig_attr.relative_to("/")
    setattr(SASProcessor, attr_name, new_val)


async def main():
  # Third party imports
  from rich import get_console

  # First party imports
  from aeth_ext.rich.progress import Progress
  from scheduled_invoice_processor.database.cache import DatabaseCache

  cache = DatabaseCache()
  await cache.refresh_cache()
  now = datetime.now(SETTINGS.tz)

  with Progress(console=get_console(), auto_refresh=False) as pbar:
    sas = SASProcessor(pbar)
    orders = []

    async for order in cache.schedule.walk_typed_rows():
      if order.supplier != SuppliersEnum.SAS:
        continue

      orders.append(order)

    for order in orders:
      await sas.register_pickup(
        storenum=order.store,
        customer_id=order.customer,
        pickup_date=now,
        dropoff_date=now,
        current_week=True,
      )

    await cache.submit_queued_writes_to_pool()

    await sas.pickup_files()

    await cache.submit_queued_writes_to_pool()

    for order in orders:
      await sas.register_dropoff(
        storenum=order.store,
        customer_id=order.customer,
        pickup_date=now,
        dropoff_date=now,
        current_week=True,
      )

    await cache.submit_queued_writes_to_pool()

    await sas.dropoff_files()

    await cache.submit_queued_writes_to_pool()


if __name__ == "__main__":
  # Standard library imports
  from asyncio import run

  run(main())
