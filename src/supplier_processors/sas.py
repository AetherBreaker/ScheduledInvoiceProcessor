if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from contextvars import ContextVar
from datetime import datetime
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import Pattern, compile

from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from environment_init_vars import CWD, SETTINGS
from rich_custom import ProgressCustom
from typing_custom import CustomerID
from typing_custom.custom_path import CustomPath
from typing_custom.enums import SuppliersEnum

from supplier_processors import SupplierProcessorSFTPIntermediate
from supplier_processors.ftp_adapter import AdaptedSFTP, FTPAdapter, SASSFTPClient

logger = getLogger(__name__)


class SASProcessor(SupplierProcessorSFTPIntermediate):
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

  pickup_ftp_creds: dict = loads(SETTINGS.sas_ftp_creds_file.read_text())

  checks_date_in_filename = True

  pickup_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices")
  pickup_archive_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices/Archive")
  pre_processing_waiting_folder = PurePosixPath("/Waiting/SAS")
  pre_processing_archive_folder = PurePosixPath("/Waiting/SAS/Archive")
  post_processing_waiting_folder = PurePosixPath("/Processed/SAS")
  destination_ftp_folder = PurePosixPath("/SAS")

  local_pre_processing_folder = CWD / "SAS_files" / "pre_processing"
  local_post_processing_folder = CWD / "SAS_files" / "post_processing"

  identifier_prefix: str = "SAS"
  log_file_loc: CustomPath = CWD / "logs" / "sas"
  ctx_var_identifier = ContextVar("sas_log_identifier", default=None)
  ctx_var_log_loc = ContextVar("sas_log_loc", default=log_file_loc)

  def __init__(self, pbar: ProgressCustom = None) -> None:
    if pbar is not None:
      self.vendor_ftp.pbar = pbar
    super().__init__(pbar)

  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern:
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


if __debug__:
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
  from database.cache import DatabaseCache
  from logging_config import RICH_CONSOLE
  from rich_custom import LiveCustom

  cache = DatabaseCache()
  await cache.refresh_cache()
  now = datetime.now()

  with LiveCustom(refresh_per_second=10, console=RICH_CONSOLE) as live:
    sas = SASProcessor(live.pbar)
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
  from sys import platform

  if platform in ("win32", "cygwin", "cli"):
    from winloop import run
  else:
    # if we're on apple or linux do this instead
    from uvloop import run  # type: ignore
  run(main())
