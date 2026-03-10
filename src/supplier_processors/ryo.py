if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import gather, to_thread
from contextvars import ContextVar
from datetime import datetime
from json import loads
from logging import getLogger
from logging.handlers import QueueHandler
from pathlib import Path, PurePosixPath
from queue import Queue
from re import Pattern, compile
from typing import Optional

from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from environment_init_vars import CWD, SETTINGS
from logging_config import ContextAdapter, DynamicQueueListener, add_log_context
from paramiko import AutoAddPolicy, SFTPClient, SSHClient
from typing_custom import CustomerID, StoreNum
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

from supplier_processors import FileRegisterData, SupplierProcessorBase

logger = getLogger(__name__)
contextual_logs_queue = Queue(-1)
logger.addHandler(QueueHandler(contextual_logs_queue))  # type: ignore

contextual_log_listener = DynamicQueueListener(contextual_logs_queue, respect_handler_level=True)  # type: ignore


class RYOSFTPClient:
  policy = AutoAddPolicy()

  def __init__(self, creds: dict):
    self.creds = creds

  def __enter__(self) -> SFTPClient:
    self.ssh_client = SSHClient()
    self.ssh_client.set_missing_host_key_policy(self.policy)

    self.ssh_client.connect(
      hostname=self.creds["HOSTNAME"],
      port=self.creds.get("PORT", 2222),
      username=self.creds["USER"],
      password=self.creds["PWD"],
    )

    self.sftp_client = self.ssh_client.open_sftp()

    return self.sftp_client

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    self.sftp_client.close()
    self.ssh_client.close()


class RYOProcessor(SupplierProcessorBase):
  vendor_ftp = RYOSFTPClient
  pickup_ftp_creds: dict = loads(SETTINGS.ryo_ftp_creds_file.read_text())

  pickup_ftp_folder: PurePosixPath = PurePosixPath("/RYOtoSFT")
  pickup_archive_ftp_folder: PurePosixPath = PurePosixPath("/RYOtoSFT/Archive")
  waiting_folder = PurePosixPath("/Waiting/RYO")
  destination_ftp_folder = PurePosixPath("/RYO")

  queue_backup_prefix: str = "ryo"

  supplier_name: SuppliersEnum = SuppliersEnum.RYO

  _identifier_prefix: str = "RYO"
  _log_file_loc: Path = CWD / "logs" / "ryo"
  _ctx_var = ContextVar("ryo_log_context", default=None)

  @add_log_context(identifier_prefix=LogActionEnum.REGISTERED_PICKUP, log_subfolder=LogActionEnum.REGISTERED_PICKUP)
  async def _register_pickup(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger if adapted_logger is not None else logger
    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )

    if picked_up:
      local_logger.warning(
        f"{self.__class__.__name__}: Attempted to register pickup for already grabbed invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      local_logger.warning(
        f"{self.__class__.__name__}: Attempted to register pickup for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return

    pattern = self.assemble_filename_pattern(customer_id, pickup_date, dropoff_date, current_week)

    register_data = FileRegisterData(
      storenum=storenum,
      customer_id=customer_id,
      pickup_date=pickup_date,
      dropoff_date=dropoff_date,
      file_pattern=pattern,
      current_week=current_week,
      _waiting_folder=self.waiting_folder,
    )

    queue_key = self.assemble_queue_key(storenum, customer_id, pickup_date)

    if items_to_log is not None:
      items_to_log[queue_key] = (StatusCode.UNKNOWN, register_data)

    # Protect queue modification with lock for consistency
    async with self.lock:
      self.file_pickup_queue[queue_key] = register_data
    local_logger.info(f"{self.__class__.__name__}: Added {storenum} to pickup queue")

    if items_to_log is not None:
      items_to_log[queue_key] = (StatusCode.SUCCESS, register_data)

  @add_log_context(identifier_prefix=LogActionEnum.REGISTERED_DROPOFF, log_subfolder=LogActionEnum.REGISTERED_DROPOFF)
  async def _register_dropoff(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool,
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger if adapted_logger is not None else logger
    key = f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )
    if not picked_up:
      local_logger.warning(
        f"{self.__class__.__name__}: Attempted to register dropoff for not-yet picked up invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      local_logger.warning(
        f"{self.__class__.__name__}: Attempted to register dropoff for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return

    # Protect queue operations with lock to prevent race conditions
    async with self.lock:
      # first check if key is already in dropoff queue
      if key not in self.file_dropoff_queue:
        try:
          matched_item = self.file_waiting_queue.pop(key)
        except KeyError:
          local_logger.error(
            f"{self.__class__.__name__}: No waiting file found for: {self.supplier_name}, {storenum}, {customer_id}, {pickup_date.isoformat()}\n"
            f"Invoice may not have been picked up or is missing!"
          )
          return

        if items_to_log is not None:
          items_to_log[key] = (StatusCode.SUCCESS, matched_item)

        self.file_dropoff_queue[key] = matched_item
        local_logger.info(f"{self.__class__.__name__}: Registered dropoff for: {matched_item.storenum}")

      else:
        local_logger.warning(f"{self.__class__.__name__}: File already registered for dropoff: {key}")

  def assemble_queue_key(self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime) -> str:
    return f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

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

  def _archive_file(self, remote_file: str, adapted_logger: Optional[ContextAdapter] = None) -> None:
    local_logger = adapted_logger if adapted_logger is not None else logger
    archive_loc = (self.pickup_archive_ftp_folder / remote_file).as_posix()
    with self.vendor_ftp(self.pickup_ftp_creds) as sftp_client:
      try:
        sftp_client.stat(archive_loc)
        local_logger.info(
          f"{self.__class__.__name__}: Archive file already exists at [yellow]{archive_loc}[/]\nDeleting new file instead of moving."
        )

      except IOError:
        sftp_client.rename((self.pickup_ftp_folder / remote_file).as_posix(), archive_loc)
        local_logger.info(
          f"{self.__class__.__name__}: Archived [yellow]{remote_file}[/] to {self.pickup_archive_ftp_folder.as_posix()}",
          extra={"markup": True},
        )
        return

      sftp_client.remove((self.pickup_ftp_folder / remote_file).as_posix())
    pass

  @add_log_context(identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  async def _pickup_files(
    self,
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger if adapted_logger is not None else logger
    if not self.file_pickup_queue:
      return
    async with self.lock:
      with self.vendor_ftp(self.pickup_ftp_creds) as sftp_client:
        remote_files = [file_attr.filename for file_attr in sftp_client.listdir_attr(self.pickup_ftp_folder.as_posix())]

      items_to_dl: dict[str, FileRegisterData] = {}
      for key, file_meta in self.file_pickup_queue.items():
        matched_files = []

        for remote_file in remote_files:
          if match := file_meta.file_pattern.match(remote_file):
            matched_files.append(match)

        if matched_files:
          file_meta.file_name = [m.string for m in matched_files]
          items_to_dl[key] = file_meta
          if items_to_log is not None:
            items_to_log[key] = (StatusCode.UNKNOWN, file_meta)
          local_logger.info(f"{self.__class__.__name__}: Matched {len(matched_files)} files for: {file_meta.storenum}")
        else:
          local_logger.info(
            f"{self.__class__.__name__}: No files matched for: {key} with pattern {file_meta.file_pattern.pattern}"
          )

      with self.pbar.add_task(
        "Transferring Files", total=sum(len(v.file_name) for v in items_to_dl.values())
      ) as move_files_task:
        dl_futures = []
        for key, file_meta in items_to_dl.items():
          dl_futures.extend(
            to_thread(
              self._transfer_file_vend_to_main,
              send_path=(self.pickup_ftp_folder / filename),
              recv_path=local_path,
              move_files_task=move_files_task,
              file_meta=file_meta,
              idx=idx,
              key=key,
              adapted_logger=adapted_logger if adapted_logger is not None else None,
              items_to_log=items_to_log,
            )
            for idx, (filename, local_path) in enumerate(zip(file_meta.file_name, file_meta.file_loc))
          )
        await gather(*dl_futures)

      archive_futures = []
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if all(file_meta.pickup_success.values()):
          archive_futures.extend(
            to_thread(self._archive_file, filename, adapted_logger if adapted_logger is not None else None)
            for filename in file_meta.file_name
          )
          items_to_advance[key] = file_meta
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          local_logger.info(
            f"{self.__class__.__name__}: Checking off {self.supplier_name}_{file_meta.storenum} invoice_grabbed"
          )
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

      await gather(*archive_futures)

    for key, item in items_to_advance.items():
      self.file_waiting_queue[key] = item
      self.file_pickup_queue.pop(key)
      local_logger.info(f"{self.__class__.__name__}: Moved {item.storenum} to waiting queue")


# async def main():
#   with LiveCustom(refresh_per_second=10, console=RICH_CONSOLE) as live:
#     file = PurePosixPath("/Fastrax Invoices/Archive/EF45254_20250722040106566837.TXT")
#     with live.pbar.add_task("Test Transfer", total=1) as task:
#       RYOProcessor(live.pbar)._transfer_file_vend_to_main(
#         send_path=file,
#         recv_path=RYOProcessor.waiting_folder / file.name,
#         move_files_task=task,
#         file_meta=FileRegisterData(
#           storenum=123,
#           customer_id="45254",
#           pickup_date=datetime.now(),
#           dropoff_date=datetime.now(),
#           file_pattern=compile(r".*"),
#           current_week=True,
#           file_name=[file.name],
#           _waiting_folder=PurePosixPath("/Waiting/RYO"),
#         ),
#         idx=0,
#       )
#       pass


# if __name__ == "__main__":
#   run(main())
