from asyncio import gather, to_thread
from datetime import datetime
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import Pattern, compile

from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from environment_init_vars import SAS_FTP_CREDS_FILE
from paramiko import AutoAddPolicy, SFTPClient, SSHClient
from typing_custom import CustomerID, StoreNum
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import SuppliersEnum

from supplier_processors import FileRegisterData, SupplierProcessorBase

logger = getLogger(__name__)


class SASSFTPClient:
  policy = AutoAddPolicy()

  def __init__(self, creds: dict):
    self.creds = creds

  def __enter__(self) -> SFTPClient:
    self.ssh_client = SSHClient()
    self.ssh_client.set_missing_host_key_policy(self.policy)

    self.ssh_client.connect(
      hostname=self.creds["HOSTNAME"],
      port=self.creds.get("PORT", 22),
      username=self.creds["USER"],
      password=self.creds["PWD"],
    )

    self.sftp_client = self.ssh_client.open_sftp()

    return self.sftp_client

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    self.sftp_client.close()
    self.ssh_client.close()


class SASProcessor(SupplierProcessorBase):
  vendor_ftp = SASSFTPClient
  pickup_ftp_creds: dict = loads(SAS_FTP_CREDS_FILE.read_text())

  pickup_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices")
  # pickup_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices/Archive")
  pickup_archive_ftp_folder: PurePosixPath = PurePosixPath("/Fastrax Invoices/Archive")
  waiting_folder = PurePosixPath("/Waiting/SAS")
  destination_ftp_folder = PurePosixPath("/SAS")

  queue_backup_prefix: str = "sas"

  supplier_name: SuppliersEnum = SuppliersEnum.SAS

  async def register_pickup(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool = True
  ) -> None:
    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )

    if picked_up:
      logger.warning(
        f"{self.__class__.__name__}: Attempted to register pickup for already grabbed invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      logger.warning(
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

    # Protect queue modification with lock for consistency
    async with self.lock:
      self.file_pickup_queue[self.assemble_queue_key(storenum, customer_id, pickup_date)] = register_data
    logger.info(f"{self.__class__.__name__}: Added {storenum} to pickup queue")

  async def register_application(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool
  ) -> None:
    key = f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )
    if not picked_up:
      logger.warning(
        f"{self.__class__.__name__}: Attempted to register application for not-yet picked up invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      logger.warning(
        f"{self.__class__.__name__}: Attempted to register application for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return

    # Protect queue operations with lock to prevent race conditions
    async with self.lock:
      # first check if key is already in application queue
      if key not in self.file_application_queue:
        try:
          matched_item = self.file_waiting_queue.pop(key)
        except KeyError:
          logger.error(
            f"{self.__class__.__name__}: No waiting file found for: {self.supplier_name}, {storenum}, {customer_id}, {pickup_date.isoformat()}\n"
            f"Invoice may not have been picked up or is missing!"
          )
          return

        self.file_application_queue[key] = matched_item
        logger.info(f"{self.__class__.__name__}: Moved {matched_item.storenum} from waiting to application queue")
      else:
        logger.warning(f"{self.__class__.__name__}: File already registered for application: {key}")

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

  def _archive_file(self, remote_file: str) -> None:
    archive_loc = (self.pickup_archive_ftp_folder / remote_file).as_posix()
    with SASSFTPClient(self.pickup_ftp_creds) as sftp_client:
      try:
        sftp_client.stat(archive_loc)
        logger.warning(
          f"{self.__class__.__name__}: Archive file already exists at [yellow]{archive_loc}[/]\nDeleting new file instead of moving."
        )

      except IOError:
        sftp_client.rename((self.pickup_ftp_folder / remote_file).as_posix(), archive_loc)
        logger.info(
          f"{self.__class__.__name__}: Archived [yellow]{remote_file}[/] to {self.pickup_archive_ftp_folder.as_posix()}",
          extra={"markup": True},
        )
        return

      sftp_client.remove((self.pickup_ftp_folder / remote_file).as_posix())
    pass

  async def pickup_files(self) -> None:
    if not self.file_pickup_queue:
      return
    async with self.lock:
      with SASSFTPClient(self.pickup_ftp_creds) as sftp_client:
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
          logger.info(f"{self.__class__.__name__}: Matched {len(matched_files)} files for: {file_meta.storenum}")
        else:
          logger.warning(f"{self.__class__.__name__}: No files matched for: {key} with pattern {file_meta.file_pattern.pattern}")

      with self.pbar.add_task("Transferring Files", total=sum(len(v.file_name) for v in items_to_dl.values())) as move_files_task:
        dl_futures = []
        for file_meta in items_to_dl.values():
          dl_futures.extend(
            to_thread(
              self._transfer_file_vend_to_main,
              send_path=(self.pickup_ftp_folder / filename),
              recv_path=local_path,
              move_files_task=move_files_task,
              file_meta=file_meta,
              idx=idx,
            )
            for idx, (filename, local_path) in enumerate(zip(file_meta.file_name, file_meta.file_loc))
          )
        await gather(*dl_futures)

      archive_futures = []
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if all(file_meta.pickup_success.values()):
          archive_futures.extend(to_thread(self._archive_file, filename) for filename in file_meta.file_name)
          items_to_advance[key] = file_meta
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          logger.info(f"{self.__class__.__name__}: Checking off {self.supplier_name}_{file_meta.storenum} invoice_grabbed")
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

      await gather(*archive_futures)

    for key, item in items_to_advance.items():
      self.file_waiting_queue[key] = item
      self.file_pickup_queue.pop(key)
      logger.info(f"{self.__class__.__name__}: Moved {item.storenum} to waiting queue")


# async def main():
#   with LiveCustom(refresh_per_second=10, console=RICH_CONSOLE) as live:
#     file = PurePosixPath("/Fastrax Invoices/Archive/EF45254_20250722040106566837.TXT")
#     with live.pbar.add_task("Test Transfer", total=1) as task:
#       SASProcessor(live.pbar)._transfer_file_vend_to_main(
#         send_path=file,
#         recv_path=SASProcessor.waiting_folder / file.name,
#         move_files_task=task,
#         file_meta=FileRegisterData(
#           storenum=123,
#           customer_id="45254",
#           pickup_date=datetime.now(),
#           dropoff_date=datetime.now(),
#           file_pattern=compile(r".*"),
#           current_week=True,
#           file_name=[file.name],
#           _waiting_folder=PurePosixPath("/Waiting/SAS"),
#         ),
#         idx=0,
#       )
#       pass


# if __name__ == "__main__":
#   run(main())
