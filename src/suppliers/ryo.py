from __future__ import annotations

from asyncio import as_completed, to_thread
from contextvars import ContextVar
from datetime import datetime
from hashlib import file_digest
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING

from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from environment_init_vars import CWD, SETTINGS
from logging_config import add_log_context
from typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

from suppliers import SupplierProcessorBase
from suppliers.file_register_data import FileRegisterData
from suppliers.ftp_adapter import FTPAdapter, RYOSFTPClient
from suppliers.log_action import log_actions

if TYPE_CHECKING:
  from collections.abc import Coroutine
  from logging import LoggerAdapter
  from pathlib import Path
  from re import Pattern
  from typing import Any

  from rich_custom import ProgressCustom
  from typing_custom import CustomerID, SupplierQueueKey

  from suppliers.ftp_adapter import AdaptedSFTP
  from suppliers.log_action import LogActionHandlerType

logger = getLogger(__name__)


class RYOProcessor(SupplierProcessorBase):
  vendor_ftp: FTPAdapter[AdaptedSFTP] = FTPAdapter(RYOSFTPClient, container_cls="RYOProcessor")

  queue_backup_prefix: str = "ryo"

  invoice_num_pattern: Pattern[str] = compile(  # type: ignore
    r"^(?P<customer_num>[^\|]+)\|"
    r"(?P<invoice_num>\d+)\|"
    r"(?P<po_num>(\d+)|.*)(?P<invoice_type>[A-Za-z]*)\|"
    r"(?P<invoice_date>\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AP]M)$"
  )

  header_format = "{customer_num}|{invoice_num}|{po_num}|{invoice_date}"
  file_name_format = "{customer_id}_{invoice_num}.txt"

  supplier_name: SuppliersEnum = SuppliersEnum.RYO

  pickup_ftp_creds: dict = loads(SETTINGS.ryo_ftp_creds_file.read_text())

  checks_date_in_filename: bool = True

  pickup_ftp_folder = PurePosixPath("/RYOtoSFT")
  pickup_archive_ftp_folder = PurePosixPath("/RYOtoSFT/Archive")
  pre_processing_waiting_folder = PurePosixPath("/Waiting/RYO")
  pre_processing_archive_folder = PurePosixPath("/Waiting/RYO/Archive")
  post_processing_waiting_folder = PurePosixPath("/Processed/RYO")
  destination_ftp_folder = PurePosixPath("/RYO")

  local_pre_processing_folder = CWD / "RYO_files" / "pre_processing"
  local_post_processing_folder = CWD / "RYO_files" / "post_processing"

  identifier_prefix = "RYO"
  log_file_loc = SupplierProcessorBase.log_file_loc / supplier_name
  ctx_var_identifier = ContextVar("ryo_log_identifier", default=None)
  ctx_var_log_loc = ContextVar("ryo_log_loc", default=log_file_loc)

  def __init__(self, pbar: ProgressCustom = None) -> None:
    if pbar is not None:
      self.vendor_ftp.pbar = pbar
    super().__init__(pbar)

  # def assemble_filename_pattern(
  #   self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  # ) -> Pattern:
  #   pattern = (
  #     rf"^{customer_id}_"
  #     r"(?P<invoice_num>[\d\-]+)"
  #     r"\.txt$"
  #   )
  #   return compile(pattern)

  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern:
    # sourcery skip: swap-if-expression
    rng_start = (start_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0, microsecond=0)) - relativedelta(
      weeks=1 if not current_week else 0
    )
    rng_end = (end_date + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59, microsecond=999999)) - relativedelta(
      weeks=1 if not current_week else 0
    )

    dates = list(rrule(DAILY, dtstart=rng_start, until=rng_end))

    years = {str(date.year) for date in dates}
    months = {f"{date.month:02d}" for date in dates}
    days = {f"{date.day:02d}" for date in dates}

    years_part = "|".join(years)
    months_part = "|".join(months)
    days_part = "|".join(days)

    pattern = (
      rf"^{customer_id}_"
      r"(?P<invoice_num>[\d\-]+)_"
      r"(?P<timestamp>"
      rf"(?P<year>{years_part})"
      rf"(?P<month>{months_part})"
      rf"(?P<day>{days_part})"
      r"(?P<hour>\d{2})"
      r"(?P<minute>\d{2})"
      r"(?P<second>\d{2})"
      r"(?P<microsecond>\d{6})"
      r")\.txt$"
    )
    return compile(pattern)

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED, log_subfolder=LogActionEnum.FILE_PREPROCESSED)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED)
  async def _preprocess_files(
    self,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    if not self._file_preprocess_queue:
      return

    # check that the waiting ftp is online before continuing
    if not self.waiting_ftp.test_connection():
      local_logger.warning(f"{self.__class__.__name__}: Waiting FTP server is not online. Cancelling preprocessing step.")
      return

    async with self._lock:
      items_to_advance = {**self._file_preprocess_queue}

      if not items_to_advance:
        return

      local_logger.info(f"{self.__class__.__name__}: Beginning preprocessing for {len(items_to_advance)} files")

      errors = []

      with self.pbar.add_task(
        f"{self.__class__.__name__}: Preprocessing files", total=len(items_to_advance)
      ) as files_preprocessing_task:
        futures: dict[SupplierQueueKey, Coroutine[None, None, tuple[SupplierQueueKey, FileRegisterData]]] = {}
        for key, file_meta in tuple(self._file_preprocess_queue.items()):
          # for idx, (key, file_meta) in enumerate(tuple(self._file_preprocess_queue.items())):
          # if idx > 0:
          #   continue
          future = to_thread(self._preprocess_off_thread, key=key, old_file_meta=file_meta, adapted_logger=adapted_logger)
          futures[key] = future

          if log_action_handler is not None:
            log_action_handler(key, StatusCode.UNKNOWN, file_meta)

        async for result in as_completed(futures.values()):
          try:
            key, file_meta = await result

            local_logger.info(f"{self.__class__.__name__}: {key}: Successfully preprocessed files")

            if log_action_handler is not None:
              log_action_handler(key, StatusCode.SUCCESS, file_meta)
            self.pbar.update(files_preprocessing_task, advance=1)

          except Exception as e:
            matched_results = [k for k, v in futures.items() if result is v]
            if not matched_results:
              local_logger.error(f"{self.__class__.__name__}: Could not find matching key for result {result} in futures")
              raise RuntimeError(f"Could not find matching key for result {result} in futures") from e

            key = matched_results[0]

            local_logger.error(f"{self.__class__.__name__}: {key}: Error preprocessing files {e}")
            errors.append((key, e))

            if log_action_handler is not None:
              log_action_handler(key, StatusCode.FAILURE, items_to_advance[key])

      if errors:
        local_logger.error(f"{self.__class__.__name__}: Completed preprocessing with {len(errors)} errors")

  def _preprocess_off_thread(
    self,
    key: SupplierQueueKey,
    old_file_meta: FileRegisterData,
    adapted_logger: LoggerAdapter | None = None,
  ) -> tuple[SupplierQueueKey, FileRegisterData]:
    try:
      local_logger = adapted_logger or logger

      # Create the merged filed
      new_file_meta = self._create_new_merged_file(key, old_file_meta, adapted_logger)
      local_logger.info(
        f"{self.__class__.__name__}: {key}: Created merged file at location [yellow]{new_file_meta.local_copy_loc[0].without_cwd}[/]",
        extra={"markup": True},
      )

      # TODO Upload the original invoice files to a shared store specific google drive
      ...

      # Update the queues with the new file meta
      self._file_dropoff_queue[key] = new_file_meta
      old_file_meta = self._file_preprocess_queue.pop(key)
      local_logger.info(f"{self.__class__.__name__}: {key}: Updated queues")

      # Then we clean up the old invoice files left on the remote waiting folder
      for remote_file_loc in old_file_meta.remote_file_locs.values():
        self._middle_archive_file(
          source_folder=self.pre_processing_waiting_folder,
          remote_file=remote_file_loc.name,
          archive_folder=self.pre_processing_archive_folder,
          adapted_logger=adapted_logger,
        )

      for local_file_loc in old_file_meta.local_copy_loc.values():
        try:
          local_file_loc.unlink()
          local_logger.info(f"{self.__class__.__name__}: {key}: Deleted local file {local_file_loc.without_cwd}")
        except Exception as e:
          local_logger.error(f"{self.__class__.__name__}: {key}: Failed to delete local file {local_file_loc.without_cwd}", exc_info=e)

      # Uploaded the new file to the remote waiting folder, replacing the old invoice files.
      for new_file_loc in new_file_meta.local_copy_loc.values():
        send_path = self.post_processing_waiting_folder / new_file_loc.name
        with new_file_loc.open("rb") as f:
          with self.waiting_ftp.start_session() as waiting_client:
            waiting_client.upload_file(
              send_path.as_posix(), callback=f.read, file_size=new_file_loc.stat().st_size, task_msg=f"Uploading {send_path.name}"
            )
        local_logger.info(f"{self.__class__.__name__}: {key}: Uploaded merged file to remote location {send_path}")

        try:
          new_file_loc.unlink()
          local_logger.info(f"{self.__class__.__name__}: {key}: Deleted local merged file {new_file_loc.without_cwd}")
        except Exception as e:
          local_logger.error(f"{self.__class__.__name__}: {key}: Failed to delete local merged file {new_file_loc.without_cwd}: {e}")

      # return the new file meta and queue key to be updated in the logging list
      return key, new_file_meta
    except Exception as e:
      logger.error(f"{self.__class__.__name__}: {key}: Unexpected error in preprocessing off thread: {e}")
      raise e

  def _create_new_merged_file(
    self, key: SupplierQueueKey, old_file_meta: FileRegisterData, adapted_logger: LoggerAdapter | None = None
  ) -> FileRegisterData:
    local_logger = adapted_logger or logger
    original_invoice_files: list[Path] = []

    with self.waiting_ftp.start_session() as waiting_client:
      for remote_file_loc, local_file_loc in zip(old_file_meta.remote_file_locs.values(), old_file_meta.local_copy_loc.values()):
        with local_file_loc.open("wb") as local_file:
          waiting_client.download_file(
            remote_file_loc.as_posix(), callback=local_file.write, task_msg=f"Downloading {remote_file_loc.name}"
          )
        original_invoice_files.append(local_file_loc)
        local_logger.info(
          f"{self.__class__.__name__}: {key}: Downloaded original invoice file from\n[yellow]{remote_file_loc}[/] to\n[yellow]{local_file_loc.without_cwd}[/]",
          extra={"markup": True},
        )

    # grab the contents of all the files
    first_lines: list[dict[str, str | None]] = []
    body_lines: list[bytes] = []

    found_invoice_nums = set()
    file_hashes = set()

    for file in original_invoice_files:
      # open the files in binary for speed, but decote the first line separately to check for the invoice type (A or B)
      with file.open("rb") as fb:
        digest = file_digest(fb, "sha256")
        if digest in file_hashes:
          local_logger.error(f"{self.__class__.__name__}: {key}: Duplicate file hash found for file {file.name}: {digest}")
          continue  # skip this file since it has a duplicate hash
        else:
          file_hashes.add(digest.hexdigest())

      with file.open("rb") as f:
        first_line = f.readline().decode().strip()
        match = self.invoice_num_pattern.match(first_line)
        if not match:
          local_logger.error(
            f"{self.__class__.__name__}: {key}: First line of file {file.name} did not match expected format:\n{first_line}"
          )
        attrs = (
          match.groupdict()
          if match
          else {
            "customer_num": None,
            "invoice_num": None,
            "po_num": None,
            "invoice_type": None,
            "invoice_date": None,
          }
        )
        if attrs["invoice_num"] not in [None, ""]:
          if attrs["invoice_num"] in found_invoice_nums:
            local_logger.error(
              f"{self.__class__.__name__}: {key}: Duplicate invoice number found in file {file.name}: {attrs['invoice_num']}"
            )
            continue  # skip this file since it has a duplicate invoice number
          else:
            found_invoice_nums.add(attrs["invoice_num"])

        first_lines.append(attrs)

        body_lines.extend(f.readlines())

    invoice_nums = []
    found_values: dict[str, Any] = {
      "customer_num": None,
      "po_num": None,
      "invoice_date": None,
    }

    for first_line_attrs, body in zip(first_lines, body_lines):
      invoice_nums.append(first_line_attrs["invoice_num"] or "unknown")
      if found_values["customer_num"] is None and first_line_attrs["customer_num"] not in [None, ""]:
        found_values["customer_num"] = first_line_attrs["customer_num"]
      if found_values["po_num"] is None and first_line_attrs["po_num"] not in [None, ""]:
        found_values["po_num"] = first_line_attrs["po_num"]
      if found_values["invoice_date"] is None and first_line_attrs["invoice_date"] not in [None, ""]:
        found_values["invoice_date"] = first_line_attrs["invoice_date"]

    invoice_num_result = "-".join(invoice_nums)
    header_result = self.header_format.format(**found_values, invoice_num=invoice_num_result).encode()

    new_file_name = self.file_name_format.format(
      customer_id=found_values["customer_num"] or "unknown_customer", invoice_num=invoice_num_result
    )

    new_file_loc = self.local_post_processing_folder / new_file_name

    line_separator = b"\r\n" if any(b"\r\n" in line for line in body_lines) else b"\n"

    with new_file_loc.open("wb") as new_file:
      new_file.write(header_result + line_separator)
      new_file.writelines(body_lines)

    local_logger.info(
      f"{self.__class__.__name__}: {key}: Created new merged file at location [yellow]{new_file_loc.without_cwd}[/] with header\n[blue]{header_result.decode()}[/]",
      extra={"markup": True},
    )

    # Then we remake the file meta to reflect the new file and filename
    return FileRegisterData(
      storenum=old_file_meta.storenum,
      customer_id=old_file_meta.customer_id,
      pickup_date=old_file_meta.pickup_date,
      dropoff_date=old_file_meta.dropoff_date,
      file_pattern=old_file_meta.file_pattern,
      _current_week=old_file_meta._current_week,
      _waiting_folder=self.post_processing_waiting_folder,
      _local_copy_folder=self.local_post_processing_folder,
      file_names={0: new_file_name},
      invoice_nums={0: invoice_num_result},
      pickup_success={0: True},
    )


if __debug__ and SETTINGS.use_testing_folders:
  for attr_name in [
    "pre_processing_waiting_folder",
    "pre_processing_archive_folder",
    "post_processing_waiting_folder",
    "destination_ftp_folder",
  ]:
    # prepend /Testing to each of the FTP folder paths for testing
    orig_attr: PurePosixPath = getattr(RYOProcessor, attr_name)
    new_val = PurePosixPath("/Testing") / orig_attr.relative_to("/")
    setattr(RYOProcessor, attr_name, new_val)


async def main():
  from database.cache import DatabaseCache
  from logging_config import RICH_CONSOLE
  from rich_custom import ProgressCustom

  cache = DatabaseCache()
  await cache.refresh_cache()
  now = datetime.now()

  with ProgressCustom(refresh_per_second=10, console=RICH_CONSOLE) as pbar:
    ryo = RYOProcessor(pbar)

    # inp = FileRegisterData(
    #   storenum=22,
    #   customer_id="9893681235",
    #   pickup_date=now,
    #   dropoff_date=now,
    #   file_pattern=ryo.assemble_filename_pattern("9893681235", now, now, True),
    #   _current_week=True,
    #   _waiting_folder=ryo.pre_processing_waiting_folder,
    #   _local_copy_folder=ryo.local_pre_processing_folder,
    #   file_names={0: "9893681235_35835.txt", 1: "9893681235_35836.txt"},
    #   invoice_nums={0: "35835", 1: "35836"},
    #   pickup_success={0: True, 1: True},
    # )

    # outp = ryo._create_new_merged_file(inp)

    orders = []

    async for order in cache.schedule.walk_typed_rows():
      if order.supplier != SuppliersEnum.RYO:
        continue
      # if order.store != 32:
      #   continue

      orders.append(order)

    for order in orders:
      await ryo.register_pickup(
        storenum=order.store,
        customer_id=order.customer,
        pickup_date=now,
        dropoff_date=now,
        current_week=True,
      )

    await cache.submit_queued_writes_to_pool()

    await ryo.pickup_files()

    await cache.submit_queued_writes_to_pool()

    for order in orders:
      await ryo.register_dropoff(
        storenum=order.store,
        customer_id=order.customer,
        pickup_date=now,
        dropoff_date=now,
        current_week=True,
      )

    await cache.submit_queued_writes_to_pool()

    await ryo.dropoff_files()

    await cache.submit_queued_writes_to_pool()


if __name__ == "__main__":
  from asyncio import run

  run(main())
