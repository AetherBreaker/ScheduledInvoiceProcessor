# Standard library imports
from asyncio import as_completed, to_thread
from contextvars import ContextVar
from datetime import datetime
from hashlib import file_digest
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# Third party imports
from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from pydantic import SecretStr

# First party imports
from aeth_ext.ftp import create_ftp_adapter
from aeth_ext.ftp.credentials import SFTPCredentials
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.logging_config import add_log_context
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

# Local folder imports
from . import SupplierProcessorBase
from .file_register_data import FileRegisterData
from .log_action import log_actions

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Coroutine
  from logging import LoggerAdapter
  from pathlib import Path
  from re import Pattern
  from typing import Any

  # First party imports
  from scheduled_invoice_processor.typing_custom import CustomerID, SupplierQueueKey

  # Local folder imports
  from .log_action import LogActionHandlerType

logger = getLogger(__name__)


def load_credentials() -> SFTPCredentials:
  """RYO SFTP credentials (Bitvise), read from `ryo_ftp_creds.json`; the password is wrapped in a `SecretStr`."""
  raw = loads(SETTINGS.ryo_ftp_creds_file.read_text())
  return SFTPCredentials(
    host=raw["HOSTNAME"],
    username=raw["USER"],
    password=SecretStr(raw["PWD"]),
    port=int(raw.get("PORT", 22)),
    host_key_policy="auto_add",
  )


class RYOProcessor(SupplierProcessorBase):
  vendor_ftp = create_ftp_adapter(load_credentials(), container_cls="RYOProcessor")

  queue_backup_prefix: str = "ryo"

  invoice_num_pattern: Pattern[str] = compile(  # pyright: ignore[reportIncompatibleVariableOverride]
    r"^(?P<customer_num>[^\|]+)\|"
    r"(?P<invoice_num>\d+)\|"
    r"(?P<po_num>(\d+)|.*)(?P<invoice_type>[A-Za-z]*)\|"
    r"(?P<invoice_date>\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [AP]M)$"
  )

  header_format = "{customer_num}|{invoice_num}|{po_num}|{invoice_date}"
  file_name_format = "{customer_id}_{invoice_num}_{timestamp}.txt"

  supplier_name: SuppliersEnum = SuppliersEnum.RYO

  checks_date_in_filename: bool = True

  pickup_ftp_folder = PurePosixPath("/RYOtoSFT")
  pickup_archive_ftp_folder = PurePosixPath("/RYOtoSFT/Archive")
  pre_processing_waiting_folder = PurePosixPath("/Waiting/RYO")
  pre_processing_archive_folder = PurePosixPath("/Waiting/RYO/Archive")
  post_processing_waiting_folder = PurePosixPath("/Processed/RYO")
  destination_ftp_folder = PurePosixPath("/RYO")

  identifier_prefix = "RYO"
  log_file_loc = SupplierProcessorBase.log_file_loc / supplier_name
  ctx_var_identifier = ContextVar("ryo_log_identifier", default=None)
  ctx_var_log_loc = ContextVar("ryo_log_loc", default=log_file_loc)

  def __post_init__(self) -> None:
    self.local_pre_processing_folder = self.job_holding_folder / "RYO_files" / "pre_processing"
    self.local_post_processing_folder = self.job_holding_folder / "RYO_files" / "post_processing"
    self.local_pre_processing_folder.mkdir(exist_ok=True, parents=True)
    self.local_post_processing_folder.mkdir(exist_ok=True, parents=True)

  # def assemble_filename_pattern(
  #   self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  # ) -> Pattern:
  #   pattern = (
  #     rf"^{customer_id}_"
  #     r"(?P<invoice_num>[\d\-]+)"
  #     r"\.txt$"
  #   )
  #   return compile(pattern)

  @override
  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern[str]:
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
  async def _preprocess_files(  # noqa: C901
    self,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    if not self._file_preprocess_queue:
      return

    # check that the waiting ftp is online before continuing
    if not self.waiting_ftp.test_connection():
      local_logger.warning("%s: Waiting FTP server is not online. Cancelling preprocessing step.", self.__class__.__name__)
      return

    async with self._lock:
      items_to_advance = {**self._file_preprocess_queue}

      if not items_to_advance:
        return

      local_logger.info("%s: Beginning preprocessing for %s files", self.__class__.__name__, len(items_to_advance))

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

            local_logger.info("%s: %s: Successfully preprocessed files", self.__class__.__name__, key)

            if log_action_handler is not None:
              log_action_handler(key, StatusCode.SUCCESS, file_meta)
            self.pbar.update(files_preprocessing_task, advance=1, refresh=True)

          except Exception as e:
            matched_results = [k for k, v in futures.items() if result is v]  # pyright: ignore[reportUnnecessaryComparison]
            if not matched_results:
              local_logger.error("%s: Could not find matching key for result %s in futures", self.__class__.__name__, result)
              raise RuntimeError(f"Could not find matching key for result {result} in futures") from e

            key = matched_results[0]

            local_logger.exception("%s: %s: Error preprocessing files", self.__class__.__name__, key)
            errors.append((key, e))

            if log_action_handler is not None:
              log_action_handler(key, StatusCode.FAILURE, items_to_advance[key])

      if errors:
        local_logger.error("%s: Completed preprocessing with %s errors", self.__class__.__name__, len(errors))

  def _preprocess_off_thread(
    self,
    key: SupplierQueueKey,
    old_file_meta: FileRegisterData,
    adapted_logger: LoggerAdapter[Any] | None = None,
  ) -> tuple[SupplierQueueKey, FileRegisterData]:
    """Merge one entry's files and move it from the preprocess queue to the dropoff queue.

    Ordering is the whole point: **upload → commit → cleanup**. The merged file must exist on the holding FTP
    before the dropoff queue claims it does, and the originals must survive until the commit so a stop before it
    re-runs from intact inputs (the re-upload overwrites). A stop after the commit at worst leaves un-archived
    originals in the pre-processing folder, which nothing re-matches.
    """
    try:
      local_logger = adapted_logger or logger

      new_file_meta = self._create_new_merged_file(key, old_file_meta, adapted_logger)
      local_logger.info(
        "%s: %s: Created merged file at location [yellow]%s[/]",
        self.__class__.__name__,
        key,
        new_file_meta.local_copy_loc[0].without_cwd(),
        extra={"markup": True},
      )

      # TODO Upload the original invoice files to a shared store specific google drive

      # 1. Upload the merged file to the post-processing waiting folder.
      for new_file_loc in new_file_meta.local_copy_loc.values():
        send_path = self.post_processing_waiting_folder / new_file_loc.name
        with new_file_loc.open("rb") as f, self.waiting_ftp.start_session() as waiting_client:
          waiting_client.upload_file(
            send_path.as_posix(), callback=f.read, file_size=new_file_loc.stat().st_size, task_msg=f"Uploading {send_path.name}"
          )
        local_logger.info("%s: %s: Uploaded merged file to remote location %s", self.__class__.__name__, key, send_path)

      # 2. Commit: the dropoff queue now describes a file that really exists.
      with self._persist_lock:
        self._file_dropoff_queue[key] = new_file_meta
        old_file_meta = self._file_preprocess_queue.pop(key)
        self._persist_queues()
      local_logger.info("%s: %s: Updated queues", self.__class__.__name__, key)

      # 3. Cleanup: archive the originals on the holding FTP, delete local copies.
      for remote_file_loc in old_file_meta.remote_file_locs.values():
        self._middle_archive_file(
          source_folder=self.pre_processing_waiting_folder,
          remote_file=remote_file_loc.name,
          archive_folder=self.pre_processing_archive_folder,
          adapted_logger=adapted_logger,
        )

      for local_file_loc in (*old_file_meta.local_copy_loc.values(), *new_file_meta.local_copy_loc.values()):
        try:
          local_file_loc.unlink()
          local_logger.info("%s: %s: Deleted local file %s", self.__class__.__name__, key, local_file_loc.without_cwd())
        except Exception:
          local_logger.exception("%s: %s: Failed to delete local file %s", self.__class__.__name__, key, local_file_loc.without_cwd())

      return key, new_file_meta
    except Exception:
      logger.exception("%s: %s: Unexpected error in preprocessing off thread", self.__class__.__name__, key)
      raise

  def _create_new_merged_file(  # noqa: C901, PLR0915
    self, key: SupplierQueueKey, old_file_meta: FileRegisterData, adapted_logger: LoggerAdapter[Any] | None = None
  ) -> FileRegisterData:
    local_logger = adapted_logger or logger
    original_invoice_files: list[Path] = []

    with self.waiting_ftp.start_session() as waiting_client:
      for remote_file_loc, local_file_loc in zip(
        old_file_meta.remote_file_locs.values(), old_file_meta.local_copy_loc.values(), strict=False
      ):
        with local_file_loc.open("wb") as local_file:
          waiting_client.download_file(
            remote_file_loc.as_posix(), callback=local_file.write, task_msg=f"Downloading {remote_file_loc.name}"
          )
        original_invoice_files.append(local_file_loc)
        local_logger.info(
          "%s: %s: Downloaded original invoice file from\n[yellow]%s[/] to\n[yellow]%s[/]",
          self.__class__.__name__,
          key,
          remote_file_loc,
          local_file_loc.without_cwd(),
        )

    # grab the contents of all the files
    first_lines: list[dict[str, str | None]] = []
    body_lines: list[bytes] = []

    found_invoice_nums = set()
    found_timestamps = set()
    file_hashes = set()

    for file in original_invoice_files:
      # open the files in binary for speed, but decote the first line separately to check for the invoice type (A or B)
      with file.open("rb") as fb:
        digest = file_digest(fb, "sha256")
        if digest in file_hashes:
          local_logger.error("%s: %s: Duplicate file hash found for file %s: %s", self.__class__.__name__, key, file.name, digest)
          continue  # skip this file since it has a duplicate hash
        else:
          file_hashes.add(digest.hexdigest())

      with file.open("rb") as f:
        first_line = f.readline().decode().strip()
        filename_match = old_file_meta.file_pattern.match(file.name)
        assert filename_match is not None
        file_extracted_timestamp = filename_match.group("timestamp")

        file_timestamp = datetime.strptime(file_extracted_timestamp, "%Y%m%d%H%M%S%f")  # noqa: DTZ007

        match = self.invoice_num_pattern.match(first_line)
        if not match:
          local_logger.error(
            "%s: %s: First line of file %s did not match expected format:\n%s",
            self.__class__.__name__,
            key,
            file.name,
            first_line,
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
              "%s: %s: Duplicate invoice number found in file %s: %s",
              self.__class__.__name__,
              key,
              file.name,
              attrs["invoice_num"],
            )
            continue  # skip this file since it has a duplicate invoice number
          else:
            found_invoice_nums.add(attrs["invoice_num"])

        first_lines.append(attrs)
        found_timestamps.add(file_timestamp)

        body_lines.extend(f.readlines())

    invoice_nums = []
    header_invoiced_dates = []
    found_values: dict[str, Any] = {
      "customer_num": None,
      "po_num": None,
      "invoice_date": None,
    }

    for first_line_attrs in first_lines:
      invoice_nums.append(first_line_attrs["invoice_num"] or "unknown")
      if found_values["customer_num"] is None and first_line_attrs["customer_num"] not in [None, ""]:
        found_values["customer_num"] = first_line_attrs["customer_num"]
      if found_values["po_num"] is None and first_line_attrs["po_num"] not in [None, ""]:
        found_values["po_num"] = first_line_attrs["po_num"]
      if first_line_attrs["invoice_date"] is not None and first_line_attrs["invoice_date"] != "":
        # 05/20/2026 11:44:55 AM
        header_invoiced_dates.append(datetime.strptime(first_line_attrs["invoice_date"], "%m/%d/%Y %I:%M:%S %p"))  # noqa: DTZ007

    found_values["invoice_date"] = min(header_invoiced_dates).strftime("%m/%d/%Y %I:%M:%S %p") if header_invoiced_dates else "unknown"

    invoice_num_result = "-".join(invoice_nums)
    header_result = self.header_format.format(**found_values, invoice_num=invoice_num_result).encode()

    new_file_name = self.file_name_format.format(
      customer_id=found_values["customer_num"] or "unknown_customer",
      invoice_num=invoice_num_result,
      timestamp=max(found_timestamps).strftime("%Y%m%d%H%M%S%f"),
    )

    new_file_loc = self.local_post_processing_folder / new_file_name

    line_separator = b"\r\n" if any(b"\r\n" in line for line in body_lines) else b"\n"

    with new_file_loc.open("wb") as new_file:
      new_file.write(header_result + line_separator)
      new_file.writelines(body_lines)

    local_logger.info(
      "%s: %s: Created new merged file at location [yellow]%s[/] with header\n[blue]%s[/]",
      self.__class__.__name__,
      key,
      new_file_loc.without_cwd(),
      header_result.decode(),
      extra={"markup": True},
    )

    # Then we remake the file meta to reflect the new file and filename
    return FileRegisterData(
      storenum=old_file_meta.storenum,
      customer_id=old_file_meta.customer_id,
      pickup_date=old_file_meta.pickup_date,
      dropoff_date=old_file_meta.dropoff_date,
      file_pattern=old_file_meta.file_pattern,
      _current_week=old_file_meta._current_week,  # pyright: ignore[reportPrivateUsage]
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
  # Third party imports
  from rich import get_console

  # First party imports
  from aeth_ext.rich.progress import Progress
  from scheduled_invoice_processor.database import DatabaseCache

  cache = DatabaseCache()
  await cache.refresh_cache()
  now = datetime.now(SETTINGS.tz)

  with Progress(console=get_console(), auto_refresh=False) as pbar:
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
  # Standard library imports
  from asyncio import run

  run(main())
