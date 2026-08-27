# Standard library imports
from asyncio import as_completed, to_thread
from contextvars import ContextVar
from datetime import datetime
from hashlib import file_digest
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# First party imports
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


class SFTProcessor(SupplierProcessorBase):
  """SFT's own warehouse. The vendor side *is* the holding FTP -- `vendor_ftp` is the `waiting_ftp` pool -- so
  the base class's pickup does the whole job unmodified: `transfer_file` streams the invoice from the pickup
  folder to the waiting folder through that one pool, and the source is archived after the commit exactly as it
  is for a supplier on a remote server.

  Dates therefore come from the file's mtime, like every other `checks_date_in_filename = False` supplier. These
  files live on a server people work in, so an mtime can be rewritten by anyone who touches the file on the way
  past; the fix is for the warehouse export to put a timestamp in the filename, at which point this supplier
  turns on `checks_date_in_filename` and the ambiguity goes away. Until then the `[OUTSIDE_WEEK_PICKUP]`
  diagnostic reports anything the window accepts from outside the strict Sunday-Saturday week.

  The invoice format is RYO's, so preprocessing merges an entry's files the same way RYO does.
  """

  # Same server as the holding FTP: one adapter, no separate credentials.
  vendor_ftp = SupplierProcessorBase.waiting_ftp

  queue_backup_prefix: str = "sft"

  supplier_name: SuppliersEnum = SuppliersEnum.SFT

  # Header: SFT017|13842|49273|6/19/2025 9:46:46 AM  (month/day/hour are NOT zero-padded)
  invoice_num_pattern: Pattern[str] = compile(  # pyright: ignore[reportIncompatibleVariableOverride]
    r"^(?P<customer_num>[^|]+)\|"
    r"(?P<invoice_num>\d+)\|"
    r"(?P<po_num>[^|]*)\|"
    r"(?P<invoice_date>\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [AP]M)$"
  )
  header_date_format = "%m/%d/%Y %I:%M:%S %p"

  header_format = "{customer_num}|{invoice_num}|{po_num}|{invoice_date}"
  file_name_format = "{customer_id}_{invoice_num}.edi"

  # No date in the filename yet, so the base pickup judges candidates on their mtime. Flip this to True once the
  # warehouse export names files with a timestamp, and teach `assemble_filename_pattern` the date groups.
  checks_date_in_filename: bool = False

  pickup_ftp_folder = PurePosixPath("/SFT_Invoice_Pickup")
  pickup_archive_ftp_folder = PurePosixPath("/SFT_Invoice_Pickup/Archive")
  pre_processing_waiting_folder = PurePosixPath("/Waiting/SFT")
  pre_processing_archive_folder = PurePosixPath("/Waiting/SFT/Archive")
  post_processing_waiting_folder = PurePosixPath("/Processed/SFT")
  destination_ftp_folder = PurePosixPath("/SFT")

  identifier_prefix = "SFT"
  log_file_loc = SupplierProcessorBase.log_file_loc / supplier_name
  ctx_var_identifier = ContextVar("sft_log_identifier", default=None)
  ctx_var_log_loc = ContextVar("sft_log_loc", default=log_file_loc)

  def __post_init__(self) -> None:
    self.local_pre_processing_folder = self.job_holding_folder / "SFT_files" / "pre_processing"
    self.local_post_processing_folder = self.job_holding_folder / "SFT_files" / "post_processing"
    self.local_pre_processing_folder.mkdir(exist_ok=True, parents=True)
    self.local_post_processing_folder.mkdir(exist_ok=True, parents=True)

  @override
  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern[str]:
    # No date in the filename; `[\d\-]+` so a merged `SFT017_13842-13843.edi` still matches.
    return compile(rf"^{customer_id}_(?P<invoice_num>[\d\-]+)\.edi$")

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
        persisted = self._persist_queues()
      local_logger.info("%s: %s: Updated queues", self.__class__.__name__, key)

      # 3. Cleanup: archive the originals on the holding FTP, delete local copies. Same gate as `_pickup_files`:
      # if the backup could not be written, the on-disk ledger still says "preprocess", so the originals must
      # stay where a re-run from that ledger would look for them.
      if persisted:
        for remote_file_loc in old_file_meta.remote_file_locs.values():
          self._middle_archive_file(
            source_folder=self.pre_processing_waiting_folder,
            remote_file=remote_file_loc.name,
            archive_folder=self.pre_processing_archive_folder,
            adapted_logger=adapted_logger,
          )
      else:
        local_logger.warning(
          "%s: %s: queue backup could not be persisted; leaving the original files in %s un-archived",
          self.__class__.__name__,
          key,
          self.pre_processing_waiting_folder,
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
    file_hashes = set()

    for file in original_invoice_files:
      # open the files in binary for speed, but decote the first line separately to check for the invoice type (A or B)
      with file.open("rb") as fb:
        digest = file_digest(fb, "sha256").hexdigest()
        if digest in file_hashes:
          local_logger.error("%s: %s: Duplicate file hash found for file %s: %s", self.__class__.__name__, key, file.name, digest)
          continue  # skip this file since it has a duplicate hash
        else:
          file_hashes.add(digest)

      with file.open("rb") as f:
        first_line = f.readline().decode().strip()
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

        body_lines.extend(f.readlines())

    invoice_nums = []
    # (parsed date, original header string). The original is what gets re-emitted: the source format leaves
    # month/day/hour unpadded (`6/19/2025 9:46:46 AM`) and the downstream consumer of the merged file is unknown,
    # so re-serialising through `strftime` (which pads) is a change nobody asked for.
    header_invoiced_dates: list[tuple[datetime, str]] = []
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
      raw_date = first_line_attrs["invoice_date"]
      if raw_date is not None and raw_date != "":
        # 05/20/2026 11:44:55 AM  (or 5/20/2026 1:44:55 PM -- the source does not zero-pad)
        header_invoiced_dates.append((datetime.strptime(raw_date, self.header_date_format), raw_date))  # noqa: DTZ007

    # Earliest by parsed datetime, emitted verbatim as it appeared in that file's header.
    found_values["invoice_date"] = min(header_invoiced_dates)[1] if header_invoiced_dates else "unknown"

    invoice_num_result = "-".join(invoice_nums)
    header_result = self.header_format.format(**found_values, invoice_num=invoice_num_result).encode()

    new_file_name = self.file_name_format.format(
      customer_id=found_values["customer_num"] or "unknown_customer",
      invoice_num=invoice_num_result,
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
  # Every folder lives on the SFT server here (SAS/RYO only prefix the four holding-side folders because their
  # pickup folders are on the vendor's server).
  for attr_name in [
    "pickup_ftp_folder",
    "pickup_archive_ftp_folder",
    "pre_processing_waiting_folder",
    "pre_processing_archive_folder",
    "post_processing_waiting_folder",
    "destination_ftp_folder",
  ]:
    orig_attr: PurePosixPath = getattr(SFTProcessor, attr_name)
    new_val = PurePosixPath("/Testing") / orig_attr.relative_to("/")
    setattr(SFTProcessor, attr_name, new_val)


async def main():
  # Third party imports
  from aeth_ext.rich.progress import Progress
  from rich import get_console

  # First party imports
  from scheduled_invoice_processor.database import DatabaseCache

  cache = DatabaseCache()
  await cache.refresh_cache()
  now = datetime.now(SETTINGS.tz)

  with Progress(console=get_console(), auto_refresh=False) as pbar:
    sft = SFTProcessor(pbar)
    orders = [order async for order in cache.schedule.walk_typed_rows() if order.supplier == SuppliersEnum.SFT]

    for order in orders:
      await sft.register_pickup(storenum=order.store, customer_id=order.customer, pickup_date=now, dropoff_date=now, current_week=True)
    await cache.submit_queued_writes_to_pool()

    await sft.pickup_files()
    await cache.submit_queued_writes_to_pool()

    for order in orders:
      await sft.register_dropoff(
        storenum=order.store, customer_id=order.customer, pickup_date=now, dropoff_date=now, current_week=True
      )
    await cache.submit_queued_writes_to_pool()

    await sft.dropoff_files()
    await cache.submit_queued_writes_to_pool()


if __name__ == "__main__":
  # Standard library imports
  from asyncio import run

  run(main())
