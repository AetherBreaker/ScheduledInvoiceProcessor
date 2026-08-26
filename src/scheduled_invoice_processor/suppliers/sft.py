# Standard library imports
from asyncio import as_completed, gather, to_thread
from contextvars import ContextVar
from datetime import datetime
from ftplib import all_errors
from hashlib import file_digest
from io import BytesIO
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from time import sleep
from typing import TYPE_CHECKING, override

# Third party imports
from dateutil.relativedelta import SA, SU, relativedelta

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.logging_config import add_log_context
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

# Local folder imports
from . import SupplierProcessorBase
from .file_register_data import FileRegisterData
from .log_action import log_actions

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Coroutine
  from logging import Logger, LoggerAdapter
  from pathlib import Path
  from re import Pattern
  from typing import Any

  # First party imports
  from aeth_ext.ftp.session import AdapterBase
  from aeth_ext.rich.progress import TaskID
  from scheduled_invoice_processor.typing_custom import CustomerID, SupplierQueueKey

  # Local folder imports
  from .log_action import LogActionHandlerType

logger = getLogger(__name__)


class SFTProcessor(SupplierProcessorBase):
  """SFT's own warehouse. The vendor side *is* the holding FTP, so pickup is a header-checked rename.

  Date windows are decided from the header line's date, never the filename (it has none) and never mtime (a
  human touching the file poisons it).
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

  # The override of `_pickup_files` decides the window from the header; this flag only matters to the base
  # implementation, which SFT does not use for pickup.
  checks_date_in_filename: bool = False

  # ===========================================================================================================
  # !!! PLACEHOLDER FTP PATHS — MUST BE REPLACED BEFORE THIS SUPPLIER IS ENABLED IN PRODUCTION !!!
  # The real pickup/dropoff locations on the SFT FTP server have not been decided yet. Every path below is a
  # stand-in. Search for "TODO_SFT" to find them all. Do NOT ship with these values.
  # ===========================================================================================================
  pickup_ftp_folder = PurePosixPath("/TODO_SFT/Pickup")
  pre_processing_waiting_folder = PurePosixPath("/TODO_SFT/Waiting")
  pre_processing_archive_folder = PurePosixPath("/TODO_SFT/Waiting/Archive")
  post_processing_waiting_folder = PurePosixPath("/TODO_SFT/Processed")
  destination_ftp_folder = PurePosixPath("/TODO_SFT/Destination")
  # ===========================================================================================================

  # The pickup rename *is* the removal from the pickup folder, so there is nothing left to archive vendor-side.
  # `None` is the base class's documented "this supplier has no vendor archive" value and makes the base pickup's
  # archive wave a no-op.
  pickup_archive_ftp_folder = None

  _placeholder_marker = "TODO_SFT"
  """Substring that marks an FTP path as a stand-in; `check_connections` refuses to register while any remains."""

  identifier_prefix = "SFT"
  log_file_loc = SupplierProcessorBase.log_file_loc / supplier_name
  ctx_var_identifier = ContextVar("sft_log_identifier", default=None)
  ctx_var_log_loc = ContextVar("sft_log_loc", default=log_file_loc)

  def __post_init__(self) -> None:
    self.local_pre_processing_folder = self.job_holding_folder / "SFT_files" / "pre_processing"
    self.local_post_processing_folder = self.job_holding_folder / "SFT_files" / "post_processing"
    self.local_pre_processing_folder.mkdir(exist_ok=True, parents=True)
    self.local_post_processing_folder.mkdir(exist_ok=True, parents=True)

  @classmethod
  @override
  def check_connections(cls) -> bool:
    """Refuse registration while the FTP paths are still placeholders.

    `startup.py` registers every supplier whose `check_connections()` is true, and SFT's "vendor" side is the
    always-online holding FTP -- so without this gate a production run would happily pick up from `/TODO_SFT/...`.
    Only enforced outside testing-folder mode, where the placeholders are the intended sandbox paths.
    """
    if not SETTINGS.use_testing_folders:
      placeholders = [
        folder.as_posix()
        for folder in (
          cls.pickup_ftp_folder,
          cls.pickup_archive_ftp_folder,
          cls.pre_processing_waiting_folder,
          cls.pre_processing_archive_folder,
          cls.post_processing_waiting_folder,
          cls.destination_ftp_folder,
        )
        if folder is not None and cls._placeholder_marker in folder.as_posix()
      ]
      if placeholders:
        logger.critical(
          "%s: SFT folder paths are still placeholders; refusing to register SFTProcessor. Offending paths: %s",
          cls.__name__,
          ", ".join(placeholders),
        )
        return False
    return super().check_connections()

  @override
  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern[str]:
    # No date in the filename; `[\d\-]+` so a merged `SFT017_13842-13843.edi` still matches.
    return compile(rf"^{customer_id}_(?P<invoice_num>[\d\-]+)\.edi$")

  def parse_header_date(self, first_line: str) -> datetime | None:
    """Header date localised to `SETTINGS.tz` (the header carries no offset), or None if the line is not a header."""
    match = self.invoice_num_pattern.match(first_line.strip())
    if match is None:
      return None
    try:
      return datetime.strptime(match.group("invoice_date"), self.header_date_format).replace(tzinfo=SETTINGS.tz)
    except ValueError:
      return None

  def header_date_in_window(self, file_meta: FileRegisterData, header_date: datetime) -> bool:
    """Same Sun-Sat window the base class applies to mtimes, applied to the header date instead."""
    current_week = file_meta.current_week
    start_date = (
      file_meta.pickup_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0, microsecond=0)
    ) - relativedelta(weeks=1 if current_week else 0)
    end_date = (
      file_meta.dropoff_date + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59, microsecond=999999)
    ) - relativedelta(weeks=0 if current_week else 1)
    return start_date <= header_date < end_date

  def _rename_same_server(
    self,
    client: AdapterBase,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    local_logger: LoggerAdapter[Any] | Logger,
  ) -> bool:
    """Rename `send_path` to `recv_path` on `client`, verifying the destination afterwards. A rename that fails
    because it already happened on an earlier, interrupted run is reported as success (see `_already_moved`)."""
    if send_path == recv_path:
      # The candidate was found in the waiting folder: an earlier run renamed it there but died before the queue
      # entry advanced. It is already where it belongs, so there is nothing to move. Handled here rather than by
      # `_already_moved`, which needs a *distinct* source and destination to prove a move happened.
      local_logger.info(
        "%s: [yellow]%s[/] is already in the waiting folder; no rename needed",
        self.__class__.__name__,
        recv_path,
        extra={"markup": True},
      )
      return True
    try:
      client.rename(send_path.as_posix(), recv_path.as_posix())
    except (*all_errors, OSError):
      if self._already_moved(client, send_path, recv_path, local_logger):
        local_logger.info(
          "%s: [yellow]%s[/] was already moved to [yellow]%s[/] by an earlier run; treating as success",
          self.__class__.__name__,
          send_path,
          recv_path,
          extra={"markup": True},
        )
        return True
      raise
    else:
      try:
        client.get_size(recv_path.as_posix())
        local_logger.info(
          "%s: Moved [yellow]%s[/] to [yellow]%s[/]",
          self.__class__.__name__,
          send_path,
          recv_path,
          extra={"markup": True},
        )
      except (*all_errors, OSError) as e:
        local_logger.warning("%s: Failed to verify move of %s", self.__class__.__name__, send_path.name, exc_info=e)
        return False
      return True

  def _transfer_file_same_server(  # noqa: PLR0917
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: TaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    file_bytes: bytes,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ) -> bool:
    """Pickup for a vendor that shares the holding FTP: rename in place, then take the invoice number from the
    bytes already downloaded for the header check. Idempotent like `_transfer_file_main_to_main`: a rename that
    already happened on an earlier, interrupted run is reported as success. Never raises; the outcome lands in
    `file_meta.pickup_success[idx]` and the log-action handler."""
    local_logger = adapted_logger or logger
    success = False
    if self.errored:
      local_logger.warning("%s: Disabled due to error state. Skipping same-server transfer", self.__class__.__name__)
      file_meta.pickup_success[idx] = False
      self._advance_progress(move_files_task)
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)
      return False
    try:
      with self.vendor_ftp.start_session() as client:
        success = self._rename_same_server(client, send_path, recv_path, local_logger)
      file_meta.pickup_success[idx] = success
      if success:
        self.extract_invoice_num(BytesIO(file_bytes), file_meta, idx, adapted_logger=adapted_logger)
      self._advance_progress(move_files_task)
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.SUCCESS if success else StatusCode.FAILURE, file_meta)
    except Exception:
      success = False
      local_logger.exception(
        "%s: Error moving\n[yellow]%s[/] to\n[yellow]%s[/]",
        self.__class__.__name__,
        send_path,
        recv_path,
        extra={"markup": True},
      )
      file_meta.pickup_success[idx] = False
      self._advance_progress(move_files_task)
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)
    return success

  def _download_candidate(self, remote_path: PurePosixPath, adapted_logger: LoggerAdapter[Any] | None = None) -> bytes | None:
    """Fetch a filename-matched file so its header can be inspected. Transient errors retry with the base
    backoff; anything else is logged and the file is skipped for this run (it stays in the pickup folder)."""
    local_logger = adapted_logger or logger
    for attempt in range(1, self._transient_transfer_retries + 2):
      # Checked per attempt, like `_transfer_file_vend_to_main`: the error state can be set while this loop backs off.
      if self.errored:
        local_logger.warning("%s: Disabled due to error state. Skipping header read of %s", self.__class__.__name__, remote_path.name)
        return None
      try:
        buffer = BytesIO()
        with self.vendor_ftp.start_session() as client:
          client.download_file(remote_path.as_posix(), callback=buffer.write, task_msg=f"Reading {remote_path.name} (Attempt {attempt})")
        return buffer.getvalue()
      except Exception as e:
        if self._is_transient_transfer_error(e) and attempt <= self._transient_transfer_retries:
          backoff_seconds = 2 ** (attempt - 1)
          local_logger.warning(
            "%s: Transient read failure for %s on attempt %s of %s. Retrying in %s seconds",
            self.__class__.__name__,
            remote_path.name,
            attempt,
            self._transient_transfer_retries + 1,
            backoff_seconds,
            exc_info=e,
          )
          sleep(backoff_seconds)
          continue
        local_logger.exception("%s: Could not read %s for header inspection; skipping this run", self.__class__.__name__, remote_path)
        return None
    return None

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP)
  @override
  async def _pickup_files(  # noqa: C901, PLR0912, PLR0915
    self,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    """Copy of the base pickup with three substitutions: the date window is decided from each candidate's header
    line (downloaded up front), the transfer is a same-server rename, and candidates are gathered from the waiting
    folder as well as the pickup folder.

    That last one is what makes a partial pickup recoverable. The rename *is* the removal from the pickup folder,
    so a wave that renames A and then fails on B (or dies before `_persist_queues`) leaves A in the waiting folder
    with its entry still queued for pickup. Listing only the pickup folder would then re-match B alone, reset
    `file_names` to `{0: B}` and advance the entry without A. Listing both folders means the re-run sees A and B
    again and finishes the job; A's "rename" is a no-op because it is already at its destination.
    """
    local_logger = adapted_logger or logger
    if not self._file_pickup_queue:
      return
    if not self.vendor_ftp.test_connection():
      local_logger.warning("%s: Aborting pickup_files due to offline FTP server", self.__class__.__name__)
      return

    async with self._lock:
      # 0. Gather candidate names from both folders, remembering where each one currently sits. A name present in
      #    both is a genuine conflict (nothing here overwrites): the waiting-folder copy wins, because it is the
      #    one a previous, interrupted run already committed to, and the pickup-folder copy is left untouched.
      source_folders: dict[str, PurePosixPath] = {}
      with self.vendor_ftp.start_session() as client:
        for entry in client.listdir(self.pickup_ftp_folder.as_posix()):
          source_folders[entry.filename] = self.pickup_ftp_folder
        for entry in client.listdir(self.pre_processing_waiting_folder.as_posix()):
          if entry.filename in source_folders:
            local_logger.warning(
              "%s: %s exists in both %s and %s; preferring the waiting-folder copy and leaving the pickup copy in place",
              self.__class__.__name__,
              entry.filename,
              self.pickup_ftp_folder,
              self.pre_processing_waiting_folder,
            )
          source_folders[entry.filename] = self.pre_processing_waiting_folder

      # 1. Filename match, then download every candidate once; the bytes serve both the header check and the
      #    invoice-number extraction after the rename.
      candidates: dict[str, list[str]] = {}
      for key, file_meta in self._file_pickup_queue.items():
        names = [name for name in source_folders if file_meta.file_pattern.match(name)]
        if names:
          candidates[key] = names
        else:
          local_logger.warning(
            "%s: %s: No files matched with pattern %s", self.__class__.__name__, key, file_meta.file_pattern.pattern
          )

      unique_names = sorted({name for names in candidates.values() for name in names})
      downloaded = dict(
        zip(
          unique_names,
          await gather(
            *(to_thread(self._download_candidate, source_folders[name] / name, adapted_logger) for name in unique_names)
          ),
          strict=True,
        )
      )

      # 2. Keep only files whose header date is inside the entry's window.
      items_to_dl: dict[str, FileRegisterData] = {}
      kept_bytes: dict[str, dict[int, bytes]] = {}
      for key, names in candidates.items():
        file_meta = self._file_pickup_queue[key]
        kept: list[tuple[str, bytes]] = []
        for name in names:
          data = downloaded.get(name)
          if data is None:
            continue
          first_line = data.splitlines()[0].decode("utf-8", errors="ignore") if data else ""
          header_date = self.parse_header_date(first_line)
          if header_date is None:
            local_logger.warning("%s: %s: %s has no parseable header line; leaving in place", self.__class__.__name__, key, name)
            continue
          if not self.header_date_in_window(file_meta, header_date):
            local_logger.warning(
              "%s: %s: %s header date %s is outside the pickup window; leaving in place",
              self.__class__.__name__,
              key,
              name,
              header_date.isoformat(),
            )
            continue
          kept.append((name, data))

        if kept:
          file_meta.file_names = {idx: name for idx, (name, _) in enumerate(kept)}
          file_meta.pickup_success = {}
          file_meta.invoice_nums = {}
          items_to_dl[key] = file_meta
          kept_bytes[key] = {idx: data for idx, (_, data) in enumerate(kept)}
          if log_action_handler is not None:
            log_action_handler(key, StatusCode.UNKNOWN, file_meta)
          local_logger.info("%s: %s: Matched %s files for: %s", self.__class__.__name__, key, len(kept), file_meta.storenum)

      # 3. Rename kept files into the waiting folder.
      with self.pbar.add_task("Transferring Files", total=sum(len(v.file_names) for v in items_to_dl.values())) as move_files_task:
        futures = []
        for key, file_meta in items_to_dl.items():
          remote_file_locs = file_meta.remote_file_locs
          futures.extend(
            to_thread(
              self._transfer_file_same_server,
              send_path=(source_folders[filename] / filename),
              recv_path=remote_file_locs[idx],
              move_files_task=move_files_task,
              file_meta=file_meta,
              idx=idx,
              key=key,
              file_bytes=kept_bytes[key][idx],
              adapted_logger=adapted_logger,
              log_action_handler=log_action_handler,
            )
            for idx, filename in file_meta.file_names.items()
          )
        await gather(*futures)

      # 4. Commit first, clean up last (same ordering rationale as the base class).
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if file_meta.pickup_success and all(file_meta.pickup_success.values()):
          items_to_advance[key] = file_meta
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule
          local_logger.info(
            "%s: %s: Checking off %s_%s invoice_grabbed", self.__class__.__name__, key, self.supplier_name, file_meta.storenum
          )
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

      for key, item in items_to_advance.items():
        self._file_waiting_queue[key] = item
        self._file_pickup_queue.pop(key)
        local_logger.info("%s: %s: Moved %s to waiting queue", self.__class__.__name__, key, item.storenum)

      persisted = self._persist_queues()

      if not persisted:
        local_logger.warning(
          "%s: Queue backup could not be written; the entries advanced this run are only in memory. A restart from "
          "the stale backup re-runs pickup for them, finds the already-renamed files in %s (candidates are gathered "
          "from the waiting folder as well as the pickup folder), skips the rename as a no-op and re-advances them",
          self.__class__.__name__,
          self.pre_processing_waiting_folder,
        )
      # No vendor-side archive step: the rename *is* the removal from the pickup folder, so
      # `pickup_archive_ftp_folder` is `None` and nothing is ever written to a vendor archive.

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
    orig_attr: PurePosixPath | None = getattr(SFTProcessor, attr_name)
    if orig_attr is None:
      # `pickup_archive_ftp_folder`: SFT has no vendor archive, so there is nothing to prefix.
      continue
    new_val = PurePosixPath("/Testing") / orig_attr.relative_to("/")
    setattr(SFTProcessor, attr_name, new_val)


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
    sft = SFTProcessor(pbar)
    orders = [order async for order in cache.schedule.walk_typed_rows() if order.supplier == SuppliersEnum.SFT]

    for order in orders:
      await sft.register_pickup(
        storenum=order.store, customer_id=order.customer, pickup_date=now, dropoff_date=now, current_week=True
      )
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
