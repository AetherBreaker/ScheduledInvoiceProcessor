# pyright: reportImportCycles=false
# pyright: reportUninitializedInstanceVariable=false
# Standard library imports
from asyncio import gather, to_thread
from atexit import register as register_at_exit
from copy import deepcopy
from datetime import datetime
from errno import EACCES
from ftplib import all_errors
from io import BytesIO
from json import loads
from logging import Logger, getLogger
from os import replace
from threading import RLock
from time import sleep
from typing import TYPE_CHECKING, Any, ClassVar, cast

# Third party imports
from aiologic import Lock
from dateutil.relativedelta import SA, SU, relativedelta
from paramiko import SSHException
from pydantic import SecretStr, TypeAdapter

# First party imports
from aeth_ext.errors.send_alert_email import send_alert_email
from aeth_ext.ftp import create_ftp_adapter
from aeth_ext.ftp.credentials import FTPCredentials
from aeth_ext.types.abc import SingletonType
from scheduled_invoice_processor.database import DatabaseCache
from scheduled_invoice_processor.environment_init_vars import CWD, SETTINGS
from scheduled_invoice_processor.logging_config import add_log_context
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.log_action import log_actions
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode

if TYPE_CHECKING:
  # Standard library imports
  from contextvars import ContextVar
  from logging import LoggerAdapter
  from pathlib import Path, PurePosixPath
  from re import Match, Pattern

  # First party imports
  from aeth_ext.ftp.pool.ftp_adapter import FTPAdapter
  from aeth_ext.ftp.pool.sftp_adapter import SFTPAdapter
  from aeth_ext.ftp.session import AdapterBase
  from aeth_ext.rich.progress import Progress, TaskID
  from scheduled_invoice_processor.suppliers.log_action import LogActionHandlerType
  from scheduled_invoice_processor.typing_custom import CustomerID, StoreNum, SupplierQueueKey
  from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

logger = getLogger(__name__)
TRANSIENT_TRANSFER_ERROR_STRINGS = (
  "connection reset",
  "connection aborted",
  "connection closed",
  "server disconnected",
  "timed out",
  "timeout",
  "broken pipe",
  "socket is closed",
  "eof",
)


HOLDING_FOLDER = CWD / "file_holding"


def load_sft_credentials() -> FTPCredentials:
  """Credentials for the shared SFT holding FTP server (`waiting_ftp`), read from `sft_creds.json`.

  The raw JSON never outlives this call: the password leaves here wrapped in a `SecretStr`, so nothing on the
  processor classes holds a plaintext credential.
  """
  raw = loads(SETTINGS.sft_website_creds_file.read_text())
  return FTPCredentials(host=raw["HOST"], username=raw["USER"], password=SecretStr(raw["PWD"]), port=int(raw.get("PORT", 21)))


class SupplierProcessorBase(metaclass=SingletonType):
  _file_pickup_queue: dict[SupplierQueueKey, FileRegisterData]
  _file_preprocess_queue: dict[SupplierQueueKey, FileRegisterData]
  _file_waiting_queue: dict[SupplierQueueKey, FileRegisterData]
  _file_dropoff_queue: dict[SupplierQueueKey, FileRegisterData]
  _queue_ta = TypeAdapter(dict[str, FileRegisterData])
  _file_queue_backup_folder: Path = SETTINGS.persisted_dir_loc / "queue_backups"
  _corrupted_queue_backup_folder: Path = _file_queue_backup_folder / "corrupted"
  _transient_transfer_retries = 3
  _lock: Lock = Lock()
  _persist_lock: RLock = RLock()
  """Serialises queue-dict mutation-plus-persist across OS threads. `_lock` (aiologic) only excludes the event loop;
  `_preprocess_off_thread` workers run concurrently in the default thread pool while the loop holds `_lock`, so the
  swap-and-persist in those workers and every `_persist_queues()` write take this lock. Re-entrant because event-loop
  callers already hold `_lock` when they call `_persist_queues()`; lock order is always `_lock` -> `_persist_lock`
  (workers never take `_lock`), so there is no deadlock."""

  vendor_ftp: FTPAdapter | SFTPAdapter
  waiting_ftp: FTPAdapter = create_ftp_adapter(load_sft_credentials(), container_cls="SupplierProcessorBase")

  queue_backup_prefix: str

  supplier_name: SuppliersEnum

  invoice_num_pattern: ClassVar[Pattern[str] | None]

  checks_date_in_filename: bool = False

  pickup_ftp_folder: PurePosixPath
  pickup_archive_ftp_folder: PurePosixPath | None
  pre_processing_waiting_folder: PurePosixPath
  pre_processing_archive_folder: PurePosixPath
  post_processing_waiting_folder: PurePosixPath
  destination_ftp_folder: PurePosixPath

  local_pre_processing_folder: Path
  local_post_processing_folder: Path

  identifier_prefix: str = ""
  log_file_loc: Path = SETTINGS.log_loc_folder
  ctx_var_identifier: ContextVar[str | None]
  ctx_var_log_loc: ContextVar[Path | None]

  errored: bool

  def __init__(self, pbar: Progress = None) -> None:  # pyright: ignore[reportArgumentType]
    self._file_pickup_queue = {}
    self._file_preprocess_queue = {}
    self._file_waiting_queue = {}
    self._file_dropoff_queue = {}

    self.errored = False

    if pbar is not None:  # pyright: ignore[reportUnnecessaryComparison]
      self.waiting_ftp.pbar = pbar
      if vendor_ftp := cast("FTPAdapter | SFTPAdapter | None", getattr(self, "vendor_ftp", None)):
        vendor_ftp.pbar = pbar

    self._file_queue_backup_folder.mkdir(exist_ok=True, parents=True)
    self._corrupted_queue_backup_folder.mkdir(exist_ok=True, parents=True)
    self.log_file_loc.mkdir(exist_ok=True, parents=True)

    self.job_holding_folder = HOLDING_FOLDER / self.__class__.__name__.lower()
    self.job_holding_folder.mkdir(parents=True, exist_ok=True)

    self.pickup_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_pickup_queue.json"
    self.waiting_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_waiting_queue.json"
    self.dropoff_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_dropoff_queue.json"

    self.preprocess_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_preprocess_queue.json"

    self.pbar = pbar

    self._load_queue_backups()

    # Replaces __del__, which CPython does not guarantee to run for module-level singletons.
    register_at_exit(self._persist_queues_at_exit)

    self.cache: DatabaseCache = DatabaseCache()

    self.__post_init__()

  def __post_init__(self) -> None:
    pass

  def _persist_queues(self) -> None:
    """Write all four queues to their backup files, atomically per file.

    Callers either hold `self._lock` (the queue-mutating blocks) or are the at-exit path; the write itself is
    additionally serialised by `self._persist_lock` so concurrent `_preprocess_off_thread` workers (which run on
    the default thread pool while the event loop holds `_lock`) never iterate/dump the same dict at once. Each
    file is written to `<name>.tmp` and then `os.replace`d onto the real path, so a crash mid-write leaves the
    previous backup intact and the loader's quarantine path only ever sees real corruption. An `OSError` on any
    single file's write/replace is logged and that file's previous backup is left intact rather than propagating
    -- a one-cycle-stale backup beats aborting the business operation mid-way -- and the remaining files are still
    attempted.
    """
    with self._persist_lock:
      for backup_file, queue in (
        (self.pickup_queue_backup_file, self._file_pickup_queue),
        (self.preprocess_queue_backup_file, self._file_preprocess_queue),
        (self.waiting_queue_backup_file, self._file_waiting_queue),
        (self.dropoff_queue_backup_file, self._file_dropoff_queue),
      ):
        tmp_file = backup_file.with_name(f"{backup_file.name}.tmp")
        try:
          tmp_file.write_bytes(self._queue_ta.dump_json(queue, indent=2, round_trip=True))
          replace(tmp_file, backup_file)
        except OSError:
          logger.exception(
            "%s: Failed to persist queue backup %s; the previous backup file is left intact", self.__class__.__name__, backup_file
          )
          tmp_file.unlink(missing_ok=True)

  def _persist_queues_at_exit(self) -> None:
    """Final save at interpreter exit. Waits up to 1 s for the queue lock; if it is still held (a transfer was
    mid-flight when the process was told to exit) the snapshot is written anyway — a possibly mid-mutation but
    always parseable file beats losing the last change."""
    acquired = self._lock.green_acquire(timeout=1.0)
    if not acquired:
      logger.warning(
        "%s: queue lock still held at interpreter exit; persisting a possibly mid-mutation snapshot", self.__class__.__name__
      )
    try:
      self._persist_queues()
    except Exception:
      logger.exception("%s: Error persisting queue backups at interpreter exit", self.__class__.__name__)
    finally:
      if acquired:
        self._lock.green_release()

  def _load_queue_backups(self) -> None:
    # Note: Called during __init__, no need for lock protection
    to_load = (
      (
        self._load_queue_backup_file(self.pickup_queue_backup_file, "pickup"),
        self._file_pickup_queue,
      ),
      (
        self._load_queue_backup_file(self.preprocess_queue_backup_file, "preprocess"),
        self._file_preprocess_queue,
      ),
      (
        self._load_queue_backup_file(self.waiting_queue_backup_file, "waiting"),
        self._file_waiting_queue,
      ),
      (
        self._load_queue_backup_file(self.dropoff_queue_backup_file, "dropoff"),
        self._file_dropoff_queue,
      ),
    )

    for loaded, target in to_load:
      target.clear()
      target.update(deepcopy(loaded))

  def _load_queue_backup_file(self, backup_file: Path, queue_name: str) -> dict[str, FileRegisterData]:
    if not backup_file.exists():
      return {}

    raw_backup = backup_file.read_text()

    try:
      return self._queue_ta.validate_json(raw_backup)
    except Exception as e:
      quarantined_file = self._quarantine_corrupted_queue_backup(backup_file, raw_backup)
      logger.error(
        "%s: Failed to load %s queue backup from %s. Quarantined corrupted backup to %s: %s",
        self.__class__.__name__,
        queue_name,
        backup_file,
        quarantined_file,
        exc_info=e,
      )
      send_alert_email(
        subject=f"Corrupted {self.queue_backup_prefix} {queue_name} queue backup",
        content=(
          f"{self.__class__.__name__} could not load the {queue_name} queue backup.\n\n"
          f"Original backup: {backup_file}\n"
          f"Quarantined copy: {quarantined_file}\n"
          f"Error: {e}\n\n"
          "Startup will continue with this queue cleared."
        ),
      )
      return {}

  async def clean_stale_queue_entries(self) -> None:
    if self.errored:
      logger.warning("%s: Disabled due to error state. Skipping cleanup of stale queue entries", self.__class__.__name__)
      return
    async with self._lock:
      changed_entries = await self._clean_stale_queue_entries()
      if changed_entries:
        self._persist_queues()

  async def _clean_stale_queue_entries(self) -> int:
    # Note: Called during __init__, no need for lock protection
    changed_entries = 0

    for key, item in self._file_pickup_queue.copy().items():
      if item.stale:
        self._file_pickup_queue.pop(key)
        changed_entries += 1
        logger.warning("%s: Removed stale queue entry %s from pickup queue", self.__class__.__name__, key)
      elif await (self.cache.schedule if item.current_week else self.cache.prev_week_schedule).check_toggled(
        (self.supplier_name, item.storenum), DatabaseScheduleColumns.invoice_grabbed
      ) or await (self.cache.schedule if item.current_week else self.cache.prev_week_schedule).check_toggled(
        (self.supplier_name, item.storenum), DatabaseScheduleColumns.manually_moved
      ):
        entry = self._file_pickup_queue.pop(key)
        self._file_waiting_queue[key] = entry
        changed_entries += 1
        logger.warning(
          "%s: queue entry %s found in pickup queue while marked as invoice_grabbed. Moved entry to waiting queue.",
          self.__class__.__name__,
          key,
        )

    for queue_name, queue in {
      "waiting": self._file_waiting_queue,
      "preprocess": self._file_preprocess_queue,
      "dropoff": self._file_dropoff_queue,
    }.items():
      for key, item in tuple(queue.copy().items()):
        if (
          item.stale
          or await (self.cache.schedule if item.current_week else self.cache.prev_week_schedule).check_toggled(
            (self.supplier_name, item.storenum), DatabaseScheduleColumns.invoice_applied
          )
          or await (self.cache.schedule if item.current_week else self.cache.prev_week_schedule).check_toggled(
            (self.supplier_name, item.storenum), DatabaseScheduleColumns.manually_moved
          )
        ):
          queue.pop(key)
          changed_entries += 1
          logger.warning("%s: Removed stale queue entry %s from %s queue", self.__class__.__name__, key, queue_name)

    return changed_entries

  def _quarantine_corrupted_queue_backup(self, backup_file: Path, raw_backup: str) -> Path:
    timestamp = datetime.now(tz=SETTINGS.tz).strftime("%Y%m%d_%H%M%S")
    quarantined_file = self._corrupted_queue_backup_folder / f"{backup_file.stem}_{timestamp}{backup_file.suffix}"
    quarantined_file.write_text(raw_backup)
    return quarantined_file

  @classmethod
  def check_connections(cls) -> bool:
    waiting_ftp_online = cls.waiting_ftp.test_connection()
    vendor_ftp_online = cls.vendor_ftp.test_connection()

    if not waiting_ftp_online:
      logger.error("%s: Waiting FTP server is offline.", cls.__name__)
    if not vendor_ftp_online:
      logger.error("%s: Vendor FTP server is offline.", cls.__name__)

    return waiting_ftp_online and vendor_ftp_online

  async def register_pickup(
    self,
    storenum: "StoreNum",  # noqa: UP037
    customer_id: "CustomerID",  # noqa: UP037
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
  ) -> None:
    if self.errored:
      logger.warning(
        "%s: Disabled due to error state. Skipping registration of pickup for %s, %s",
        self.__class__.__name__,
        storenum,
        customer_id,
      )
      return
    await self._register_pickup(storenum, customer_id, pickup_date, dropoff_date, current_week)

  async def register_dropoff(
    self,
    storenum: "StoreNum",  # noqa: UP037
    customer_id: "CustomerID",  # noqa: UP037
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool,
  ) -> None:
    if self.errored:
      logger.warning(
        "%s: Disabled due to error state. Skipping registration of dropoff for %s, %s",
        self.__class__.__name__,
        storenum,
        customer_id,
      )
      return
    await self._register_dropoff(storenum, customer_id, pickup_date, dropoff_date, current_week)

  async def pickup_files(self) -> None:
    if self.errored:
      logger.warning("%s: Disabled due to error state. Skipping pickup of files", self.__class__.__name__)
      return
    await self._pickup_files()

  async def dropoff_files(self) -> None:
    if self.errored:
      logger.warning("%s: Disabled due to error state. Skipping dropoff of files", self.__class__.__name__)
      return
    await self._dropoff_files()

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED, log_subfolder=LogActionEnum.FILE_PREPROCESSED)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED)
  async def _preprocess_files(
    self,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    if self.errored:
      local_logger.warning("%s: Disabled due to error state. Skipping preprocessing of files", self.__class__.__name__)
      return
    if not self._file_preprocess_queue:
      return
    if not self.waiting_ftp.test_connection(logit=True):
      local_logger.warning("%s: Waiting FTP server is not online. Cancelling preprocessing step.", self.__class__.__name__)
      return
    async with self._lock:
      if not self._file_preprocess_queue:
        return

      num_files = sum(len(v.file_names) for v in self._file_preprocess_queue.values())

      local_logger.info("%s: Beginning preprocessing for %s files", self.__class__.__name__, num_files)

      with self.pbar.add_task(f"{self.__class__.__name__}: Preprocessing files", total=num_files) as files_move_task:
        futures = []
        for key, file_meta in tuple(self._file_preprocess_queue.items()):
          futures.extend(
            to_thread(
              self._transfer_file_main_to_main,
              send_path=waiting_path,
              recv_path=(self.post_processing_waiting_folder / waiting_path.name),
              move_files_task=files_move_task,
              file_meta=file_meta,
              idx=idx,
              key=key,
              success_attr="preprocess_success",
              adapted_logger=adapted_logger,
              log_action_handler=log_action_handler,
            )
            for idx, waiting_path in file_meta.remote_file_locs.items()
          )

          # Now that we are certain there are items to be moved, add them  to log_action_handler immediately
          # incase an error occurs during transfer, we will still have the context of which files were being processed for logging purposes
          if log_action_handler is not None:
            log_action_handler(key, StatusCode.UNKNOWN, file_meta)

        await gather(*futures)

      # Now that the transfers are complete, clear the items to log

      for key, file_meta in tuple(self._file_preprocess_queue.items()):
        if file_meta.preprocess_success and all(file_meta.preprocess_success.values()):
          file_meta._waiting_folder = self.post_processing_waiting_folder  # pyright: ignore[reportPrivateUsage]
          self._file_preprocess_queue.pop(key)
          self._file_dropoff_queue[key] = file_meta

      self._persist_queues()

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_DROPPED_OFF, log_subfolder=LogActionEnum.FILE_DROPPED_OFF)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_DROPPED_OFF)
  async def _dropoff_files(
    self,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger

    if not self._file_preprocess_queue and not self._file_dropoff_queue:
      return

    if not self.waiting_ftp.test_connection(logit=True):
      local_logger.warning("%s: Waiting FTP server is not online. Cancelling dropoff step.", self.__class__.__name__)
      return

    await self._preprocess_files()

    if self.errored:
      local_logger.warning("%s: Disabled due to error state. Skipping dropoff of files", self.__class__.__name__)
      return

    if not self._file_dropoff_queue:
      local_logger.warning(
        "%s: No files to drop off after preprocessing step (preprocessing did not advance any entry this run)",
        self.__class__.__name__,
      )
      return
    async with self._lock:
      with self.pbar.add_task(
        f"{self.__class__.__name__}: Moving files to dropoff folder",
        total=sum(len(v.file_names) for v in self._file_dropoff_queue.values()),
      ) as files_move_task:
        futures = []
        for key, file_meta in tuple(self._file_dropoff_queue.items()):
          futures.extend(
            to_thread(
              self._transfer_file_main_to_main,
              send_path=waiting_path,
              recv_path=(self.destination_ftp_folder / waiting_path.name),
              move_files_task=files_move_task,
              file_meta=file_meta,
              idx=idx,
              key=key,
              success_attr="dropoff_success",
              adapted_logger=adapted_logger,
              log_action_handler=log_action_handler,
            )
            for idx, waiting_path in file_meta.remote_file_locs.items()
          )

          # Now that we are certain there are items to be moved, add them  to log_action_handler immediately
          # incase an error occurs during transfer, we will still have the context of which files were being processed for logging purposes
          if log_action_handler is not None:
            log_action_handler(key, StatusCode.UNKNOWN, file_meta)

        await gather(*futures)

      # Now that the transfers are complete, clear the items to log

      for key, file_meta in tuple(self._file_dropoff_queue.items()):
        if file_meta.dropoff_success and all(file_meta.dropoff_success.values()):
          self._file_dropoff_queue.pop(key)
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          local_logger.info("%s: Checking off %s_%s invoice_applied", self.__class__.__name__, self.supplier_name, file_meta.storenum)
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_applied)

      self._persist_queues()

  def _transfer_file_vend_to_main(  # noqa: PLR0917
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: TaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    success_attr: str,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    result = StatusCode.UNKNOWN
    for attempt in range(1, self._transient_transfer_retries + 2):
      if self.errored:
        local_logger.warning(
          "%s: Disabled due to error state. Skipping transfer of files from vendor to main FTP",
          self.__class__.__name__,
        )
        return False
      try:
        transient_file = BytesIO()

        with self.vendor_ftp.start_session() as source_client, self.waiting_ftp.start_session() as dest_client:
          success = source_client.transfer_file(
            source_remote_path=send_path.as_posix(),
            dest_remote_path=recv_path.as_posix(),
            other=dest_client,
            task_msg=f"Transferring {send_path.name} (Attempt {attempt})",
            mem_stream=transient_file,
          )

        result = StatusCode.SUCCESS if success else StatusCode.FAILURE

        # update items to log with result of transfer and pickup success status
        getattr(file_meta, success_attr)[idx] = success

        local_logger.info(
          "%s: Transferred %s [yellow]%s[/] to SFT FTP [yellow]%s[/]",
          self.__class__.__name__,
          self.supplier_name,
          send_path,
          recv_path,
          extra={"markup": True},
        )
        self.extract_invoice_num(transient_file, file_meta, idx, adapted_logger=adapted_logger)
        if log_action_handler is not None:
          log_action_handler(key, result, file_meta)
        self.pbar.update(move_files_task, advance=1, refresh=True)
        return success
      except Exception as e:
        if self._is_transient_transfer_error(e) and attempt <= self._transient_transfer_retries:
          backoff_seconds = 2 ** (attempt - 1)
          local_logger.warning(
            "%s: Transient transfer failure for %s on attempt %s of %s. Retrying in %s seconds",
            self.__class__.__name__,
            send_path.name,
            attempt,
            self._transient_transfer_retries + 1,
            backoff_seconds,
            exc_info=e,
          )
          sleep(backoff_seconds)
          continue

        local_logger.exception("%s: Error transferring %s to %s", self.__class__.__name__, send_path, recv_path)
        getattr(file_meta, success_attr)[idx] = False
        if log_action_handler is not None:
          log_action_handler(key, StatusCode.FAILURE, file_meta)
        raise

  def _is_transient_transfer_error(self, exc: BaseException) -> bool:
    if isinstance(
      exc,
      (TimeoutError, ConnectionError, BrokenPipeError, EOFError, SSHException),
    ):
      return True

    message = str(exc).lower()
    if any(fragment in message for fragment in TRANSIENT_TRANSFER_ERROR_STRINGS):
      return True

    return any(
      nested_exc is not None and self._is_transient_transfer_error(nested_exc) for nested_exc in (exc.__cause__, exc.__context__)
    )

  def extract_invoice_num(
    self, bytestream: BytesIO, file_meta: FileRegisterData, idx: int, adapted_logger: LoggerAdapter[Any] | None = None
  ):
    # convert transient file to string
    # extract first line from transient file and apply regex pattern to extract invoice number, then store in file_meta.invoice_nums[idx]
    local_logger = adapted_logger or logger
    try:
      bytestream.seek(0)
      bytes_data = bytestream.read()
      first_line = bytes_data.splitlines()[0].decode("utf-8", errors="ignore")
      if self.invoice_num_pattern is not None:
        if match := self.invoice_num_pattern.search(first_line):
          file_meta.invoice_nums[idx] = match.group("invoice_num")
        else:
          local_logger.warning(
            "%s: Failed to extract invoice number from file for %s using pattern %s",
            self.__class__.__name__,
            file_meta.storenum,
            self.invoice_num_pattern.pattern,
          )
    except Exception:
      local_logger.exception("%s: Error extracting invoice number for %s", self.__class__.__name__, file_meta.storenum)

  def _transfer_file_main_to_main(  # noqa: PLR0917
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: TaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    success_attr: str,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    """Move a file within the holding FTP. Idempotent: if the rename fails because it already happened (a stop
    mid-wave lets the rename thread finish but never runs the bookkeeping that records it), the move is reported
    as a success so the re-run can advance the queue instead of stranding the entry."""
    local_logger = adapted_logger or logger
    if self.errored:
      local_logger.warning("%s: Disabled due to error state. Skipping transfer of files within main FTP", self.__class__.__name__)
      getattr(file_meta, success_attr)[idx] = False
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)
      return False
    success = False
    try:
      with self.waiting_ftp.start_session() as client:
        try:
          client.rename(send_path.as_posix(), recv_path.as_posix())
        except (*all_errors, OSError):
          if self._already_moved(client, send_path, recv_path):
            local_logger.info(
              "%s: [yellow]%s[/] was already moved to [yellow]%s[/] by an earlier run; treating as success",
              self.__class__.__name__,
              send_path,
              recv_path,
              extra={"markup": True},
            )
            success = True
          else:
            raise
        else:
          # Verify file was moved successfully
          try:
            client.get_size(recv_path.as_posix())
            success = True
            local_logger.info(
              "%s: Moved [yellow]%s[/] to [yellow]%s[/]",
              self.__class__.__name__,
              send_path,
              recv_path,
              extra={"markup": True},
            )
          except (*all_errors, OSError) as e:
            local_logger.warning("%s: Failed to verify move of %s", self.__class__.__name__, send_path.name, exc_info=e)
      result = StatusCode.SUCCESS if success else StatusCode.FAILURE
      getattr(file_meta, success_attr)[idx] = success
      self.pbar.update(move_files_task, advance=1, refresh=True)
      if log_action_handler is not None:
        log_action_handler(key, result, file_meta)
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception:
      success = False
      local_logger.exception(
        "%s: Error moving\n[yellow]%s[/] to\n[yellow]%s[/]",
        self.__class__.__name__,
        send_path,
        recv_path,
        extra={"markup": True},
      )
      getattr(file_meta, success_attr)[idx] = False
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)
    return success

  @staticmethod
  def _already_moved(client: AdapterBase, send_path: PurePosixPath, recv_path: PurePosixPath) -> bool:
    """True only when the destination holds a non-empty file *and* the source is gone. A destination beside a
    still-present source is a genuine conflict and is never treated as done (nothing here ever overwrites)."""
    try:
      recv_size = client.get_size(recv_path.as_posix())
    except (*all_errors, OSError):
      return False
    if not recv_size:
      return False
    try:
      client.get_size(send_path.as_posix())
    except (*all_errors, OSError):
      return True
    return False

  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern[str]: ...

  @add_log_context(action_identifier_prefix=LogActionEnum.REGISTERED_PICKUP, log_subfolder=LogActionEnum.REGISTERED_PICKUP)
  @log_actions(action_identifier_prefix=LogActionEnum.REGISTERED_PICKUP)
  async def _register_pickup(  # noqa: PLR0917
    self,
    storenum: "StoreNum",  # noqa: UP037
    customer_id: "CustomerID",  # noqa: UP037
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )
    manually_moved = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.manually_moved
    )

    if picked_up:
      local_logger.info(
        "%s: Attempted to register pickup for already grabbed invoice: %s, %s, %s",
        self.__class__.__name__,
        self.supplier_name,
        storenum,
        customer_id,
      )
      return
    if applied:
      local_logger.info(
        "%s: Attempted to register pickup for already applied invoice: %s, %s, %s",
        self.__class__.__name__,
        self.supplier_name,
        storenum,
        customer_id,
      )
      return
    if manually_moved:
      local_logger.info(
        "%s: Attempted to register pickup for manually moved invoice: %s, %s, %s",
        self.__class__.__name__,
        self.supplier_name,
        storenum,
        customer_id,
      )
      return

    queue_key = self.assemble_queue_key(storenum, customer_id, pickup_date)

    # check if file already registered for pickup
    if queue_key in self._file_pickup_queue or (
      picked_up := any(
        (queue_key in self._file_waiting_queue, queue_key in self._file_preprocess_queue, queue_key in self._file_dropoff_queue)
      )
    ):
      # program constantly attempts to re-register things for pickup. So no need to emit a warning
      if picked_up:
        local_logger.error("%s: %s: File has already been picked up and has not been checked off", self.__class__.__name__, queue_key)
      return

    pattern = self.assemble_filename_pattern(customer_id, pickup_date, dropoff_date, current_week)

    register_data = FileRegisterData(
      storenum=storenum,
      customer_id=customer_id,
      pickup_date=pickup_date,
      dropoff_date=dropoff_date,
      file_pattern=pattern,
      _current_week=current_week,
      _waiting_folder=self.pre_processing_waiting_folder,
      _local_copy_folder=self.local_pre_processing_folder,
    )

    if log_action_handler is not None:
      log_action_handler(queue_key, StatusCode.UNKNOWN, register_data)

    # Protect queue modification with lock for consistency
    async with self._lock:
      self._file_pickup_queue[queue_key] = register_data
      self._persist_queues()
    local_logger.info("%s: Added %s to pickup queue", self.__class__.__name__, storenum)

    if log_action_handler is not None:
      log_action_handler(queue_key, StatusCode.SUCCESS, register_data)

  @add_log_context(action_identifier_prefix=LogActionEnum.REGISTERED_DROPOFF, log_subfolder=LogActionEnum.REGISTERED_DROPOFF)
  @log_actions(action_identifier_prefix=LogActionEnum.REGISTERED_DROPOFF)
  async def _register_dropoff(  # noqa: PLR0917
    self,
    storenum: "StoreNum",  # noqa: UP037
    customer_id: "CustomerID",  # noqa: UP037
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    key = self.assemble_queue_key(storenum, customer_id, pickup_date)

    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )
    manually_moved = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.manually_moved
    )

    if not picked_up:
      local_logger.debug(
        "%s: %s: Attempted to register dropoff for not-yet picked up invoice: %s, %s, %s",
        self.__class__.__name__,
        key,
        self.supplier_name,
        storenum,
        customer_id,
      )
      return
    if applied:
      local_logger.debug(
        "%s: %s: Attempted to register dropoff for already applied invoice: %s, %s, %s",
        self.__class__.__name__,
        key,
        self.supplier_name,
        storenum,
        customer_id,
      )
      return
    if manually_moved:
      local_logger.debug(
        "%s: %s: Attempted to register dropoff for manually moved invoice: %s, %s, %s",
        self.__class__.__name__,
        key,
        self.supplier_name,
        storenum,
        customer_id,
      )
      return

    # Protect queue operations with lock to prevent race conditions
    async with self._lock:
      # first check if key is already in dropoff queue
      if key in self._file_preprocess_queue or key in self._file_dropoff_queue:
        local_logger.warning("%s: %s: File already registered for dropoff", self.__class__.__name__, key)
        return
      if key not in self._file_waiting_queue:
        local_logger.error(
          "%s: %s: Key not found in the file waiting queue! %s, %s, %s, %s",
          self.__class__.__name__,
          key,
          self.supplier_name,
          storenum,
          customer_id,
          pickup_date.isoformat(),
        )
        return
      else:
        try:
          matched_item = self._file_waiting_queue.pop(key)
        except KeyError as e:
          local_logger.critical(
            "%s: %s: No waiting file found for: %s, %s, %s, %s\nInvoice may not have been picked up or is missing!",
            self.__class__.__name__,
            key,
            self.supplier_name,
            storenum,
            customer_id,
            pickup_date.isoformat(),
            stack_info=True,
            exc_info=e,
          )
          if log_action_handler is not None:
            log_action_handler(
              key,
              StatusCode.FAILURE,
              FileRegisterData(
                storenum=storenum,
                customer_id=customer_id,
                pickup_date=pickup_date,
                dropoff_date=dropoff_date,
                file_pattern=self.assemble_filename_pattern(customer_id, pickup_date, dropoff_date, current_week),
                _current_week=current_week,
                _waiting_folder=self.pre_processing_waiting_folder,
                _local_copy_folder=self.local_pre_processing_folder,
              ),
            )
          return

      if log_action_handler is not None:
        log_action_handler(key, StatusCode.SUCCESS, matched_item)

      matched_item.dropoff_date = dropoff_date

      self._file_preprocess_queue[key] = matched_item
      local_logger.info("%s: %s: Registered dropoff for: %s", self.__class__.__name__, key, matched_item.storenum)
      self._persist_queues()

  def assemble_queue_key(self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime) -> SupplierQueueKey:
    return f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

  def _handle_existing_archive(  # noqa: PLR0917
    self,
    client: AdapterBase,
    source_loc: str,
    archive_loc: str,
    archive_size: int | None,
    archive_folder: PurePosixPath,
    remote_file: str,
    debug: bool,
    adapted_logger: LoggerAdapter[Any] | Logger,
  ) -> None:
    local_logger = adapted_logger or logger
    source_size = client.get_size(source_loc)
    if source_size == 0:
      local_logger.warning(
        "%s: Source file [yellow]%s[/] is empty. Skipping archive to preserve existing archive at [yellow]%s[/].",
        self.__class__.__name__,
        source_loc,
        archive_loc,
        extra={"markup": True},
      )
    elif archive_size == 0:
      local_logger.warning(
        "%s: Existing archive at [yellow]%s[/] is empty. Replacing with source file.",
        self.__class__.__name__,
        archive_loc,
        extra={"markup": True},
      )
      if not debug:
        client.remove(archive_loc)
        client.rename(source_loc, archive_loc)
    elif source_size != archive_size:
      existing_path = archive_folder / remote_file
      timestamp = datetime.now(tz=SETTINGS.tz).strftime("%Y%m%d_%H%M%S")
      new_archive_loc = (archive_folder / f"{existing_path.stem}_{timestamp}{existing_path.suffix}").as_posix()
      local_logger.warning(
        "%s: Source file [yellow]%s[/] (%s bytes) differs from existing archive (%s bytes). Archiving source to [yellow]%s[/] instead.",
        self.__class__.__name__,
        source_loc,
        source_size,
        archive_size,
        new_archive_loc,
        extra={"markup": True},
      )
      if not debug:
        client.rename(source_loc, new_archive_loc)
    elif not debug:
      local_logger.info(
        "%s: Deleting new file from %s instead of moving.",
        self.__class__.__name__,
        source_loc,
      )
      client.remove(source_loc)

  def _middle_archive_file(
    self,
    source_folder: PurePosixPath,
    remote_file: str,
    archive_folder: PurePosixPath,
    adapted_logger: LoggerAdapter[Any] | None = None,
    debug: bool = False,
  ) -> None:
    local_logger = adapted_logger or logger
    if self.errored:
      local_logger.warning(
        "%s: Disabled due to error state. Skipping archiving of file %s from %s to %s",
        self.__class__.__name__,
        remote_file,
        source_folder,
        archive_folder,
      )
      return
    try:
      source_loc = (source_folder / remote_file).as_posix()
      archive_loc = (archive_folder / remote_file).as_posix()
      with self.waiting_ftp.start_session() as ftp_client:
        try:
          archive_size = ftp_client.get_size(archive_loc)
          local_logger.info(
            "%s: Archive file already exists at [yellow]%s[/]",
            self.__class__.__name__,
            archive_loc,
            extra={"markup": True},
          )

        except (*all_errors, OSError):
          if not debug:
            local_logger.info(
              "%s: Archiving [yellow]%s[/] to %s",
              self.__class__.__name__,
              remote_file,
              archive_folder.as_posix(),
              extra={"markup": True},
            )
            ftp_client.rename(source_loc, archive_loc)

        else:
          self._handle_existing_archive(
            ftp_client, source_loc, archive_loc, archive_size, archive_folder, remote_file, debug, local_logger
          )
    except (*all_errors, OSError):
      local_logger.exception("%s: File %s not found at %s for archiving", self.__class__.__name__, remote_file, source_folder)
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception:
      local_logger.exception("%s: Error archiving file %s at %s", self.__class__.__name__, remote_file, source_folder)
      raise

  def _vendor_archive_file(
    self,
    source_folder: PurePosixPath,
    remote_file: str,
    archive_folder: PurePosixPath,
    adapted_logger: LoggerAdapter[Any] | None = None,
    debug: bool = False,
  ) -> None:
    local_logger = adapted_logger or logger
    if self.errored:
      local_logger.warning(
        "%s: Disabled due to error state. Skipping archiving of file %s from %s to %s",
        self.__class__.__name__,
        remote_file,
        source_folder,
        archive_folder,
      )
      return
    try:
      source_loc = (source_folder / remote_file).as_posix()
      archive_loc = (archive_folder / remote_file).as_posix()
      with self.vendor_ftp.start_session() as sftp_client:
        try:
          archive_size = sftp_client.get_size(archive_loc)
          local_logger.info(
            "%s: Archive file already exists at [yellow]%s[/]",
            self.__class__.__name__,
            archive_loc,
            extra={"markup": True},
          )

        except FileNotFoundError:
          if not debug:
            local_logger.info(
              "%s: Archiving [yellow]%s[/] to %s",
              self.__class__.__name__,
              remote_file,
              archive_folder.as_posix(),
              extra={"markup": True},
            )
            sftp_client.rename(source_loc, archive_loc)

        else:
          self._handle_existing_archive(
            sftp_client, source_loc, archive_loc, archive_size, archive_folder, remote_file, debug, local_logger
          )
    except FileNotFoundError:
      local_logger.exception("%s: File %s not found at %s for archiving", self.__class__.__name__, remote_file, source_folder)
    # Ensure that exceptions actually get logged while executing off main thread
    except OSError as e:
      if e.args and e.args[0] is EACCES:
        local_logger.exception("%s: Permission denied archiving file %s at %s", self.__class__.__name__, remote_file, source_folder)
      else:
        local_logger.exception("%s: IOError archiving file %s at %s", self.__class__.__name__, remote_file, source_folder)
        raise

    except Exception:
      local_logger.exception("%s: Error archiving file %s", self.__class__.__name__, remote_file)
      raise

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP)
  async def _pickup_files(  # noqa: C901, PLR0912
    self,
    adapted_logger: LoggerAdapter[Any] | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    if not self._file_pickup_queue:
      return
    if not self.vendor_ftp.test_connection() or not self.waiting_ftp.test_connection():
      local_logger.warning("%s: Aborting pickup_files due to offline FTP server(s)", self.__class__.__name__)
      return

    async with self._lock:
      with self.vendor_ftp.start_session() as client:
        remote_files = [*client.listdir(self.pickup_ftp_folder.as_posix())]

      items_to_dl: dict[str, FileRegisterData] = {}
      for key, file_meta in self._file_pickup_queue.items():
        matched_files: list[Match[str]] = []

        for remote_file in remote_files:
          if match := file_meta.file_pattern.match(remote_file.filename):
            if self.checks_date_in_filename:
              matched_files.append(match)
            else:
              file_date = remote_file.modified_time
              start_date = (
                file_meta.pickup_date - relativedelta(weekday=SU(-1), hour=0, minute=0, second=0, microsecond=0)
              ) - relativedelta(weeks=1 if file_meta.current_week else 0)
              end_date = (
                file_meta.dropoff_date + relativedelta(weekday=SA(+1), hour=23, minute=59, second=59, microsecond=999999)
              ) - relativedelta(weeks=0 if file_meta.current_week else 1)
              if start_date <= file_date < end_date:
                matched_files.append(match)

        if matched_files:
          file_meta.file_names = {idx: m.string for idx, m in enumerate(matched_files)}
          file_meta.pickup_success = {}
          file_meta.invoice_nums = {}
          items_to_dl[key] = file_meta
          if log_action_handler is not None:
            log_action_handler(key, StatusCode.UNKNOWN, file_meta)
          local_logger.info("%s: %s: Matched %s files for: %s", self.__class__.__name__, key, len(matched_files), file_meta.storenum)
        else:
          local_logger.warning(
            "%s: %s: No files matched with pattern %s", self.__class__.__name__, key, file_meta.file_pattern.pattern
          )

      with self.pbar.add_task("Transferring Files", total=sum(len(v.file_names) for v in items_to_dl.values())) as move_files_task:
        dl_futures = []
        for key, file_meta in items_to_dl.items():
          remote_file_locs = file_meta.remote_file_locs
          dl_futures.extend(
            to_thread(
              self._transfer_file_vend_to_main,
              send_path=(self.pickup_ftp_folder / filename),
              recv_path=remote_file_locs[idx],
              move_files_task=move_files_task,
              file_meta=file_meta,
              idx=idx,
              key=key,
              success_attr="pickup_success",
              adapted_logger=adapted_logger,
              log_action_handler=log_action_handler,
            )
            for idx, filename in file_meta.file_names.items()
          )
        await gather(*dl_futures)

      # Commit first, clean up last. Once the copies have landed, the durable facts are the sheet tick and the
      # queue move; the vendor-side archive only removes inputs a re-run would need. A stop between the commit and
      # the archive leaves an already-copied file in the vendor folder, which `_vendor_archive_file` reconciles
      # on the next run; a stop before the commit re-runs the copies from intact inputs.
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if file_meta.pickup_success and all(file_meta.pickup_success.values()):
          items_to_advance[key] = file_meta
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule
          local_logger.info(
            "%s: %s: Checking off %s_%s invoice_grabbed",
            self.__class__.__name__,
            key,
            self.supplier_name,
            file_meta.storenum,
          )
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

      for key, item in items_to_advance.items():
        self._file_waiting_queue[key] = item
        self._file_pickup_queue.pop(key)
        local_logger.info("%s: %s: Moved %s to waiting queue", self.__class__.__name__, key, item.storenum)

      self._persist_queues()

      if self.pickup_archive_ftp_folder is not None:
        archive_futures = [
          to_thread(
            self._vendor_archive_file,
            source_folder=self.pickup_ftp_folder,
            remote_file=filename,
            archive_folder=self.pickup_archive_ftp_folder,
            adapted_logger=adapted_logger,
            debug=__debug__,
          )
          for file_meta in items_to_advance.values()
          for filename in file_meta.file_names.values()
        ]
        await gather(*archive_futures)
