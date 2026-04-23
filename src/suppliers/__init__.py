if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import gather, to_thread
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime
from errno import EACCES
from ftplib import all_errors
from io import BytesIO
from logging import LoggerAdapter, getLogger
from pathlib import Path, PurePosixPath
from re import Match, Pattern
from socket import timeout as SocketTimeout
from time import sleep
from typing import cast

from aiologic import Lock
from database.cache import DatabaseCache
from dateutil.relativedelta import SA, SU, relativedelta
from environment_init_vars import SETTINGS
from logging_config import LOG_LOC_FOLDER, add_log_context
from paramiko import SSHException
from pydantic import TypeAdapter
from rich_custom import CustomTaskID, ProgressCustom
from send_alert_email import send_alert_email
from typing_custom import CustomerID, StoreNum, SupplierQueueKey
from typing_custom.abc import SingletonType
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

from suppliers.ftp_adapter import AdaptedFTP, FTPAdapter, SFTFTPClient
from suppliers.log_action import LogActionHandlerType, log_actions

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

  vendor_ftp: FTPAdapter
  waiting_ftp: FTPAdapter[AdaptedFTP] = FTPAdapter(SFTFTPClient, container_cls="SupplierProcessorBase")

  queue_backup_prefix: str

  supplier_name: SuppliersEnum

  invoice_num_pattern: Pattern[str] | None

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
  log_file_loc: Path = LOG_LOC_FOLDER
  ctx_var_identifier: ContextVar[str | None]
  ctx_var_log_loc: ContextVar[Path | None]

  errored: bool

  def __init__(self, pbar: ProgressCustom = None) -> None:  # type: ignore
    self._file_pickup_queue = {}
    self._file_preprocess_queue = {}
    self._file_waiting_queue = {}
    self._file_dropoff_queue = {}

    self.errored = False

    if pbar is not None:
      self.waiting_ftp.pbar = pbar

    self._file_queue_backup_folder.mkdir(exist_ok=True, parents=True)
    self._corrupted_queue_backup_folder.mkdir(exist_ok=True, parents=True)
    self.local_pre_processing_folder.mkdir(exist_ok=True, parents=True)
    if self.local_post_processing_folder:
      self.local_post_processing_folder.mkdir(exist_ok=True, parents=True)
    self.log_file_loc.mkdir(exist_ok=True, parents=True)

    self.pickup_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_pickup_queue.json"
    self.waiting_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_waiting_queue.json"
    self.dropoff_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_dropoff_queue.json"

    self.preprocess_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_preprocess_queue.json"

    self.pbar = cast(ProgressCustom, pbar)

    self._load_queue_backups()

    self.cache: DatabaseCache = DatabaseCache()

  def __del__(self) -> None:
    self._save_backups()

  async def save_queue_backups_off_thread(self) -> None:
    await to_thread(self._save_backups)

  def _save_backups(self) -> None:
    try:
      with self._lock:
        backup = (
          (
            self.pickup_queue_backup_file,
            self._queue_ta.dump_json(self._file_pickup_queue, indent=2, round_trip=True),
          ),
          (
            self.preprocess_queue_backup_file,
            self._queue_ta.dump_json(self._file_preprocess_queue, indent=2, round_trip=True),
          ),
          (
            self.waiting_queue_backup_file,
            self._queue_ta.dump_json(self._file_waiting_queue, indent=2, round_trip=True),
          ),
          (
            self.dropoff_queue_backup_file,
            self._queue_ta.dump_json(self._file_dropoff_queue, indent=2, round_trip=True),
          ),
        )
        pass

        # for _, bak in backup:
        #   for k, v in bak.items():
        #     v["file_pattern"] = v["file_pattern"].pattern
        #     v["_waiting_folder"] = str(v["_waiting_folder"])
        #     v["pickup_date"] = v["pickup_date"].isoformat()
        #     v["dropoff_date"] = v["dropoff_date"].isoformat()

        for file, data in backup:
          with file.open("wb") as f:
            f.write(data)
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception as e:
      logger.error(f"{self.__class__.__name__}: Error saving queue backups: {e}")
      raise e

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
    except Exception as exc:
      quarantined_file = self._quarantine_corrupted_queue_backup(backup_file, raw_backup)
      logger.error(
        f"{self.__class__.__name__}: Failed to load {queue_name} queue backup from {backup_file}. "
        f"Quarantined corrupted backup to {quarantined_file}: {exc}"
      )
      send_alert_email(
        subject=f"Corrupted {self.queue_backup_prefix} {queue_name} queue backup",
        content=(
          f"{self.__class__.__name__} could not load the {queue_name} queue backup.\n\n"
          f"Original backup: {backup_file}\n"
          f"Quarantined copy: {quarantined_file}\n"
          f"Error: {exc}\n\n"
          "Startup will continue with this queue cleared."
        ),
      )
      return {}

  async def clean_stale_queue_entries(self) -> None:
    if self.errored:
      logger.warning(f"{self.__class__.__name__}: Disabled due to error state. Skipping cleanup of stale queue entries")
      return
    async with self._lock:
      changed_entries = await self._clean_stale_queue_entries()

    if changed_entries:
      await to_thread(self._save_backups)

  async def _clean_stale_queue_entries(self) -> int:
    # Note: Called during __init__, no need for lock protection
    changed_entries = 0

    for key, item in self._file_pickup_queue.items():
      if item.stale:
        self._file_pickup_queue.pop(key)
        changed_entries += 1
        logger.warning(f"{self.__class__.__name__}: Removed stale queue entry {key} from pickup queue")
      elif await (self.cache.schedule if item.current_week else self.cache.prev_week_schedule).check_toggled(
        (self.supplier_name, item.storenum), DatabaseScheduleColumns.invoice_grabbed
      ):
        entry = self._file_pickup_queue.pop(key)
        self._file_waiting_queue[key] = entry
        changed_entries += 1
        logger.warning(
          f"{self.__class__.__name__}: queue entry {key} found in pickup queue while marked as invoice_grabbed. Moved entry to waiting queue."
        )

    for queue_name, queue in {
      "waiting": self._file_waiting_queue,
      "preprocess": self._file_preprocess_queue,
      "dropoff": self._file_dropoff_queue,
    }.items():
      for key, item in tuple(queue.items()):
        if item.stale or await (self.cache.schedule if item.current_week else self.cache.prev_week_schedule).check_toggled(
          (self.supplier_name, item.storenum), DatabaseScheduleColumns.invoice_applied
        ):
          queue.pop(key)
          changed_entries += 1
          logger.warning(f"{self.__class__.__name__}: Removed stale queue entry {key} from {queue_name} queue")

    return changed_entries

  def _quarantine_corrupted_queue_backup(self, backup_file: Path, raw_backup: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    quarantined_file = self._corrupted_queue_backup_folder / f"{backup_file.stem}_{timestamp}{backup_file.suffix}"
    quarantined_file.write_text(raw_backup)
    return quarantined_file

  @classmethod
  def check_connections(cls) -> bool:
    waiting_ftp_online = cls.waiting_ftp.test_connection()
    vendor_ftp_online = cls.vendor_ftp.test_connection()

    if not waiting_ftp_online:
      logger.error(f"{cls.__name__}: Waiting FTP server is offline.")
    if not vendor_ftp_online:
      logger.error(f"{cls.__name__}: Vendor FTP server is offline.")

    return waiting_ftp_online and vendor_ftp_online

  async def register_pickup(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
  ) -> None:
    if self.errored:
      logger.warning(
        f"{self.__class__.__name__}: Disabled due to error state. Skipping registration of pickup for {storenum}, {customer_id}"
      )
      return
    await self._register_pickup(storenum, customer_id, pickup_date, dropoff_date, current_week)

  async def register_dropoff(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool
  ) -> None:
    if self.errored:
      logger.warning(
        f"{self.__class__.__name__}: Disabled due to error state. Skipping registration of dropoff for {storenum}, {customer_id}"
      )
      return
    await self._register_dropoff(storenum, customer_id, pickup_date, dropoff_date, current_week)

  async def pickup_files(self) -> None:
    if self.errored:
      logger.warning(f"{self.__class__.__name__}: Disabled due to error state. Skipping pickup of files")
      return
    await self._pickup_files()

  async def dropoff_files(self) -> None:
    if self.errored:
      logger.warning(f"{self.__class__.__name__}: Disabled due to error state. Skipping dropoff of files")
      return
    await self._dropoff_files()

  @add_log_context(action_identifier_prefix=LogActionEnum.REGISTERED_PICKUP, log_subfolder=LogActionEnum.REGISTERED_PICKUP)
  @log_actions(action_identifier_prefix=LogActionEnum.REGISTERED_PICKUP)
  async def _register_pickup(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ): ...

  @add_log_context(action_identifier_prefix=LogActionEnum.REGISTERED_DROPOFF, log_subfolder=LogActionEnum.REGISTERED_DROPOFF)
  @log_actions(action_identifier_prefix=LogActionEnum.REGISTERED_DROPOFF)
  async def _register_dropoff(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ): ...

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP)
  async def _pickup_files(
    self,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ): ...

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED, log_subfolder=LogActionEnum.FILE_PREPROCESSED)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PREPROCESSED)
  async def _preprocess_files(
    self,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    if self.errored:
      local_logger.warning(f"{self.__class__.__name__}: Disabled due to error state. Skipping preprocessing of files")
      return
    if not self._file_preprocess_queue:
      return
    if not self.waiting_ftp.test_connection(logit=True):
      local_logger.warning(f"{self.__class__.__name__}: Waiting FTP server is not online. Cancelling preprocessing step.")
      return
    async with self._lock:
      if not self._file_preprocess_queue:
        return

      num_files = sum(len(v.file_names) for v in self._file_preprocess_queue.values())

      local_logger.info(f"{self.__class__.__name__}: Beginning preprocessing for {num_files} files")

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
        if all(file_meta.preprocess_success.values()):
          file_meta._waiting_folder = self.post_processing_waiting_folder
          self._file_preprocess_queue.pop(key)
          self._file_dropoff_queue[key] = file_meta

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_DROPPED_OFF, log_subfolder=LogActionEnum.FILE_DROPPED_OFF)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_DROPPED_OFF)
  async def _dropoff_files(
    self,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger

    if not self._file_preprocess_queue:
      return

    if not self.waiting_ftp.test_connection(logit=True):
      local_logger.warning(f"{self.__class__.__name__}: Waiting FTP server is not online. Cancelling dropoff step.")
      return

    await self._preprocess_files()

    if self.errored:
      local_logger.warning(f"{self.__class__.__name__}: Disabled due to error state. Skipping dropoff of files")
      return

    if not self._file_dropoff_queue:
      local_logger.error(
        f"{self.__class__.__name__}: No files to drop off after preprocessing step."
        "This likely indicates an error in the preprocessing step."
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
        if all(file_meta.dropoff_success.values()):
          self._file_dropoff_queue.pop(key)
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          local_logger.info(f"{self.__class__.__name__}: Checking off {self.supplier_name}_{file_meta.storenum} invoice_applied")
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_applied)

  def _transfer_file_vend_to_main(
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: CustomTaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    success_attr: str,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    result = StatusCode.UNKNOWN
    for attempt in range(1, self._transient_transfer_retries + 2):
      if self.errored:
        local_logger.warning(
          f"{self.__class__.__name__}: Disabled due to error state. Skipping transfer of files from vendor to main FTP"
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
        if log_action_handler is not None:
          log_action_handler(key, result, file_meta)

        local_logger.info(
          f"{self.__class__.__name__}: Transferred {self.supplier_name} [yellow]{send_path}[/] to SFT FTP [yellow]{recv_path}[/]",
          extra={"markup": True},
        )
        self.extract_invoice_num(transient_file, file_meta, idx, adapted_logger=adapted_logger)
        self.pbar.update(move_files_task, advance=1)
        return success
      except Exception as e:
        if self._is_transient_transfer_error(e) and attempt <= self._transient_transfer_retries:
          backoff_seconds = 2 ** (attempt - 1)
          local_logger.warning(
            f"{self.__class__.__name__}: Transient transfer failure for {send_path.name} on attempt {attempt} of "
            f"{self._transient_transfer_retries + 1}. Retrying in {backoff_seconds} seconds",
            exc_info=e,
          )
          sleep(backoff_seconds)
          continue

        local_logger.error(f"{self.__class__.__name__}: Error transferring {send_path} to {recv_path}", exc_info=e)
        getattr(file_meta, success_attr)[idx] = False
        if log_action_handler is not None:
          log_action_handler(key, StatusCode.FAILURE, file_meta)
        raise

  def _is_transient_transfer_error(self, exc: BaseException) -> bool:
    if isinstance(
      exc,
      (TimeoutError, SocketTimeout, ConnectionError, BrokenPipeError, EOFError, SSHException),
    ):
      return True

    message = str(exc).lower()
    if any(fragment in message for fragment in TRANSIENT_TRANSFER_ERROR_STRINGS):
      return True

    return any(
      nested_exc is not None and self._is_transient_transfer_error(nested_exc) for nested_exc in (exc.__cause__, exc.__context__)
    )

  def extract_invoice_num(
    self, bytestream: BytesIO, file_meta: FileRegisterData, idx: int, adapted_logger: LoggerAdapter | None = None
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
            f"{self.__class__.__name__}: Failed to extract invoice number from file for {file_meta.storenum} using pattern {self.invoice_num_pattern.pattern}"
          )
    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error extracting invoice number for {file_meta.storenum}: {e}")

  def _transfer_file_main_to_main(
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: CustomTaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    success_attr: str,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    result = StatusCode.UNKNOWN
    if self.errored:
      local_logger.warning(f"{self.__class__.__name__}: Disabled due to error state. Skipping transfer of files within main FTP")
      return False
    try:
      with self.waiting_ftp.start_session() as client:
        client.rename(send_path.as_posix(), recv_path.as_posix())

        # Verify file was moved successfully
        success = False
        try:
          client.get_size(recv_path.as_posix())
          success = True
          result = StatusCode.SUCCESS
          local_logger.info(
            f"{self.__class__.__name__}: Moved [yellow]{send_path}[/] to [yellow]{recv_path}[/]", extra={"markup": True}
          )
        except (*all_errors, OSError) as e:
          local_logger.warning(f"{self.__class__.__name__}: Failed to verify move of {send_path.name}: {e}")
          result = StatusCode.FAILURE
        getattr(file_meta, success_attr)[idx] = success

      self.pbar.update(move_files_task, advance=1)
      if log_action_handler is not None:
        log_action_handler(key, result, file_meta)
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception as e:
      local_logger.error(
        f"{self.__class__.__name__}: Error moving\n[yellow]{send_path}[/] to\n[yellow]{recv_path}[/]",
        exc_info=e,
        extra={"markup": True},
      )
      if log_action_handler is not None:
        log_action_handler(key, StatusCode.FAILURE, file_meta)


class SupplierProcessorSFTPIntermediate(SupplierProcessorBase):
  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern: ...

  @add_log_context(action_identifier_prefix=LogActionEnum.REGISTERED_PICKUP, log_subfolder=LogActionEnum.REGISTERED_PICKUP)
  @log_actions(action_identifier_prefix=LogActionEnum.REGISTERED_PICKUP)
  async def _register_pickup(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )

    if picked_up:
      local_logger.info(
        f"{self.__class__.__name__}: "
        f"Attempted to register pickup for already grabbed invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      local_logger.info(
        f"{self.__class__.__name__}: "
        f"Attempted to register pickup for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}"
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
        local_logger.error(f"{self.__class__.__name__}: {queue_key}: File has already been picked up and has not been checked off")
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
    local_logger.info(f"{self.__class__.__name__}: Added {storenum} to pickup queue")

    if log_action_handler is not None:
      log_action_handler(queue_key, StatusCode.SUCCESS, register_data)

  @add_log_context(action_identifier_prefix=LogActionEnum.REGISTERED_DROPOFF, log_subfolder=LogActionEnum.REGISTERED_DROPOFF)
  @log_actions(action_identifier_prefix=LogActionEnum.REGISTERED_DROPOFF)
  async def _register_dropoff(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool,
    adapted_logger: LoggerAdapter | None = None,
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

    if not picked_up:
      local_logger.debug(
        f"{self.__class__.__name__}: {key}: "
        f"Attempted to register dropoff for not-yet picked up invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      local_logger.debug(
        f"{self.__class__.__name__}: {key}: "
        f"Attempted to register dropoff for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return

    # Protect queue operations with lock to prevent race conditions
    async with self._lock:
      # first check if key is already in dropoff queue
      if key in self._file_preprocess_queue or key in self._file_dropoff_queue:
        local_logger.warning(f"{self.__class__.__name__}: {key}: File already registered for dropoff")
        return
      if key not in self._file_waiting_queue:
        local_logger.error(
          f"{self.__class__.__name__}: {key}: Key not found in the file waiting queue! "
          f"{self.supplier_name}, {storenum}, {customer_id}, {pickup_date.isoformat()}"
        )
        return
      else:
        try:
          matched_item = self._file_waiting_queue.pop(key)
        except KeyError as e:
          local_logger.critical(
            f"{self.__class__.__name__}: {key}: "
            f"No waiting file found for: {self.supplier_name}, {storenum}, {customer_id}, {pickup_date.isoformat()}\n"
            f"Invoice may not have been picked up or is missing!",
            exc_info=e,
            stack_info=True,
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

      self._file_preprocess_queue[key] = matched_item
      local_logger.info(f"{self.__class__.__name__}: {key}: Registered dropoff for: {matched_item.storenum}")

  def assemble_queue_key(self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime) -> SupplierQueueKey:
    return f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

  def _middle_archive_file(
    self,
    source_folder: PurePosixPath,
    remote_file: str,
    archive_folder: PurePosixPath,
    adapted_logger: LoggerAdapter | None = None,
    debug: bool = False,
  ) -> None:
    local_logger = adapted_logger or logger
    if self.errored:
      local_logger.warning(
        f"{self.__class__.__name__}: Disabled due to error state. Skipping archiving of file {remote_file} from {source_folder} to {archive_folder}"
      )
      return
    try:
      source_loc = (source_folder / remote_file).as_posix()
      archive_loc = (archive_folder / remote_file).as_posix()
      with self.waiting_ftp.start_session() as ftp_client:
        try:
          ftp_client.get_size(archive_loc)
          local_logger.info(
            f"{self.__class__.__name__}: Archive file already exists at [yellow]{archive_loc}[/]",
            extra={"markup": True},
          )

        except (*all_errors, OSError):
          if not debug:
            local_logger.info(
              f"{self.__class__.__name__}: Archiving [yellow]{remote_file}[/] to {archive_folder.as_posix()}",
              extra={"markup": True},
            )
            ftp_client.rename(source_loc, archive_loc)

        else:
          if not debug:
            local_logger.info(
              f"{self.__class__.__name__}: Deleting new file from {source_loc} instead of moving.",
            )
            ftp_client.remove(source_loc)
    except (*all_errors, OSError) as e:
      local_logger.error(f"{self.__class__.__name__}: File {remote_file} not found at {source_folder} for archiving", exc_info=e)
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error archiving file {remote_file} at {source_folder}", exc_info=e)
      raise e

  def _vendor_archive_file(
    self,
    source_folder: PurePosixPath,
    remote_file: str,
    archive_folder: PurePosixPath,
    adapted_logger: LoggerAdapter | None = None,
    debug: bool = False,
  ) -> None:
    local_logger = adapted_logger or logger
    if self.errored:
      local_logger.warning(
        f"{self.__class__.__name__}: Disabled due to error state. Skipping archiving of file {remote_file} from {source_folder} to {archive_folder}"
      )
      return
    try:
      source_loc = (source_folder / remote_file).as_posix()
      archive_loc = (archive_folder / remote_file).as_posix()
      with self.vendor_ftp.start_session() as sftp_client:
        try:
          sftp_client.get_size(archive_loc)
          local_logger.info(
            f"{self.__class__.__name__}: Archive file already exists at [yellow]{archive_loc}[/]",
            extra={"markup": True},
          )

        except FileNotFoundError:
          if not debug:
            local_logger.info(
              f"{self.__class__.__name__}: Archiving [yellow]{remote_file}[/] to {archive_folder.as_posix()}",
              extra={"markup": True},
            )
            sftp_client.rename(source_loc, archive_loc)

        else:
          if not debug:
            local_logger.info(
              f"{self.__class__.__name__}: Deleting new file from {source_loc} instead of moving.",
            )
            sftp_client.remove(source_loc)
    except FileNotFoundError as e:
      local_logger.error(f"{self.__class__.__name__}: File {remote_file} not found at {source_folder} for archiving", exc_info=e)
    # Ensure that exceptions actually get logged while executing off main thread
    except IOError as e:
      if e.args and e.args[0] is EACCES:
        local_logger.error(f"{self.__class__.__name__}: Permission denied archiving file {remote_file} at {source_folder}", exc_info=e)
      else:
        local_logger.error(f"{self.__class__.__name__}: IOError archiving file {remote_file} at {source_folder}", exc_info=e)
        raise e

    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error archiving file {remote_file}", exc_info=e)
      raise e

  @add_log_context(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  @log_actions(action_identifier_prefix=LogActionEnum.FILE_PICKED_UP)
  async def _pickup_files(
    self,
    adapted_logger: LoggerAdapter | None = None,
    log_action_handler: LogActionHandlerType | None = None,
  ):
    local_logger = adapted_logger or logger
    if not self._file_pickup_queue:
      return
    if not self.vendor_ftp.test_connection() or not self.waiting_ftp.test_connection():
      local_logger.warning(f"{self.__class__.__name__}: Aborting pickup_files due to offline FTP server(s)")
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
              if file_date is not None:
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
          items_to_dl[key] = file_meta
          if log_action_handler is not None:
            log_action_handler(key, StatusCode.UNKNOWN, file_meta)
          local_logger.info(f"{self.__class__.__name__}: {key}: Matched {len(matched_files)} files for: {file_meta.storenum}")
        else:
          local_logger.warning(f"{self.__class__.__name__}: {key}: No files matched with pattern {file_meta.file_pattern.pattern}")

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

      archive_futures = []
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if all(file_meta.pickup_success.values()):
          if self.pickup_archive_ftp_folder is not None:
            archive_futures.extend(
              to_thread(
                self._vendor_archive_file,
                source_folder=self.pickup_ftp_folder,
                remote_file=filename,
                archive_folder=self.pickup_archive_ftp_folder,
                adapted_logger=adapted_logger,
                debug=__debug__,
              )
              for filename in file_meta.file_names.values()
            )
          items_to_advance[key] = file_meta
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          local_logger.info(
            f"{self.__class__.__name__}: {key}: Checking off {self.supplier_name}_{file_meta.storenum} invoice_grabbed"
          )
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_grabbed)

      if self.pickup_archive_ftp_folder is not None:
        await gather(*archive_futures)

    for key, item in items_to_advance.items():
      self._file_waiting_queue[key] = item
      self._file_pickup_queue.pop(key)
      local_logger.info(f"{self.__class__.__name__}: {key}: Moved {item.storenum} to waiting queue")


if __name__ == "__main__":
  from suppliers.sas import SASProcessor

  processor = SASProcessor()
  pass
