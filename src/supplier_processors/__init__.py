if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import gather, to_thread
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime, timedelta
from ftplib import FTP, _SSLSocket, error_perm, error_temp  # type: ignore
from io import BytesIO
from json import loads
from logging import LoggerAdapter, getLogger
from pathlib import PurePosixPath
from re import Match, Pattern
from typing import Optional, Protocol, Self

from aiologic import Lock
from database.cache import DatabaseCache
from dateutil.relativedelta import SU, relativedelta
from environment_init_vars import CWD, SETTINGS
from logging_config import add_log_context
from paramiko import SFTPClient
from pydantic import ConfigDict, Field, TypeAdapter
from pydantic.dataclasses import dataclass
from rich_custom import CustomTaskID, ProgressCustom
from typing_custom import CustomerID, StoreNum, SupplierQueueKey
from typing_custom.abc import SingletonType
from typing_custom.custom_path import CustomPath
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

# from logging.handlers import QueueHandler
# from queue import Queue
# from logging_config import DynamicQueueListener

logger = getLogger(__name__)
# contextual_logs_queue = Queue(-1)
# logger.addHandler(QueueHandler(contextual_logs_queue))  # type: ignore

# contextual_log_listener = DynamicQueueListener(contextual_logs_queue, respect_handler_level=True)  # type: ignore


def advance_pbar(pbar: ProgressCustom, task_id: CustomTaskID):
  def advance(data: bytes):
    pbar.update(task_id, advance=len(data))

  return advance


@dataclass
class FileRegisterData:
  __pydantic_config__ = ConfigDict(
    populate_by_name=True,
    use_enum_values=True,
    validate_default=True,
    validate_assignment=True,
    coerce_numbers_to_str=True,
  )

  storenum: StoreNum
  customer_id: CustomerID
  pickup_date: datetime
  dropoff_date: datetime
  file_pattern: Pattern[str]
  current_week: bool
  _waiting_folder: PurePosixPath
  _local_copy_folder: CustomPath

  file_names: dict[int, str] = Field(default_factory=dict)
  invoice_nums: dict[int, str] = Field(default_factory=dict)
  pickup_success: dict[int, bool] = Field(default_factory=dict)
  preprocess_success: dict[int, bool] = Field(default_factory=dict)
  dropoff_success: dict[int, bool] = Field(default_factory=dict)

  @property
  def remote_file_locs(self) -> dict[int, PurePosixPath]:
    return {idx: self._waiting_folder / name for idx, name in self.file_names.items()}

  @property
  def local_copy_loc(self) -> dict[int, CustomPath]:
    return {idx: self._local_copy_folder / name for idx, name in self.file_names.items()}

  @property
  def stale(self) -> bool:
    return datetime.now() > ((self.dropoff_date + relativedelta(weekday=SU(+1), hour=0, minute=0, second=0)) + timedelta(days=7))


class FTPProtocol(Protocol):
  def __init__(self, creds: dict) -> None: ...
  def __enter__(self) -> Self: ...
  def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...


class SFTPProtocol(Protocol):
  def __init__(self, creds: dict) -> None: ...
  def __enter__(self) -> SFTPClient: ...
  def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...


class SFTFTPClient(FTP, FTPProtocol):
  def __init__(self, creds: dict) -> None:
    self.creds = creds
    super().__init__()

  def __enter__(self) -> Self:
    self.connect(host=self.creds["HOST"], port=self.creds["PORT"])
    self.login(user=self.creds["USER"], passwd=self.creds["PWD"])
    return self

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    self.quit()


class SupplierProcessorBase(metaclass=SingletonType):
  _file_pickup_queue: dict[SupplierQueueKey, FileRegisterData]
  _file_preprocess_queue: dict[SupplierQueueKey, FileRegisterData]
  _file_waiting_queue: dict[SupplierQueueKey, FileRegisterData]
  _file_dropoff_queue: dict[SupplierQueueKey, FileRegisterData]
  _queue_ta = TypeAdapter(dict[str, FileRegisterData])
  _file_queue_backup_folder: CustomPath = CWD / "queue_backups"
  _lock: Lock = Lock()

  vendor_ftp: type[SFTPProtocol]
  waiting_ftp = SFTFTPClient

  queue_backup_prefix: str

  supplier_name: SuppliersEnum

  invoice_num_pattern: Optional[Pattern[str]]

  pickup_ftp_creds: dict
  waiting_ftp_creds: dict = loads(SETTINGS.sft_website_creds_file.read_text())

  pickup_ftp_folder: PurePosixPath
  pickup_archive_ftp_folder: PurePosixPath
  pre_processing_waiting_folder: PurePosixPath
  pre_processing_archive_folder: PurePosixPath
  post_processing_waiting_folder: PurePosixPath
  destination_ftp_folder: PurePosixPath

  local_pre_processing_folder: CustomPath
  local_post_processing_folder: CustomPath

  identifier_prefix: str = ""
  log_file_loc: CustomPath = CWD / "logs"
  ctx_var: ContextVar[str | None]

  def __init__(self, pbar: ProgressCustom = None) -> None:  # type: ignore
    self._file_pickup_queue = {}
    self._file_preprocess_queue = {}
    self._file_waiting_queue = {}
    self._file_dropoff_queue = {}

    self._file_queue_backup_folder.mkdir(exist_ok=True, parents=True)
    self.local_pre_processing_folder.mkdir(exist_ok=True, parents=True)
    if self.local_post_processing_folder:
      self.local_post_processing_folder.mkdir(exist_ok=True, parents=True)
    self.log_file_loc.mkdir(exist_ok=True, parents=True)

    self.pickup_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_pickup_queue.json"
    self.waiting_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_waiting_queue.json"
    self.dropoff_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_dropoff_queue.json"

    self.preprocess_queue_backup_file = self._file_queue_backup_folder / f"{self.queue_backup_prefix}_preprocess_queue.json"

    self.pbar = pbar

    self._load_queue_backups()

    self.cache: DatabaseCache = DatabaseCache()

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

  def __del__(self) -> None:
    self._save_backups()

  def _load_queue_backups(self) -> None:
    # Note: Called during __init__, no need for lock protection
    to_load = (
      (
        self._queue_ta.validate_json(self.pickup_queue_backup_file.read_text() if self.pickup_queue_backup_file.exists() else "{}"),
        self._file_pickup_queue,
      ),
      (
        self._queue_ta.validate_json(
          self.preprocess_queue_backup_file.read_text() if self.preprocess_queue_backup_file.exists() else "{}"
        ),
        self._file_preprocess_queue,
      ),
      (
        self._queue_ta.validate_json(self.waiting_queue_backup_file.read_text() if self.waiting_queue_backup_file.exists() else "{}"),
        self._file_waiting_queue,
      ),
      (
        self._queue_ta.validate_json(self.dropoff_queue_backup_file.read_text() if self.dropoff_queue_backup_file.exists() else "{}"),
        self._file_dropoff_queue,
      ),
    )

    for loaded, target in to_load:
      target.clear()
      target.update(deepcopy(loaded))

  def _clean_stale_queue_entries(self) -> None:
    # Note: Called during __init__, no need for lock protection
    for queue in (
      self._file_pickup_queue,
      self._file_preprocess_queue,
      self._file_waiting_queue,
      self._file_dropoff_queue,
    ):
      for key, item in tuple(queue.items()):
        if item.stale:
          queue.pop(key)

  @classmethod
  def check_connections(cls) -> bool:
    waiting_ftp_online = cls._check_waiting_ftp_online()
    vendor_ftp_online = cls._check_vendor_ftp_online()

    if not waiting_ftp_online:
      logger.error(f"{cls.__name__}: Waiting FTP server is offline.")
    if not vendor_ftp_online:
      logger.error(f"{cls.__name__}: Vendor FTP server is offline.")

    return waiting_ftp_online and vendor_ftp_online

  @classmethod
  def _check_waiting_ftp_online(cls) -> bool:
    """Check if the waiting FTP server is online by attempting to connect and send a NOOP command."""
    try:
      with cls.waiting_ftp(cls.waiting_ftp_creds) as ftp:
        ftp.voidcmd("NOOP")
      return True
    except Exception:
      return False

  @classmethod
  def _check_vendor_ftp_online(cls) -> bool:
    """Check if the vendor SFTP server is online by attempting to connect"""
    try:
      with cls.vendor_ftp(cls.pickup_ftp_creds) as ftp:
        ftp.listdir(".")
      return True
    except Exception:
      return False

  async def register_pickup(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
  ) -> None:
    await self._register_pickup(storenum, customer_id, pickup_date, dropoff_date, current_week)

  async def register_dropoff(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool
  ) -> None:
    await self._register_dropoff(storenum, customer_id, pickup_date, dropoff_date, current_week)

  async def pickup_files(self) -> None:
    await self._pickup_files()

  async def dropoff_files(self) -> None:
    await self._dropoff_files()

  @add_log_context(identifier_prefix=LogActionEnum.REGISTERED_PICKUP, log_subfolder=LogActionEnum.REGISTERED_PICKUP)
  async def _register_pickup(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ): ...

  @add_log_context(identifier_prefix=LogActionEnum.REGISTERED_DROPOFF, log_subfolder=LogActionEnum.REGISTERED_DROPOFF)
  async def _register_dropoff(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool,
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ): ...

  @add_log_context(identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  async def _pickup_files(
    self,
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ): ...

  @add_log_context(identifier_prefix=LogActionEnum.FILE_PREPROCESSED, log_subfolder=LogActionEnum.FILE_PREPROCESSED)
  async def _preprocess_files(
    self,
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger or logger
    if not self._file_preprocess_queue:
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
              items_to_log=items_to_log,
            )
            for idx, waiting_path in file_meta.remote_file_locs.items()
          )

          # Now that we are certain there are items to be moved, add them  to items_to_log immediately
          # incase an error occurs during transfer, we will still have the context of which files were being processed for logging purposes
          if items_to_log is not None:
            items_to_log[key] = StatusCode.UNKNOWN, file_meta

        await gather(*futures)

      # Now that the transfers are complete, clear the items to log

      for key, file_meta in tuple(self._file_preprocess_queue.items()):
        if all(file_meta.preprocess_success.values()):
          file_meta._waiting_folder = self.post_processing_waiting_folder
          self._file_preprocess_queue.pop(key)
          self._file_dropoff_queue[key] = file_meta

  @add_log_context(identifier_prefix=LogActionEnum.FILE_DROPPED_OFF, log_subfolder=LogActionEnum.FILE_DROPPED_OFF)
  async def _dropoff_files(
    self,
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger or logger

    if not self._file_preprocess_queue:
      return

    await self._preprocess_files()

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
              items_to_log=items_to_log,
            )
            for idx, waiting_path in file_meta.remote_file_locs.items()
          )

          # Now that we are certain there are items to be moved, add them  to items_to_log immediately
          # incase an error occurs during transfer, we will still have the context of which files were being processed for logging purposes
          if items_to_log is not None:
            items_to_log[key] = StatusCode.UNKNOWN, file_meta

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
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger or logger
    result = StatusCode.UNKNOWN
    try:
      transient_file = BytesIO()

      with self.vendor_ftp(self.pickup_ftp_creds) as origin_client:  # type: ignore
        file_size = origin_client.stat(send_path.as_posix()).st_size
        with self.waiting_ftp(self.waiting_ftp_creds) as dest_client:
          dest_client.voidcmd("TYPE I")
          with self.pbar.add_task(f"Transferring {send_path.name}") as transfer_task:
            with origin_client.open(send_path.as_posix(), "rb") as read_file:
              read_file.prefetch(file_size)
              with dest_client.transfercmd(f"STOR {recv_path.as_posix()}") as write_file:
                while buffer := read_file.read(8192):
                  write_file.sendall(buffer)
                  transient_file.write(buffer)
                  self.pbar.update(transfer_task, advance=len(buffer))
                if _SSLSocket is not None and isinstance(write_file, _SSLSocket):
                  write_file.unwrap()  # type: ignore
              dest_client.voidresp()

          # Verify file was transferred successfully
          success = False
          try:
            dest_client.size(recv_path.as_posix())
            success = True
            result = StatusCode.SUCCESS
          except (error_perm, error_temp, OSError) as e:
            local_logger.warning(f"{self.__class__.__name__}: Failed to verify transfer of {send_path.name}: {e}")
            result = StatusCode.FAILURE

          # update items to log with result of transfer and pickup success status
          getattr(file_meta, success_attr)[idx] = success
          if items_to_log is not None:
            items_to_log[key] = result, file_meta

      local_logger.info(
        f"{self.__class__.__name__}: Transferred {self.supplier_name} [yellow]{send_path}[/] to SFT FTP [yellow]{recv_path}[/]",
        extra={"markup": True},
      )
      self.extract_invoice_num(transient_file, file_meta, idx, adapted_logger=adapted_logger)
      self.pbar.update(move_files_task, advance=1)
      return success
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error transferring {send_path.name} to {recv_path.name}: {e}")
      if items_to_log is not None:
        items_to_log[key] = StatusCode.FAILURE, file_meta
      return False

  def extract_invoice_num(
    self, bytestream: BytesIO, file_meta: FileRegisterData, idx: int, adapted_logger: Optional[LoggerAdapter] = None
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
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict] = None,
  ):
    local_logger = adapted_logger or logger
    result = StatusCode.UNKNOWN
    try:
      with self.waiting_ftp(self.waiting_ftp_creds) as origin_client:
        origin_client.voidcmd("TYPE I")
        origin_client.rename(send_path.as_posix(), recv_path.as_posix())

        # Verify file was moved successfully
        success = False
        try:
          origin_client.size(recv_path.as_posix())
          success = True
          result = StatusCode.SUCCESS
          local_logger.info(
            f"{self.__class__.__name__}: Moved [yellow]{send_path}[/] to [yellow]{recv_path}[/]", extra={"markup": True}
          )
        except (error_perm, error_temp, OSError) as e:
          local_logger.warning(f"{self.__class__.__name__}: Failed to verify move of {send_path.name}: {e}")
          result = StatusCode.FAILURE
        getattr(file_meta, success_attr)[idx] = success

      self.pbar.update(move_files_task, advance=1)
      if items_to_log is not None:
        items_to_log[key] = result, file_meta
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception as e:
      local_logger.error(
        f"{self.__class__.__name__}: Error moving\n[yellow]{send_path}[/] to\n[yellow]{recv_path}[/]: {e}", extra={"markup": True}
      )
      if items_to_log is not None:
        items_to_log[key] = StatusCode.FAILURE, file_meta


class SupplierProcessorSFTPIntermediate(SupplierProcessorBase):
  def assemble_filename_pattern(
    self, customer_id: CustomerID, start_date: datetime, end_date: datetime, current_week: bool
  ) -> Pattern: ...

  @add_log_context(identifier_prefix=LogActionEnum.REGISTERED_PICKUP, log_subfolder=LogActionEnum.REGISTERED_PICKUP)
  async def _register_pickup(
    self,
    storenum: StoreNum,
    customer_id: CustomerID,
    pickup_date: datetime,
    dropoff_date: datetime,
    current_week: bool = True,
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger or logger
    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )

    if picked_up:
      local_logger.warning(
        f"{self.__class__.__name__}: "
        f"Attempted to register pickup for already grabbed invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      local_logger.warning(
        f"{self.__class__.__name__}: "
        f"Attempted to register pickup for already applied invoice: {self.supplier_name}, {storenum}, {customer_id}"
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
      _waiting_folder=self.pre_processing_waiting_folder,
      _local_copy_folder=self.local_pre_processing_folder,
    )

    queue_key = self.assemble_queue_key(storenum, customer_id, pickup_date)

    if items_to_log is not None:
      items_to_log[queue_key] = (StatusCode.UNKNOWN, register_data)

    # Protect queue modification with lock for consistency
    async with self._lock:
      self._file_pickup_queue[queue_key] = register_data
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
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger or logger
    key = f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

    picked_up = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_grabbed
    )
    applied = await (self.cache.schedule if current_week else self.cache.prev_week_schedule).check_toggled(
      (self.supplier_name, storenum), DatabaseScheduleColumns.invoice_applied
    )
    if not picked_up:
      local_logger.warning(
        f"{self.__class__.__name__}: {key}: "
        f"Attempted to register dropoff for not-yet picked up invoice: {self.supplier_name}, {storenum}, {customer_id}"
      )
      return
    if applied:
      local_logger.warning(
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
      try:
        matched_item = self._file_waiting_queue.pop(key)
      except KeyError:
        local_logger.error(
          f"{self.__class__.__name__}: {key}: "
          f"No waiting file found for: {self.supplier_name}, {storenum}, {customer_id}, {pickup_date.isoformat()}\n"
          f"Invoice may not have been picked up or is missing!"
        )
        return

      if items_to_log is not None:
        items_to_log[key] = (StatusCode.SUCCESS, matched_item)

      self._file_preprocess_queue[key] = matched_item
      local_logger.info(f"{self.__class__.__name__}: {key}: Registered dropoff for: {matched_item.storenum}")

  def assemble_queue_key(self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime) -> SupplierQueueKey:
    return f"{storenum}-{customer_id}-{pickup_date.isoformat()}"

  def _middle_archive_file(
    self,
    source_folder: PurePosixPath,
    remote_file: str,
    archive_folder: PurePosixPath,
    adapted_logger: Optional[LoggerAdapter] = None,
    debug: bool = False,
  ) -> None:
    local_logger = adapted_logger or logger
    try:
      source_loc = (source_folder / remote_file).as_posix()
      archive_loc = (archive_folder / remote_file).as_posix()
      with self.waiting_ftp(self.waiting_ftp_creds) as sftp_client:
        try:
          sftp_client.size(archive_loc)
          local_logger.info(
            f"{self.__class__.__name__}: Archive file already exists at [yellow]{archive_loc}[/]",
            extra={"markup": True},
          )

        except (error_perm, error_temp, OSError):
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
            sftp_client.delete(source_loc)
    except (error_perm, error_temp, OSError) as e:
      local_logger.error(f"{self.__class__.__name__}: File {remote_file} not found at {source_folder} for archiving: {e}")
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error archiving file {remote_file}: {e}")
      raise e

  def _vendor_archive_file(
    self,
    source_folder: PurePosixPath,
    remote_file: str,
    archive_folder: PurePosixPath,
    adapted_logger: Optional[LoggerAdapter] = None,
    debug: bool = False,
  ) -> None:
    local_logger = adapted_logger or logger
    try:
      source_loc = (source_folder / remote_file).as_posix()
      archive_loc = (archive_folder / remote_file).as_posix()
      with self.vendor_ftp(self.pickup_ftp_creds) as sftp_client:
        try:
          sftp_client.stat(archive_loc)
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
      local_logger.error(f"{self.__class__.__name__}: File {remote_file} not found at {source_folder} for archiving: {e}")
    # Ensure that exceptions actually get logged while executing off main thread
    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error archiving file {remote_file}: {e}")
      raise e

  @add_log_context(identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  async def _pickup_files(
    self,
    adapted_logger: Optional[LoggerAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger or logger
    if not self._file_pickup_queue:
      return
    async with self._lock:
      with self.vendor_ftp(self.pickup_ftp_creds) as sftp_client:
        remote_files = [file_attr.filename for file_attr in sftp_client.listdir_attr(self.pickup_ftp_folder.as_posix())]

      items_to_dl: dict[str, FileRegisterData] = {}
      for key, file_meta in self._file_pickup_queue.items():
        matched_files: list[Match[str]] = []

        for remote_file in remote_files:
          if match := file_meta.file_pattern.match(remote_file):
            matched_files.append(match)

        if matched_files:
          file_meta.file_names = {idx: m.string for idx, m in enumerate(matched_files)}
          items_to_dl[key] = file_meta
          if items_to_log is not None:
            items_to_log[key] = (StatusCode.UNKNOWN, file_meta)
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
              items_to_log=items_to_log,
            )
            for idx, filename in file_meta.file_names.items()
          )
        await gather(*dl_futures)

      archive_futures = []
      items_to_advance: dict[str, FileRegisterData] = {}
      for key, file_meta in items_to_dl.items():
        if all(file_meta.pickup_success.values()):
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

      await gather(*archive_futures)

    for key, item in items_to_advance.items():
      self._file_waiting_queue[key] = item
      self._file_pickup_queue.pop(key)
      local_logger.info(f"{self.__class__.__name__}: {key}: Moved {item.storenum} to waiting queue")


if __name__ == "__main__":
  from supplier_processors.sas import SASProcessor

  processor = SASProcessor()
  pass
