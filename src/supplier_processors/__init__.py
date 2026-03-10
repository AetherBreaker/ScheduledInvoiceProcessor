if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from asyncio import gather, to_thread
from contextvars import ContextVar
from copy import deepcopy
from datetime import datetime
from ftplib import FTP, _SSLSocket, error_perm, error_temp  # type: ignore
from io import BytesIO
from json import loads
from logging import getLogger
from logging.handlers import QueueHandler
from pathlib import Path, PurePosixPath
from queue import Queue
from re import Pattern
from typing import Optional, Self

from aiologic import Lock
from database.cache import DatabaseCache
from environment_init_vars import CWD, SETTINGS
from logging_config import ContextAdapter, DynamicQueueListener, add_log_context
from pydantic import ConfigDict, Field, TypeAdapter
from pydantic.dataclasses import dataclass
from rich_custom import CustomTaskID, ProgressCustom
from typing_custom import CustomerID, StoreNum
from typing_custom.abc import SingletonType
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum

logger = getLogger(__name__)
contextual_logs_queue = Queue(-1)
logger.addHandler(QueueHandler(contextual_logs_queue))  # type: ignore

contextual_log_listener = DynamicQueueListener(contextual_logs_queue, respect_handler_level=True)  # type: ignore


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

  file_name: list[str] = Field(default_factory=list)
  invoice_nums: dict[int, str] = Field(default_factory=dict)
  pickup_success: dict[int, bool] = Field(default_factory=dict)
  dropoff_success: dict[int, bool] = Field(default_factory=dict)

  @property
  def file_loc(self) -> list[PurePosixPath]:
    return [self._waiting_folder / name for name in self.file_name]


class SupplierProcessorBase[T_VendorFTP](metaclass=SingletonType):
  vendor_ftp: T_VendorFTP

  file_pickup_queue: dict[str, FileRegisterData] = {}
  file_waiting_queue: dict[str, FileRegisterData] = {}
  file_dropoff_queue: dict[str, FileRegisterData] = {}

  queue_ta = TypeAdapter(dict[str, FileRegisterData])

  file_queue_backup_folder: Path = CWD / "queue backups"

  queue_backup_prefix: str

  supplier_name: SuppliersEnum

  invoice_num_pattern: Pattern[str]

  lock: Lock = Lock()

  pickup_ftp_creds: dict
  sft_ftp_creds: dict = loads(SETTINGS.sft_website_creds_file.read_text())

  pickup_ftp_folder: PurePosixPath
  waiting_folder: PurePosixPath
  destination_ftp_folder: PurePosixPath

  _identifier_prefix: str = ""
  _log_file_loc: Path = CWD / "logs"
  _ctx_var: ContextVar[str | None]

  def __init__(self, pbar: ProgressCustom = None) -> None:  # type: ignore
    self._log_file_loc.mkdir(exist_ok=True)
    self.file_queue_backup_folder.mkdir(exist_ok=True)

    self.pickup_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_pickup_queue.json"
    self.waiting_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_waiting_queue.json"
    self.dropoff_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_dropoff_queue.json"

    self.pbar = pbar

    self._load_queue_backups()

    self.cache: DatabaseCache = DatabaseCache()

  async def save_queue_backups_off_thread(self) -> None:
    await to_thread(self._save_backups)

  def _save_backups(self) -> None:
    with self.lock:
      backup = (
        (
          self.pickup_queue_backup_file,
          self.queue_ta.dump_json(self.file_pickup_queue, indent=2, round_trip=True),
        ),
        (
          self.waiting_queue_backup_file,
          self.queue_ta.dump_json(self.file_waiting_queue, indent=2, round_trip=True),
        ),
        (
          self.dropoff_queue_backup_file,
          self.queue_ta.dump_json(self.file_dropoff_queue, indent=2, round_trip=True),
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

  def __del__(self) -> None:
    self._save_backups()

  def _load_queue_backups(self) -> None:
    # Note: Called during __init__, no need for lock protection
    to_load = (
      (
        self.queue_ta.validate_json(
          self.pickup_queue_backup_file.read_text() if self.pickup_queue_backup_file.exists() else "{}"
        ),
        self.file_pickup_queue,
      ),
      (
        self.queue_ta.validate_json(
          self.waiting_queue_backup_file.read_text() if self.waiting_queue_backup_file.exists() else "{}"
        ),
        self.file_waiting_queue,
      ),
      (
        self.queue_ta.validate_json(
          self.dropoff_queue_backup_file.read_text() if self.dropoff_queue_backup_file.exists() else "{}"
        ),
        self.file_dropoff_queue,
      ),
    )

    for loaded, target in to_load:
      target.clear()
      target.update(deepcopy(loaded))

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
    adapted_logger: Optional[ContextAdapter] = None,
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
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ): ...

  @add_log_context(identifier_prefix=LogActionEnum.FILE_PICKED_UP, log_subfolder=LogActionEnum.FILE_PICKED_UP)
  async def _pickup_files(
    self,
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ): ...

  @add_log_context(identifier_prefix=LogActionEnum.FILE_DROPPED_OFF, log_subfolder=LogActionEnum.FILE_DROPPED_OFF)
  async def _dropoff_files(
    self,
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger if adapted_logger is not None else logger
    if not self.file_dropoff_queue:
      return
    async with self.lock:
      with self.pbar.add_task(
        "Moving files to dropoff folder", total=sum(len(v.file_name) for v in self.file_dropoff_queue.values())
      ) as files_move_task:
        futures = []
        for key, file_meta in tuple(self.file_dropoff_queue.items()):
          futures.extend(
            to_thread(
              self._transfer_file_main_to_main,
              send_path=waiting_path,
              recv_path=(self.destination_ftp_folder / waiting_path.name),
              move_files_task=files_move_task,
              file_meta=file_meta,
              idx=idx,
              key=key,
              adapted_logger=adapted_logger if adapted_logger is not None else None,
              items_to_log=items_to_log,
            )
            for idx, waiting_path in enumerate(file_meta.file_loc)
          )

          # Now that we are certain there are items to be moved, add them  to items_to_log immediately
          # incase an error occurs during transfer, we will still have the context of which files were being processed for logging purposes
          if items_to_log is not None:
            items_to_log[key] = StatusCode.UNKNOWN, file_meta

        await gather(*futures)

      # Now that the transfers are complete, clear the items to log

      for key, file_meta in tuple(self.file_dropoff_queue.items()):
        if all(file_meta.dropoff_success.values()):
          self.file_dropoff_queue.pop(key)
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          local_logger.info(
            f"{self.__class__.__name__}: Checking off {self.supplier_name}_{file_meta.storenum} invoice_applied"
          )
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_applied)

  def _transfer_file_vend_to_main(
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: CustomTaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict[str, tuple[StatusCode, FileRegisterData]]] = None,
  ):
    local_logger = adapted_logger if adapted_logger is not None else logger
    result = StatusCode.UNKNOWN
    try:
      transient_file = BytesIO()
      with self.vendor_ftp(self.pickup_ftp_creds) as origin_client:  # type: ignore
        file_size = origin_client.stat(send_path.as_posix()).st_size
        with SFTFTPClient(self.sft_ftp_creds) as dest_client:
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

          local_logger.info(
            f"{self.__class__.__name__}: Transferred SAS [yellow]{send_path}[/] to SFT FTP [yellow]{recv_path}[/]",
            extra={"markup": True},
          )

          # Verify file was transferred successfully
          success = False
          try:
            dest_client.size(recv_path.as_posix())
            success = True
            result = StatusCode.SUCCESS
          except (error_perm, error_temp, OSError) as e:
            local_logger.warning(f"{self.__class__.__name__}: Failed to verify transfer of {send_path.name}: {e}")
            result = StatusCode.FAILURE
          file_meta.pickup_success[idx] = success
          if items_to_log is not None:
            items_to_log[key] = result, file_meta
      # convert transient file to string
      # extract first line from transient file and apply regex pattern to extract invoice number, then store in file_meta.invoice_nums[idx]
      transient_file.seek(0)
      first_line = transient_file.readline().decode("utf-8", errors="ignore")
      if match := self.invoice_num_pattern.search(first_line):
        file_meta.invoice_nums[idx] = match.group("invoice_num")
      else:
        pass
      self.pbar.update(move_files_task, advance=1)
      return success
    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error transferring {send_path.name} to {recv_path.name}: {e}")
      if items_to_log is not None:
        items_to_log[key] = StatusCode.FAILURE, file_meta
      return False

  def _transfer_file_main_to_main(
    self,
    send_path: PurePosixPath,
    recv_path: PurePosixPath,
    move_files_task: CustomTaskID,
    file_meta: FileRegisterData,
    idx: int,
    key: str,
    adapted_logger: Optional[ContextAdapter] = None,
    items_to_log: Optional[dict] = None,
  ):
    local_logger = adapted_logger if adapted_logger is not None else logger
    result = StatusCode.UNKNOWN
    try:
      with SFTFTPClient(self.sft_ftp_creds) as origin_client:
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
        file_meta.dropoff_success[idx] = success

      self.pbar.update(move_files_task, advance=1)
      if items_to_log is not None:
        items_to_log[key] = result, file_meta
    except Exception as e:
      local_logger.error(f"{self.__class__.__name__}: Error moving {send_path.name} to {recv_path.name}: {e}")
      if items_to_log is not None:
        items_to_log[key] = StatusCode.FAILURE, file_meta


class SFTFTPClient(FTP):
  def __init__(self, creds: dict) -> None:
    self.creds = creds
    super().__init__()

  def __enter__(self) -> Self:
    self.connect(host=self.creds["HOST"], port=self.creds["PORT"])
    self.login(user=self.creds["USER"], passwd=self.creds["PWD"])
    return self


if __name__ == "__main__":
  from supplier_processors.sas import SASProcessor

  processor = SASProcessor()
  pass
