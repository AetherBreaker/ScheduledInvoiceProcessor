from asyncio import gather, to_thread
from copy import deepcopy
from datetime import datetime
from ftplib import FTP, _SSLSocket, error_perm, error_temp  # type: ignore
from json import loads
from logging import getLogger
from pathlib import Path, PurePosixPath
from re import Pattern
from typing import Self

from aiologic import Lock
from database.cache import DatabaseCache
from environment_init_vars import CWD, SFT_WEBSITE_CREDS_FILE
from pydantic import ConfigDict, Field, TypeAdapter
from pydantic.dataclasses import dataclass
from rich_custom import CustomTaskID, ProgressCustom
from typing_custom import CustomerID, StoreNum
from typing_custom.abc import SingletonType
from typing_custom.dataframe_column_names import DatabaseScheduleColumns
from typing_custom.enums import SuppliersEnum

logger = getLogger(__name__)


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
  pickup_success: dict[int, bool] = Field(default_factory=dict)
  application_success: dict[int, bool] = Field(default_factory=dict)

  @property
  def file_loc(self) -> list[PurePosixPath]:
    return [self._waiting_folder / name for name in self.file_name]


class SupplierProcessorBase[T_VendorFTP](metaclass=SingletonType):
  vendor_ftp: T_VendorFTP

  file_pickup_queue: dict[str, FileRegisterData] = {}
  file_waiting_queue: dict[str, FileRegisterData] = {}
  file_application_queue: dict[str, FileRegisterData] = {}

  queue_ta = TypeAdapter(dict[str, FileRegisterData])

  file_queue_backup_folder: Path = CWD / "queue backups"

  queue_backup_prefix: str

  supplier_name: SuppliersEnum

  lock: Lock = Lock()

  pickup_ftp_creds: dict
  sft_ftp_creds: dict = loads(SFT_WEBSITE_CREDS_FILE.read_text())

  pickup_ftp_folder: PurePosixPath
  waiting_folder: PurePosixPath
  destination_ftp_folder: PurePosixPath

  def __init__(self, pbar: ProgressCustom = None) -> None:  # type: ignore
    self.file_queue_backup_folder.mkdir(exist_ok=True)

    self.pickup_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_pickup_queue.json"
    self.waiting_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_waiting_queue.json"
    self.application_queue_backup_file = self.file_queue_backup_folder / f"{self.queue_backup_prefix}_application_queue.json"

    self._load_queue_backups()

    self.cache: DatabaseCache = DatabaseCache()

    self.pbar = pbar

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
          self.application_queue_backup_file,
          self.queue_ta.dump_json(self.file_application_queue, indent=2, round_trip=True),
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
          self.application_queue_backup_file.read_text() if self.application_queue_backup_file.exists() else "{}"
        ),
        self.file_application_queue,
      ),
    )

    for loaded, target in to_load:
      target.clear()
      target.update(deepcopy(loaded))

  async def register_pickup(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool = True
  ) -> None: ...

  async def register_application(
    self, storenum: StoreNum, customer_id: CustomerID, pickup_date: datetime, dropoff_date: datetime, current_week: bool
  ) -> None: ...

  async def pickup_files(self) -> None: ...

  async def dropoff_files(self) -> None:
    if not self.file_application_queue:
      return
    async with self.lock:
      with self.pbar.add_task(
        "Moving files to application folder", total=sum(len(v.file_name) for v in self.file_application_queue.values())
      ) as files_move_task:
        futures = []
        for key, file_meta in tuple(self.file_application_queue.items()):
          futures.extend(
            to_thread(
              self._transfer_file_main_to_main,
              send_path=waiting_path,
              recv_path=(self.destination_ftp_folder / waiting_path.name),
              move_files_task=files_move_task,
              file_meta=file_meta,
              idx=idx,
            )
            for idx, waiting_path in enumerate(file_meta.file_loc)
          )
        await gather(*futures)

      for key, file_meta in tuple(self.file_application_queue.items()):
        if all(file_meta.application_success.values()):
          self.file_application_queue.pop(key)
          schedule = self.cache.schedule if file_meta.current_week else self.cache.prev_week_schedule

          logger.info(f"{self.__class__.__name__}: Checking off {self.supplier_name}_{file_meta.storenum} invoice_applied")
          await schedule.check_box((self.supplier_name, file_meta.storenum), DatabaseScheduleColumns.invoice_applied)

  def _transfer_file_vend_to_main(
    self, send_path: PurePosixPath, recv_path: PurePosixPath, move_files_task: CustomTaskID, file_meta: FileRegisterData, idx: int
  ):
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
                self.pbar.update(transfer_task, advance=len(buffer))
              if _SSLSocket is not None and isinstance(write_file, _SSLSocket):
                write_file.unwrap()  # type: ignore
            dest_client.voidresp()
        logger.info(
          f"{self.__class__.__name__}: Transferred SAS [yellow]{send_path}[/] to SFT FTP [yellow]{recv_path}[/]",
          extra={"markup": True},
        )

        # Verify file was transferred successfully
        success = False
        try:
          dest_client.size(recv_path.as_posix())
          success = True
        except (error_perm, error_temp, OSError) as e:
          logger.warning(f"{self.__class__.__name__}: Failed to verify transfer of {send_path.name}: {e}")
        file_meta.pickup_success[idx] = success
    self.pbar.update(move_files_task, advance=1)
    return success

  def _transfer_file_main_to_main(
    self, send_path: PurePosixPath, recv_path: PurePosixPath, move_files_task: CustomTaskID, file_meta: FileRegisterData, idx: int
  ) -> None:
    with SFTFTPClient(self.sft_ftp_creds) as origin_client:
      origin_client.voidcmd("TYPE I")
      origin_client.rename(send_path.as_posix(), recv_path.as_posix())

      # Verify file was moved successfully
      success = False
      try:
        origin_client.size(recv_path.as_posix())
        success = True
        logger.info(f"{self.__class__.__name__}: Moved [yellow]{send_path}[/] to [yellow]{recv_path}[/]", extra={"markup": True})
      except (error_perm, error_temp, OSError) as e:
        logger.warning(f"{self.__class__.__name__}: Failed to verify move of {send_path.name}: {e}")
      file_meta.application_success[idx] = success

    if self.pbar and move_files_task is not None:
      self.pbar.update(move_files_task, advance=1)


class SFTFTPClient(FTP):
  def __init__(self, creds: dict) -> None:
    self.creds = creds
    super().__init__()

  def __enter__(self) -> Self:
    self.connect(host=self.creds["HOST"], port=self.creds["PORT"])
    self.login(user=self.creds["USER"], passwd=self.creds["PWD"])
    return self
