# Standard library imports
from contextvars import ContextVar
from datetime import datetime
from ftplib import all_errors
from io import BytesIO
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# Third party imports
from dateutil.relativedelta import SA, SU, relativedelta

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom.enums import StatusCode, SuppliersEnum

# Local folder imports
from . import SupplierProcessorBase

if TYPE_CHECKING:
  # Standard library imports
  from logging import Logger, LoggerAdapter
  from re import Pattern
  from typing import Any

  # First party imports
  from aeth_ext.ftp.session import AdapterBase
  from aeth_ext.rich.progress import TaskID
  from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
  from scheduled_invoice_processor.typing_custom import CustomerID

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
  pickup_archive_ftp_folder = PurePosixPath("/TODO_SFT/Pickup/Archive")
  pre_processing_waiting_folder = PurePosixPath("/TODO_SFT/Waiting")
  pre_processing_archive_folder = PurePosixPath("/TODO_SFT/Waiting/Archive")
  post_processing_waiting_folder = PurePosixPath("/TODO_SFT/Processed")
  destination_ftp_folder = PurePosixPath("/TODO_SFT/Destination")
  # ===========================================================================================================

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
