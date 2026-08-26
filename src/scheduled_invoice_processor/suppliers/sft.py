# Standard library imports
from contextvars import ContextVar
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

# Local folder imports
from . import SupplierProcessorBase

if TYPE_CHECKING:
  # Standard library imports
  from datetime import datetime
  from re import Pattern

  # First party imports
  from scheduled_invoice_processor.typing_custom import CustomerID

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
