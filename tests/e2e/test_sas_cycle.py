# Standard library imports
from datetime import timedelta
from typing import TYPE_CHECKING

# Third party imports
import pytest

# First party imports
# Local imports
from tests.e2e import constants as C
from tests.e2e.cycle import assert_log_has_full_trail, run_sas_cycle
from tests.e2e.generator import now_eastern, sas_file

if TYPE_CHECKING:
  # Local imports
  # First party imports
  from tests.e2e.remote import FtpBox, SftpBox
  from tests.e2e.sheet import SheetHarness

pytestmark = pytest.mark.usefixtures("clean_remote", "reset_processor_singletons")


async def test_sas_cycle(sheet: SheetHarness, sas_box: SftpBox, sft_box: FtpBox):
  # --- arrange: one invoice file per reserved SAS order on the vendor server, one schedule row per order ---
  started = base = now_eastern()
  uploaded: dict[int, str] = {}  # store -> filename
  invoice_nums: set[str] = set()
  for i, (store, customer) in enumerate(C.SAS_CYCLE_ORDERS):
    invoice = f"{700000 + store}"
    name, content = sas_file(customer, invoice, base + timedelta(seconds=i))
    sas_box.upload(f"{C.SAS_PICKUP_DIR}/{name}", content)
    uploaded[store] = name
    invoice_nums.add(invoice)
  sheet.seed_orders("SAS", C.SAS_CYCLE_ORDERS)

  # --- act: the production cycle ---
  await run_sas_cycle()

  # --- assert: files ---
  vendor_now = sas_box.listdir(C.SAS_PICKUP_DIR)
  for name in uploaded.values():
    assert name in vendor_now, "vendor original must be untouched (__debug__ archive is simulated)"
    assert name not in sft_box.listdir(C.SFT_WAITING_SAS), f"{name} should have left /Testing/Waiting/SAS"
    assert name not in sft_box.listdir(C.SFT_PROCESSED_SAS), f"{name} should have left /Testing/Processed/SAS"
    assert name in sft_box.listdir(C.SFT_DEST_SAS), f"{name} should be in /Testing/SAS"
    assert sft_box.read(f"{C.SFT_DEST_SAS}/{name}") == sas_box.read(f"{C.SAS_PICKUP_DIR}/{name}")

  # --- assert: sheet ---
  for store, _ in C.SAS_CYCLE_ORDERS:
    assert sheet.schedule_flags(store) == (True, True), f"store {store} should be grabbed+applied"
  assert_log_has_full_trail(sheet.log_rows([s for s, _ in C.SAS_CYCLE_ORDERS], since=started), invoice_nums)
