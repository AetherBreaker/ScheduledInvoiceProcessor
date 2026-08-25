# Standard library imports
from asyncio import gather
from datetime import timedelta

# Third party imports
import pytest

# Local imports
from tests.e2e import constants as C
from tests.e2e.cycle import assert_log_has_full_trail, run_ryo_cycle, run_sas_cycle
from tests.e2e.generator import now_eastern, ryo_file, sas_file
from tests.e2e.remote import FtpBox, SftpBox
from tests.e2e.sheet import SheetHarness

pytestmark = pytest.mark.usefixtures("clean_remote", "reset_processor_singletons")


async def test_both_suppliers_same_process(sheet: SheetHarness, sas_box: SftpBox, ryo_box: SftpBox, sft_box: FtpBox):
  started = base = now_eastern()
  (sas_store, sas_customer), = C.BOTH_SAS_ORDERS
  (ryo_store, ryo_customer), = C.BOTH_RYO_ORDERS

  sas_name, sas_content = sas_file(sas_customer, "700003", base)
  sas_box.upload(f"{C.SAS_PICKUP_DIR}/{sas_name}", sas_content)
  ryo_names = []
  for i, invoice in enumerate(("58001", "58002")):
    name, content = ryo_file(ryo_customer, invoice, base + timedelta(seconds=i))
    ryo_box.upload(f"{C.RYO_PICKUP_DIR}/{name}", content)
    ryo_names.append(name)
  sheet.seed_orders("SAS", C.BOTH_SAS_ORDERS)
  sheet.seed_orders("RYO", C.BOTH_RYO_ORDERS)

  await gather(run_sas_cycle(), run_ryo_cycle())

  assert sas_name in sft_box.listdir(C.SFT_DEST_SAS)
  assert sas_name in sas_box.listdir(C.SAS_PICKUP_DIR)
  assert len(sft_box.listdir(C.SFT_DEST_RYO)) == 1
  assert set(ryo_names) <= set(sft_box.listdir(C.SFT_WAITING_RYO_ARCHIVE))

  assert sheet.schedule_flags(sas_store) == (True, True)
  assert sheet.schedule_flags(ryo_store) == (True, True)
  assert_log_has_full_trail(sheet.log_rows([sas_store], since=started), {"700003"})
  assert_log_has_full_trail(sheet.log_rows([ryo_store], since=started), {"58001", "58002"})
