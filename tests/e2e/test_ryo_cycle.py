# Standard library imports
from datetime import timedelta
from typing import TYPE_CHECKING

# Third party imports
import pytest

# First party imports
# Local imports
from tests.e2e import constants as C
from tests.e2e.cycle import assert_log_has_full_trail, run_ryo_cycle
from tests.e2e.generator import RYO_TIMESTAMP_FORMAT, now_eastern, ryo_file

if TYPE_CHECKING:
  # Local imports
  # First party imports
  from tests.e2e.remote import FtpBox, SftpBox
  from tests.e2e.sheet import SheetHarness

pytestmark = pytest.mark.usefixtures("clean_remote", "reset_processor_singletons")


def _parse_merged_name(name: str) -> tuple[str, set[str], str]:
  stem = name.removesuffix(".txt")
  customer, invoices, stamp = stem.split("_")
  return customer, set(invoices.split("-")), stamp


async def test_ryo_cycle(sheet: SheetHarness, ryo_box: SftpBox, sft_box: FtpBox):
  ((store, customer),) = C.RYO_CYCLE_ORDERS
  started = base = now_eastern()
  invoice_nums = {"57872", "57873"}
  originals: list[str] = []
  latest_stamp = ""
  for i, invoice in enumerate(sorted(invoice_nums)):
    at = base + timedelta(seconds=i)
    name, content = ryo_file(customer, invoice, at)
    ryo_box.upload(f"{C.RYO_PICKUP_DIR}/{name}", content)
    originals.append(name)
    latest_stamp = at.strftime(RYO_TIMESTAMP_FORMAT)
  sheet.seed_orders("RYO", C.RYO_CYCLE_ORDERS)

  await run_ryo_cycle()

  # vendor originals untouched
  assert set(originals) <= set(ryo_box.listdir(C.RYO_PICKUP_DIR))

  # originals archived on the SFT side, nothing left in waiting/processed
  assert set(originals) <= set(sft_box.listdir(C.SFT_WAITING_RYO_ARCHIVE))
  assert sft_box.listdir(C.SFT_WAITING_RYO) == []
  assert sft_box.listdir(C.SFT_PROCESSED_RYO) == []

  # exactly one merged file in the destination with the expected name parts and header
  dest = sft_box.listdir(C.SFT_DEST_RYO)
  assert len(dest) == 1, dest
  merged_customer, merged_invoices, merged_stamp = _parse_merged_name(dest[0])
  assert merged_customer == customer
  assert merged_invoices == invoice_nums
  assert merged_stamp == latest_stamp

  merged = sft_box.read(f"{C.SFT_DEST_RYO}/{dest[0]}")
  header, *body = merged.split(b"\r\n")
  h_customer, h_invoices, h_po, _h_date = header.decode().split("|")
  assert h_customer == customer
  assert set(h_invoices.split("-")) == invoice_nums
  assert h_po == "125536"
  # 3 template body lines per original file, both files merged, trailing empty element from final CRLF
  assert len([b for b in body if b]) == 6

  assert sheet.schedule_flags(store) == (True, True)
  assert_log_has_full_trail(sheet.log_rows([store], since=started), invoice_nums)
