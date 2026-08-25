# Standard library imports
from typing import TYPE_CHECKING

# First party imports
# Local imports
from tests.e2e import constants as C

if TYPE_CHECKING:
  # Local imports
  # First party imports
  from tests.e2e.sheet import SheetHarness


def test_seed_read_delete_roundtrip(sheet: SheetHarness):
  store, customer = C.PROBE_STORE, "99999"
  sheet.seed_orders("SAS", [(store, customer)])
  try:
    assert sheet.schedule_flags(store) == (False, False)
  finally:
    sheet.delete_orders([store])
  assert not any(r for r in sheet.log_rows([store]))  # nothing logged for a never-processed store
