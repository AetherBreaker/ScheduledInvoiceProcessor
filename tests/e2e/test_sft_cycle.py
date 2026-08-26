"""SFT warehouse cycle. Blocked on the real FTP folder paths (see the TODO_SFT banner in suppliers/sft.py)."""

# Third party imports
import pytest

pytestmark = pytest.mark.skip(reason="SFT FTP folder paths are placeholders (TODO_SFT); unskip once real paths are set")


async def test_sft_cycle() -> None:
  # When unskipping: mirror tests/e2e/test_ryo_cycle.py using only `sft_box` (the vendor side is the same
  # server), seed two .edi files built from testing_files/SFT017_13842.edi with header dates inside the
  # current week, and assert the merged `SFT017_<a>-<b>.edi` lands in `destination_ftp_folder`.
  raise AssertionError("unreachable while skipped")
