# Standard library imports
from typing import TYPE_CHECKING
from uuid import uuid4

# Third party imports
import pytest

# First party imports
# Local imports
from tests.e2e import constants as C
from tests.e2e.remote import FtpBox, SftpBox

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Callable


@pytest.mark.parametrize(
  "box_factory",
  [
    pytest.param(lambda: FtpBox(C.SFT_HOST, C.SFT_PORT, C.SFT_USER, C.SFT_PASS), id="ftp"),
    pytest.param(lambda: SftpBox(C.SAS_HOST, C.SAS_PORT, C.SAS_USER, C.SAS_PASS), id="sftp"),
  ],
)
def test_box_roundtrip(box_factory: Callable[[], FtpBox | SftpBox]):
  folder = f"/probe-{uuid4().hex[:8]}/nested"
  with box_factory() as box:
    box.mkdirs(folder)
    assert box.exists(folder)
    box.upload(f"{folder}/a.txt", b"hello\r\n")
    assert box.listdir(folder) == ["a.txt"]
    assert box.read(f"{folder}/a.txt") == b"hello\r\n"
    box.purge(folder)
    assert box.listdir(folder) == []
