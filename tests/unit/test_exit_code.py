# Third party imports
import pytest

# First party imports
from aeth_ext.errors.shutdown import ShutdownKind
from scheduled_invoice_processor.startup import exit_code_for_shutdown


@pytest.mark.parametrize(
  ("kind", "expected"),
  [
    (ShutdownKind.RUNNING, 0),
    (ShutdownKind.GRACEFUL, 0),
    (ShutdownKind.FATAL, 1),
    (ShutdownKind.FORCED, 1),
  ],
)
def test_exit_code_for_shutdown(kind: ShutdownKind, expected: int) -> None:
  assert exit_code_for_shutdown(kind) == expected
