# Standard library imports
from typing import TYPE_CHECKING

# Third party imports
import pytest

# First party imports
from aeth_ext.errors.exception_trail import build_exception_trail
from scheduled_invoice_processor import database
from scheduled_invoice_processor.shutdown_hooks import trail_is_database_origin

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Iterator


@pytest.fixture
def fresh_database_singleton() -> Iterator[None]:
  def _drop() -> None:
    if "__shared_instance__" in database.DatabaseCache.__dict__:
      delattr(database.DatabaseCache, "__shared_instance__")

  _drop()
  yield
  _drop()


def _is_database_origin(exc: BaseException) -> bool:
  return trail_is_database_origin(build_exception_trail(exc))


def _raise_outside_database() -> None:
  raise RuntimeError("raised in the test module, not the database layer")


def test_exception_raised_outside_database_is_not_database_origin() -> None:
  try:
    _raise_outside_database()
  except RuntimeError as exc:
    assert _is_database_origin(exc) is False
  else:  # pragma: no cover
    pytest.fail("helper did not raise")


def test_exception_raised_inside_database_module_is_database_origin(fresh_database_singleton: None) -> None:
  # DatabaseCache.__init__ calls asyncio.get_running_loop() before touching the network; outside an event loop that
  # raises RuntimeError from inside scheduled_invoice_processor.database, which is exactly the frame the trail must see.
  try:
    database.DatabaseCache()
  except RuntimeError as exc:
    assert _is_database_origin(exc) is True
  else:  # pragma: no cover
    pytest.fail("DatabaseCache() did not raise outside an event loop")


def test_chained_cause_from_database_module_counts(fresh_database_singleton: None) -> None:
  try:
    try:
      database.DatabaseCache()
    except RuntimeError as inner:
      raise ValueError("wrapped by the test") from inner
  except ValueError as exc:
    assert _is_database_origin(exc) is True
  else:  # pragma: no cover
    pytest.fail("no exception raised")


def test_exception_raised_inside_gspread_is_database_origin() -> None:
  # Third party imports
  from gspread.utils import a1_to_rowcol

  try:
    a1_to_rowcol("this is not an A1 reference")
  except Exception as exc:  # noqa: BLE001 - whatever gspread raises, its frame is what matters
    assert _is_database_origin(exc) is True
  else:  # pragma: no cover
    pytest.fail("gspread did not raise")
