"""The shutdown flush skips Google Sheets when the fatal error came from the database layer itself."""

# Standard library imports
from typing import TYPE_CHECKING

# Third party imports
import pytest

# First party imports
from scheduled_invoice_processor import database

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Iterator


@pytest.fixture
def fresh_database_singleton() -> Iterator[None]:
  """DatabaseCache is a SingletonType; a constructor that raised must not leave a cached instance behind."""

  def _drop() -> None:
    if "__shared_instance__" in database.DatabaseCache.__dict__:
      delattr(database.DatabaseCache, "__shared_instance__")

  _drop()
  yield
  _drop()


def _raise_outside_database() -> None:
  raise RuntimeError("raised in the test module, not the database layer")


def test_exception_raised_outside_database_is_not_database_origin() -> None:
  try:
    _raise_outside_database()
  except RuntimeError as exc:
    assert database.exception_is_database_origin(exc) is False
  else:  # pragma: no cover
    pytest.fail("helper did not raise")


def test_exception_raised_inside_database_module_is_database_origin(fresh_database_singleton: None) -> None:
  # DatabaseCache.__init__ calls asyncio.get_running_loop() before touching the network; outside an event loop that
  # raises RuntimeError from inside scheduled_invoice_processor.database, which is exactly the frame the trail must see.
  try:
    database.DatabaseCache()
  except RuntimeError as exc:
    assert database.exception_is_database_origin(exc) is True
  else:  # pragma: no cover
    pytest.fail("DatabaseCache() did not raise outside an event loop")


def test_chained_cause_from_database_module_counts(fresh_database_singleton: None) -> None:
  try:
    try:
      database.DatabaseCache()
    except RuntimeError as inner:
      raise ValueError("wrapped by the test") from inner
  except ValueError as exc:
    assert database.exception_is_database_origin(exc) is True
  else:  # pragma: no cover
    pytest.fail("no exception raised")


def test_patterns_cover_the_three_origins() -> None:
  assert database.DATABASE_ORIGIN_PATTERNS == ("scheduled_invoice_processor.database", "**.gspread.**", "**.google.oauth2.**")
