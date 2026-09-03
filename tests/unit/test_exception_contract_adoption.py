"""The consumer side of `aeth_ext.ftp.session.AdapterBase`'s exception contract.

Every adapter method raises stdlib `OSError` types only, so the processor classifies transfer failures and
archive refusals by type alone -- no `ftplib`/`paramiko` imports, no reply-code substrings. These pin the two
places that depended on the old protocol-specific types: the transient-transfer retry classifier and the
archive path's permission-denied branch.
"""

# This file tests private methods by design.
# pyright: reportPrivateUsage=false

# Standard library imports
import atexit
from contextlib import contextmanager
from errno import EACCES
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import TYPE_CHECKING

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.suppliers.sas import SASProcessor

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Generator


class _FakeClient:
  """`get_size` raises `size_error` if set, else answers 1; `rename` records and succeeds."""

  def __init__(self, size_error: BaseException | None = None) -> None:
    self.size_error = size_error
    self.renames: list[tuple[str, str]] = []

  def get_size(self, path: str) -> int:
    if self.size_error is not None:
      raise self.size_error
    return 1

  def rename(self, old: str, new: str) -> None:
    self.renames.append((old, new))

  def remove(self, path: str) -> None:
    pass


class _FakePool:
  def __init__(self, client: _FakeClient) -> None:
    self.client = client

  @contextmanager
  def start_session(self) -> Generator[_FakeClient]:
    yield self.client


def _drop_singleton() -> None:
  if "__shared_instance__" in SASProcessor.__dict__:
    delattr(SASProcessor, "__shared_instance__")


@pytest.fixture
def processor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[SASProcessor]:
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", SimpleNamespace)
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(SASProcessor, "_file_queue_backup_folder", tmp_path / "queue_backups")
  monkeypatch.setattr(SASProcessor, "_corrupted_queue_backup_folder", tmp_path / "queue_backups" / "corrupted")
  monkeypatch.setattr(SASProcessor, "log_file_loc", tmp_path / "logs")
  _drop_singleton()
  proc = SASProcessor()
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton()


def _chained(outer: BaseException, cause: BaseException) -> BaseException:
  """What `raise outer from cause` leaves behind -- the shape an adapter's `_translate_*_errors` produces."""
  outer.__cause__ = cause
  outer.__suppress_context__ = True
  return outer


class TestTransientTransferClassifier:
  @pytest.mark.parametrize(
    "exc",
    [
      BlockingIOError("'/p': 425 Can't open data connection"),
      BlockingIOError("'/p': 450 Requested file action not taken: file busy"),
      ConnectionAbortedError("'/p': 421 Service closing control connection"),
      ConnectionAbortedError("426 Connection closed; transfer aborted"),
      ConnectionError("'/p': malformed FTP reply"),
      TimeoutError(),
      BrokenPipeError(),
      EOFError(),
    ],
    ids=["425", "450", "421", "426", "desync", "timeout", "broken-pipe", "eof"],
  )
  def test_retries_translated_transient_types(self, processor: SASProcessor, exc: BaseException) -> None:
    assert processor._is_transient_transfer_error(exc) is True

  @pytest.mark.parametrize(
    "exc",
    [
      FileNotFoundError("'/p': 550 No such file"),
      PermissionError("'/p': 530 Not logged in"),
      OSError("'/p': 500 Syntax error"),
      OSError("'/p': 452 Insufficient storage"),
      ValueError("not an I/O failure at all"),
    ],
    ids=["550", "530", "500", "452", "value-error"],
  )
  def test_does_not_retry_definite_refusals(self, processor: SASProcessor, exc: BaseException) -> None:
    assert processor._is_transient_transfer_error(exc) is False

  def test_a_425_inside_a_path_or_size_does_not_trigger_a_retry(self, processor: SASProcessor) -> None:
    """The reply code is carried by the exception *type* now; the digits in a message mean nothing."""
    assert processor._is_transient_transfer_error(OSError("'/inv_425.txt': 500 Syntax error")) is False
    assert processor._is_transient_transfer_error(FileNotFoundError("size 425 mismatch")) is False

  def test_dial_time_ssh_rejection_is_not_retried(self, processor: SASProcessor) -> None:
    """An `SSHException` can only reach the classifier from a rejected credential or host key at dial time
    (mid-session the adapter translates it to `ConnectionError`), and retrying either is pointless.
    """
    # Third party imports
    from paramiko import AuthenticationException, SSHException

    assert processor._is_transient_transfer_error(AuthenticationException("Authentication failed.")) is False
    assert processor._is_transient_transfer_error(SSHException("Bad host key")) is False

  def test_transient_cause_beneath_a_consumer_wrapper_still_retries(self, processor: SASProcessor) -> None:
    wrapped = _chained(RuntimeError("wave failed"), BlockingIOError("'/p': 425 Can't open data connection"))
    assert processor._is_transient_transfer_error(wrapped) is True


class TestArchivePermissionDenied:
  """A `PermissionError` while archiving is logged and swallowed regardless of which adapter raised it."""

  @pytest.mark.parametrize(
    "denied",
    [
      PermissionError("'/Archive/inv.txt': 530 Not logged in"),  # FTP adapter: message only, no errno
      PermissionError(EACCES, "Permission denied"),  # SFTP adapter: paramiko sets errno
    ],
    ids=["ftp-translated", "sftp-errno"],
  )
  def test_vendor_archive_logs_and_continues(
    self, processor: SASProcessor, monkeypatch: pytest.MonkeyPatch, denied: PermissionError
  ) -> None:
    client = _FakeClient(size_error=denied)
    monkeypatch.setattr(processor, "vendor_ftp", _FakePool(client))
    processor.errored = False

    processor._vendor_archive_file(
      source_folder=PurePosixPath("/Pickup"),
      remote_file="inv.txt",
      archive_folder=PurePosixPath("/Pickup/Archive"),
      debug=False,
    )
    assert client.renames == []

  def test_vendor_archive_reraises_other_refusals(self, processor: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient(size_error=OSError("'/Archive/inv.txt': 452 Insufficient storage"))
    monkeypatch.setattr(processor, "vendor_ftp", _FakePool(client))
    processor.errored = False

    with pytest.raises(OSError, match="452"):
      processor._vendor_archive_file(
        source_folder=PurePosixPath("/Pickup"),
        remote_file="inv.txt",
        archive_folder=PurePosixPath("/Pickup/Archive"),
        debug=False,
      )

  def test_vendor_archive_renames_when_confirmed_absent(self, processor: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _FakeClient(size_error=FileNotFoundError("'/Archive/inv.txt': 550 No such file"))
    monkeypatch.setattr(processor, "vendor_ftp", _FakePool(client))
    processor.errored = False

    processor._vendor_archive_file(
      source_folder=PurePosixPath("/Pickup"),
      remote_file="inv.txt",
      archive_folder=PurePosixPath("/Pickup/Archive"),
      debug=False,
    )
    assert client.renames == [("/Pickup/inv.txt", "/Pickup/Archive/inv.txt")]
