if __name__ == "__main__":
  from logging_config import configure_logging

  configure_logging()

from abc import abstractmethod
from collections.abc import Buffer, Callable, Iterator
from contextlib import nullcontext
from datetime import datetime
from enum import Enum, auto
from ftplib import FTP, _SSLSocket, all_errors  # type: ignore
from io import BytesIO
from json import loads
from logging import getLogger
from socket import gaierror
from typing import Any, NamedTuple, Protocol, Self

from environment_init_vars import SETTINGS, TZ
from paramiko import AutoAddPolicy, SFTPClient, SFTPError, SSHClient
from rich_custom import ProgressCustom

logger = getLogger(__name__)


type BufferSize = int
type TransferSuccess = bool


class ProtocolEnum(Enum):
  FTP = auto()
  SFTP = auto()


class ListDirResult(NamedTuple):
  filename: str
  modified_time: datetime


class ServerNotAvailableError(ConnectionError):
  pass


class FTPProtocolBase(Protocol):
  KIND: ProtocolEnum

  @abstractmethod
  def get_conn_handler(self) -> Any:
    raise NotImplementedError

  @abstractmethod
  def close_conn_handler(self) -> None:
    raise NotImplementedError


class FTPProtocol(FTPProtocolBase):
  KIND = ProtocolEnum.FTP

  @abstractmethod
  def get_conn_handler(self) -> FTP:
    raise NotImplementedError


class SFTPProtocol(FTPProtocolBase):
  KIND = ProtocolEnum.SFTP

  @abstractmethod
  def get_conn_handler(self) -> SFTPClient:
    raise NotImplementedError


class AdapterProtocol(Protocol):
  def test_connection(self, logit: bool = False) -> bool:
    """Tests the connection to the FTP/SFTP server. Returns True if successful, False otherwise."""
    raise NotImplementedError

  def get_size(self, path: str) -> int | None:
    """Expects an absolute path to a file on the FTP/SFTP server and returns its size in bytes."""
    raise NotImplementedError

  def upload_file(self, remote_path: str, callback: Callable[[BufferSize], bytes], file_size: int, task_msg: str = "") -> None:
    """Expects an absolute path to a file on the FTP/SFTP server and returns a writable file-like object (e.g. socket or SFTPFile) that can be used to send the file's contents."""
    raise NotImplementedError

  def download_file(self, remote_path: str, callback: Callable[[Buffer], Any], task_msg: str = "") -> None:
    """Expects an absolute path to a file on the FTP/SFTP server and returns a readable file-like object (e.g. socket or SFTPFile) that can be used to read the file's contents."""
    raise NotImplementedError

  def transfer_file(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: "AdaptedFTP | AdaptedSFTP",
    task_msg: str = "",
    callback: Callable[[bytes], None] | None = None,
    mem_stream: BytesIO | None = None,
  ) -> TransferSuccess:
    """Transfers a file from source_remote_path to dest_remote_path on the FTP/SFTP server.
    This is intended to be used for server to server transfers that don't save the file locally."""
    raise NotImplementedError

  def rename(self, old_remote_path: str, new_remote_path: str) -> None:
    """Renames a file on the FTP/SFTP server from old_remote_path to new_remote_path."""
    raise NotImplementedError

  def remove(self, remote_path: str) -> None:
    """Removes a file on the FTP/SFTP server at the given absolute path."""
    raise NotImplementedError

  def listdir(self, path: str) -> Iterator[ListDirResult]:
    """Expects an absolute path to a directory on the FTP/SFTP server and returns an iterator of ListDirResult containing the filename and modification time of each file in the directory.
    The filename is not a full path, just the name of the file. The modification time is a datetime object representing the last modification time of the file on the server.
    Note that the modification time may be None if it cannot be determined, and in that case the tuple will not be yielded.
    """
    raise NotImplementedError


class AdaptedFTP(AdapterProtocol):
  def __init__(self, ftp_protocol: FTPProtocol, container_cls: str, pbar: ProgressCustom | None = None):
    self.proto_instance = ftp_protocol
    self.handler = None
    self.container_cls = container_cls
    self.pbar = pbar

  def __enter__(self) -> Self:
    self.handler = self.proto_instance.get_conn_handler()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.proto_instance.close_conn_handler()
    return None

  def upload_file(self, remote_path: str, callback: Callable[[BufferSize], bytes], file_size: int, task_msg: str = "") -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    try:
      self.handler.voidcmd("TYPE I")  # Set binary mode
      with self.handler.transfercmd(f"STOR {remote_path}") as conn:
        with (
          self.pbar.add_task(task_msg or f"Transferring {remote_path}", total=file_size)
          if self.pbar is not None
          else nullcontext() as transfer_task
        ):
          while buffer := callback(8192):
            conn.sendall(buffer)
            if self.pbar is not None:
              assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
              self.pbar.update(transfer_task, advance=len(buffer))
        if _SSLSocket is not None and isinstance(conn, _SSLSocket):
          conn.unwrap()  # type: ignore
    finally:
      self.handler.voidresp()

  def download_file(self, remote_path: str, callback: Callable[[Buffer], Any], task_msg: str = "") -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    try:
      self.handler.voidcmd("TYPE I")  # Set binary mode
      socket, size = self.handler.ntransfercmd(f"RETR {remote_path}")
      if size is None:
        size = self.handler.size(remote_path)
      with socket as conn:
        with (
          self.pbar.add_task(task_msg or f"Transferring {remote_path}", total=size)
          if self.pbar is not None
          else nullcontext() as transfer_task
        ):
          while data := conn.recv(8192):
            callback(data)
            if self.pbar is not None:
              assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
              self.pbar.update(transfer_task, advance=len(data))
        if _SSLSocket is not None and isinstance(conn, _SSLSocket):
          conn.unwrap()  # type: ignore
    finally:
      self.handler.voidresp()

  def transfer_file(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: "AdaptedFTP | AdaptedSFTP",
    task_msg: str = "",
    callback: Callable[[bytes], None] | None = None,
    mem_stream: BytesIO | None = None,
  ) -> TransferSuccess:
    if isinstance(other, AdaptedFTP):
      return self._ftp_to_ftp(source_remote_path, dest_remote_path, other, task_msg, callback, mem_stream)
    elif isinstance(other, AdaptedSFTP):
      return self._ftp_to_sftp(source_remote_path, dest_remote_path, other, task_msg, callback, mem_stream)
    else:
      raise ValueError(f"Unsupported other protocol: {other.__class__}")

  def _ftp_to_sftp(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: "AdaptedSFTP",
    task_msg: str = "",
    callback: Callable[[bytes], None] | None = None,
    mem_stream: BytesIO | None = None,
  ) -> TransferSuccess:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    assert other.handler is not None, "Other adapter must also be opened as a context manager"
    conn, source_file_size = self.handler.ntransfercmd(f"RETR {source_remote_path}")
    if source_file_size is None:
      try:
        source_file_size = self.handler.size(source_remote_path)
      except all_errors as e:
        logger.exception(f"{self.container_cls}: Failed to get source file size for {source_remote_path}.", exc_info=e)
        source_file_size = None
    mem_stream = mem_stream or BytesIO()
    with (
      other.handler.open(dest_remote_path, mode="wb") as dest_file,
    ):
      with (
        self.pbar.add_task(task_msg or f"Transferring {source_remote_path}", total=source_file_size)
        if self.pbar is not None
        else nullcontext() as transfer_task
      ):
        with conn as source_conn:
          while data := source_conn.recv(8192):
            if callback is not None:
              callback(data)
            dest_file.write(data)
            mem_stream.write(data)
            if self.pbar is not None:
              assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
              self.pbar.update(transfer_task, advance=len(data))
          if _SSLSocket is not None and isinstance(source_conn, _SSLSocket):
            source_conn.unwrap()  # type: ignore
        self.handler.voidresp()

      streamed_file_size = mem_stream.tell()
      try:
        dest_file_size = dest_file.tell()
      except Exception as e:
        dest_file_size = None
        logger.exception(f"{self.container_cls}: Failed to get destination file size after transfer", exc_info=e)
        return (
          source_file_size == streamed_file_size == dest_file_size
          if source_file_size is not None
          else streamed_file_size == dest_file_size
        )
    # all three file sizes should be equal
    result = (
      source_file_size == streamed_file_size == dest_file_size
      if source_file_size is not None
      else streamed_file_size == dest_file_size
    )
    if not result:
      logger.exception(
        f"{self.container_cls}: File size mismatch after transfer: {source_file_size=}, {streamed_file_size=}, {dest_file_size=}"
      )
    return result

  def _ftp_to_ftp(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: "AdaptedFTP",
    task_msg: str = "",
    callback: Callable[[bytes], None] | None = None,
    mem_stream: BytesIO | None = None,
  ) -> TransferSuccess:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    assert other.handler is not None, "Other adapter must also be opened as a context manager"
    self.handler.voidcmd("TYPE I")  # Set binary mode
    socket, source_file_size = self.handler.ntransfercmd(f"RETR {source_remote_path}")
    if source_file_size is None:
      try:
        source_file_size = self.handler.size(source_remote_path)
      except all_errors as e:
        source_file_size = None
        logger.exception(f"{self.container_cls}: Failed to get source file size.", exc_info=e)
    mem_stream = mem_stream or BytesIO()
    with (
      self.pbar.add_task(task_msg or f"Transferring {source_remote_path}", total=source_file_size)
      if self.pbar is not None
      else nullcontext() as transfer_task
    ):
      self.handler.voidcmd("TYPE I")  # Set binary mode
      other.handler.voidcmd("TYPE I")  # Set binary mode
      with (
        socket as source_conn,
        other.handler.transfercmd(f"STOR {dest_remote_path}") as dest_conn,
      ):
        while data := source_conn.recv(8192):
          if callback is not None:
            callback(data)
          dest_conn.sendall(data)
          mem_stream.write(data)
          if self.pbar is not None:
            assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
            self.pbar.update(transfer_task, advance=len(data))
        if _SSLSocket is not None:
          if isinstance(source_conn, _SSLSocket):
            source_conn.unwrap()  # type: ignore
          if isinstance(dest_conn, _SSLSocket):
            dest_conn.unwrap()  # type: ignore
      self.handler.voidresp()
      other.handler.voidresp()
    streamed_file_size = mem_stream.tell()
    try:
      dest_file_size = other.handler.size(dest_remote_path)
    except all_errors as e:
      dest_file_size = None
      logger.exception(f"{self.container_cls}: Failed to get destination file size after transfer.", exc_info=e)
      return (
        source_file_size == streamed_file_size == dest_file_size
        if source_file_size is not None
        else streamed_file_size == dest_file_size
      )
    # all three file sizes should be equal
    result = (
      source_file_size == streamed_file_size == dest_file_size
      if source_file_size is not None
      else streamed_file_size == dest_file_size
    )
    if not result:
      logger.exception(
        f"{self.container_cls}: File size mismatch after transfer: {source_file_size=}, {streamed_file_size=}, {dest_file_size=}"
      )
    return result

  def get_size(self, path: str) -> int | None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    self.handler.voidcmd("TYPE I")  # Set binary mode
    return self.handler.size(path)

  def rename(self, old_remote_path: str, new_remote_path: str) -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    self.handler.rename(old_remote_path, new_remote_path)

  def remove(self, remote_path: str) -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    self.handler.delete(remote_path)

  def listdir(self, path: str) -> Iterator[ListDirResult]:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    for entry in self.handler.mlsd(path):
      name, facts = entry
      if "modify" in facts:
        dt = datetime.strptime(facts["modify"], "%Y%m%d%H%M%S")
        new_dt = dt.replace(tzinfo=TZ)
        yield ListDirResult(filename=name, modified_time=new_dt)

  def test_connection(self, logit: bool = False) -> bool:
    try:
      with self as ftp:
        assert isinstance(ftp.handler, FTP)
        ftp.handler.voidcmd("NOOP")
      return True
    except Exception as e:
      if logit:
        logger.exception(f"{self.container_cls}: Waiting FTP server is offline: {e}")
      return False


class AdaptedSFTP(AdapterProtocol):
  def __init__(self, ftp_protocol: SFTPProtocol, container_cls: str, pbar: ProgressCustom | None = None):
    self.proto_instance = ftp_protocol
    self.handler = None
    self.container_cls = container_cls
    self.pbar = pbar

  def __enter__(self) -> Self:
    self.handler = self.proto_instance.get_conn_handler()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.proto_instance.close_conn_handler()
    return None

  def upload_file(self, remote_path: str, callback: Callable[[BufferSize], bytes], file_size: int, task_msg: str = "") -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    with self.handler.open(remote_path, mode="wb") as remote_file:
      with (
        self.pbar.add_task(task_msg or f"Transferring {remote_path}", total=file_size)
        if self.pbar is not None
        else nullcontext() as transfer_task
      ):
        while buffer := callback(8192):
          remote_file.write(buffer)
          if self.pbar is not None:
            assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
            self.pbar.update(transfer_task, advance=len(buffer))

  def download_file(self, remote_path: str, callback: Callable[[bytes], Any], task_msg: str = "") -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    with self.handler.open(remote_path, mode="rb") as remote_file:
      size = remote_file.stat().st_size
      remote_file.prefetch(size)
      with (
        self.pbar.add_task(task_msg or f"Transferring {remote_path}", total=size)
        if self.pbar is not None
        else nullcontext() as transfer_task
      ):
        while data := remote_file.read(8192):
          callback(data)
          if self.pbar is not None:
            assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
            self.pbar.update(transfer_task, advance=len(data))

  def transfer_file(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: "AdaptedSFTP | AdaptedFTP",
    task_msg: str = "",
    callback: Callable[[bytes], None] | None = None,
    mem_stream: BytesIO | None = None,
  ) -> TransferSuccess:
    if isinstance(other, AdaptedFTP):
      return self._sftp_to_ftp(source_remote_path, dest_remote_path, other, task_msg, callback, mem_stream)
    elif isinstance(other, AdaptedSFTP):
      return self._sftp_to_sftp(source_remote_path, dest_remote_path, other, task_msg, callback, mem_stream)
    else:
      raise ValueError(f"Unsupported protocol kind: {other.__class__}")

  def _sftp_to_ftp(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: AdaptedFTP,
    task_msg: str = "",
    callback: Callable[[bytes], None] | None = None,
    mem_stream: BytesIO | None = None,
  ) -> TransferSuccess:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    assert other.handler is not None, "Other adapter must also be opened as a context manager"
    try:
      source_file_size = self.handler.stat(source_remote_path).st_size
    except SFTPError as e:
      source_file_size = None
      logger.exception(f"{self.container_cls}: Failed to get source file size for {source_remote_path}.", exc_info=e)
    mem_stream = mem_stream or BytesIO()
    with (
      self.pbar.add_task(task_msg or f"Transferring {source_remote_path}", total=source_file_size)
      if self.pbar is not None
      else nullcontext() as transfer_task
    ):
      other.handler.voidcmd("TYPE I")  # Set binary mode
      with (
        other.handler.transfercmd(f"STOR {dest_remote_path}") as dest_conn,
        self.handler.open(source_remote_path, mode="rb") as source_file,
      ):
        while data := source_file.read(8192):
          if callback is not None:
            callback(data)
          dest_conn.sendall(data)
          mem_stream.write(data)
          if self.pbar is not None:
            assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
            self.pbar.update(transfer_task, advance=len(data))
        if _SSLSocket is not None and isinstance(dest_conn, _SSLSocket):
          dest_conn.unwrap()  # type: ignore
      other.handler.voidresp()
    streamed_file_size = mem_stream.tell()
    try:
      dest_file_size = other.handler.size(dest_remote_path)
    except all_errors as e:
      dest_file_size = None
      logger.exception(f"{self.container_cls}: Failed to get destination file size after transfer", exc_info=e)
      return (
        source_file_size == streamed_file_size == dest_file_size
        if source_file_size is not None
        else streamed_file_size == dest_file_size
      )
    # all three file sizes should be equal
    result = (
      source_file_size == streamed_file_size == dest_file_size
      if source_file_size is not None
      else streamed_file_size == dest_file_size
    )
    if not result:
      logger.exception(
        f"{self.container_cls}: File size mismatch after transfer: {source_file_size=}, {streamed_file_size=}, {dest_file_size=}"
      )
    return result

  def _sftp_to_sftp(
    self,
    source_remote_path: str,
    dest_remote_path: str,
    other: "AdaptedSFTP",
    task_msg: str = "",
    callback: Callable[[bytes], None] | None = None,
    mem_stream: BytesIO | None = None,
  ) -> TransferSuccess:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    assert other.handler is not None, "Other adapter must also be opened as a context manager"
    try:
      source_file_size = self.handler.stat(source_remote_path).st_size
    except SFTPError as e:
      source_file_size = None
      logger.exception(f"{self.container_cls}: Failed to get source file size for {source_remote_path}.", exc_info=e)
    mem_stream = mem_stream or BytesIO()
    with other.handler.open(dest_remote_path, mode="wb") as dest_file:
      with (
        self.pbar.add_task(task_msg or f"Transferring {source_remote_path}", total=source_file_size)
        if self.pbar is not None
        else nullcontext() as transfer_task
      ):
        with self.handler.open(source_remote_path, mode="rb") as source_file:
          while data := source_file.read(8192):
            if callback is not None:
              callback(data)
            dest_file.write(data)
            mem_stream.write(data)
            if self.pbar is not None:
              assert transfer_task is not None, "transfer_task should not be None when self.pbar is not None"
              self.pbar.update(transfer_task, advance=len(data))
      streamed_file_size = mem_stream.tell()
      try:
        dest_file_size = dest_file.tell()
      except Exception as e:
        dest_file_size = None
        logger.exception(f"{self.container_cls}: Failed to get destination file size after transfer", exc_info=e)
        return (
          source_file_size == dest_file_size == streamed_file_size
          if source_file_size is not None
          else streamed_file_size == dest_file_size
        )
    # all three file sizes should be equal
    result = (
      source_file_size == dest_file_size == streamed_file_size
      if source_file_size is not None
      else streamed_file_size == dest_file_size
    )
    if not result:
      logger.exception(
        f"{self.container_cls}: File size mismatch after transfer: {source_file_size=}, {dest_file_size=}, {streamed_file_size=}"
      )
    return result

  def get_size(self, path: str) -> int | None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    try:
      return self.handler.stat(path).st_size
    except SFTPError as e:
      logger.exception(f"{self.container_cls}: Failed to get file size for {path}", exc_info=e)
      return None

  def rename(self, old_remote_path: str, new_remote_path: str) -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    self.handler.rename(old_remote_path, new_remote_path)

  def remove(self, remote_path: str) -> None:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    self.handler.remove(remote_path)

  def listdir(self, path: str) -> Iterator[ListDirResult]:
    assert self.handler is not None, "This can only be called while the adapter is opened as a context manager"
    for entry in self.handler.listdir_iter(path):
      if entry.st_mtime is None:
        raise ValueError(f"Entry {entry.filename} does not have a modification time, cannot be used in _sftp_listdir")
      yield ListDirResult(filename=entry.filename, modified_time=datetime.fromtimestamp(entry.st_mtime, tz=TZ))

  def test_connection(self, logit: bool = False) -> bool:
    try:
      with self as sftp:
        assert isinstance(sftp.handler, SFTPClient)
        sftp.handler.listdir(".")
      return True
    except Exception as e:
      if logit:
        logger.exception(f"{self.container_cls}: Waiting SFTP server is offline: {e}")
      return False


class FTPAdapter[HandlerType_T: AdaptedFTP | AdaptedSFTP]:
  def __init__(self, ftp_protocol: type[FTPProtocol | SFTPProtocol], container_cls: str, pbar: ProgressCustom | None = None):
    self.container_cls = container_cls
    self.ftp_protocol = ftp_protocol
    self.pbar = pbar

    if issubclass(ftp_protocol, FTPProtocol):
      self.protocol_handler = AdaptedFTP
      self.ftp_protocol = ftp_protocol
    elif issubclass(ftp_protocol, SFTPProtocol):
      self.protocol_handler = AdaptedSFTP
      self.ftp_protocol = ftp_protocol
    else:
      raise ValueError(f"Unsupported protocol type: {ftp_protocol}")

  def start_session(self) -> HandlerType_T:
    return self.protocol_handler(self.ftp_protocol(), container_cls=self.container_cls, pbar=self.pbar)  # type: ignore

  def test_connection(self, logit: bool = False) -> bool:
    return self.start_session().test_connection(logit)  # type: ignore


class SFTFTPClient(FTPProtocol):
  creds = loads(SETTINGS.sft_website_creds_file.read_text())
  KIND = ProtocolEnum.FTP

  def get_conn_handler(self) -> FTP:
    try:
      self.handler = FTP()
      self.handler.connect(host=self.creds["HOST"], port=self.creds["PORT"])
      self.handler.login(user=self.creds["USER"], passwd=self.creds["PWD"])
    except ConnectionRefusedError as e:
      raise ServerNotAvailableError(
        f"Could not connect to FTP server at {self.creds['HOST']}:{self.creds['PORT']}"
        f"\n Server exists but is not running an FTP service or is blocking the connection."
      ) from e
    except TimeoutError as e:
      raise ServerNotAvailableError(
        f"Connection to FTP server at {self.creds['HOST']}:{self.creds['PORT']} timed out."
        f"\n Server may be offline or experiencing connectivity issues."
      ) from e
    except gaierror as e:
      raise ServerNotAvailableError(f"FTP server hostname {self.creds['HOST']} could not be resolved.\n DNS has likely failed") from e
    return self.handler

  def close_conn_handler(self) -> None:
    self.handler.quit()


class CoremarkFTPClient(FTPProtocol):
  creds = loads(SETTINGS.coremark_ftp_creds_file.read_text())
  KIND = ProtocolEnum.FTP

  def get_conn_handler(self) -> FTP:
    try:
      self.handler = FTP()
      self.handler.connect(host=self.creds["HOST"], port=self.creds["PORT"])
      self.handler.login(user=self.creds["USER"], passwd=self.creds["PWD"])

    except ConnectionRefusedError as e:
      raise ServerNotAvailableError(
        f"Could not connect to FTP server at {self.creds['HOST']}:{self.creds['PORT']}"
        f"\n Server exists but is not running an FTP service or is blocking the connection."
      ) from e
    except TimeoutError as e:
      raise ServerNotAvailableError(
        f"Connection to FTP server at {self.creds['HOST']}:{self.creds['PORT']} timed out."
        f"\n Server may be offline or experiencing connectivity issues."
      ) from e
    except gaierror as e:
      raise ServerNotAvailableError(f"FTP server hostname {self.creds['HOST']} could not be resolved.\n DNS has likely failed") from e

    return self.handler

  def close_conn_handler(self) -> None:
    self.handler.quit()


class SASSFTPClient(SFTPProtocol):
  policy = AutoAddPolicy()
  creds = loads(SETTINGS.sas_ftp_creds_file.read_text())
  KIND = ProtocolEnum.SFTP

  def get_conn_handler(self) -> SFTPClient:
    try:
      self.ssh_client = SSHClient()
      self.ssh_client.set_missing_host_key_policy(self.policy)

      self.ssh_client.connect(
        hostname=self.creds["HOSTNAME"],
        port=self.creds.get("PORT", 22),
        username=self.creds["USER"],
        password=self.creds["PWD"],
      )
      self.handler = self.ssh_client.open_sftp()
    except ConnectionRefusedError as e:
      raise ServerNotAvailableError(
        f"Could not connect to SFTP server at {self.creds['HOSTNAME']}:{self.creds.get('PORT', 22)}"
        f"\n Server exists but is not running an SFTP service or is blocking the connection."
      ) from e
    except TimeoutError as e:
      raise ServerNotAvailableError(
        f"Connection to SFTP server at {self.creds['HOSTNAME']}:{self.creds.get('PORT', 22)} timed out."
        f"\n Server may be offline or experiencing connectivity issues."
      ) from e
    except gaierror as e:
      raise ServerNotAvailableError(
        f"SFTP server hostname {self.creds['HOSTNAME']} could not be resolved.\n DNS has likely failed"
      ) from e

    return self.handler

  def close_conn_handler(self) -> None:
    self.handler.close()
    self.ssh_client.close()


class RYOSFTPClient(SFTPProtocol):
  policy = AutoAddPolicy()
  creds = loads(SETTINGS.ryo_ftp_creds_file.read_text())
  KIND = ProtocolEnum.SFTP

  def get_conn_handler(self) -> SFTPClient:
    try:
      self.ssh_client = SSHClient()
      self.ssh_client.set_missing_host_key_policy(self.policy)

      self.ssh_client.connect(
        hostname=self.creds["HOSTNAME"],
        port=self.creds.get("PORT", 22),
        username=self.creds["USER"],
        password=self.creds["PWD"],
      )
      self.handler = self.ssh_client.open_sftp()
    except ConnectionRefusedError as e:
      raise ServerNotAvailableError(
        f"Could not connect to FTP server at {self.creds['HOSTNAME']}:{self.creds.get('PORT', 22)}"
        f"\n Server exists but is not running an FTP service or is blocking the connection."
      ) from e
    except TimeoutError as e:
      raise ServerNotAvailableError(
        f"Connection to FTP server at {self.creds['HOSTNAME']}:{self.creds.get('PORT', 22)} timed out."
        f"\n Server may be offline or experiencing connectivity issues."
      ) from e
    except gaierror as e:
      raise ServerNotAvailableError(
        f"FTP server hostname {self.creds['HOSTNAME']} could not be resolved.\n DNS has likely failed"
      ) from e
    return self.handler

  def close_conn_handler(self) -> None:
    self.handler.close()
    self.ssh_client.close()


if __name__ == "__main__":
  from contextlib import suppress
  from pathlib import PurePosixPath

  from environment_init_vars import CWD
  from logging_config import RICH_CONSOLE
  from rich_custom import LiveCustom

  local_testing_file = CWD / "test.txt"
  local_testing_file.write_text("This is a test file for FTP/SFTP upload and download testing.\n" * 1000)

  testing_file_size = local_testing_file.stat().st_size

  local_test_receiving_folder = CWD / "test_receiving"
  local_test_receiving_folder.mkdir(exist_ok=True)
  ftp_test_folder = PurePosixPath("/Testing")
  ftp_test_rename_folder = PurePosixPath("/Testing/Sub Testing")
  sftp_test_folder = PurePosixPath("/SFT Testing")
  sftp_test_rename_folder = PurePosixPath("/SFT Testing/Sub Testing")

  local_test_receiving_file = local_test_receiving_folder / local_testing_file.name
  remote_test_file_ftp_path = ftp_test_folder / local_testing_file.name
  remote_test_file_sftp_path = sftp_test_folder / local_testing_file.name

  with LiveCustom(refresh_per_second=10, console=RICH_CONSOLE) as live:
    ftp = FTPAdapter["AdaptedFTP"](SFTFTPClient, container_cls="FTPTestContainer", pbar=live.pbar)
    sftp = FTPAdapter["AdaptedSFTP"](SASSFTPClient, container_cls="SFTPTestContainer", pbar=live.pbar)
    coremark = FTPAdapter["AdaptedFTP"](CoremarkFTPClient, container_cls="CoremarkTestContainer", pbar=live.pbar)

    # Testing Connections
    assert ftp.test_connection(logit=True), "FTP connection test failed"
    logger.info("FTP connection test succeeded")
    assert sftp.test_connection(logit=True), "SFTP connection test failed"
    logger.info("SFTP connection test succeeded")

    with ftp.start_session() as ftp_adapter, sftp.start_session() as sftp_adapter:
      # TESTING UPLOAD
      logger.info("Testing FTP Upload")
      with local_testing_file.open("rb") as f:
        ftp_adapter.upload_file(
          remote_path=remote_test_file_ftp_path.as_posix(),
          callback=f.read,
          file_size=testing_file_size,
          task_msg="Testing FTP Upload",
        )
        logger.info("FTP Upload Finished")
        f.seek(0)
        logger.info("Testing SFTP Upload")
        sftp_adapter.upload_file(
          remote_path=remote_test_file_sftp_path.as_posix(),
          callback=f.read,
          file_size=testing_file_size,
          task_msg="Testing SFTP Upload",
        )
      logger.info("SFTP Upload Finished")
      logger.info("Testing FTP get_size")
      # check file size on remote server and ensure it matches local file size
      ftp_file_size = ftp_adapter.get_size(remote_test_file_ftp_path.as_posix())
      assert ftp_file_size == testing_file_size, f"FTP file size {ftp_file_size} does not match local file size {testing_file_size}"
      logger.info("get_size test passed for FTP")
      logger.info("FTP Upload test passed\n")
      logger.info("Testing SFTP get_size")
      sftp_file_size = sftp_adapter.get_size(remote_test_file_sftp_path.as_posix())
      assert sftp_file_size == testing_file_size, f"SFTP file size {sftp_file_size} does not match local file size {testing_file_size}"
      logger.info("get_size test passed for SFTP")
      logger.info("SFTP Upload test passed\n")

      # TESTING DOWNLOAD
      logger.info("Testing FTP Download")
      with local_test_receiving_file.open("wb") as f:
        ftp_adapter.download_file(
          remote_path=remote_test_file_ftp_path.as_posix(),
          callback=f.write,
          task_msg="Testing FTP Download",
        )
      # check downloaded file size and ensure it matches remote file size
      downloaded_file_size = local_test_receiving_file.stat().st_size
      assert downloaded_file_size == ftp_file_size, (
        f"Downloaded file size {downloaded_file_size} does not match FTP file size {ftp_file_size}"
      )
      logger.info("FTP Download test passed\n")
      local_test_receiving_file.unlink()

      logger.info("Testing SFTP Download")
      with local_test_receiving_file.open("wb") as f:
        sftp_adapter.download_file(
          remote_path=remote_test_file_sftp_path.as_posix(),
          callback=f.write,
          task_msg="Testing SFTP Download",
        )
      downloaded_file_size = local_test_receiving_file.stat().st_size
      assert downloaded_file_size == sftp_file_size, (
        f"Downloaded file size {downloaded_file_size} does not match SFTP file size {sftp_file_size}"
      )
      logger.info("SFTP Download test passed\n")
      local_test_receiving_file.unlink()

      # TESTING RENAME
      logger.info("Testing FTP Rename")
      ftp_adapter.rename(remote_test_file_ftp_path.as_posix(), (ftp_test_rename_folder / remote_test_file_ftp_path.name).as_posix())
      old_check_exist = None
      new_check_exist = None
      with suppress(Exception):
        new_check_exist = ftp_adapter.get_size((ftp_test_rename_folder / remote_test_file_ftp_path.name).as_posix())
        old_check_exist = ftp_adapter.get_size(remote_test_file_ftp_path.as_posix())
      assert not bool(old_check_exist), "File still exists at old path after rename"
      assert bool(new_check_exist), "File does not exist at new path after rename"
      logger.info("FTP Rename test passed\n")

      logger.info("Testing SFTP Rename")
      sftp_adapter.rename(
        remote_test_file_sftp_path.as_posix(), (sftp_test_rename_folder / remote_test_file_sftp_path.name).as_posix()
      )
      old_check_exist = None
      new_check_exist = None
      with suppress(Exception):
        new_check_exist = sftp_adapter.get_size((sftp_test_rename_folder / remote_test_file_sftp_path.name).as_posix())
        old_check_exist = sftp_adapter.get_size(remote_test_file_sftp_path.as_posix())
      assert not bool(old_check_exist), "File still exists at old path after rename"
      assert bool(new_check_exist), "File does not exist at new path after rename"
      logger.info("SFTP Rename test passed\n")

      # TESTING LISTDIR
      logger.info("Testing FTP Listdir")
      listdir_results = list(ftp_adapter.listdir(ftp_test_rename_folder.as_posix()))
      assert any(name == remote_test_file_ftp_path.name for name, _ in listdir_results), (
        f"File {remote_test_file_ftp_path.name} not found in listdir results: {[name for name, _ in listdir_results]}"
      )
      logger.info("FTP Listdir test passed\n")
      logger.info("Testing SFTP Listdir")
      listdir_results = list(sftp_adapter.listdir(sftp_test_rename_folder.as_posix()))
      assert any(name == remote_test_file_sftp_path.name for name, _ in listdir_results), (
        f"File {remote_test_file_sftp_path.name} not found in listdir results: {[name for name, _ in listdir_results]}"
      )
      logger.info("SFTP Listdir test passed\n")

      # TESTING REMOVE
      logger.info("Testing FTP Remove")
      ftp_adapter.remove((ftp_test_rename_folder / remote_test_file_ftp_path.name).as_posix())
      check_exist = None
      with suppress(Exception):
        check_exist = ftp_adapter.get_size((ftp_test_rename_folder / remote_test_file_ftp_path.name).as_posix())
      assert not bool(check_exist), "File still exists after remove"
      logger.info("FTP Remove test passed\n")

      logger.info("Testing SFTP Remove")
      sftp_adapter.remove((sftp_test_rename_folder / remote_test_file_sftp_path.name).as_posix())
      check_exist = None
      with suppress(Exception):
        check_exist = sftp_adapter.get_size((sftp_test_rename_folder / remote_test_file_sftp_path.name).as_posix())
      assert not bool(check_exist), "File still exists after remove"
      logger.info("SFTP Remove test passed\n")

      # Testing Server to Server Transfers
      logger.info("Uploading file to SAS FTP for server to server transfer test")
      with local_testing_file.open("rb") as f:
        sftp_adapter.upload_file(
          remote_path=remote_test_file_sftp_path.as_posix(),
          callback=f.read,
          file_size=testing_file_size,
          task_msg="Uploading file to SAS FTP for server to server transfer test",
        )

      logger.info("Testing SFTP to FTP Transfer")
      transfer_result = sftp_adapter.transfer_file(
        source_remote_path=remote_test_file_sftp_path.as_posix(),
        dest_remote_path=remote_test_file_ftp_path.as_posix(),
        other=ftp_adapter,
        task_msg="Testing SFTP to FTP Transfer",
      )
      assert transfer_result, "SFTP to FTP transfer failed: file size mismatch after transfer"
      sftp_adapter.remove(remote_test_file_sftp_path.as_posix())
      ftp_adapter.remove(remote_test_file_ftp_path.as_posix())
      logger.info("SFTP to FTP transfer test passed\n")

      logger.info("Testing FTP to FTP Transfer")
      coremark_test_folder = PurePosixPath("/Sweetfire_out")
      with coremark.start_session() as coremark_adapter:
        logger.info("Uploading file to Coremark FTP for FTP to FTP transfer test")
        with local_testing_file.open("rb") as f:
          coremark_adapter.upload_file(
            remote_path=(coremark_test_folder / remote_test_file_ftp_path.name).as_posix(),
            callback=f.read,
            file_size=testing_file_size,
            task_msg="Uploading file to Coremark FTP for FTP to FTP transfer test",
          )

        transfer_result = coremark_adapter.transfer_file(
          source_remote_path=(coremark_test_folder / remote_test_file_ftp_path.name).as_posix(),
          dest_remote_path=(ftp_test_folder / remote_test_file_ftp_path.name).as_posix(),
          other=ftp_adapter,
          task_msg="Testing FTP to FTP Transfer",
        )
        assert transfer_result, "FTP to FTP transfer failed: file size mismatch after transfer"
        coremark_adapter.remove((coremark_test_folder / remote_test_file_ftp_path.name).as_posix())
        ftp_adapter.remove((ftp_test_folder / remote_test_file_ftp_path.name).as_posix())
        logger.info("FTP to FTP transfer test passed\n")

  # cleanup
  local_testing_file.unlink()
  from shutil import rmtree

  rmtree(local_test_receiving_folder)
