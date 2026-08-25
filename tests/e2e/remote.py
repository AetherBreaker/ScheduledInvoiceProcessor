"""Direct ftplib/paramiko access to the e2e servers. Deliberately independent of aeth_ext so the
suite behaves identically across aeth_ext versions."""

# Standard library imports
from ftplib import FTP, error_perm
from io import BytesIO
from posixpath import join as pjoin
from stat import S_ISDIR
from typing import Self

# Third party imports
import paramiko


class FtpBox:
  def __init__(self, host: str, port: int, user: str, password: str) -> None:
    self._conn = (host, port, user, password)
    self.ftp: FTP

  def __enter__(self) -> Self:
    host, port, user, password = self._conn
    self.ftp = FTP()
    self.ftp.connect(host, port, timeout=30)
    self.ftp.login(user, password)
    return self

  def __exit__(self, *exc: object) -> None:
    try:
      self.ftp.quit()
    except Exception:
      self.ftp.close()

  def exists(self, path: str) -> bool:
    parent, name = path.rstrip("/").rsplit("/", 1)
    try:
      # NLST on Pure-FTPd returns full paths (e.g. "/probe-x/nested"), not basenames, so it can't
      # be compared against `name` directly. MLSD returns clean basenames and raises error_perm
      # (550) when the parent itself doesn't exist.
      return name in (entry for entry, _ in self.ftp.mlsd(parent or "/"))
    except error_perm:
      return False

  def mkdirs(self, path: str) -> None:
    current = ""
    for part in path.strip("/").split("/"):
      current = f"{current}/{part}"
      try:
        self.ftp.mkd(current)
      except error_perm as e:
        if not str(e).startswith("550"):  # 550 = already exists on Pure-FTPd
          raise

  def listdir(self, path: str) -> list[str]:
    names: list[str] = []
    for name, facts in self.ftp.mlsd(path):
      if name in (".", "..") or facts.get("type") == "dir":
        continue
      names.append(name)
    return sorted(names)

  def upload(self, path: str, data: bytes) -> None:
    self.ftp.storbinary(f"STOR {path}", BytesIO(data))

  def read(self, path: str) -> bytes:
    buf = BytesIO()
    self.ftp.retrbinary(f"RETR {path}", buf.write)
    return buf.getvalue()

  def purge(self, path: str) -> None:
    for name in self.listdir(path):
      self.ftp.delete(pjoin(path, name))


class SftpBox:
  def __init__(self, host: str, port: int, user: str, password: str) -> None:
    self._conn = (host, port, user, password)
    self.transport: paramiko.Transport
    self.sftp: paramiko.SFTPClient

  def __enter__(self) -> Self:
    host, port, user, password = self._conn
    self.transport = paramiko.Transport((host, port))
    self.transport.connect(username=user, password=password)
    client = paramiko.SFTPClient.from_transport(self.transport)
    assert client is not None
    self.sftp = client
    return self

  def __exit__(self, *exc: object) -> None:
    self.sftp.close()
    self.transport.close()

  def exists(self, path: str) -> bool:
    try:
      self.sftp.stat(path)
    except FileNotFoundError:
      return False
    return True

  def mkdirs(self, path: str) -> None:
    current = ""
    for part in path.strip("/").split("/"):
      current = f"{current}/{part}"
      if not self.exists(current):
        self.sftp.mkdir(current)

  def listdir(self, path: str) -> list[str]:
    return sorted(e.filename for e in self.sftp.listdir_attr(path) if not S_ISDIR(e.st_mode or 0))

  def upload(self, path: str, data: bytes) -> None:
    with self.sftp.open(path, "wb") as f:
      f.write(data)

  def read(self, path: str) -> bytes:
    with self.sftp.open(path, "rb") as f:
      return f.read()

  def purge(self, path: str) -> None:
    for name in self.listdir(path):
      self.sftp.remove(pjoin(path, name))


type RemoteBox = FtpBox | SftpBox
