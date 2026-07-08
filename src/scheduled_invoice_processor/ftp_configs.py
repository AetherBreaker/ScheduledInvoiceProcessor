# Standard library imports
from ftplib import FTP
from json import loads
from logging import getLogger
from socket import gaierror
from typing import override

# Third party imports
from paramiko import AutoAddPolicy, SFTPClient, SSHClient
from rich import get_console

# First party imports
from aeth_ext.ftp.errors import ServerNotAvailableError
from aeth_ext.ftp.types import FTPProtocol, ProtocolEnum, SFTPProtocol
from aeth_ext.rich.progress import Progress

# Local folder imports
from .environment_init_vars import SETTINGS

logger = getLogger(__name__)


class SFTFTPClient(FTPProtocol):
  creds = loads(SETTINGS.sft_website_creds_file.read_text())
  KIND = ProtocolEnum.FTP

  @override
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

  @override
  def close_conn_handler(self) -> None:
    self.handler.quit()


class CoremarkFTPClient(FTPProtocol):
  creds = loads(SETTINGS.coremark_ftp_creds_file.read_text())
  KIND = ProtocolEnum.FTP

  @override
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

  @override
  def close_conn_handler(self) -> None:
    self.handler.quit()


class SASSFTPClient(SFTPProtocol):
  policy = AutoAddPolicy()
  creds = loads(SETTINGS.sas_ftp_creds_file.read_text())
  KIND = ProtocolEnum.SFTP

  @override
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

  @override
  def close_conn_handler(self) -> None:
    self.handler.close()
    self.ssh_client.close()


class RYOSFTPClient(SFTPProtocol):
  policy = AutoAddPolicy()
  creds = loads(SETTINGS.ryo_ftp_creds_file.read_text())
  KIND = ProtocolEnum.SFTP

  @override
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

  @override
  def close_conn_handler(self) -> None:
    self.handler.close()
    self.ssh_client.close()


if __name__ == "__main__":
  # Standard library imports
  from contextlib import suppress
  from pathlib import PurePosixPath

  # First party imports
  from aeth_ext.ftp.adapter import AdaptedFTP, AdaptedSFTP, FTPAdapter
  from scheduled_invoice_processor.environment_init_vars import CWD

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

  with Progress(console=get_console(), auto_refresh=False) as pbar:
    ftp = FTPAdapter[AdaptedFTP](SFTFTPClient, container_cls="FTPTestContainer", pbar=pbar)
    sftp = FTPAdapter[AdaptedSFTP](SASSFTPClient, container_cls="SFTPTestContainer", pbar=pbar)
    coremark = FTPAdapter[AdaptedFTP](CoremarkFTPClient, container_cls="CoremarkTestContainer", pbar=pbar)

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
  # Standard library imports
  from shutil import rmtree

  rmtree(local_test_receiving_folder)
