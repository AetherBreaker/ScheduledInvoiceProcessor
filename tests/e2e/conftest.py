"""Bootstraps the e2e environment.

Top-level code here runs when pytest imports this conftest, i.e. before any test module under tests/e2e is
imported. That ordering is load-bearing: scheduled_invoice_processor reads SETTINGS, the credential JSON files and
applies USE_TESTING_FOLDERS at import time.

Required environment (CI secrets, or exported locally):
  E2E_DB_KEY_JSON                 - full contents of the Google service-account key JSON
  E2E_DATABASE_ID                 - spreadsheet id of the TESTING sheet
  E2E_DATABASE_BASE_SCHEDULE_ID   - gid of the base schedule tab in that sheet
  E2E_DATABASE_ORDER_LOG_ID       - gid of the 'Processing Log' tab in that sheet
"""

# Standard library imports
import json
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

# Third party imports
import pytest

# Local imports
from tests.e2e import constants as C
from tests.e2e.remote import FtpBox, SftpBox


def _require(name: str) -> str:
  value = os.environ.get(name)
  if not value:
    raise RuntimeError(f"e2e suite needs environment variable {name} (see tests/e2e/README.md)")
  return value


def _bootstrap_environment() -> Path:
  persisted = Path(tempfile.mkdtemp(prefix="sip-e2e-persisted-"))
  secrets = persisted / "secrets"
  secrets.mkdir()

  (secrets / "db-key.json").write_text(_require("E2E_DB_KEY_JSON"))
  (secrets / "sft_creds.json").write_text(
    json.dumps({"USER": C.SFT_USER, "PWD": C.SFT_PASS, "HOST": C.SFT_HOST, "PORT": C.SFT_PORT})
  )
  (secrets / "sas_ftp_creds.json").write_text(
    json.dumps({"USER": C.SAS_USER, "PWD": C.SAS_PASS, "HOSTNAME": C.SAS_HOST, "PORT": C.SAS_PORT})
  )
  (secrets / "ryo_ftp_creds.json").write_text(
    json.dumps({"USER": C.RYO_USER, "PWD": C.RYO_PASS, "HOSTNAME": C.RYO_HOST, "PORT": C.RYO_PORT})
  )
  # Coremark has no e2e stand-in (no docker container, no constants); CoremarkFTPClient reads this file as a
  # class-body side effect at import time (scheduled_invoice_processor.ftp_configs), so it must exist even
  # though nothing in the e2e suite talks to it.
  (secrets / "coremark_ftp_creds.json").write_text(json.dumps({"USER": "unused", "PWD": "unused", "HOST": "127.0.0.1", "PORT": 0}))

  os.environ["PERSISTED_DIR_LOC"] = str(persisted)
  os.environ["USE_TESTING_FOLDERS"] = "True"
  os.environ["DATABASE_ID"] = _require("E2E_DATABASE_ID")
  os.environ["DATABASE_BASE_SCHEDULE_ID"] = _require("E2E_DATABASE_BASE_SCHEDULE_ID")
  os.environ["DATABASE_ORDER_LOG_ID"] = _require("E2E_DATABASE_ORDER_LOG_ID")
  # aeth_ext BaseSettings requires this with no default; nothing in the e2e path sends email.
  os.environ.setdefault("ALERTS_EMAIL_PWD", "e2e-dummy")
  os.environ.setdefault("ALERTS_RECIPIENTS", '["e2e@example.invalid"]')
  return persisted


PERSISTED_DIR = _bootstrap_environment()


@pytest.fixture(scope="session", autouse=True)
def e2e_env() -> Path:
  return PERSISTED_DIR


@pytest.fixture
def sft_box() -> Iterator[FtpBox]:
  with FtpBox(C.SFT_HOST, C.SFT_PORT, C.SFT_USER, C.SFT_PASS) as box:
    yield box


@pytest.fixture
def sas_box() -> Iterator[SftpBox]:
  with SftpBox(C.SAS_HOST, C.SAS_PORT, C.SAS_USER, C.SAS_PASS) as box:
    yield box


@pytest.fixture
def ryo_box() -> Iterator[SftpBox]:
  with SftpBox(C.RYO_HOST, C.RYO_PORT, C.RYO_USER, C.RYO_PASS) as box:
    yield box


@pytest.fixture(scope="session", autouse=True)
def remote_dirs() -> None:
  with FtpBox(C.SFT_HOST, C.SFT_PORT, C.SFT_USER, C.SFT_PASS) as sft:
    for folder in C.SFT_DIRS:
      sft.mkdirs(folder)
  with SftpBox(C.SAS_HOST, C.SAS_PORT, C.SAS_USER, C.SAS_PASS) as sas:
    sas.mkdirs(C.SAS_PICKUP_ARCHIVE_DIR)
  with SftpBox(C.RYO_HOST, C.RYO_PORT, C.RYO_USER, C.RYO_PASS) as ryo:
    ryo.mkdirs(C.RYO_PICKUP_ARCHIVE_DIR)


def _purge_everything() -> None:
  with FtpBox(C.SFT_HOST, C.SFT_PORT, C.SFT_USER, C.SFT_PASS) as sft:
    for folder in C.SFT_DIRS:
      sft.purge(folder)
  with SftpBox(C.SAS_HOST, C.SAS_PORT, C.SAS_USER, C.SAS_PASS) as sas:
    sas.purge(C.SAS_PICKUP_DIR)
    sas.purge(C.SAS_PICKUP_ARCHIVE_DIR)
  with SftpBox(C.RYO_HOST, C.RYO_PORT, C.RYO_USER, C.RYO_PASS) as ryo:
    ryo.purge(C.RYO_PICKUP_DIR)
    ryo.purge(C.RYO_PICKUP_ARCHIVE_DIR)


@pytest.fixture
def clean_remote(remote_dirs: None) -> Iterator[None]:
  _purge_everything()
  yield
  _purge_everything()


@pytest.fixture
def reset_processor_singletons() -> Iterator[None]:
  """Each scenario should build its processors fresh, as a new process would.

  SupplierProcessorBase uses aeth_ext's SingletonType metaclass, which caches the instance on the class as
  `__shared_instance__`. Deleting that attribute is the documented reset for that metaclass on both v6 and v8.
  """
  # First party imports
  from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  def _drop() -> None:
    for cls in (SASProcessor, RYOProcessor):
      if "__shared_instance__" in cls.__dict__:
        delattr(cls, "__shared_instance__")

  _drop()
  yield
  _drop()
