# The pools keep their credentials on a private connector; reading them back is the point of these tests.
# pyright: reportPrivateUsage=false

# Standard library imports
import importlib
import json
from typing import Any

# Third party imports
import pytest

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS


def _credentials(pool: Any) -> Any:
  return pool._connector._credentials


def test_sft_credentials_match_json() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers import SupplierProcessorBase

  raw = json.loads(SETTINGS.sft_website_creds_file.read_text())
  creds = _credentials(SupplierProcessorBase.waiting_ftp)
  assert (creds.host, creds.username, creds.port) == (raw["HOST"], raw["USER"], int(raw["PORT"]))
  assert creds.password.get_secret_value() == raw["PWD"]
  assert raw["PWD"] not in repr(creds)
  assert raw["PWD"] not in str(creds)


def test_sas_credentials_match_json() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  raw = json.loads(SETTINGS.sas_ftp_creds_file.read_text())
  creds = _credentials(SASProcessor.vendor_ftp)
  assert (creds.host, creds.username, creds.port) == (raw["HOSTNAME"], raw["USER"], int(raw.get("PORT", 22)))
  assert creds.password is not None
  assert creds.password.get_secret_value() == raw["PWD"]
  assert creds.host_key_policy == "auto_add"
  assert raw["PWD"] not in repr(creds)


def test_ryo_credentials_match_json() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers.ryo import RYOProcessor

  raw = json.loads(SETTINGS.ryo_ftp_creds_file.read_text())
  creds = _credentials(RYOProcessor.vendor_ftp)
  assert (creds.host, creds.username, creds.port) == (raw["HOSTNAME"], raw["USER"], int(raw.get("PORT", 22)))
  assert creds.password is not None
  assert creds.password.get_secret_value() == raw["PWD"]
  assert creds.host_key_policy == "auto_add"
  assert raw["PWD"] not in repr(creds)


def test_coremark_credentials_match_json() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers.coremark import load_credentials

  raw = json.loads(SETTINGS.coremark_ftp_creds_file.read_text())
  creds = load_credentials()
  assert (creds.host, creds.username, creds.port) == (raw["HOST"], raw["USER"], int(raw.get("PORT", 21)))
  assert creds.password.get_secret_value() == raw["PWD"]
  assert raw["PWD"] not in repr(creds)


def test_plaintext_credential_attributes_are_gone() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers import SupplierProcessorBase
  from scheduled_invoice_processor.suppliers.coremark import CoremarkProcessor
  from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  for cls in (SupplierProcessorBase, SASProcessor, RYOProcessor, CoremarkProcessor):
    assert not hasattr(cls, "pickup_ftp_creds")
    assert not hasattr(cls, "creds")
    assert "_raw" not in cls.__dict__, "the raw credential dict must be deleted from the class namespace"


def test_ftp_configs_module_is_gone() -> None:
  with pytest.raises(ModuleNotFoundError):
    importlib.import_module("scheduled_invoice_processor.ftp_configs")


def test_processors_expose_pools_with_settable_pbar() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers import SupplierProcessorBase
  from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  assert SASProcessor.waiting_ftp is SupplierProcessorBase.waiting_ftp
  assert RYOProcessor.waiting_ftp is SupplierProcessorBase.waiting_ftp
  assert SASProcessor.vendor_ftp is not RYOProcessor.vendor_ftp
  for pool in (SupplierProcessorBase.waiting_ftp, SASProcessor.vendor_ftp, RYOProcessor.vendor_ftp):
    assert hasattr(pool, "start_session")
    assert hasattr(pool, "test_connection")
    assert hasattr(pool, "pbar")
