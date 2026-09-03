# Standard library imports
import json
import os
import tempfile
from pathlib import Path

# Third party imports
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def _dummy_service_account_key() -> str:
  private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
  pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
  ).decode()
  return json.dumps(
    {
      "type": "service_account",
      "project_id": "unit-dummy",
      "private_key_id": "unit-dummy",
      "private_key": pem,
      "client_email": "unit-dummy@unit-dummy.iam.gserviceaccount.com",
      "client_id": "0",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/unit-dummy%40unit-dummy.iam.gserviceaccount.com",
    }
  )


def _bootstrap_environment() -> None:
  existing = os.environ.get("PERSISTED_DIR_LOC")
  if existing and Path(existing).name.startswith("sip-e2e-persisted-"):
    return
  persisted = Path(tempfile.mkdtemp(prefix="sip-unit-persisted-"))
  secrets = persisted / "secrets"
  secrets.mkdir()
  (secrets / "db-key.json").write_text(_dummy_service_account_key())
  (secrets / "sft_creds.json").write_text(json.dumps({"USER": "sft-user", "PWD": "sft-pass", "HOST": "127.0.0.1", "PORT": 2121}))
  (secrets / "sas_ftp_creds.json").write_text(
    json.dumps({"USER": "sas-user", "PWD": "sas-pass", "HOSTNAME": "127.0.0.1", "PORT": 2022})
  )
  # RYO deliberately omits PORT to exercise the default.
  (secrets / "ryo_ftp_creds.json").write_text(json.dumps({"USER": "ryo-user", "PWD": "ryo-pass", "HOSTNAME": "127.0.0.1"}))
  (secrets / "coremark_ftp_creds.json").write_text(json.dumps({"USER": "cm-user", "PWD": "cm-pass", "HOST": "127.0.0.1", "PORT": 21}))
  os.environ["PERSISTED_DIR_LOC"] = str(persisted)
  os.environ.setdefault("USE_TESTING_FOLDERS", "True")
  os.environ.setdefault("DATABASE_ID", "unit-dummy-sheet-id")
  os.environ.setdefault("DATABASE_BASE_SCHEDULE_ID", "0")
  os.environ.setdefault("DATABASE_ORDER_LOG_ID", "0")
  # aeth_ext BaseSettings requires this with no default; nothing in the unit tests sends email.
  os.environ.setdefault("ALERTS_EMAIL_PWD", "unit-dummy")
  os.environ.setdefault("ALERTS_RECIPIENTS", '["unit@example.invalid"]')


_bootstrap_environment()
