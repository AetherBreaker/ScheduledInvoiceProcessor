# aeth-ext v8.0.0 Migration — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `scheduled-invoice-processor` imports, runs and passes the e2e suite on `aeth-ext[sftp, async]>=8.0.0`, using v8's pooled FTP adapters with per-vendor `SecretStr` credentials, the v8 fatal-exception-trail API, and queue backups that are durable on every change.

**Architecture:** Each supplier module owns a `load_credentials()` function that turns its JSON secrets file into an `aeth_ext.ftp.credentials` value object; the processors build their pooled adapters from those at class level via `aeth_ext.ftp.create_ftp_adapter`, exactly where the old `FTPAdapter(<Client>)` lived, so every `start_session()` call site is untouched. The old `err_handling.py`/`ftp_configs.py` modules are deleted; the "did the fatal error come from the database layer?" check becomes a two-line predicate over `ExceptionTrail.matches()` in `database.py`. Queue backups are written atomically (`.tmp` + `os.replace`) at the end of every mutating block that already holds `_lock`, with an `atexit` safety net replacing `__del__`.

**Tech Stack:** Python 3.14, uv (`uv run --frozen`), pydantic (`SecretStr`), aeth_ext 8.0.0 (`aeth_ext.ftp`, `aeth_ext.errors.shutdown`, `aeth_ext.errors.exception_trail`), aiologic `Lock`, pytest (+pytest-asyncio auto mode), ruff, pyright, Docker stand-ins for the e2e suite.

**Spec:** `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md` (Phase 1 sections only; the "Phase 2 — deferred" section must NOT be implemented by this plan).

## Global Constraints

Copied from the spec's "Goal and hard constraints" plus the working agreements for this repo. Every task's requirements implicitly include this section.

- Work happens on branch `chore/update-to-aeth-ext-v8` and is pushed to **PR #10 only. The PR is never merged by the implementer or the controller** — Jacob reviews the final diff and merges himself. Never run `gh pr merge`, never use the GitHub merge tool.
- `docker/Dockerfile` is **not modified** (it stays pinned to the pre-v8 image; the `fonts-dejavu-core` block stays).
- Test code never imports `aeth_ext`. The e2e suite's assertions are not weakened (tests may only be *added* or have comments/dummy fixtures adjusted; no assertion is removed or loosened).
- Coremark is migrated mechanically (its module must import) but stays unwired and untested.
- `__debug__` semantics stay as they are (testing folders, simulated vendor archive, no-op fatal decorators under `__debug__`).
- Never print secret values. Real credentials live in `.env` and `persisted_data/secrets/*.json` — read them, never echo them into logs, reports, commits or chat.
- Repo `AetherBreaker/ScheduledInvoiceProcessor` is **public**. Do not add secrets to any tracked file. Any change to `.github/workflows/*` is batched into the last task and walked through with Jacob before it is pushed.
- Dependency pin stays `aeth-ext[sftp, async]>=8.0.0` in `pyproject.toml`; `uv.lock` is already resolved at 8.0.0 — do not run `uv lock`/`uv sync` without `--frozen`, and do not touch the `[tool.uv.sources]` block (on Windows the dev install is the editable sibling `../aeth_ext`, which is on 8.0.0).
- Code style: 2-space indentation, import blocks grouped with the repo's comment headers (`# Standard library imports`, `# Third party imports`, `# First party imports`, `# Local folder imports`); `aeth_ext` and `scheduled_invoice_processor` are first-party. Run `uv run --frozen ruff check src tests scripts` and `uv run --frozen ruff format --check src tests scripts` before every commit; run `uv run --frozen pyright src` for every task that touches `src/`.
- Phase 2 (shutdown lifecycle, retiring `sleep(600)`, `register_for_shutdown` callbacks, `run_app` exit codes) is **out of scope**. Do not pre-empt any of it.

## How to run the test suites locally

Unit tests (network-free, fast — added by this plan):

```bash
uv run --frozen pytest tests/unit -v --no-cov
```

E2E suite (needs Docker and the testing-sheet secrets; the values come from `.env` and `persisted_data/secrets/db-key.json` — never print them):

```bash
docker compose -f tests/docker/compose.yaml up -d --wait
export E2E_DB_KEY_JSON="$(cat persisted_data/secrets/db-key.json)"
export E2E_DATABASE_ID="$(grep -E '^DATABASE_ID=' .env | head -1 | cut -d= -f2- | tr -d '\r"')"
export E2E_DATABASE_BASE_SCHEDULE_ID="$(grep -E '^DATABASE_BASE_SCHEDULE_ID=' .env | head -1 | cut -d= -f2- | tr -d '\r"')"
export E2E_DATABASE_ORDER_LOG_ID="$(grep -E '^DATABASE_ORDER_LOG_ID=' .env | head -1 | cut -d= -f2- | tr -d '\r"')"
uv run --frozen pytest tests/e2e -v --no-cov
docker compose -f tests/docker/compose.yaml down -v
```

If Docker or the secrets are unavailable, say so in the task report ("e2e not run locally: <reason>") — do not fake a result. The controller runs the e2e suite (locally or via PR #10's CI) before marking a task complete.

## File structure

| File | Responsibility after Phase 1 |
|---|---|
| `src/scheduled_invoice_processor/suppliers/__init__.py` | `SupplierProcessorBase`; `load_sft_credentials()`; class-level `waiting_ftp` pool; `_persist_queues()` / `_persist_queues_at_exit()` |
| `src/scheduled_invoice_processor/suppliers/sas.py` | `load_credentials() -> SFTPCredentials`; `SASProcessor.vendor_ftp` pool |
| `src/scheduled_invoice_processor/suppliers/ryo.py` | `load_credentials() -> SFTPCredentials`; `RYOProcessor.vendor_ftp` pool; persist after the preprocess queue swap |
| `src/scheduled_invoice_processor/suppliers/coremark.py` | `load_credentials() -> FTPCredentials`; `CoremarkProcessor.vendor_ftp` pool; persist after the preprocess queue swap |
| `src/scheduled_invoice_processor/ftp_configs.py` | **deleted** |
| `src/scheduled_invoice_processor/err_handling.py` | **deleted** |
| `src/scheduled_invoice_processor/typing_custom/__init__.py` | `FatalDetails` removed |
| `src/scheduled_invoice_processor/scheduler_config.py` | `@handle_fatal_exc_sync` without arguments |
| `src/scheduled_invoice_processor/database.py` | `DATABASE_ORIGIN_PATTERNS`, `trail_is_database_origin()`, `exception_is_database_origin()` |
| `src/scheduled_invoice_processor/startup.py` | post-`await SHUTDOWN` block reads `get_current_fatal_trails()`; `save_queue_backups` cron job removed |
| `tests/unit/__init__.py`, `tests/unit/conftest.py` | network-free env bootstrap (dummy secrets in a temp `PERSISTED_DIR_LOC`) |
| `tests/unit/test_credentials.py` | credential loaders |
| `tests/unit/test_database_origin.py` | database-origin predicate |
| `tests/unit/test_queue_persistence.py` | atomic persist, at-exit paths, stale-entry cleanup persists |
| `tests/e2e/conftest.py` | comment + dummy Coremark port fixed (no assertion changes) |
| `tests/e2e/README.md` | `HOME`/`USERPROFILE` quirk removed |
| `scripts/benchmarks/dragrace_ryo.py`, `scripts/benchmarks/dragrace_before.json` | committed drag-race harness + v6.3.1 baseline |
| `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md` | after-run numbers recorded in "Drag race" |
| `.github/workflows/e2e.yml` | unit-test step (last task, walkthrough) |

---

### Task 1: Pooled FTP adapters with per-vendor credentials (B2 + C2)

**Files:**
- Delete: `src/scheduled_invoice_processor/ftp_configs.py`
- Modify: `src/scheduled_invoice_processor/suppliers/__init__.py` (imports lines 1-46, class attributes lines 78-79, `__init__` lines 109-113, `_handle_existing_archive` signature line ~849)
- Modify: `src/scheduled_invoice_processor/suppliers/sas.py` (imports lines 1-21, class lines 33-49)
- Modify: `src/scheduled_invoice_processor/suppliers/ryo.py` (imports lines 1-21, class lines 45-62)
- Modify: `src/scheduled_invoice_processor/suppliers/coremark.py` (imports lines 1-17, class lines 41-58)
- Modify: `tests/e2e/conftest.py:57-61`
- Modify: `tests/e2e/README.md` ("Local-run quirks")
- Create: `tests/unit/__init__.py`, `tests/unit/conftest.py`, `tests/unit/test_credentials.py`

**Interfaces:**
- Consumes (aeth_ext 8.0.0, verified): `aeth_ext.ftp.create_ftp_adapter(credentials, *, container_cls: str | None = None, pbar=None, ...)` returning `aeth_ext.ftp.pool.ftp_adapter.FTPAdapter` for `FTPCredentials` and `aeth_ext.ftp.pool.sftp_adapter.SFTPAdapter` for `SFTPCredentials`; `aeth_ext.ftp.credentials.FTPCredentials(host, username, password: SecretStr, port=21)`; `aeth_ext.ftp.credentials.SFTPCredentials(host, username, port=22, password: SecretStr | None, host_key_policy: "auto_add" | "reject" = "reject")`; pools expose `.pbar` (public attribute), `.start_session()`, `.test_connection(logit=False)`; session objects subclass `aeth_ext.ftp.session.AdapterBase`.
- Produces: `scheduled_invoice_processor.suppliers.load_sft_credentials() -> FTPCredentials`; `scheduled_invoice_processor.suppliers.sas.load_credentials() -> SFTPCredentials`; `scheduled_invoice_processor.suppliers.ryo.load_credentials() -> SFTPCredentials`; `scheduled_invoice_processor.suppliers.coremark.load_credentials() -> FTPCredentials`; `SupplierProcessorBase.waiting_ftp: FTPAdapter`; `SupplierProcessorBase.vendor_ftp: FTPAdapter | SFTPAdapter` (assigned, not re-annotated, in subclasses). The `tests/unit` bootstrap is reused by Tasks 2 and 4.

Secrets JSON shapes (do not change the files): `sft_creds.json` and `coremark_ftp_creds.json` have keys `HOST`, `PORT`, `USER`, `PWD`; `sas_ftp_creds.json` and `ryo_ftp_creds.json` have `HOSTNAME`, `USER`, `PWD` and optionally `PORT`.

- [ ] **Step 1: Create the unit-test bootstrap**

`tests/unit/__init__.py` — empty file.

`tests/unit/conftest.py`:

```python
"""Bootstraps a network-free environment for the unit tests.

Top-level code runs when pytest imports this conftest, before any test module under tests/unit. That ordering
is load-bearing: scheduled_invoice_processor reads SETTINGS and the credential JSON files at import time (the
supplier modules build their FTP pools at class level from those files).

When tests/e2e/conftest.py has already bootstrapped this process (PERSISTED_DIR_LOC set), that environment is
reused untouched; the unit tests never assume specific credential values, they read the JSON back.
"""

# Standard library imports
import json
import os
import tempfile
from pathlib import Path


def _bootstrap_environment() -> None:
  if os.environ.get("PERSISTED_DIR_LOC"):
    return
  persisted = Path(tempfile.mkdtemp(prefix="sip-unit-persisted-"))
  secrets = persisted / "secrets"
  secrets.mkdir()
  (secrets / "sft_creds.json").write_text(json.dumps({"USER": "sft-user", "PWD": "sft-pass", "HOST": "127.0.0.1", "PORT": 2121}))
  (secrets / "sas_ftp_creds.json").write_text(
    json.dumps({"USER": "sas-user", "PWD": "sas-pass", "HOSTNAME": "127.0.0.1", "PORT": 2022})
  )
  # RYO deliberately omits PORT to exercise the default.
  (secrets / "ryo_ftp_creds.json").write_text(json.dumps({"USER": "ryo-user", "PWD": "ryo-pass", "HOSTNAME": "127.0.0.1"}))
  (secrets / "coremark_ftp_creds.json").write_text(
    json.dumps({"USER": "cm-user", "PWD": "cm-pass", "HOST": "127.0.0.1", "PORT": 21})
  )
  os.environ["PERSISTED_DIR_LOC"] = str(persisted)
  os.environ.setdefault("USE_TESTING_FOLDERS", "True")
  os.environ.setdefault("DATABASE_ID", "unit-dummy-sheet-id")
  os.environ.setdefault("DATABASE_BASE_SCHEDULE_ID", "0")
  os.environ.setdefault("DATABASE_ORDER_LOG_ID", "0")
  # aeth_ext BaseSettings requires this with no default; nothing in the unit tests sends email.
  os.environ.setdefault("ALERTS_EMAIL_PWD", "unit-dummy")
  os.environ.setdefault("ALERTS_RECIPIENTS", '["unit@example.invalid"]')


_bootstrap_environment()
```

- [ ] **Step 2: Write the failing credential tests**

`tests/unit/test_credentials.py`:

```python
"""Credential loaders: one per vendor module, returning aeth_ext value objects with the password wrapped."""

# Standard library imports
import importlib
import json

# Third party imports
import pytest

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS


def test_sft_credentials_match_json() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers import load_sft_credentials

  raw = json.loads(SETTINGS.sft_website_creds_file.read_text())
  creds = load_sft_credentials()
  assert (creds.host, creds.username, creds.port) == (raw["HOST"], raw["USER"], int(raw["PORT"]))
  assert creds.password.get_secret_value() == raw["PWD"]
  assert raw["PWD"] not in repr(creds)
  assert raw["PWD"] not in str(creds)


def test_sas_credentials_match_json() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers.sas import load_credentials

  raw = json.loads(SETTINGS.sas_ftp_creds_file.read_text())
  creds = load_credentials()
  assert (creds.host, creds.username, creds.port) == (raw["HOSTNAME"], raw["USER"], int(raw.get("PORT", 22)))
  assert creds.password is not None
  assert creds.password.get_secret_value() == raw["PWD"]
  assert creds.host_key_policy == "auto_add"
  assert raw["PWD"] not in repr(creds)


def test_ryo_credentials_match_json() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers.ryo import load_credentials

  raw = json.loads(SETTINGS.ryo_ftp_creds_file.read_text())
  creds = load_credentials()
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run --frozen pytest tests/unit -v --no-cov`
Expected: collection/import errors — `ModuleNotFoundError: No module named 'aeth_ext.ftp.adapter'` (the current code imports the removed v6 module), and `test_ftp_configs_module_is_gone` fails because the module still exists.

- [ ] **Step 4: Rewrite the base module's imports, credentials loader and pool**

In `src/scheduled_invoice_processor/suppliers/__init__.py` replace lines 1-46 (everything up to and including the `TYPE_CHECKING` block) with:

```python
# pyright: reportImportCycles=false
# pyright: reportUninitializedInstanceVariable=false
# Standard library imports
from asyncio import gather, to_thread
from copy import deepcopy
from datetime import datetime
from errno import EACCES
from ftplib import all_errors
from io import BytesIO
from json import loads
from logging import Logger, getLogger
from time import sleep
from typing import TYPE_CHECKING, Any, ClassVar, cast

# Third party imports
from aiologic import Lock
from dateutil.relativedelta import SA, SU, relativedelta
from paramiko import SSHException
from pydantic import SecretStr, TypeAdapter

# First party imports
from aeth_ext.errors.send_alert_email import send_alert_email
from aeth_ext.ftp import create_ftp_adapter
from aeth_ext.ftp.credentials import FTPCredentials
from aeth_ext.types.abc import SingletonType
from scheduled_invoice_processor.database import DatabaseCache
from scheduled_invoice_processor.environment_init_vars import CWD, SETTINGS
from scheduled_invoice_processor.logging_config import add_log_context
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.log_action import log_actions
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode

if TYPE_CHECKING:
  # Standard library imports
  from contextvars import ContextVar
  from logging import LoggerAdapter
  from pathlib import Path, PurePosixPath
  from re import Match, Pattern

  # First party imports
  from aeth_ext.ftp.pool.ftp_adapter import FTPAdapter
  from aeth_ext.ftp.pool.sftp_adapter import SFTPAdapter
  from aeth_ext.ftp.session import AdapterBase
  from aeth_ext.rich.progress import Progress, TaskID
  from scheduled_invoice_processor.suppliers.log_action import LogActionHandlerType
  from scheduled_invoice_processor.typing_custom import CustomerID, StoreNum, SupplierQueueKey
  from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum
```

Then, directly after `HOLDING_FOLDER = CWD / "file_holding"` and before `class SupplierProcessorBase`, add:

```python
def load_sft_credentials() -> FTPCredentials:
  """Credentials for the shared SFT holding FTP server (`waiting_ftp`), read from `sft_creds.json`.

  The raw JSON never outlives this call: the password leaves here wrapped in a `SecretStr`, so nothing on the
  processor classes holds a plaintext credential.
  """
  raw = loads(SETTINGS.sft_website_creds_file.read_text())
  return FTPCredentials(host=raw["HOST"], username=raw["USER"], password=SecretStr(raw["PWD"]), port=int(raw.get("PORT", 21)))
```

Replace the two class attributes

```python
  vendor_ftp: FTPAdapter
  waiting_ftp: FTPAdapter[AdaptedFTP] = FTPAdapter(SFTFTPClient, container_cls="SupplierProcessorBase")
```

with

```python
  vendor_ftp: FTPAdapter | SFTPAdapter
  waiting_ftp: FTPAdapter = create_ftp_adapter(load_sft_credentials(), container_cls="SupplierProcessorBase")
```

In `__init__`, change the `cast` so the union is spelled out:

```python
    if pbar is not None:  # pyright: ignore[reportUnnecessaryComparison]
      self.waiting_ftp.pbar = pbar
      if vendor_ftp := cast("FTPAdapter | SFTPAdapter | None", getattr(self, "vendor_ftp", None)):
        vendor_ftp.pbar = pbar
```

In `_handle_existing_archive`, change the parameter annotation `client: AdapterProtocol,` to `client: AdapterBase,`.

Leave every `start_session()` / `test_connection()` call site as it is.

- [ ] **Step 5: Rewrite `sas.py`'s imports and pool**

Replace `sas.py` lines 1-21 with:

```python
# Standard library imports
from contextvars import ContextVar
from datetime import datetime
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# Third party imports
from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from pydantic import SecretStr

# First party imports
from aeth_ext.ftp import create_ftp_adapter
from aeth_ext.ftp.credentials import SFTPCredentials
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

# Local folder imports
from . import SupplierProcessorBase
```

Directly after `logger = getLogger(__name__)` add:

```python
def load_credentials() -> SFTPCredentials:
  """SAS SFTP credentials (Files.com), read from `sas_ftp_creds.json`; the password is wrapped in a `SecretStr`."""
  raw = loads(SETTINGS.sas_ftp_creds_file.read_text())
  return SFTPCredentials(
    host=raw["HOSTNAME"],
    username=raw["USER"],
    password=SecretStr(raw["PWD"]),
    port=int(raw.get("PORT", 22)),
    host_key_policy="auto_add",
  )
```

In the class body, replace

```python
  vendor_ftp: FTPAdapter[AdaptedSFTP] = FTPAdapter(SASSFTPClient, container_cls="SASProcessor")
```

with

```python
  vendor_ftp = create_ftp_adapter(load_credentials(), container_cls="SASProcessor")
```

and delete the line `pickup_ftp_creds: dict[str, str] = loads(SETTINGS.sas_ftp_creds_file.read_text())` (plus the blank line after it).

- [ ] **Step 6: Rewrite `ryo.py`'s imports and pool**

Replace `ryo.py` lines 1-21 with:

```python
# Standard library imports
from asyncio import as_completed, to_thread
from contextvars import ContextVar
from datetime import datetime
from hashlib import file_digest
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# Third party imports
from dateutil.relativedelta import SA, SU, relativedelta
from dateutil.rrule import DAILY, rrule
from pydantic import SecretStr

# First party imports
from aeth_ext.ftp import create_ftp_adapter
from aeth_ext.ftp.credentials import SFTPCredentials
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.logging_config import add_log_context
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum
```

(The `# Local folder imports` block that follows is unchanged.) Directly after `logger = getLogger(__name__)` add:

```python
def load_credentials() -> SFTPCredentials:
  """RYO SFTP credentials (Bitvise), read from `ryo_ftp_creds.json`; the password is wrapped in a `SecretStr`."""
  raw = loads(SETTINGS.ryo_ftp_creds_file.read_text())
  return SFTPCredentials(
    host=raw["HOSTNAME"],
    username=raw["USER"],
    password=SecretStr(raw["PWD"]),
    port=int(raw.get("PORT", 22)),
    host_key_policy="auto_add",
  )
```

In the class body replace `vendor_ftp: FTPAdapter[AdaptedSFTP] = FTPAdapter(RYOSFTPClient, container_cls="RYOProcessor")` with `vendor_ftp = create_ftp_adapter(load_credentials(), container_cls="RYOProcessor")`, and delete `pickup_ftp_creds: dict[str, str] = loads(SETTINGS.ryo_ftp_creds_file.read_text())` (plus its trailing blank line).

- [ ] **Step 7: Rewrite `coremark.py`'s imports and pool**

Replace `coremark.py` lines 1-17 with:

```python
# Standard library imports
from asyncio import as_completed, to_thread
from contextvars import ContextVar
from datetime import datetime
from hashlib import file_digest
from json import loads
from logging import getLogger
from pathlib import PurePosixPath
from re import compile
from typing import TYPE_CHECKING, override

# Third party imports
from pydantic import SecretStr

# First party imports
from aeth_ext.ftp import create_ftp_adapter
from aeth_ext.ftp.credentials import FTPCredentials
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.logging_config import add_log_context
from scheduled_invoice_processor.typing_custom.enums import LogActionEnum, StatusCode, SuppliersEnum
```

Directly after `logger = getLogger(__name__)` add:

```python
def load_credentials() -> FTPCredentials:
  """Coremark FTP credentials (IIS FTP), read from `coremark_ftp_creds.json`; the password is wrapped in a `SecretStr`."""
  raw = loads(SETTINGS.coremark_ftp_creds_file.read_text())
  return FTPCredentials(host=raw["HOST"], username=raw["USER"], password=SecretStr(raw["PWD"]), port=int(raw.get("PORT", 21)))
```

In the class body replace `vendor_ftp: FTPAdapter[AdaptedFTP] = FTPAdapter(CoremarkFTPClient, container_cls="CoremarkProcessor")` with `vendor_ftp = create_ftp_adapter(load_credentials(), container_cls="CoremarkProcessor")`, and delete `pickup_ftp_creds: dict[str, str] = loads(SETTINGS.coremark_ftp_creds_file.read_text())` (plus its trailing blank line).

- [ ] **Step 8: Delete `ftp_configs.py`**

```bash
git rm src/scheduled_invoice_processor/ftp_configs.py
```

Then confirm nothing references it: `uv run --frozen ruff check src` must report no undefined names, and `grep -rn "ftp_configs\|aeth_ext.ftp.adapter\|aeth_ext.ftp.types\|pickup_ftp_creds" src tests` must return only the `tests/e2e/conftest.py` comment fixed in the next step.

- [ ] **Step 9: Fix the e2e conftest's Coremark dummy (v8 validates `port > 0`)**

In `tests/e2e/conftest.py` replace lines 57-61 with:

```python
  # Coremark has no e2e stand-in (no docker container, no constants); suppliers/coremark.py loads this file at
  # import time to build its FTP pool, so it must exist even though nothing in the e2e suite talks to it. The
  # pool is lazy (no connection until a session starts), but the credentials object validates 1 <= PORT <= 65535.
  (secrets / "coremark_ftp_creds.json").write_text(json.dumps({"USER": "unused", "PWD": "unused", "HOST": "127.0.0.1", "PORT": 21}))
```

Also scope the e2e skip hook to e2e items, so `pytest tests` without the `E2E_*` environment skips only the e2e suite and still runs the unit tests. In the same file, change the loop at the end of `pytest_collection_modifyitems` from

```python
  reason = "e2e suite needs E2E_* environment (see tests/e2e/README.md)"
  for item in items:
    item.add_marker(pytest.mark.skip(reason=reason))
```

to

```python
  reason = "e2e suite needs E2E_* environment (see tests/e2e/README.md)"
  e2e_dir = Path(__file__).parent
  for item in items:
    if item.path.is_relative_to(e2e_dir):
      item.add_marker(pytest.mark.skip(reason=reason))
```

(`Path` is already imported in that conftest.) These are the only e2e test-code changes in the task; no assertion changes.

- [ ] **Step 10: Run the unit tests, ruff, ruff format and pyright**

Run: `uv run --frozen pytest tests/unit -v --no-cov`
Expected: all 8 tests PASS.

Run: `uv run --frozen ruff check src tests && uv run --frozen ruff format --check src tests && uv run --frozen pyright src`
Expected: no errors. If pyright complains about `vendor_ftp` assignment in a subclass, the base annotation must read `vendor_ftp: FTPAdapter | SFTPAdapter` (a union, not a `ClassVar`) and the subclasses must *assign* without re-annotating.

- [ ] **Step 11: Remove the `HOME`/`USERPROFILE` workaround from the e2e README**

In `tests/e2e/README.md`, under "## Local-run quirks", delete the first bullet (the one starting "The app's paramiko client auto-loads `~/.ssh` keys" through "CI runners have no keys and don't need this.") entirely. Keep the other three bullets. v8's SFTP connector uses `look_for_keys=False, allow_agent=False`, so the workaround is obsolete.

- [ ] **Step 12: Run the e2e suite (no `HOME` override)**

Use the "How to run the test suites locally" block at the top of this plan (Docker + secrets). Expected: `10 passed`. This proves the pooled adapters against Pure-FTPd and SFTPGo and that `~/.ssh` keys are no longer auto-loaded. If it cannot be run locally, report "e2e not run locally: <reason>" and the controller runs it.

- [ ] **Step 13: Commit**

```bash
git add -A src/scheduled_invoice_processor tests/unit tests/e2e/conftest.py tests/e2e/README.md
git commit -m "refactor(ftp): pooled v8 adapters with per-vendor SecretStr credentials; drop ftp_configs.py"
```

---

### Task 2: Minimal fatal-trail adoption (A4) — decorator, `err_handling.py` removal, inline origin check

**Files:**
- Modify: `src/scheduled_invoice_processor/scheduler_config.py:19-24,122`
- Delete: `src/scheduled_invoice_processor/err_handling.py`
- Modify: `src/scheduled_invoice_processor/typing_custom/__init__.py:66-69` (remove `FatalDetails`)
- Modify: `src/scheduled_invoice_processor/database.py` (imports; new constants/functions after `DEFAULT_SCOPES`)
- Modify: `src/scheduled_invoice_processor/startup.py:21,366-387`
- Create: `tests/unit/test_database_origin.py`

**Interfaces:**
- Consumes (aeth_ext 8.0.0, verified): `aeth_ext.errors.err_handling.handle_fatal_exc_sync` (decorator, **no parameters**, no-op under `__debug__` unless the module is `__main__`); `aeth_ext.errors.shutdown.get_current_fatal_trails() -> tuple[ExceptionTrail, ...]`; `aeth_ext.errors.exception_trail.build_exception_trail(exc) -> ExceptionTrail`; `ExceptionTrail.matches(*patterns) -> tuple[TrailEntry, ...]` (empty tuple is falsy; patterns are dot-segment globs where `*` = one segment, `**` = zero or more); `ExceptionTrail.origin: TrailEntry(module: str, category, file: str)`.
- Produces: `scheduled_invoice_processor.database.DATABASE_ORIGIN_PATTERNS: tuple[str, ...]`; `scheduled_invoice_processor.database.trail_is_database_origin(trail: ExceptionTrail) -> bool`; `scheduled_invoice_processor.database.exception_is_database_origin(exc: BaseException) -> bool`.

**Ruling recorded for reviewers:** the spec says the check is inlined in `main()` and may be "extracted into a tiny private helper in `startup.py` only if needed for testability". `startup.py` opens network connections at import (`supplier_register` calls `check_connections()`), so a unit test cannot import it. The helper therefore lives in `database.py` — the module whose origin it describes — and `main()` calls it inline. `exception_is_database_origin` exists so the unit test can feed raw exceptions without importing `aeth_ext` (Global Constraint). No new module is created (Jacob's "no module for one function" rule).

- [ ] **Step 1: Write the failing test**

`tests/unit/test_database_origin.py`:

```python
"""The shutdown flush skips Google Sheets when the fatal error came from the database layer itself."""

# Standard library imports
from collections.abc import Iterator

# Third party imports
import pytest

# First party imports
from scheduled_invoice_processor import database


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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --frozen pytest tests/unit/test_database_origin.py -v --no-cov`
Expected: FAIL with `AttributeError: module 'scheduled_invoice_processor.database' has no attribute 'exception_is_database_origin'` (and `DATABASE_ORIGIN_PATTERNS`).

- [ ] **Step 3: Add the predicate to `database.py`**

Add to the first-party imports of `src/scheduled_invoice_processor/database.py`:

```python
from aeth_ext.errors.exception_trail import build_exception_trail
```

and, inside the existing `if TYPE_CHECKING:` block of that file, `from aeth_ext.errors.exception_trail import ExceptionTrail` (if the module has no `TYPE_CHECKING` block, add `from typing import TYPE_CHECKING` to the stdlib imports and create one after the imports).

Directly after `DEFAULT_SCOPES = [...]` add:

```python
DATABASE_ORIGIN_PATTERNS: tuple[str, ...] = ("scheduled_invoice_processor.database", "**.gspread.**", "**.google.oauth2.**")
"""Dot-segment globs (see `ExceptionTrail.matches`) for "the fatal error came from the Google Sheets layer": this
module, gspread, or the google-auth credentials stack. A fatal error from any of these means a final flush of queued
writes at shutdown would only fail again, so `startup.main()` skips it."""


def trail_is_database_origin(trail: ExceptionTrail) -> bool:
  """Whether any module on *trail* (origin-first, causes/contexts included) is part of the database layer."""
  return bool(trail.matches(*DATABASE_ORIGIN_PATTERNS))


def exception_is_database_origin(exc: BaseException) -> bool:
  """`trail_is_database_origin` over a raised exception (must have a live `__traceback__`)."""
  return trail_is_database_origin(build_exception_trail(exc))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run --frozen pytest tests/unit/test_database_origin.py -v --no-cov`
Expected: 4 PASS.

- [ ] **Step 5: Fix the decorator in `scheduler_config.py`**

Delete the line `from .err_handling import extract_exc_details` (line 24) and, if the `# Local folder imports` block is now only `from .environment_init_vars import SETTINGS`, leave that line in place. Change line 122 from

```python
    @handle_fatal_exc_sync(extract_details_callable=extract_exc_details)
```

to

```python
    @handle_fatal_exc_sync
```

- [ ] **Step 6: Delete `err_handling.py` and `FatalDetails`**

```bash
git rm src/scheduled_invoice_processor/err_handling.py
```

In `src/scheduled_invoice_processor/typing_custom/__init__.py` delete the `class FatalDetails(TypedDict):` block (lines 66-69, including its three fields) and the blank lines that separated it. If `TypedDict` is now unused in that file, remove it from the `typing` import; ruff will tell you.

- [ ] **Step 7: Substitute the origin check in `startup.main()`**

In `src/scheduled_invoice_processor/startup.py` replace line 21

```python
from scheduled_invoice_processor.err_handling import get_last_fatal_details
```

with (keeping alphabetical order inside the first-party block):

```python
from aeth_ext.errors.shutdown import SHUTDOWN, get_current_fatal_trails
```

(merge into the existing `from aeth_ext.errors.shutdown import SHUTDOWN` line) and

```python
from scheduled_invoice_processor.database import DatabaseCache, trail_is_database_origin
```

(merge into the existing `DatabaseCache` import line).

Replace the block from `fatal_details = get_last_fatal_details()` (line 366) through the end of the `else:` flush branch (line 395) with:

```python
      database_origin_trail = next((trail for trail in get_current_fatal_trails() if trail_is_database_origin(trail)), None)

      if database_origin_trail is None and any(processor().errored for processor in supplier_register.values()):
        await sleep(
          600
        )  # Sleep for 10 minutes to allow pending operations from non-error-state processors to flush through before exiting

      try:
        logger.warning("Fatal shutdown: stopping scheduler to freeze application state")
        scheduler.pause()
        scheduler.shutdown(wait=False)
      except Exception:
        logger.exception("Fatal shutdown: failed to stop scheduler cleanly")

      # Re-read: another fatal error may have arrived during the sleep above.
      database_origin_trail = next((trail for trail in get_current_fatal_trails() if trail_is_database_origin(trail)), None)

      if database_origin_trail is not None:
        logger.warning(
          "Fatal shutdown: skipping final Google Sheets flush because fatal error originated in database interface (origin=%s in %s)",
          database_origin_trail.origin.module,
          database_origin_trail.origin.file,
        )
      else:
        try:
          if await cache.has_pending_writes():
            logger.warning("Fatal shutdown: attempting final Google Sheets flush of queued writes")
            await cache.submit_queued_writes_to_pool()
            logger.warning("Fatal shutdown: final Google Sheets flush completed")
        except Exception:
          logger.exception("Fatal shutdown: final Google Sheets flush failed")
```

The `sys.exit(1)` line after it stays. Nothing else in `main()` changes (the `sleep(600)` heuristic is a Phase 2 decision — leave it).

- [ ] **Step 8: Verify the app imports and the tooling is clean**

Run: `grep -rn "err_handling\|FatalDetails\|get_last_fatal_details\|extract_exc_details" src tests` — expected: only `from aeth_ext.errors.err_handling import handle_fatal_exc_sync` in `scheduler_config.py`.

Run (from the repo root, where the developer's normal `.env` and `persisted_data/` are in place — `scheduler_config` is not imported by the unit tests, so this is the import check for it): `uv run --frozen python -c "import scheduled_invoice_processor.scheduler_config, scheduled_invoice_processor.database; print('ok')"`
Expected: `ok`. (No network is touched: `scheduler_config` only builds the scheduler class; `startup` is deliberately not imported.)

Run: `uv run --frozen pytest tests/unit -v --no-cov && uv run --frozen ruff check src tests && uv run --frozen ruff format --check src tests && uv run --frozen pyright src`
Expected: all pass, no errors.

- [ ] **Step 9: Run the e2e suite**

Same command block as Task 1 Step 12. Expected: `10 passed`. Report "not run locally: <reason>" if it cannot run.

- [ ] **Step 10: Commit**

```bash
git add -A src/scheduled_invoice_processor tests/unit/test_database_origin.py
git commit -m "refactor(errors): adopt v8 fatal trails; drop err_handling.py and FatalDetails; inline database-origin check in main()"
```

---

### Task 3: Queue state durable on every change (A2) with an `atexit` safety net

**Files:**
- Modify: `src/scheduled_invoice_processor/suppliers/__init__.py` (imports; `__init__`; delete `__del__`, `save_queue_backups_off_thread`, `_save_backups`; add `_persist_queues`, `_persist_queues_at_exit`; call sites in `clean_stale_queue_entries`, `_preprocess_files`, `_dropoff_files`, `_pickup_files`, `_register_pickup`, `_register_dropoff`)
- Modify: `src/scheduled_invoice_processor/suppliers/ryo.py` (`_preprocess_off_thread`, after the queue swap ~line 222)
- Modify: `src/scheduled_invoice_processor/suppliers/coremark.py` (`_preprocess_off_thread`, after the queue swap ~line 186)
- Modify: `src/scheduled_invoice_processor/startup.py:104-109` (remove the `save_queue_backups` cron job)
- Create: `tests/unit/test_queue_persistence.py`

**Interfaces:**
- Consumes: `SupplierProcessorBase._lock: aiologic.Lock` (sync API: `green_acquire(*, blocking=True, timeout: float | None = None) -> bool`, `green_release()`; `with self._lock:` and `async with self._lock:` both work); `SupplierProcessorBase._queue_ta: TypeAdapter[dict[str, FileRegisterData]]`; the four `*_queue_backup_file: Path` instance attributes set in `__init__`; `FileRegisterData(storenum: int, customer_id: str, pickup_date, dropoff_date, file_pattern: re.Pattern[str], _current_week: bool, _waiting_folder: PurePosixPath, _local_copy_folder: Path)`.
- Produces: `SupplierProcessorBase._persist_queues() -> None` (sync, **does not take the lock**; callers hold `_lock` or are the at-exit path); `SupplierProcessorBase._persist_queues_at_exit() -> None`.

Locking model, for the implementer: every queue mutation in the codebase happens either inside an `async with self._lock:` block on the event-loop thread, or inside `_preprocess_off_thread`, which runs in a worker thread *while the event-loop caller holds the lock*. `_persist_queues` therefore never acquires the lock itself. `_load_queue_backups` is unchanged and reads only the exact backup paths, so a leftover `.tmp` file is ignored.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_queue_persistence.py`:

```python
"""Queue backups are written atomically on every change, and once more at interpreter exit."""

# Standard library imports
import atexit
import json
import logging
import re
import threading
from collections.abc import Iterator
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any

# Third party imports
import pytest

# First party imports
import scheduled_invoice_processor.suppliers as suppliers_mod
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers.file_register_data import FileRegisterData
from scheduled_invoice_processor.suppliers.sas import SASProcessor


def _drop_singleton() -> None:
  if "__shared_instance__" in SASProcessor.__dict__:
    delattr(SASProcessor, "__shared_instance__")


@pytest.fixture
def backup_dir(tmp_path: Path) -> Path:
  return tmp_path / "queue_backups"


@pytest.fixture
def processor(tmp_path: Path, backup_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[SASProcessor]:
  """A real SASProcessor with its filesystem redirected to tmp_path and its DatabaseCache stubbed (no network)."""
  monkeypatch.setattr(suppliers_mod, "DatabaseCache", lambda: SimpleNamespace())
  monkeypatch.setattr(suppliers_mod, "HOLDING_FOLDER", tmp_path / "file_holding")
  monkeypatch.setattr(SASProcessor, "_file_queue_backup_folder", backup_dir)
  monkeypatch.setattr(SASProcessor, "_corrupted_queue_backup_folder", backup_dir / "corrupted")
  monkeypatch.setattr(SASProcessor, "log_file_loc", tmp_path / "logs")
  _drop_singleton()
  proc = SASProcessor()
  yield proc
  atexit.unregister(proc._persist_queues_at_exit)
  _drop_singleton()


def _entry(days_from_now: int = 7) -> FileRegisterData:
  now = datetime.now(SETTINGS.tz)
  return FileRegisterData(
    storenum=9001,
    customer_id="900100",
    pickup_date=now + timedelta(days=days_from_now),
    dropoff_date=now + timedelta(days=days_from_now + 1),
    file_pattern=re.compile(r"^unit-test-.*\.txt$"),
    _current_week=True,
    _waiting_folder=PurePosixPath("/Waiting/SAS"),
    _local_copy_folder=Path("unit-test-local"),
  )


def _read(path: Path) -> dict[str, Any]:
  return json.loads(path.read_text())


def test_persist_writes_all_four_queues_and_leaves_no_tmp(processor: SASProcessor, backup_dir: Path) -> None:
  processor._file_pickup_queue["p"] = _entry()
  processor._file_waiting_queue["w"] = _entry()
  processor._file_preprocess_queue["pp"] = _entry()
  processor._file_dropoff_queue["d"] = _entry()

  processor._persist_queues()

  assert set(_read(processor.pickup_queue_backup_file)) == {"p"}
  assert set(_read(processor.waiting_queue_backup_file)) == {"w"}
  assert set(_read(processor.preprocess_queue_backup_file)) == {"pp"}
  assert set(_read(processor.dropoff_queue_backup_file)) == {"d"}
  assert not list(backup_dir.glob("*.tmp"))


def test_persist_reflects_each_change_immediately(processor: SASProcessor) -> None:
  processor._file_pickup_queue["first"] = _entry()
  processor._persist_queues()
  assert set(_read(processor.pickup_queue_backup_file)) == {"first"}

  processor._file_pickup_queue.pop("first")
  processor._file_pickup_queue["second"] = _entry()
  processor._persist_queues()
  assert set(_read(processor.pickup_queue_backup_file)) == {"second"}


def test_failed_replace_leaves_previous_file_intact(processor: SASProcessor, monkeypatch: pytest.MonkeyPatch) -> None:
  processor._file_pickup_queue["kept"] = _entry()
  processor._persist_queues()
  before = processor.pickup_queue_backup_file.read_text()

  def _boom(src: Any, dst: Any) -> None:
    raise OSError("simulated replace failure")

  monkeypatch.setattr(suppliers_mod, "replace", _boom)
  processor._file_pickup_queue["lost"] = _entry()
  with pytest.raises(OSError, match="simulated replace failure"):
    processor._persist_queues()

  assert processor.pickup_queue_backup_file.read_text() == before


def test_loader_ignores_stale_tmp_files(processor: SASProcessor, tmp_path: Path, backup_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
  processor._file_pickup_queue["real"] = _entry()
  processor._persist_queues()
  stale = backup_dir / f"{processor.pickup_queue_backup_file.name}.tmp"
  stale.write_text("{ this is not json")

  atexit.unregister(processor._persist_queues_at_exit)
  _drop_singleton()
  reloaded = SASProcessor()
  try:
    assert set(reloaded._file_pickup_queue) == {"real"}
    assert not list((backup_dir / "corrupted").glob("*"))
  finally:
    atexit.unregister(reloaded._persist_queues_at_exit)


def test_at_exit_persists_when_lock_is_free(processor: SASProcessor, caplog: pytest.LogCaptureFixture) -> None:
  processor._file_dropoff_queue["d"] = _entry()
  with caplog.at_level(logging.WARNING):
    processor._persist_queues_at_exit()
  assert set(_read(processor.dropoff_queue_backup_file)) == {"d"}
  assert "still held" not in caplog.text
  assert not processor._lock.locked()


def test_at_exit_still_persists_with_warning_when_lock_is_held(processor: SASProcessor, caplog: pytest.LogCaptureFixture) -> None:
  processor._file_dropoff_queue["d"] = _entry()
  holder_ready = threading.Event()
  release = threading.Event()

  def _hold() -> None:
    with processor._lock:
      holder_ready.set()
      release.wait(timeout=10)

  holder = threading.Thread(target=_hold, daemon=True)
  holder.start()
  assert holder_ready.wait(timeout=5)
  try:
    with caplog.at_level(logging.WARNING):
      processor._persist_queues_at_exit()
  finally:
    release.set()
    holder.join(timeout=5)

  assert set(_read(processor.dropoff_queue_backup_file)) == {"d"}
  assert "still held" in caplog.text
  assert not processor._lock.locked()


def test_at_exit_handler_is_registered_on_construction(processor: SASProcessor) -> None:
  # atexit._ncallbacks() is CPython's count of registered handlers; construction (in the fixture) registered exactly one
  # bound method, so unregistering it drops the count by one. Re-register so the fixture teardown stays symmetric.
  before = atexit._ncallbacks()  # noqa: SLF001
  atexit.unregister(processor._persist_queues_at_exit)
  after = atexit._ncallbacks()  # noqa: SLF001
  atexit.register(processor._persist_queues_at_exit)
  assert before - after == 1


async def test_clean_stale_entries_persists_under_lock(processor: SASProcessor) -> None:
  processor._file_pickup_queue["stale"] = _entry(days_from_now=-30)
  processor._persist_queues()
  assert set(_read(processor.pickup_queue_backup_file)) == {"stale"}

  await processor.clean_stale_queue_entries()

  assert processor._file_pickup_queue == {}
  assert _read(processor.pickup_queue_backup_file) == {}


def test_legacy_save_paths_are_gone() -> None:
  assert not hasattr(SASProcessor, "_save_backups")
  assert not hasattr(SASProcessor, "save_queue_backups_off_thread")
  assert "__del__" not in suppliers_mod.SupplierProcessorBase.__dict__
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen pytest tests/unit/test_queue_persistence.py -v --no-cov`
Expected: FAIL — `AttributeError: 'SASProcessor' object has no attribute '_persist_queues'` / `_persist_queues_at_exit`, and `test_legacy_save_paths_are_gone` fails on `_save_backups`.

- [ ] **Step 3: Add the imports and the persistence methods to the base class**

In `src/scheduled_invoice_processor/suppliers/__init__.py` add to the stdlib imports (alphabetical):

```python
from atexit import register as register_at_exit
from os import replace
```

In `__init__`, directly after `self._load_queue_backups()` add:

```python
    # Replaces __del__, which CPython does not guarantee to run for module-level singletons.
    register_at_exit(self._persist_queues_at_exit)
```

Delete these three members entirely:

```python
  def __del__(self) -> None:
    self._save_backups()

  async def save_queue_backups_off_thread(self) -> None:
    await to_thread(self._save_backups)

  def _save_backups(self) -> None:
    ...  # the whole method, through `raise`
```

and in their place add:

```python
  def _persist_queues(self) -> None:
    """Write all four queues to their backup files, atomically per file.

    Sync and lock-free by design: every caller either holds `self._lock` (the queue-mutating blocks) or is the
    at-exit path. Each file is written to `<name>.tmp` and then `os.replace`d onto the real path, so a crash
    mid-write leaves the previous backup intact and the loader's quarantine path only ever sees real corruption.
    """
    for backup_file, queue in (
      (self.pickup_queue_backup_file, self._file_pickup_queue),
      (self.preprocess_queue_backup_file, self._file_preprocess_queue),
      (self.waiting_queue_backup_file, self._file_waiting_queue),
      (self.dropoff_queue_backup_file, self._file_dropoff_queue),
    ):
      tmp_file = backup_file.with_name(f"{backup_file.name}.tmp")
      tmp_file.write_bytes(self._queue_ta.dump_json(queue, indent=2, round_trip=True))
      replace(tmp_file, backup_file)

  def _persist_queues_at_exit(self) -> None:
    """Final save at interpreter exit. Waits up to 1 s for the queue lock; if it is still held (a transfer was
    mid-flight when the process was told to exit) the snapshot is written anyway — a possibly mid-mutation but
    always parseable file beats losing the last change."""
    acquired = self._lock.green_acquire(timeout=1.0)
    if not acquired:
      logger.warning(
        "%s: queue lock still held at interpreter exit; persisting a possibly mid-mutation snapshot", self.__class__.__name__
      )
    try:
      self._persist_queues()
    except Exception:
      logger.exception("%s: Error persisting queue backups at interpreter exit", self.__class__.__name__)
    finally:
      if acquired:
        self._lock.green_release()
```

- [ ] **Step 4: Wire the call sites in the base class**

1. `clean_stale_queue_entries` — replace its body after the `errored` guard with:

```python
    async with self._lock:
      changed_entries = await self._clean_stale_queue_entries()
      if changed_entries:
        self._persist_queues()
```

(the old `if changed_entries: await to_thread(self._save_backups)` after the lock goes away).

2. `_preprocess_files` (base) — after the final loop that pops from `_file_preprocess_queue` into `_file_dropoff_queue` (still inside `async with self._lock:`), add at the loop's indentation level:

```python
      self._persist_queues()
```

3. `_dropoff_files` — after the final `for key, file_meta in tuple(self._file_dropoff_queue.items()):` loop that pops and calls `schedule.check_box(...)` (still inside `async with self._lock:`), add at the loop's indentation level:

```python
      self._persist_queues()
```

4. `_pickup_files` — the last loop currently sits *outside* the lock:

```python
    for key, item in items_to_advance.items():
      self._file_waiting_queue[key] = item
      self._file_pickup_queue.pop(key)
      local_logger.info("%s: %s: Moved %s to waiting queue", self.__class__.__name__, key, item.storenum)
```

Indent it by one level so it is the last statement inside `async with self._lock:` (after `if self.pickup_archive_ftp_folder is not None: await gather(*archive_futures)`), and add `self._persist_queues()` after the loop at the same indentation as the `for`. (`items_to_advance` is defined inside the lock block already, so nothing else moves.)

5. `_register_pickup` — change

```python
    async with self._lock:
      self._file_pickup_queue[queue_key] = register_data
```

to

```python
    async with self._lock:
      self._file_pickup_queue[queue_key] = register_data
      self._persist_queues()
```

6. `_register_dropoff` — at the very end of its `async with self._lock:` block, after

```python
      self._file_preprocess_queue[key] = matched_item
      local_logger.info("%s: %s: Registered dropoff for: %s", self.__class__.__name__, key, matched_item.storenum)
```

add:

```python
      self._persist_queues()
```

- [ ] **Step 5: Wire the two off-thread preprocess call sites**

In both `ryo.py` and `coremark.py`, inside `_preprocess_off_thread`, change

```python
      self._file_dropoff_queue[key] = new_file_meta
      old_file_meta = self._file_preprocess_queue.pop(key)
      local_logger.info("%s: %s: Updated queues", self.__class__.__name__, key)
```

to

```python
      self._file_dropoff_queue[key] = new_file_meta
      old_file_meta = self._file_preprocess_queue.pop(key)
      self._persist_queues()  # the event-loop caller of this worker holds self._lock for the whole preprocess step
      local_logger.info("%s: %s: Updated queues", self.__class__.__name__, key)
```

- [ ] **Step 6: Remove the cron job in `startup.py`**

Delete lines 104-109 of `src/scheduled_invoice_processor/startup.py`:

```python
    scheduler.add_job(
      processor().save_queue_backups_off_thread,
      CronTrigger(minute="8-59/10", timezone=SETTINGS.tz),
      id=f"{supplier}_save_queue_backups",
      replace_existing=True,
    )
```

and the blank line that followed it.

- [ ] **Step 7: Run the tests and tooling**

Run: `uv run --frozen pytest tests/unit -v --no-cov`
Expected: all PASS (Task 1's 8 + Task 2's 4 + this task's 9).

Run: `grep -rn "_save_backups\|save_queue_backups\|__del__" src` — expected: no output.

Run: `uv run --frozen ruff check src tests && uv run --frozen ruff format --check src tests && uv run --frozen pyright src`
Expected: clean. (If ruff flags `to_thread` as unused in `suppliers/__init__.py`, it is still used by `_pickup_files`/`_preprocess_files`/`_dropoff_files` — check before removing.)

- [ ] **Step 8: Run the e2e suite**

Same command block as Task 1 Step 12. Expected: `10 passed`. Additionally, after the run, `ls <PERSISTED_DIR>/queue_backups/` in the temp dir printed by the suite (or inspect via the test output) should show the `sas_*`/`ryo_*` JSON files and no `.tmp` leftovers — mention what you saw in the report. Report "not run locally: <reason>" if it cannot run.

- [ ] **Step 9: Commit**

```bash
git add -A src/scheduled_invoice_processor tests/unit/test_queue_persistence.py
git commit -m "feat(queues): persist queue backups atomically on every change; atexit final save replaces __del__ and the cron job"
```

---

### Task 4: Commit the drag-race harness and baseline

**Files:**
- Create: `scripts/benchmarks/dragrace_ryo.py`
- Create: `scripts/benchmarks/dragrace_before.json` (byte-for-byte copy of `.cache/dragrace/dragrace_before.json`)
- Create: `scripts/benchmarks/README.md`

**Interfaces:**
- Consumes: `RYOProcessor(pbar)`, `SupplierProcessorBase._transfer_file_vend_to_main`, `DatabaseCache` (`refresh_cache`, `schedule.walk_typed_rows`, `schedule.read_value`, `schedule.write_value`, `schedule._field_type_adapters`, `submit_queued_writes_to_pool`), `Patches.patch_the_monkey()`, `SETTINGS.tz`.
- Produces: a runnable `scripts/benchmarks/dragrace_ryo.py <label> <out.json>` and the committed v6.3.1 baseline that Task 5 compares against.

`scripts/` is outside pytest's `testpaths`, but ruff lints it — the file must pass `ruff check` and `ruff format --check`. It is a benchmark, not test code; it may import `aeth_ext` (only `aeth_ext.rich.progress.Progress`, like `suppliers.ryo.main()` does).

- [ ] **Step 1: Write the harness**

`scripts/benchmarks/dragrace_ryo.py`:

```python
"""Drag race: time one real RYO pickup/dropoff cycle (mirrors `suppliers.ryo.main()`), per stage and per file.

Usage (from the repo root, real `.env` with the *testing* DATABASE_ID and USE_TESTING_FOLDERS=True):

  uv run --frozen python scripts/benchmarks/dragrace_ryo.py <label> <out.json>

What it does: copies `persisted_data/secrets` into a temp PERSISTED_DIR_LOC, registers pickups for every RYO row on the
testing sheet, runs pickup_files (vendor -> /Testing/Waiting/RYO), register_dropoff and dropoff_files, then undoes its
own footprint: rows it ticked are restored and `/Testing/RYO`, `/Testing/Waiting/RYO[/Archive]`, `/Testing/Processed/RYO`
are emptied. `__debug__` must be on (the default) so the vendor-side archive is only simulated. Writes `<out>.partial.json`
before cleanup so a cleanup failure never loses the numbers. Manual only — never run this from CI.
"""

# Standard library imports
import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime
from ftplib import FTP, error_perm
from importlib.metadata import version
from pathlib import Path
from typing import Any

LABEL, OUT_PATH = sys.argv[1], Path(sys.argv[2])
REPO = Path.cwd()
PERSISTED = Path(tempfile.mkdtemp(prefix=f"dragrace-{LABEL}-"))
shutil.copytree(REPO / "persisted_data" / "secrets", PERSISTED / "secrets")
os.environ["PERSISTED_DIR_LOC"] = str(PERSISTED)
os.environ["USE_TESTING_FOLDERS"] = "True"

# The app reads SETTINGS and the secrets at import time, so these imports must follow the environment setup above.
# First party imports
from scheduled_invoice_processor.monkey_patches import Patches  # noqa: E402

Patches.patch_the_monkey()

# Third party imports
from rich import get_console  # noqa: E402

# First party imports
from aeth_ext.rich.progress import Progress  # noqa: E402
from scheduled_invoice_processor.database import DatabaseCache  # noqa: E402
from scheduled_invoice_processor.environment_init_vars import SETTINGS  # noqa: E402
from scheduled_invoice_processor.suppliers import SupplierProcessorBase  # noqa: E402
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor  # noqa: E402
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns as Cols  # noqa: E402
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum  # noqa: E402

PER_FILE: list[dict[str, Any]] = []
STAGES: dict[str, float] = {}

_original_transfer = SupplierProcessorBase._transfer_file_vend_to_main


def _timed_transfer(self: SupplierProcessorBase, *args: Any, **kwargs: Any) -> Any:
  started = time.perf_counter()
  try:
    return _original_transfer(self, *args, **kwargs)
  finally:
    PER_FILE.append({"file": kwargs["send_path"].name, "secs": round(time.perf_counter() - started, 3)})


SupplierProcessorBase._transfer_file_vend_to_main = _timed_transfer


class Stage:
  def __init__(self, name: str) -> None:
    self.name = name
    self.started = 0.0

  def __enter__(self) -> None:
    self.started = time.perf_counter()

  def __exit__(self, *exc: object) -> None:
    STAGES[self.name] = round(time.perf_counter() - self.started, 3)


def _clear_remote_testing_folders(ryo: RYOProcessor) -> int:
  secrets = json.loads((PERSISTED / "secrets" / "sft_creds.json").read_text())
  removed = 0
  ftp = FTP()
  ftp.connect(secrets["HOST"], int(secrets["PORT"]))
  ftp.login(secrets["USER"], secrets["PWD"])
  try:
    for folder in (
      ryo.destination_ftp_folder,
      ryo.pre_processing_archive_folder,
      ryo.post_processing_waiting_folder,
      ryo.pre_processing_waiting_folder,
    ):
      for name, facts in ftp.mlsd(folder.as_posix()):
        if name in {".", "..", ""} or facts.get("type") != "file":
          continue
        try:
          ftp.delete(f"{folder.as_posix()}/{name}")
          removed += 1
        except error_perm as exc:
          print("cleanup skip", folder, name, exc)
  finally:
    ftp.quit()
  return removed


async def main() -> dict[str, Any]:
  cache = DatabaseCache()
  with Stage("refresh_cache"):
    await cache.refresh_cache()
  now = datetime.now(SETTINGS.tz)
  with Progress(console=get_console(), auto_refresh=False) as pbar:
    ryo = RYOProcessor(pbar)
    orders = [order async for order in cache.schedule.walk_typed_rows() if order.supplier == SuppliersEnum.RYO]
    before = {order.store: (order.invoice_grabbed, order.invoice_applied) for order in orders}

    with Stage("register_pickup"):
      for order in orders:
        await ryo.register_pickup(
          storenum=order.store, customer_id=order.customer, pickup_date=now, dropoff_date=now, current_week=True
        )
      await cache.submit_queued_writes_to_pool()
    with Stage("pickup_files"):
      await ryo.pickup_files()
    with Stage("flush_after_pickup"):
      await cache.submit_queued_writes_to_pool()
    with Stage("register_dropoff"):
      for order in orders:
        await ryo.register_dropoff(
          storenum=order.store, customer_id=order.customer, pickup_date=now, dropoff_date=now, current_week=True
        )
      await cache.submit_queued_writes_to_pool()
    with Stage("dropoff_files"):
      await ryo.dropoff_files()
    with Stage("flush_after_dropoff"):
      await cache.submit_queued_writes_to_pool()

    OUT_PATH.with_suffix(".partial.json").write_text(json.dumps({"stages": dict(STAGES), "per_file": list(PER_FILE)}, indent=2))

    # Undo the footprint so the run is repeatable: restore the rows we ticked, empty the testing folders.
    touched: list[int] = []
    for order in orders:
      grabbed = await cache.schedule.read_value((SuppliersEnum.RYO, order.store), Cols.invoice_grabbed)
      applied = await cache.schedule.read_value((SuppliersEnum.RYO, order.store), Cols.invoice_applied)
      if (bool(grabbed), bool(applied)) != tuple(map(bool, before[order.store])):
        touched.append(order.store)
        await cache.schedule.write_value(
          (SuppliersEnum.RYO, order.store),
          Cols.invoice_grabbed,
          before[order.store][0],
          cache.schedule._field_type_adapters["invoice_grabbed"],  # noqa: SLF001
        )
        await cache.schedule.write_value(
          (SuppliersEnum.RYO, order.store),
          Cols.invoice_applied,
          before[order.store][1],
          cache.schedule._field_type_adapters["invoice_applied"],  # noqa: SLF001
        )
    await cache.submit_queued_writes_to_pool()
    removed = _clear_remote_testing_folders(ryo)

  per_file_secs = [entry["secs"] for entry in PER_FILE]
  return {
    "label": LABEL,
    "aeth_ext": version("aeth-ext"),
    "when": datetime.now(SETTINGS.tz).isoformat(timespec="seconds"),
    "orders": len(orders),
    "files_transferred": len(PER_FILE),
    "stages": STAGES,
    "total_cycle_secs": round(sum(STAGES.values()), 3),
    "per_file": PER_FILE,
    "per_file_mean": round(sum(per_file_secs) / max(1, len(per_file_secs)), 3),
    "per_file_max": max(per_file_secs, default=0),
    "rows_reset": touched,
    "remote_cleaned": removed,
    "errored": ryo.errored,
  }


if __name__ == "__main__":
  result = asyncio.run(main())
  OUT_PATH.write_text(json.dumps(result, indent=2))
  print(json.dumps({key: value for key, value in result.items() if key != "per_file"}, indent=2))
```

If ruff reports rules this file still violates (the repo's ruff config lives in the parent `../pyproject.toml`), fix the code rather than adding a per-file ignore, except for `E402` (imports after environment setup are required) and `SLF001` (private sheet adapter access mirrors the existing throwaway harness) which are already annotated inline.

- [ ] **Step 2: Copy the baseline and write the README**

```bash
mkdir -p scripts/benchmarks
cp .cache/dragrace/dragrace_before.json scripts/benchmarks/dragrace_before.json
```

`scripts/benchmarks/README.md`:

```markdown
# Benchmarks

Manual, real-server benchmarks. Never run from CI: they need the real credentials in `persisted_data/secrets`,
a `.env` pointing at the *testing* Google Sheet with `USE_TESTING_FOLDERS=True`, and they write to the holding
server's `/Testing` tree (which they clean up afterwards).

## dragrace_ryo.py — before/after connection pooling

    uv run --frozen python scripts/benchmarks/dragrace_ryo.py after-v8.0.0 scripts/benchmarks/dragrace_after.json

`dragrace_before.json` is the aeth-ext 6.3.1 baseline (2026-08-25, 7 files): per-file mean 5.35 s, max 5.45 s,
`pickup_files` 10.8 s, whole cycle 40.5 s. Compare **per-file mean/max** (the file count depends on what is in the
vendor folder that day) and the `pickup_files` wall time (proxy for one concurrent transfer wave). Results are
recorded in the "Drag race" section of `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md`.
```

- [ ] **Step 3: Lint and dry-import**

Run: `uv run --frozen ruff check scripts && uv run --frozen ruff format --check scripts`
Expected: clean (fix formatting with `uv run --frozen ruff format scripts` if needed).

Run: `uv run --frozen python -c "import ast, pathlib; ast.parse(pathlib.Path('scripts/benchmarks/dragrace_ryo.py').read_text()); print('parses')"`
Expected: `parses`. Do **not** execute the harness in this task (it touches the real testing sheet and server; Task 5 does that deliberately).

- [ ] **Step 4: Commit**

```bash
git add scripts/benchmarks
git commit -m "chore(benchmarks): commit the RYO drag-race harness and the aeth-ext 6.3.1 baseline"
```

---

### Task 5: Drag-race after-run on v8 and record the numbers (controller/manual)

**Files:**
- Create: `scripts/benchmarks/dragrace_after.json`
- Modify: `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md` ("Drag race — before/after connection pooling")

**Interfaces:**
- Consumes: `scripts/benchmarks/dragrace_ryo.py` from Task 4, real credentials, the testing sheet, the holding server's `/Testing` tree.
- Produces: the Phase 2 decision input (per-file mean/max on v8, `pickup_files` wall time).

This task runs against real servers and the live testing sheet; it is executed by the controller (or Jacob), not dispatched to an implementer subagent, and it is never run from CI. It is only meaningful once Tasks 1-4 are committed (the app must be on v8 pools).

- [ ] **Step 1: Pre-flight**

Confirm: `git status` clean on `chore/update-to-aeth-ext-v8`; `.env` points at the testing `DATABASE_ID` and has `USE_TESTING_FOLDERS=True`; `uv run --frozen python -c "from importlib.metadata import version; print(version('aeth-ext'))"` prints `8.0.0`; there are files in the RYO vendor pickup folder for the current week (if there are none, the run produces no per-file numbers — say so and wait for a day with files rather than fabricating).

- [ ] **Step 2: Run**

```bash
uv run --frozen python scripts/benchmarks/dragrace_ryo.py after-v8.0.0 scripts/benchmarks/dragrace_after.json
```

Expected: the JSON summary prints with `"errored": false`, `rows_reset` listing the stores it ticked and restored, and `remote_cleaned` > 0. If it crashes after writing `dragrace_after.partial.json`, keep that file, finish the cleanup by hand (restore the ticked rows on the testing sheet; empty `/Testing/RYO`, `/Testing/Waiting/RYO`, `/Testing/Waiting/RYO/Archive`, `/Testing/Processed/RYO` on the holding server) and report what was done.

- [ ] **Step 3: Record the results in the spec**

In the spec's "Drag race — before/after connection pooling" section, replace the "After-run: same command on v8; compare …" bullet with the measured values in this shape (fill every placeholder from `dragrace_after.json`; keep the baseline bullet as-is):

```markdown
- After-run (aeth-ext 8.0.0, <when>, <files_transferred> current-week files): per-file mean **<per_file_mean> s**,
  max <per_file_max> s; `pickup_files` <stages.pickup_files> s; `dropoff_files` <stages.dropoff_files> s; whole cycle
  <total_cycle_secs> s. Raw: `scripts/benchmarks/dragrace_after.json`.
- Phase 2 decision input: one concurrent wave (`pickup_files`) took <stages.pickup_files> s against the 7 s GRACEFUL
  budget — <"an in-flight wave can finish inside the budget" | "an in-flight wave cannot be expected to finish inside
  the budget">.
```

- [ ] **Step 4: Commit**

```bash
git add scripts/benchmarks/dragrace_after.json docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md
git commit -m "docs(spec): record the v8.0.0 drag-race after-run numbers"
```

---

### Task 6: CI — run the unit tests in the e2e workflow (walkthrough before push)

**Files:**
- Modify: `.github/workflows/e2e.yml` (one new step before "Run e2e suite", ~line 44)

**Interfaces:**
- Consumes: `tests/unit` from Tasks 1-3 (network-free; needs no secrets).
- Produces: unit tests gate PR #10 alongside the e2e suite.

Per the Global Constraints this is the only workflow change in the plan, it is batched last, and the controller walks Jacob through the diff before pushing it. No secrets are added or changed.

- [ ] **Step 1: Add the step**

In `.github/workflows/e2e.yml`, directly before the step whose `run:` is `uv run --frozen pytest tests/e2e -v --no-cov`, add (matching the file's existing indentation):

```yaml
      - name: Run unit tests
        run: uv run --frozen pytest tests/unit -v --no-cov
```

- [ ] **Step 2: Validate the YAML locally**

Run: `git diff .github/workflows/e2e.yml`
Expected: exactly two added lines (`- name: Run unit tests` and its `run:`), indented like the neighbouring steps, placed immediately before the "Run e2e suite" step. Nothing else in the file changes (the `if:` actor gate, secrets mapping and `permissions: contents: read` stay as they are).

- [ ] **Step 3: Walkthrough, then commit**

Controller: show Jacob the `git diff .github/workflows/e2e.yml` and get an explicit go before the commit is pushed.

```bash
git add .github/workflows/e2e.yml
git commit -m "ci: run the unit tests before the e2e suite"
```

---

## Finishing the branch

After all tasks (and the final whole-branch review), the only outward action is pushing `chore/update-to-aeth-ext-v8` to `origin` so PR #10 updates and CI runs. **Do not merge PR #10.** Report to Jacob: the commit list, the CI result, the drag-race numbers, and that the PR is ready for his review.

## Self-review against the spec

- B2 + C2 (delete `ftp_configs.py`; per-vendor `load_credentials`; `load_sft_credentials`; class-level `create_ftp_adapter` with `container_cls`; `pbar` injection kept; defaults kept; type hints from `aeth_ext.ftp.session` / `pool.ftp_adapter` / `pool.sftp_adapter`; `pickup_ftp_creds` removed; smoke script deleted) → Task 1.
- A4 minimal (`@handle_fatal_exc_sync` bare; `err_handling.py` and `FatalDetails` deleted; `main()` structurally as-is incl. `sleep(600)`; inline check over `get_current_fatal_trails()` with the three patterns; log fields from `trail.origin`; unit test with real trails) → Task 2 (ruling: predicate lives in `database.py` because `startup.py` cannot be imported network-free; test feeds raw exceptions so tests never import `aeth_ext`).
- README cleanup → Task 1 Step 11 (folded into the task that makes the workaround obsolete, verified by the same e2e run).
- A2 (`_persist_queues` sync atomic; called in `_register_pickup`, `_register_dropoff`, `_pickup_files`, `_preprocess_off_thread`, `_dropoff_files`, `_clean_stale_queue_entries`; `__del__`/`save_queue_backups_off_thread`/`_save_backups`/cron job removed; `_load_queue_backups` unchanged; `atexit` with 1 s lock wait; unit tests for immediate write, failed replace, stale `.tmp`, both at-exit paths) → Task 3. Also covered: base `_preprocess_files` (the base-class variant of the preprocess queue swap, which SAS uses).
- Drag race (harness committed to `scripts/benchmarks/`, baseline beside it, after-run recorded in the spec, manual not CI) → Tasks 4 and 5.
- Constraints: PR never merged (header + Finishing); Dockerfile untouched (no task touches it); tests never import `aeth_ext` (all new tests import only the app); e2e assertions not weakened (Task 1 changes a comment and a dummy port only); Coremark mechanical (Task 1 Step 7, no tests beyond the loader); `__debug__` semantics untouched; Phase 2 untouched (Task 2 keeps `sleep(600)`).
- Placeholder scan: the only angle-bracket placeholders are in Task 5 Step 3's results template, which is filled from measured data by design.
- Type consistency: `load_sft_credentials() -> FTPCredentials`, `load_credentials() -> SFTPCredentials | FTPCredentials`, `vendor_ftp: FTPAdapter | SFTPAdapter`, `_persist_queues() -> None`, `_persist_queues_at_exit() -> None`, `trail_is_database_origin(trail) -> bool`, `exception_is_database_origin(exc) -> bool`, `DATABASE_ORIGIN_PATTERNS: tuple[str, ...]` are used with the same names in every task and test.
