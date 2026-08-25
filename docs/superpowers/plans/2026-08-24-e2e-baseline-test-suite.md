# E2E Baseline Test Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A GitHub-Actions-run end-to-end suite that proves the SAS and RYO order pickup/dropoff cycle works against Docker stand-ins for the production FTP/SFTP servers and the live testing Google Sheet, on `main` pinned to aeth-ext `<7`.

**Architecture:** Three Docker containers (Pure-FTPd for the SFT holding server, two SFTPGo instances for the SAS/RYO vendor servers) are started by CI; a pytest session fixture writes a temporary `persisted_data/` tree with credentials pointing at `127.0.0.1` and sets env vars *before* the app is imported. Tests seed invoice files onto the vendor containers and schedule rows into the testing sheet, then call each supplier module's existing `main()` coroutine — the production cycle sequence — and assert on remote folder contents and sheet state. Test code talks to the servers with `ftplib`/`paramiko`/`gspread` directly, never through `aeth_ext`, so the suite is valid on both aeth-ext v6 and v8.

**Tech Stack:** Python 3.14, uv, pytest + pytest-asyncio (session-scoped loop) + pytest-cov, paramiko, ftplib, gspread, Docker Compose (`stilliard/pure-ftpd`, `drakkan/sftpgo`), GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-24-e2e-baseline-test-suite-design.md`

## Global Constraints

- Work on branch `test/e2e-baseline` (already created off `main`). Never commit to `main` directly.
- `pyproject.toml` dependency pin must become exactly `"aeth-ext[sftp, async]>=6.2.2,<7"`.
- **No changes to anything under `src/`.** The spec allows a singleton-reset hook only if unavoidable; this plan achieves reset from test code instead (Task 4), so `src/` stays untouched.
- Test code must not `import aeth_ext` (directly or via `from aeth_ext...`). Importing `scheduled_invoice_processor.*` is fine.
- Only SAS and RYO. No Coremark. No failure-condition tests. No scheduler.
- Python files use 2-space indentation and the `# Standard library imports` / `# Third party imports` / `# First party imports` comment-grouped import style seen in `src/`.
- All env/creds for the app are provided through `os.environ` + a temp `persisted_data/` tree; the repo's `.env` must not be relied upon (CI has none).
- Timezone for every generated timestamp is `US/Eastern` (the app's `SETTINGS.tz` default).
- Commit after every task with the message given in that task. Commit messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

## File map

| Path | Responsibility |
|---|---|
| `pyproject.toml` | pin cap, dev deps, pytest-asyncio loop scope |
| `uv.lock` | re-locked after pyproject change |
| `tests/__init__.py`, `tests/e2e/__init__.py` | make `tests.e2e` importable as a package |
| `tests/docker/compose.yaml` | the three server containers |
| `tests/docker/sftpgo/sas_users.json`, `ryo_users.json` | SFTPGo `loaddata` user dumps |
| `tests/e2e/constants.py` | ports, users, passwords, folder paths, reserved store/customer ids — single source of truth |
| `tests/e2e/conftest.py` | env + temp `persisted_data/` setup at import time; session fixtures for server readiness, folder trees, singleton reset, sheet |
| `tests/e2e/remote.py` | thin `ftplib`/`paramiko` helpers: connect, mkdir -p, listdir, upload, read, purge |
| `tests/fixtures/templates/sas_invoice.TXT`, `ryo_invoice.txt` | sanitized invoice bodies |
| `tests/e2e/generator.py` | builds invoice filenames/headers/bodies for a customer + invoice numbers |
| `tests/e2e/sheet.py` | gspread helper: seed rows, delete rows, read schedule flags, read Processing Log rows |
| `tests/e2e/test_generator.py` | small unit test: generated names match the app's own filename pattern and header regex |
| `tests/e2e/test_sas_cycle.py`, `test_ryo_cycle.py`, `test_both_suppliers.py` | the three e2e scenarios |
| `.github/workflows/e2e.yml` | CI |
| `tests/e2e/README.md` | how to run, which secrets |

---

### Task 1: Pin cap, dev dependencies, pytest config

**Files:**
- Modify: `pyproject.toml` (dependency line 6; `[dependency-groups] dev`; `[tool.pytest.ini_options]`)
- Modify: `uv.lock` (regenerated)
- Create: `tests/__init__.py`, `tests/e2e/__init__.py` (empty)

**Interfaces:**
- Produces: a runnable `uv run pytest` with `asyncio_mode = "auto"` and session-scoped event loop for every async test/fixture.

- [ ] **Step 1: Edit the dependency pin**

In `pyproject.toml` change

```toml
    "aeth-ext[sftp, async]>=6.2.2",
```

to

```toml
    "aeth-ext[sftp, async]>=6.2.2,<7",
```

- [ ] **Step 2: Add dev dependencies**

In `[dependency-groups] dev = [...]` add these two lines (keep alphabetical order):

```toml
    "pytest-asyncio>=1.0.0",
    "pytest-cov>=6.0.0",
```

- [ ] **Step 3: Extend pytest ini options**

Replace the existing `[tool.pytest.ini_options]` block with:

```toml
[tool.pytest.ini_options]
  addopts      = [
    "--cov",
    "--cov-report=term-missing",
    "--strict-markers",
    "--strict-config",
    "-ra"
  ]
  cache_dir    = ".cache/pytest"
  testpaths    = ["tests"]
  xfail_strict = true
  asyncio_mode = "auto"
  asyncio_default_fixture_loop_scope = "session"
  asyncio_default_test_loop_scope    = "session"
```

The session-scoped loop matters: `DatabaseCache` (a process-wide singleton) captures `get_running_loop()` in `__init__`, so every test must run on the same loop.

- [ ] **Step 4: Create empty package files**

Create `tests/__init__.py` and `tests/e2e/__init__.py`, both containing exactly:

```python
```

(empty file).

- [ ] **Step 5: Re-lock and sync**

Run: `uv lock`
Expected: exits 0; `uv.lock` now records `pytest-asyncio` and `pytest-cov` and the aeth-ext requirement `>=6.2.2,<7`.

Run: `uv sync`
Expected: exits 0.

- [ ] **Step 6: Verify pytest boots with the new config**

Run: `uv run pytest --co -q`
Expected: exit code 5 ("no tests ran") with no config errors. Any `--strict-config` error about unknown ini keys means pytest-asyncio is too old — check `uv pip show pytest-asyncio` reports ≥1.0.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock tests/__init__.py tests/e2e/__init__.py
git commit -m "chore: cap aeth-ext <7 and add pytest-asyncio/pytest-cov for the e2e suite

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Docker environment (Pure-FTPd + two SFTPGo)

**Files:**
- Create: `tests/docker/compose.yaml`
- Create: `tests/docker/sftpgo/sas_users.json`
- Create: `tests/docker/sftpgo/ryo_users.json`
- Create: `tests/e2e/constants.py`

**Interfaces:**
- Produces: `tests/e2e/constants.py` names used by every later task:
  - `SFT_HOST = "127.0.0.1"`, `SFT_PORT = 21`, `SFT_USER = "sft"`, `SFT_PASS = "sft-test-pw"`
  - `SAS_HOST = "127.0.0.1"`, `SAS_PORT = 2022`, `SAS_USER = "sas"`, `SAS_PASS = "sas-test-pw"`
  - `RYO_HOST = "127.0.0.1"`, `RYO_PORT = 2222`, `RYO_USER = "ryo"`, `RYO_PASS = "ryo-test-pw"`
  - `SAS_PICKUP_DIR = "/Fastrax Invoices"`, `SAS_PICKUP_ARCHIVE_DIR = "/Fastrax Invoices/Archive"`
  - `RYO_PICKUP_DIR = "/RYOtoSFT"`, `RYO_PICKUP_ARCHIVE_DIR = "/RYOtoSFT/Archive"`
  - `SFT_DIRS: tuple[str, ...]` — all `/Testing/...` folders the app expects
  - reserved store numbers / customer ids per scenario (see code)

- [ ] **Step 1: Write constants**

Create `tests/e2e/constants.py`:

```python
"""Single source of truth for the e2e environment: hosts, ports, users, remote folders, reserved ids.

The Docker services in tests/docker/compose.yaml publish onto 127.0.0.1 with these ports and users.
The app's own folder layout (see suppliers/sas.py and suppliers/ryo.py) with USE_TESTING_FOLDERS=True
prefixes the SFT-side folders with /Testing; vendor-side folders are unchanged.
"""

# --- SFT holding server (Pure-FTPd, plain FTP) ---
SFT_HOST = "127.0.0.1"
SFT_PORT = 21
SFT_USER = "sft"
SFT_PASS = "sft-test-pw"

# --- SAS vendor (SFTPGo, SFTP) ---
SAS_HOST = "127.0.0.1"
SAS_PORT = 2022
SAS_USER = "sas"
SAS_PASS = "sas-test-pw"

# --- RYO vendor (SFTPGo, SFTP) ---
RYO_HOST = "127.0.0.1"
RYO_PORT = 2222
RYO_USER = "ryo"
RYO_PASS = "ryo-test-pw"

# --- Vendor-side folders (NOT prefixed by USE_TESTING_FOLDERS) ---
SAS_PICKUP_DIR = "/Fastrax Invoices"
SAS_PICKUP_ARCHIVE_DIR = "/Fastrax Invoices/Archive"
RYO_PICKUP_DIR = "/RYOtoSFT"
RYO_PICKUP_ARCHIVE_DIR = "/RYOtoSFT/Archive"

# --- SFT-side folders as the app sees them with USE_TESTING_FOLDERS=True ---
SFT_WAITING_SAS = "/Testing/Waiting/SAS"
SFT_WAITING_SAS_ARCHIVE = "/Testing/Waiting/SAS/Archive"
SFT_PROCESSED_SAS = "/Testing/Processed/SAS"
SFT_DEST_SAS = "/Testing/SAS"
SFT_WAITING_RYO = "/Testing/Waiting/RYO"
SFT_WAITING_RYO_ARCHIVE = "/Testing/Waiting/RYO/Archive"
SFT_PROCESSED_RYO = "/Testing/Processed/RYO"
SFT_DEST_RYO = "/Testing/RYO"

SFT_DIRS: tuple[str, ...] = (
  SFT_WAITING_SAS,
  SFT_WAITING_SAS_ARCHIVE,
  SFT_PROCESSED_SAS,
  SFT_DEST_SAS,
  SFT_WAITING_RYO,
  SFT_WAITING_RYO_ARCHIVE,
  SFT_PROCESSED_RYO,
  SFT_DEST_RYO,
)

# --- Reserved schedule identities. Each scenario uses its own so runs never collide in the sheet. ---
# (store number, customer id). Store numbers are the sheet's index with the supplier; they must be unique per supplier.
SAS_CYCLE_ORDERS: tuple[tuple[int, str], ...] = ((9001, "90001"), (9002, "90002"))
RYO_CYCLE_ORDERS: tuple[tuple[int, str], ...] = ((9101, "9100000001"),)
BOTH_SAS_ORDERS: tuple[tuple[int, str], ...] = ((9003, "90003"),)
BOTH_RYO_ORDERS: tuple[tuple[int, str], ...] = ((9102, "9100000002"),)

ALL_RESERVED_STORES: frozenset[int] = frozenset(
  store for orders in (SAS_CYCLE_ORDERS, RYO_CYCLE_ORDERS, BOTH_SAS_ORDERS, BOTH_RYO_ORDERS) for store, _ in orders
)
```

- [ ] **Step 2: Write the SFTPGo user dumps**

Create `tests/docker/sftpgo/sas_users.json`:

```json
{
  "version": 10,
  "users": [
    {
      "id": 1,
      "status": 1,
      "username": "sas",
      "password": "sas-test-pw",
      "home_dir": "/srv/sftpgo/data/sas",
      "permissions": { "/": ["*"] },
      "max_sessions": 5,
      "quota_size": 0,
      "quota_files": 0,
      "upload_bandwidth": 0,
      "download_bandwidth": 0
    }
  ]
}
```

Create `tests/docker/sftpgo/ryo_users.json` identically but with `"username": "ryo"`, `"password": "ryo-test-pw"`, `"home_dir": "/srv/sftpgo/data/ryo"`.

`max_sessions: 5` mirrors that the production vendors (Bitvise, Files.com) cap concurrent sessions per user. `permissions "/": ["*"]` makes the root writable — the property OpenSSH images can't provide.

- [ ] **Step 3: Write the compose file**

Create `tests/docker/compose.yaml`:

```yaml
# E2E stand-ins for the production servers. See docs/superpowers/specs/2026-08-24-e2e-baseline-test-suite-design.md
services:

  sft-ftp:
    # Production SFT holding server runs Pure-FTPd; this is the same software.
    image: stilliard/pure-ftpd:bookworm-latest
    container_name: e2e-sft-ftp
    ports:
      - "21:21"
      - "30000-30009:30000-30009"
    environment:
      PUBLICHOST: "127.0.0.1"
      FTP_USER_NAME: "sft"
      FTP_USER_PASS: "sft-test-pw"
      FTP_USER_HOME: "/home/sft"
      FTP_PASSIVE_PORTS: "30000:30009"
      FTP_MAX_CLIENTS: "50"
      FTP_MAX_CONNECTIONS: "50"
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/127.0.0.1/21'"]
      interval: 2s
      timeout: 3s
      retries: 15

  sas-sftp:
    # Production SAS server is Files.com (not self-hostable). SFTPGo gives a writable root + per-user session cap.
    image: drakkan/sftpgo:v2.7-alpine
    container_name: e2e-sas-sftp
    ports:
      - "2022:2022"
    environment:
      SFTPGO_DATA_PROVIDER__CREATE_DEFAULT_ADMIN: "true"
      SFTPGO_DEFAULT_ADMIN_USERNAME: "admin"
      SFTPGO_DEFAULT_ADMIN_PASSWORD: "e2e-admin-pw"
      SFTPGO_LOADDATA_FROM: "/srv/sftpgo/loaddata.json"
      SFTPGO_LOADDATA_MODE: "0"
    volumes:
      - ./sftpgo/sas_users.json:/srv/sftpgo/loaddata.json:ro
      - sas-data:/srv/sftpgo/data
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O- http://127.0.0.1:8080/healthz || exit 1"]
      interval: 2s
      timeout: 3s
      retries: 15

  ryo-sftp:
    # Production RYO server is Bitvise SSH Server on Windows (port 2222). Same SFTPGo stand-in, published on 2222.
    image: drakkan/sftpgo:v2.7-alpine
    container_name: e2e-ryo-sftp
    ports:
      - "2222:2022"
    environment:
      SFTPGO_DATA_PROVIDER__CREATE_DEFAULT_ADMIN: "true"
      SFTPGO_DEFAULT_ADMIN_USERNAME: "admin"
      SFTPGO_DEFAULT_ADMIN_PASSWORD: "e2e-admin-pw"
      SFTPGO_LOADDATA_FROM: "/srv/sftpgo/loaddata.json"
      SFTPGO_LOADDATA_MODE: "0"
    volumes:
      - ./sftpgo/ryo_users.json:/srv/sftpgo/loaddata.json:ro
      - ryo-data:/srv/sftpgo/data
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O- http://127.0.0.1:8080/healthz || exit 1"]
      interval: 2s
      timeout: 3s
      retries: 15

volumes:
  sas-data:
  ryo-data:
```

- [ ] **Step 4: Bring the stack up and verify logins (requires Docker; if Docker is not available locally, skip to Step 6 — CI will verify in Task 9)**

Run: `docker compose -f tests/docker/compose.yaml up -d --wait`
Expected: all three services report `Healthy`.

Run this probe (uses the project venv, which already has paramiko):

```bash
uv run python - <<'EOF'
from ftplib import FTP
import paramiko
ftp = FTP(); ftp.connect("127.0.0.1", 21); print("FTP login:", ftp.login("sft", "sft-test-pw")); ftp.mkd("probe"); print("FTP root writable:", "probe" in ftp.nlst()); ftp.rmd("probe"); ftp.quit()
for user, port in (("sas", 2022), ("ryo", 2222)):
  t = paramiko.Transport(("127.0.0.1", port)); t.connect(username=user, password=f"{user}-test-pw")
  s = paramiko.SFTPClient.from_transport(t); s.mkdir("/probe"); print(user, "SFTP root writable:", "probe" in s.listdir("/")); s.rmdir("/probe"); t.close()
EOF
```

Expected output includes `FTP root writable: True`, `sas SFTP root writable: True`, `ryo SFTP root writable: True`.

If an SFTPGo container is unhealthy, run `docker logs e2e-sas-sftp` — a `loaddata` schema complaint means the `"version"` key needs to match the image's dump version; remove the `"version"` line from both JSON files and retry.

- [ ] **Step 5: Tear down**

Run: `docker compose -f tests/docker/compose.yaml down -v`

- [ ] **Step 6: Commit**

```bash
git add tests/docker tests/e2e/constants.py
git commit -m "test(e2e): add Pure-FTPd and SFTPGo docker stand-ins for production servers

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Remote helpers (`ftplib` / `paramiko`)

**Files:**
- Create: `tests/e2e/remote.py`
- Create: `tests/e2e/test_remote.py`

**Interfaces:**
- Produces:
  - `class FtpBox` with `__init__(host: str, port: int, user: str, password: str)`, context manager, `mkdirs(path: str) -> None`, `listdir(path: str) -> list[str]`, `upload(path: str, data: bytes) -> None`, `read(path: str) -> bytes`, `purge(path: str) -> None` (delete all *files* directly inside `path`, leave subdirs), `exists(path: str) -> bool`
  - `class SftpBox` with the identical method set.
  - `type RemoteBox = FtpBox | SftpBox`

- [ ] **Step 1: Write the failing test**

Create `tests/e2e/test_remote.py`:

```python
# Standard library imports
from uuid import uuid4

# Third party imports
import pytest

# Local imports
from tests.e2e import constants as C
from tests.e2e.remote import FtpBox, SftpBox


@pytest.mark.parametrize(
  "box_factory",
  [
    pytest.param(lambda: FtpBox(C.SFT_HOST, C.SFT_PORT, C.SFT_USER, C.SFT_PASS), id="ftp"),
    pytest.param(lambda: SftpBox(C.SAS_HOST, C.SAS_PORT, C.SAS_USER, C.SAS_PASS), id="sftp"),
  ],
)
def test_box_roundtrip(box_factory):
  folder = f"/probe-{uuid4().hex[:8]}/nested"
  with box_factory() as box:
    box.mkdirs(folder)
    assert box.exists(folder)
    box.upload(f"{folder}/a.txt", b"hello\r\n")
    assert box.listdir(folder) == ["a.txt"]
    assert box.read(f"{folder}/a.txt") == b"hello\r\n"
    box.purge(folder)
    assert box.listdir(folder) == []
```

- [ ] **Step 2: Run it to verify it fails**

Run: `docker compose -f tests/docker/compose.yaml up -d --wait && uv run pytest tests/e2e/test_remote.py -v --no-cov`
Expected: FAIL with `ModuleNotFoundError: No module named 'tests.e2e.remote'`.

- [ ] **Step 3: Implement**

Create `tests/e2e/remote.py`:

```python
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
    return name in self.ftp.nlst(parent or "/")

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
```

- [ ] **Step 4: Run the test**

Run: `uv run pytest tests/e2e/test_remote.py -v --no-cov`
Expected: both parametrized cases PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/remote.py tests/e2e/test_remote.py
git commit -m "test(e2e): add ftplib/paramiko remote helpers with roundtrip test

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: conftest — environment bootstrap, server fixtures, singleton reset

**Files:**
- Create: `tests/e2e/conftest.py`

**Interfaces:**
- Consumes: `tests.e2e.constants`, `tests.e2e.remote`
- Produces fixtures:
  - `e2e_env` (session, autouse): guarantees env/creds are in place; yields the temp `persisted_data` `Path`
  - `sft_box` (function): open `FtpBox` on the SFT server
  - `sas_box`, `ryo_box` (function): open `SftpBox` on each vendor
  - `remote_dirs` (session, autouse): creates every folder on all three servers once
  - `clean_remote` (function): purges every e2e folder before *and* after the test
  - `reset_processor_singletons` (function): drops `__shared_instance__` from `SASProcessor`/`RYOProcessor` so each test constructs fresh processors (with a live `Progress`), mirroring a fresh process. `DatabaseCache` is intentionally **not** reset (its loop is session-scoped and its cache is refreshed by `main()`).

The import-time contract: `scheduled_invoice_processor.*` evaluates `SETTINGS`, reads the four creds JSONs, and applies the `/Testing` prefix all at **import**. Therefore this conftest performs env setup at module top level, before any test module can import the app. Test modules must import the app **inside** test functions/fixtures, never at module top.

- [ ] **Step 1: Write conftest**

Create `tests/e2e/conftest.py`:

```python
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
```

- [ ] **Step 2: Verify the app imports under the bootstrapped environment**

With the Docker stack up and the four `E2E_*` variables exported, run:

```bash
uv run pytest tests/e2e/test_remote.py -v --no-cov -p no:cacheprovider
```

Expected: PASS (proves conftest imports cleanly and `_require` finds the vars).

Then run this one-off check that the app itself imports with the `/Testing` prefix applied:

```bash
uv run python -c "
import os, sys
sys.path.insert(0, '.')
import tests.e2e.conftest  # performs bootstrap
from scheduled_invoice_processor.suppliers.sas import SASProcessor
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
print(SASProcessor.destination_ftp_folder, RYOProcessor.pre_processing_archive_folder)
"
```

Expected output: `/Testing/SAS /Testing/Waiting/RYO/Archive`.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/conftest.py
git commit -m "test(e2e): bootstrap env, creds and server fixtures in conftest

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Sanitized invoice templates and the file generator

**Files:**
- Create: `tests/fixtures/templates/sas_invoice.TXT`
- Create: `tests/fixtures/templates/ryo_invoice.txt`
- Create: `tests/e2e/generator.py`
- Create: `tests/e2e/test_generator.py`

**Interfaces:**
- Produces:
  - `sas_filename(customer_id: str, at: datetime) -> str` → `EF{customer_id}_{YYYYMMDDHHMMSSffffff}.TXT`
  - `sas_file(customer_id: str, invoice_num: str, at: datetime) -> tuple[str, bytes]` → `(filename, content)`
  - `ryo_filename(customer_id: str, invoice_num: str, at: datetime) -> str` → `{customer_id}_{invoice_num}_{YYYYMMDDHHMMSSffffff}.txt`
  - `ryo_file(customer_id: str, invoice_num: str, at: datetime, po_num: str = "125536") -> tuple[str, bytes]`
  - `TZ = ZoneInfo("US/Eastern")`, `now_eastern() -> datetime`
  - `RYO_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S%f"` (shared with the app's parsing)

Format facts (from `suppliers/sas.py:40-47` and `suppliers/ryo.py:50-58`, and real archive samples):
- SAS header line, fixed width, padded with spaces to 80 chars, CRLF: `ASAS` + 7 spaces + `invoice_num` (6 digits) + `invoice_date` (MMDDYY) + `+` + `invoice_total` (9 digits) + `customer_num` (6 digits).
- RYO header line, CRLF: `customer_num|invoice_num|po_num|MM/DD/YYYY hh:mm:ss AM`. Body lines are `item|description|qty|qty|price`.

- [ ] **Step 1: Write the templates (body lines only; headers are generated)**

Create `tests/fixtures/templates/sas_invoice.TXT` with CRLF line endings and exactly these three lines (product codes are synthetic; layout copied from a real archive file):

```
B00000000001MI TEST BRAND A BX FK     10001000001CT000010+000100000001 10001     
B00000000002MI TEST BRAND B 100BX     10002000002CT000010+000200000001 10002     
B00000000003MI TEST BRAND C MN BOX    10003000003CT000010 000000000001 10003     
```

Each line must be exactly 80 characters before the CRLF (pad with trailing spaces).

Create `tests/fixtures/templates/ryo_invoice.txt` with CRLF line endings:

```
10001|TEST ITEM ALPHA 5 FOR 4     5PK/10|1|1|50.39
10002|TEST ITEM BRAVO             24CT|1|1|14.75
10003|TEST ITEM CHARLIE       12PK/10|1|1|53.69
```

To force CRLF regardless of git settings, write both files from Python:

```bash
uv run python - <<'EOF'
from pathlib import Path
sas = [
  "B00000000001MI TEST BRAND A BX FK     10001000001CT000010+000100000001 10001",
  "B00000000002MI TEST BRAND B 100BX     10002000002CT000010+000200000001 10002",
  "B00000000003MI TEST BRAND C MN BOX    10003000003CT000010 000000000001 10003",
]
Path("tests/fixtures/templates").mkdir(parents=True, exist_ok=True)
Path("tests/fixtures/templates/sas_invoice.TXT").write_bytes(b"".join(l.ljust(80).encode() + b"\r\n" for l in sas))
ryo = [
  "10001|TEST ITEM ALPHA 5 FOR 4     5PK/10|1|1|50.39",
  "10002|TEST ITEM BRAVO             24CT|1|1|14.75",
  "10003|TEST ITEM CHARLIE       12PK/10|1|1|53.69",
]
Path("tests/fixtures/templates/ryo_invoice.txt").write_bytes(b"".join(l.encode() + b"\r\n" for l in ryo))
EOF
```

Add a `.gitattributes` line so git never rewrites them: create/append to `.gitattributes` at repo root:

```
tests/fixtures/templates/* -text
```

- [ ] **Step 2: Write the failing generator test**

Create `tests/e2e/test_generator.py`:

```python
# Standard library imports
from datetime import datetime
from typing import Any, cast

# Local imports
from tests.e2e.generator import now_eastern, ryo_file, ryo_filename, sas_file, sas_filename


def test_sas_filename_matches_app_pattern():
  # First party imports
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  now = now_eastern()
  pattern = SASProcessor.assemble_filename_pattern(cast(Any, None), "90001", now, now, True)
  assert pattern.match(sas_filename("90001", now))


def test_sas_header_matches_app_regex():
  # First party imports
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  name, content = sas_file("90001", "252338", now_eastern())
  first_line = content.splitlines()[0].decode()
  match = SASProcessor.invoice_num_pattern.match(first_line)
  assert match and match.group("invoice_num") == "252338"
  assert len(first_line) == 80
  assert content.count(b"\r\n") == 4  # header + 3 template lines


def test_ryo_filename_matches_app_pattern():
  # First party imports
  from scheduled_invoice_processor.suppliers.ryo import RYOProcessor

  now = now_eastern()
  pattern = RYOProcessor.assemble_filename_pattern(cast(Any, None), "9100000001", now, now, True)
  match = pattern.match(ryo_filename("9100000001", "57872", now))
  assert match and match.group("invoice_num") == "57872"
  datetime.strptime(match.group("timestamp"), "%Y%m%d%H%M%S%f")  # noqa: DTZ007


def test_ryo_header_matches_app_regex():
  # First party imports
  from scheduled_invoice_processor.suppliers.ryo import RYOProcessor

  _, content = ryo_file("9100000001", "57872", now_eastern())
  first_line = content.splitlines()[0].decode()
  match = RYOProcessor.invoice_num_pattern.match(first_line)
  assert match
  assert match.group("customer_num") == "9100000001"
  assert match.group("invoice_num") == "57872"
```

- [ ] **Step 3: Run to verify it fails**

Run: `uv run pytest tests/e2e/test_generator.py -v --no-cov`
Expected: FAIL with `ModuleNotFoundError: No module named 'tests.e2e.generator'`.

- [ ] **Step 4: Implement the generator**

Create `tests/e2e/generator.py`:

```python
"""Builds vendor invoice files that the app's filename patterns and header regexes accept.

Filenames carry the timestamp the app matches on (both SAS and RYO set checks_date_in_filename = True), so
`at` must be a US/Eastern datetime inside the current Sun-Sat week; now_eastern() always is.
"""

# Standard library imports
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("US/Eastern")
TEMPLATES = Path(__file__).resolve().parents[1] / "fixtures" / "templates"
RYO_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S%f"
CRLF = b"\r\n"


def now_eastern() -> datetime:
  return datetime.now(TZ)


def _stamp(at: datetime) -> str:
  return at.strftime(RYO_TIMESTAMP_FORMAT)


# --- SAS ---------------------------------------------------------------------------------------------------------


def sas_filename(customer_id: str, at: datetime) -> str:
  return f"EF{customer_id}_{_stamp(at)}.TXT"


def sas_header(invoice_num: str, at: datetime, customer_num: str = "900001", invoice_total: int = 130087) -> str:
  header = f"ASAS       {invoice_num:0>6}{at.strftime('%m%d%y')}+{invoice_total:09d}{customer_num:0>6}"
  return header.ljust(80)


def sas_file(customer_id: str, invoice_num: str, at: datetime) -> tuple[str, bytes]:
  body = (TEMPLATES / "sas_invoice.TXT").read_bytes()
  return sas_filename(customer_id, at), sas_header(invoice_num, at).encode() + CRLF + body


# --- RYO ---------------------------------------------------------------------------------------------------------


def ryo_filename(customer_id: str, invoice_num: str, at: datetime) -> str:
  return f"{customer_id}_{invoice_num}_{_stamp(at)}.txt"


def ryo_header(customer_id: str, invoice_num: str, at: datetime, po_num: str = "125536") -> str:
  return f"{customer_id}|{invoice_num}|{po_num}|{at.strftime('%m/%d/%Y %I:%M:%S %p')}"


def ryo_file(customer_id: str, invoice_num: str, at: datetime, po_num: str = "125536") -> tuple[str, bytes]:
  body = (TEMPLATES / "ryo_invoice.txt").read_bytes()
  return ryo_filename(customer_id, invoice_num, at), ryo_header(customer_id, invoice_num, at, po_num).encode() + CRLF + body
```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/e2e/test_generator.py -v --no-cov`
Expected: 4 PASS.

- [ ] **Step 6: Commit**

```bash
git add .gitattributes tests/fixtures tests/e2e/generator.py tests/e2e/test_generator.py
git commit -m "test(e2e): add sanitized invoice templates and file generator

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Google Sheet helper

**Files:**
- Create: `tests/e2e/sheet.py`
- Modify: `tests/e2e/conftest.py` (add `sheet` fixture and week-flip guard)

**Interfaces:**
- Produces `class TestSheet` (in `sheet.py`, name it `SheetHarness` to avoid pytest collecting it):
  - `SheetHarness(key_file: Path, spreadsheet_id: str)`
  - `seed_orders(supplier: str, orders: Iterable[tuple[int, str]]) -> None` — appends one `Current Week` row per `(store, customer)` with `invoice_grabbed/applied/manually_moved = False`
  - `delete_orders(stores: Iterable[int]) -> None` — deletes every `Current Week` row whose store is in `stores`
  - `schedule_flags(store: int) -> tuple[bool, bool]` — `(invoice_grabbed, invoice_applied)` for the row with that store
  - `log_rows(stores: Iterable[int]) -> list[dict[str, str]]` — Processing Log rows (as dicts keyed by header) whose store is in `stores`
  - `assert_not_near_week_flip() -> None` — raises if now (US/Eastern) is Saturday ≥ 23:00 or Sunday < 01:00
- Sheet layout (from `typing_custom/dataframe_column_names.py`): `Current Week` columns A–J = `supplier, store, customer, state, expected_delivery_day, invoice_pickup_time, invoice_dropoff_time, invoice_grabbed, invoice_applied, manually_moved`; `Processing Log` columns A–I = `supplier, store, invoice_number, customer, action, status, action_datetime, week_end_date, notes`. Row 1 is the header (the app rewrites it on startup). Time cells must match `Weekday H:MM(AM|PM)`, e.g. `Monday 6:00AM`.
- Fixture `sheet` (session): builds the harness from `PERSISTED_DIR / "secrets" / "db-key.json"` and `os.environ["DATABASE_ID"]`, calls `assert_not_near_week_flip()`, deletes any leftover rows for `ALL_RESERVED_STORES` at session start and again at session end.

- [ ] **Step 1: Write sheet.py**

```python
"""gspread access to the TESTING spreadsheet. Only touches rows whose store number is in the reserved e2e set."""

# Standard library imports
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Third party imports
import gspread

SCHEDULE_TAB = "Current Week"
LOG_TAB = "Processing Log"
STORE_COL = 2  # 1-based column index of `store` in both tabs
TZ = ZoneInfo("US/Eastern")


class SheetHarness:
  def __init__(self, key_file: Path, spreadsheet_id: str) -> None:
    self._client = gspread.service_account(filename=str(key_file))
    self._book = self._client.open_by_key(spreadsheet_id)

  # --- schedule rows -------------------------------------------------------------------------------------------------

  def seed_orders(self, supplier: str, orders: Iterable[tuple[int, str]]) -> None:
    rows = [
      [supplier, store, customer, "TX", "Monday", "Monday 6:00AM", "Monday 8:00AM", False, False, False]
      for store, customer in orders
    ]
    self._book.worksheet(SCHEDULE_TAB).append_rows(rows, value_input_option="RAW")

  def delete_orders(self, stores: Iterable[int]) -> None:
    wanted = {str(s) for s in stores}
    ws = self._book.worksheet(SCHEDULE_TAB)
    values = ws.get_all_values()
    # row numbers are 1-based; skip header; delete bottom-up so indices stay valid
    for row_number in sorted(
      (i + 1 for i, row in enumerate(values) if i > 0 and len(row) >= STORE_COL and row[STORE_COL - 1] in wanted),
      reverse=True,
    ):
      ws.delete_rows(row_number)

  def schedule_flags(self, store: int) -> tuple[bool, bool]:
    ws = self._book.worksheet(SCHEDULE_TAB)
    for row in ws.get_all_values()[1:]:
      if len(row) >= 9 and row[STORE_COL - 1] == str(store):
        return row[7].strip().upper() == "TRUE", row[8].strip().upper() == "TRUE"
    raise AssertionError(f"store {store} not found in '{SCHEDULE_TAB}'")

  # --- processing log ------------------------------------------------------------------------------------------------

  def log_rows(self, stores: Iterable[int]) -> list[dict[str, str]]:
    wanted = {str(s) for s in stores}
    values = self._book.worksheet(LOG_TAB).get_all_values()
    header, body = values[0], values[1:]
    return [dict(zip(header, row, strict=False)) for row in body if len(row) >= STORE_COL and row[STORE_COL - 1] in wanted]

  # --- guards --------------------------------------------------------------------------------------------------------

  @staticmethod
  def assert_not_near_week_flip() -> None:
    now = datetime.now(TZ)
    if (now.weekday() == 5 and now.hour >= 23) or (now.weekday() == 6 and now.hour < 1):
      raise AssertionError("Refusing to run e2e within an hour of the Sunday 00:00 week flip (US/Eastern)")
```

- [ ] **Step 2: Add the sheet fixture to conftest**

Append to `tests/e2e/conftest.py`:

```python
@pytest.fixture(scope="session")
def sheet() -> Iterator["SheetHarness"]:
  # Local imports
  from tests.e2e.sheet import SheetHarness

  harness = SheetHarness(PERSISTED_DIR / "secrets" / "db-key.json", os.environ["DATABASE_ID"])
  harness.assert_not_near_week_flip()
  harness.delete_orders(C.ALL_RESERVED_STORES)
  yield harness
  harness.delete_orders(C.ALL_RESERVED_STORES)
```

and add `from tests.e2e.sheet import SheetHarness` under a `TYPE_CHECKING` guard at the top of conftest:

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  # Local imports
  from tests.e2e.sheet import SheetHarness
```

- [ ] **Step 3: Write a small live check**

Create `tests/e2e/test_sheet.py`:

```python
# Local imports
from tests.e2e.sheet import SheetHarness


def test_seed_read_delete_roundtrip(sheet: SheetHarness):
  store, customer = 9999, "99999"
  sheet.seed_orders("SAS", [(store, customer)])
  try:
    assert sheet.schedule_flags(store) == (False, False)
  finally:
    sheet.delete_orders([store])
  assert not any(r for r in sheet.log_rows([store]))  # nothing logged for a never-processed store
```

- [ ] **Step 4: Run it**

Run: `uv run pytest tests/e2e/test_sheet.py -v --no-cov`
Expected: PASS. If `gspread` raises `SpreadsheetNotFound`, the service account in `E2E_DB_KEY_JSON` has not been shared on the testing sheet — share it with edit rights and retry.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/sheet.py tests/e2e/conftest.py tests/e2e/test_sheet.py
git commit -m "test(e2e): add Google Sheet harness for seeding and asserting schedule rows

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: SAS end-to-end cycle

**Files:**
- Create: `tests/e2e/cycle.py`
- Create: `tests/e2e/test_sas_cycle.py`

**Interfaces:**
- Produces `tests/e2e/cycle.py`:
  - `async def run_sas_cycle() -> None` — imports and awaits `scheduled_invoice_processor.suppliers.sas.main()`
  - `async def run_ryo_cycle() -> None` — same for `ryo.main()`
  - `def assert_log_has_full_trail(rows: list[dict[str, str]], invoice_nums: set[str]) -> None` — asserts, for every expected action in `("registered_pickup", "file_picked_up", "registered_dropoff", "file_preprocessed", "file_dropped_off")`, at least one row with `status == "success"` (case-insensitive), and that every `invoice_num` appears in a `file_picked_up` success row.
- `main()` (see `suppliers/sas.py:121-171`) does: `DatabaseCache().refresh_cache()` → `register_pickup` for every SAS row → flush → `pickup_files` → flush → `register_dropoff` → flush → `dropoff_files` → flush. It registers **every** SAS row in `Current Week`, so any non-reserved rows the testing sheet already contains simply produce "No files matched" warnings — harmless.

- [ ] **Step 1: Write cycle.py**

```python
"""Runs the production cycle exactly as each supplier module's main() does, and shared log assertions."""

EXPECTED_ACTIONS = ("registered_pickup", "file_picked_up", "registered_dropoff", "file_preprocessed", "file_dropped_off")


async def run_sas_cycle() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers.sas import main

  await main()


async def run_ryo_cycle() -> None:
  # First party imports
  from scheduled_invoice_processor.suppliers.ryo import main

  await main()


def _norm(value: str) -> str:
  return value.strip().lower()


def assert_log_has_full_trail(rows: list[dict[str, str]], invoice_nums: set[str]) -> None:
  successes = [r for r in rows if _norm(r.get("status", "")) == "success"]
  seen_actions = {_norm(r.get("action", "")) for r in successes}
  missing = [a for a in EXPECTED_ACTIONS if a not in seen_actions]
  assert not missing, f"Processing Log missing success rows for {missing}; rows={rows}"

  picked = {r.get("invoice_number", "").strip() for r in successes if _norm(r.get("action", "")) == "file_picked_up"}
  assert invoice_nums <= picked, f"expected invoices {invoice_nums} in file_picked_up rows, saw {picked}"
```

- [ ] **Step 2: Write the SAS scenario**

Create `tests/e2e/test_sas_cycle.py`:

```python
# Standard library imports
from datetime import timedelta

# Third party imports
import pytest

# Local imports
from tests.e2e import constants as C
from tests.e2e.cycle import assert_log_has_full_trail, run_sas_cycle
from tests.e2e.generator import now_eastern, sas_file
from tests.e2e.remote import FtpBox, SftpBox
from tests.e2e.sheet import SheetHarness

pytestmark = pytest.mark.usefixtures("clean_remote", "reset_processor_singletons")


async def test_sas_cycle(sheet: SheetHarness, sas_box: SftpBox, sft_box: FtpBox):
  # --- arrange: one invoice file per reserved SAS order on the vendor server, one schedule row per order ---
  base = now_eastern()
  uploaded: dict[int, str] = {}  # store -> filename
  invoice_nums: set[str] = set()
  for i, (store, customer) in enumerate(C.SAS_CYCLE_ORDERS):
    invoice = f"{700000 + store}"
    name, content = sas_file(customer, invoice, base + timedelta(seconds=i))
    sas_box.upload(f"{C.SAS_PICKUP_DIR}/{name}", content)
    uploaded[store] = name
    invoice_nums.add(invoice)
  sheet.seed_orders("SAS", C.SAS_CYCLE_ORDERS)

  # --- act: the production cycle ---
  await run_sas_cycle()

  # --- assert: files ---
  vendor_now = sas_box.listdir(C.SAS_PICKUP_DIR)
  for store, name in uploaded.items():
    assert name in vendor_now, "vendor original must be untouched (__debug__ archive is simulated)"
    assert name not in sft_box.listdir(C.SFT_WAITING_SAS), f"{name} should have left /Testing/Waiting/SAS"
    assert name not in sft_box.listdir(C.SFT_PROCESSED_SAS), f"{name} should have left /Testing/Processed/SAS"
    assert name in sft_box.listdir(C.SFT_DEST_SAS), f"{name} should be in /Testing/SAS"
    assert sft_box.read(f"{C.SFT_DEST_SAS}/{name}") == sas_box.read(f"{C.SAS_PICKUP_DIR}/{name}")

  # --- assert: sheet ---
  for store, _ in C.SAS_CYCLE_ORDERS:
    assert sheet.schedule_flags(store) == (True, True), f"store {store} should be grabbed+applied"
  assert_log_has_full_trail(sheet.log_rows([s for s, _ in C.SAS_CYCLE_ORDERS]), invoice_nums)
```

- [ ] **Step 3: Run it**

Run: `uv run pytest tests/e2e/test_sas_cycle.py -v --no-cov -s`
Expected: PASS. The run takes ~1–2 minutes (Sheets API throttling at 1.1 s per call plus four flushes).

Troubleshooting guide for the executor (do **not** change `src/`):
- `No files matched with pattern` in the log → the uploaded filename timestamp is outside the week window or the customer id in the sheet differs from the one in the filename; compare `uploaded` with the sheet row.
- `IndexError: Index provided is either a partial index...` → duplicate `(SAS, store)` rows in `Current Week`; the `sheet` fixture's cleanup didn't run — delete rows for stores 9001/9002 manually and rerun.
- Sheets `429` → wait a minute and rerun; the app backs off but the test harness's own gspread reads do not.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/cycle.py tests/e2e/test_sas_cycle.py
git commit -m "test(e2e): SAS full pickup/dropoff cycle against docker servers and testing sheet

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: RYO end-to-end cycle (merge path)

**Files:**
- Create: `tests/e2e/test_ryo_cycle.py`

**Interfaces:**
- Consumes: everything from Tasks 3–7.
- RYO specifics (from `suppliers/ryo.py:131-415`): two files for one customer are downloaded, merged into one file named `{customer_id}_{inv1-inv2}_{max_timestamp}.txt` with header `{customer}|{inv1-inv2}|{po}|{earliest header date}`, uploaded to `/Testing/Processed/RYO`, originals renamed to `/Testing/Waiting/RYO/Archive`, then the merged file is renamed to `/Testing/RYO`. Invoice order within the name follows directory-listing order, so compare as sets.

- [ ] **Step 1: Write the RYO scenario**

Create `tests/e2e/test_ryo_cycle.py`:

```python
# Standard library imports
from datetime import timedelta

# Third party imports
import pytest

# Local imports
from tests.e2e import constants as C
from tests.e2e.cycle import assert_log_has_full_trail, run_ryo_cycle
from tests.e2e.generator import RYO_TIMESTAMP_FORMAT, now_eastern, ryo_file
from tests.e2e.remote import FtpBox, SftpBox
from tests.e2e.sheet import SheetHarness

pytestmark = pytest.mark.usefixtures("clean_remote", "reset_processor_singletons")


def _parse_merged_name(name: str) -> tuple[str, set[str], str]:
  stem = name.removesuffix(".txt")
  customer, invoices, stamp = stem.split("_")
  return customer, set(invoices.split("-")), stamp


async def test_ryo_cycle(sheet: SheetHarness, ryo_box: SftpBox, sft_box: FtpBox):
  (store, customer), = C.RYO_CYCLE_ORDERS
  base = now_eastern()
  invoice_nums = {"57872", "57873"}
  originals: list[str] = []
  latest_stamp = ""
  for i, invoice in enumerate(sorted(invoice_nums)):
    at = base + timedelta(seconds=i)
    name, content = ryo_file(customer, invoice, at)
    ryo_box.upload(f"{C.RYO_PICKUP_DIR}/{name}", content)
    originals.append(name)
    latest_stamp = at.strftime(RYO_TIMESTAMP_FORMAT)
  sheet.seed_orders("RYO", C.RYO_CYCLE_ORDERS)

  await run_ryo_cycle()

  # vendor originals untouched
  assert set(originals) <= set(ryo_box.listdir(C.RYO_PICKUP_DIR))

  # originals archived on the SFT side, nothing left in waiting/processed
  assert set(originals) <= set(sft_box.listdir(C.SFT_WAITING_RYO_ARCHIVE))
  assert sft_box.listdir(C.SFT_WAITING_RYO) == []
  assert sft_box.listdir(C.SFT_PROCESSED_RYO) == []

  # exactly one merged file in the destination with the expected name parts and header
  dest = sft_box.listdir(C.SFT_DEST_RYO)
  assert len(dest) == 1, dest
  merged_customer, merged_invoices, merged_stamp = _parse_merged_name(dest[0])
  assert merged_customer == customer
  assert merged_invoices == invoice_nums
  assert merged_stamp == latest_stamp

  merged = sft_box.read(f"{C.SFT_DEST_RYO}/{dest[0]}")
  header, *body = merged.split(b"\r\n")
  h_customer, h_invoices, h_po, _h_date = header.decode().split("|")
  assert h_customer == customer
  assert set(h_invoices.split("-")) == invoice_nums
  assert h_po == "125536"
  # 3 template body lines per original file, both files merged, trailing empty element from final CRLF
  assert len([b for b in body if b]) == 6

  assert sheet.schedule_flags(store) == (True, True)
  assert_log_has_full_trail(sheet.log_rows([store]), invoice_nums)
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/e2e/test_ryo_cycle.py -v --no-cov -s`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_ryo_cycle.py
git commit -m "test(e2e): RYO full cycle including merge and archive assertions

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Both suppliers concurrently in one process

**Files:**
- Create: `tests/e2e/test_both_suppliers.py`

**Interfaces:**
- Consumes: Tasks 3–8. Production runs both suppliers' jobs on one scheduler at the same cron minutes, sharing the class-level `waiting_ftp` adapter and the `DatabaseCache` singleton; `asyncio.gather` reproduces that overlap.

- [ ] **Step 1: Write the scenario**

Create `tests/e2e/test_both_suppliers.py`:

```python
# Standard library imports
from asyncio import gather
from datetime import timedelta

# Third party imports
import pytest

# Local imports
from tests.e2e import constants as C
from tests.e2e.cycle import assert_log_has_full_trail, run_ryo_cycle, run_sas_cycle
from tests.e2e.generator import now_eastern, ryo_file, sas_file
from tests.e2e.remote import FtpBox, SftpBox
from tests.e2e.sheet import SheetHarness

pytestmark = pytest.mark.usefixtures("clean_remote", "reset_processor_singletons")


async def test_both_suppliers_same_process(sheet: SheetHarness, sas_box: SftpBox, ryo_box: SftpBox, sft_box: FtpBox):
  base = now_eastern()
  (sas_store, sas_customer), = C.BOTH_SAS_ORDERS
  (ryo_store, ryo_customer), = C.BOTH_RYO_ORDERS

  sas_name, sas_content = sas_file(sas_customer, "700003", base)
  sas_box.upload(f"{C.SAS_PICKUP_DIR}/{sas_name}", sas_content)
  ryo_names = []
  for i, invoice in enumerate(("58001", "58002")):
    name, content = ryo_file(ryo_customer, invoice, base + timedelta(seconds=i))
    ryo_box.upload(f"{C.RYO_PICKUP_DIR}/{name}", content)
    ryo_names.append(name)
  sheet.seed_orders("SAS", C.BOTH_SAS_ORDERS)
  sheet.seed_orders("RYO", C.BOTH_RYO_ORDERS)

  await gather(run_sas_cycle(), run_ryo_cycle())

  assert sas_name in sft_box.listdir(C.SFT_DEST_SAS)
  assert sas_name in sas_box.listdir(C.SAS_PICKUP_DIR)
  assert len(sft_box.listdir(C.SFT_DEST_RYO)) == 1
  assert set(ryo_names) <= set(sft_box.listdir(C.SFT_WAITING_RYO_ARCHIVE))

  assert sheet.schedule_flags(sas_store) == (True, True)
  assert sheet.schedule_flags(ryo_store) == (True, True)
  assert_log_has_full_trail(sheet.log_rows([sas_store]), {"700003"})
  assert_log_has_full_trail(sheet.log_rows([ryo_store]), {"58001", "58002"})
```

- [ ] **Step 2: Run the whole e2e directory**

Run: `uv run pytest tests/e2e -v --no-cov -s`
Expected: all tests PASS (`test_remote`, `test_generator`, `test_sheet`, `test_sas_cycle`, `test_ryo_cycle`, `test_both_suppliers`).

If `test_both_suppliers` fails while the single-supplier tests pass, that is a genuine finding about concurrent use of the shared adapter/cache — record the traceback in the commit message body of a WIP commit and report it; do not patch `src/`.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_both_suppliers.py
git commit -m "test(e2e): run SAS and RYO cycles concurrently in one process

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: GitHub Actions workflow and README

**Files:**
- Create: `.github/workflows/e2e.yml`
- Create: `tests/e2e/README.md`

**Interfaces:**
- Consumes: `tests/docker/compose.yaml`, `tests/e2e/*`.
- Secrets the repository must define (names are load-bearing): `SFTPYPI_USERNAME`, `SFTPYPI_PASSWORD`, `E2E_DB_KEY_JSON`, `E2E_DATABASE_ID`, `E2E_DATABASE_BASE_SCHEDULE_ID`, `E2E_DATABASE_ORDER_LOG_ID`.
- `uv` reads index credentials from `UV_INDEX_SFTPYPI_USERNAME` / `UV_INDEX_SFTPYPI_PASSWORD` (index name `SFTPyPI` in `pyproject.toml` upper-cased).

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/e2e.yml`:

```yaml
name: e2e

on:
  pull_request:
  workflow_dispatch:

# The testing Google Sheet is a shared resource: never run two e2e jobs at once.
concurrency:
  group: e2e-sheet
  cancel-in-progress: false

jobs:
  e2e:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    env:
      UV_INDEX_SFTPYPI_USERNAME: ${{ secrets.SFTPYPI_USERNAME }}
      UV_INDEX_SFTPYPI_PASSWORD: ${{ secrets.SFTPYPI_PASSWORD }}
      E2E_DB_KEY_JSON: ${{ secrets.E2E_DB_KEY_JSON }}
      E2E_DATABASE_ID: ${{ secrets.E2E_DATABASE_ID }}
      E2E_DATABASE_BASE_SCHEDULE_ID: ${{ secrets.E2E_DATABASE_BASE_SCHEDULE_ID }}
      E2E_DATABASE_ORDER_LOG_ID: ${{ secrets.E2E_DATABASE_ORDER_LOG_ID }}
    steps:
      - uses: actions/checkout@v4

      - name: Start server stand-ins
        run: docker compose -f tests/docker/compose.yaml up -d --wait

      - uses: astral-sh/setup-uv@v6
        with:
          enable-cache: true

      - name: Install
        run: uv sync --frozen

      - name: Run e2e suite
        run: uv run pytest tests/e2e -v --no-cov

      - name: Server logs on failure
        if: failure()
        run: docker compose -f tests/docker/compose.yaml logs

      - name: Stop server stand-ins
        if: always()
        run: docker compose -f tests/docker/compose.yaml down -v
```

`--frozen` makes CI fail loudly if `uv.lock` drifts from `pyproject.toml` — which is exactly the guard the `<7` pin is for.

- [ ] **Step 2: Write the README**

Create `tests/e2e/README.md`:

```markdown
# E2E suite

Proves the SAS and RYO pickup/dropoff cycle end to end against Docker stand-ins for the production
servers and the *testing* Google Sheet. Design: `docs/superpowers/specs/2026-08-24-e2e-baseline-test-suite-design.md`.

| Production | Stand-in |
|---|---|
| SFT holding FTP (Pure-FTPd) | `stilliard/pure-ftpd` on 127.0.0.1:21 |
| SAS SFTP (Files.com) | `drakkan/sftpgo` on 127.0.0.1:2022 |
| RYO SFTP (Bitvise) | `drakkan/sftpgo` on 127.0.0.1:2222 |
| Google Sheet | the testing spreadsheet (live) |

## Running in CI

`.github/workflows/e2e.yml` runs on every pull request and via *Run workflow*. Required repository secrets:

- `SFTPYPI_USERNAME`, `SFTPYPI_PASSWORD` — internal package index (for `aeth-ext`)
- `E2E_DB_KEY_JSON` — service-account key JSON (must have edit access to the testing sheet)
- `E2E_DATABASE_ID`, `E2E_DATABASE_BASE_SCHEDULE_ID`, `E2E_DATABASE_ORDER_LOG_ID` — testing sheet id and tab gids

## Running locally (best effort)

```bash
export E2E_DB_KEY_JSON="$(cat path/to/db-key.json)"
export E2E_DATABASE_ID=... E2E_DATABASE_BASE_SCHEDULE_ID=... E2E_DATABASE_ORDER_LOG_ID=...
docker compose -f tests/docker/compose.yaml up -d --wait
uv run pytest tests/e2e -v --no-cov
docker compose -f tests/docker/compose.yaml down -v
```

The suite refuses to run within an hour of the Sunday 00:00 (US/Eastern) week flip.

## What the suite deliberately does not cover

Failure paths, the APScheduler wiring, Coremark, and vendor-exact server software (Files.com / Bitvise).
```

- [ ] **Step 3: Push and watch CI**

```bash
git add .github/workflows/e2e.yml tests/e2e/README.md
git commit -m "ci: add e2e workflow running the suite against docker stand-ins

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push -u origin test/e2e-baseline
```

Then open a PR from `test/e2e-baseline` to `main` (`gh pr create --fill --base main`) and confirm the `e2e` check passes. If the workflow fails at *Install* with an index auth error, the two `SFTPYPI_*` secrets are missing. If it fails in `conftest` with `e2e suite needs environment variable`, the corresponding `E2E_*` secret is missing.

- [ ] **Step 4: Report**

When CI is green, report the PR URL and the run time of the e2e job. The branch is then ready to merge to `main`; the v8 migration branch rebases onto it afterwards.

---

## Self-review notes (already applied)

- Spec coverage: pin cap (T1), dev deps (T1), Docker stand-ins with writable root + session cap (T2), sanitized templates + generator (T5), sheet seeding/cleanup/flip guard (T6), three scenarios with the spec's assertions (T7–T9), CI with concurrency group and secrets (T10), no `aeth_ext` imports in tests (all tasks), no `src/` changes (singleton reset done in T4 from test code).
- The `reset_processor_singletons` fixture relies on `SingletonType.__shared_instance__`, which is the attribute name in both aeth_ext 6.3.1 and the v8.0.0-dev branch (verified 2026-08-24).
- Known non-goal surfaced by the trace: `_dropoff_files` returns early when `_file_preprocess_queue` is empty; every scenario registers dropoff in the same run, so the path is never hit here.
