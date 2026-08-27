"""Break one real vendor->main transfer into per-operation timings, to find where the seconds go.

Usage (from the repo root, real `.env` with the *testing* DATABASE_ID and USE_TESTING_FOLDERS=True):

  uv run --frozen python scripts/benchmarks/profile_single_transfer.py [--repeat N] [--out out.json]

`dragrace_ryo.py` measured ~5 s per file on the vendor (SFTP) -> SFT waiting (FTP) path. The invoices
on that path are 100 B - 3 KB, so essentially none of that is payload time: it is per-file fixed
cost, spread across pool acquisition, SFTP metadata calls, and the FTP control-connection handshake
around `STOR`. This script attributes it.

Rather than hand-placing timers inside aeth_ext, it wraps the protocol methods on `SFTPClient`,
`SFTPFile` and `FTP` and tallies (count, total seconds) per method, then runs the real
`transfer_file` call. Nothing in aeth_ext or SIP is modified.

**Times are inclusive.** `FTP.voidcmd` calls `sendcmd` calls `putcmd`/`getresp`, so an outer entry
contains its inner ones; read the call *counts* alongside, and compare siblings rather than summing
the column. Phase timers (the `>>` rows) are non-overlapping and do sum.

Reads one real invoice from the vendor and writes it to the *testing* waiting folder under a
throwaway name, deleting it afterwards; cleanup runs even if the transfer raises. The vendor side is
read-only -- the archive step that would rename a file there is never invoked. Manual only.
"""

# Standard library imports
import argparse
import asyncio
import json
import os
import shutil
import tempfile
import time
from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Callable, Generator

parser = argparse.ArgumentParser(description="Attribute the per-file cost of one vendor->main transfer.")
parser.add_argument("--repeat", type=int, default=3, help="Transfers to profile (default 3); the first is reported separately.")
parser.add_argument("--out", type=Path, default=None, help="Optional JSON output path.")
ARGS = parser.parse_args()

REPO = Path.cwd()
PERSISTED = Path(tempfile.mkdtemp(prefix="profile-transfer-"))
shutil.copytree(REPO / "persisted_data" / "secrets", PERSISTED / "secrets")
os.environ["PERSISTED_DIR_LOC"] = str(PERSISTED)
os.environ["USE_TESTING_FOLDERS"] = "True"

# The app reads SETTINGS and the secrets at import time, so these imports must follow the environment setup above.
# First party imports
from scheduled_invoice_processor.monkey_patches import Patches

Patches.patch_the_monkey()

# Standard library imports
from ftplib import FTP

# Third party imports
from paramiko import SFTPClient, SFTPFile

# First party imports
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor

# name -> [call count, inclusive seconds]
TALLY: defaultdict[str, list[float]] = defaultdict(lambda: [0, 0.0])
PHASES: list[tuple[str, float]] = []
_ENABLED = False


def _instrument(cls: type, *names: str) -> None:
  """Wraps each named method to tally (count, inclusive seconds) under `Cls.method`.

  Applied once at import; `_ENABLED` gates recording so the warm-up transfer and the pool's own
  lazy connection setup do not pollute the numbers for the run being measured.
  """
  for name in names:
    original: Callable[..., Any] = getattr(cls, name)

    def make(original: Callable[..., Any] = original, key: str = f"{cls.__name__}.{name}") -> Callable[..., Any]:
      @wraps(original)
      def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not _ENABLED:
          return original(*args, **kwargs)
        started = time.perf_counter()
        try:
          return original(*args, **kwargs)
        finally:
          entry = TALLY[key]
          entry[0] += 1
          entry[1] += time.perf_counter() - started

      return wrapper

    setattr(cls, name, make())


# The FTP control connection is the prime suspect: every `voidcmd`/`transfercmd` is a command plus a
# blocking wait for its reply, and `STOR` needs several before a byte moves.
_instrument(FTP, "connect", "login", "sendcmd", "voidcmd", "putcmd", "getresp", "voidresp", "transfercmd", "ntransfercmd", "size", "delete")
_instrument(SFTPClient, "stat", "open", "listdir", "listdir_attr", "normalize", "remove", "rename")
_instrument(SFTPFile, "read", "close", "prefetch", "stat")


@contextmanager
def _phase(name: str) -> Generator[None]:
  started = time.perf_counter()
  try:
    yield
  finally:
    PHASES.append((name, time.perf_counter() - started))


def _report(title: str) -> dict[str, Any]:
  print(f"\n=== {title} ===")
  phase_total = sum(secs for _, secs in PHASES)
  print(f"{'phase (non-overlapping)':<44} {'secs':>8}  {'share':>7}")
  for name, secs in PHASES:
    print(f"  >> {name:<40} {secs:>8.3f}  {secs / phase_total * 100 if phase_total else 0:>6.1f}%")
  print(f"  >> {'TOTAL':<40} {phase_total:>8.3f}")

  print(f"\n{'operation (INCLUSIVE - see docstring)':<44} {'calls':>6} {'secs':>8}  {'per call':>9}")
  for key, (count, secs) in sorted(TALLY.items(), key=lambda item: -item[1][1]):
    if not count:
      continue
    print(f"  {key:<42} {int(count):>6} {secs:>8.3f}  {secs / count:>9.4f}")

  return {
    "phases": [{"phase": name, "secs": round(secs, 4)} for name, secs in PHASES],
    "phase_total_secs": round(phase_total, 4),
    "operations": {key: {"calls": int(count), "secs": round(secs, 4)} for key, (count, secs) in TALLY.items() if count},
  }


def _profile_one(ryo: RYOProcessor, source_remote: str, dest_path: str) -> None:
  """Runs one instrumented transfer, filling `PHASES`/`TALLY`.

  Extracted only because PLR0915 fires on `main()` otherwise. The sessions are entered by hand
  rather than with `with`, so acquisition and release land in their own phases instead of being
  folded into the transfer.
  """
  global _ENABLED
  _ENABLED = True
  try:
    with _phase("acquire SFTP session (vendor)"):
      source_cm = ryo.vendor_ftp.start_session()
      source = source_cm.__enter__()
    try:
      with _phase("acquire FTP session (waiting)"):
        dest_cm = ryo.waiting_ftp.start_session()
        dest = dest_cm.__enter__()
      try:
        with _phase("transfer_file"):
          source.transfer_file(source_remote_path=source_remote, dest_remote_path=dest_path, other=dest, mem_stream=BytesIO())
      finally:
        with _phase("release FTP session"):
          dest_cm.__exit__(None, None, None)
    finally:
      with _phase("release SFTP session"):
        source_cm.__exit__(None, None, None)
  finally:
    _ENABLED = False


async def main() -> dict[str, Any]:

  ryo = RYOProcessor(None)
  ryo.vendor_ftp.pbar = None
  ryo.waiting_ftp.pbar = None

  waiting = ryo.pre_processing_waiting_folder.as_posix()
  if not waiting.startswith("/Testing/"):
    raise SystemExit(f"Refusing to run: destination {waiting} is not under /Testing. Set USE_TESTING_FOLDERS=True.")
  if not __debug__:
    raise SystemExit("Refusing to run under -O: the /Testing folder rewrite needs __debug__.")

  pickup = ryo.pickup_ftp_folder.as_posix()
  archive_name = ryo.pickup_archive_ftp_folder.name
  created: list[str] = []
  runs: list[dict[str, Any]] = []

  try:
    with ryo.vendor_ftp.start_session() as vendor:
      # `Archive` is a *directory* in the pickup folder; streaming it as a file hangs.
      names = [entry.filename for entry in vendor.listdir(pickup) if entry.filename != archive_name]
      if not names:
        raise SystemExit(f"No files in {pickup}; nothing to profile.")
      source_name = names[0]
      source_size = vendor.get_size(f"{pickup}/{source_name}")

    print(f"source : {pickup}/{source_name}  ({source_size:,} B)")
    print(f"dest   : {waiting}")

    for run_idx in range(ARGS.repeat):
      TALLY.clear()
      PHASES.clear()
      dest_path = f"{waiting}/profile-r{run_idx}-{source_name}"
      _profile_one(ryo, f"{pickup}/{source_name}", dest_path)
      created.append(dest_path)
      runs.append({"run": run_idx, **_report(f"run {run_idx}{' (cold: includes pool/connection setup)' if not run_idx else ''}")})
  finally:
    if created:
      with ryo.waiting_ftp.start_session() as dest:
        for path in created:
          try:
            dest.remove(path)
          except Exception as exc:  # noqa: BLE001 - best-effort; a leftover must not mask the result
            print("cleanup skip", path, exc)

  return {"source": source_name, "source_bytes": source_size, "runs": runs}


if __name__ == "__main__":
  result = asyncio.run(main())
  if ARGS.out:
    ARGS.out.write_text(json.dumps(result, indent=2))
