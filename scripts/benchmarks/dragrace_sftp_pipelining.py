"""Drag race: A/B the SFTP request-pipelining change (aeth_ext `perf/sftp-pipelined-transfers`).

Usage (from the repo root, real `.env` with the *testing* DATABASE_ID and USE_TESTING_FOLDERS=True):

  uv run --frozen python scripts/benchmarks/dragrace_sftp_pipelining.py <label> <out.json> [--files N] [--rounds N]

Measures `AdaptedSFTP.transfer_file` on the RYO vendor (SFTP) -> SFT waiting (FTP) path -- i.e.
`_sftp_to_ftp`, which is what `_transfer_file_vend_to_main` actually calls -- across three arms:

  legacy-8k      prefetch/pipelining neutered, chunk_size 8192   (pre-PR behaviour)
  pipelined-8k   prefetch active, chunk_size 8192                (the PR)
  pipelined-32k  prefetch active, chunk_size 32768               (the PR + the deferred chunk bump)

Unlike `dragrace_ryo.py`, which compared two whole-cycle runs taken ~90 minutes apart on two
different aeth_ext versions, the arms here are **interleaved round-robin inside one process on one
branch**. Transfers on this path take seconds and are dominated by network round trips, so drift
between separately-scheduled runs is the same order as the effect being measured; interleaving
removes it. Running every arm on one branch also means the comparison isolates *this* change rather
than every difference between two releases.

The legacy arm is produced by neutering `SFTPFile.prefetch`/`set_pipelined` (see `_legacy_sftp`), not
by checking out the old code -- same binary, same everything else, one variable.

Touches no database rows and no spreadsheet: it transfers real vendor files into the testing waiting
folder under throwaway names and deletes them again. `<out>.partial.json` is written before cleanup
on both the success and failure paths, and cleanup runs even if an arm raises. Manual only -- never
run this from CI, and never against non-testing folders.
"""

# Standard library imports
import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime
from importlib.metadata import version
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  # Standard library imports
  from collections.abc import Generator

parser = argparse.ArgumentParser(description="A/B the SFTP request-pipelining change.")
parser.add_argument("label")
parser.add_argument("out_path", type=Path)
parser.add_argument("--files", type=int, default=4, help="How many of the largest vendor files to race (default 4).")
parser.add_argument("--rounds", type=int, default=3, help="Times each arm transfers each file (default 3).")
ARGS = parser.parse_args()

REPO = Path.cwd()
PERSISTED = Path(tempfile.mkdtemp(prefix=f"dragrace-pipelining-{ARGS.label}-"))
shutil.copytree(REPO / "persisted_data" / "secrets", PERSISTED / "secrets")
os.environ["PERSISTED_DIR_LOC"] = str(PERSISTED)
os.environ["USE_TESTING_FOLDERS"] = "True"

# The app reads SETTINGS and the secrets at import time, so these imports must follow the environment setup above.
# First party imports
from scheduled_invoice_processor.monkey_patches import Patches

Patches.patch_the_monkey()

# Third party imports
from paramiko import SFTPFile

# First party imports
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor

_REAL_PREFETCH = SFTPFile.prefetch
_REAL_SET_PIPELINED = SFTPFile.set_pipelined


@contextmanager
def _legacy_sftp() -> Generator[None]:
  """Restores pre-PR paramiko behaviour: every read and write costs its own round trip.

  `prefetch` and `set_pipelined` are the only two levers the PR pulls, so stubbing them reproduces
  the old timings without needing the old code checked out -- which keeps every other difference
  between releases out of the comparison.
  """
  # Signatures must match paramiko's exactly -- names included: pyright treats any deviation as an
  # incompatible assignment to the class attribute.
  SFTPFile.prefetch = lambda self, file_size=None, max_concurrent_requests=None: None
  SFTPFile.set_pipelined = lambda self, pipelined=True: None
  try:
    yield
  finally:
    SFTPFile.prefetch = _REAL_PREFETCH
    SFTPFile.set_pipelined = _REAL_SET_PIPELINED


@contextmanager
def _stock_sftp() -> Generator[None]:
  yield


# (name, chunk_size, patch). Ordered so the round-robin below alternates legacy/pipelined rather than
# running each arm as a block -- a block would re-admit the drift this design exists to rule out.
_ARMS: tuple[tuple[str, int, Any], ...] = (
  ("legacy-8k", 8192, _legacy_sftp),
  ("pipelined-8k", 8192, _stock_sftp),
  ("pipelined-32k", 32768, _stock_sftp),
)


def _summarize(samples: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
  """Collapses the raw samples into per-arm statistics, each scored against the legacy baseline."""
  by_arm: dict[str, dict[str, Any]] = {}
  for arm_name, _, _ in _ARMS:
    secs = [sample["secs"] for sample in samples if sample["arm"] == arm_name]
    rates = [sample["kb_per_sec"] for sample in samples if sample["arm"] == arm_name]
    if not secs:
      continue
    by_arm[arm_name] = {
      "n": len(secs),
      # Median, not mean: one retried or congested transfer would otherwise dominate an arm.
      "median_secs": round(statistics.median(secs), 3),
      "mean_secs": round(statistics.fmean(secs), 3),
      "stdev_secs": round(statistics.stdev(secs), 3) if len(secs) > 1 else 0.0,
      "min_secs": round(min(secs), 3),
      "max_secs": round(max(secs), 3),
      "median_kb_per_sec": round(statistics.median(rates), 1),
    }
  baseline = by_arm.get("legacy-8k", {}).get("median_secs")
  for stats in by_arm.values():
    stats["speedup_vs_legacy"] = round(baseline / stats["median_secs"], 2) if baseline else None
  return by_arm


def main() -> dict[str, Any]:
  # No pbar: a per-chunk redraw would tax the 8k arms hardest, which is the opposite of the bias we want.
  ryo = RYOProcessor(None)
  ryo.vendor_ftp.pbar = None
  ryo.waiting_ftp.pbar = None

  samples: list[dict[str, Any]] = []
  created: list[str] = []
  candidates: list[tuple[str, int]] = []

  try:
    with ryo.vendor_ftp.start_session() as vendor:
      pickup = ryo.pickup_ftp_folder.as_posix()
      sized = [(entry.filename, vendor.get_size(f"{pickup}/{entry.filename}")) for entry in vendor.listdir(pickup)]
    # Largest first: the change removes a fixed cost *per chunk*, so a file too small to span many
    # chunks cannot show the effect however real the effect is.
    candidates = sorted(((name, size) for name, size in sized if size), key=lambda pair: -pair[1])[: ARGS.files]
    if not candidates:
      raise SystemExit(f"No sized files found in {pickup}; nothing to race.")

    for round_idx in range(ARGS.rounds):
      for arm_name, chunk_size, patch in _ARMS:
        ryo.vendor_ftp.chunk_size = chunk_size
        ryo.waiting_ftp.chunk_size = chunk_size
        for filename, size in candidates:
          dest_path = f"{ryo.pre_processing_waiting_folder.as_posix()}/dragrace-{ARGS.label}-{arm_name}-r{round_idx}-{filename}"
          with patch(), ryo.vendor_ftp.start_session() as source, ryo.waiting_ftp.start_session() as dest:
            started = time.perf_counter()
            success = source.transfer_file(
              source_remote_path=f"{ryo.pickup_ftp_folder.as_posix()}/{filename}",
              dest_remote_path=dest_path,
              other=dest,
              mem_stream=BytesIO(),
            )
            elapsed = time.perf_counter() - started
          created.append(dest_path)
          rate = size / 1024 / elapsed
          samples.append(
            {
              "arm": arm_name,
              "round": round_idx,
              "file": filename,
              "bytes": size,
              "secs": round(elapsed, 3),
              "kb_per_sec": round(rate, 1),
              # A false here means the three-way size check failed: the timing is meaningless, and
              # the transfer itself is broken. Surfaced in `all_transfers_verified` below.
              "success": success,
            }
          )
          print(f"{arm_name:>14}  r{round_idx}  {filename[:40]:<40} {size:>9,}B  {elapsed:6.2f}s  {rate:8.1f} KB/s")
  finally:
    # Written before cleanup on both the success and failure paths, so a cleanup failure -- or a
    # failed arm -- never loses the numbers captured so far.
    ARGS.out_path.with_suffix(".partial.json").write_text(json.dumps({"samples": samples}, indent=2))
    if created:
      with ryo.waiting_ftp.start_session() as dest:
        for path in created:
          try:
            dest.remove(path)
          except Exception as exc:  # noqa: BLE001 - best-effort; a leftover file must not mask the result
            print("cleanup skip", path, exc, file=sys.stderr)

  return {
    "label": ARGS.label,
    "aeth_ext": version("aeth-ext"),
    "when": datetime.now(SETTINGS.tz).isoformat(timespec="seconds"),
    "files": [{"file": name, "bytes": size} for name, size in candidates],
    "rounds": ARGS.rounds,
    "all_transfers_verified": all(sample["success"] for sample in samples),
    "by_arm": _summarize(samples),
    "samples": samples,
  }


if __name__ == "__main__":
  result = main()
  ARGS.out_path.write_text(json.dumps(result, indent=2))
  print(json.dumps({key: value for key, value in result.items() if key != "samples"}, indent=2))
