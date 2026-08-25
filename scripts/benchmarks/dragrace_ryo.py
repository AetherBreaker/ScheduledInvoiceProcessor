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
from scheduled_invoice_processor.monkey_patches import Patches

Patches.patch_the_monkey()

# Third party imports
from rich import get_console

# First party imports
from aeth_ext.rich.progress import Progress
from scheduled_invoice_processor.database import DatabaseCache
from scheduled_invoice_processor.environment_init_vars import SETTINGS
from scheduled_invoice_processor.suppliers import SupplierProcessorBase
from scheduled_invoice_processor.suppliers.ryo import RYOProcessor
from scheduled_invoice_processor.typing_custom.dataframe_column_names import DatabaseScheduleColumns as Cols
from scheduled_invoice_processor.typing_custom.enums import SuppliersEnum

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
          cache.schedule._field_type_adapters["invoice_grabbed"],
        )
        await cache.schedule.write_value(
          (SuppliersEnum.RYO, order.store),
          Cols.invoice_applied,
          before[order.store][1],
          cache.schedule._field_type_adapters["invoice_applied"],
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
