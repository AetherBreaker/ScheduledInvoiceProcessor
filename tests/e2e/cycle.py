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
