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
