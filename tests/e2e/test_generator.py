# Standard library imports
from datetime import datetime
from typing import Any, cast

# First party imports
# Local imports
from tests.e2e.generator import now_eastern, ryo_file, ryo_filename, sas_file, sas_filename


def test_sas_filename_matches_app_pattern():
  # First party imports
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  now = now_eastern()
  pattern = SASProcessor.assemble_filename_pattern(cast("Any", None), "90001", now, now, True)
  assert pattern.match(sas_filename("90001", now))


def test_sas_header_matches_app_regex():
  # First party imports
  from scheduled_invoice_processor.suppliers.sas import SASProcessor

  _name, content = sas_file("90001", "252338", now_eastern())
  first_line = content.splitlines()[0].decode()
  assert SASProcessor.invoice_num_pattern is not None
  match = SASProcessor.invoice_num_pattern.match(first_line)
  assert match is not None
  assert match.group("invoice_num") == "252338"
  assert len(first_line) == 80
  assert content.count(b"\r\n") == 4  # header + 3 template lines


def test_ryo_filename_matches_app_pattern():
  # First party imports
  from scheduled_invoice_processor.suppliers.ryo import RYOProcessor

  now = now_eastern()
  pattern = RYOProcessor.assemble_filename_pattern(cast("Any", None), "9100000001", now, now, True)
  match = pattern.match(ryo_filename("9100000001", "57872", now))
  assert match is not None
  assert match.group("invoice_num") == "57872"
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
