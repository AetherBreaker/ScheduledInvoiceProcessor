"""The Sheets session must absorb socket-level failures and never wait unbounded on the event loop.

A `ConnectionResetError` on the 02:30 `refresh_cache` run (2026-09-03) escaped straight to the fatal handler because
gspread's `BackOffHTTPClient` only retries HTTP status codes, and the default requests adapter carries zero retries
and no timeout.
"""

# pyright: reportPrivateUsage=false

# Standard library imports
from types import SimpleNamespace

# First party imports
from scheduled_invoice_processor import database


def _client_from_fresh_state() -> object:
  # `DatabaseCache.__init__` needs a running loop and touches the network via `update_db_header`; the property only
  # needs these attributes, so drive its getter on a stand-in and let it build the real gspread client.
  cls = database.DatabaseCache
  state = SimpleNamespace(
    _client=None,
    _client_last_auth_time=None,
    loop=SimpleNamespace(time=lambda: 0.0),
    reauth_interval=cls.reauth_interval,
    _creds=cls._creds,
    api_timeout=cls.api_timeout,
    api_retry=cls.api_retry,
  )
  return cls.client.fget(state)  # pyright: ignore[reportOptionalCall]


def test_sheets_session_retries_connect_and_read_errors_without_touching_status_backoff() -> None:
  client = _client_from_fresh_state()
  retry = client.http_client.session.get_adapter("https://sheets.googleapis.com/").max_retries  # pyright: ignore[reportAttributeAccessIssue]
  assert retry.connect and retry.connect > 0
  assert retry.read and retry.read > 0
  assert retry.status == 0
  assert "GET" in retry.allowed_methods
  assert "POST" not in retry.allowed_methods  # a lost batchUpdate response must never be replayed


def test_sheets_requests_carry_a_finite_timeout() -> None:
  client = _client_from_fresh_state()
  connect, read = client.http_client.timeout  # pyright: ignore[reportAttributeAccessIssue]
  assert connect > 0
  assert read > 0
