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

# Store used by test_sheet.py's seed/read/delete roundtrip probe.
PROBE_STORE = 9999

ALL_RESERVED_STORES: frozenset[int] = frozenset(
  store for orders in (SAS_CYCLE_ORDERS, RYO_CYCLE_ORDERS, BOTH_SAS_ORDERS, BOTH_RYO_ORDERS) for store, _ in orders
) | {PROBE_STORE}
