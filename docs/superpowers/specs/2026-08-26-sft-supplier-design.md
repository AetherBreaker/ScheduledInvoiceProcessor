# SFT Warehouse Supplier — Design

**Date:** 2026-08-26
**Branch:** `feature/sft-supplier`
**Status:** Approved design; FTP folder paths pending

## Goal

Migrate SFT's own warehouse invoices onto the ScheduledInvoiceProcessor pipeline as a
new supplier, `SFT`. The warehouse already exports to the existing SFT FTP server, so
the vendor side and the holding side are the *same* server: pickup becomes a rename
plus a download (for header inspection), not a cross-server copy.

## Decisions made

| Topic         | Decision                                                                                                                                                      |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Template      | Skeleton copied from `SASProcessor`; merge logic copied verbatim from `RYOProcessor` into `sft.py` (no mixin yet — extract later if the two prove identical). |
| Vendor FTP    | `vendor_ftp = SupplierProcessorBase.waiting_ftp` — same adapter object. No new credentials file, no `Settings` change.                                        |
| File content  | Same pipe-delimited format as RYO. Header line: `customer\|invoice_num\|po_num\|date`.                                                                        |
| Filename      | `{customer_id}_{invoice_num}.edi`, e.g. `SFT017_13842.edi`. No timestamp.                                                                                     |
| Date window   | Taken from the **header line date**, not filename and not mtime (mtime can be poisoned by human intervention).                                                |
| Merged output | `{customer_id}_{inv1}-{inv2}-{inv3}.edi`; header format unchanged from RYO. Downstream consumer is **not** assumed to be RYO's.                               |
| Folder paths  | Placeholders now; real values supplied later.                                                                                                                 |

## Components

### `typing_custom/enums.py`
Add `SuppliersEnum.SFT = "SFT"`.

### `startup.py`
Add `SuppliersEnum.SFT: SFTProcessor` to `expected_suppliers`.

### `suppliers/sft.py` — `SFTProcessor(SupplierProcessorBase)`

Class attributes:

```python
vendor_ftp = SupplierProcessorBase.waiting_ftp   # same server
queue_backup_prefix = "sft"
supplier_name = SuppliersEnum.SFT
identifier_prefix = "SFT"
checks_date_in_filename = False   # base mtime path is bypassed by the _pickup_files override anyway

invoice_num_pattern = compile(
  r"^(?P<customer_num>[^|]+)\|"
  r"(?P<invoice_num>\d+)\|"
  r"(?P<po_num>[^|]*)\|"
  r"(?P<invoice_date>\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} [AP]M)$"
)
header_date_format = "%m/%d/%Y %I:%M:%S %p"   # tolerant of non-padded 6/19/2025 9:46:46 AM
header_format = "{customer_num}|{invoice_num}|{po_num}|{invoice_date}"
file_name_format = "{customer_id}_{invoice_num}.edi"

# ============================================================================
# !!! PLACEHOLDER FTP PATHS — MUST BE REPLACED BEFORE THIS SUPPLIER IS ENABLED !!!
# The real pickup/dropoff locations on the SFT FTP server have not been decided.
# ============================================================================
pickup_ftp_folder            = PurePosixPath("/TODO_SFT/Pickup")
pickup_archive_ftp_folder    = PurePosixPath("/TODO_SFT/Pickup/Archive")
pre_processing_waiting_folder  = PurePosixPath("/TODO_SFT/Waiting")
pre_processing_archive_folder  = PurePosixPath("/TODO_SFT/Waiting/Archive")
post_processing_waiting_folder = PurePosixPath("/TODO_SFT/Processed")
destination_ftp_folder         = PurePosixPath("/TODO_SFT/Destination")
```

Plus the SAS-style `log_file_loc`, `ctx_var_identifier`, `ctx_var_log_loc`, and a
`__post_init__` creating `SFT_files/pre_processing` and `SFT_files/post_processing`.

Testing mode (`__debug__ and SETTINGS.use_testing_folders`) prefixes `/Testing` to
**all six** folders (SAS prefixes only four because its pickup folders live on the
vendor server; here every folder is on the SFT server).

`assemble_filename_pattern` returns `^{customer_id}_(?P<invoice_num>[\d\-]+)\.edi$`
(`[\d\-]+` so merged files re-match, as in RYO). The date args are ignored.

### Pickup — `_pickup_files` override

Copy-and-modify of the base method. Steps:

1. List `pickup_ftp_folder`; match filenames against each pickup-queue entry's pattern.
2. For each filename match, download the file to a `BytesIO` (off-thread). Parse line 1
   with `invoice_num_pattern`; parse `invoice_date` with `header_date_format` and
   localise to `SETTINGS.tz` (header carries no offset).
3. Keep the file only if the header date lies in the Sun–Sat window the base computes
   from `pickup_date`, `dropoff_date`, and `current_week` (same arithmetic as the mtime
   branch at `suppliers/__init__.py` `_pickup_files`). Unparseable header or
   out-of-window → `warning`, file untouched, not added to `items_to_dl`.
4. For kept files, transfer is a **rename** `pickup_ftp_folder/name → remote_file_locs[idx]`
   on the shared adapter, using the same `_already_moved` idempotency as
   `_transfer_file_main_to_main`. `extract_invoice_num` is fed the bytes already
   downloaded in step 2 — no second read. Implemented as a new method
   `_transfer_file_same_server(...)` with the same signature/contract as
   `_transfer_file_vend_to_main` (sets `pickup_success[idx]`, advances progress,
   calls `log_action_handler`).
5. Commit (sheet check-box) → move to waiting queue → persist → archive via
   `_vendor_archive_file` (already a rename). Unchanged from base.

### Preprocess / merge

`_preprocess_files`, `_preprocess_off_thread`, `_create_new_merged_file` copied from
`RYOProcessor`. Only differences: `invoice_num_pattern`, `file_name_format`, and the
`.edi` extension. RYO already joins invoice numbers with `-`.

### Dropoff

Inherited; `_transfer_file_main_to_main` already renames within the SFT FTP.

## Error handling

- Header unparseable / out of window: warn, skip; queue entry stays in the pickup
  queue and retries next run (same as a no-match today).
- Download failure: transient errors retried with the base backoff
  (`_is_transient_transfer_error`); others logged, file skipped this run.
- Rename failure: `_already_moved` check; if already at the waiting path → success;
  otherwise log, `pickup_success[idx] = False`, entry not advanced.
- Merge / dropoff / archive: inherited RYO/base behaviour.

## Testing

Unit (`tests/unit/test_sft_processor.py`):
- Header regex matches `SFT017|13842|49273|6/19/2025 9:46:46 AM` and a zero-padded variant.
- Header date parse + window filter: file with correct header date but wrong mtime is
  kept; file with in-window mtime but out-of-window header is skipped.
- Filename pattern matches `SFT017_13842.edi` and `SFT017_13842-13843.edi`, rejects
  `SFT017_13842.txt` and `SFT018_13842.edi`.
- Merged filename output for two inputs.
- `_transfer_file_same_server` with a fake adapter: success, already-moved, failure.

E2E (`tests/e2e/test_sft_cycle.py`): mirrors `test_ryo_cycle.py`, fixture
`testing_files/SFT017_13842.edi`. **Blocked until real folder paths exist.**

## Prerequisites outside this repo

- Schedule sheet must contain `SFT` rows per store (`check_box((supplier, storenum), ...)`
  is keyed on the enum value).
- The six real FTP folders must exist on the SFT server before enabling.

## Pending inputs

| Input                    | Owner | Where it lands                |
| ------------------------ | ----- | ------------------------------ |
| Six FTP folder paths     | Jacob | `sft.py` — search `TODO_SFT`  |
| Confirm `SFT` sheet rows | Jacob | Google Sheet                  |
| Unskip e2e cycle test    | Jacob | tests/e2e/test_sft_cycle.py   |

## Out of scope

- Extracting RYO's merge logic into a shared mixin (revisit once SFT is stable).
- Changing `CoremarkProcessor` or its absence from `expected_suppliers`.
