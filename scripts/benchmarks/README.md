# Benchmarks

Manual, real-server benchmarks. Never run from CI: they need the real credentials in `persisted_data/secrets`,
a `.env` pointing at the *testing* Google Sheet with `USE_TESTING_FOLDERS=True`, and they write to the holding
server's `/Testing` tree (which they clean up afterwards).

## dragrace_ryo.py — before/after connection pooling

    uv run --frozen python scripts/benchmarks/dragrace_ryo.py after-v8.0.0 scripts/benchmarks/dragrace_after.json

`dragrace_before.json` is the aeth-ext 6.3.1 baseline (2026-08-25, 7 files): per-file mean 5.35 s, max 5.45 s,
`pickup_files` 10.8 s, whole cycle 40.5 s. Compare **per-file mean/max** (the file count depends on what is in the
vendor folder that day) and the `pickup_files` wall time (proxy for one concurrent transfer wave). Results are
recorded in the "Drag race" section of `docs/superpowers/specs/2026-08-25-aeth-ext-v8-migration-design.md`.

## dragrace_sftp_pipelining.py — A/B the SFTP request-pipelining change

    uv run --frozen python scripts/benchmarks/dragrace_sftp_pipelining.py pipelining scripts/benchmarks/dragrace_pipelining.json

Races three arms over the RYO vendor (SFTP) → SFT waiting (FTP) path — `_sftp_to_ftp`, what
`_transfer_file_vend_to_main` actually calls: `legacy-8k` (prefetch/pipelining neutered, the pre-PR
behaviour), `pipelined-8k` (aeth_ext `perf/sftp-pipelined-transfers`), and `pipelined-32k` (that plus
the `chunk_size` bump the PR deferred). Compare **`by_arm[*].median_secs`** and `speedup_vs_legacy`.

Two design differences from `dragrace_ryo.py`, both deliberate:

- **Arms interleave round-robin inside one process**, rather than being separate whole-cycle runs.
  `dragrace_before/after.json` were taken ~90 minutes apart on different aeth_ext versions; on a path
  where a single transfer takes seconds and is dominated by network round trips, drift between two
  scheduled runs is the same order as the effect. Interleaving removes it.
- **The legacy arm is produced by stubbing `SFTPFile.prefetch`/`set_pipelined`**, not by checking out
  the old aeth_ext. Every arm therefore runs on one branch, so the comparison isolates this change
  rather than every difference between two releases.

No database rows or spreadsheet cells are touched — it writes throwaway names into the testing
waiting folder and deletes them. `--files` / `--rounds` control sample count (default 4 × 3 = 12
transfers per arm); the largest vendor files are chosen, since the change removes a per-chunk cost
and a file too small to span many chunks cannot show it.

`dropoff_files` in `dragrace_ryo.py` is a useful negative control: it is main→main (FTP→FTP), a path
this change does not touch, so it should stay flat.
