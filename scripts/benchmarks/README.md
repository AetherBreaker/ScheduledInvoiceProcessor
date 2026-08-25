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
