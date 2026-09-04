# TODO

- [ ] **HIGH PRIORITY — required before adopting the next aeth-devkit Docker standard.** `HOLDING_FOLDER`
  (`suppliers/__init__.py`, `CWD / "file_holding"`) is an entirely ephemeral scratch directory but lives
  beside the code under `/app`. The standardized image keeps `/app` root-owned and runs the app as
  `nonroot`, and the entrypoint no longer honours `[tool.docker].mkdirs`, so `HOLDING_FOLDER.mkdir()` in
  `startup.py` will raise `PermissionError` at container start. Move it to a temp directory
  (`tempfile.mkdtemp()` / `Path(tempfile.gettempdir())`), then delete `mkdirs` from `[tool.docker]`.

- [ ] **Filename date windows: stop matching year/month/day as a cross product.** `assemble_filename_pattern` in
  the SFT, RYO and SAS processors builds the date part of the regex as independent `(year)(month)(day)`
  alternations, so a week that straddles a month boundary admits dates outside it -- e.g. for Sun 2025-08-31 ..
  Sat 2025-09-06 the pattern is `(08|09)(31|01|...|06)`, which also accepts 08-01 .. 08-06 and 09-31. Those files
  are picked up and only *reported* by the `[OUTSIDE_WEEK_PICKUP]` diagnostic. Fix: emit one full `YYYYMMDD`
  alternative per day in the window (`20250831|20250901|...|20250906`) so the regex matches exactly the week.
  `tests/unit/test_sft_processor.py::test_month_straddling_week_admits_cross_products_that_the_diagnostic_reports`
  pins the current behaviour and should be inverted when this lands.
