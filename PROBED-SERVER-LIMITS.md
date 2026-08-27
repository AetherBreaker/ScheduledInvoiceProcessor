# Probed server limits

Connection limits measured directly against each production server on **2026-08-27**, from one
client IP, using live credentials from `persisted_data/secrets/`. Recorded here so the pool
maximums in each supplier processor can be revisited once the `perf/ftp-optimization` work in
`aeth_ext` lands in `main` and ships a release.

## Measurements

| Server | Software | Port | Max channels / Transport | Max concurrent Transports | Round trip |
| ------ | -------- | ---- | ------------------------ | ------------------------- | ---------- |
| RYO vendor SFTP (`sftp.ryodist.com`) | Bitvise SSH Server 9.66 | 2222 | **10** (11th refused) | **19** (20th refused) | 108 ms |
| SAS vendor SFTP (`sassftp.sasinc.com`) | Files.com | 22 | ≥20 (no refusal) | ≥24 (no refusal) | 45 ms |
| SFT holding FTP (`178.63.233.44`) | Pure-FTPd | 21 | n/a (FTP has no channels) | **50 globally**, no per-IP cap | 125 ms |

Only the **bold** numbers are true ceilings. SAS's figures are probe caps, not limits — it refused
nothing at either level, so its real maximum is unknown and higher than what is listed.

## What is configured today

| Processor | `max_connections` | `channels_per_transport` |
| --------- | ----------------- | ------------------------ |
| `RYOProcessor` | 18 | not set — see below |
| `SASProcessor` | 20 | not set — see below |
| `CoremarkProcessor` | 16 (library default) | n/a (FTP) |
| `SupplierProcessorBase.waiting_ftp` (SFT holding) | 16 | n/a (FTP) |

**`channels_per_transport=10` needs to be re-added to `RYOProcessor` and `SASProcessor` once
`aeth_ext`'s `perf/ftp-optimization` work lands in `main` and ships a release.** The measurements
below (10 for RYO, ≥20 for SAS) are still valid — the kwarg was pulled from both
`create_ftp_adapter(...)` calls because the current released `aeth_ext` doesn't support it yet, not
because the numbers changed.

## Notes that affect these numbers

- **RYO's 19 is shared.** It is a per-server concurrency cap, not a per-program allowance. Anything
  else connecting to this vendor competes for the same 19, so 18 leaves exactly one spare slot.
- **The SFT holding FTP has no meaningful per-IP limit.** 48 concurrent logins were accepted from a
  single IP before hitting the server-wide 50-user cap (`421 50 users (the maximum) are already
  logged in`). One client can starve the pool for every sibling program on that host, so the 16
  there is politeness, not a server-enforced bound. It also disconnects after 15 min idle.
- **Channel count does not degrade SAS.** Per-operation latency was flat (~85–92 ms) from 2 to 20
  concurrent channels on one Transport, with throughput scaling ~linearly to 211 op/s. There is a
  single fixed ~2x step going from 1 to 2 channels (45 ms → 87 ms) that does not compound; that
  looks like paramiko's single Transport reader thread rather than the server, but was not isolated.
- **`channels_per_transport` adapts downward on its own.** The pool pins a lower per-Transport cap
  when a server refuses a channel open (`TransportState.channel_cap`), so overshooting costs one
  refused open per Transport rather than breaking. It never probes back upward.
- **Bitvise's refusal arrives as an auth failure.** RYO's 20th Transport fails with
  `AuthenticationException: transport shut down or saw EOF`, not a clean capacity error. It
  reproduced at exactly 19 across two runs and left no lingering block, so it is a concurrency cap
  rather than rate limiting — but it is not self-describing, and a future change in its wording or
  behaviour would be easy to misread as a credentials problem.

## Reproducing

The probes were one-off scripts, not committed. Each opens connections one at a time until refused:
channels via repeated `SFTPClient.from_transport()` on a single `paramiko.Transport`, Transports via
repeated `Transport.connect()`, and FTP logins via repeated `ftplib.FTP.login()`. Re-probe before
raising any maximum — vendors change these without notice, and the SAS figures in particular are
lower bounds that were never pushed to failure.
