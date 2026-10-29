# ADR 0007: journal durability policies and the M5 bench

- Status: **Accepted** — 2026-10-25, implemented by M5a
- Date: 2026-10-25
- Deciders: project owner

## Context

ADR 0002 fixed M1's durability policy: every outbound FIX message is appended
to the session journal and `fdatasync`ed before the socket sees it, and the
inbound/outbound sequence state is written through a temporary file and an
atomic rename with a directory sync. That is three sync operations per
message. The M5 bench showed the price directly:

- on a disk-backed session directory (`/tmp` in WSL2), the accept loop
  managed about 120 reports/s and the client's RTT grew to seconds;
- on `/dev/shm`, the cluster path managed about 5.5k orders/s at a 10k/s
  offered rate, with p50 1.07 ms and p99 1.76 ms.

The design asks M5 to measure batching and to document how much a crash can
lose. It also needs a client-side RTT table that is not distorted by
coordinated omission.

The bullet points above are the WSL2 development machine. On the Linux bench
machine the M1 policy keeps up with the same offered rate and the difference
shows in the tail instead; "Consequences" carries that measurement.

## Decision

1. **`SyncPolicy` is explicit and default-preserving.** `deepbook_fix::journal`
   has `Each` (the M1 policy), `Every(n)` (one sync per `n` appends) and
   `AfterMillis(n)` (sync when the interval elapsed). `FileJournal::open`
   and `FileSequenceStore::new` keep `Each`; the batching constructors are
   opt-in. A time policy on a slow device degenerates to one sync per append
   (the sync itself outlasts the interval), so the bench uses `Every(64)`:
   the loss window is bounded by **63 outbound messages**, not by a clock.
2. **The sequence store batches with the same policy.** It keeps the latest
   `Sequences` in memory and writes the atomic file when due. On recovery the
   session already takes the larger of the journal's next sequence and the
   stored one, so a lagging state file cannot move the stream backwards; the
   worst case is a duplicate inbound replay, which the engine's
   `(account, ClOrdID)` window answers idempotently.
3. **The window is closed on disconnect.** `Session::flush` forces both
   stores, and `Drop` calls it, so a logout or a connection replacement does
   not leave a deferred write behind. `FileJournal::clear` (a `141=Y` reset)
   also resets the counters.
4. **The bench is open-loop and measures at the client.** `fix-bench` queues
   an IOC buy at an absolute send deadline, records `t0` at queueing time and
   `t1` when the first report for its `ClOrdID` is decoded. Falling behind
   shows up in the sample rather than silently lowering the send rate.
   Percentiles are nearest-rank on the sorted samples. The binary prints a
   table or one JSON object for scripts.
5. **One-click reproduction.** `deploy/bench.sh` prints the machine
   fingerprint (kernel, CPU, rustc, commit, binary digests,
   `io_uring_disabled`), starts driver + module + `engine-container` +
   `fix-gateway --cluster` in a `/dev/shm` run directory, runs the bench and
   tears the stack down (or keeps it with `--keep`). The gateway exposes
   `--tick-us` (the poll ceiling), `--journal-sync each|batch` and
   `--journal-batch N`; the defaults are still 100 ms and `Each`.

## Consequences

- The default deployment keeps the ADR 0002 promise. A deployment that wants
  throughput asks for `--journal-sync batch` and accepts the documented
  window; the crash story is "at most 63 unsynced reports", not "no loss".
- The bench's numbers are client-side and include the gateway's tick and the
  cluster round trip. On the Linux bench machine (2026-10-29, deepmsg
  `4bddeef`, `/dev/shm`, 10k/s offered, 20k samples, default 200 µs tick) both
  policies keep up with the offered rate, so batching buys the tail rather than
  the rate:

  | journal policy | achieved | p50 | p99 | p99.9 |
  |---|---|---|---|---|
  | `each` (M1) | 10.0k orders/s | 205 µs | 3.31 ms | 6.20 ms |
  | `batch 64` | 10.0k orders/s | 197 µs | 234 µs | 1.40 ms |

  p99 falls by 14× and p99.9 by 4.4×. The WSL2 run of the same comparison also
  showed a throughput difference (5.5k against 9.8k orders/s); that did not
  reproduce here, so the claim this ADR rests on is the tail, which does.
- `deploy/bench.sh` uses `/dev/shm` for the session directories so a tmpfs
  `fdatasync` does not dominate. A run on a physical disk is a different
  measurement and must set `BENCH_RUN_DIR` explicitly.
- The M5 io_uring backend, the admin/UI and Grafana are separate slices; this
  ADR covers the measurement foundation they will be compared on.
