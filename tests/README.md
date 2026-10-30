# Tests

Unit tests live beside the code in each crate. This directory is for the tests
that need more than one component:

- `fix_conformance` — **exists (M1d)**: QuickFIX/J plus a byte-level probe
  against the real `fix-gateway` (logon, order/report, resend with
  `43`/`122`, admin gap fill, over-window logout). Run
  `./tests/fix_conformance/run.sh`; see that directory's README.
- `differential` — **exists (M2)** at
  `crates/engine-core/tests/differential.rs`: proptest streams through the slab
  engine and a naive `Vec` oracle, comparing trades and the book view. The
  allocator report is `crates/engine-core/tests/allocation.rs`.
- `replay` — **not implemented (layout only)**: the same log twice; compare
  book hash, trade hash, and `md_seq`.
- `recovery` — **exists**: snapshot, kill -9, restart, compare state; see
  `crates/marketdata/tests/recovery.rs`,
  `crates/fix/tests/session_recovery.rs` and the cluster e2e's engine-host
  restart.
- `marketdata` — **exists (M3)** at `crates/marketdata/tests/deepmsg_loss.rs`:
  the production media driver with its send-side loss generator, a
  `reliable=false` incremental subscription, and recovery through the
  request/response channel. The pure recovery path is
  `crates/marketdata/tests/recovery.rs`.
- `cluster_e2e` — **exists (M4c)** at `crates/gateway/tests/cluster_e2e.rs`:
  a real media driver, consensus module, `engine-container` and
  `fix-gateway --cluster`, a snapshot, `SIGKILL` of the container, a restart
  and a continuation fill through the same FIX session (epoll and io_uring).
  `just engine-e2e` runs it locally; the `cluster-e2e` workflow runs the epoll
  variant on pull requests and nightly against the pinned deepmsg revision.

`fix_conformance`, the engine's differential/allocation tests, the market data
recovery tests (`crates/marketdata/tests/recovery.rs`), the FIX session
recovery test (`crates/fix/tests/session_recovery.rs`) and the cluster e2e
exist today; the replay harness is still layout.
