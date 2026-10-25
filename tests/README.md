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
- `replay` — the same log twice; compare book hash, trade hash, and `md_seq`.
- `recovery` — snapshot, kill -9, restart, compare state.
- `marketdata` — **exists (M3)** at `crates/marketdata/tests/deepmsg_loss.rs`:
  the production media driver with its send-side loss generator, a
  `reliable=false` incremental subscription, and recovery through the
  request/response channel. The pure recovery path is
  `crates/marketdata/tests/recovery.rs`.

`fix_conformance`, the engine's differential/allocation tests and the market
data recovery tests exist today; the rest are fixed layout for later
milestones.
