# Tests

Unit tests live beside the code in each crate. This directory is for the tests
that need more than one component:

- `fix_conformance` — **exists (M1d)**: QuickFIX/J plus a byte-level probe
  against the real `fix-gateway` (logon, order/report, resend with
  `43`/`122`, admin gap fill, over-window logout). Run
  `./tests/fix_conformance/run.sh`; see that directory's README.
- `differential` — the same command stream against `engine-core` and a naive
  `Vec` order book, compared event by event.
- `replay` — the same log twice; compare book hash, trade hash, and `md_seq`.
- `recovery` — snapshot, kill -9, restart, compare state.
- `marketdata` — forced gaps via the driver's loss generator; recovery must
  fill them and `md_seq` must not go backwards.

Only `fix_conformance` exists today; the rest are fixed layout for later
milestones.
