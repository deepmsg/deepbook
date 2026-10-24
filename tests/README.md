# Tests

Unit tests live beside the code in each crate. This directory is for the tests
that need more than one component:

- `fix_conformance` — QuickFIX/J against the gateway (logon, heartbeat, resend,
  gap fill, application messages).
- `differential` — the same command stream against `engine-core` and a naive
  `Vec` order book, compared event by event.
- `replay` — the same log twice; compare book hash, trade hash, and `md_seq`.
- `recovery` — snapshot, kill -9, restart, compare state.
- `marketdata` — forced gaps via the driver's loss generator; recovery must
  fill them and `md_seq` must not go backwards.

M0 has none of these yet. The directory exists so the layout is fixed before
the first test is written.
