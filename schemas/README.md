# SBE schemas

The command, event and market-data families are hand-written `wire.rs`
formats, not SBE:

| Family | Carries |
|---|---|
| command | `NewOrder`, `CancelOrder`, `ReplaceOrder`, `KillSwitch`, `Snapshot` |
| event | `OrderAccepted`, `OrderRejected`, `ExecutionReport`, `Trade`, `BookUpdate`, `Snapshot` |
| market data | incremental `BookUpdate` / `Trade`, periodic `Snapshot` |

The engine snapshot is the one SBE contract:

| Schema | id / version | Generated crate |
|---|---|---|
| `deepbook-snapshot.xml` | 200 / 1 | `crates/codec/snapshot` (`deepbook-codec-snapshot`) |

Generation:

- `just gen-snapshot` runs `scripts/gen-snapshot-sbe.sh`, which uses the pinned
  `sbe-tool` 1.40.2 and `agrona` 2.6.1 with the Rust target and validates
  against `fpl/sbe.xsd`.
- The generated `src/` is committed; the crate's `Cargo.toml` is hand-written.
  Regeneration replaces `src/` wholesale, so nothing hand-written may go there.
- CI regenerates with the pinned jars and fails if the tree moves.
- `fpl/sbe.xsd` is copied verbatim from FIX Protocol Limited. Its licence
  (CC BY-ND 4.0) permits redistribution unchanged and nothing more; keep the
  header.

Rules:

- Fixed length, little-endian. No strings: `symbol` / `account` are ids.
- `(log_pos, event_seq)` is the global event id; it is the FIX `ExecID` and the
  market-data de-duplication key.
- `md_seq` is assigned by the engine, monotonic per symbol, and is part of the
  snapshot.
- `ClOrdID` / `OrigClOrdID`: **do not hard-code 20 bytes.** FIX 4.4 itself does
  not impose a 20-byte limit (that is a FIX 4.2-era number); venues commonly
  allow 36–40. Choose a fixed width of 36 or 40, or reject longer ids at the
  gateway explicitly. Truncation would make two distinct client ids collide in
  the idempotency key. Schema 200 currently fixes 20 bytes to match
  `ClientOrderId::LENGTH`; widening it is a separate wire change and a schema
  version bump.
- Schema evolution is append-only: new fields at the end of a block, new enum
  values at the end, never reordering or retyping.
