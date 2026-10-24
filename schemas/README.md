# SBE schemas

Three message families, frozen at v0 in M0:

| Family | Carries |
|---|---|
| command | `NewOrder`, `CancelOrder`, `ReplaceOrder`, `KillSwitch`, `Snapshot` |
| event | `OrderAccepted`, `OrderRejected`, `ExecutionReport`, `Trade`, `BookUpdate`, `Snapshot` |
| market data | incremental `BookUpdate` / `Trade`, periodic `Snapshot` |

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
  the idempotency key.
- The deepmsg SBE generator is hard-coded to its own five schemas
  (`crates/tools/src/bin/gen-sbe-codecs.rs`). Generating deepbook codecs needs
  either an extension of that tool or a local generator; decide at M0 before
  writing the schemas.
