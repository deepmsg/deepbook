# ADR 0005: Engine snapshot state and record

- Status: **Accepted** — 2026-10-25, implemented by M4a/M4b (the gateway cluster client and e2e are M4c)
- Date: 2026-10-25
- Deciders: project owner

## Context

M4 puts `engine-core` behind deepmsg's `ClusteredService`. The cluster takes a
snapshot between commands and replays the log after it. The snapshot must
restore enough state that the book hash, the idempotency window and future
order identities match the uninterrupted engine. The deployment configuration
(symbol, capacity, window size, risk limits) is not state and must not be
silently changed by a snapshot.

## Decision

1. **Snapshot at a command boundary.** `on_take_snapshot` runs between
   commands, so the engine state is self-consistent. The snapshot records
   `last_log_pos`; replay starts after it.
2. **State in the snapshot.**
   - the order book: used slab slots (hot fields + ClOrdID), the generation
     counter for **every** slot (free slots included), the free-list order and
     head, the bid/ask levels, `last_trade` and `md_seq`;
   - the `(account, ClOrdID)` window in insertion order, including terminal
     records;
   - per-account open-order counts and the global/per-account kill switches;
   - `last_log_pos`.
3. **Configuration is not state.** `Engine::restore(config, snapshot)` takes
   the deployment's `EngineConfig` and validates the symbol and slab capacity
   against the snapshot. Risk limits, window capacity and the symbol come from
   the deployment, not the record.
4. **Identity is preserved.** Slot indices, generation counters and the free
   list order are restored, so a new order after recovery gets the same packed
   `OrderId` it would have received without the restart. The ClOrdID window is
   restored, so a client retry after recovery is rejected as a duplicate rather
   than becoming a second order.
5. **Record format.** `engine-cluster` owns the `DBS1` version-1 little-endian
   record: magic, version, counts, fixed fields. Counts are bounded; a short,
   foreign or inconsistent record is refused. SBE may replace the record later
   without changing `EngineSnapshot`.
6. **Restore is tested against the uninterrupted engine.** A snapshot taken
   mid-stream, encoded and decoded, then restored, must have the same
   `book_hash`, `view`, `md_seq` and `last_log_pos`; continuing the same
   commands on both engines must produce identical events. A reused ClOrdID and
   a per-account kill switch are checked after restore.

## Consequences

- The snapshot size grows with the number of used orders plus one generation
  byte per slab slot; the free list is explicit. This is the price of identical
  future order ids. M6 can compact it once the allocation scheme changes.
- `engine-cluster`'s `ClusteredService` adapter (`service.rs`) calls
  `Engine::snapshot` in `on_take_snapshot` and `Engine::restore` in `on_start`;
  the `engine-container` binary hosts it.
- The gateway's cluster client and the single-node process e2e are M4c.
