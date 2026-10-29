# ADR 0003: Order identity, matching and allocation

- Status: **Accepted** — 2026-10-25, implemented by M2
- Date: 2026-10-25
- Deciders: project owner

## Context

The design (§5) makes `OrderId` the slab slot and splits orders into hot and
cold fields. The review (P0-3) found that a slot is reused after a terminal
order, so a bare slot is not a stable FIX `OrderID`: a client holding an old id
could cancel a different order. M2 also has to settle the matching rules, the
`ClOrdID` idempotency window and the allocation claim.

## Decision

1. **Packed order identity.** `OrderId = generation << 24 | slot`, generation
   `1..=254` (255 is reserved so a packed id never equals `EMPTY_ORDER_ID`).
   A lookup checks `orders[slot].id == id` before using it. The level links and
   the free list store the bare slot index with `u32::MAX` as the sentinel.
   The FIX `OrderID(37)` is the packed id rendered as a decimal string.
2. **Fixed slab.** The slab, cold table, generation table and the
   `(account, ClOrdID)` window are allocated once at engine construction. An
   order beyond the slab capacity is rejected with `BookFull`.
3. **Price-time priority.** Price levels are `BTreeMap`s (M6 replaces them with
   a price band and a bitmap); each level is a FIFO doubly linked list. Bids
   are matched highest first, asks lowest first.
4. **TIF.** DAY and GTC limit remainders rest. IOC cancels the remainder.
   FOK checks the fillable quantity first and rejects the order completely if
   it cannot fill. A market order never rests: its remainder is canceled even
   when its TIF is DAY/GTC. DAY orders die when the cluster service offers
   `Command::ExpireDayOrders` (a service message sequenced through the log at
   the trading-day boundary); GTC orders are untouched. The DAY flag is part
   of the order's snapshot state.
5. **Post-only.** A post-only limit order that would cross is rejected; it
   never takes liquidity. Post-only with a market order is invalid.
6. **Self-trade.** `None` allows the trade; `CancelResting` cancels the
   same-account maker and keeps matching; `RejectTaker` refuses the incoming
   order before any fill.
7. **Replace.** The original stays in place when the price is unchanged and
   the new quantity is not larger: only the level quantity changes and queue
   priority is kept. Any other change unlinks the order, applies the new terms,
   gives it a new queue position and matches it as a taker. A refused replace
   leaves the original untouched.
8. **Idempotency.** The `(account, ClOrdID)` window is a fixed-capacity,
   open-addressed table with tombstones; the oldest entry is evicted at the
   configured bound. A repeated new order for an order that was **accepted**
   replays the original acknowledgement (same `OrderID`, `ExecType=New`,
   `OrdStatus=New`) instead of rejecting it: a gateway retry after a lost
   report must not turn a live order into a failure. A repeated new order for
   one that was **rejected at acceptance** answers `DuplicateClOrdID`,
   because the window does not keep the original reject reason. A repeated
   cancel of a canceled order answers the terminal state. Rejected new orders
   are recorded too, so a retry does not become a second order.
9. **Risk** is a pure function checked before matching, in a fixed order:
   duplicate id, symbol, kill switch, quantity, notional, price band, fat
   finger, open-order limit, self-trade, FOK. Cancels are allowed while a kill
   switch is active; replaces are not.
10. **No wall clock.** The only ordering input is the cluster log position;
    `last_trade` and `md_seq` are state. The engine emits
    `(log_pos, event_seq)` on every event; that pair is the FIX `ExecID` and
    the market-data dedupe key.
11. **Allocation.** Slab and id-window operations after construction are
    allocation-free; inserting a **new** price level allocates. The M2 test
    installs a counting allocator and asserts zero allocations for matching at
    an existing level and for canceling after warmup.

## Consequences

- M4's snapshot must save the book, the terminal snapshots and the id window;
  the slab generation is part of an order's identity.
- The id window is bounded: an id older than the window can be reused by the
  client, which is the design's accepted window semantics.
- Duplicate **cancel/replace request** ids are not separately deduped in M2;
  the original order's state makes a repeated cancel safe, and the gateway's
  FIX sequence numbers make duplicate replaces a later concern (M4).
- M6 can replace the `BTreeMap` levels without changing the engine's public
  commands or events; the priced band removes the remaining new-level
  allocation.
