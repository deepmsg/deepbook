# ADR 0012: multi-seat routing and the DeepOUCH binary gateway

- Status: **Proposed**
- Date: 2026-11-27
- Deciders: project owner
- Review record: `analysis/deepbook/review-2026-11-27-ouch-adr.md`

## Context

`deepbook` M0–M6 established a deterministic single-symbol matching engine
(`engine-core`, upgraded to a cache-line-aligned price band and radix-64 bitmap
in ADR 0011), an `AeronCluster` service adapter (`engine-cluster`), and a
single-threaded `epoll`/`io_uring` gateway (`fix-gateway`). Two architectural
limits remain in the order-entry path (ADR 0006, `crates/gateway/src/cluster.rs`):

1. **Single-peer gateway binding.** The gateway hosts exactly one FIX
   `SenderCompID`/`TargetCompID` pair over one cluster session. Engine egress
   records carry an `AccountId` but no gateway route, so a passive Maker fill
   for a seat on another connection or another gateway has nowhere to go.
2. **ASCII protocol tax on the hot path.** FIX 4.4 requires tag-value scanning,
   decimal ASCII integer parsing and formatting, `BodyLength(9)` framing,
   `CheckSum(10)` accumulation, and header rewriting (`43=Y`,
   `122=OrigSendingTime`) on resend. Latency-sensitive liquidity providers use
   fixed-layout binary protocols (Nasdaq OUCH / SoupBinTCP, CME iLink 3).

This ADR defines multi-seat routing across `engine-cluster` and `gateway`,
and **DeepOUCH/1.0**, a fixed-width little-endian binary order-entry protocol
served by the same single-threaded event loop, with a pre-registered ABBA
comparison against FIX 4.4.

The design deliberately does **not** touch the `engine-core` state machine or
its snapshot schema. `engine-core` gains one read-only enumeration helper
(§2.8) and the gateway/`fix` crates gain protocol fields; the matching state,
the `DBS2` SBE schema, and the `DBS3` envelope's nesting of `DBS2` are unchanged.

## Decision

### 1. Strict seat ownership and a single sequence authority

1. **1 Seat = 1 Account.** Each configured seat (`seat_id: u16`, non-zero)
   owns exactly one `AccountId: u32`; an account belongs to at most one seat.
   Firms running several strategies provision several seats/accounts.
2. **The gateway stamps the account.** Inbound DeepOUCH/FIX frames never carry
   a trusted account. The gateway fills `Command`'s account from the authenticated
   seat; `EngineService` re-validates `command.account() == seat.account_id` and
   drops a mismatch (§3.5).
3. **`seat_seq` is assigned only by `EngineService`.** Every outbound order
   event for a seat gets a monotonically increasing `seat_seq: u64`, starting
   at 1, in consensus-log order. Gateways never mint business sequence numbers.
4. **Malformed input goes through the log.** A gateway must not synthesize a
   local business reject that consumes `seat_seq`. Semantic errors that can be
   parsed become `ServiceIngress::RejectMalformed` (§3.2) and are answered
   through the same outbox. Framing errors are a connection-level
   `Logout(ProtocolViolation)` and never enter the log (§5.6).
5. **Seat pins are immutable and permanent in v1.** `BindSeat` carries both
   `seat_id` and `account_id`; the first bind pins the pair in replicated state
   (persisted in `DBS3`). Re-binding the same seat to another account, or the
   same account to another seat, is refused with `BindRejected`. Decommissioning
   drains a seat and leaves an empty pin; reusing a seat id for a different
   account is an out-of-scope state migration, not a config edit.
6. **Static protocol per seat.** `seats.toml` fixes each seat to `fix` or
   `ouch`. A seat never switches protocol while it has open orders, unacknowledged
   outbox entries, or a live connection. Changing the protocol is a flat-seat,
   config-rollout, restart operation (§4.8).

### 2. Service envelopes and the protocol-neutral outbox

One `fix-gateway` process keeps **one** cluster session and multiplexes all its
seats over service envelopes. `engine-core` commands are wrapped unchanged.

#### 2.1 Envelope headers

Ingress and egress share one 8-byte header (all integers little-endian):

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `magic` | `u8` | `0xDB` |
| 1 | `version` | `u8` | `1` |
| 2 | `kind` | `u8` | see below |
| 3 | `reserved` | `u8` | `0` |
| 4 | `payload_len` | `u32` | bytes after this header |

A legacy bare `engine_core::wire` command begins with its own version byte `1`
and never with `0xDB`; the service distinguishes the two encodings.

**Ingress kinds**

| kind | name | payload |
|---|---|---|
| 1 | `BindSeat` | `seat_id: u16`, `account_id: u32`, `last_journal_seq: u64` |
| 2 | `SeatCommand` | `seat_id: u16`, `epoch: u64`, then `engine_core::wire::encode_command` bytes |
| 3 | `AckSeat` | `seat_id: u16`, `epoch: u64`, `up_to_seq: u64` |
| 4 | `RejectMalformed` | `seat_id: u16`, `epoch: u64`, `raw_cl_ord_id: u64`, `raw_orig_cl_ord_id: u64`, `is_cancel_or_replace: u8`, `reason: u8` (§5.6) |
| 5 | `UnbindSeat` | `seat_id: u16`, `epoch: u64`, `reason: u8` |

`SeatCommand` is dropped without side effects if the epoch is stale, the seat
is unbound, or the wrapped command is service-only (`Snapshot`,
`ExpireDayOrders`, `KillSwitch { account: None }`). `Command` gains a small
`account()` accessor for the validation.

**Egress kinds**

| kind | name | payload |
|---|---|---|
| 10 | `SeatBound` | `seat_id: u16`, `epoch: u64`, `generation: u64`, `replay_from_seq: u64`, `next_seq: u64`, `resynced: u8` |
| 11 | `BindRejected` | `seat_id: u16`, `reason: u8` (`1 = AccountConflict`, `2 = UnknownSeat`, `3 = InvalidSeat`) |
| 12 | `SeatEvent` | see §2.2 |
| 13 | `FenceEvicted` | `seat_id: u16`, `new_epoch: u64`, `reason: u8` (`1 = SupersededByNewBind`, `2 = OutboxOverflow`) |

#### 2.2 `SeatEvent` body

| Offset | Field | Type |
|---|---|---|
| 0 | `seat_id` | `u16` |
| 2 | `epoch` | `u64` |
| 10 | `generation` | `u64` |
| 18 | `seat_seq` | `u64` |
| 26 | `liquidity` | `u8` (`0 = None`, `1 = Maker`, `2 = Taker`) |
| 27 | `payload_kind` | `u8` (`0 = EngineEvent`, `1 = MalformedReject`) |
| 28 | `flags` | `u8` (bit0 `resync`, bit1 `replace_req_present`) |
| 29 | optional `replace_req_cl_ord_id` | `[u8; 20]` when bit1 |
| … | `payload_len` | `u32` |
| … | `payload` | bytes |

For `payload_kind = 0`, the payload is the exact
`engine_core::wire::encode_event` encoding of `Event::ExecutionReport`
(≤ 106 B) or `Event::CancelReject` (≤ 66 B). For `payload_kind = 1`, the
payload is `raw_cl_ord_id: u64`, `raw_orig_cl_ord_id: u64`,
`is_cancel_or_replace: u8`, `reason: u8` (18 B).

`Event::Trade` (two accounts) and `Event::BookUpdate` (no account) never enter
an outbox; they continue through `publish_market_data`.

#### 2.3 Outbox entry

```text
OutboxEntry {
    seat_seq: u64,
    liquidity: u8,          // 0 for service rejects and resync events
    payload_kind: u8,       // 0 engine event, 1 malformed reject
    flags: u8,              // resync, replace_req_present
    replace_req_cl_ord_id: Option<ClientOrderId>,
    payload: Vec<u8>,       // engine wire event, or the 18-byte reject body
}
```

`replace_req_cl_ord_id` is attached **only** to the `ExecType::Replaced`
report produced by a `ReplaceOrder` command (not to later `Trade` reports from
the same command). The engine keeps the original order and its original
`cl_ord_id` (`engine.rs:845-890`), so this field is what lets both protocols
correlate the replacement acknowledgement with the request.

`SeatOutbox` tracks `seat_id`, `account_id`, `epoch`, `generation`,
`acked_seq`, `send_cursor`, `next_seq`, `buffered_bytes`, `overflowed`, and
`entries: VecDeque<OutboxEntry>`. It replaces `EngineService::pending_egress`
for all seat-routed traffic; live transmission advances `send_cursor` only
after `cluster.offer` succeeds and retries on Aeron back-pressure.

#### 2.4 Liquidity derivation

Within one applied command, the service scans the emitted `Event::Trade`
records and tags the order-state reports:

- `ExecutionReport(ExecType::Trade)` with `order_id == trade.maker_order_id`
  → `liquidity = 1`;
- the matching taker report → `liquidity = 2`;
- every other report, service reject, and resync event → `liquidity = 0`.

The scan is order-independent: collect trades first, then tag reports.

### 3. Acknowledgements, fencing, and budgets

#### 3.1 Durability-gated ACKs and clamping

- A gateway emits `AckSeat(up_to_seq)` (batched every 256 events or 50 ms)
  **only after** the corresponding journal records satisfy the seat journal's
  `SyncPolicy` (ADR 0007), never after a page-cache write.
- The service ignores `AckSeat` with a stale epoch and clamps
  `effective_ack = up_to_seq.min(next_seq.saturating_sub(1))`. The clamp is
  **not** `send_cursor`: after a leader change the new leader may legitimately
  receive an ack for records journaled under the previous leader.
- `BindSeat` clamps `last_journal_seq` to `next_seq - 1`; a value above that is
  not silently accepted (see the resync condition in §3.4).
- Gateways run inside the venue's trusted VPC. `acked_seq` is the gateway's
  declaration that it has durably journaled the range; v1 does not require a
  journal digest. A future hardening step would add one.

#### 3.2 Leader change

`on_role_change(Role::Leader)` (and `on_start` when the node starts as leader)
sets every seat's transient `send_cursor = acked_seq`. Unacknowledged entries
are re-offered to the bound session; gateways deduplicate by `seat_seq`
against their persisted `last_applied_seq` and never mint a duplicate FIX
`MsgSeqNum` or a duplicate OUCH frame. `send_cursor` is never snapshotted.

#### 3.3 Takeover and duplicate connections

- **Same gateway, same seat:** a second login is refused locally with
  `LoginRejected(SeatInUse)`; the cluster is not involved.
- **Cross-gateway:** `BindSeat` increments `seat.epoch`, sends
  `FenceEvicted(SupersededByNewBind)` to the old bound session (if this node
  holds it), rewinds `send_cursor` to `effective_last`, and answers the new
  gateway with `SeatBound`. The old gateway sends `Logout(Fenced)` to its peer
  and discards the seat. Stale-epoch commands and acks are dropped by the
  service.

#### 3.4 Resync: condition, clearing, and synthetic state

The service computes:

```text
effective_last   = min(last_journal_seq, next_seq - 1)
oldest_available = entries.front().map_or(next_seq, |e| e.seat_seq)
needs_resync     = overflowed
                || last_journal_seq > next_seq.saturating_sub(1)
                || effective_last < oldest_available.saturating_sub(1)
```

- The fresh-seat case is safe by construction: `next_seq = 1`, `entries`
  empty, `last_journal_seq = 0` → `effective_last = 0`,
  `oldest_available - 1 = 0`, no resync.
- A gateway that claims to be ahead of the cluster (`last > next_seq - 1`) is
  invalid and **must resync**; the upper clamp must not hide it.
- On the normal path, `send_cursor = effective_last`,
  `replay_from_seq = effective_last + 1`, `resynced = 0`, and the outbox
  re-offers `[replay_from_seq .. next_seq)`.

On resync the service performs, in this order:

1. `generation += 1`;
2. `entries.clear(); buffered_bytes = 0`;
3. `acked_seq = send_cursor = next_seq - 1`;
4. `replay_from_seq = next_seq`;
5. enumerate the account's resting orders deterministically (§3.5) and append
   one synthetic `ExecutionReport` per order, consuming contiguous
   `seat_seq`s from `next_seq`;
6. answer `SeatBound { generation, replay_from_seq, next_seq, resynced = 1 }`.

Clearing the entries is unconditional, not just on the overflow path: a lost
gateway journal can trigger resync while unacknowledged entries still exist,
and those entries must not be re-offered under the new generation.

Synthetic reports are `engine_core::event::ExecType::New` with the order's
current `OrderState` (`New` or `Partial`), full `price`, `order_qty`,
`leaves_qty`, `cum_qty`, `avg_px`, and `resync = 1` in the outbox entry.
`engine-core`'s `ExecType` is unchanged.

**Synthetic `ExecID`.** Synthetic reports use the `LogHeader.position` of the
`BindSeat` command that triggered the resync as `header.log_pos`, and
`header.event_seq = 0x8000_0000 | ordinal`. `EventSeq` is `u32`; real per-command
event sequences are far below `2^31`, and one log position never produces both
a real command batch and a resync batch, so FIX `ExecID = (log_pos, event_seq)`
stays unique. No extra counter is persisted.

#### 3.5 Resting-order enumeration

`engine-core` gains one **read-only** method:

```rust
pub fn resting_orders_for_account(
    &self,
    account: AccountId,
    out: &mut Vec<RestingOrderSummary>,
)
```

It walks the non-empty price levels — bids descending, asks ascending, FIFO
within a level (`PriceLevel::head`/`tail`) — and filters by account. The order
is deterministic and does not scan the `1 << 20` slab. Each summary carries
`cl_ord_id`, `order_id`, `side`, `price`, `order_qty`, `filled`, `notional`,
and `state`. `avg_px` is derived from `notional / filled`.

In multi-seat mode the startup configuration enforces
`max_open_orders_per_account <= 4096`. A full resync batch is at most
`4096 × (106 B payload + 18 B entry overhead) ≈ 496 KiB`, below the 1 MiB
per-seat cap once the resync clears the entries (§3.4).

#### 3.6 Memory budgets and deterministic eviction

- `MAX_SEAT_OUTBOX_BYTES = 1 MiB` per seat;
- `MAX_TOTAL_OUTBOX_BYTES = 64 MiB` across all seats;
- `MAX_GATEWAY_OUTBOUND_BYTES = 256 MiB` per gateway process;
- `MAX_SEAT_OUTBOUND_BYTES = 16 MiB` per seat socket.

When an append exceeds a seat cap, that seat overflows. When it exceeds the
global cap, the service evicts seats in the deterministic order
**`(buffered_bytes DESC, seat_id ASC)`** until the total fits. The order uses
only log-derived state: `bound_session` is transient, is not snapshotted, and
may differ between leader and follower, so it must never participate in
eviction. If the evicted seat is bound on this node, the service additionally
queues `FenceEvicted(OutboxOverflow)`; the state change (`overflowed = true`,
entries dropped) is decided solely by the log.

Capacity assumption for the 64 MiB budget: at most 64 seats at full backlog;
with the 500-seat target that is 128 KiB per seat on average. Beyond that,
deterministic eviction followed by resync is the designed degradation.

#### 3.7 Slow consumers

If a seat's outbound socket buffer exceeds 16 MiB (or the gateway's total
exceeds 256 MiB), the gateway:

1. sends `Logout(SlowConsumer)` and closes the TCP connection;
2. sends `ServiceIngress::UnbindSeat { seat_id, epoch }` so the cluster stops
   offering that seat's events and the gateway stops appending to a journal no
   consumer reads;
3. keeps the journal; the unacknowledged range stays in the cluster outbox
   (bounded by §3.6) and is replayed on the next `BindSeat`.

The gateway never pauses polling its cluster session; that would create
head-of-line blocking for every other seat on the same session.

### 4. Snapshot `DBS3`

`engine-core`'s snapshot (`DBS2`, SBE schema 200, engine state v3) is unchanged
and nested verbatim.

#### 4.1 Envelope

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `magic` | `[u8; 4]` | `*b"DBS3"` |
| 4 | `version` | `u8` | `1` |
| 5 | `flags` | `u8` | bit0 `has_md_seq` |
| 6 | `md_seq` | `u64` | `0` when absent |
| 14 | `engine_blob_len` | `u64` | bytes of nested `DBS2` |
| 22 | `engine_blob` | `[u8; len]` | exact `DBS2` bytes |
| 22+len | `seat_count` | `u32` | seats sorted by `seat_id` |
| … | `seats[]` | | see below |

Each seat record:

```text
seat_id: u16, account_id: u32, epoch: u64, generation: u64,
acked_seq: u64, next_seq: u64, overflowed: u8, entry_count: u32,
entries[entry_count]: {
    seat_seq: u64, liquidity: u8, payload_kind: u8, flags: u8,
    [replace_req_cl_ord_id: [u8; 20] when flags & 2],
    payload_len: u32, payload: [u8; payload_len]
}
```

`bound_session`, `send_cursor`, and `buffered_bytes` are transient and are not
persisted.

#### 4.2 Digests

- `snapshot_hash` covers every `DBS3` byte and is used to check that the three
  members applied the same log.
- `state_hash` keeps its current meaning: the decoded engine snapshot with
  `last_log_pos` zeroed, plus the pinned `(seat_id, account_id)` pairs. It
  intentionally excludes acks, outbox contents, and epochs, because gateway
  timing makes those differ between otherwise identical runs; including them
  would break the cross-failover comparison the diagnostic exists for.

#### 4.3 Compatibility and rollout

`decode` accepts bare `DBS1`, bare `DBS2`, `MDS1`-prefixed `DBS1`/`DBS2`
(the MDS1 prefix is the optional 13-byte market-data header), and `DBS3`.
Rollout follows ADR 0005/0011: every node reads `DBS3` before the writer
switches to it.

#### 4.4 Budget

Worst case is 64 MiB of outbox plus ~16 MiB of engine snapshot, about 80 MiB
per snapshot; with two followers that is roughly 160 MiB of snapshot transfer
per snapshot event. In steady state most entries are acknowledged and trimmed,
so snapshots are normally far smaller.

### 5. DeepOUCH/1.0 wire protocol

#### 5.1 Frame header (4 bytes)

| Offset | Field | Type | Notes |
|---|---|---|---|
| 0 | `payload_len` | `u16` | payload after this header; `<= 80`, else `Logout(ProtocolViolation)` |
| 2 | `msg_type` | `u8` | see §5.2; unknown → `Logout(ProtocolViolation)` |
| 3 | `version` | `u8` | must be `1`, else `Logout(ProtocolViolation)` |

TCP with `TCP_NODELAY`; safe slice decoding via `from_le_bytes` /
`to_le_bytes`; no unsafe casts, arbitrary alignment is fine.

#### 5.2 Message catalog (14 types)

| Dir | Type | Name | Payload (offsets from 0, LE) | Payload | Total |
|---|---|---|---|---|---|
| C→S | `L` | `LoginRequest` | `0..2 seat_id:u16`, `2..4 reserved:u16`, `4..36 token:[u8;32]`, `36..44 requested_seq:u64` | 44 | 48 |
| S→C | `A` | `LoginAccepted` | `0..2 seat_id:u16`, `2..4 reserved:u16`, `4..12 generation:u64`, `12..20 next_seq:u64` | 20 | 24 |
| S→C | `J` | `LoginRejected` | `0 reason:u8`, `1..8 reserved:[u8;7]` | 8 | 12 |
| C→S | `R` | `ClientHeartbeat` | empty | 0 | 4 |
| S→C | `H` | `ServerHeartbeat` | empty | 0 | 4 |
| Both | `G` | `Logout` | `0 reason:u8`, `1..8 reserved:[u8;7]` | 8 | 12 |
| C→S | `N` | `EnterOrder` | `0..8 cl_ord_id:u64`, `8..16 price:i64`, `16..24 qty:u64`, `24..26 symbol:u16`, `26 side:u8`, `27 ord_type:u8`, `28 tif:u8`, `29 flags:u8`, `30..32 reserved:[u8;2]` | 32 | 36 |
| C→S | `X` | `CancelOrder` | `0..8 cl_ord_id:u64`, `8..16 orig_cl_ord_id:u64`, `16..18 symbol:u16`, `18..24 reserved:[u8;6]` | 24 | 28 |
| C→S | `M` | `ReplaceOrder` | `0..8 cl_ord_id:u64`, `8..16 orig_cl_ord_id:u64`, `16..24 new_price:i64`, `24..32 new_qty:u64`, `32..34 symbol:u16`, `34 ord_type:u8`, `35 tif:u8`, `36 flags:u8`, `37..40 reserved:[u8;3]` | 40 | 44 |
| S→C | `U` | `OrderReplaced` | `0..8 seq:u64`, `8..16 req_cl_ord_id:u64`, `16..24 orig_cl_ord_id:u64`, `24..28 order_id:u32`, `28..36 price:i64`, `36..44 qty:u64`, `44..52 leaves_qty:u64`, `52 state:u8`, `53..56 reserved:[u8;3]` | 56 | 60 |
| S→C | `E` | `OrderAccepted` | `0..8 seq:u64`, `8..16 cl_ord_id:u64`, `16..20 order_id:u32`, `20..22 symbol:u16`, `22 side:u8`, `23 state:u8`, `24..32 price:i64`, `32..40 qty:u64`, `40..48 leaves_qty:u64`, `48..56 log_pos:i64`, `56..64 cum_qty:u64`, `64..72 avg_px:i64` | 72 | 76 |
| S→C | `F` | `OrderExecuted` | `0..8 seq:u64`, `8..16 cl_ord_id:u64`, `16..20 order_id:u32`, `20..28 fill_price:i64`, `28..36 fill_qty:u64`, `36..44 leaves_qty:u64`, `44 state:u8`, `45 liquidity:u8`, `46..48 reserved:[u8;2]` | 48 | 52 |
| S→C | `C` | `OrderCanceled` | `0..8 seq:u64`, `8..16 cl_ord_id:u64`, `16..24 orig_cl_ord_id:u64`, `24..28 order_id:u32`, `28 reason:u8`, `29..32 reserved:[u8;3]` | 32 | 36 |
| S→C | `D` | `OrderRejected` | `0..8 seq:u64`, `8..16 cl_ord_id:u64`, `16..24 orig_cl_ord_id:u64`, `24 reason:u8`, `25 is_cancel_reject:u8`, `26..32 reserved:[u8;6]` | 32 | 36 |

Reserved bytes are sent as `0` and ignored on receipt, except that a non-zero
reserved byte in an inbound frame is a protocol violation
(`Logout(ProtocolViolation)`), matching the strict v1 posture.

`OrderAccepted` carries `cum_qty`/`avg_px` so a synthetic resync report for a
partially filled resting order is complete. `OrderExecuted` does not; clients
accumulate. `OrderCanceled` no longer carries a `Replaced` reason: replacement
is reported by `OrderReplaced`.

#### 5.3 Field and enum rules

- **`cl_ord_id`.** `0` is invalid on every request and on `orig_cl_ord_id`
  (→ `RejectMalformed` 101). A non-zero `u64` maps to a 20-byte zero-padded
  ASCII decimal `ClientOrderId` with stack-only base-10 division
  (`42 ↔ b"00000000000000000042"`; `u64::MAX` is exactly 20 digits). No NUL
  byte is produced, satisfying `ClientOrderId`'s invariant. Because seats are
  static-protocol and OUCH-originated ids are decimal, no outbound fallback to
  `0` exists.
- **Prices.** Limit orders require `price > 0`; market orders require
  `price == 0` and ignore the value. Violations → `RejectMalformed` 103.
- **Quantities.** `1 <= qty <= i64::MAX`; otherwise `RejectMalformed` 102.
- **Flags.** Bit0 = `post_only`; bits 1–7 are reserved and must be 0.
  Per-order STP is intentionally absent: `RiskConfig.self_trade` is engine-global.
- **Sides.** `1 = Buy`, `2 = Sell`.
- **Ord types.** `1 = Limit`, `2 = Market`.
- **TIF.** `0 = Day`, `1 = GTC`, `2 = IOC`, `3 = FOK`.
- **States.** `0 = New`, `1 = Partial`, `2 = Filled`, `3 = Canceled`,
  `4 = Rejected`.
- **Liquidity.** `0 = None`, `1 = Maker`, `2 = Taker`.
- **`LoginRejected.reason`.** `1 = InvalidToken`, `2 = UnknownSeat`,
  `3 = SeatInUse`, `4 = SeqOutOfRange`, `5 = ClusterDown`,
  `6 = AccountConflict`.
- **`Logout.reason`.** `0 = ClientRequested`, `1 = HeartbeatTimeout`,
  `2 = ProtocolViolation`, `3 = ResyncRequired`, `4 = Fenced`,
  `5 = SlowConsumer`, `6 = ServerShutdown`.
- **`OrderCanceled.reason`.** `0 = UserRequested`, `1 = ImmediateOrCancel`,
  `2 = SelfTradePrevention`, `3 = DayExpired`.
- **`OrderRejected.reason`.** Engine `RejectReason` values `1..=17` exactly as
  `engine_core::wire` encodes them, plus service reasons:
  `101 = ZeroOrInvalidClOrdId`, `102 = QtyOverflowOrZero`,
  `103 = InvalidWirePrice`, `104 = InvalidWireEnum`.

#### 5.4 Session continuity

`requested_seq` on login:

- `0`: replay from the oldest entry of the current generation (the first
  synthetic resync report when one exists); if the generation is empty, start
  live at `next_seq`.
- `u64::MAX`: no replay; start live at `next_seq`.
- `1 <= r < journal.oldest_seq()` or `r > journal.next_seq()`:
  `LoginRejected(SeqOutOfRange)`.
- `r == journal.next_seq()`: no replay; enter live immediately.

`LoginAccepted.next_seq` is **`first_outbound_seq`**: the sequence of the first
business message the server will send after `LoginAccepted` (`requested_seq`
when replaying, `journal.next_seq()` when not).

The gateway keeps a strict barrier: while it catches up from the cluster and
replays the journal, newly arriving `SeatEvent`s are durably appended to the
journal but not written to the socket; when the replay cursor reaches the
catch-up point the same socket switches to live writes.

#### 5.5 Heartbeats and security

- One second without outbound business traffic → send the direction's
  heartbeat (`R` from client, `H` from server).
- Three seconds without any inbound byte → `Logout(HeartbeatTimeout)` and close.
- The 32-byte pre-shared token is compared in constant time (fold-XOR or
  `subtle`), never with an early-exit comparison.
- DeepOUCH/1.0 is plaintext. It is permitted only inside a colocation private
  network or a dedicated VPC/private link; public exposure requires a TLS
  termination proxy in front.

#### 5.6 Error policy

| Condition | Action |
|---|---|
| truncated frame, `payload_len` mismatch, `payload_len > 80`, unknown `msg_type`, `version != 1`, non-zero reserved | `Logout(ProtocolViolation)`; no log entry |
| unparsable `cl_ord_id`, `qty`/price/enum out of range | `RejectMalformed` → service reject with a `seat_seq` |
| token failure, unknown seat | `LoginRejected`; close |
| malformed budget exhausted | `Logout(ProtocolViolation)`; close |

Each authenticated seat has a gateway-local malformed budget **keyed by
`seat_id` and shared across reconnects** (capacity 8, refill 2/s). Within the
budget, malformed frames go through the log; beyond it the connection is closed
and further bytes are discarded, so a client cannot amplify malformed traffic
into consensus.

### 6. Gateway host

#### 6.1 `SeatRegistry`

Startup loads `seats.toml` (shared by `engine-container` and `fix-gateway`):

```text
seat_id, account_id, protocol = "fix" | "ouch",
fix: sender_comp_id, target_comp_id
ouch: token (32 bytes)
journal_dir (default <root>/seat_<id>/)
rate limits
```

The loader prints the file's SHA-256 and **fails startup** if the cluster and
gateway copies differ. `BindSeat` is refused (`BindRejected`) if the seat is
unknown or the account disagrees with the local configuration.

Connections before authentication live in a bounded `AwaitingLogin` slab
(max 256, `logon_timeout_ms = 3000`) with no seat journal opened.

#### 6.2 Seat handshake

```text
Disconnected
  -> BindingCluster      (BindSeat sent; awaiting SeatBound)
  -> CatchingUpJournal   (append [replay_from_seq .. next_seq) to the journal)
  -> ReplayingToClient   (LoginAccepted; stream [requested_seq .. catchup))
  -> StreamingLive
  -> Closing
```

Cluster unreachable within the bind timeout → `LoginRejected(ClusterDown)`.
A resynced `SeatBound` first rotates the journal file to
`seat-<id>.gen-<generation>.journal`; old generations are never truncated in
place and remain for audit.

#### 6.3 FIX seats

FIX `MsgSeqNum` and `seat_seq` are separate. The seat persists
`(fix_in_seq, fix_out_seq, last_applied_seat_seq)` atomically in its sequence
store. Recovery order is:

1. append the translated FIX message to the FIX journal;
2. update `last_applied_seat_seq` and `fix_out_seq` in memory;
3. flush/sync per `SyncPolicy`;
4. emit `AckSeat(last_applied_seat_seq)`.

On crash recovery a re-delivered `SeatEvent` with
`seat_seq <= persisted.last_applied_seat_seq` is skipped without minting a new
FIX `MsgSeqNum`. The FIX mapping uses outbox metadata:

- `ExecType::Replaced` → `ExecutionReport` with `ClOrdID` = the replace
  request (`replace_req_cl_ord_id`) and `OrigClOrdID` = the original order;
- `resync` → `ExecutionReport` with `ExecType = 'D' (Restated)` and
  `58=RESYNC`;
- everything else maps as today.

This requires two small `crates/fix` additions: `ExecType::Restated = 'D'` and
an optional `orig_cl_ord_id` on `ExecutionReport`. Neither touches
`engine-core`.

#### 6.4 OUCH seats

The gateway maps outbox entries to wire frames and appends the frames to the
seat journal; `seat_seq` is the wire `seq` 1:1. Client replay writes raw
journal slices without re-framing or recomputing anything. Duplicate
`SeatEvent`s are dropped before mapping.

#### 6.5 Dual listeners

`deepbook-io` registers two `TcpListener`s (`--fix-bind`, `--ouch-bind`) tagged
by listener kind on the same `epoll`/`io_uring` loop and the same
`ClusterClient` session.

### 7. Verification and the ABBA comparison

#### 7.1 Fault-injection and conformance tests

1. Cross-gateway Maker routing: seat 1 buys on gateway A, seat 2 sells on
   gateway B; seat 2 receives `OrderExecuted(liquidity = Taker)`, seat 1
   receives `liquidity = Maker`, no cross-seat leakage.
2. Offline Maker catch-up: stop gateway A; seat 2 fills 50 of seat 1's orders;
   restart A with `last_journal_seq`; the client reconnects at its requested
   seq and sees 50 strictly consecutive, duplicate-free reports.
3. Leader failover during replay: `send_cursor` rewinds to `acked_seq`;
   gateways deduplicate; no gap and no duplicate FIX `MsgSeqNum`.
4. `BindSeat` race: two gateways bind the same seat in the same log window;
   the later log entry wins the epoch, the earlier gets `FenceEvicted`, and its
   in-flight commands are dropped by the epoch check.
5. Overflow on a bound seat: `FenceEvicted(OutboxOverflow)` immediately,
   `Logout(ResyncRequired)`, generation rotation, exact synthetic
   `OrderAccepted` stream on reconnect.
6. Overflow while unbound + journal loss: `last_journal_seq < oldest - 1`
   triggers the same resync path; entries are cleared first.
7. A gateway that claims `last_journal_seq > next_seq - 1` is forced to resync.
8. Slow consumer: a non-reading peer trips the 16 MiB seat disconnect,
   `UnbindSeat` is sent, another seat keeps sub-millisecond RTT.
9. Malformed flood: the per-seat budget closes the connection before the log
   is amplified; reconnects do not reset the budget.
10. Snapshot restore: `DBS3` round-trips all payload kinds/flags; restart three
    members and compare `snapshot_hash`.

#### 7.2 ABBA

- Topology: one cluster, one multi-seat gateway (`--backend uring`,
  `/dev/shm` journals, `SyncPolicy::Every(64)`).
- Arms: `FIX → OUCH → OUCH → FIX`, 5,000 warmup + 100,000 measured orders per
  arm, distinct seats/accounts per arm.
- `fix-bench` and a new `ouch-bench` share the absolute-time scheduler and
  missed-send accounting.
- Pre-registered criteria:
  1. gateway user-space CPU cycles per order (`perf stat`) falls ≥ 30% versus
     the FIX arm in the same run;
  2. at 10k orders/s, OUCH `E[X | X < p99]` beats FIX by more than `2×` the
     inter-arm FIX-vs-FIX noise floor, with no `p99` regression beyond the
     noise floor; `p99.9` is recorded, not gated;
  3. the saturation knee (first rate with backlog > 0 or `p99 > 2 ms`) on the
     stepped ramp is higher for OUCH than for FIX on the same binary; the
     historical single-seat `45.9k/s` is context only.
- 16-seat mixed load: 2 Maker seats quoting two-sided ladders + 14 Taker seats
  at an aggregate 32k orders/s (2k/seat), below consensus saturation so
  protocol and routing overhead are isolated.

## Consequences

- `engine-core` keeps its protocol- and session-agnostic state machine and its
  `DBS2`/v3 snapshot. It gains one read-only enumeration method and a
  `Command::account()` helper; the `ExecType` enum is unchanged.
- The outbox stores protocol-neutral engine events (~106 B) rather than compact
  OUCH structs, roughly doubling outbox memory versus an OUCH-only design. It
  is bounded per seat and globally, and it buys lossless FIX and OUCH output
  from one buffer.
- Strict 1:1 seat/account ownership removes multi-authority sequence merging at
  the cost of one TCP session per sub-account and permanent seat pins in v1.
- The 64 MiB outbox plus ~16 MiB engine snapshot is the dominant worst-case
  snapshot cost (~80 MiB per snapshot; ~160 MiB of transfer to two followers).
  Steady-state snapshots are much smaller because acknowledged entries are
  trimmed.
- Gateways are trusted infrastructure inside the venue VPC: token auth protects
  the client edge, but a compromised gateway can bind seats and acknowledge
  ranges. Hardening that boundary is out of scope for v1.
- DeepOUCH/1.0 is frozen at `version = 1` as specified here; there is no
  compatibility promise for draft implementations.

## Alternatives considered

- **One cluster session per client seat.** Rejected: N sessions amplify
  heartbeats and reconnect storms on leader change; the 500-client target
  argues for one multiplexed session per gateway.
- **Per-session egress queues instead of a per-account outbox.** Rejected: a
  gateway restart changes the session id, and an offline Maker fill has no
  session to queue against; the durable unit is the account, not the session.
- **Extending `engine-core`'s snapshot schema with routing state.** Rejected:
  routing is a service-container concern and would force an engine snapshot
  version bump for no matching benefit.
- **Compact OUCH-only outbox entries.** Rejected: they cannot reconstruct FIX
  `cum_qty`/`avg_px`/`ExecID`/`orig_cl_ord_id`, so dual-protocol seats would
  need two buffers and two loss modes.
- **Per-order STP modes on the wire.** Rejected: `RiskConfig.self_trade` is
  engine-global; adding per-order modes is an engine feature, not a protocol
  field.

## References

- ADR 0002 — FIX session semantics
- ADR 0005 — engine snapshot (`DBS2`, schema 200)
- ADR 0006 — gateway cluster client
- ADR 0007 — journal sync policy
- ADR 0011 — price band book and engine snapshot v3
- `analysis/deepbook/review-2026-11-27-ouch-adr.md` — third-round review
- `analysis/deepbook/OUCH-SBE对比详细设计.md` / `_v2.md` — design history
