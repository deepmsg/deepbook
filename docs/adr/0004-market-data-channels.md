# ADR 0004: Market data — two channels, `md_seq` and recovery

- Status: **Accepted** — 2026-10-25, implemented by M3
- Date: 2026-10-25
- Deciders: project owner

## Context

The design (§6.3, §7.3) splits market data into an incremental channel and a
recovery request/response channel, with one monotonic `md_seq` per symbol. It
also requires a gap test made by the driver's loss generator, not by
hand-written message drops. The engine (M2) already emits `BookUpdate` and
`Trade` events, but only `BookUpdate` carries an engine-side `md_seq`.

## Decision

1. **Two channels.** Incrementals go to one IPC/UDP channel; recovery is a
   request/response pair. A subscriber that sees `md_seq != last + 1` asks the
   recovery channel with `(symbol, last_md_seq)`.
2. **One contiguous sequence per symbol.** The producer assigns `md_seq` to
   **every** incremental, trades and book updates alike. The service refuses a
   stale or skipped sequence; a consumer ignores anything at or below its last
   sequence and reports a hole for anything above `last + 1`. `md_seq` never
   goes backwards, including after a snapshot.
3. **Service state.** The publishing side mirrors the levels, keeps a bounded
   ring of recent incrementals, and snapshots the current book at its last
   sequence. Recovery returns:
   - the missing incrementals when the ring covers them;
   - a snapshot when the request is older than the ring;
   - a snapshot for `last_md_seq = 0` (a subscriber without a base);
   - `UpToDate` when the subscriber is current.
4. **Snapshots are caller-timed.** The library has no clock. The deployment
   broadcasts a snapshot once a second; the loss test uses a short timer.
5. **Slowness and loss.** Incrementals have no per-subscriber ring; conflation
   stays in the admin → UI WebSocket layer. The test subscribes with
   `reliable=false` so the driver's send-side loss reaches the application and
   the recovery path is exercised. The production deployment can choose a
   reliable subscription and keep recovery for process restarts and slow
   consumers.
6. **Wire frame.** `marketdata::wire` is a versioned fixed little-endian frame
   (incremental, snapshot, recovery request/response). It is the M3 internal
   format; SBE replaces it when the schema toolchain is wired in. Trailing
   bytes are ignored, which lets the loss test pad a frame to one datagram.
7. **Engine integration is M4.** The engine's `BookUpdate` carries its own
   `md_seq`, while `Trade` does not. The M4 adapter that turns engine events
   into market data must assign the single market-data sequence across trades
   and book updates (or the engine's trade event gains the sequence). M3's
   message types accept the sequence from the producer and do not assume the
   engine's value.

## Consequences

- M4 wires `engine-cluster` events into `MarketDataService`; the adapter owns
  the trade/book-update sequence assignment noted above.
- Recovery ring size is a memory-versus-snapshot trade-off; the default is
  10,000 incrementals.
- The loss test starts the production media driver with
  `deepmsg.debug.send.data.loss.drop.every=4` and one 1024-byte frame per
  datagram. It skips when the deepmsg checkout or driver binary is absent, so
  the default CI check does not need the external build.
- SBE will replace the internal wire frame without changing the
  `Incremental` / `Snapshot` / `Recovery` types.
