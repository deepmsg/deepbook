# ADR 0008: market-data wiring, the admin and metrics

- Status: **Accepted** — 2026-10-25, implemented by M5b
- Date: 2026-10-25
- Deciders: project owner

## Context

M3 built the incremental, snapshot and recovery channels (ADR 0004) but M4's
`EngineService` only answered the FIX session; nothing published the engine's
`BookUpdate` and `Trade` events. The M5 dashboard needs a live feed, and the
Grafana panels need a scrapeable source. The engine container is a
`ClusteredService`: it has no scheduler exposed through the pinned deepmsg
API, and its hot path must not block on a slow or absent subscriber.

## Decision

1. **One market-data stream, best effort.** `EngineService` owns a
   `MarketDataService` and a non-blocking `DeepmsgIncrementalPublisher`. Every
   `BookUpdate` and `Trade` gets the next contiguous market-data sequence and
   is offered once; `NotConnected`/back pressure drops the incremental rather
   than stalling the cluster service. A subscriber repairs a gap through the
   recovery channel, which answers with a snapshot at the latest sequence.
2. **The market-data sequence is part of the restored state.** The container
   snapshot is the `DBS2`/`DBS1` engine record prefixed with a 13-byte `MDS1`
   record (magic, version, `u64` sequence). `decode_service_snapshot` accepts
   both forms, so a snapshot written before market data was enabled still
   restores (sequence 0). `Session`/`Engine` state and the market-data stream
   therefore resume together. On restore the mirror is seeded from the
   restored engine view, so a recovery snapshot has the right levels even
   before the next command arrives.
3. **Recovery gets its own thread and client.** A recovery request must be
   answered while the cluster is idle (no command callback to piggy-back on),
   so `engine-container` starts a `deepbook-md-recovery` thread. It owns the
   only subscription to the recovery request stream and the response
   publication, and answers with the latest snapshot from an
   `Arc<Mutex<Snapshot>>` cell the service updates after every market-data
   batch. The service's publisher deliberately has no recovery endpoints, so
   requests are not split between two subscribers.
4. **The admin is a subscriber, not a query engine.** It connects to the same
   driver and channels, mirrors the levels, collects the trade tape from the
   transport callback, and serves:
   - `/` and the build-free UI in `crates/ui`;
   - `/ws`, the whole conflated view as JSON at 10 Hz (conflation is the
     WebSocket layer, as ADR 0004 requires);
   - `/api/book`, `/api/trades`, `/api/latency`;
   - `/metrics`, Prometheus text with `deepbook_md_seq`,
     `deepbook_book_levels/qty`, `deepbook_trades_total`, the cluster's
     commit position / snapshot count / role from the CnC file, and the last
     benchmark's RTT gauges.
   HTTP/1.1 and the RFC 6455 handshake are implemented in-tree (SHA-1 and
   base64 are the only primitives needed), so the admin has no web framework
   and no CDN dependency for the UI.
5. **The benchmark writes a Prometheus text file.** `fix-bench --prometheus
   PATH` writes the run's counts and RTT percentiles; `deploy/bench.sh
   --admin --serve` starts the dashboard and passes that path. Prometheus and
   Grafana are provisioned from `deploy/grafana/` and scrape
   `host.docker.internal:26060`; `--admin-host 0.0.0.0` exposes the admin to
   the compose network.
6. **The bench seeds a book for the dashboard.** `--seed-book` sends four
   resting levels and two crossing trades before the timed run, so the UI has
   depth and a tape; the measured IOC orders never rest. This is off by
   default in the library and on in the one-click script.

## Consequences

- Market data is not durable: a dropped incremental costs a recovery
  snapshot. That is the intended M5 trade-off; reliability belongs to the
  cluster's log, not to the UI feed.
- A container restart restores the market-data sequence with the engine
  snapshot and seeds the mirror from the restored book, so the first recovery
  snapshot after a restart has the right levels. Commands replayed after the
  snapshot are deterministic, so their incrementals carry the same `md_seq`
  the subscriber already saw; the subscriber and the admin's trade tape
  (which keeps the highest `md_seq` it has) ignore them as stale. Events at or
  below the snapshot position are not republished at all. The `log_pos`
  dedupe is therefore no longer needed for the common restart path.
- The dashboard is read-only. The kill switch and order queries still need an
  admin→cluster command session (a later slice), and the crate description's
  "control plane" is not claimed yet.
- The admin uses one thread for the feed and one per connection; that is
  adequate for a local demo and is not a hardened multi-tenant control plane.
