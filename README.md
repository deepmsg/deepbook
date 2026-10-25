# deepbook

[![ci](https://github.com/deepmsg/deepbook/actions/workflows/ci.yml/badge.svg)](https://github.com/deepmsg/deepbook/actions/workflows/ci.yml)

An order book, matching engine, and FIX 4.4 acceptor, built as a **deterministic
state machine** on top of [deepmsg](https://github.com/deepmsg/deepmsg).

The goal is a runnable vertical slice that can be demonstrated and measured:
FIX client → gateway → replicated log → risk → matching → execution reports →
market data → snapshot/recovery, with a client-side RTT number and a replay
that produces the same state twice.

> Status: **M0–M3 merged; M4 in progress on this branch.** The FIX session,
> epoll acceptor, engine seam, matching engine and market-data channels are in
> place. The engine now has snapshot/restore and a versioned snapshot record;
> the `ClusteredService` adapter, gateway cluster client and the single-node
> process e2e are still to come.

## What deepmsg provides, and what deepbook brings

deepmsg is the infrastructure: a media driver, client, archive, and a Rust port
of Aeron Cluster (consensus module, service container, cluster client, backup,
operator tool). deepbook is the application. The dependency direction is
one-way:

```
deepbook  ──▶  deepmsg
```

- deepmsg owns ordering, the replicated log, snapshots, recovery, transport,
  and cluster membership.
- deepbook owns the order book, matching, risk, the FIX session, market data,
  and the UI.

The matching engine is a pure state machine (`engine-core`) that depends only
on `types`; the cluster adapter (`engine-cluster`) is the only crate that knows
about `deepmsg-cluster-service`.

## Architecture

```
FIX client ──TCP──▶ fix-gateway ──▶ deepmsg cluster (log / consensus)
                         ▲                    │
                         │ egress             ▼
                         └──────────── engine (ClusteredService)
                                              │
                                              ▼
                                        market-data ──▶ UI / recovery
```

A single-node cluster is the default deployment. Three nodes are used only for
the failover demo; a single node is a first-class configuration (quorum of one),
not a degraded mode.

## Design

Decisions that affect the code are recorded as ADRs in [`docs/adr/`](docs/adr/).
Longer design documents are maintained outside this repository. The deepmsg pin
and the local patch recipe are in [`docs/deps.md`](docs/deps.md).

## Layout

```
crates/
  types/           Price/Qty/OrderId/...; SBE constants
  fix/             FIX 4.4 session + codec + outbound journal
  gateway/         FIX acceptor, session registry, cluster client
  engine-core/     pure state machine: order book, matching, risk
  engine-cluster/  ClusteredService adapter; snapshot serialization
  marketdata/      incremental/snapshot/recovery
  admin/           HTTP/WS, kill switch, queries
  bench/           open-loop client, latency harness
  spike/           cluster checks: process control, latency, recovery
  ui/              static dashboard (not a workspace member)
schemas/           SBE XML
deploy/            compose, bind-core scripts, spike scripts
docs/              ADRs and dependency notes
tests/             integration / conformance / differential / replay
```

## Build

```bash
cargo build --workspace
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
# or
just check
```

Toolchain: Rust 1.95.0 (`rust-toolchain.toml`), edition 2024, MSRV 1.85.

## Run the FIX gateway

```bash
cargo run -p deepbook-gateway --bin fix-gateway -- \
  --bind 127.0.0.1:9876 --session-dir target/fix-sessions
```

It accepts the configured `SenderCompID`/`TargetCompID` pair (default
`DEEPBOOK`/`CLIENT`) and journals each session under `--session-dir`. The M1
engine acknowledges NewOrderSingle, cancel, replace and status with
ExecutionReports; it does not match yet (M2). `--help` lists the options.

## FIX conformance

```bash
./tests/fix_conformance/run.sh
```

Starts the real gateway and drives it with QuickFIX/J and a byte-level probe:
logon, order/report, an in-window resend with `43`/`122` and an admin gap fill,
and an over-window `Logout`. The script fetches the QuickFIX/J jars into
`target/quickfixj/` on first use; see
[`tests/fix_conformance/README.md`](tests/fix_conformance/README.md).

## Matching engine (M2)

```bash
cargo test -p deepbook-engine-core
```

`engine-core` is a pure state machine: the only ordering input is the log
position, and every output is an event. It implements price-time matching with
a slab and FIFO price levels, limit and market orders, DAY/GTC/IOC/FOK,
cancel/replace (a same-price size reduction keeps queue priority), post-only,
self-trade prevention (off, cancel resting, or reject taker), the common
pre-trade limits and a bounded duplicate-`ClOrdID` window.

The acceptance tests are in the crate:

- `tests/differential.rs` runs proptest command streams through the slab engine
  and a naive `Vec` oracle, comparing trades and the final book view.
- `tests/allocation.rs` installs a counting allocator and asserts that
  matching at an existing level and canceling after warmup allocate **zero**.
- Unit tests cover partial fills, price-time priority, market sweeps, TIF,
  post-only, self-trade, replace priority, risk and replay determinism.

Snapshot serialization and the cluster adapter are M4; market data is M3.

## Market data (M3)

```bash
cargo test -p deepbook-marketdata

# the driver loss-generator test (needs the deepmsg checkout and its binaries)
DEEPBOOK_DEEPMSG_DIR=../deepmsg cargo test -p deepbook-marketdata \
  --test deepmsg_loss -- --nocapture
```

`marketdata` has a producer-side `MarketDataService` and a consumer-side
`Subscriber`:

- one contiguous `md_seq` per symbol, assigned to every incremental (trade and
  book update);
- a bounded ring of recent incrementals and a level snapshot at the last
  sequence;
- a subscriber that ignores stale/duplicate messages, reports a hole, and
  recovers through the request/response channel with either the missing
  incrementals or a snapshot;
- a versioned fixed wire frame (`marketdata::wire`), with a deepmsg transport
  adapter (`marketdata::deepmsg`) and a `reliable=false` subscription for the
  loss path.

`tests/recovery.rs` drives a simulated lossy link (including a blackout longer
than the ring), and `tests/deepmsg_loss.rs` starts the production media driver
with `deepmsg.debug.send.data.loss.drop.every=4`, publishes 1024-byte frames,
and proves the subscriber catches up with the same book. The driver test skips when the
deepmsg checkout is absent, so the default workspace run needs no external
build. ADR 0004 records the channel and recovery semantics.

## Cluster integration (M4, in progress)

`engine-core` now has `Engine::snapshot()` / `Engine::restore()`:
the snapshot preserves the slab slots, every slot's generation, the free-list
order, the levels, the idempotency window, the open-order counts and the kill
switches. A restored engine has the same book hash and produces the same future
order ids and events as the engine that wrote the snapshot.

`engine-cluster` has the versioned `DBS1` snapshot record. The
`ClusteredService` adapter, the deepbook container binary, the gateway's
cluster client and the process-based single-node kill/restart e2e are the
remaining M4 slices; ADR 0005 records the snapshot decisions.

## Dependency on deepmsg

deepmsg is pinned by revision, not by branch. The pin and the local-development
patch recipe are in [`docs/deps.md`](docs/deps.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).
