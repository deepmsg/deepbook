# deepbook

[![ci](https://github.com/deepmsg/deepbook/actions/workflows/ci.yml/badge.svg)](https://github.com/deepmsg/deepbook/actions/workflows/ci.yml)

An order book, matching engine, and FIX 4.4 acceptor, built as a **deterministic
state machine** on top of [deepmsg](https://github.com/deepmsg/deepmsg).

The goal is a runnable vertical slice that can be demonstrated and measured:
FIX client → gateway → replicated log → risk → matching → execution reports →
market data → snapshot/recovery, with a client-side RTT number and a replay
that produces the same state twice.

> Status: **M0–M5 merged.** The FIX session, epoll/io_uring acceptor, engine
> seam, matching engine, market-data channels and dashboard are in place. M4
> adds engine snapshot/restore, the `ClusteredService` adapter and
> `engine-container`, the gateway's `ClusterEngine`, `fix-gateway --cluster`,
> and a process e2e that kills the engine's host and continues the book from
> the recovered state (ADR 0005, ADR 0006). M5 adds the open-loop `fix-bench`
> client, opt-in journal batching and the epoll/io_uring comparison (ADR 0007,
> ADR 0009), plus the market-data → admin → WebSocket/Grafana path
> (ADR 0008).

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
`DEEPBOOK`/`CLIENT`) and journals each session under `--session-dir`. Without
`--cluster` the M1 stub answers; with it the matching engine runs in the
cluster (below). `--help` lists the options.

## Cluster mode (M4)

The engine is a deepmsg `ClusteredService` hosted by `engine-container`; the
gateway is a FIX acceptor in front of a deepmsg cluster session:

```text
FIX client ──TCP──▶ fix-gateway --cluster ──▶ cluster ingress ──▶ engine-container
                         ▲                          │
                         └──── cluster egress ◀─────┘
```

With the deepmsg binaries built (see
[`crates/spike/README.md`](crates/spike/README.md)) and a driver + single-node
cluster already running in `-Daeron.dir`:

```bash
cargo run -p deepbook-engine-cluster --bin engine-container -- \
  -Daeron.dir=/dev/shm/aeron -Daeron.cluster.dir=/dev/shm/cluster \
  -Daeron.cluster.members=0,localhost:25000,localhost:25010,localhost:25020,localhost:0,localhost:25040 \
  -Daeron.cluster.member.id=0 -Daeron.cluster.service.count=1 \
  -Daeron.cluster.ingress.channel=aeron:ipc -Daeron.cluster.egress.channel=aeron:ipc

cargo run -p deepbook-gateway --bin fix-gateway -- \
  --bind 127.0.0.1:9876 --session-dir target/fix-sessions \
  --cluster --aeron-dir /dev/shm/aeron
```

FIX `Account` is the engine's numeric account id (a missing account is `0`);
`--symbol`/`--symbol-id` name the one engine symbol. See ADR 0006 for the
mapping and its limits.

The process e2e covers the whole path — order, crossing, cancel, a cluster
snapshot, `SIGKILL` of the container and module, restart, an identical
`Snapshot` book hash and a continuation fill through the same FIX session:

```bash
cargo build -p deepbook-engine-cluster --bin engine-container
cargo test -p deepbook-gateway --test cluster_e2e -- --ignored --nocapture
```

(`just engine-e2e`). It is opt-in (`#[ignore]`) because it starts a media
driver, a consensus module, a service container and the gateway, and it skips
rather than fails when the deepmsg binaries are not built.

## Benchmark (M5)

`fix-bench` is an open-loop FIX client. It queues IOC buys at absolute send
deadlines, takes `t0` when the order is queued and `t1` when the first report
for its `ClOrdID` is decoded, and prints nearest-rank percentiles:

```bash
cargo run --release -p deepbook-bench --bin fix-bench -- \
  --connect 127.0.0.1:9876 --rate 10000 --samples 100000 --warmup 5000
# or one JSON object for a script:
cargo run --release -p deepbook-bench --bin fix-bench -- --json ...
```

The one-click stack (driver + module + `engine-container` + `fix-gateway
--cluster`) prints the machine fingerprint and the result table:

```bash
deploy/bench.sh --rate 10000 --samples 20000 --warmup 2000
deploy/bench.sh --journal-sync each ...   # the M1 durability policy
deploy/bench.sh --tick-us 50 ...          # poll ceiling; 100 ms default
```

`--journal-sync batch --journal-batch 64` (the script's default) batches the
session journal and sequence-file syncs; a crash then loses at most 63
outbound messages (ADR 0007). The default deployment keeps M1's
`fdatasync`-per-message policy.

The gateway runs on `epoll` by default and on `io_uring` with `--backend
uring` (ADR 0009). The backend comparison is one command:

```bash
deploy/bench.sh --compare --rate 5000 --samples 20000 --warmup 2000
```

It runs `epoll, uring, uring, epoll` (ABBA), a separate gateway process and
account per arm, and prints:

| backend | p50 | p90 | p99 | p99.9 | completed |
|---|---|---|---|---|---|
| epoll | 390 µs | 478 µs | 662 µs | 1.96 ms | 20 000 / 20 000 |
| io_uring | 173 µs | 257 µs | 383 µs | 1.85 ms | 20 000 / 20 000 |

Both arms sustained 5 000 orders/s on WSL2 at a 5 k/s offered rate; the uring
p50 is about 56% lower. The run takes about a minute because each arm waits
out the consensus module's session timeout before the next. These are test
numbers, not the bare-metal acceptance run.

Measured on the WSL2 development machine (i7-10875H, 16 vCPU) with a 10k/s
offered rate, 20k samples, `--tick-us 100`:

| journal policy | achieved | p50 | p99 | p99.9 |
|---|---|---|---|---|
| `each` (M1) | 5.5k orders/s | 1.07 ms | 1.76 ms | 1.77 ms |
| `batch 64` | 9.8k orders/s | 0.54 ms | 1.04 ms | 8.04 ms |

These are a smoke check on WSL2, **not** the design's acceptance numbers;
`deploy/bench.sh` on the bare-metal bench machine is the reproducible
measurement. The 50k/s offered run saturates below that rate and is reported
as a backlog, not as a throughput.

### Dashboard and Grafana

The engine publishes its `BookUpdate`/`Trade` stream on the same deepmsg
driver. `deepbook-admin` subscribes, mirrors the book and trade tape, and
serves a build-free dashboard plus Prometheus metrics:

```bash
deploy/bench.sh --admin --serve --rate 2000 --samples 5000
# dashboard: http://127.0.0.1:26060
# metrics:   http://127.0.0.1:26060/metrics
```

The page shows order-book depth, recent trades and the last benchmark's
p50/p90/p99; `/ws` pushes the conflated view at 10 Hz. The recovery channel is
served by a dedicated thread in `engine-container`, so a dashboard that
connects while the cluster is idle still gets a snapshot.

Grafana and Prometheus are provisioned from `deploy/`:

```bash
deploy/bench.sh --admin --serve --admin-host 0.0.0.0
docker compose -f deploy/docker-compose.monitoring.yml up -d
# Grafana http://127.0.0.1:3000, Prometheus http://127.0.0.1:9090
```

The dashboard panels cover benchmark RTT/throughput, market-data rate, book
levels and quantity, trades, and the cluster's commit position, snapshot count
and role. ADR 0008 records the market-data wiring and the admin design.

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
pre-trade limits, a bounded duplicate-`ClOrdID` window and end-of-day expiry
of resting DAY orders (a logged `ExpireDayOrders` command offered by the
cluster service at UTC midnight). A retry of an accepted order replays its
original acknowledgement instead of rejecting it.

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

## Cluster integration (M4)

`engine-core` has `Engine::snapshot()` / `Engine::restore()`:
the snapshot preserves the slab slots, every slot's generation, the free-list
order, the levels, the idempotency window, the DAY/GTC flag, the open-order
counts and the kill switches. A restored engine has the same book hash and
produces the same future order ids and events as the engine that wrote the
snapshot.

`engine-cluster` has the SBE `DBS2` snapshot record, the `EngineService`
`ClusteredService` adapter (command/event wire, per-session egress with a
back-pressure queue, chunked snapshot write and restore) and the
`engine-container` binary. The gateway's cluster client and the process-based
single-node kill/restart e2e are covered above; ADR 0005 and ADR 0006 record
the decisions.

## Dependency on deepmsg

deepmsg is pinned by revision, not by branch. The pin and the local-development
patch recipe are in [`docs/deps.md`](docs/deps.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).
