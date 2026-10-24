# deepbook

[![ci](https://github.com/deepmsg/deepbook/actions/workflows/ci.yml/badge.svg)](https://github.com/deepmsg/deepbook/actions/workflows/ci.yml)

An order book, matching engine, and FIX 4.4 acceptor, built as a **deterministic
state machine** on top of [deepmsg](https://github.com/deepmsg/deepmsg).

The goal is a runnable vertical slice that can be demonstrated and measured:
FIX client → gateway → replicated log → risk → matching → execution reports →
market data → snapshot/recovery, with a client-side RTT number and a replay
that produces the same state twice.

> Status: **M0 — skeleton**. The workspace builds and the tests run, but there
> is no engine, gateway, or wire protocol yet. See the milestone table below.

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

## Milestones

| Stage | Days | Deliverable | Acceptance |
|---|---|---|---|
| M0 | 3 | skeleton, `types` v0, SBE schema v0, cluster spike | `cargo test` green; Rust CM + container + client echo |
| M1 | 5 | FIX 4.4 session subset (epoll), NewOrderSingle, ExecutionReport, outbound journal | QuickFIX/J logon, order, report; resend/gap rules |
| M2 | 4 | `engine-core`: limit/market, GTC/IOC/FOK, cancel/replace; counting allocator; Vec oracle | unit + proptest + differential oracle green |
| M3 | 3 | market data incremental + snapshot + recovery; loss-generator test | subscriber recovers from a forced gap |
| M4 | 4 | single-node cluster integration; snapshot + recovery + determinism | replay twice → same hashes; kill -9 recovery |
| M5 | 3 | io_uring, benchmark, Grafana, UI | client-side RTT table; epoll vs io_uring |
| M6 (optional) | 4 | three-node failover; price-band array | leader kill → recovery under 5 s, no duplicate fills |

The full design is in [`docs/design/deepbook_架构设计_v0.3.md`](docs/design/deepbook_架构设计_v0.3.md).
Reviews and porting notes are under [`docs/reviews/`](docs/reviews/) and
[`docs/notes/`](docs/notes/).

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
  ui/              static dashboard (not a workspace member)
schemas/           SBE XML
deploy/            compose, bind-core scripts, spike scripts
docs/              design, reviews, ADRs
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

## Dependency on deepmsg

deepmsg is pinned by revision, not by branch. The pin and the local-development
patch recipe are in [`docs/deps.md`](docs/deps.md).

## License

Apache-2.0. See [`LICENSE`](LICENSE).
