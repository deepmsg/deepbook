# ADR 0001: cluster or CommandLog?

- Status: **Proposed** — decided at the M0 spike
- Date: 2026-10-24
- Deciders: project owner

## Context

The design (v0.3, D11/D12) puts the engine inside a deepmsg cluster as a
`ClusteredService`, and keeps a `CommandLog` publication implementation as the
fallback. The cluster path buys ordering, a replicated log, snapshots,
recovery, and failover; the fallback buys independence from a codebase that is
still being ported.

The M0 spike exists to make this decision on evidence, not prose.

## Decision

Deferred to the M0 spike. The gate is four measurements; **passing three of
four adopts the cluster**, otherwise the `CommandLog` publication path is used
and the cluster integration moves behind the same seam for a later milestone.

| # | Gate | Instrument |
|---|---|---|
| 1 | Rust consensus + Rust container + Rust client, single node, `echo` round trip | deepmsg `cluster_harness` or the three binaries |
| 2 | `offer → egress` p99 < 200 µs, local | HDR histogram, same machine |
| 3 | snapshot, kill -9, restart, state hashes equal | `basic-auction` + `ClusterTool` |
| 4 | three-node leader kill, client recovers < 5 s, no duplicate fills | optional/stretch; LC-2 already exercises the mechanism |

Gate 4 is a stretch for M0. It is the M6 story; the decision should not wait on
it if gates 1–3 are green.

## Consequences

- **Adopt cluster:** `engine-cluster` depends on `deepmsg-cluster-service`; the
  gateway depends on `deepmsg-cluster-client`; snapshot format is versioned;
  the M0 pin of deepmsg becomes load-bearing.
- **Fall back:** `CommandLog` is a publication + a replay cursor; ordering is
  single-node only; snapshots and failover are deferred. The `engine-core`
  crate is unaffected either way because it depends only on `types`.

## Spike results (2026-10-24)

Environment: WSL2 on an Intel i7-10875H (16 logical CPUs), kernel
6.6.87.2-microsoft-standard-WSL2, **release** profile, deepmsg `8c70972`.
The cluster is deepmsg's production binaries as child processes:

```
deepmsg-archiving-media-driver
deepmsg-cluster-node
deepmsg-clustered-service-container   (stock `basic-auction`)
```

Client ingress/egress and the cluster's log/consensus channels are IPC; the
archive control channel is UDP on the member string's endpoint. The driver, CM
and container use busy-spin idle strategies. The client is `deepbook-spike`.
Gate 4 runs three members, each with a driver + archive, module and container,
plus a driver of the client's own; the member channels are UDP with an endpoint
per member.

```bash
DEEPBOOK_DEEPMSG_DIR=../deepmsg cargo run --release -p deepbook-spike --bin cluster-spike -- smoke
DEEPBOOK_DEEPMSG_DIR=../deepmsg cargo run --release -p deepbook-spike --bin cluster-spike -- p99 --rate 50000
DEEPBOOK_DEEPMSG_DIR=../deepmsg cargo run --release -p deepbook-spike --bin cluster-spike -- gate3 --bids 100
DEEPBOOK_DEEPMSG_DIR=../deepmsg cargo run --release -p deepbook-spike --bin cluster-spike -- gate4 --bids 100
```

| Gate | Result |
|---|---|
| 1. Rust CM + Rust container + Rust client, single node, echo | **pass** — smoke: one `basic-auction` bid and its egress; the p99 run also completes 21,000 offers with every egress accounted for |
| 2. `offer -> egress` p99 < 200 µs | **borderline on WSL2** — p50 4.5–5.4 µs; p99 46 / 141 / 188 / 203 µs at 50K bids/s, 182 µs at 100K/s. Three of five runs are under 200 µs; the tail is host noise. The median is far under the gate; re-run on the bare-metal bench machine for a stable pass |
| 3. snapshot, kill -9, restart, hashes equal | **pass** — `basic-auction` best price 199 / winner 99 before the snapshot; SNAPSHOT toggle; `SIGKILL` of every process; restart on the same archive and cluster directories at archive sync level 2; a lower post-restart bid loses and reports winner=99, best=199 |
| 4. three-node leader kill | **pass on the test configuration** — ten consecutive `gate4 --bids 20` runs: 10/10, recovery (kill → the probe's egress) 4.22–4.72 s, every correlation id exactly once, pre-kill winner/price intact on the new leader. Two runs opened a fresh session (see below) |

Gate 4 uses test timing values, not the product defaults:
`aeron.cluster.leader.heartbeat.timeout=2s` (the reference's non-Windows
`TestCluster` value), `aeron.cluster.election.timeout=250ms`, and a 500 ms
`aeron.image.liveness.timeout` on the **member** drivers. The product's 10 s
heartbeat timeout cannot meet a five-second budget: failure detection alone
spends the whole budget. The client driver keeps the 10 s image default because
its egress subscription is `rejoin=false`, so its image must outlive the
election.

Gate 4 also found a client limitation worth recording: the new leader sends
**one** `NewLeaderEvent` per open session, and on the pinned deepmsg rev that
event is sometimes lost when member 0 becomes the new leader (5 losses in
roughly 20 runs before the fallback). The new leader is active and serves a
fresh client immediately; only the old session is never told. `gate4` therefore
falls back to a fresh session on its own egress port when no event arrives
shortly after the leader is active — the same recovery the reference's
`reconnectClient()` tests use — and reports `reconnected=true`. The gate's
timing includes that path.

Gate 3's "hash" is the auction's `(best price, winner)` — the book-hash
equivalent for the stock business. The in-process harness used for the first
pass has been removed; its numbers (p50 81–91 µs, p99 177–349 µs) are
superseded by the process-based run above. deepmsg stays clean; deepbook owns
the spike.

Preliminary: gates 1, 3 and 4 pass; gate 2 is borderline on WSL2 (p50 ≈ 5 µs,
p99 mostly under 200 µs, one run at 203 µs). The design's "3 of 4" rule is
therefore met, and the evidence favours the cluster. Gate 2 still needs the
bench machine for a stable latency verdict, and the owner should decide whether
the client's lost-`NewLeaderEvent` behaviour (above) is acceptable at this
rev before flipping this ADR to Accepted.

### Implementation note for the gateway

`AeronCluster::offer` returns `Some(Appended::EndOfLog)` when the ingress
publication rotates its term. A caller that treats any `Some` as success loses
one command per term: the first spike run lost 15 of 21,000 at 128 KiB and 30
of 21,000 at 64 KiB. The gateway must retry unless the result is
`Appended::Ok { .. }` (retry on `EndOfLog`, `MidRotation`, `BackPressured`,
`NotConnected`). This is now an assertion in the spike.

A gateway should also own the failover of its cluster session: the new leader's
`NewLeaderEvent` is single-shot, and on this rev it can be lost. If the session
is not told shortly after the new leader is active, open a fresh session on a
new egress port (the client cannot refine a wildcard endpoint) and keep the
old session until it times out. `gate4` is the reference implementation of
that loop.

