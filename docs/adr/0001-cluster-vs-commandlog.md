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

## Evidence

Record the readings in this ADR when the spike closes: machine, kernel,
deepmsg rev, p50/p99, recovery time, and the exact commands. A number without
its machine is not evidence.
