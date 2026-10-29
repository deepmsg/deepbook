# deepbook-spike

The M0 cluster check: start deepmsg's **production binaries** as processes, drive
them with a Rust client, and measure what the design's M0 gate asks for.

This crate is `publish = false` and is not part of the product. It does not use
deepmsg's test harness (`deepmsg-tests`); deepmsg is an external, pinned
dependency (see `docs/deps.md`).

## What it runs

`smoke`, `p99` and `gate3` run one member:

```
deepmsg-archiving-media-driver        media driver + archive
deepmsg-cluster-node                  consensus module, one member
deepmsg-clustered-service-container   stock `basic-auction` business
```

The client-facing ingress/egress and the cluster's log/consensus channels are
IPC: every process shares one driver on one machine. The archive's control
channel is UDP on the member string's archive endpoint.

`gate4` runs three members (one driver + archive, one module and one container
each) plus a driver of the client's own; see [Gate 4](#gate-4-three-nodes-and-a-leader-kill).

**Child environment.** Children are started with every `DEEPMSG_*` and
`AERON_*` variable removed. deepmsg's configuration lets `DEEPMSG_DIR` beat a
`-D` argument, so a parent that sets it to locate the binaries would otherwise
point the driver's aeron directory at the source checkout. The spike passes the
directory explicitly (`deepmsg.dir` and `aeron.dir`) and the children inherit
nothing.

## Build the deepmsg binaries

From the deepmsg checkout named by `DEEPBOOK_DEEPMSG_DIR` (default `../deepmsg`):

```bash
just deps
```

That builds the pinned revision and stamps it (`scripts/deepmsg-deps.sh`); the
spike runs the same four binaries by hand when it is not going through `just`:

```bash
cargo build --release \
  -p deepmsg-archive --bin deepmsg-archiving-media-driver \
  -p deepmsg-cluster --bin deepmsg-cluster-node \
  -p deepmsg-cluster-service --bin deepmsg-clustered-service-container \
  -p deepmsg-cluster-tool --bin deepmsg-cluster-tool
```

## Run

```bash
# one bid, one egress
cargo run -p deepbook-spike --bin cluster-spike -- smoke

# open-loop offer -> egress
cargo run -p deepbook-spike --bin cluster-spike -- p99 --rate 50000

# snapshot, SIGKILL every process, restart, compare the auction state
cargo run -p deepbook-spike --bin cluster-spike -- gate3 --bids 1000

# three nodes, kill the leader, measure the client's recovery and check the
# egress accounting
cargo run -p deepbook-spike --bin cluster-spike -- gate4 --bids 100
```

Use `--keep` to keep the run directory, and `--base-port` when another process
holds the block. The run directory defaults to
`/dev/shm/deepbook-spike-<pid>`.

## Gate 4: three nodes and a leader kill

`gate4` stands up three members, each with its own driver + archive, consensus
module and `basic-auction` container, plus a driver the client owns so a killed
member cannot take it down. The members form one cluster over UDP: each
member's ingress, consensus and log channel has an endpoint of its own and the
member string names all three.

The run bids, `SIGKILL`s the leader's three processes, and times:

```
kill -> new leader ACTIVE -> client told -> first accepted bid -> its egress
```

The gate is the last number: under five seconds. The first bid after the kill
is deliberately lower than the pre-kill best, so its losing egress also proves
the committed state survived. Every correlation id must arrive exactly once;
that is the "no duplicate fill" check for a service with no fill ledger.

Timing values are test values, not the product's defaults:

| Property | Value | Why |
|---|---|---|
| `aeron.cluster.leader.heartbeat.timeout` | 2 s | the reference's non-Windows `TestCluster` value; the 10 s default cannot fit a five-second gate |
| `aeron.cluster.election.timeout` | 250 ms | the ballot wait is part of the budget |
| `aeron.image.liveness.timeout` (members) | 500 ms | `prepareForNewLeadership` waits for the failing member's log image; the 10 s default spends the budget there |
| `aeron.image.liveness.timeout` (client) | driver default | the client's egress image must outlive the election: its subscription is `rejoin=false` |

**The lost `NewLeaderEvent`.** The new leader sends the event once per open
session. On the pinned deepmsg rev it is sometimes lost when member 0 is the
new leader: the leader is active and serves a fresh client immediately, but the
old session is never told. `gate4` therefore opens a fresh session on its own
egress port if the event has not arrived shortly after the leader is active
(the reference's own failover tests use `reconnectClient()` the same way). The
result line says `reconnected=true` when that happened; the number still
measures kill-to-egress. The client's inability to refine a wildcard egress
endpoint is why the fresh session needs a port of its own.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `DEEPBOOK_DEEPMSG_DIR` | `../deepmsg` | deepmsg checkout with the built binaries |
| `DEEPMSG_PROFILE` | `release` | `target/<profile>` |
| `SPIKE_BASE_PORT` | `25000` | first port of the block |
| `SPIKE_RUN_DIR` | `/dev/shm/deepbook-spike-<pid>` | run directory |
| `SPIKE_KEEP` | unset | keep the run directory |

## Why `basic-auction`

It is the reference tutorial business that ships in deepmsg's compiled-in
registry. It has observable state — best price and winning customer — writes
that state to its snapshot, and reports it in every egress. Gate 3 therefore
needs no custom service and no custom container binary.

The spike is not part of CI: it needs the deepmsg checkout and real child
processes. `cargo check` still compiles it.
