# cluster-4host

Three cluster members on three machines, the FIX gateway and the load client on
a fourth. This is the same topology as `crates/spike`'s `failover` run, with
`localhost` replaced by each member's own address and the processes placed on
different hosts.

```
deploy/cluster-4host/run-cluster.sh up
deploy/cluster-4host/run-cluster.sh status
deploy/cluster-4host/run-cluster.sh bench --rate 20000 --samples 200000 --warmup 20000
deploy/cluster-4host/run-cluster.sh kill-leader
deploy/cluster-4host/run-cluster.sh counters
deploy/cluster-4host/run-cluster.sh down
```

The four hosts and every tunable live in `hosts.env` (all overridable from the
environment). `run-cluster.sh list` prints the topology it would stand up —
read it before the first `up`.

## What runs where

| host | processes |
|---|---|
| node0 / node1 / node2 | `deepmsg-archiving-media-driver`, `deepmsg-cluster-node`, `engine-container` |
| client | its own `deepmsg-archiving-media-driver`, `fix-gateway`, `fix-bench` |

`member-node.sh` is the per-member half and the only file that is copied to the
members. It reads `member.properties` (written by `run-cluster.sh`) and starts,
stops and checks the three processes; `counters` is `cnc-dump --all` against the
member's own aeron directory, taken **before** anything is stopped.

## Readiness is two checks, not one

`up` waits for both:

1. every member's `up` file — three green members, not "two out of three" (a
   quorum of two lets a three-member cluster elect and trade with a dead
   member); and
2. exactly one member whose `deepmsg-cluster-tool <cluster_dir> is-leader`
   exits 0. That tool reads the `Cluster node role` and `Cluster election
   state` counters, so it is the cluster's own answer rather than a log line.

## The channels

Taken from `crates/spike/src/nodes.rs` with the member's address substituted:

- ingress and consensus: `aeron:udp?term-length=128k|endpoint=<host>:<port>`
- log: `aeron:udp?term-length=512k` — **no endpoint**: a three-member cluster
  publishes to the others and the module applies `control-mode=manual` itself.
  An endpoint here makes followers time out with "failed to join live log".
- control, replay, snapshot, local archive control: `aeron:ipc`
- replication: `aeron:udp?endpoint=<host>:0` (the port is resolved at run time)
- the gateway's ingress: `aeron:udp?term-length=128k` plus
  `--ingress-endpoints 0=<host>:<port>,…`; the client library substitutes the
  endpoint of whichever member the leader is.
- the gateway's egress template: `aeron:udp?endpoint=<client host>:0`

Ports are `base + …` with `base = 25000`, which is below the ephemeral range
(32768–60999): a fixed port inside that range can be taken by any process
between two runs.

## Before the first failover run

`--new-leader-timeout-ms` is set to 15000 here, not the gateway's own 4000.
The single-machine failover run measures an election at **4.2–4.5 s** with the
`2s` heartbeat, so a 4 s gateway timeout would expire before a new leader
exists and the run would measure the gateway, not the cluster.

`market-data` is off (`deepbook.marketdata.enabled=false`, matching the spike's
engine service): every member applies the log, so every member would publish its
own stream.

## Not this script's job

- Building. Binaries come from `$CLUSTER_DEEPMSG_DIR/target/release` and
  `$CLUSTER_DEEPBOOK_DIR/target/release`; `scripts/deepmsg-deps.sh build` makes
  the deepmsg side and stamps it with the revision `Cargo.toml` pins.
- The pinned revision. See `docs/deps.md`.
