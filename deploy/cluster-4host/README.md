# cluster-4host

Three cluster members on three machines, the FIX gateway and the load client on
a fourth. This is the same topology as `crates/spike`'s `failover` run, with
`localhost` replaced by each member's own address and the processes placed on
different hosts.

```
deploy/cluster-4host/run-cluster.sh up
deploy/cluster-4host/run-cluster.sh status
deploy/cluster-4host/run-cluster.sh bench --rate 20000 --samples 200000 --warmup 20000
deploy/cluster-4host/run-cluster.sh scenario my-orders.txt
deploy/cluster-4host/run-cluster.sh kill-leader --with-bench 2000 40000
deploy/cluster-4host/run-cluster.sh restart 1 --keep
deploy/cluster-4host/run-cluster.sh counters
deploy/cluster-4host/run-cluster.sh observability up|status|down
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

The driver's term buffers and its CnC are memory-mapped files, so they live in
`CLUSTER_AERON_ROOT` — a tmpfs, `/dev/shm/deepbook` by default. The run root is
on the ESSD, and a memory-mapped file there puts a page fault in whatever path
touches it, which for this workload is the measured path. The archive and the
cluster directory stay with the run root: those have to survive the run.

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

`--new-leader-timeout-ms` is set to 15000 here, not the gateway's own 4000, and
the knob that decides the budget is **not** the heartbeat. On four machines a
surviving member's election state does not move until
`aeron.image.liveness.timeout` — 10 s by default — plus about 3.6 s, and the
election itself takes 0.4 s. That is 13.6 s measured, so 15 s leaves under
1.5 s of margin; anything else you change has to come out of that.

`CLUSTER_IMAGE_LIVENESS` is that knob, and setting it to `2s` brings a failover
under 6 s. Do not set it small without knowing how much the log channel jitters:
a gap longer than the window is indistinguishable from a dead leader, and the
cluster will elect a new one that nobody asked for.

`market-data` is off (`deepbook.marketdata.enabled=false`, matching the spike's
engine service): every member applies the log, so every member would publish its
own stream.

## Three things that look like bugs and are not

**A killed member still answers `is-leader`.** The tool reads the member's CnC
file, and that file outlives the process: for the whole of a failover the dead
member's role counter still says leader. `_leader` therefore asks the member's
own pid file first, and `status` prints `not running` rather than `leader`.

**`up` deletes the previous run.** `CLUSTER_RUN_ROOT/member*` holds the logs,
the archives and the counters; `up` removes it. Pull what a run produced before
starting the next one, or it is gone — the archives take the snapshot with them.

**The leader's host changes the numbers.** These machines were measured with an
8–15% p50 difference between a leader on `node0` and the same cell with the
leader elsewhere, so `CLUSTER_APPOINTED_LEADER` pins it by default. Clear it
(`CLUSTER_APPOINTED_LEADER=`) for a failover run, where the election is the
thing being measured — a pinned leader is not replaced when it dies.

## Observability

The applications write local JSON lines and a loopback Prometheus page; a node
agent reads them and ships them to a fifth machine. That machine is neither a
member nor the client — the gateway and the load client own those cores, and a
log store's compactions must not be part of a reading.

```
deploy/cluster-4host/run-cluster.sh observability up
deploy/cluster-4host/run-cluster.sh observability status
deploy/cluster-4host/run-cluster.sh observability down
```

| host | units | what the applications write |
|---|---|---|
| node0 / node1 / node2 | `deepmsg-app-agent`, `deepmsg-counters-exporter`, `deepmsg-errorlog-exporter` | `/var/log/deepmsg/{driver,cluster,engine}-node<N>.json` |
| client | the same three | `/var/log/deepmsg/{driver,gateway,tool}-client.json` |
| `CLUSTER_OBS_SSH` | `victoria-logs`, `victoria-metrics`, `vmauth`, and `grafana` when installed | — |

`up` needs three variables it cannot invent:

```bash
export CLUSTER_OBS_SSH=root@172.16.1.21      # the fifth host, private address
export CLUSTER_OBS_WRITE_TOKEN=...            # vmauth's write bearer
export CLUSTER_OBS_READ_TOKEN=...             # vmauth's read bearer (Grafana)
# optional: CLUSTER_OBS_GRAFANA_PASSWORD=...
# optional: CLUSTER_OBS_WEBHOOK_URL=https://...  (alerts; default is the E2E's
#           local receiver at http://127.0.0.1:9099/alerts)
```

The units, the collector configs and the pinned tool releases are deepmsg's
(`deploy/observability/`), read from the controller's checkout
(`DEEPBOOK_DEEPMSG_DIR`, default the sibling `deepmsg`); the hosts' own
checkouts are at `CLUSTER_DEEPMSG_DIR`, whose `deploy/observability/e2e/
fetch-tools.sh` downloads and SHA-verifies `otelcol-contrib`, `victoria-logs`,
`victoria-metrics` and `vmauth`.

Three things worth knowing:

- **The log directory is outside the run root** (`CLUSTER_LOG_DIR`,
  `/var/log/deepmsg`), because `up` removes the run root. `observability down`
  stops processes and keeps the logs and the backends' data.
- **`deepmsg-app-agent` replaces `deepmsg-log-agent`**, not adds to it: they
  share a state directory, and two collectors would each keep their own file
  offsets. All four application hosts expose a `/metrics` endpoint (D3), so the
  overlay unit is the enabled one; the base unit is installed for a host that
  later has none.
- **A member's cluster directory is under the run root** (`/root/...`) and
  `ProtectHome=yes` hides it from the error-log exporter. The installer adds a
  drop-in with `ProtectHome=read-only` for that unit on members only — the
  exporter reads that directory and nothing else there.

### Dashboards

The log host's Grafana loads deepmsg's three dashboards into the `deepmsg`
folder and deepbook's `deepbook-engine` into a `deepbook` folder of its own:
the provider is `deploy/observability/grafana/provisioning/dashboards/deepbook.yaml`
and the directory it scans is `/etc/deepmsg/grafana/dashboards-deepbook`. The
dashboard source is `deploy/observability/grafana/dashboards/deepbook-engine.json`
and `just check-observability` (or `python3 deploy/observability/check.py`) is
its gate: every metric a panel names must be a series the deployment
publishes, and the `node` variable reads `node_id` from `deepmsg_driver_up`, so
it covers the client host as well as the members.

`observability up` changes nothing about the run: the applications are still
started by `run-cluster.sh`, which exports `DEEPMSG_LOG_DIR`,
`DEEPMSG_NODE_ID` and `DEEPMSG_LOG_LEVEL` for them (`member-node.sh` and the
client's scripts). With the agent stopped, the processes keep writing and their
stderr stays empty; that contract is deepmsg's `crates/log/README.md`.

### Alerts and the runbook

The log host's Grafana provisions deepmsg's six rules and deepbook's six. They
are separate files in the same alerting directory — Grafana loads every YAML
there, while deepmsg's own config check only counts `rules.yaml`, so the split
keeps deepmsg's "exactly six" green and this repository owns its own six:

- source: `deploy/observability/grafana/alerting/deepbook-rules.yaml`
- installed as `/etc/deepmsg/grafana/provisioning/alerting/deepbook-rules.yaml`
- group `deepbook` in folder `deepbook` (the same folder as the dashboard)

The six: fewer than three engines reporting, no leader, commit stalled while
orders arrive, no gateway session, a reject stream, and dropped log lines. Each
names an anchor in `deploy/observability/runbook.md`, which is the page to open
when one fires. `just check-observability` validates the rules the same way it
validates the dashboard: every series they name is one the deployment
publishes, every rule has the classic condition, a P1/P2/P3 severity and a
runbook anchor that exists.

`CLUSTER_OBS_WEBHOOK_URL` is where the log host's contact point posts; it is
read by Grafana's provisioning as `$__env{DEEPMSG_GRAFANA_WEBHOOK_URL}`.

## Not this script's job

- Building. Binaries come from `$CLUSTER_DEEPMSG_DIR/target/release` and
  `$CLUSTER_DEEPBOOK_DIR/target/release`; `scripts/deepmsg-deps.sh build` makes
  the deepmsg side and stamps it with the revision `Cargo.toml` pins.
- The pinned revision. See `docs/deps.md`.
