# Deploy

## Single-node cluster (the default)

deepmsg does not currently ship an all-in-one `ClusteredMediaDriver` binary, so
a single-node cluster is three processes:

```
deepmsg-archiving-media-driver        # media driver + archive (records the cluster log)
deepmsg-cluster-node                  # consensus module
deepmsg-clustered-service-container   # hosts the engine (M4)
```

Optional: `deepmsg-cluster-backup` (copy/recovery, not a voting member) and
`deepmsg-cluster-tool` (operator commands).

A single node is a real cluster: it elects itself, writes a leadership term to
`recording.log`, takes snapshots, and recovers from them. It has no failover.

## M0 spike

The M0 spike uses deepmsg's own `echo` or `basic-auction` business, not the
deepbook engine:

1. Start the archiving media driver.
2. Start the consensus module with a one-member `aeron.cluster.members` string.
3. Start the service container with `deepmsg.cluster.service=echo`.
4. Connect a Rust cluster client; measure `offer → egress`.
5. Take a snapshot, `kill -9` the container, restart, compare state.

The script and the exact property values land with the M0 spike; this file is
the checklist, not a claim that the script exists.

## M5 benchmark (`bench.sh`)

`bench.sh` is the one-click M5 measurement. It prints the machine fingerprint
(kernel, CPU, rustc, commit, binary digests, `io_uring_disabled`), starts one
driver + one consensus module + `engine-container` + `fix-gateway --cluster`
in a `/dev/shm` run directory, runs `fix-bench`, and tears everything down.

```bash
deploy/bench.sh --rate 10000 --samples 20000 --warmup 2000
deploy/bench.sh --journal-sync each ...    # M1's one-sync-per-message policy
deploy/bench.sh --tick-us 50 ...           # epoll poll ceiling (default 100 ms)
deploy/bench.sh --json ...                 # the client's JSON object
deploy/bench.sh --backend uring ...        # io_uring gateway (default epoll)
deploy/bench.sh --compare ...              # epoll, uring, uring, epoll (ABBA)
deploy/bench.sh --keep ...                 # keep the run directory and logs
```

Environment: `DEEPBOOK_DEEPMSG_DIR` (default `../deepmsg`), `DEEPMSG_PROFILE`
(default `release`), `BENCH_RUN_DIR`, `BENCH_BASE_PORT`. The deepmsg binaries
must be built first (see `crates/spike/README.md`).

`--journal-sync batch --journal-batch N` is the default: the session journal
and sequence file sync once per N appends (ADR 0007). The default deployment
still uses `each`.

## Dashboard and monitoring

`--admin` starts `deepbook-admin` with the stack; `--serve` keeps everything
running after the benchmark and prints the URLs:

```bash
deploy/bench.sh --admin --serve --rate 2000 --samples 5000
# dashboard: http://127.0.0.1:26060
# metrics:   http://127.0.0.1:26060/metrics
```

The dashboard serves a build-free `crates/ui` page (order book, trade tape,
benchmark RTT) over HTTP and WebSocket. `--bench-metrics` names the Prometheus
text file `fix-bench` writes (default `target/bench/last.prom`).

Grafana and Prometheus are provisioned from this directory:

```bash
deploy/bench.sh --admin --serve --admin-host 0.0.0.0
docker compose -f deploy/docker-compose.monitoring.yml up -d
# Grafana:    http://127.0.0.1:3000  (admin/admin)
# Prometheus: http://127.0.0.1:9090
```

The compose file scrapes `host.docker.internal:26060`, so the admin must bind
`0.0.0.0` (or the container's host gateway must be adjusted). The dashboard
shows textbook RTT p50/p90/p99, throughput, market-data rate, book levels and
quantity, trades, and the cluster's commit position/snapshot count/role.

## Later

- A compose file for the whole stack (driver / cluster / gateway / market-data /
  admin) with the monitoring profile; today only Prometheus + Grafana are
  composed.
- A three-node profile for the failover demo, no CPU pinning.
- Bind-core and huge-page scripts for the benchmark machine.
- io_uring multishot accept/recv, registered buffers and optional SQPOLL;
  today the backend uses plain completion operations (ADR 0009).
