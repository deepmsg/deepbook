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
deploy/bench.sh --keep ...                 # keep the run directory and logs
```

Environment: `DEEPBOOK_DEEPMSG_DIR` (default `../deepmsg`), `DEEPMSG_PROFILE`
(default `release`), `BENCH_RUN_DIR`, `BENCH_BASE_PORT`. The deepmsg binaries
must be built first (see `crates/spike/README.md`).

`--journal-sync batch --journal-batch N` is the default: the session journal
and sequence file sync once per N appends (ADR 0007). The default deployment
still uses `each`.

## Later

- `docker-compose.yml` for driver / cluster / gateway / market-data / admin /
  prometheus / grafana.
- A three-node profile for the failover demo, no CPU pinning.
- Bind-core and huge-page scripts for the benchmark machine.
- The io_uring backend selector and the epoll-vs-uring ABBA run in `bench.sh`.
