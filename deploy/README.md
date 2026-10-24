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

## Later

- `docker-compose.yml` for driver / cluster / gateway / market-data / admin /
  prometheus / grafana.
- A three-node profile for the failover demo, no CPU pinning.
- Bind-core and huge-page scripts for the benchmark machine.
