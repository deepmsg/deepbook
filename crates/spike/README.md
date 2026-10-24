# deepbook-spike

The M0 cluster check: start deepmsg's **production binaries** as processes, drive
them with a Rust client, and measure what the design's M0 gate asks for.

This crate is `publish = false` and is not part of the product. It does not use
deepmsg's test harness (`deepmsg-tests`); deepmsg is an external, pinned
dependency (see `docs/deps.md`).

## What it runs

```
deepmsg-archiving-media-driver        media driver + archive
deepmsg-cluster-node                  consensus module, one member
deepmsg-clustered-service-container   stock `basic-auction` business
```

The client-facing ingress/egress and the cluster's log/consensus channels are
IPC: every process shares one driver on one machine. The archive's control
channel is UDP on the member string's archive endpoint.

**Child environment.** Children are started with every `DEEPMSG_*` and
`AERON_*` variable removed. deepmsg's configuration lets `DEEPMSG_DIR` beat a
`-D` argument, so a parent that sets it to locate the binaries would otherwise
point the driver's aeron directory at the source checkout. The spike passes the
directory explicitly (`deepmsg.dir` and `aeron.dir`) and the children inherit
nothing.

## Build the deepmsg binaries

From the deepmsg checkout named by `DEEPBOOK_DEEPMSG_DIR` (default `../deepmsg`):

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
```

Use `--keep` to keep the run directory, and `--base-port` when another process
holds the block. The run directory defaults to
`/dev/shm/deepbook-spike-<pid>`.

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
