# deepmsg dependency

deepbook depends on deepmsg as infrastructure. deepmsg is **not** published to
crates.io, so the dependency is a git dependency pinned to a revision.

## Policy

- Pin a `rev`, never `branch = "main"`. deepmsg is under active development;
  a moving branch would make builds non-reproducible.
- Bump the pin deliberately, in its own commit, with a note in the commit
  message saying which deepmsg change is being taken.
- CI builds against the pinned rev. A separate scheduled job may build against
  deepmsg `main` to catch breakage early, but that job is advisory only.

## Current pin

Wired in the root `Cargo.toml` under `[workspace.dependencies]` at:

```
4bddeef
```

The cluster checks in `crates/spike` consume those crates and run deepmsg's
**production binaries** from `DEEPBOOK_DEEPMSG_DIR` (default `../deepmsg`). The
spike strips every `DEEPMSG_*` and `AERON_*` variable from its children: deepmsg
lets `DEEPMSG_DIR` beat a `-D` argument, so a parent that sets it to locate the
binaries must not leak it into the driver process.

The bump from `d8eb6cf` takes twelve deepmsg commits, all in the archive,
consensus-module and driver internals; no client or service API changed:

- `A member keeps the append position it recovered` and `The extend waits for
  the recording it extends` — recovery positions;
- `The ingress abort is not swallowed`, `A publication that has drained is not
  shared` — publication lifecycle;
- `A module stops before the containers it waits on`, `A closing module does
  not wait for its own container` — shutdown ordering;
- the port bands moving out of the ephemeral range — the reason the process
  tests no longer race a random listener.

It contains the single-node cluster (consensus module, service container,
client, `echo` / `basic-auction` businesses), which is all the spike needs.
The service API now has `Cluster::client()`, `Cluster::context()`,
`SnapshotWriter::client()` (read-only, for the snapshot callback) and the
vectored/claim offers. Two gaps remain: `on_role_change` is handed no `Cluster`
and so cannot publish from the business's own publication, and there is no
all-in-one `ClusteredMediaDriver` launcher — `deepmsg-cluster-node` attaches to
a driver and archive somebody else runs. See ADR 0001.

## Local binaries

The cluster e2e, `crates/spike` and `deploy/bench.sh` start deepmsg's binaries
as processes, so cargo's pinned *crates* do not make those binaries pinned. One
command builds and records them:

```bash
just deps          # scripts/deepmsg-deps.sh build
just deps-check    # scripts/deepmsg-deps.sh check
```

`deps` builds the four binaries (`deepmsg-archiving-media-driver`,
`deepmsg-cluster-node`, `deepmsg-clustered-service-container`,
`deepmsg-cluster-tool`) in `DEEPBOOK_DEEPMSG_DIR` at `DEEPMSG_PROFILE`
(`release` by default) and writes `target/<profile>/deepbook-deps.txt`: the
revision, the profile, the compiler and the SHA-256 of each binary. `check`
fails when the stamp is missing, names another revision, or a binary no longer
matches its hash — that is what makes a benchmark number reproducible rather
than "whatever was last built". `bench.sh` and `just engine-e2e` run it by
themselves; `DEEPBOOK_SKIP_DEPS_CHECK=1` measures an unpinned stack on purpose.

The checkout itself must be at the pin. When it is a branch of your own, build
the pin in a worktree instead — the script prints the two commands:

```bash
git -C ~/data/trader/deepmsg worktree add --detach ~/data/trader/deepmsg-pin 4bddeef
DEEPBOOK_DEEPMSG_DIR=~/data/trader/deepmsg-pin just deps
```

CI does not use the script: the `cluster-e2e` workflow checks the same revision
out and builds it with the same package/binary list.

## Cargo.toml shape

```toml
[workspace.dependencies]
deepmsg-client          = { git = "https://github.com/deepmsg/deepmsg", rev = "<sha>" }
deepmsg-cluster-client  = { git = "https://github.com/deepmsg/deepmsg", rev = "<sha>" }
deepmsg-cluster-service = { git = "https://github.com/deepmsg/deepmsg", rev = "<sha>" }
```

## Local co-development

When editing deepmsg and deepbook together, point Cargo at the local checkout
with `[patch]` in the **root** `Cargo.toml`:

```toml
[patch."https://github.com/deepmsg/deepmsg"]
deepmsg-client          = { path = "../deepmsg/crates/client" }
deepmsg-cluster-client  = { path = "../deepmsg/crates/cluster/client" }
deepmsg-cluster-service = { path = "../deepmsg/crates/cluster/service" }
```

Do not commit the patch. It is a local-development convenience; CI and release
builds use the pinned rev.
