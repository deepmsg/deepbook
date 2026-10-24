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
8c70972
```

The cluster checks in `crates/spike` consume those crates and run deepmsg's
**production binaries** from `DEEPBOOK_DEEPMSG_DIR` (default `../deepmsg`). The
spike strips every `DEEPMSG_*` and `AERON_*` variable from its children: deepmsg
lets `DEEPMSG_DIR` beat a `-D` argument, so a parent that sets it to locate the
binaries must not leak it into the driver process.

It contains the single-node cluster (consensus module, service container,
client, `echo` / `basic-auction` businesses), which is all the spike needs.
The known gaps (`Cluster.aeron()`, `Cluster.context()`, vectored offer, and an
all-in-one `ClusteredMediaDriver` binary) are not on the spike path; they
matter once the engine is hosted in the cluster. See ADR 0001.

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
