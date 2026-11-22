# ADR 0010: identifiers do not carry the Aeron brand

- Status: **Accepted** — 2026-11-22
- Date: 2026-11-22
- Deciders: project owner

## Context

deepbook runs on deepmsg, which is a from-scratch implementation of the Aeron
stack, so a good part of what deepbook configures is named by Aeron: property
keys (`aeron.dir`, `aeron.cluster.*`), channel URIs (`aeron:ipc`,
`aeron:udp?…`), the reference's environment dialect (`AERON_*`) and the
`-Daeron.*` arguments handed to a driver or module. Those names are the
driver's and the protocol's — they have to match the reference byte for byte
and deepbook cannot rename them.

That is not the same thing as carrying the brand into deepbook's own names,
and the two had been mixed. The directory a media driver publishes its CnC
file into was named `aeron`, `aeron_dir` and `client_aeron_dir` in Rust and
shell alike; a reader meeting `dirs.aeron` has no way to tell whether it is
deepbook's concept or the dependency's name, and the brand is also the one
thing that survives a future decision to change the media layer underneath.
deepbook's *own* configuration had borrowed it too —
`deepbook.marketdata.aeron.dir`, the `--aeron-dir` flag, the deployment's
`aeron_dir` key and `CLUSTER_AERON_ROOT` — although nothing outside deepbook
reads any of them.

## Decision

- **Rust and shell identifiers — types, fields, functions, parameters, locals,
  modules — do not contain `aeron` in any case.** The path that holds a
  driver's CnC file and its state is `driver_dir` (`client_driver_dir` for the
  client's own driver), including the `DirSet`/`Harness`/`Driver` fields and
  the `Paths` helper. `driver` would collide with the child process in the
  gateway's test harness, so `driver_dir` is the one spelling.
- **Official names keep the brand.** Anything the reference or deepmsg reads
  by that name stays: property keys (`aeron.dir`, `aeron.cluster.*`,
  `aeron.archive.*`, `aeron.image.*`, `aeron.threading.mode`), URI schemes
  (`aeron:ipc`, `aeron:udp?…`), the reference's environment dialect
  (`AERON_*`), the `-Daeron.*` arguments handed to a driver or module, and the
  on-disk directory names (`aeron/`, `aeron-<n>/`). Renaming one of those
  changes a wire or deployment contract, not a name. `aeron.dir` is in this
  group even though deepmsg also accepts its native `deepmsg.dir`: a reference
  tool pointed at the same deployment reads the official one.
- **deepbook's own configuration surfaces drop it.** A property deepbook
  defines, a flag deepbook parses and a deployment key deepbook generates are
  ours to name: `deepbook.marketdata.driver.dir` (was
  `deepbook.marketdata.aeron.dir`), `--driver-dir` (was `--aeron-dir`), the
  `member.properties` key `driver_dir` (was `aeron_dir`) and the deploy
  scripts' `CLUSTER_DRIVER_ROOT` (was `CLUSTER_AERON_ROOT`).
- **A dependency's field is adapted once.** deepmsg's
  `ServiceContainerConfig::aeron_dir` is its official name; the one use copies
  it into a local `driver_dir` and says why.
- **Citations keep the reference's names.** A comment citing
  `AeronCluster.java:2574` or an ADR quoting `AeronCluster::offer` names the
  reference's class; prose may say "the Aeron directory" when it explains the
  concept. The brand is banned from identifiers, not from reading.

## Consequences

- deepbook and deepmsg now agree on the client's type names:
  `ClusterClient` / `ClusterClientContext` (`AeronCluster` /
  `AeronClusterContext` before the deepmsg pin moved to `654ece4`; deepmsg's
  ADR-0005 carries the same rule for mirrored class names).
- What moves is deepbook's own: the `--driver-dir` flag in `fix-gateway` and
  `deepbook-admin` (the scripts and README move with it), the
  `deepbook.marketdata.driver.dir` property, the deployment's `driver_dir`
  key and `CLUSTER_DRIVER_ROOT`. What the driver and the reference read —
  property names, channels, `-Daeron.*` — is untouched.
- The on-disk `aeron/` and `aeron-<n>/` directory names do not move, so a run
  root written by an older deployment is still readable.
- Review enforces the rule; there is no lint for it yet, because a text scan
  cannot tell an identifier from a property key.
