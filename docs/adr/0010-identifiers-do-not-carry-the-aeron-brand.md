# ADR 0010: identifiers do not carry the Aeron brand

- Status: **Accepted** — 2026-11-22
- Date: 2026-11-22
- Deciders: project owner

## Context

deepbook runs on deepmsg, which is a from-scratch implementation of the Aeron
stack, so a good part of what deepbook configures is named by Aeron: property
keys (`aeron.dir`, `aeron.cluster.*`), channel URIs (`aeron:ipc`,
`aeron:udp?…`), environment variables (`AERON_*`) and one CLI flag
(`--aeron-dir`). Those names are the driver's and the protocol's — they have to
match the reference byte for byte and deepbook cannot rename them.

That is not the same thing as carrying the brand into deepbook's own code.
The directory a media driver publishes its CnC file into was named `aeron`,
`aeron_dir` and `client_aeron_dir` in Rust and shell alike; a reader meeting
`dirs.aeron` has no way to tell whether it is deepbook's concept or the
dependency's name, and the brand is also the one thing that survives a future
decision to change the media layer underneath.

## Decision

- **Rust and shell identifiers — types, fields, functions, parameters, locals,
  modules — do not contain `aeron` in any case.** The path that holds a
  driver's CnC file and its state is `driver_dir` (`client_driver_dir` for the
  client's own driver), including the `DirSet`/`Harness`/`Driver` fields and
  the `Paths` helper. `driver` would collide with the child process in the
  gateway's test harness, so `driver_dir` is the one spelling.
- **Configuration surfaces keep the name.** Property keys (`aeron.dir`,
  `aeron.cluster.*`, `deepbook.marketdata.aeron.dir`), URI schemes
  (`aeron:ipc`, `aeron:udp?…`), environment variables (`AERON_*`), the
  `--aeron-dir` flag, the on-disk directory names (`aeron/`, `aeron-<n>/`) and
  the `member.properties` key `aeron_dir` are what the driver reads. Renaming
  any of them changes a wire or deployment contract, not a variable.
- **A dependency's field is adapted once.** deepmsg's
  `ServiceContainerConfig::aeron_dir` is its name; the one use copies it into a
  local `driver_dir` and says why.
- **Citations keep the reference's names.** A comment citing
  `AeronCluster.java:2574` or an ADR quoting `AeronCluster::offer` names the
  reference's class; prose may say "the Aeron directory" when it explains the
  concept. The brand is banned from identifiers, not from reading.

## Consequences

- deepbook and deepmsg now agree on the client's type names:
  `ClusterClient` / `ClusterClientContext` (`AeronCluster` /
  `AeronClusterContext` before the deepmsg pin moved to `654ece4`; deepmsg's
  ADR-0005 carries the same rule for mirrored class names).
- Nothing on the wire, on disk or on a command line moves. A deployment's
  properties, channels and flags are untouched.
- Review enforces the rule; there is no lint for it yet, because a text scan
  cannot tell an identifier from a property key.
