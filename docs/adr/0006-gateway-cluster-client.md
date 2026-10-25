# ADR 0006: the gateway's cluster client

- Status: **Accepted** — 2026-10-25, implemented by M4c
- Date: 2026-10-25
- Deciders: project owner

## Context

ADR 0001 adopted the deepmsg cluster and ADR 0002 fixed the gateway seam:
the FIX session calls `CommandSink::deliver`, and the engine answers through
`EventSource::poll_event` with a FIX message addressed to a `SessionKey`.
M4a/M4b supplied the deterministic engine snapshot and the `ClusteredService`
adapter (ADR 0005). M4c has to make the seam real: one process holds a
deepmsg cluster session, maps FIX messages to `engine-core` commands, maps
cluster egress back to FIX reports, and recovers after the engine host is
killed.

The deepmsg client's shape constrains the design. It is caller-driven
(`Client::poll`, `AeronCluster::poll_egress`), it is not `Send`, and its
egress listener is a boxed callback on the context. The acceptor is one
thread, which matches.

## Decision

1. **One `ClusterEngine` type owns the client, the session and the mapping.**
   It implements `CommandSink` and `EventSource`; the acceptor and its seam do
   not change. Commands are `engine-core`'s versioned wire records. Events are
   decoded and turned into `ExecutionReport` / `OrderCancelReject`; a trade's
   two execution reports are the FIX fills, the `Trade` and `BookUpdate`
   events are market data, and a `Snapshot` marker is cluster state.
   `ExecID` is `(log_pos, event_seq)`.
2. **One FIX session = one cluster session.** M1's acceptor hosts one
   `SenderCompID`/`TargetCompID` pair, so every egress event belongs to that
   pair and needs no FIX-session field in the engine record. Hosting several
   peers needs a per-peer cluster session and a route; that is a later slice.
3. **Accounts are numeric.** FIX `1 Account` is a string and the engine's
   account is a `u32`. In cluster mode the string must be the decimal id and
   a missing account is `0`. A non-numeric account is refused, not hashed: a
   hash would not have a stable meaning across a gateway restart. The symbol
   name is configured (`--symbol`) and sent as the configured id
   (`--symbol-id`); an unknown `55` is refused locally.
4. **The gateway keeps a last-report projection.** `OrderStatusRequest` has
   no engine command, so it is answered from the last `ExecutionReport` the
   gateway saw. The projection also supplies the account of a cancel or
   replace that does not name one. An order the projection does not know is
   refused locally with `OrderCancelReject` (`102=1`). The projection is per
   gateway process; a client that sends `1` on the cancel does not depend on
   it.
5. **A command that cannot be offered is queued, not dropped.** Back
   pressure, an end of log, a mid-rotation claim, `NotConnected` and a
   released ingress publication are transient: the command stays at the front
   of the queue and is retried in order. Only the terminal append answers
   (`MaxPositionExceeded`, `MessageTooLarge`, `Malformed`) drop a command.
   A closed cluster session is reconnected with a fresh connect;
   `--new-leader-timeout-ms` bounds how long a session waits for a new leader
   before it closes and connects again.
6. **A mapping failure is a FIX answer.** A malformed client order id, a bad
   account, an unknown symbol, a cancel of an order this gateway never sent
   and a full command queue are reported as `ExecutionReport(ExecType=8)` or
   `OrderCancelReject`, with a gateway `ExecID` (`GW-n`). They never tear the
   FIX connection down.
7. **The process e2e recovers the engine's host, not only the container.**
   The test starts the media driver, one consensus module, `engine-container`
   and `fix-gateway --cluster`, sends a crossing order and a cancel, asks
   `deepmsg-cluster-tool` for a snapshot, `SIGKILL`s the container **and** the
   module, restarts both over the same archive and cluster directories, then
   compares the engine's `Snapshot` event `book_hash` before and after and
   sends the rest of the crossing order through the same FIX session.
   A container-only restart is not possible in the pinned deepmsg revision:
   `ClusteredServiceAgent::recover_state` reads a recovery-state counter that
   the module allocates once, at module startup, and removes once the
   services have acked. The test is `#[ignore]` because it needs the deepmsg
   binaries and real child processes; it skips rather than fails when they
   are absent.

## Consequences

- The e2e proves log replay + service snapshot recovery, not just the
  in-memory `Engine::restore` of M4a.
- The FIX gateway now has two engines behind one seam: `StubEngine` (M1) and
  `ClusterEngine` (M4), selected by `--cluster`. The command line and session
  directories are shared.
- The egress path is one FIX peer per gateway. Multi-peer routing, a
  persistent account directory and a status query that survives a gateway
  restart are M5/M6 work.
- A reconnect drives the client on the acceptor thread, so the FIX loop is
  not polling while `AeronCluster::connect` runs (bounded by the message
  timeout). Moving the connect off the acceptor thread changes the seam and
  is deliberately left out of M4.
- The pinned deepmsg client can lose the `NewLeaderEvent` (deepmsg's own
  README records it for a restarted member 0). A short
  `--new-leader-timeout-ms` makes the gateway close and reconnect instead of
  waiting for the full derived timeout; the FIX session is unaffected.
