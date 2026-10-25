# ADR 0009: event-loop abstraction and the io_uring backend

- Status: **Accepted** — 2026-10-25, implemented by M5c
- Date: 2026-10-25
- Deciders: project owner

## Context

M1's acceptor used `mio`/epoll directly. The M5 gate asks for an io_uring
backend on the same bench machine and an epoll-versus-uring comparison. The
gateway crate is `#![forbid(unsafe_code)]`, and the two I/O models differ:
epoll reports readiness, io_uring reports completion.

## Decision

1. **One completion-shaped trait.** `deepbook-io` defines `EventLoop` with
   `start_accept`, `start_read`, `submit_write`, `read_data`, `close` and
   `wait`, and an `Event` enum (`Accepted`, `AcceptFailed`, `Read`,
   `Written`). The gateway consumes completions only; it never calls `read` or
   `write` on a socket.
2. **Readiness is adapted, not exposed.** The epoll backend registers the
   listener and one read interest per connection. `wait` performs the
   non-blocking read/write when readiness arrives and reports the result as
   the same `Read`/`Written` completion. The io_uring backend submits real
   `Accept`, `Read`, `Write` and `Timeout` operations. The FIX session,
   journal and engine code are identical for both.
3. **The backend owns the sockets and buffers; the gateway owns the session.**
   A connection has one outstanding read (64 KiB buffer) and at most one
   outstanding write (the front of its outbound queue). A write is copied into
   the backend; a partial completion drains the front and the gateway submits
   the remainder. Reads are re-armed after the accepted bytes are processed.
4. **Timers survive an idle cluster.** epoll waits with the gateway's tick as
   the `poll` timeout. io_uring submits a `Timeout` completion with the same
   timeout; the previous timeout is removed in the same submission. This is
   what lets heartbeats and cluster egress polling run when no FIX traffic
   arrives.
5. **The accept is one-shot.** After an `Accepted` (or `AcceptFailed`) the
   gateway re-arms it. The epoll backend treats arming as a flag; the uring
   backend submits another `Accept`. One CompID pair means low accept rates.
6. **Selection is explicit.** `GatewayConfig.backend` (default `Epoll`) and
   `fix-gateway --backend epoll|uring` choose the backend. The default stays
   epoll so deployments that do not ask for io_uring keep the M1 path.
7. **The unsafe boundary is one small crate.** `deepbook-io` is the only crate
   with `unsafe`: `OwnedFd::from_raw_fd` for a descriptor the kernel just
   accepted, a zeroed `sockaddr_storage`, and the io_uring `SubmissionQueue::push`
   call. Each block is marked with its invariant. The gateway remains
   `#![forbid(unsafe_code)]`.
8. **The comparison is ABBA with one gateway per arm.** `deploy/bench.sh
   --compare` runs `epoll, uring, uring, epoll`. Each arm gets its own gateway
   process, FIX port, session directory and account (so repeated `ClOrdID`s do
   not hit the engine's idempotency window), and the book is seeded once.
   A gateway is stopped after its arm and the next arm waits out the consensus
   module's session timeout: a dead session's egress competes with the next
   client until it expires, which distorted an earlier concurrent-gateway
   version of the comparison.

## Consequences

- The io_uring backend uses plain `accept`/`read`/`write` operations. The
  design's multishot accept/recv, registered buffers and SQPOLL are a later
  tuning step; the trait does not change when they are added.
- The epoll backend is the control arm, not the old code path: its readiness
  is converted to the same completion API, so the FIX logic is measured once.
- Each connection costs a 64 KiB read buffer in both backends.
- The ABBA run takes about a minute because of the session-timeout wait
  between arms; that is a correctness cost of measuring a restarted client
  against one consensus module.

## Measured (WSL2, single node, 5k orders/s offered, 20k samples)

| backend | p50 | p90 | p99 | p99.9 | completed |
|---|---|---|---|---|---|
| epoll | 390 µs | 478 µs | 662 µs | 1.96 ms | 20 000 / 20 000 |
| io_uring | 173 µs | 257 µs | 383 µs | 1.85 ms | 20 000 / 20 000 |

Both arms sustained 5 000 orders/s with the same journal policy
(`batch 64`). The uring p50 is about 56% lower. These are test-machine
numbers; `deploy/bench.sh --compare` on the bare-metal bench machine is the
acceptance run.
