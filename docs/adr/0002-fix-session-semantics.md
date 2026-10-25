# ADR 0002: FIX 4.4 session semantics

- Status: **Accepted** — 2026-10-25, implemented by M1a
- Date: 2026-10-25
- Deciders: project owner

## Context

The design (v0.3, §6.1) lists the FIX 4.4 message and field subset but leaves
the session state machine implicit. The design review flagged five points that
a FIX acceptor must decide once, or every later change becomes an
interpretation: `ResendRequest(16=0)`, `141=Y`, a Logon whose sequence does not
match, inbound `43=Y` duplicates, and `BodyLength`/`CheckSum` validation.
`crates/fix` is the single implementation of these rules; QuickFIX/J is the
interoperability oracle (M1d).

## Decision

1. **Framing is validated before fields are read.** `8` must be `FIX.4.4` and
   first, `9` counts from the start of `35` to the SOH before `10`, `10` is the
   byte sum modulo 256 as three digits. A failure ends the connection; it is
   never repaired.
2. **Sequences are independent.** Inbound `seq > expected` sends one
   `ResendRequest(expected, 0)` and queues the message until the gap closes.
   Inbound `seq < expected` with `43=Y` is a duplicate and is ignored; without
   `43=Y` it is a protocol error, answered with `Logout` and a disconnect.
3. **`141=Y` resets both sequences to 1 and clears the outbound journal.** A
   reset Logon is accepted mid-session; its `34` must be 1.
4. **A Logon with `34 > expected` is accepted** and answered with a
   `ResendRequest`; a lower one without `43=Y` is refused. A second Logon
   without `141=Y` ends the session.
5. **The journal is written before the socket.** Every outbound message is
   appended and `fdatasync`ed before it is returned to the caller; the window
   is the design's 4096 entries. Inbound and outbound sequences are persisted
   beside it and restored at Logon. A recovery uses the larger of the journal's
   next sequence and the stored one.
6. **Resend.** `16=0` means to the end of the stream. An application message is
   resent under its original `34` with `43=Y` and `122=OrigSendingTime`; an
   admin run becomes one `SequenceReset-GapFill`. **Application messages are
   never gap-filled.** A request that starts before the window sends `Logout`
   and disconnects rather than silently losing a command.
7. **Heartbeats.** The peer's `108` sets the interval for the session. Silence
   on the inbound side sends `TestRequest`; an unanswered request sends
   `Logout` and disconnects. Silence on the outbound side sends `Heartbeat`.
8. **Prices are integer hundredths.** `"100.25"` is `10_025`; more than two
   decimal places is refused rather than rounded. No floating point enters the
   fix crate.
9. **The engine seam is `CommandSink`.** The session emits
   `Action::Deliver(ApplicationMessage)`; the gateway's sink owns the engine
   call. M1c supplies an in-process sink, M4 the cluster client. The session
   never calls the engine and the engine never sees a FIX byte.

## Consequences

- M1d's QuickFIX/J conformance suite is the acceptance test for these rules:
  logon, order and report, an in-window resend with `43`/`122`, an over-window
  disconnect, and no application GapFill.
- The 4096-entry window is a hard limit: a peer that falls further behind is
  logged out, not served from disk.
- One `fdatasync` per journal append and one atomic state write per message are
  the M1 correctness policy. Batching them is an M5 measurement; the session
  API does not change when that happens.
- A `CommandSink` failure is not a FIX session failure: M1c decides whether a
  refused command becomes a `Reject` or an `ExecutionReport` with
  `OrdStatus=Rejected`.
