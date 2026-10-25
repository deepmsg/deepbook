# FIX conformance (M1d)

`run.sh` starts the real `fix-gateway` process and drives it from outside:

1. **QuickFIX/J initiator** — logon, one NewOrderSingle, the
   ExecutionReport, logout (`QuickFixInitiator.java`).
2. **Resend** — a byte-level probe requests sequences 1–4. The admin Logon
   reply is answered with a `SequenceReset-GapFill`; the three application
   reports are replayed with `43=Y` and `122=OrigSendingTime`, never with a
   gap fill (`RawProbe.java resend`).
3. **Over-window** — the gateway runs with a one-entry journal; a resend that
   starts before the window is answered with `Logout` naming the window
   (`RawProbe.java over-window`).

The two replay scenarios use a raw socket because QuickFIX/J's session layer
would consume or suppress the resent messages before an application could
assert their header fields. QuickFIX/J is still the interoperability oracle
for the normal logon/order/report path.

## Run

```bash
./tests/fix_conformance/run.sh
```

Requirements: JDK 21+, `curl`, and a built Rust toolchain. The script fetches
four jars into `target/quickfixj/` on first use (QuickFIX/J 2.3.1 plus
`mina-core`, `slf4j-api` and `slf4j-simple`); nothing under `target/` is
committed. It builds `target/debug/fix-gateway`, starts it on an ephemeral
port for each scenario, and uses a fresh session directory each time.

The QuickFIX/J screen log prints the wire messages; the `PASS` lines are the
result. A non-zero exit is a failed scenario.

## Why a separate runner

`cargo test` cannot start a JDK, and the harness needs the gateway binary
rather than the in-process pieces. The CI workflow runs this script as a
separate job (`fix-conformance`) so the FIX path is not only covered by Rust
unit tests.
