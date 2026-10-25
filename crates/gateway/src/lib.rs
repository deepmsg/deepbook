//! FIX acceptor, session registry and cluster client (M1/M4).
//!
//! One event-loop thread owns every FIX connection. Commands are offered to
//! the engine through the `CommandSink` seam; execution reports come back on
//! `EventSource` and are routed to the FIX session by `(fix_session,
//! correlation)`. M1c supplies an in-process sink; M4 swaps in the cluster
//! client. ADR 0001 adopts the cluster, so the seam is designed for that
//! client from the start.
#![forbid(unsafe_code)]
