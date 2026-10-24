//! FIX acceptor, session registry and cluster client (M1/M4).
//!
//! One event-loop thread owns every FIX connection. Commands are offered to
//! the cluster; execution reports come back on egress and are routed to the
//! FIX session by `(fix_session, correlation)`. The cluster client is added at
//! M4; M1–M3 talk to the `CommandLog` seam instead.
#![forbid(unsafe_code)]
