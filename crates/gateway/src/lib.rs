//! FIX acceptor, session registry and cluster client (M1/M4).
//!
//! One event-loop thread owns every FIX connection. Commands are offered to
//! the engine through the [`CommandSink`] seam; execution reports come back on
//! `EventSource` and are routed to the FIX session by `(fix_session,
//! correlation)`. M1c supplies an in-process sink; M4 swaps in the cluster
//! client. ADR 0001 adopts the cluster, so the seam is designed for that
//! client from the start.
//!
//! M1b is the acceptor: [`Gateway`] owns a `mio` poller (epoll on Linux), one
//! [`FrameDecoder`] per connection, and one [`Session`](deepbook_fix::Session)
//! per CompID pair. A session's journal directory is keyed by the pair, so a
//! reconnect resumes the sequence numbers and the resend window.
//!
//! M1c is [`StubEngine`]: the gateway hands an accepted command to
//! [`CommandSink`] and routes the reports the engine publishes through
//! [`EventSource`] back to the same session. M2 replaces the stub with
//! `engine-core`; M4's [`ClusterEngine`] carries the same seam over the
//! deepmsg cluster client.
#![forbid(unsafe_code)]

pub mod acceptor;
pub mod cluster;
pub mod sink;
pub mod stub;

pub use acceptor::{Gateway, GatewayConfig};
pub use cluster::{ClusterEngine, ClusterEngineConfig, ClusterEngineError};
pub use deepbook_fix::frame;
pub use deepbook_fix::frame::{FrameDecoder, FrameError};
pub use sink::{CommandSink, Engine, EventSource, SessionEvent, SessionKey};
pub use stub::StubEngine;
