//! `ClusteredService` adapter and snapshot serialization (M4).
//!
//! The adapter is the only crate that knows about deepmsg's service
//! container. It translates log messages into `engine-core` commands, offers
//! execution reports to the gateway's cluster session, and writes/reads the
//! engine snapshot. The gateway's side of the same session is
//! `deepbook-gateway`'s `ClusterEngine` (ADR 0006).
#![forbid(unsafe_code)]

pub mod envelope;
pub mod metrics;
pub mod obs;
pub mod outbox;
pub mod service;
pub mod snapshot;

pub use metrics::EngineMetrics;
pub use outbox::{OutboxEntry, SeatOutbox, SeatSnapshot};
pub use service::{EngineService, MarketDataConfig, ServiceError};
pub use snapshot::{CodecError, ServiceSnapshot, decode, decode_service, encode, encode_service};
