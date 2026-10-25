//! `ClusteredService` adapter and snapshot serialization (M4).
//!
//! The adapter is the only crate that knows about deepmsg's service
//! container. It translates log messages into `engine-core` commands, offers
//! execution reports to the gateway's cluster session, and writes/reads the
//! engine snapshot. The gateway's side of the same session is
//! `deepbook-gateway`'s `ClusterEngine` (ADR 0006).
#![forbid(unsafe_code)]

pub mod service;
pub mod snapshot;

pub use service::{EngineService, ServiceError};
pub use snapshot::{CodecError, decode, encode};
