//! `ClusteredService` adapter and snapshot serialization (M4).
//!
//! The adapter is the only crate that knows about deepmsg's service
//! container. It translates log messages into `engine-core` commands, offers
//! execution reports to the gateway's cluster session, and writes/reads the
//! engine snapshot. The deepmsg dependency is added at M4; until then the
//! gateway uses the `CommandLog` seam.
#![forbid(unsafe_code)]

pub mod snapshot;

pub use snapshot::{CodecError, decode, encode};
