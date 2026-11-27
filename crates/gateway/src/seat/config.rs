//! The shared seat registry (`seats.toml`, ADR 0012 §6.1).
//!
//! The parser lives in `deepbook-engine-cluster` so the service and the
//! gateways load exactly the same file; this module keeps the historical
//! `crate::seat::config::*` path.

pub use deepbook_engine_cluster::seats::{SeatConfig, SeatConfigError, SeatProtocol, SeatRegistry};
