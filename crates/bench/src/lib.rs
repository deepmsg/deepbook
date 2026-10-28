//! Open-loop FIX client and latency harness (M5).
//!
//! The client sends at a fixed rate so the measurement does not suffer from
//! coordinated omission. t0 is taken when a NewOrderSingle is queued for the
//! socket; t1 is when the first report for its `ClOrdID` has been decoded.
//! Queueing time is part of the number: a client that cannot write at the
//! requested rate sees it, rather than the sender silently slowing down.
//!
//! Orders are `TimeInForce=IOC` buys at a price the book does not cross, so a
//! long run does not fill the order slab: each order is acknowledged and then
//! canceled by the engine. Only the first report (the acknowledgement) is an
//! RTT sample.
#![forbid(unsafe_code)]

pub mod client;
pub mod report;

pub use client::{BenchConfig, ClientError, run};
pub use report::Report;
