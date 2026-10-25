//! M0 cluster spike: drive deepmsg's production binaries as processes and
//! measure the cluster, not a test harness.
//!
//! The crate is `publish = false` and is not part of the product. It exists to
//! answer the M0 decision gate: does a single-node cluster of deepmsg's
//! binaries carry an echo/basic-auction workload with acceptable
//! `offer -> egress` latency, does a service recover from its snapshot after
//! `kill -9`, and does a three-node cluster hand the client to a new leader
//! after the old one is killed.
//!
//! deepmsg is an external, pinned dependency. Nothing here reaches into its
//! test crates; a single-node cluster is three child processes plus a client,
//! and gate 4's cluster is three members with a driver each plus a client
//! driver.
#![forbid(unsafe_code)]

pub mod client;
pub mod config;
pub mod nodes;
pub mod process;
pub mod report;
pub mod scenarios;
