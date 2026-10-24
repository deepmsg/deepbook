//! Deterministic order book, matching and pre-trade risk (M2).
//!
//! This crate depends only on `deepbook-types`. It does not know about
//! deepmsg, sockets, threads, clocks, or serialization. Every state transition
//! is a function of the current state and the command; replay of the same
//! command stream must produce the same state.
#![forbid(unsafe_code)]
