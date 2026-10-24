//! Market data: incremental updates, periodic snapshots and gap recovery
//! (M3).
//!
//! The engine assigns `md_seq`; this crate mirrors the book, publishes
//! incrementals (multicast/IPC), and serves snapshots on a request/response
//! channel. A subscriber that sees `md_seq != last + 1` asks for a snapshot
//! or the missing incrementals. Conflation lives in the admin → UI WebSocket
//! layer, not here.
#![forbid(unsafe_code)]
