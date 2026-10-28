//! Market data: incremental updates, periodic snapshots and gap recovery
//! (M3).
//!
//! The producer assigns `md_seq` to every incremental; this crate mirrors the
//! book, publishes incrementals (multicast/IPC), and serves snapshots on a
//! request/response channel. A subscriber that sees `md_seq != last + 1` asks
//! for a snapshot or the missing incrementals. Conflation lives in the
//! admin → UI WebSocket layer, not here.
//!
//! The pure core is [`MarketDataService`] (producer) and [`Subscriber`]
//! (consumer); [`wire`] is the small internal frame. The deepmsg transport
//! adapter and the driver loss-generator test are the transport slice on top
//! of them.
#![forbid(unsafe_code)]

pub mod deepmsg;
pub mod message;
pub mod mirror;
pub mod service;
pub mod subscriber;
pub mod wire;

pub use deepmsg::{
    DeepmsgConfig, DeepmsgError, DeepmsgIncrementalPublisher, DeepmsgPublisher,
    DeepmsgRecoveryResponder, DeepmsgSubscriber,
};
pub use message::{BookAction, Incremental, Level, RecoveryRequest, RecoveryResponse, Snapshot};
pub use mirror::MirrorBook;
pub use service::{MarketDataError, MarketDataService, ServiceConfig};
pub use subscriber::{ApplyOutcome, Subscriber};
pub use wire::{WireError, WireMessage, decode, encode};
