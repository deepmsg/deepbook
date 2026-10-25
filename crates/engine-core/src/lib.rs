//! Deterministic order book, matching and pre-trade risk (M2).
//!
//! This crate depends only on `deepbook-types`. It does not know about
//! deepmsg, sockets, threads, clocks, or serialization. Every state transition
//! is a function of the current state and the command; replay of the same
//! command stream must produce the same state.
//!
//! The public surface is:
//!
//! - [`Engine::on_command`] — the single entry point, with the log position as
//!   the only ordering input and an [`EventSink`] for output;
//! - [`Command`] / [`NewOrder`] / [`CancelOrder`] / [`ReplaceOrder`] — the
//!   input values M4's cluster adapter will decode;
//! - [`Event`] — order-state reports, trades, book updates and snapshots;
//! - [`Engine::view`] / [`Engine::book_hash`] — deterministic state for the
//!   differential oracle and replay checks.
//!
//! M2 implements limit and market orders, DAY/GTC/IOC/FOK, cancel and replace,
//! post-only, self-trade prevention, the common pre-trade limits, and a
//! bounded duplicate-ClOrdID window. Snapshot serialization and cluster wiring
//! are M4; market-data fan-out is M3.
#![forbid(unsafe_code)]

pub mod book;
pub mod command;
pub mod engine;
pub mod event;
pub mod ids;
pub mod risk;

pub use book::{BookView, LevelView, OrderView};
pub use command::{
    CancelOrder, Command, NewOrder, OrdType, RejectReason, ReplaceOrder, TimeInForce,
};
pub use engine::{Engine, EngineConfig};
pub use event::{
    BookUpdateEvent, CancelRejectEvent, Event, EventHeader, EventSeq, EventSink, ExecType,
    ExecutionReportEvent, SnapshotEvent, TradeEvent,
};
pub use ids::{ClientOrderId, IdError, Seen, SeenState};
pub use risk::{RiskConfig, SelfTradePrevention};
