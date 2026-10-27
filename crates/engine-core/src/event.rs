//! What the engine emits.
//!
//! An [`ExecutionReportEvent`] is the order-state event the gateway turns into
//! FIX `ExecutionReport`. [`TradeEvent`] and [`BookUpdateEvent`] are the
//! market-data stream M3 consumes. Every event carries `(log_pos, event_seq)`;
//! the pair is the FIX `ExecID` and the market-data dedupe key.

use deepbook_types::{AccountId, LogPos, OrderId, OrderState, Price, Qty, Seq, Side, SymbolId};

use crate::command::RejectReason;
use crate::ids::ClientOrderId;

/// The per-command event counter. The design says `u16`; `u32` is used so a
/// market order sweeping more than 65,535 resting orders cannot overflow the
/// key.
pub type EventSeq = u32;

/// The unique id of one event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventHeader {
    /// The command's cluster log position.
    pub log_pos: LogPos,
    /// The event's index within the command.
    pub event_seq: EventSeq,
}

/// `ExecType` for an order-state event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecType {
    /// `0`.
    New,
    /// `F`.
    Trade,
    /// `4`.
    Canceled,
    /// `5`.
    Replaced,
    /// `8`.
    Rejected,
}

/// One order-state change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReportEvent {
    /// Event id.
    pub header: EventHeader,
    /// The packed engine order id.
    pub order_id: OrderId,
    /// The order's client order id.
    pub cl_ord_id: ClientOrderId,
    /// Account.
    pub account: AccountId,
    /// Symbol.
    pub symbol: SymbolId,
    /// Side.
    pub side: Side,
    /// What changed.
    pub exec_type: ExecType,
    /// The order's state after the change.
    pub state: OrderState,
    /// Limit price, if any.
    pub price: Option<Price>,
    /// Original order quantity.
    pub order_qty: Qty,
    /// Quantity filled by this event (0 for a non-trade event).
    pub last_qty: Qty,
    /// Price of this fill.
    pub last_px: Option<Price>,
    /// Remaining quantity.
    pub leaves_qty: Qty,
    /// Cumulative filled quantity.
    pub cum_qty: Qty,
    /// Average fill price in ticks, rounded down.
    pub avg_px: Price,
    /// Set on a rejection.
    pub reason: Option<RejectReason>,
}

/// A cancel or replace the engine refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelRejectEvent {
    /// Event id.
    pub header: EventHeader,
    /// The order id, when known.
    pub order_id: Option<OrderId>,
    /// The cancel/replace request's client order id.
    pub cl_ord_id: ClientOrderId,
    /// The order it named.
    pub orig_cl_ord_id: ClientOrderId,
    /// Account.
    pub account: AccountId,
    /// The order's current state, when known.
    pub state: OrderState,
    /// Why it was refused.
    pub reason: RejectReason,
    /// Whether the refused command was a replace (`true`) or a cancel.
    pub is_replace: bool,
}

/// One fill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeEvent {
    /// Event id.
    pub header: EventHeader,
    /// The resting order.
    pub maker_order_id: OrderId,
    /// The incoming order.
    pub taker_order_id: OrderId,
    /// The resting order's client id.
    pub maker_cl_ord_id: ClientOrderId,
    /// The incoming order's client id.
    pub taker_cl_ord_id: ClientOrderId,
    /// The resting account.
    pub maker_account: AccountId,
    /// The incoming account.
    pub taker_account: AccountId,
    /// Trade price.
    pub price: Price,
    /// Trade quantity.
    pub qty: Qty,
    /// The taker's side.
    pub aggressor_side: Side,
}

/// A change to one price level.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookUpdateEvent {
    /// Event id.
    pub header: EventHeader,
    /// Symbol.
    pub symbol: SymbolId,
    /// Side of the level.
    pub side: Side,
    /// Price of the level.
    pub price: Price,
    /// Total resting quantity at the level after the change (0 when empty).
    pub qty: Qty,
    /// Market-data sequence, incremented per update.
    pub md_seq: Seq,
}

/// A snapshot marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotEvent {
    /// Event id.
    pub header: EventHeader,
    /// Symbol.
    pub symbol: SymbolId,
    /// Market-data sequence at the snapshot.
    pub md_seq: Seq,
    /// The deterministic book hash.
    pub book_hash: u64,
    /// The last command's log position.
    pub last_log_pos: LogPos,
}

/// One engine output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// An order-state change.
    ExecutionReport(ExecutionReportEvent),
    /// A refused cancel/replace.
    CancelReject(CancelRejectEvent),
    /// A fill.
    Trade(TradeEvent),
    /// A price-level change.
    BookUpdate(BookUpdateEvent),
    /// A snapshot marker.
    Snapshot(SnapshotEvent),
}

impl Event {
    /// The event's id.
    #[must_use]
    pub const fn header(&self) -> EventHeader {
        match self {
            Self::ExecutionReport(event) => event.header,
            Self::CancelReject(event) => event.header,
            Self::Trade(event) => event.header,
            Self::BookUpdate(event) => event.header,
            Self::Snapshot(event) => event.header,
        }
    }
}

/// Where the engine writes events.
pub trait EventSink {
    /// Take one event.
    fn emit(&mut self, event: &Event);
}

impl EventSink for Vec<Event> {
    fn emit(&mut self, event: &Event) {
        self.push(event.clone());
    }
}
