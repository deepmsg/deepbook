//! Engine state snapshots.
//!
//! The snapshot is the state M4 serializes into the cluster's snapshot record
//! and restores in `on_start`. It preserves the **slab slots, generations and
//! free-list order** as well as the book levels and the idempotency window, so
//! a restored engine produces the same book hash and the same future order ids
//! as the uninterrupted engine.

use std::fmt;

use deepbook_types::{AccountId, LogPos, OrderId, OrderState, Price, Qty, Seq, Side, SymbolId};

use crate::ids::{ClientOrderId, Seen};

/// One used slab slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderSlotSnapshot {
    /// Slab index.
    pub slot: u32,
    /// Packed order id.
    pub id: OrderId,
    /// Limit price (0 for a market order).
    pub price: Price,
    /// Original quantity.
    pub qty: Qty,
    /// Filled quantity.
    pub filled: Qty,
    /// Filled notional.
    pub notional: i128,
    /// Account.
    pub account: AccountId,
    /// Previous slot in the level.
    pub prev: u32,
    /// Next slot in the level.
    pub next: u32,
    /// Side.
    pub side: Side,
    /// State.
    pub state: OrderState,
    /// Whether the order is a market order.
    pub is_market: bool,
    /// Client order id.
    pub cl_ord_id: ClientOrderId,
}

/// One price level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelSnapshot {
    /// Price.
    pub price: Price,
    /// Total quantity.
    pub total_qty: Qty,
    /// Head slot.
    pub head: u32,
    /// Tail slot.
    pub tail: u32,
}

/// The order-book state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookSnapshot {
    /// Used slots, sorted by slot.
    pub slots: Vec<OrderSlotSnapshot>,
    /// Generation counters for **every** slot, including free ones.
    pub generations: Vec<u8>,
    /// Free slots in free-list order.
    pub free: Vec<u32>,
    /// The free-list head.
    pub free_head: u32,
    /// Bid levels, ascending by price.
    pub bids: Vec<LevelSnapshot>,
    /// Ask levels, ascending by price.
    pub asks: Vec<LevelSnapshot>,
    /// Last trade price.
    pub last_trade: Option<Price>,
    /// Market-data sequence.
    pub md_seq: Seq,
}

/// One live idempotency-window entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeenEntry {
    /// Account.
    pub account: AccountId,
    /// Client order id.
    pub cl_ord_id: ClientOrderId,
    /// What the window remembers.
    pub seen: Seen,
}

/// The whole engine state at a command boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineSnapshot {
    /// Symbol.
    pub symbol: SymbolId,
    /// The last applied command's log position.
    pub last_log_pos: LogPos,
    /// The book.
    pub book: BookSnapshot,
    /// The idempotency window, in insertion order.
    pub seen: Vec<SeenEntry>,
    /// Per-account open-order counts, sorted by account.
    pub open_orders: Vec<(AccountId, usize)>,
    /// Whether the global kill switch is active.
    pub kill_all: bool,
    /// Accounts whose kill switch is active, sorted.
    pub killed_accounts: Vec<AccountId>,
}

/// Why a snapshot could not be restored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// The snapshot belongs to another symbol.
    WrongSymbol {
        /// The configured symbol.
        expected: SymbolId,
        /// The snapshot's symbol.
        actual: SymbolId,
    },
    /// The slab capacity differs from the configured one.
    WrongCapacity {
        /// The configured capacity.
        expected: usize,
        /// The snapshot's generation count.
        actual: usize,
    },
    /// A slot index is outside the slab.
    BadSlot(u32),
    /// A slot appears twice.
    DuplicateSlot(u32),
    /// A used or free slot is missing from the snapshot.
    MissingSlot(u32),
    /// A price level points outside the slab.
    BadLevel {
        /// The level price.
        price: Price,
    },
    /// The idempotency window does not fit the configured one.
    WindowTooSmall {
        /// Entries in the snapshot.
        entries: usize,
        /// The configured capacity.
        capacity: usize,
    },
    /// The snapshot is internally inconsistent.
    Corrupt(String),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongSymbol { expected, actual } => {
                write!(
                    formatter,
                    "snapshot is for symbol {actual}, expected {expected}"
                )
            }
            Self::WrongCapacity { expected, actual } => {
                write!(
                    formatter,
                    "snapshot has {actual} slots, expected {expected}"
                )
            }
            Self::BadSlot(slot) => write!(formatter, "slot {slot} is outside the slab"),
            Self::DuplicateSlot(slot) => write!(formatter, "slot {slot} appears twice"),
            Self::MissingSlot(slot) => write!(formatter, "slot {slot} is missing"),
            Self::BadLevel { price } => write!(formatter, "level {price} points outside the slab"),
            Self::WindowTooSmall { entries, capacity } => {
                write!(formatter, "{entries} window entries do not fit {capacity}")
            }
            Self::Corrupt(detail) => write!(formatter, "snapshot is corrupt: {detail}"),
        }
    }
}

impl std::error::Error for SnapshotError {}
