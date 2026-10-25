//! Market-data messages: incrementals, snapshots and recovery traffic.
//!
//! The producer assigns `md_seq` to **every** incremental, so the stream is
//! contiguous per symbol. A subscriber that sees a hole asks the recovery
//! channel for either the missing incrementals or a snapshot.

use deepbook_types::{Price, Qty, Seq, Side, SymbolId};

/// What happened to one price level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BookAction {
    /// The level had no quantity before this update.
    Add,
    /// The level already existed.
    Update,
    /// The level is now empty.
    Delete,
}

/// One incremental market-data message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Incremental {
    /// A price level changed. `qty` is the level's total after the change.
    BookUpdate {
        /// Per-symbol sequence, strictly `last + 1`.
        md_seq: Seq,
        /// Symbol.
        symbol: SymbolId,
        /// Side of the level.
        side: Side,
        /// Price.
        price: Price,
        /// Total quantity at the level after the change (0 removes it).
        qty: Qty,
        /// Add, update or delete.
        action: BookAction,
    },
    /// A trade happened. The trade does not change the book by itself; the
    /// book update that follows carries the new level total.
    Trade {
        /// Per-symbol sequence, strictly `last + 1`.
        md_seq: Seq,
        /// Symbol.
        symbol: SymbolId,
        /// Trade price.
        price: Price,
        /// Trade quantity.
        qty: Qty,
        /// The aggressor's side.
        aggressor: Side,
    },
}

impl Incremental {
    /// The message's sequence.
    #[must_use]
    pub const fn md_seq(self) -> Seq {
        match self {
            Self::BookUpdate { md_seq, .. } | Self::Trade { md_seq, .. } => md_seq,
        }
    }

    /// The message's symbol.
    #[must_use]
    pub const fn symbol(self) -> SymbolId {
        match self {
            Self::BookUpdate { symbol, .. } | Self::Trade { symbol, .. } => symbol,
        }
    }

    /// Whether this is a book update.
    #[must_use]
    pub const fn is_book_update(self) -> bool {
        matches!(self, Self::BookUpdate { .. })
    }
}

/// One price level in a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Level {
    /// Price.
    pub price: Price,
    /// Total quantity at the price.
    pub qty: Qty,
}

/// A full book snapshot at `md_seq`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The last incremental included in the book.
    pub md_seq: Seq,
    /// Symbol.
    pub symbol: SymbolId,
    /// Bids, best first.
    pub bids: Vec<Level>,
    /// Asks, best first.
    pub asks: Vec<Level>,
}

/// A subscriber's request to the recovery channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryRequest {
    /// Symbol.
    pub symbol: SymbolId,
    /// The last `md_seq` the subscriber applied. `0` asks for a snapshot.
    pub last_md_seq: Seq,
}

/// The service's answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryResponse {
    /// A full snapshot at its `md_seq`.
    Snapshot(Snapshot),
    /// The incrementals after the subscriber's last sequence.
    Incrementals {
        /// Symbol.
        symbol: SymbolId,
        /// The first sequence in the list (the subscriber's last + 1).
        from_md_seq: Seq,
        /// Contiguous incrementals through the service's last.
        incrementals: Vec<Incremental>,
    },
    /// The subscriber is already current.
    UpToDate {
        /// Symbol.
        symbol: SymbolId,
        /// The service's last sequence.
        md_seq: Seq,
    },
    /// The symbol is not served here.
    UnknownSymbol {
        /// The requested symbol.
        symbol: SymbolId,
    },
}
