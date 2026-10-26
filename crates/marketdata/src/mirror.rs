//! A mirrored price-level book.
//!
//! Both the publishing service and the subscriber use this: the service keeps
//! the authoritative mirror from which it builds snapshots, and the subscriber
//! applies the same incrementals after a snapshot. It is deliberately
//! level-only: trades do not change it.

use std::collections::BTreeMap;

use deepbook_types::{Price, Qty, Seq, Side, SymbolId};

use crate::message::{Incremental, Level, Snapshot};

/// A price-level book for one symbol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MirrorBook {
    symbol: SymbolId,
    bids: BTreeMap<Price, Qty>,
    asks: BTreeMap<Price, Qty>,
}

impl MirrorBook {
    /// An empty book for `symbol`.
    #[must_use]
    pub fn new(symbol: SymbolId) -> Self {
        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    /// The symbol.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// Replace the book with a snapshot. `false` when the symbol differs.
    pub fn install(&mut self, snapshot: &Snapshot) -> bool {
        if snapshot.symbol != self.symbol {
            return false;
        }
        self.bids.clear();
        self.asks.clear();
        for level in &snapshot.bids {
            self.insert(Side::Buy, level.price, level.qty);
        }
        for level in &snapshot.asks {
            self.insert(Side::Sell, level.price, level.qty);
        }
        true
    }

    /// Apply one incremental. Trades leave the levels alone.
    pub fn apply(&mut self, incremental: &Incremental) -> bool {
        match incremental {
            Incremental::BookUpdate {
                symbol,
                side,
                price,
                qty,
                ..
            } => {
                if *symbol != self.symbol {
                    return false;
                }
                self.insert(*side, *price, *qty);
                true
            }
            Incremental::Trade { symbol, .. } => *symbol == self.symbol,
        }
    }

    /// Set one level; a non-positive quantity removes it.
    pub fn insert(&mut self, side: Side, price: Price, qty: Qty) {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if qty > 0 {
            levels.insert(price, qty);
        } else {
            levels.remove(&price);
        }
    }

    /// The best bid.
    #[must_use]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.last_key_value().map(|(price, _)| *price)
    }

    /// The best ask.
    #[must_use]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first_key_value().map(|(price, _)| *price)
    }

    /// The quantity at a level.
    #[must_use]
    pub fn qty_at(&self, side: Side, price: Price) -> Qty {
        match side {
            Side::Buy => self.bids.get(&price).copied(),
            Side::Sell => self.asks.get(&price).copied(),
        }
        .unwrap_or(0)
    }

    /// Whether both sides are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Number of bid levels.
    #[must_use]
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    /// Number of ask levels.
    #[must_use]
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }

    /// The book as a snapshot at `md_seq`; bids best first, asks best first.
    #[must_use]
    pub fn snapshot(&self, md_seq: Seq) -> Snapshot {
        Snapshot {
            md_seq,
            symbol: self.symbol,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(price, qty)| Level {
                    price: *price,
                    qty: *qty,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(price, qty)| Level {
                    price: *price,
                    qty: *qty,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_are_removed_when_their_quantity_reaches_zero() {
        let mut book = MirrorBook::new(1);
        book.insert(Side::Sell, 100, 10);
        assert_eq!(10, book.qty_at(Side::Sell, 100));
        book.insert(Side::Sell, 100, 0);
        assert_eq!(0, book.qty_at(Side::Sell, 100));
        assert!(book.is_empty());
    }

    #[test]
    fn a_snapshot_is_sorted_best_first() {
        let mut book = MirrorBook::new(1);
        book.insert(Side::Buy, 99, 1);
        book.insert(Side::Buy, 100, 2);
        book.insert(Side::Sell, 101, 3);
        book.insert(Side::Sell, 102, 4);
        let snapshot = book.snapshot(7);
        assert_eq!(7, snapshot.md_seq);
        assert_eq!(100, snapshot.bids[0].price);
        assert_eq!(99, snapshot.bids[1].price);
        assert_eq!(101, snapshot.asks[0].price);
        assert_eq!(102, snapshot.asks[1].price);
        let mut restored = MirrorBook::new(1);
        assert!(restored.install(&snapshot));
        assert_eq!(book, restored);
    }

    #[test]
    fn a_trade_does_not_change_the_levels() {
        let mut book = MirrorBook::new(1);
        let trade = Incremental::Trade {
            md_seq: 1,
            symbol: 1,
            price: 100,
            qty: 5,
            aggressor: Side::Buy,
        };
        assert!(book.apply(&trade));
        assert!(book.is_empty());
    }
}
