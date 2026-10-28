//! The dashboard's view of one symbol: levels, the trade tape and the last
//! market-data sequence.
//!
//! The feed thread owns the deepmsg client and updates this view; HTTP and
//! WebSocket threads read snapshots of it under one mutex. Conflation is
//! deliberate: a WebSocket client receives the whole view at its own cadence,
//! so a slow browser cannot grow a queue.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use deepbook_marketdata::{Incremental, Subscriber, WireMessage};
use deepbook_types::{Price, Qty, Seq, Side};

/// How many trades the tape keeps.
pub const TRADE_CAPACITY: usize = 100;

/// One price level, in engine ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelView {
    /// Price in ticks (hundredths).
    pub price: Price,
    /// Total quantity at the level.
    pub qty: Qty,
}

/// One trade on the tape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TradeView {
    /// Market-data sequence.
    pub md_seq: Seq,
    /// Trade price in ticks.
    pub price: Price,
    /// Trade quantity.
    pub qty: Qty,
    /// The aggressor.
    pub aggressor: Side,
}

/// The current state the UI renders.
#[derive(Clone, Debug, Default)]
pub struct View {
    /// Last applied market-data sequence.
    pub md_seq: Seq,
    /// Whether the subscriber has a base and no hole.
    pub synced: bool,
    /// Bids, best first.
    pub bids: Vec<LevelView>,
    /// Asks, best first.
    pub asks: Vec<LevelView>,
    /// Recent trades, oldest first.
    pub trades: VecDeque<TradeView>,
    /// Every trade observed on the incremental stream.
    pub trades_total: u64,
    /// When the view last changed.
    pub updated: Option<Instant>,
}

/// The view shared by the feed and every connection.
pub type SharedView = Arc<Mutex<View>>;

/// A new, empty shared view.
#[must_use]
pub fn shared_view() -> SharedView {
    Arc::new(Mutex::new(View::default()))
}

impl View {
    /// Replace the levels and sequence from the subscriber's mirrored book.
    pub fn apply_market(&mut self, subscriber: &Subscriber) {
        let md_seq = subscriber.last_md_seq().unwrap_or(0);
        let snapshot = subscriber.book().snapshot(md_seq);
        self.md_seq = md_seq;
        self.synced = subscriber.is_synced();
        self.bids = snapshot
            .bids
            .iter()
            .map(|level| LevelView {
                price: level.price,
                qty: level.qty,
            })
            .collect();
        self.asks = snapshot
            .asks
            .iter()
            .map(|level| LevelView {
                price: level.price,
                qty: level.qty,
            })
            .collect();
        self.updated = Some(Instant::now());
    }

    /// Observe a transport message; only trades reach the tape.
    pub fn observe(&mut self, message: &WireMessage) {
        let WireMessage::Incremental(Incremental::Trade {
            md_seq,
            price,
            qty,
            aggressor,
            ..
        }) = message
        else {
            return;
        };
        if self
            .trades
            .back()
            .is_some_and(|last| *md_seq <= last.md_seq)
        {
            return;
        }
        self.trades_total = self.trades_total.saturating_add(1);
        self.trades.push_back(TradeView {
            md_seq: *md_seq,
            price: *price,
            qty: *qty,
            aggressor: *aggressor,
        });
        while self.trades.len() > TRADE_CAPACITY {
            self.trades.pop_front();
        }
        self.updated = Some(Instant::now());
    }

    /// The levels and sequence as JSON.
    #[must_use]
    pub fn book_json(&self) -> String {
        format!(
            "{{\"md_seq\":{},\"synced\":{},\"bids\":[{}],\"asks\":[{}]}}",
            self.md_seq,
            self.synced,
            levels_json(&self.bids),
            levels_json(&self.asks)
        )
    }

    /// The tape as JSON, newest first.
    #[must_use]
    pub fn trades_json(&self) -> String {
        let trades = self
            .trades
            .iter()
            .rev()
            .map(|trade| {
                format!(
                    "{{\"md_seq\":{},\"price\":{},\"qty\":{},\"aggressor\":\"{}\"}}",
                    trade.md_seq,
                    trade.price,
                    trade.qty,
                    side_name(trade.aggressor)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"trades\":[{trades}],\"trades_total\":{}}}",
            self.trades_total
        )
    }

    /// The whole view as one JSON object (prices and quantities stay in ticks).
    #[must_use]
    pub fn to_json(&self) -> String {
        format!(
            "{{\"md_seq\":{},\"synced\":{},\"bids\":[{}],\"asks\":[{}],\
             \"trades\":[{}],\"trades_total\":{}}}",
            self.md_seq,
            self.synced,
            levels_json(&self.bids),
            levels_json(&self.asks),
            self.trades
                .iter()
                .rev()
                .map(|trade| format!(
                    "{{\"md_seq\":{},\"price\":{},\"qty\":{},\"aggressor\":\"{}\"}}",
                    trade.md_seq,
                    trade.price,
                    trade.qty,
                    side_name(trade.aggressor)
                ))
                .collect::<Vec<_>>()
                .join(","),
            self.trades_total
        )
    }

    /// The best bid and ask, for the header.
    #[must_use]
    pub fn best(&self) -> (Option<Price>, Option<Price>) {
        (
            self.bids.first().map(|level| level.price),
            self.asks.first().map(|level| level.price),
        )
    }
}

fn levels_json(levels: &[LevelView]) -> String {
    levels
        .iter()
        .map(|level| format!("{{\"price\":{},\"qty\":{}}}", level.price, level.qty))
        .collect::<Vec<_>>()
        .join(",")
}

fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trade_enters_the_tape_once_and_keeps_it_bounded() {
        let mut view = View::default();
        for seq in 1..=(TRADE_CAPACITY as Seq + 5) {
            view.observe(&WireMessage::Incremental(Incremental::Trade {
                md_seq: seq,
                symbol: 1,
                price: 10_000 + seq as Price,
                qty: 1,
                aggressor: Side::Buy,
            }));
        }
        assert_eq!(TRADE_CAPACITY, view.trades.len());
        assert_eq!((TRADE_CAPACITY as u64) + 5, view.trades_total);
        // A duplicate is ignored.
        view.observe(&WireMessage::Incremental(Incremental::Trade {
            md_seq: TRADE_CAPACITY as Seq + 5,
            symbol: 1,
            price: 1,
            qty: 1,
            aggressor: Side::Sell,
        }));
        assert_eq!((TRADE_CAPACITY as u64) + 5, view.trades_total);
    }

    #[test]
    fn the_json_names_every_level_and_trade() {
        let mut view = View {
            md_seq: 7,
            synced: true,
            bids: vec![LevelView {
                price: 10_025,
                qty: 60,
            }],
            asks: vec![LevelView {
                price: 10_050,
                qty: 20,
            }],
            ..View::default()
        };
        view.observe(&WireMessage::Incremental(Incremental::Trade {
            md_seq: 7,
            symbol: 1,
            price: 10_025,
            qty: 40,
            aggressor: Side::Sell,
        }));
        let json = view.to_json();
        assert!(json.contains("\"md_seq\":7"));
        assert!(json.contains("\"bids\":[{\"price\":10025,\"qty\":60}]"));
        assert!(json.contains("\"asks\":[{\"price\":10050,\"qty\":20}]"));
        assert!(json.contains("\"aggressor\":\"sell\""));
    }
}
