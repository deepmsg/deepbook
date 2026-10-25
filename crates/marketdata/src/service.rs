//! The publishing side of market data.
//!
//! [`MarketDataService`] mirrors the levels from the engine's book updates,
//! assigns the contiguous `md_seq` stream, keeps a bounded ring of recent
//! incrementals and answers recovery requests. The transport (deepmsg IPC/UDP
//! or an in-memory test channel) is outside this type; the periodic snapshot
//! broadcast is the caller's timer, not a clock hidden in here.

use std::collections::VecDeque;
use std::fmt;

use deepbook_types::{Price, Qty, Seq, Side, SymbolId};

use crate::message::{BookAction, Incremental, RecoveryRequest, RecoveryResponse, Snapshot};
use crate::mirror::MirrorBook;

/// Service configuration.
#[derive(Clone, Copy, Debug)]
pub struct ServiceConfig {
    /// The served symbol.
    pub symbol: SymbolId,
    /// Incrementals kept for recovery. A request older than the ring is
    /// answered with a snapshot.
    pub recent_capacity: usize,
}

impl ServiceConfig {
    /// A service keeping 10,000 incrementals.
    #[must_use]
    pub const fn new(symbol: SymbolId) -> Self {
        Self {
            symbol,
            recent_capacity: 10_000,
        }
    }
}

/// Why the service refused to publish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketDataError {
    /// The incremental names another symbol.
    WrongSymbol {
        /// The configured symbol.
        expected: SymbolId,
        /// The increment's symbol.
        actual: SymbolId,
    },
    /// The sequence was already published.
    Stale {
        /// The service's last sequence.
        last: Seq,
        /// The rejected sequence.
        received: Seq,
    },
    /// A sequence was skipped; the producer must not create holes.
    Gap {
        /// The next sequence the service expects.
        expected: Seq,
        /// The rejected sequence.
        received: Seq,
    },
    /// A book update or trade had a negative quantity.
    InvalidQuantity(Qty),
}

impl fmt::Display for MarketDataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongSymbol { expected, actual } => {
                write!(
                    formatter,
                    "market data is for symbol {expected}, got {actual}"
                )
            }
            Self::Stale { last, received } => {
                write!(formatter, "sequence {received} is not after {last}")
            }
            Self::Gap { expected, received } => {
                write!(formatter, "sequence {received} skipped {expected}")
            }
            Self::InvalidQuantity(qty) => write!(formatter, "invalid quantity {qty}"),
        }
    }
}

impl std::error::Error for MarketDataError {}

/// The producer-side market-data state.
#[derive(Debug)]
pub struct MarketDataService {
    config: ServiceConfig,
    book: MirrorBook,
    last_md_seq: Seq,
    recent: VecDeque<Incremental>,
}

impl MarketDataService {
    /// A service starting at sequence 0 (the first incremental is 1).
    #[must_use]
    pub fn new(config: ServiceConfig) -> Self {
        Self::with_sequence(config, 0)
    }

    /// A service continuing after `last_md_seq` (e.g. after a snapshot).
    #[must_use]
    pub fn with_sequence(config: ServiceConfig, last_md_seq: Seq) -> Self {
        Self {
            config,
            book: MirrorBook::new(config.symbol),
            last_md_seq,
            recent: VecDeque::with_capacity(config.recent_capacity.max(1)),
        }
    }

    /// The configured symbol.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.config.symbol
    }

    /// The last published sequence.
    #[must_use]
    pub const fn last_md_seq(&self) -> Seq {
        self.last_md_seq
    }

    /// The mirrored book, for snapshots and tests.
    #[must_use]
    pub const fn book(&self) -> &MirrorBook {
        &self.book
    }

    /// Publish a book update, deriving its [`BookAction`].
    ///
    /// # Errors
    ///
    /// As [`Self::publish`], or a negative quantity.
    pub fn on_book_update(
        &mut self,
        md_seq: Seq,
        side: Side,
        price: Price,
        qty: Qty,
    ) -> Result<Incremental, MarketDataError> {
        if qty < 0 {
            return Err(MarketDataError::InvalidQuantity(qty));
        }
        let previous = self.book.qty_at(side, price);
        let action = if qty == 0 {
            BookAction::Delete
        } else if previous == 0 {
            BookAction::Add
        } else {
            BookAction::Update
        };
        let incremental = Incremental::BookUpdate {
            md_seq,
            symbol: self.config.symbol,
            side,
            price,
            qty,
            action,
        };
        self.publish(incremental)?;
        Ok(incremental)
    }

    /// Publish a trade.
    ///
    /// # Errors
    ///
    /// As [`Self::publish`], or a non-positive quantity.
    pub fn on_trade(
        &mut self,
        md_seq: Seq,
        price: Price,
        qty: Qty,
        aggressor: Side,
    ) -> Result<Incremental, MarketDataError> {
        if qty <= 0 {
            return Err(MarketDataError::InvalidQuantity(qty));
        }
        let incremental = Incremental::Trade {
            md_seq,
            symbol: self.config.symbol,
            price,
            qty,
            aggressor,
        };
        self.publish(incremental)?;
        Ok(incremental)
    }

    /// Publish one already-built incremental. The sequence must be exactly the
    /// next one; the producer is the only source of the stream and must not
    /// create holes.
    ///
    /// # Errors
    ///
    /// A wrong symbol, a stale sequence or a skipped sequence.
    pub fn publish(&mut self, incremental: Incremental) -> Result<(), MarketDataError> {
        if incremental.symbol() != self.config.symbol {
            return Err(MarketDataError::WrongSymbol {
                expected: self.config.symbol,
                actual: incremental.symbol(),
            });
        }
        if incremental.md_seq() <= self.last_md_seq {
            return Err(MarketDataError::Stale {
                last: self.last_md_seq,
                received: incremental.md_seq(),
            });
        }
        if incremental.md_seq() != self.last_md_seq + 1 {
            return Err(MarketDataError::Gap {
                expected: self.last_md_seq + 1,
                received: incremental.md_seq(),
            });
        }

        self.book.apply(&incremental);
        self.last_md_seq = incremental.md_seq();
        self.recent.push_back(incremental);
        while self.recent.len() > self.config.recent_capacity.max(1) {
            self.recent.pop_front();
        }
        Ok(())
    }

    /// A snapshot of the current levels at the last sequence.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.book.snapshot(self.last_md_seq)
    }

    /// Answer a recovery request.
    ///
    /// A request with `last_md_seq == 0` (a subscriber without a base) or one
    /// older than the ring gets a snapshot; a request covered by the ring gets
    /// the missing incrementals; a current subscriber gets `UpToDate`.
    #[must_use]
    pub fn recover(&self, request: &RecoveryRequest) -> RecoveryResponse {
        if request.symbol != self.config.symbol {
            return RecoveryResponse::UnknownSymbol {
                symbol: request.symbol,
            };
        }
        if request.last_md_seq != 0 && request.last_md_seq >= self.last_md_seq {
            return RecoveryResponse::UpToDate {
                symbol: self.config.symbol,
                md_seq: self.last_md_seq,
            };
        }
        let from_md_seq = request.last_md_seq + 1;
        let covered = request.last_md_seq != 0
            && self
                .recent
                .front()
                .is_some_and(|first| from_md_seq >= first.md_seq());
        if !covered {
            return RecoveryResponse::Snapshot(self.snapshot());
        }
        let incrementals: Vec<Incremental> = self
            .recent
            .iter()
            .copied()
            .filter(|incremental| incremental.md_seq() >= from_md_seq)
            .collect();
        RecoveryResponse::Incrementals {
            symbol: self.config.symbol,
            from_md_seq,
            incrementals,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service() -> MarketDataService {
        MarketDataService::new(ServiceConfig {
            symbol: 1,
            recent_capacity: 4,
        })
    }

    #[test]
    fn sequences_must_be_contiguous() {
        let mut service = service();
        service.on_book_update(1, Side::Sell, 100, 10).expect("1");
        assert_eq!(
            Err(MarketDataError::Gap {
                expected: 2,
                received: 3
            }),
            service.on_book_update(3, Side::Sell, 101, 5)
        );
        assert_eq!(
            Err(MarketDataError::Stale {
                last: 1,
                received: 1
            }),
            service.on_trade(1, 100, 1, Side::Buy)
        );
    }

    #[test]
    fn the_book_action_is_derived_from_the_level() {
        let mut service = service();
        let add = service.on_book_update(1, Side::Buy, 99, 5).expect("add");
        let update = service.on_book_update(2, Side::Buy, 99, 8).expect("update");
        let delete = service.on_book_update(3, Side::Buy, 99, 0).expect("delete");
        assert!(matches!(
            add,
            Incremental::BookUpdate {
                action: BookAction::Add,
                ..
            }
        ));
        assert!(matches!(
            update,
            Incremental::BookUpdate {
                action: BookAction::Update,
                ..
            }
        ));
        assert!(matches!(
            delete,
            Incremental::BookUpdate {
                action: BookAction::Delete,
                ..
            }
        ));
        assert!(service.book().is_empty());
    }

    #[test]
    fn recovery_returns_missing_incrementals_or_a_snapshot() {
        let mut service = service();
        for seq in 1..=6 {
            service
                .on_book_update(seq, Side::Sell, 100 + i64::try_from(seq).expect("price"), 1)
                .expect("update");
        }
        let missing = service.recover(&RecoveryRequest {
            symbol: 1,
            last_md_seq: 4,
        });
        match missing {
            RecoveryResponse::Incrementals {
                from_md_seq,
                incrementals,
                ..
            } => {
                assert_eq!(5, from_md_seq);
                assert_eq!(2, incrementals.len());
                assert_eq!(5, incrementals[0].md_seq());
            }
            other => panic!("unexpected {other:?}"),
        }

        // The ring keeps four entries (3..=6); a request at 1 needs 2, which
        // is outside it.
        let old = service.recover(&RecoveryRequest {
            symbol: 1,
            last_md_seq: 1,
        });
        assert!(matches!(old, RecoveryResponse::Snapshot(_)));

        // No base yet: a snapshot.
        let cold = service.recover(&RecoveryRequest {
            symbol: 1,
            last_md_seq: 0,
        });
        assert!(matches!(cold, RecoveryResponse::Snapshot(_)));

        let current = service.recover(&RecoveryRequest {
            symbol: 1,
            last_md_seq: 6,
        });
        assert!(matches!(
            current,
            RecoveryResponse::UpToDate { md_seq: 6, .. }
        ));
    }
}
