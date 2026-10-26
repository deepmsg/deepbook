//! The subscriber-side state machine.
//!
//! It applies a snapshot, then incrementals with `md_seq == last + 1`. A hole
//! sets [`Subscriber::recovery_request`], and the transport asks the recovery
//! channel. Incrementals at or below `last` are ignored, so `md_seq` never goes
//! backwards on a replay or a duplicate delivery.

use deepbook_types::{Seq, SymbolId};

use crate::message::{Incremental, RecoveryRequest, RecoveryResponse, Snapshot};
use crate::mirror::MirrorBook;

/// What happened when a message was offered to the subscriber.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The message was applied.
    Applied,
    /// A snapshot installed a new base.
    Synced,
    /// The message was at or below the last applied sequence.
    DuplicateOrStale,
    /// No base snapshot yet; ask for one.
    NeedsSnapshot,
    /// A sequence was skipped; the missing range is `expected..received`.
    Gap {
        /// The next sequence the subscriber expected.
        expected: Seq,
        /// The sequence that arrived.
        received: Seq,
    },
    /// The message names another symbol.
    WrongSymbol,
    /// A recovery response did not close the hole.
    RecoveryIncomplete {
        /// The next sequence still missing.
        expected: Seq,
        /// The sequence that arrived.
        received: Seq,
    },
}

/// A market-data subscriber's mirrored book and sequence state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscriber {
    symbol: SymbolId,
    book: MirrorBook,
    last_md_seq: Option<Seq>,
    needs_recovery: bool,
}

impl Subscriber {
    /// A subscriber with no base yet.
    #[must_use]
    pub fn new(symbol: SymbolId) -> Self {
        Self {
            symbol,
            book: MirrorBook::new(symbol),
            last_md_seq: None,
            needs_recovery: true,
        }
    }

    /// The symbol.
    #[must_use]
    pub const fn symbol(&self) -> SymbolId {
        self.symbol
    }

    /// The mirrored book.
    #[must_use]
    pub const fn book(&self) -> &MirrorBook {
        &self.book
    }

    /// The last applied sequence, if a base snapshot has been installed.
    #[must_use]
    pub const fn last_md_seq(&self) -> Option<Seq> {
        self.last_md_seq
    }

    /// Whether the subscriber has a base and no outstanding hole.
    #[must_use]
    pub const fn is_synced(&self) -> bool {
        self.last_md_seq.is_some() && !self.needs_recovery
    }

    /// The request the transport should send, if any.
    ///
    /// A subscriber without a base asks with `last_md_seq = 0`, which the
    /// service answers with a snapshot.
    #[must_use]
    pub fn recovery_request(&self) -> Option<RecoveryRequest> {
        if self.needs_recovery {
            Some(RecoveryRequest {
                symbol: self.symbol,
                last_md_seq: self.last_md_seq.unwrap_or(0),
            })
        } else {
            None
        }
    }

    /// Install a snapshot. An older snapshot than the current base is ignored.
    pub fn on_snapshot(&mut self, snapshot: &Snapshot) -> ApplyOutcome {
        if snapshot.symbol != self.symbol {
            return ApplyOutcome::WrongSymbol;
        }
        if let Some(last) = self.last_md_seq {
            if snapshot.md_seq < last {
                return ApplyOutcome::DuplicateOrStale;
            }
        }
        self.book.install(snapshot);
        self.last_md_seq = Some(snapshot.md_seq);
        self.needs_recovery = false;
        ApplyOutcome::Synced
    }

    /// Apply an incremental.
    pub fn on_incremental(&mut self, incremental: &Incremental) -> ApplyOutcome {
        if incremental.symbol() != self.symbol {
            return ApplyOutcome::WrongSymbol;
        }
        let Some(last) = self.last_md_seq else {
            self.needs_recovery = true;
            return ApplyOutcome::NeedsSnapshot;
        };
        let received = incremental.md_seq();
        if received <= last {
            return ApplyOutcome::DuplicateOrStale;
        }
        if received != last + 1 {
            self.needs_recovery = true;
            return ApplyOutcome::Gap {
                expected: last + 1,
                received,
            };
        }
        self.book.apply(incremental);
        self.last_md_seq = Some(received);
        ApplyOutcome::Applied
    }

    /// Apply a recovery response.
    pub fn on_recovery(&mut self, response: &RecoveryResponse) -> ApplyOutcome {
        match response {
            RecoveryResponse::Snapshot(snapshot) => self.on_snapshot(snapshot),
            RecoveryResponse::Incrementals {
                symbol,
                incrementals,
                ..
            } => {
                if *symbol != self.symbol {
                    return ApplyOutcome::WrongSymbol;
                }
                if self.last_md_seq.is_none() {
                    self.needs_recovery = true;
                    return ApplyOutcome::NeedsSnapshot;
                }
                if incrementals.is_empty() {
                    return ApplyOutcome::RecoveryIncomplete {
                        expected: self.last_md_seq.unwrap_or(0) + 1,
                        received: self.last_md_seq.unwrap_or(0),
                    };
                }
                let mut outcome = ApplyOutcome::Applied;
                for incremental in incrementals {
                    outcome = self.on_incremental(incremental);
                    match outcome {
                        ApplyOutcome::Applied => {}
                        ApplyOutcome::DuplicateOrStale => {}
                        other => {
                            return match other {
                                ApplyOutcome::Gap { expected, received } => {
                                    ApplyOutcome::RecoveryIncomplete { expected, received }
                                }
                                other => other,
                            };
                        }
                    }
                }
                self.needs_recovery = false;
                outcome
            }
            RecoveryResponse::UpToDate { symbol, md_seq } => {
                if *symbol != self.symbol {
                    return ApplyOutcome::WrongSymbol;
                }
                let Some(last) = self.last_md_seq else {
                    self.needs_recovery = true;
                    return ApplyOutcome::NeedsSnapshot;
                };
                if *md_seq <= last {
                    self.needs_recovery = false;
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::RecoveryIncomplete {
                        expected: last + 1,
                        received: *md_seq,
                    }
                }
            }
            RecoveryResponse::UnknownSymbol { .. } => ApplyOutcome::WrongSymbol,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Level;
    use deepbook_types::Side;

    fn snapshot(md_seq: Seq) -> Snapshot {
        Snapshot {
            md_seq,
            symbol: 1,
            bids: vec![Level { price: 100, qty: 5 }],
            asks: vec![Level { price: 101, qty: 6 }],
        }
    }

    fn update(md_seq: Seq, side: Side, price: i64, qty: i64) -> Incremental {
        Incremental::BookUpdate {
            md_seq,
            symbol: 1,
            side,
            price,
            qty,
            action: crate::message::BookAction::Update,
        }
    }

    #[test]
    fn a_subscriber_needs_a_snapshot_before_incrementals() {
        let mut subscriber = Subscriber::new(1);
        assert_eq!(
            ApplyOutcome::NeedsSnapshot,
            subscriber.on_incremental(&update(1, Side::Sell, 101, 7))
        );
        assert_eq!(
            Some(RecoveryRequest {
                symbol: 1,
                last_md_seq: 0
            }),
            subscriber.recovery_request()
        );
        assert_eq!(ApplyOutcome::Synced, subscriber.on_snapshot(&snapshot(3)));
        assert_eq!(
            ApplyOutcome::Applied,
            subscriber.on_incremental(&update(4, Side::Sell, 101, 7))
        );
        assert!(subscriber.is_synced());
    }

    #[test]
    fn a_hole_is_reported_and_recovered_with_missing_incrementals() {
        let mut subscriber = Subscriber::new(1);
        subscriber.on_snapshot(&snapshot(1));
        assert_eq!(
            ApplyOutcome::Gap {
                expected: 2,
                received: 4
            },
            subscriber.on_incremental(&update(4, Side::Sell, 101, 9))
        );
        assert!(!subscriber.is_synced());
        assert_eq!(
            Some(RecoveryRequest {
                symbol: 1,
                last_md_seq: 1
            }),
            subscriber.recovery_request()
        );

        let response = RecoveryResponse::Incrementals {
            symbol: 1,
            from_md_seq: 2,
            incrementals: vec![
                update(2, Side::Buy, 99, 3),
                update(3, Side::Sell, 101, 8),
                update(4, Side::Sell, 101, 9),
            ],
        };
        assert_eq!(ApplyOutcome::Applied, subscriber.on_recovery(&response));
        assert!(subscriber.is_synced());
        assert_eq!(Some(4), subscriber.last_md_seq());
        assert_eq!(9, subscriber.book().qty_at(Side::Sell, 101));
    }

    #[test]
    fn a_snapshot_is_used_when_the_ring_cannot_cover_the_hole() {
        let mut subscriber = Subscriber::new(1);
        subscriber.on_snapshot(&snapshot(1));
        subscriber.on_incremental(&update(3, Side::Sell, 101, 8));
        let recovered = RecoveryResponse::Snapshot(snapshot(10));
        assert_eq!(ApplyOutcome::Synced, subscriber.on_recovery(&recovered));
        assert_eq!(Some(10), subscriber.last_md_seq());
        assert_eq!(6, subscriber.book().qty_at(Side::Sell, 101));
    }

    #[test]
    fn stale_and_duplicate_messages_never_move_the_sequence_backwards() {
        let mut subscriber = Subscriber::new(1);
        subscriber.on_snapshot(&snapshot(5));
        assert_eq!(
            ApplyOutcome::DuplicateOrStale,
            subscriber.on_incremental(&update(5, Side::Sell, 101, 1))
        );
        assert_eq!(
            ApplyOutcome::DuplicateOrStale,
            subscriber.on_snapshot(&snapshot(4))
        );
        assert_eq!(Some(5), subscriber.last_md_seq());
    }

    #[test]
    fn a_wrong_symbol_is_ignored() {
        let mut subscriber = Subscriber::new(1);
        let wrong = Incremental::Trade {
            md_seq: 1,
            symbol: 2,
            price: 100,
            qty: 1,
            aggressor: Side::Buy,
        };
        assert_eq!(ApplyOutcome::WrongSymbol, subscriber.on_incremental(&wrong));
    }
}
