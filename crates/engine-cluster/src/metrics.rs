//! The engine's cumulative numbers, shared between the service and the
//! container's log tick.
//!
//! The service writes them from the cluster's callbacks; the container reads
//! them every five seconds to emit `engine.stats`. Neither may wait for the
//! other, so every counter is an atomic rather than a lock around the engine.
//!
//! These are the same numbers the `/metrics` endpoint will render; the log
//! event is the deployment-independent copy (the endpoint arrives with the
//! collectors).

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

/// The engine's cumulative numbers.
#[derive(Debug)]
pub struct EngineMetrics {
    applied: AtomicU64,
    rejected: AtomicU64,
    filled: AtomicU64,
    snapshots: AtomicU64,
    /// The log position of the last command applied.
    commit_position: AtomicI64,
    leader: AtomicBool,
    /// The book's shape, refreshed at most once a second by the service.
    book_bid_levels: AtomicU64,
    book_ask_levels: AtomicU64,
    book_bid_qty: AtomicI64,
    book_ask_qty: AtomicI64,
    /// The engine's market-data sequence.
    md_seq: AtomicI64,
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self {
            applied: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            filled: AtomicU64::new(0),
            snapshots: AtomicU64::new(0),
            // No command has been applied yet, and log positions start at 0.
            commit_position: AtomicI64::new(-1),
            leader: AtomicBool::new(false),
            book_bid_levels: AtomicU64::new(0),
            book_ask_levels: AtomicU64::new(0),
            book_bid_qty: AtomicI64::new(0),
            book_ask_qty: AtomicI64::new(0),
            md_seq: AtomicI64::new(0),
        }
    }
}

impl EngineMetrics {
    /// Commands applied to the engine.
    #[must_use]
    pub fn applied(&self) -> u64 {
        self.applied.load(Ordering::Relaxed)
    }

    /// Commands the engine refused (a reject report or a refused cancel).
    #[must_use]
    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Fills produced.
    #[must_use]
    pub fn filled(&self) -> u64 {
        self.filled.load(Ordering::Relaxed)
    }

    /// Snapshots taken.
    #[must_use]
    pub fn snapshots(&self) -> u64 {
        self.snapshots.load(Ordering::Relaxed)
    }

    /// The last applied log position; `-1` before the first command.
    #[must_use]
    pub fn commit_position(&self) -> i64 {
        self.commit_position.load(Ordering::Relaxed)
    }

    /// Whether this member believes it is the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.leader.load(Ordering::Relaxed)
    }

    pub(crate) fn note_applied(&self, log_pos: i64) {
        self.applied.fetch_add(1, Ordering::Relaxed);
        self.commit_position.store(log_pos, Ordering::Relaxed);
    }

    pub(crate) fn note_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_filled(&self) {
        self.filled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn note_snapshot(&self) {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set_leader(&self, leader: bool) {
        self.leader.store(leader, Ordering::Relaxed);
    }

    /// The book's shape, as of the last refresh.
    #[must_use]
    pub fn book(&self) -> BookShape {
        BookShape {
            bid_levels: self.book_bid_levels.load(Ordering::Relaxed),
            ask_levels: self.book_ask_levels.load(Ordering::Relaxed),
            bid_qty: self.book_bid_qty.load(Ordering::Relaxed),
            ask_qty: self.book_ask_qty.load(Ordering::Relaxed),
            md_seq: self.md_seq.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn note_book(&self, shape: BookShape) {
        self.book_bid_levels
            .store(shape.bid_levels, Ordering::Relaxed);
        self.book_ask_levels
            .store(shape.ask_levels, Ordering::Relaxed);
        self.book_bid_qty.store(shape.bid_qty, Ordering::Relaxed);
        self.book_ask_qty.store(shape.ask_qty, Ordering::Relaxed);
        self.md_seq.store(shape.md_seq, Ordering::Relaxed);
    }
}

/// The book's shape the `/metrics` page reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BookShape {
    /// Resting bid levels.
    pub bid_levels: u64,
    /// Resting ask levels.
    pub ask_levels: u64,
    /// Total resting bid quantity.
    pub bid_qty: i64,
    /// Total resting ask quantity.
    pub ask_qty: i64,
    /// The engine's market-data sequence.
    pub md_seq: i64,
}

/// The `/metrics` body (Prometheus text format 0.0.4).
#[must_use]
pub fn prometheus(metrics: &EngineMetrics) -> String {
    let shape = metrics.book();
    let mut out = String::with_capacity(1024);
    out.push_str("# TYPE deepbook_engine_up gauge\ndeepbook_engine_up 1\n");
    push_gauge(
        &mut out,
        "deepbook_engine_is_leader",
        i64::from(metrics.is_leader()),
    );
    push_gauge(
        &mut out,
        "deepbook_engine_commit_position",
        metrics.commit_position(),
    );
    push_counter(&mut out, "deepbook_engine_applied_total", metrics.applied());
    push_counter(
        &mut out,
        "deepbook_engine_rejected_total",
        metrics.rejected(),
    );
    push_counter(&mut out, "deepbook_engine_filled_total", metrics.filled());
    out.push_str("# TYPE deepbook_engine_book_levels gauge\n");
    out.push_str(&format!(
        "deepbook_engine_book_levels{{side=\"bid\"}} {}\n",
        shape.bid_levels
    ));
    out.push_str(&format!(
        "deepbook_engine_book_levels{{side=\"ask\"}} {}\n",
        shape.ask_levels
    ));
    out.push_str("# TYPE deepbook_engine_book_qty gauge\n");
    out.push_str(&format!(
        "deepbook_engine_book_qty{{side=\"bid\"}} {}\n",
        shape.bid_qty
    ));
    out.push_str(&format!(
        "deepbook_engine_book_qty{{side=\"ask\"}} {}\n",
        shape.ask_qty
    ));
    push_counter(
        &mut out,
        "deepbook_engine_snapshot_count",
        metrics.snapshots(),
    );
    push_gauge(&mut out, "deepbook_engine_md_seq", shape.md_seq);
    out
}

fn push_gauge(out: &mut String, name: &str, value: i64) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" gauge\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn push_counter(out: &mut String, name: &str, value: u64) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" counter\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::EngineMetrics;

    #[test]
    fn the_counters_only_move_forward() {
        let metrics = EngineMetrics::default();
        assert_eq!(0, metrics.applied());
        assert_eq!(-1, metrics.commit_position());
        metrics.note_applied(7);
        metrics.note_applied(9);
        metrics.note_rejected();
        metrics.note_filled();
        metrics.note_snapshot();
        metrics.set_leader(true);
        assert_eq!(2, metrics.applied());
        assert_eq!(9, metrics.commit_position());
        assert_eq!(1, metrics.rejected());
        assert_eq!(1, metrics.filled());
        assert_eq!(1, metrics.snapshots());
        assert!(metrics.is_leader());
    }
}
