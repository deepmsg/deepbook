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
