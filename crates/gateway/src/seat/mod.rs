//! The multi-seat gateway pieces (ADR 0012 §6).
//!
//! - [`config`] parses the shared `seats.toml` registry;
//! - [`journal`] is the per-seat, per-generation outbound journal;
//! - [`render`] projects engine events onto DeepOUCH frames;
//! - [`resolve_requested`] decides whether a login replays or starts live.
//!
//! The TCP listener and the cluster-envelope loop are separate: these pieces
//! are pure and testable without a cluster or a socket.
#![forbid(unsafe_code)]

pub mod config;
pub mod fix_engine;
pub mod fix_render;
pub mod fix_session;
pub mod gateway;
pub mod journal;
pub mod render;
pub mod session;

pub use config::{SeatConfig, SeatConfigError, SeatProtocol, SeatRegistry};
pub use fix_engine::SeatFixEngine;
pub use fix_session::{FixEventOutcome, FixSeatError, FixSeatSession};
pub use gateway::{SeatGateway, SeatGatewayConfig};
pub use journal::{SeatJournal, SeatJournalError, latest as latest_journal};
pub use render::{RenderError, render_event};
pub use session::{BoundOutcome, EventOutcome, LoginOutcome, SeatSession, SeatSessionError};

/// What a login should do with the client's `requested_seq`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginPlan {
    /// Start at `next_seq` with no replay.
    Live {
        /// The first sequence the server will send.
        next_seq: u64,
    },
    /// Replay the journal from `from` and then continue at `next_seq`.
    Replay {
        /// The first replayed sequence.
        from: u64,
        /// The next live sequence after the journal.
        next_seq: u64,
    },
}

/// Why a `requested_seq` cannot be served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestedSeqError {
    /// The requested sequence.
    pub requested: u64,
    /// The generation's oldest retained sequence, if any.
    pub oldest: Option<u64>,
    /// The generation's next sequence.
    pub next_seq: u64,
}

impl std::fmt::Display for RequestedSeqError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "requested sequence {} is outside [oldest {:?}, next {})",
            self.requested, self.oldest, self.next_seq
        )
    }
}

impl std::error::Error for RequestedSeqError {}

/// Resolve the login sequence semantics of ADR 0012 §5.4.
///
/// - `0`: replay from the oldest retained frame, or start live when the
///   generation is empty;
/// - `u64::MAX`: start live, no replay;
/// - inside `[oldest, next]`: replay from the requested sequence (`next`
///   itself is a zero-length replay, i.e. live);
/// - anything else: [`RequestedSeqError`].
///
/// The generation is the one the service's `SeatBound` selected, so `next_seq`
/// is the cluster's current sequence, not a stale local guess.
///
/// # Errors
///
/// [`RequestedSeqError`] when `requested` cannot be served.
pub fn resolve_requested(
    requested: u64,
    oldest: Option<u64>,
    next_seq: u64,
) -> Result<LoginPlan, RequestedSeqError> {
    if requested == 0 {
        return Ok(match oldest {
            Some(from) if from < next_seq => LoginPlan::Replay { from, next_seq },
            _ => LoginPlan::Live { next_seq },
        });
    }
    if requested == u64::MAX {
        return Ok(LoginPlan::Live { next_seq });
    }
    if requested > next_seq {
        return Err(RequestedSeqError {
            requested,
            oldest,
            next_seq,
        });
    }
    if requested == next_seq {
        return Ok(LoginPlan::Live { next_seq });
    }
    match oldest {
        Some(from) if requested >= from => Ok(LoginPlan::Replay {
            from: requested,
            next_seq,
        }),
        _ => Err(RequestedSeqError {
            requested,
            oldest,
            next_seq,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_replays_the_oldest_or_starts_live() {
        assert_eq!(
            resolve_requested(0, Some(5), 10),
            Ok(LoginPlan::Replay {
                from: 5,
                next_seq: 10
            })
        );
        assert_eq!(
            resolve_requested(0, None, 1),
            Ok(LoginPlan::Live { next_seq: 1 })
        );
        assert_eq!(
            resolve_requested(0, Some(10), 10),
            Ok(LoginPlan::Live { next_seq: 10 })
        );
    }

    #[test]
    fn max_skips_replay() {
        assert_eq!(
            resolve_requested(u64::MAX, Some(5), 10),
            Ok(LoginPlan::Live { next_seq: 10 })
        );
    }

    #[test]
    fn a_sequence_inside_the_generation_replays() {
        assert_eq!(
            resolve_requested(5, Some(5), 10),
            Ok(LoginPlan::Replay {
                from: 5,
                next_seq: 10
            })
        );
        assert_eq!(
            resolve_requested(9, Some(5), 10),
            Ok(LoginPlan::Replay {
                from: 9,
                next_seq: 10
            })
        );
        assert_eq!(
            resolve_requested(10, Some(5), 10),
            Ok(LoginPlan::Live { next_seq: 10 })
        );
    }

    #[test]
    fn out_of_range_is_refused() {
        assert_eq!(
            resolve_requested(4, Some(5), 10),
            Err(RequestedSeqError {
                requested: 4,
                oldest: Some(5),
                next_seq: 10
            })
        );
        assert_eq!(
            resolve_requested(11, Some(5), 10),
            Err(RequestedSeqError {
                requested: 11,
                oldest: Some(5),
                next_seq: 10
            })
        );
        assert!(resolve_requested(2, None, 1).is_err());
    }
}
