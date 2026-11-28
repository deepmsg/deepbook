//! One DeepOUCH seat's protocol state (ADR 0012 §6.2–§6.4).
//!
//! This is the pure half of the gateway's seat host: it turns the cluster's
//! `SeatBound`/`SeatEvent` frames into a journal and client frames, decides
//! replay versus live at login, and batches the durable acknowledgements. It
//! owns no socket and no cluster client, so the event loop can drive it and
//! the tests can exercise the recovery rules directly.

use std::fmt;
use std::time::{Duration, Instant};

use deepbook_engine_cluster::envelope::{
    AckSeat, KIND_MALFORMED_REJECT, MALFORMED_REJECT_LEN, SeatBound, SeatEvent,
};
use deepbook_engine_core::{ClientOrderId, decode_event};
use deepbook_ouch::ids::from_client_order_id;
use deepbook_ouch::wire::{FrameError, Message, encode};

use crate::seat::config::SeatConfig;
use crate::seat::journal::{SeatJournal, SeatJournalError};
use crate::seat::render::{RenderError, render_event};
use crate::seat::{LoginPlan, RequestedSeqError, resolve_requested};

/// Maximum events between acknowledgements.
const ACK_BATCH: u64 = 64;
/// Maximum time between acknowledgements.
const ACK_INTERVAL: Duration = Duration::from_millis(50);

/// A seat-session failure.
#[derive(Debug)]
pub enum SeatSessionError {
    /// No `SeatBound` has been applied.
    NotBound,
    /// The frame names another seat.
    WrongSeat {
        /// The configured seat.
        expected: u16,
        /// The frame's seat.
        found: u16,
    },
    /// The frame names a stale epoch.
    WrongEpoch {
        /// The current epoch.
        expected: u64,
        /// The frame's epoch.
        found: u64,
    },
    /// The frame names a stale generation.
    WrongGeneration {
        /// The current generation.
        expected: u64,
        /// The frame's generation.
        found: u64,
    },
    /// The event sequence skipped forward.
    Gap {
        /// The expected sequence.
        expected: u64,
        /// The sequence received.
        found: u64,
    },
    /// The outbox payload kind is not one this build knows.
    UnsupportedPayloadKind(u8),
    /// An engine event that is not an order-state event reached a seat.
    UnsupportedEvent,
    /// A malformed-reject payload is not exactly 18 bytes.
    MalformedPayload,
    /// The engine event could not be decoded.
    Decode(deepbook_engine_core::WireError),
    /// The event could not be projected onto the wire.
    Render(RenderError),
    /// The journal failed.
    Journal(SeatJournalError),
    /// The requested sequence cannot be served.
    Requested(RequestedSeqError),
    /// The replace-request id is not decimal.
    NonDecimalReplaceId(ClientOrderId),
    /// The outbound frame could not be encoded.
    Frame(FrameError),
}

impl fmt::Display for SeatSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBound => write!(formatter, "seat is not bound"),
            Self::WrongSeat { expected, found } => {
                write!(formatter, "seat {found}, expected {expected}")
            }
            Self::WrongEpoch { expected, found } => {
                write!(formatter, "seat epoch {found}, expected {expected}")
            }
            Self::WrongGeneration { expected, found } => {
                write!(formatter, "seat generation {found}, expected {expected}")
            }
            Self::Gap { expected, found } => {
                write!(formatter, "seat sequence {found}, expected {expected}")
            }
            Self::UnsupportedPayloadKind(kind) => {
                write!(formatter, "unsupported seat payload kind {kind}")
            }
            Self::UnsupportedEvent => write!(formatter, "event is not an order-state event"),
            Self::MalformedPayload => write!(formatter, "malformed reject has the wrong length"),
            Self::Decode(error) => write!(formatter, "{error}"),
            Self::Render(error) => write!(formatter, "{error}"),
            Self::Journal(error) => write!(formatter, "{error}"),
            Self::Requested(error) => write!(formatter, "{error}"),
            Self::NonDecimalReplaceId(id) => {
                write!(formatter, "non-decimal replace request id {id}")
            }
            Self::Frame(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for SeatSessionError {}

impl From<SeatJournalError> for SeatSessionError {
    fn from(error: SeatJournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<RenderError> for SeatSessionError {
    fn from(error: RenderError) -> Self {
        Self::Render(error)
    }
}

impl From<RequestedSeqError> for SeatSessionError {
    fn from(error: RequestedSeqError) -> Self {
        Self::Requested(error)
    }
}

impl From<FrameError> for SeatSessionError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

/// What a successful bind established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoundOutcome {
    /// The seat epoch.
    pub epoch: u64,
    /// The journal generation.
    pub generation: u64,
    /// The first sequence the cluster will offer.
    pub replay_from_seq: u64,
    /// The cluster's next sequence.
    pub next_seq: u64,
    /// Whether the cluster generated a resync batch.
    pub resynced: bool,
}

/// The result of one `SeatEvent`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventOutcome {
    /// The event was already journaled.
    Duplicate,
    /// A frame to write to the client's journal/socket.
    Frame {
        /// The sequence.
        seat_seq: u64,
        /// The encoded DeepOUCH frame.
        frame: Vec<u8>,
    },
}

/// What a login should send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoginOutcome {
    /// The resolved plan.
    pub plan: LoginPlan,
    /// The first business sequence after `LoginAccepted`.
    pub first_seq: u64,
    /// Raw journal frames to send before live traffic.
    pub replay: Vec<u8>,
}

/// One seat's journal, sequence and login state.
#[derive(Debug)]
pub struct SeatSession {
    config: SeatConfig,
    journal: Option<SeatJournal>,
    epoch: u64,
    generation: u64,
    bound: bool,
    last_applied_seq: u64,
    acked_seq: u64,
    next_seq: u64,
    unacked: u64,
    last_ack: Instant,
}

impl SeatSession {
    /// A seat with no journal or binding yet.
    #[must_use]
    pub fn new(config: SeatConfig) -> Self {
        Self {
            config,
            journal: None,
            epoch: 0,
            generation: 0,
            bound: false,
            last_applied_seq: 0,
            acked_seq: 0,
            next_seq: 1,
            unacked: 0,
            last_ack: Instant::now(),
        }
    }

    /// The configured seat.
    #[must_use]
    pub fn config(&self) -> &SeatConfig {
        &self.config
    }

    /// The current epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The current journal generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Whether a bind is current.
    #[must_use]
    pub const fn is_bound(&self) -> bool {
        self.bound
    }

    /// The last sequence journaled.
    #[must_use]
    pub const fn last_applied_seq(&self) -> u64 {
        self.last_applied_seq
    }

    /// The cluster's next sequence as of the last bind/event.
    #[must_use]
    pub const fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Apply a `SeatBound`, opening the generation's journal.
    ///
    /// # Errors
    ///
    /// [`SeatSessionError`] for a foreign seat or a journal failure.
    pub fn on_seat_bound(&mut self, bound: SeatBound) -> Result<BoundOutcome, SeatSessionError> {
        if bound.seat_id != self.config.seat_id {
            return Err(SeatSessionError::WrongSeat {
                expected: self.config.seat_id,
                found: bound.seat_id,
            });
        }
        let journal = SeatJournal::open_with_sync(&self.config.journal_dir, bound.generation, 64)?;
        self.last_applied_seq = journal.oldest_seq().map_or_else(
            || bound.replay_from_seq.saturating_sub(1),
            |_| journal.next_seq().saturating_sub(1),
        );
        self.journal = Some(journal);
        self.epoch = bound.epoch;
        self.generation = bound.generation;
        self.next_seq = bound.next_seq;
        self.bound = true;
        self.acked_seq = self.last_applied_seq;
        self.unacked = 0;
        self.last_ack = Instant::now();
        Ok(BoundOutcome {
            epoch: bound.epoch,
            generation: bound.generation,
            replay_from_seq: bound.replay_from_seq,
            next_seq: bound.next_seq,
            resynced: bound.resynced,
        })
    }

    /// Apply one outbox event.
    ///
    /// # Errors
    ///
    /// [`SeatSessionError`] for a foreign epoch/generation, a gap, a corrupt
    /// payload, or a journal failure.
    pub fn on_event(&mut self, event: &SeatEvent<'_>) -> Result<EventOutcome, SeatSessionError> {
        if !self.bound {
            return Err(SeatSessionError::NotBound);
        }
        if event.seat_id != self.config.seat_id {
            return Err(SeatSessionError::WrongSeat {
                expected: self.config.seat_id,
                found: event.seat_id,
            });
        }
        if event.epoch != self.epoch {
            return Err(SeatSessionError::WrongEpoch {
                expected: self.epoch,
                found: event.epoch,
            });
        }
        if event.generation != self.generation {
            return Err(SeatSessionError::WrongGeneration {
                expected: self.generation,
                found: event.generation,
            });
        }
        if event.seat_seq <= self.last_applied_seq {
            return Ok(EventOutcome::Duplicate);
        }
        if event.seat_seq != self.last_applied_seq.saturating_add(1) {
            return Err(SeatSessionError::Gap {
                expected: self.last_applied_seq.saturating_add(1),
                found: event.seat_seq,
            });
        }

        let message = match event.payload_kind {
            0 => {
                let decoded = decode_event(event.payload).map_err(SeatSessionError::Decode)?;
                let replace_req = match event.replace_req_cl_ord_id {
                    Some(id) => Some(
                        from_client_order_id(id)
                            .ok_or(SeatSessionError::NonDecimalReplaceId(id))?,
                    ),
                    None => None,
                };
                render_event(
                    event.seat_seq,
                    event.liquidity,
                    event.resync,
                    replace_req,
                    &decoded,
                )?
                .ok_or(SeatSessionError::UnsupportedEvent)?
            }
            KIND_MALFORMED_REJECT => {
                if event.payload.len() != MALFORMED_REJECT_LEN {
                    return Err(SeatSessionError::MalformedPayload);
                }
                let payload = event.payload;
                let raw_cl = u64::from_le_bytes(payload[..8].try_into().expect("eight"));
                let raw_orig = u64::from_le_bytes(payload[8..16].try_into().expect("eight"));
                let is_cancel = match payload[16] {
                    0 => false,
                    1 => true,
                    _ => return Err(SeatSessionError::MalformedPayload),
                };
                Message::OrderRejected {
                    seq: event.seat_seq,
                    cl_ord_id: raw_cl,
                    orig_cl_ord_id: raw_orig,
                    reason: payload[17],
                    is_cancel_reject: is_cancel,
                }
            }
            other => return Err(SeatSessionError::UnsupportedPayloadKind(other)),
        };
        let frame = encode(&message)?;
        let journal = self.journal.as_mut().ok_or(SeatSessionError::NotBound)?;
        journal.append(event.seat_seq, &frame)?;
        self.last_applied_seq = event.seat_seq;
        self.next_seq = self.next_seq.max(event.seat_seq.saturating_add(1));
        self.unacked = self.unacked.saturating_add(1);
        Ok(EventOutcome::Frame {
            seat_seq: event.seat_seq,
            frame,
        })
    }

    /// Resolve a client login against the current journal.
    ///
    /// # Errors
    ///
    /// [`SeatSessionError`] for a missing bind, a sequence outside the
    /// generation, or a journal failure.
    pub fn login(&self, requested_seq: u64) -> Result<LoginOutcome, SeatSessionError> {
        let journal = self.journal.as_ref().ok_or(SeatSessionError::NotBound)?;
        let plan = resolve_requested(requested_seq, journal.oldest_seq(), self.next_seq)?;
        let (first_seq, replay) = match plan {
            LoginPlan::Live { next_seq } => (next_seq, Vec::new()),
            LoginPlan::Replay { from, .. } => (from, journal.replay(from)?),
        };
        Ok(LoginOutcome {
            plan,
            first_seq,
            replay,
        })
    }

    /// The acknowledgement owed to the cluster, when one is due.
    ///
    /// # Errors
    ///
    /// [`SeatSessionError::NotBound`] when the seat has no current bind.
    pub fn ack_due(&mut self) -> Result<Option<AckSeat>, SeatSessionError> {
        if !self.bound {
            return Err(SeatSessionError::NotBound);
        }
        if self.unacked == 0 {
            return Ok(None);
        }
        if self.unacked < ACK_BATCH && self.last_ack.elapsed() < ACK_INTERVAL {
            return Ok(None);
        }
        let journal = self.journal.as_mut().ok_or(SeatSessionError::NotBound)?;
        journal.sync()?;
        let durable = journal.synced_seq().min(self.last_applied_seq);
        if durable <= self.acked_seq {
            self.unacked = 0;
            self.last_ack = Instant::now();
            return Ok(None);
        }
        self.acked_seq = durable;
        self.unacked = 0;
        self.last_ack = Instant::now();
        Ok(Some(AckSeat {
            seat_id: self.config.seat_id,
            epoch: self.epoch,
            up_to_seq: durable,
        }))
    }

    /// Drop the binding (a fence or a transport close); the journal stays.
    pub fn mark_unbound(&mut self) {
        self.bound = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_cluster::envelope::KIND_MALFORMED_REJECT as MALFORMED_KIND;
    use deepbook_engine_core::wire::encode_event;
    use deepbook_engine_core::{Event, EventHeader, ExecType, ExecutionReportEvent};
    use deepbook_ouch::ids::to_client_order_id;
    use deepbook_ouch::wire::{self, Message};
    use deepbook_types::{OrderState, Side};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn directory() -> PathBuf {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepbook-seat-session-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn config(directory: &Path) -> SeatConfig {
        SeatConfig {
            seat_id: 3,
            account_id: 11,
            protocol: crate::seat::config::SeatProtocol::Ouch,
            token: Some([0; 32]),
            sender: None,
            target: None,
            journal_dir: directory.to_path_buf(),
        }
    }

    fn bound() -> SeatBound {
        SeatBound {
            seat_id: 3,
            epoch: 1,
            generation: 1,
            replay_from_seq: 1,
            next_seq: 1,
            resynced: false,
        }
    }

    fn cl(value: u64) -> ClientOrderId {
        to_client_order_id(value).expect("id")
    }

    fn event_bytes() -> Vec<u8> {
        encode_event(&Event::ExecutionReport(ExecutionReportEvent {
            header: EventHeader {
                log_pos: 10,
                event_seq: 1,
            },
            order_id: 9,
            cl_ord_id: cl(42),
            account: 11,
            symbol: 1,
            side: Side::Buy,
            exec_type: ExecType::New,
            state: OrderState::New,
            price: Some(100),
            order_qty: 7,
            last_qty: 0,
            last_px: None,
            leaves_qty: 7,
            cum_qty: 0,
            avg_px: 0,
            reason: None,
        }))
    }

    fn event<'a>(seq: u64, payload: &'a [u8], resync: bool) -> SeatEvent<'a> {
        SeatEvent {
            seat_id: 3,
            epoch: 1,
            generation: 1,
            seat_seq: seq,
            liquidity: 0,
            payload_kind: 0,
            resync,
            replace_req_cl_ord_id: None,
            payload,
        }
    }

    #[test]
    fn events_are_journaled_and_acked_in_a_batch() {
        let directory = directory();
        let mut session = SeatSession::new(config(&directory));
        session.on_seat_bound(bound()).expect("bind");
        let payload = event_bytes();
        let outcome = session.on_event(&event(1, &payload, false)).expect("event");
        let EventOutcome::Frame { seat_seq, frame } = outcome else {
            panic!("expected a frame");
        };
        assert_eq!(seat_seq, 1);
        assert!(matches!(
            wire::decode_exact(&frame),
            Ok(Message::OrderAccepted { seq: 1, .. })
        ));
        assert_eq!(session.last_applied_seq(), 1);
        assert!(session.ack_due().expect("ack").is_none());

        let duplicate = session.on_event(&event(1, &payload, false)).expect("dup");
        assert_eq!(duplicate, EventOutcome::Duplicate);
        assert!(matches!(
            session.on_event(&event(3, &payload, false)),
            Err(SeatSessionError::Gap { .. })
        ));
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn login_replays_the_journal_and_live_starts_at_next() {
        let directory = directory();
        let mut session = SeatSession::new(config(&directory));
        session.on_seat_bound(bound()).expect("bind");
        let payload = event_bytes();
        for seq in 1..=2 {
            session
                .on_event(&event(seq, &payload, false))
                .expect("event");
        }
        let replay = session.login(0).expect("login");
        assert_eq!(
            replay.plan,
            LoginPlan::Replay {
                from: 1,
                next_seq: 3
            }
        );
        assert_eq!(replay.first_seq, 1);
        let mut rest = replay.replay.as_slice();
        for seq in 1..=2 {
            let (message, used) = wire::decode(rest).expect("frame");
            assert!(matches!(message, Message::OrderAccepted { seq: got, .. } if got == seq));
            rest = &rest[used..];
        }
        let live = session.login(u64::MAX).expect("live");
        assert_eq!(live.plan, LoginPlan::Live { next_seq: 3 });
        assert!(live.replay.is_empty());
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_resync_frame_is_an_order_accepted() {
        let directory = directory();
        let mut session = SeatSession::new(config(&directory));
        session
            .on_seat_bound(SeatBound {
                seat_id: 3,
                epoch: 1,
                generation: 1,
                replay_from_seq: 4,
                next_seq: 4,
                resynced: true,
            })
            .expect("bind");
        let payload = event_bytes();
        let mut event = event(4, &payload, true);
        event.payload_kind = 0;
        event.resync = true;
        let EventOutcome::Frame { frame, .. } = session.on_event(&event).expect("event") else {
            panic!("expected a frame");
        };
        assert!(matches!(
            wire::decode_exact(&frame),
            Ok(Message::OrderAccepted { seq: 4, .. })
        ));
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_malformed_reject_is_projected() {
        let directory = directory();
        let mut session = SeatSession::new(config(&directory));
        session.on_seat_bound(bound()).expect("bind");
        let mut payload = [0_u8; MALFORMED_REJECT_LEN];
        payload[..8].copy_from_slice(&0_u64.to_le_bytes());
        payload[8..16].copy_from_slice(&7_u64.to_le_bytes());
        payload[16] = 1;
        payload[17] = 101;
        let event = SeatEvent {
            seat_id: 3,
            epoch: 1,
            generation: 1,
            seat_seq: 1,
            liquidity: 0,
            payload_kind: MALFORMED_KIND,
            resync: false,
            replace_req_cl_ord_id: None,
            payload: &payload,
        };
        let EventOutcome::Frame { frame, .. } = session.on_event(&event).expect("event") else {
            panic!("expected a frame");
        };
        assert!(matches!(
            wire::decode_exact(&frame),
            Ok(Message::OrderRejected {
                seq: 1,
                orig_cl_ord_id: 7,
                is_cancel_reject: true,
                ..
            })
        ));
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn stale_epochs_and_generations_are_refused() {
        let directory = directory();
        let mut session = SeatSession::new(config(&directory));
        session.on_seat_bound(bound()).expect("bind");
        let payload = event_bytes();
        let mut stale = event(1, &payload, false);
        stale.epoch = 0;
        assert!(matches!(
            session.on_event(&stale),
            Err(SeatSessionError::WrongEpoch { .. })
        ));
        let mut stale = event(1, &payload, false);
        stale.generation = 0;
        assert!(matches!(
            session.on_event(&stale),
            Err(SeatSessionError::WrongGeneration { .. })
        ));
        let _ = fs::remove_dir_all(&directory);
    }
}
