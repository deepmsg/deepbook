//! One FIX seat's engine-event projection and journal (ADR 0012 §6.3).
//!
//! FIX keeps its own `MsgSeqNum` journal and resend logic; this session only
//! journals the engine event stream for deduplication and the `AckSeat`
//! watermark, and renders each event into the `ApplicationMessage` the FIX
//! session sends. A cluster resync is rendered as `ExecType=D`.

use std::fmt;
use std::time::{Duration, Instant};

use deepbook_engine_cluster::envelope::{AckSeat, KIND_MALFORMED_REJECT, SeatBound, SeatEvent};
use deepbook_engine_core::{Event, decode_event};
use deepbook_fix::ApplicationMessage;

use crate::seat::config::SeatConfig;
use crate::seat::fix_render::{render_cancel_reject, render_report};
use crate::seat::journal::{SeatJournal, SeatJournalError};
use crate::seat::session::BoundOutcome;

const ACK_BATCH: u64 = 256;
const ACK_INTERVAL: Duration = Duration::from_millis(50);

/// A FIX-seat projection failure.
#[derive(Debug)]
pub enum FixSeatError {
    /// No `SeatBound` has been applied.
    NotBound,
    /// The event names another seat.
    WrongSeat {
        /// Configured seat.
        expected: u16,
        /// Event seat.
        found: u16,
    },
    /// The event names a stale epoch.
    WrongEpoch {
        /// Current epoch.
        expected: u64,
        /// Event epoch.
        found: u64,
    },
    /// The event names a stale generation.
    WrongGeneration {
        /// Current generation.
        expected: u64,
        /// Event generation.
        found: u64,
    },
    /// The sequence skipped.
    Gap {
        /// Expected.
        expected: u64,
        /// Received.
        found: u64,
    },
    /// The payload is not an engine order event.
    UnsupportedPayload(u8),
    /// The engine record could not be decoded.
    Decode(deepbook_engine_core::WireError),
    /// The journal failed.
    Journal(SeatJournalError),
}

impl fmt::Display for FixSeatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBound => write!(formatter, "fix seat is not bound"),
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
            Self::UnsupportedPayload(kind) => {
                write!(formatter, "unsupported fix-seat payload {kind}")
            }
            Self::Decode(error) => write!(formatter, "{error}"),
            Self::Journal(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for FixSeatError {}

impl From<SeatJournalError> for FixSeatError {
    fn from(error: SeatJournalError) -> Self {
        Self::Journal(error)
    }
}

/// The result of one event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FixEventOutcome {
    /// Already journaled.
    Duplicate,
    /// A FIX message for the acceptor's session.
    Message {
        /// The sequence.
        seat_seq: u64,
        /// The application message.
        message: Box<ApplicationMessage>,
    },
}

/// One FIX seat's event state.
#[derive(Debug)]
pub struct FixSeatSession {
    config: SeatConfig,
    symbol_id: u16,
    symbol_name: String,
    journal: Option<SeatJournal>,
    epoch: u64,
    generation: u64,
    bound: bool,
    last_applied_seq: u64,
    acked_seq: u64,
    next_seq: u64,
    last_ack: Instant,
}

impl FixSeatSession {
    /// A seat with no binding yet.
    #[must_use]
    pub fn new(config: SeatConfig, symbol_id: u16, symbol_name: impl Into<String>) -> Self {
        Self {
            config,
            symbol_id,
            symbol_name: symbol_name.into(),
            journal: None,
            epoch: 0,
            generation: 0,
            bound: false,
            last_applied_seq: 0,
            acked_seq: 0,
            next_seq: 1,
            last_ack: Instant::now(),
        }
    }

    /// The current epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The configured seat id.
    #[must_use]
    pub const fn seat_id(&self) -> u16 {
        self.config.seat_id
    }

    /// The configured account.
    #[must_use]
    pub const fn account_id(&self) -> u32 {
        self.config.account_id
    }

    /// Whether a bind is current.
    #[must_use]
    pub const fn is_bound(&self) -> bool {
        self.bound
    }

    /// The last journaled sequence.
    #[must_use]
    pub const fn last_applied_seq(&self) -> u64 {
        self.last_applied_seq
    }

    /// Apply a `SeatBound`.
    ///
    /// # Errors
    ///
    /// [`FixSeatError`] for a foreign seat or a journal failure.
    pub fn on_seat_bound(&mut self, bound: SeatBound) -> Result<BoundOutcome, FixSeatError> {
        if bound.seat_id != self.config.seat_id {
            return Err(FixSeatError::WrongSeat {
                expected: self.config.seat_id,
                found: bound.seat_id,
            });
        }
        let journal = SeatJournal::open(&self.config.journal_dir, bound.generation)?;
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
    /// [`FixSeatError`] for a stale bind, a gap, a corrupt record or a journal
    /// failure.
    pub fn on_event(&mut self, event: &SeatEvent<'_>) -> Result<FixEventOutcome, FixSeatError> {
        if !self.bound {
            return Err(FixSeatError::NotBound);
        }
        if event.seat_id != self.config.seat_id {
            return Err(FixSeatError::WrongSeat {
                expected: self.config.seat_id,
                found: event.seat_id,
            });
        }
        if event.epoch != self.epoch {
            return Err(FixSeatError::WrongEpoch {
                expected: self.epoch,
                found: event.epoch,
            });
        }
        if event.generation != self.generation {
            return Err(FixSeatError::WrongGeneration {
                expected: self.generation,
                found: event.generation,
            });
        }
        if event.seat_seq <= self.last_applied_seq {
            return Ok(FixEventOutcome::Duplicate);
        }
        if event.seat_seq != self.last_applied_seq.saturating_add(1) {
            return Err(FixSeatError::Gap {
                expected: self.last_applied_seq.saturating_add(1),
                found: event.seat_seq,
            });
        }
        if event.payload_kind == KIND_MALFORMED_REJECT {
            return Err(FixSeatError::UnsupportedPayload(event.payload_kind));
        }
        let decoded = decode_event(event.payload).map_err(FixSeatError::Decode)?;
        let replace_req = event.replace_req_cl_ord_id.map(|id| id.to_string());
        let message = match &decoded {
            Event::ExecutionReport(report) => render_report(
                report,
                event.resync,
                replace_req.as_deref(),
                self.symbol_id,
                &self.symbol_name,
            ),
            Event::CancelReject(reject) => render_cancel_reject(reject),
            _ => return Err(FixSeatError::UnsupportedPayload(event.payload_kind)),
        };
        let journal = self.journal.as_mut().ok_or(FixSeatError::NotBound)?;
        journal.append(event.seat_seq, event.payload)?;
        self.last_applied_seq = event.seat_seq;
        self.next_seq = self.next_seq.max(event.seat_seq.saturating_add(1));
        Ok(FixEventOutcome::Message {
            seat_seq: event.seat_seq,
            message: Box::new(message),
        })
    }

    /// The acknowledgement owed to the cluster, up to the last message the
    /// acceptor has already returned (and therefore journaled).
    ///
    /// # Errors
    ///
    /// [`FixSeatError::NotBound`] without a current bind.
    pub fn ack_due(&mut self, up_to_seq: u64) -> Result<Option<AckSeat>, FixSeatError> {
        if !self.bound {
            return Err(FixSeatError::NotBound);
        }
        let effective = up_to_seq.min(self.last_applied_seq);
        if effective <= self.acked_seq {
            return Ok(None);
        }
        if effective.saturating_sub(self.acked_seq) < ACK_BATCH
            && self.last_ack.elapsed() < ACK_INTERVAL
        {
            return Ok(None);
        }
        self.acked_seq = effective;
        self.last_ack = Instant::now();
        Ok(Some(AckSeat {
            seat_id: self.config.seat_id,
            epoch: self.epoch,
            up_to_seq: effective,
        }))
    }

    /// Drop the binding; the journal stays.
    pub fn mark_unbound(&mut self) {
        self.bound = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_cluster::envelope::{KIND_ENGINE_EVENT, SeatBound, SeatEvent};
    use deepbook_engine_core::wire::encode_event;
    use deepbook_engine_core::{Event, EventHeader, ExecType, ExecutionReportEvent};
    use deepbook_fix::ExecType as FixExecType;
    use deepbook_ouch::ids::to_client_order_id;
    use deepbook_types::{OrderState, Side, SymbolId};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn directory() -> PathBuf {
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("deepbook-fix-seat-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn config(directory: &Path) -> SeatConfig {
        SeatConfig {
            seat_id: 3,
            account_id: 11,
            protocol: crate::seat::config::SeatProtocol::Fix,
            token: None,
            sender: Some("DEEPBOOK".to_owned()),
            target: Some("CLIENT".to_owned()),
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

    fn event_bytes(exec_type: ExecType) -> Vec<u8> {
        encode_event(&Event::ExecutionReport(ExecutionReportEvent {
            header: EventHeader {
                log_pos: 77,
                event_seq: 3,
            },
            order_id: 9,
            cl_ord_id: to_client_order_id(42).expect("id"),
            account: 11,
            symbol: 1,
            side: Side::Buy,
            exec_type,
            state: OrderState::New,
            price: Some(100_025),
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
            payload_kind: KIND_ENGINE_EVENT,
            resync,
            replace_req_cl_ord_id: None,
            payload,
        }
    }

    fn report(message: ApplicationMessage) -> deepbook_fix::ExecutionReport {
        match message {
            ApplicationMessage::ExecutionReport(report) => report,
            other => panic!("expected a report, got {other:?}"),
        }
    }

    #[test]
    fn an_event_becomes_a_fix_report() {
        let directory = directory();
        let mut session = FixSeatSession::new(config(&directory), 1 as SymbolId, "AAPL");
        session.on_seat_bound(bound()).expect("bind");
        let payload = event_bytes(ExecType::New);
        let FixEventOutcome::Message { seat_seq, message } =
            session.on_event(&event(1, &payload, false)).expect("event")
        else {
            panic!("expected a message");
        };
        assert_eq!(seat_seq, 1);
        let report = report(*message);
        assert_eq!(report.exec_id, "77-3");
        assert_eq!(report.exec_type, FixExecType::New);
        assert_eq!(report.symbol, "AAPL");
        assert_eq!(session.last_applied_seq(), 1);
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_resync_is_restated() {
        let directory = directory();
        let mut session = FixSeatSession::new(config(&directory), 1, "AAPL");
        session.on_seat_bound(bound()).expect("bind");
        let payload = event_bytes(ExecType::New);
        let FixEventOutcome::Message { message, .. } =
            session.on_event(&event(1, &payload, true)).expect("event")
        else {
            panic!("expected a message");
        };
        let report = report(*message);
        assert_eq!(report.exec_type, FixExecType::Restated);
        assert_eq!(report.text.as_deref(), Some("RESYNC"));
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn a_duplicate_and_a_gap_are_distinguished() {
        let directory = directory();
        let mut session = FixSeatSession::new(config(&directory), 1, "AAPL");
        session.on_seat_bound(bound()).expect("bind");
        let payload = event_bytes(ExecType::New);
        session.on_event(&event(1, &payload, false)).expect("event");
        assert!(matches!(
            session.on_event(&event(1, &payload, false)),
            Ok(FixEventOutcome::Duplicate)
        ));
        assert!(matches!(
            session.on_event(&event(3, &payload, false)),
            Err(FixSeatError::Gap { .. })
        ));
        let _ = fs::remove_dir_all(&directory);
    }

    #[test]
    fn an_ack_waits_for_the_batch_or_the_interval() {
        let directory = directory();
        let mut session = FixSeatSession::new(config(&directory), 1, "AAPL");
        session.on_seat_bound(bound()).expect("bind");
        let payload = event_bytes(ExecType::New);
        session.on_event(&event(1, &payload, false)).expect("event");
        assert!(session.ack_due(1).expect("ack").is_none());
        std::thread::sleep(Duration::from_millis(51));
        assert!(session.ack_due(1).expect("ack").is_some());
        let _ = fs::remove_dir_all(&directory);
    }
}
