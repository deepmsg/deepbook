//! The per-seat outbox (ADR 0012 §2.3–§3.6).
//!
//! `EngineService` assigns every outbound order event a `seat_seq` and keeps
//! the encoded event in a per-seat ring until the owning gateway acknowledges
//! that it has durably journaled it. The payload is the protocol-neutral
//! `engine_core::wire` encoding, so one buffer feeds both FIX 4.4 and
//! DeepOUCH/1.0 without a lossy translation.
//!
//! This module is the pure state: sequence allocation, acknowledgement
//! clamping, the resync decisions, the budgets and the deterministic eviction
//! order. It performs no I/O and knows nothing about sessions.
#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fmt;

use deepbook_engine_core::ClientOrderId;
use deepbook_types::AccountId;

use crate::envelope::{
    FLAG_REPLACE_REQ, FLAG_RESYNC, KIND_ENGINE_EVENT, KIND_MALFORMED_REJECT, MALFORMED_REJECT_LEN,
};

/// The per-seat outbox cap (ADR 0012 §3.6).
pub const MAX_SEAT_OUTBOX_BYTES: usize = 8 << 20;
/// The cap across every seat on one service (ADR 0012 §3.6).
pub const MAX_TOTAL_OUTBOX_BYTES: usize = 128 << 20;
/// The persisted bytes per entry before the payload: `seat_seq` 8, `liquidity`
/// 1, `payload_kind` 1, `flags` 1, `payload_len` 4, and three bytes of slack.
/// The optional 20-byte replace request is counted separately.
pub const ENTRY_OVERHEAD: usize = 18;

/// An outbox invariant failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxError {
    /// A snapshot has generation zero.
    GenerationZero,
    /// `next_seq` is zero.
    NextSeqZero,
    /// `acked_seq` is beyond the last allocated sequence.
    AckedSeqAhead,
    /// A non-overflowed outbox's entries are not contiguous from `acked_seq`.
    SeatSeqNotContiguous,
    /// A non-overflowed outbox's entries do not reach `next_seq - 1`.
    NextSeqMismatch,
    /// An overflowed outbox still holds entries.
    OverflowNotEmpty,
    /// The liquidity byte is not `0..=2`.
    BadLiquidity(u8),
    /// The payload kind is not [`KIND_ENGINE_EVENT`] or
    /// [`KIND_MALFORMED_REJECT`].
    BadPayloadKind(u8),
    /// A flag byte is not one this build knows, or its replace bit disagrees
    /// with the optional id.
    BadFlags(u8),
    /// A malformed reject is not exactly [`MALFORMED_REJECT_LEN`] bytes.
    BadPayloadLength,
    /// The payload is empty.
    EmptyPayload,
}

impl fmt::Display for OutboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationZero => write!(formatter, "outbox generation is zero"),
            Self::NextSeqZero => write!(formatter, "outbox next sequence is zero"),
            Self::AckedSeqAhead => write!(formatter, "outbox ack is ahead of next sequence"),
            Self::SeatSeqNotContiguous => write!(formatter, "outbox entries are not contiguous"),
            Self::NextSeqMismatch => write!(formatter, "outbox entries do not reach next sequence"),
            Self::OverflowNotEmpty => write!(formatter, "overflowed outbox still holds entries"),
            Self::BadLiquidity(value) => write!(formatter, "unknown liquidity {value}"),
            Self::BadPayloadKind(value) => write!(formatter, "unknown payload kind {value}"),
            Self::BadFlags(value) => write!(formatter, "unknown outbox flags {value:#04x}"),
            Self::BadPayloadLength => write!(formatter, "malformed reject has the wrong length"),
            Self::EmptyPayload => write!(formatter, "outbox payload is empty"),
        }
    }
}

impl std::error::Error for OutboxError {}

/// One encoded outbound event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxEntry {
    /// The outbound sequence.
    pub seat_seq: u64,
    /// `0 = None`, `1 = Maker`, `2 = Taker`.
    pub liquidity: u8,
    /// [`KIND_ENGINE_EVENT`] or [`KIND_MALFORMED_REJECT`].
    pub payload_kind: u8,
    /// [`FLAG_RESYNC`] and [`FLAG_REPLACE_REQ`].
    pub flags: u8,
    /// The replace request's own id, only on an `ExecType::Replaced` report.
    pub replace_req_cl_ord_id: Option<ClientOrderId>,
    /// The `engine_core::wire` event, or the 18-byte malformed reject.
    pub payload: Vec<u8>,
}

impl OutboxEntry {
    /// The bytes this entry contributes to [`SeatOutbox::buffered_bytes`].
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        ENTRY_OVERHEAD
            + self.payload.len()
            + if self.replace_req_cl_ord_id.is_some() {
                ClientOrderId::LENGTH
            } else {
                0
            }
    }

    /// Whether this entry is synthetic resync state.
    #[must_use]
    pub const fn is_resync(&self) -> bool {
        self.flags & FLAG_RESYNC != 0
    }

    /// Check the entry's own invariants.
    ///
    /// # Errors
    ///
    /// [`OutboxError`] for an out-of-range kind, flag or payload.
    pub fn validate(&self) -> Result<(), OutboxError> {
        if self.liquidity > 2 {
            return Err(OutboxError::BadLiquidity(self.liquidity));
        }
        if self.payload_kind > KIND_MALFORMED_REJECT {
            return Err(OutboxError::BadPayloadKind(self.payload_kind));
        }
        if self.flags & !(FLAG_RESYNC | FLAG_REPLACE_REQ) != 0 {
            return Err(OutboxError::BadFlags(self.flags));
        }
        if self.payload.is_empty() {
            return Err(OutboxError::EmptyPayload);
        }
        if self.payload_kind == KIND_MALFORMED_REJECT && self.payload.len() != MALFORMED_REJECT_LEN
        {
            return Err(OutboxError::BadPayloadLength);
        }
        if self.payload_kind == KIND_MALFORMED_REJECT && self.liquidity != 0 {
            return Err(OutboxError::BadLiquidity(self.liquidity));
        }
        if self.payload_kind == KIND_MALFORMED_REJECT && self.flags != 0 {
            return Err(OutboxError::BadFlags(self.flags));
        }
        if self.replace_req_cl_ord_id.is_some() != (self.flags & FLAG_REPLACE_REQ != 0) {
            return Err(OutboxError::BadFlags(self.flags));
        }
        if self.replace_req_cl_ord_id.is_some() && self.payload_kind != KIND_ENGINE_EVENT {
            return Err(OutboxError::BadPayloadKind(self.payload_kind));
        }
        Ok(())
    }
}

/// A seat's snapshot form, used by `DBS3`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeatSnapshot {
    /// The seat.
    pub seat_id: u16,
    /// The pinned account.
    pub account_id: AccountId,
    /// The seat epoch.
    pub epoch: u64,
    /// The journal generation.
    pub generation: u64,
    /// The highest acknowledged sequence.
    pub acked_seq: u64,
    /// The next sequence to allocate.
    pub next_seq: u64,
    /// Whether the outbox overflowed and dropped its entries.
    pub overflowed: bool,
    /// The retained entries, oldest first.
    pub entries: Vec<OutboxEntry>,
}

impl SeatSnapshot {
    /// Check the restored-state invariants.
    ///
    /// # Errors
    ///
    /// [`OutboxError`] when the sequences, flags or entries are inconsistent.
    pub fn validate(&self) -> Result<(), OutboxError> {
        if self.generation == 0 {
            return Err(OutboxError::GenerationZero);
        }
        if self.next_seq == 0 {
            return Err(OutboxError::NextSeqZero);
        }
        if self.acked_seq > self.next_seq.saturating_sub(1) {
            return Err(OutboxError::AckedSeqAhead);
        }
        for entry in &self.entries {
            entry.validate()?;
        }
        if self.overflowed {
            if !self.entries.is_empty() {
                return Err(OutboxError::OverflowNotEmpty);
            }
            return Ok(());
        }
        let mut expected = self.acked_seq.saturating_add(1);
        for entry in &self.entries {
            if entry.seat_seq != expected {
                return Err(OutboxError::SeatSeqNotContiguous);
            }
            expected = expected.saturating_add(1);
        }
        if expected != self.next_seq {
            return Err(OutboxError::NextSeqMismatch);
        }
        Ok(())
    }
}

/// One seat's retained outbound events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeatOutbox {
    seat_id: u16,
    account_id: AccountId,
    epoch: u64,
    generation: u64,
    acked_seq: u64,
    send_cursor: u64,
    next_seq: u64,
    buffered_bytes: usize,
    overflowed: bool,
    entries: VecDeque<OutboxEntry>,
}

impl SeatOutbox {
    /// A fresh seat: no entries, `next_seq = 1`, generation 1.
    #[must_use]
    pub fn new(seat_id: u16, account_id: AccountId) -> Self {
        Self {
            seat_id,
            account_id,
            epoch: 0,
            generation: 1,
            acked_seq: 0,
            send_cursor: 0,
            next_seq: 1,
            buffered_bytes: 0,
            overflowed: false,
            entries: VecDeque::new(),
        }
    }

    /// Rebuild from a validated snapshot.
    ///
    /// # Errors
    ///
    /// [`OutboxError`] when the snapshot violates the state invariants.
    pub fn from_snapshot(snapshot: SeatSnapshot) -> Result<Self, OutboxError> {
        snapshot.validate()?;
        let buffered_bytes = snapshot.entries.iter().map(OutboxEntry::encoded_len).sum();
        Ok(Self {
            seat_id: snapshot.seat_id,
            account_id: snapshot.account_id,
            epoch: snapshot.epoch,
            generation: snapshot.generation,
            acked_seq: snapshot.acked_seq,
            send_cursor: snapshot.acked_seq,
            next_seq: snapshot.next_seq,
            buffered_bytes,
            overflowed: snapshot.overflowed,
            entries: snapshot.entries.into(),
        })
    }

    /// The snapshot form.
    #[must_use]
    pub fn snapshot(&self) -> SeatSnapshot {
        SeatSnapshot {
            seat_id: self.seat_id,
            account_id: self.account_id,
            epoch: self.epoch,
            generation: self.generation,
            acked_seq: self.acked_seq,
            next_seq: self.next_seq,
            overflowed: self.overflowed,
            entries: self.entries.iter().cloned().collect(),
        }
    }

    /// The seat.
    #[must_use]
    pub const fn seat_id(&self) -> u16 {
        self.seat_id
    }

    /// The pinned account.
    #[must_use]
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// The seat epoch.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Set the epoch after a successful bind.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// The journal generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// The highest acknowledged sequence.
    #[must_use]
    pub const fn acked_seq(&self) -> u64 {
        self.acked_seq
    }

    /// The highest sequence offered to the current session.
    #[must_use]
    pub const fn send_cursor(&self) -> u64 {
        self.send_cursor
    }

    /// Rewind or clamp the send cursor (a bind or a leader change).
    pub fn set_send_cursor(&mut self, sequence: u64) {
        self.send_cursor = sequence.min(self.next_seq.saturating_sub(1));
    }

    /// Advance the send cursor after a successful `offer`.
    pub fn advance_send_cursor(&mut self, sequence: u64) {
        self.send_cursor = self
            .send_cursor
            .max(sequence.min(self.next_seq.saturating_sub(1)));
    }

    /// The next sequence to allocate.
    #[must_use]
    pub const fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// The bytes the retained entries occupy.
    #[must_use]
    pub const fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    /// Whether entries were dropped by an overflow or an eviction.
    #[must_use]
    pub const fn overflowed(&self) -> bool {
        self.overflowed
    }

    /// The retained entries, oldest first.
    pub fn entries(&self) -> impl Iterator<Item = &OutboxEntry> {
        self.entries.iter()
    }

    /// The retained entry with this sequence, when it is still buffered.
    ///
    /// Entries are contiguous, so the lookup is index arithmetic, not a scan
    /// from the front.
    pub fn entry_at(&self, seat_seq: u64) -> Option<&OutboxEntry> {
        let front = self.entries.front()?;
        let index = usize::try_from(seat_seq.checked_sub(front.seat_seq)?).ok()?;
        self.entries
            .get(index)
            .filter(|entry| entry.seat_seq == seat_seq)
    }

    /// How many entries are retained.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Whether the seat is above its own cap.
    #[must_use]
    pub const fn over_capacity(&self) -> bool {
        self.buffered_bytes > MAX_SEAT_OUTBOX_BYTES
    }

    /// The oldest sequence still available for replay.
    #[must_use]
    pub fn oldest_available_seq(&self) -> u64 {
        self.entries
            .front()
            .map_or(self.next_seq, |entry| entry.seat_seq)
    }

    /// Whether a bind must rotate the generation instead of replaying.
    #[must_use]
    pub fn needs_resync(&self, last_journal_seq: u64) -> bool {
        if self.overflowed {
            return true;
        }
        if last_journal_seq > self.next_seq.saturating_sub(1) {
            return true;
        }
        let effective = last_journal_seq.min(self.next_seq.saturating_sub(1));
        effective < self.oldest_available_seq().saturating_sub(1)
    }

    /// Append one engine event and return its sequence.
    pub fn push_engine_event(
        &mut self,
        liquidity: u8,
        resync: bool,
        replace_req_cl_ord_id: Option<ClientOrderId>,
        payload: Vec<u8>,
    ) -> u64 {
        let mut flags = 0_u8;
        if resync {
            flags |= FLAG_RESYNC;
        }
        if replace_req_cl_ord_id.is_some() {
            flags |= FLAG_REPLACE_REQ;
        }
        self.push(OutboxEntry {
            seat_seq: self.next_seq,
            liquidity,
            payload_kind: KIND_ENGINE_EVENT,
            flags,
            replace_req_cl_ord_id,
            payload,
        })
    }

    /// Append one service-level malformed reject and return its sequence.
    pub fn push_malformed_reject(
        &mut self,
        raw_cl_ord_id: u64,
        raw_orig_cl_ord_id: u64,
        is_cancel_or_replace: bool,
        reason: u8,
    ) -> u64 {
        let mut payload = Vec::with_capacity(MALFORMED_REJECT_LEN);
        payload.extend_from_slice(&raw_cl_ord_id.to_le_bytes());
        payload.extend_from_slice(&raw_orig_cl_ord_id.to_le_bytes());
        payload.push(u8::from(is_cancel_or_replace));
        payload.push(reason);
        self.push(OutboxEntry {
            seat_seq: self.next_seq,
            liquidity: 0,
            payload_kind: KIND_MALFORMED_REJECT,
            flags: 0,
            replace_req_cl_ord_id: None,
            payload,
        })
    }

    fn push(&mut self, entry: OutboxEntry) -> u64 {
        let sequence = entry.seat_seq;
        debug_assert_eq!(sequence, self.next_seq);
        self.next_seq = sequence.saturating_add(1);
        // After an overflow the sequence keeps advancing, but the payload is
        // gone until the next generation's resync batch. Appending now would
        // leave a non-empty `overflowed` outbox, which is not a valid state.
        if self.overflowed {
            return sequence;
        }
        self.buffered_bytes = self.buffered_bytes.saturating_add(entry.encoded_len());
        self.entries.push_back(entry);
        sequence
    }

    /// Advance the acknowledgement, clamped to the last allocated sequence.
    ///
    /// Returns the effective acknowledgement. Entries at or below it are
    /// dropped. The clamp is not `send_cursor`: a new leader may legitimately
    /// receive an ack for records journaled under the previous leader.
    pub fn ack(&mut self, up_to_seq: u64) -> u64 {
        let effective = up_to_seq.min(self.next_seq.saturating_sub(1));
        if effective <= self.acked_seq {
            return self.acked_seq;
        }
        while let Some(front) = self.entries.front() {
            if front.seat_seq > effective {
                break;
            }
            self.buffered_bytes = self.buffered_bytes.saturating_sub(front.encoded_len());
            self.entries.pop_front();
        }
        self.acked_seq = effective;
        self.send_cursor = self.send_cursor.max(effective);
        self.acked_seq
    }

    /// Drop every entry and mark the seat as needing a resync.
    pub fn overflow(&mut self) {
        self.entries.clear();
        self.buffered_bytes = 0;
        self.overflowed = true;
    }

    /// Rotate the generation and prepare for a resync batch.
    ///
    /// Clears the retained entries (the gateway's replay window is gone),
    /// advances the acknowledgement to the last allocated sequence, and
    /// returns the first sequence the synthetic state will consume.
    pub fn begin_resync(&mut self) -> u64 {
        self.generation = self.generation.saturating_add(1);
        self.entries.clear();
        self.buffered_bytes = 0;
        self.acked_seq = self.next_seq.saturating_sub(1);
        self.send_cursor = self.acked_seq;
        self.overflowed = false;
        self.next_seq
    }
}

/// Sum the retained bytes across seats.
#[must_use]
pub fn total_buffered_bytes<'a>(seats: impl IntoIterator<Item = &'a SeatOutbox>) -> usize {
    seats.into_iter().fold(0_usize, |total, seat| {
        total.saturating_add(seat.buffered_bytes())
    })
}

/// The deterministic eviction order (ADR 0012 §3.6).
///
/// Seats are dropped largest-backlog first, ties broken by the lower
/// `seat_id`. Only log-derived state participates: `bound_session` is
/// transient and must never influence this choice.
#[must_use]
pub fn eviction_plan<'a>(
    seats: impl IntoIterator<Item = &'a SeatOutbox>,
    budget: usize,
) -> Vec<u16> {
    let mut candidates: Vec<&SeatOutbox> = seats
        .into_iter()
        .filter(|seat| seat.buffered_bytes() > 0)
        .collect();
    let mut total = candidates.iter().fold(0_usize, |sum, seat| {
        sum.saturating_add(seat.buffered_bytes())
    });
    candidates.sort_by(|left, right| {
        right
            .buffered_bytes()
            .cmp(&left.buffered_bytes())
            .then_with(|| left.seat_id().cmp(&right.seat_id()))
    });
    let mut victims = Vec::new();
    for seat in candidates {
        if total <= budget {
            break;
        }
        total = total.saturating_sub(seat.buffered_bytes());
        victims.push(seat.seat_id());
    }
    victims
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seat() -> SeatOutbox {
        SeatOutbox::new(7, 11)
    }

    fn event(byte: u8) -> Vec<u8> {
        vec![byte; 106]
    }

    fn client_order_id() -> ClientOrderId {
        ClientOrderId::from_padded_bytes(*b"00000000000000000042")
    }

    #[test]
    fn fresh_seat_does_not_resync() {
        let seat = seat();
        assert_eq!(seat.next_seq(), 1);
        assert_eq!(seat.oldest_available_seq(), 1);
        assert!(!seat.needs_resync(0));
        // A fresh seat has allocated nothing; claiming a journal beyond
        // `next_seq - 1` is invalid and must resync.
        assert!(seat.needs_resync(1));
    }

    #[test]
    fn replay_window_and_gap_detection() {
        let mut seat = seat();
        seat.push_engine_event(0, false, None, event(1));
        seat.push_engine_event(1, false, None, event(2));
        seat.push_engine_event(2, false, Some(client_order_id()), event(3));
        seat.ack(2);
        assert_eq!(seat.acked_seq(), 2);
        assert_eq!(seat.oldest_available_seq(), 3);
        assert_eq!(seat.entry_count(), 1);
        assert!(!seat.needs_resync(2));
        assert!(seat.needs_resync(1));
        assert!(!seat.needs_resync(3));
        assert!(seat.needs_resync(4));
    }

    #[test]
    fn ack_is_clamped_and_idempotent() {
        let mut seat = seat();
        seat.push_engine_event(0, false, None, event(1));
        seat.push_engine_event(0, false, None, event(2));
        assert_eq!(seat.ack(u64::MAX), 2);
        assert_eq!(seat.acked_seq(), 2);
        assert_eq!(seat.entry_count(), 0);
        assert_eq!(seat.ack(1), 2);
        assert!(!seat.needs_resync(2));
        assert!(seat.needs_resync(1));
    }

    #[test]
    fn overflow_then_resync_clears_and_rotates() {
        let mut seat = seat();
        seat.push_engine_event(0, false, None, event(1));
        seat.push_engine_event(0, false, None, event(2));
        seat.overflow();
        assert!(seat.overflowed());
        assert_eq!(seat.entry_count(), 0);
        assert_eq!(seat.next_seq(), 3);
        assert!(seat.needs_resync(0));

        let replay = seat.begin_resync();
        assert_eq!(replay, 3);
        assert_eq!(seat.generation(), 2);
        assert!(!seat.overflowed());
        assert_eq!(seat.acked_seq(), 2);
        assert_eq!(seat.send_cursor(), 2);
        assert_eq!(seat.entry_count(), 0);
        assert!(!seat.needs_resync(2));
    }

    #[test]
    fn resync_batch_fits_the_seat_budget() {
        let mut seat = seat();
        seat.begin_resync();
        for _ in 0..4096 {
            seat.push_engine_event(0, true, None, event(0));
        }
        assert!(!seat.over_capacity());
        assert_eq!(seat.entry_count(), 4096);
    }

    #[test]
    fn pushes_after_overflow_advance_the_sequence_only() {
        let mut seat = seat();
        seat.push_engine_event(0, false, None, event(1));
        seat.overflow();
        let next = seat.next_seq();
        seat.push_engine_event(0, false, None, event(2));
        seat.push_malformed_reject(0, 0, false, 101);
        assert_eq!(seat.next_seq(), next + 2);
        assert_eq!(seat.entry_count(), 0);
        assert_eq!(seat.buffered_bytes(), 0);
        assert!(seat.snapshot().validate().is_ok());
    }

    #[test]
    fn snapshot_round_trips() {
        let mut seat = seat();
        seat.set_epoch(9);
        seat.push_engine_event(0, false, None, event(1));
        seat.push_engine_event(2, true, Some(client_order_id()), event(2));
        seat.push_malformed_reject(0, 5, true, 101);
        seat.ack(1);
        let snapshot = seat.snapshot();
        assert_eq!(SeatOutbox::from_snapshot(snapshot), Ok(seat));
    }

    #[test]
    fn invalid_snapshots_are_refused() {
        let mut seat = seat();
        seat.push_engine_event(0, false, None, event(1));
        let mut snapshot = seat.snapshot();
        snapshot.entries[0].seat_seq = 2;
        assert_eq!(
            SeatOutbox::from_snapshot(snapshot),
            Err(OutboxError::SeatSeqNotContiguous)
        );

        let mut snapshot = seat.snapshot();
        snapshot.overflowed = true;
        assert_eq!(
            SeatOutbox::from_snapshot(snapshot),
            Err(OutboxError::OverflowNotEmpty)
        );

        let mut entry = OutboxEntry {
            seat_seq: 1,
            liquidity: 3,
            payload_kind: KIND_ENGINE_EVENT,
            flags: 0,
            replace_req_cl_ord_id: None,
            payload: event(0),
        };
        assert_eq!(entry.validate(), Err(OutboxError::BadLiquidity(3)));
        entry.liquidity = 0;
        entry.payload_kind = 2;
        assert_eq!(entry.validate(), Err(OutboxError::BadPayloadKind(2)));
        entry.payload_kind = KIND_ENGINE_EVENT;
        entry.flags = 4;
        assert_eq!(entry.validate(), Err(OutboxError::BadFlags(4)));
        entry.flags = FLAG_REPLACE_REQ;
        assert_eq!(
            entry.validate(),
            Err(OutboxError::BadFlags(FLAG_REPLACE_REQ))
        );
        entry.replace_req_cl_ord_id = Some(client_order_id());
        entry.payload.clear();
        assert_eq!(entry.validate(), Err(OutboxError::EmptyPayload));
    }

    #[test]
    fn eviction_is_deterministic() {
        let mut small = SeatOutbox::new(2, 2);
        small.push_engine_event(0, false, None, event(1));
        let mut large = SeatOutbox::new(1, 1);
        large.push_engine_event(0, false, None, event(1));
        large.push_engine_event(0, false, None, event(2));
        let empty = SeatOutbox::new(3, 3);
        let seats = [small.clone(), large.clone()];

        let budget = large.buffered_bytes() + small.buffered_bytes() - 1;
        assert_eq!(eviction_plan(seats.iter(), budget), vec![1]);

        let budget = 0;
        assert_eq!(eviction_plan(seats.iter(), budget), vec![1, 2]);
        assert!(eviction_plan([&empty].into_iter(), 0).is_empty());
        assert_eq!(
            total_buffered_bytes(seats.iter()),
            large.buffered_bytes() + small.buffered_bytes()
        );
    }
}
