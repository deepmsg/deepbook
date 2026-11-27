//! The `DBS3` service snapshot envelope (ADR 0012 §4).
//!
//! `DBS3` wraps the untouched `DBS2` engine snapshot and appends the pinned
//! seats and their outboxes. The writer emits `DBS3`; the reader accepts the
//! legacy bare `DBS1`/`DBS2` and the optional `MDS1` market-data prefix in
//! `EngineService`, not here.
//!
//! The record is deterministic: seats are sorted by `seat_id`, entries stay in
//! `seat_seq` order, and every sequence invariant is checked on both encode and
//! decode. A corrupt record is an error, never a panic in the container.
#![forbid(unsafe_code)]

use deepbook_engine_core::ClientOrderId;
use deepbook_types::Seq;

use super::CodecError;
use super::dbs1::Reader;
use crate::envelope::{FLAG_REPLACE_REQ, FLAG_RESYNC};
use crate::outbox::{OutboxEntry, SeatSnapshot};

/// The record magic.
pub const MAGIC: [u8; 4] = *b"DBS3";
/// The envelope version.
pub const VERSION: u8 = 1;

const MAX_SEATS: usize = 1 << 16;
const MAX_ENTRIES: usize = 1 << 20;
const MAX_ENTRY_PAYLOAD: usize = 4096;
/// A seat's fixed fields plus `entry_count`: `seat_id` 2, `account_id` 4,
/// `epoch` 8, `generation` 8, `acked_seq` 8, `next_seq` 8, `overflowed` 1,
/// `entry_count` 4.
const MIN_SEAT_LEN: usize = 43;
/// An entry's fixed fields without the optional replace id: `seat_seq` 8,
/// `liquidity` 1, `payload_kind` 1, `flags` 1, `payload_len` 4.
const MIN_ENTRY_LEN: usize = 15;

/// One service snapshot: the engine state plus every pinned seat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceSnapshot {
    /// The exact `DBS2` (or legacy `DBS1`) engine snapshot bytes.
    pub engine_blob: Vec<u8>,
    /// The market-data sequence, when market data is enabled.
    pub md_seq: Option<Seq>,
    /// The pinned seats, in `seat_id` order when encoded.
    pub seats: Vec<SeatSnapshot>,
}

/// Encode a service snapshot as `DBS3`.
///
/// # Errors
///
/// [`CodecError`] for an inconsistent seat, a duplicate seat id, or a length
/// that does not fit the record.
pub fn encode_service(snapshot: &ServiceSnapshot) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(u8::from(snapshot.md_seq.is_some()));
    out.extend_from_slice(&snapshot.md_seq.unwrap_or(0).to_le_bytes());

    let engine_len = u64::try_from(snapshot.engine_blob.len()).map_err(|_| CodecError::TooLarge)?;
    out.extend_from_slice(&engine_len.to_le_bytes());
    out.extend_from_slice(&snapshot.engine_blob);

    let mut seats: Vec<&SeatSnapshot> = snapshot.seats.iter().collect();
    seats.sort_by_key(|seat| seat.seat_id);
    let seat_count = u32::try_from(seats.len()).map_err(|_| CodecError::TooLarge)?;
    out.extend_from_slice(&seat_count.to_le_bytes());

    let mut previous: Option<u16> = None;
    for seat in seats {
        if previous == Some(seat.seat_id) {
            return Err(CodecError::Inconsistent);
        }
        previous = Some(seat.seat_id);
        seat.validate().map_err(|_| CodecError::Inconsistent)?;

        out.extend_from_slice(&seat.seat_id.to_le_bytes());
        out.extend_from_slice(&seat.account_id.to_le_bytes());
        out.extend_from_slice(&seat.epoch.to_le_bytes());
        out.extend_from_slice(&seat.generation.to_le_bytes());
        out.extend_from_slice(&seat.acked_seq.to_le_bytes());
        out.extend_from_slice(&seat.next_seq.to_le_bytes());
        out.push(u8::from(seat.overflowed));
        let entry_count = u32::try_from(seat.entries.len()).map_err(|_| CodecError::TooLarge)?;
        out.extend_from_slice(&entry_count.to_le_bytes());

        for entry in &seat.entries {
            entry.validate().map_err(|_| CodecError::Inconsistent)?;
            out.extend_from_slice(&entry.seat_seq.to_le_bytes());
            out.push(entry.liquidity);
            out.push(entry.payload_kind);
            out.push(entry.flags);
            if let Some(id) = entry.replace_req_cl_ord_id {
                out.extend_from_slice(id.as_bytes());
            }
            let payload_len =
                u32::try_from(entry.payload.len()).map_err(|_| CodecError::TooLarge)?;
            out.extend_from_slice(&payload_len.to_le_bytes());
            out.extend_from_slice(&entry.payload);
        }
    }
    Ok(out)
}

/// Decode a `DBS3` record.
///
/// # Errors
///
/// [`CodecError`] for a short, unknown or internally inconsistent record.
pub fn decode_service(bytes: &[u8]) -> Result<ServiceSnapshot, CodecError> {
    let mut reader = Reader::new(bytes);
    if reader.take(MAGIC.len())? != MAGIC {
        return Err(CodecError::BadMagic);
    }
    let version = reader.u8()?;
    if version != VERSION {
        return Err(CodecError::BadVersion(version));
    }
    let flags = reader.u8()?;
    if flags & !1 != 0 {
        return Err(CodecError::BadEnum(flags));
    }
    let md = reader.u64()?;
    if flags & 1 == 0 && md != 0 {
        return Err(CodecError::Inconsistent);
    }
    let md_seq = (flags & 1 != 0).then_some(md);

    let engine_len = usize::try_from(reader.u64()?).map_err(|_| CodecError::TooLarge)?;
    let engine_blob = reader.take(engine_len)?.to_vec();

    let seat_count = reader.u32()?;
    if seat_count as usize > MAX_SEATS || seat_count as usize > reader.remaining() / MIN_SEAT_LEN {
        return Err(CodecError::BadCount(seat_count));
    }
    let mut seats = Vec::with_capacity(seat_count as usize);
    let mut previous: Option<u16> = None;
    for _ in 0..seat_count {
        let seat_id = reader.u16()?;
        if previous.is_some_and(|previous| seat_id <= previous) {
            return Err(CodecError::Inconsistent);
        }
        previous = Some(seat_id);
        let account_id = reader.u32()?;
        let epoch = reader.u64()?;
        let generation = reader.u64()?;
        let acked_seq = reader.u64()?;
        let next_seq = reader.u64()?;
        let overflowed = match reader.u8()? {
            0 => false,
            1 => true,
            other => return Err(CodecError::BadEnum(other)),
        };
        let entry_count = reader.u32()?;
        if entry_count as usize > MAX_ENTRIES
            || entry_count as usize > reader.remaining() / MIN_ENTRY_LEN
        {
            return Err(CodecError::BadCount(entry_count));
        }
        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let seat_seq = reader.u64()?;
            let liquidity = reader.u8()?;
            let payload_kind = reader.u8()?;
            let entry_flags = reader.u8()?;
            if entry_flags & !(FLAG_RESYNC | FLAG_REPLACE_REQ) != 0 {
                return Err(CodecError::BadEnum(entry_flags));
            }
            let replace_req_cl_ord_id = if entry_flags & FLAG_REPLACE_REQ != 0 {
                let bytes: [u8; ClientOrderId::LENGTH] = reader
                    .take(ClientOrderId::LENGTH)?
                    .try_into()
                    .map_err(|_| CodecError::Truncated)?;
                Some(ClientOrderId::from_padded_bytes(bytes))
            } else {
                None
            };
            let payload_len = reader.u32()?;
            if payload_len as usize > MAX_ENTRY_PAYLOAD {
                return Err(CodecError::BadCount(payload_len));
            }
            let payload = reader.take(payload_len as usize)?.to_vec();
            entries.push(OutboxEntry {
                seat_seq,
                liquidity,
                payload_kind,
                flags: entry_flags,
                replace_req_cl_ord_id,
                payload,
            });
        }
        let seat = SeatSnapshot {
            seat_id,
            account_id,
            epoch,
            generation,
            acked_seq,
            next_seq,
            overflowed,
            entries,
        };
        seat.validate().map_err(|_| CodecError::Inconsistent)?;
        seats.push(seat);
    }
    if !reader.is_empty() {
        return Err(CodecError::Inconsistent);
    }
    Ok(ServiceSnapshot {
        engine_blob,
        md_seq,
        seats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outbox::{MAX_SEAT_OUTBOX_BYTES, SeatOutbox};

    fn client_order_id() -> ClientOrderId {
        ClientOrderId::from_padded_bytes(*b"00000000000000000042")
    }

    fn event(byte: u8) -> Vec<u8> {
        vec![byte; 106]
    }

    fn populated() -> ServiceSnapshot {
        let mut first = SeatOutbox::new(7, 11);
        first.set_epoch(3);
        first.push_engine_event(0, false, None, event(1));
        first.push_engine_event(2, true, Some(client_order_id()), event(2));
        first.push_malformed_reject(0, 5, true, 101);
        first.ack(3);

        let mut second = SeatOutbox::new(9, 12);
        second.set_epoch(4);
        second.push_engine_event(0, false, None, event(3));
        second.overflow();

        ServiceSnapshot {
            engine_blob: b"DBS2nested".to_vec(),
            md_seq: Some(42),
            seats: vec![first.snapshot(), second.snapshot()],
        }
    }

    #[test]
    fn header_is_exact() {
        let snapshot = ServiceSnapshot {
            engine_blob: b"DBS2x".to_vec(),
            md_seq: Some(42),
            seats: Vec::new(),
        };
        let bytes = encode_service(&snapshot).expect("encode");
        assert_eq!(&bytes[..4], b"DBS3");
        assert_eq!(bytes[4], VERSION);
        assert_eq!(bytes[5], 1);
        assert_eq!(&bytes[6..14], &42_u64.to_le_bytes());
        assert_eq!(&bytes[14..22], &5_u64.to_le_bytes());
        assert_eq!(&bytes[22..27], b"DBS2x");
        assert_eq!(&bytes[27..31], &0_u32.to_le_bytes());
    }

    #[test]
    fn round_trips_every_payload_kind() {
        let snapshot = populated();
        let bytes = encode_service(&snapshot).expect("encode");
        assert_eq!(decode_service(&bytes), Ok(snapshot));
    }

    #[test]
    fn round_trips_an_empty_snapshot() {
        let snapshot = ServiceSnapshot {
            engine_blob: b"DBS1x".to_vec(),
            md_seq: None,
            seats: Vec::new(),
        };
        let bytes = encode_service(&snapshot).expect("encode");
        assert_eq!(decode_service(&bytes), Ok(snapshot));
    }

    #[test]
    fn seats_are_sorted_and_restorable() {
        let mut first = SeatOutbox::new(9, 12);
        first.push_engine_event(0, false, None, event(1));
        let mut second = SeatOutbox::new(7, 11);
        second.push_engine_event(0, false, None, event(2));
        let snapshot = ServiceSnapshot {
            engine_blob: b"DBS2".to_vec(),
            md_seq: None,
            seats: vec![first.snapshot(), second.snapshot()],
        };
        let bytes = encode_service(&snapshot).expect("encode");
        let decoded = decode_service(&bytes).expect("decode");
        assert_eq!(
            decoded
                .seats
                .iter()
                .map(|seat| seat.seat_id)
                .collect::<Vec<_>>(),
            vec![7, 9]
        );
        for (original, restored) in [(&first, &decoded.seats[1]), (&second, &decoded.seats[0])] {
            let restored = SeatOutbox::from_snapshot(restored.clone()).expect("restore");
            assert_eq!(&restored, original);
        }
    }

    #[test]
    fn duplicate_seats_are_refused() {
        let seat = SeatOutbox::new(7, 11).snapshot();
        let snapshot = ServiceSnapshot {
            engine_blob: Vec::new(),
            md_seq: None,
            seats: vec![seat.clone(), seat],
        };
        assert_eq!(encode_service(&snapshot), Err(CodecError::Inconsistent));
    }

    #[test]
    fn rejects_bad_headers_and_trailing_bytes() {
        let bytes = encode_service(&populated()).expect("encode");

        let mut bad = bytes.clone();
        bad[..4].copy_from_slice(b"DBS4");
        assert_eq!(decode_service(&bad), Err(CodecError::BadMagic));

        let mut bad = bytes.clone();
        bad[4] = 2;
        assert_eq!(decode_service(&bad), Err(CodecError::BadVersion(2)));

        let mut bad = bytes.clone();
        bad[5] = 2;
        assert_eq!(decode_service(&bad), Err(CodecError::BadEnum(2)));

        assert_eq!(decode_service(&bytes[..8]), Err(CodecError::Truncated));

        let mut bad = bytes;
        bad.push(0);
        assert_eq!(decode_service(&bad), Err(CodecError::Inconsistent));
    }

    #[test]
    fn rejects_missing_market_sequence_with_a_value() {
        let snapshot = ServiceSnapshot {
            engine_blob: Vec::new(),
            md_seq: None,
            seats: Vec::new(),
        };
        let mut bytes = encode_service(&snapshot).expect("encode");
        bytes[6..14].copy_from_slice(&7_u64.to_le_bytes());
        assert_eq!(decode_service(&bytes), Err(CodecError::Inconsistent));
    }

    #[test]
    fn rejects_corrupt_entries() {
        // One seat, one entry: the entry fields are easy to locate.
        let mut seat = SeatOutbox::new(7, 11);
        seat.push_engine_event(0, false, None, event(1));
        let snapshot = ServiceSnapshot {
            engine_blob: Vec::new(),
            md_seq: None,
            seats: vec![seat.snapshot()],
        };
        let bytes = encode_service(&snapshot).expect("encode");

        // Header 22, seat count 4, seat 43, entry count 4, seat_seq 8.
        let seat = 22 + 4;
        let overflowed = seat + 2 + 4 + 8 + 8 + 8 + 8;
        let entry = seat + 43;
        let kind = entry + 8 + 1;
        let mut bad = bytes.clone();
        bad[kind] = 2;
        assert_eq!(decode_service(&bad), Err(CodecError::Inconsistent));

        let mut bad = bytes.clone();
        bad[entry..entry + 8].copy_from_slice(&2_u64.to_le_bytes());
        assert_eq!(decode_service(&bad), Err(CodecError::Inconsistent));

        let mut bad = bytes;
        bad[overflowed] = 2;
        assert_eq!(decode_service(&bad), Err(CodecError::BadEnum(2)));
    }

    #[test]
    fn resync_batch_is_under_the_seat_cap() {
        let mut seat = SeatOutbox::new(7, 11);
        seat.begin_resync();
        for _ in 0..4096 {
            seat.push_engine_event(0, true, None, event(0));
        }
        assert!(seat.buffered_bytes() < MAX_SEAT_OUTBOX_BYTES);
        let snapshot = SeatSnapshot {
            seat_id: 7,
            account_id: 11,
            epoch: 0,
            generation: 2,
            acked_seq: 0,
            next_seq: 4097,
            overflowed: false,
            entries: seat.entries().cloned().collect(),
        };
        let bytes = encode_service(&ServiceSnapshot {
            engine_blob: Vec::new(),
            md_seq: None,
            seats: vec![snapshot],
        })
        .expect("encode");
        let decoded = decode_service(&bytes).expect("decode");
        assert_eq!(decoded.seats[0].entries.len(), 4096);
    }
}
