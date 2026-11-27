//! Service envelopes between a gateway and `EngineService` (ADR 0012).
//!
//! One gateway keeps one cluster session and multiplexes every seat it hosts
//! over these envelopes. Ingress and egress share an 8-byte header; a legacy
//! bare [`deepbook_engine_core::wire`] command starts with its own version byte
//! and never with [`MAGIC`], so the service can tell the two encodings apart.
//!
//! All integers are little-endian. The envelope layer does not validate
//! business values beyond the shape of the record; `EngineService` owns the
//! seat/account/epoch checks.
#![forbid(unsafe_code)]

use std::fmt;

use deepbook_engine_core::ClientOrderId;

/// The first byte of every service envelope.
pub const MAGIC: u8 = 0xDB;
/// The envelope version.
pub const VERSION: u8 = 1;
/// Magic, version, kind, reserved, and a `u32` payload length.
pub const HEADER_LEN: usize = 8;

/// Ingress kinds.
pub mod ingress_kind {
    /// `BindSeat`.
    pub const BIND_SEAT: u8 = 1;
    /// `SeatCommand`.
    pub const SEAT_COMMAND: u8 = 2;
    /// `AckSeat`.
    pub const ACK_SEAT: u8 = 3;
    /// `RejectMalformed`.
    pub const REJECT_MALFORMED: u8 = 4;
    /// `UnbindSeat`.
    pub const UNBIND_SEAT: u8 = 5;
}

/// Egress kinds.
pub mod egress_kind {
    /// `SeatBound`.
    pub const SEAT_BOUND: u8 = 10;
    /// `BindRejected`.
    pub const BIND_REJECTED: u8 = 11;
    /// `SeatEvent`.
    pub const SEAT_EVENT: u8 = 12;
    /// `FenceEvicted`.
    pub const FENCE_EVICTED: u8 = 13;
}

/// `SeatEvent.flags` bit: the payload is synthetic resync state.
pub const FLAG_RESYNC: u8 = 0x01;
/// `SeatEvent.flags` bit: `replace_req_cl_ord_id` is present.
pub const FLAG_REPLACE_REQ: u8 = 0x02;
/// `SeatEvent.payload_kind`: an `engine_core::wire` event record.
pub const KIND_ENGINE_EVENT: u8 = 0;
/// `SeatEvent.payload_kind`: a service-level malformed reject.
pub const KIND_MALFORMED_REJECT: u8 = 1;
/// The byte length of the [`KIND_MALFORMED_REJECT`] payload.
pub const MALFORMED_REJECT_LEN: usize = 18;

/// An envelope encode/decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    /// The record ends before the header or a field it promised.
    Truncated,
    /// The first byte is not [`MAGIC`].
    BadMagic(u8),
    /// The version byte is not [`VERSION`].
    BadVersion(u8),
    /// The kind byte is not one this build knows.
    UnknownKind(u8),
    /// The reserved byte is not zero.
    NonZeroReserved(u8),
    /// The declared payload length disagrees with the bytes present.
    BadLength,
    /// An enum or flag byte is not one this build knows.
    BadEnum(u8),
    /// A length does not fit the envelope.
    TooLarge,
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(formatter, "truncated service envelope"),
            Self::BadMagic(byte) => write!(formatter, "not a service envelope (magic {byte:#04x})"),
            Self::BadVersion(version) => write!(formatter, "unknown envelope version {version}"),
            Self::UnknownKind(kind) => write!(formatter, "unknown envelope kind {kind}"),
            Self::NonZeroReserved(byte) => {
                write!(formatter, "reserved envelope byte is {byte:#04x}")
            }
            Self::BadLength => write!(formatter, "envelope payload length does not match"),
            Self::BadEnum(byte) => write!(formatter, "unknown envelope enum value {byte}"),
            Self::TooLarge => write!(formatter, "value does not fit the service envelope"),
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// `BindSeat`: a gateway claims a seat (ADR 0012 §2.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindSeat {
    /// The seat.
    pub seat_id: u16,
    /// The account the seat is pinned to.
    pub account_id: u32,
    /// The highest `seat_seq` this gateway has durably journaled; `0` is a
    /// fresh seat.
    pub last_journal_seq: u64,
}

/// `SeatCommand`: one `engine_core::wire` command on behalf of a seat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeatCommand<'a> {
    /// The seat.
    pub seat_id: u16,
    /// The seat epoch this command belongs to.
    pub epoch: u64,
    /// The wrapped `engine_core::wire::encode_command` bytes.
    pub command: &'a [u8],
}

/// `AckSeat`: the gateway has durably journaled up to this sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AckSeat {
    /// The seat.
    pub seat_id: u16,
    /// The seat epoch.
    pub epoch: u64,
    /// The highest `seat_seq` written to the seat journal.
    pub up_to_seq: u64,
}

/// `RejectMalformed`: a parsed frame that failed validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RejectMalformed {
    /// The seat.
    pub seat_id: u16,
    /// The seat epoch.
    pub epoch: u64,
    /// The request's raw `u64` id; `0` is representable here even though it is
    /// invalid on the wire.
    pub raw_cl_ord_id: u64,
    /// The raw original id for a cancel/replace; `0` otherwise.
    pub raw_orig_cl_ord_id: u64,
    /// Whether the request was a cancel or replace.
    pub is_cancel_or_replace: bool,
    /// A [`ServiceRejectReason`] byte.
    pub reason: u8,
}

/// `UnbindSeat`: the gateway has released a seat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnbindSeat {
    /// The seat.
    pub seat_id: u16,
    /// The seat epoch.
    pub epoch: u64,
    /// Why the seat was released ([`UnbindReason`]).
    pub reason: u8,
}

/// An ingress envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ingress<'a> {
    /// A seat bind.
    BindSeat(BindSeat),
    /// A wrapped engine command.
    SeatCommand(SeatCommand<'a>),
    /// A journal acknowledgement.
    AckSeat(AckSeat),
    /// A malformed-request reject.
    RejectMalformed(RejectMalformed),
    /// A seat release.
    UnbindSeat(UnbindSeat),
}

impl<'a> Ingress<'a> {
    /// Encode the envelope, header included.
    ///
    /// # Errors
    ///
    /// The payload or a length does not fit the format.
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        let mut out = Vec::new();
        match self {
            Self::BindSeat(message) => {
                put_header(&mut out, ingress_kind::BIND_SEAT, 14)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.account_id.to_le_bytes());
                out.extend_from_slice(&message.last_journal_seq.to_le_bytes());
            }
            Self::SeatCommand(message) => {
                let payload_len = 10_usize
                    .checked_add(message.command.len())
                    .ok_or(EnvelopeError::TooLarge)?;
                put_header(&mut out, ingress_kind::SEAT_COMMAND, payload_len)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.epoch.to_le_bytes());
                out.extend_from_slice(message.command);
            }
            Self::AckSeat(message) => {
                put_header(&mut out, ingress_kind::ACK_SEAT, 18)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.epoch.to_le_bytes());
                out.extend_from_slice(&message.up_to_seq.to_le_bytes());
            }
            Self::RejectMalformed(message) => {
                put_header(&mut out, ingress_kind::REJECT_MALFORMED, 28)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.epoch.to_le_bytes());
                out.extend_from_slice(&message.raw_cl_ord_id.to_le_bytes());
                out.extend_from_slice(&message.raw_orig_cl_ord_id.to_le_bytes());
                out.push(u8::from(message.is_cancel_or_replace));
                out.push(message.reason);
            }
            Self::UnbindSeat(message) => {
                put_header(&mut out, ingress_kind::UNBIND_SEAT, 11)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.epoch.to_le_bytes());
                out.push(message.reason);
            }
        }
        Ok(out)
    }

    /// Decode one complete ingress envelope.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError`] for a short, unknown or inconsistent record.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, EnvelopeError> {
        let (kind, payload) = split(bytes)?;
        match kind {
            ingress_kind::BIND_SEAT => {
                let mut reader = Reader::new(exact(payload, 14)?);
                Ok(Self::BindSeat(BindSeat {
                    seat_id: reader.u16()?,
                    account_id: reader.u32()?,
                    last_journal_seq: reader.u64()?,
                }))
            }
            ingress_kind::SEAT_COMMAND => {
                if payload.len() < 10 {
                    return Err(EnvelopeError::BadLength);
                }
                let mut reader = Reader::new(payload);
                let seat_id = reader.u16()?;
                let epoch = reader.u64()?;
                Ok(Self::SeatCommand(SeatCommand {
                    seat_id,
                    epoch,
                    command: &payload[10..],
                }))
            }
            ingress_kind::ACK_SEAT => {
                let mut reader = Reader::new(exact(payload, 18)?);
                Ok(Self::AckSeat(AckSeat {
                    seat_id: reader.u16()?,
                    epoch: reader.u64()?,
                    up_to_seq: reader.u64()?,
                }))
            }
            ingress_kind::REJECT_MALFORMED => {
                let mut reader = Reader::new(exact(payload, 28)?);
                let seat_id = reader.u16()?;
                let epoch = reader.u64()?;
                let raw_cl_ord_id = reader.u64()?;
                let raw_orig_cl_ord_id = reader.u64()?;
                let is_cancel_or_replace = match reader.u8()? {
                    0 => false,
                    1 => true,
                    other => return Err(EnvelopeError::BadEnum(other)),
                };
                let reason = reader.u8()?;
                Ok(Self::RejectMalformed(RejectMalformed {
                    seat_id,
                    epoch,
                    raw_cl_ord_id,
                    raw_orig_cl_ord_id,
                    is_cancel_or_replace,
                    reason,
                }))
            }
            ingress_kind::UNBIND_SEAT => {
                let mut reader = Reader::new(exact(payload, 11)?);
                Ok(Self::UnbindSeat(UnbindSeat {
                    seat_id: reader.u16()?,
                    epoch: reader.u64()?,
                    reason: reader.u8()?,
                }))
            }
            other => Err(EnvelopeError::UnknownKind(other)),
        }
    }
}

/// `SeatBound`: the bind applied (ADR 0012 §3.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeatBound {
    /// The seat.
    pub seat_id: u16,
    /// The epoch the gateway now owns.
    pub epoch: u64,
    /// The journal generation.
    pub generation: u64,
    /// The first sequence the gateway should receive.
    pub replay_from_seq: u64,
    /// The next sequence the service will allocate.
    pub next_seq: u64,
    /// Whether a resync batch was generated.
    pub resynced: bool,
}

/// `BindRejected`: the bind was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindRejected {
    /// The seat.
    pub seat_id: u16,
    /// A [`BindRejectReason`] byte.
    pub reason: u8,
}

/// `SeatEvent`: one outbox entry delivered to the bound gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeatEvent<'a> {
    /// The seat.
    pub seat_id: u16,
    /// The seat epoch the event belongs to.
    pub epoch: u64,
    /// The journal generation.
    pub generation: u64,
    /// The outbound sequence.
    pub seat_seq: u64,
    /// `0 = None`, `1 = Maker`, `2 = Taker`.
    pub liquidity: u8,
    /// [`KIND_ENGINE_EVENT`] or [`KIND_MALFORMED_REJECT`].
    pub payload_kind: u8,
    /// Whether this is synthetic resync state.
    pub resync: bool,
    /// The replace request's own id, on an `ExecType::Replaced` report.
    pub replace_req_cl_ord_id: Option<ClientOrderId>,
    /// The engine event or the 18-byte malformed reject.
    pub payload: &'a [u8],
}

/// `FenceEvicted`: a newer bind took the seat.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceEvicted {
    /// The seat.
    pub seat_id: u16,
    /// The epoch of the gateway that took the seat.
    pub new_epoch: u64,
    /// A [`FenceReason`] byte.
    pub reason: u8,
}

/// An egress envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Egress<'a> {
    /// The bind applied.
    SeatBound(SeatBound),
    /// The bind was refused.
    BindRejected(BindRejected),
    /// One outbox entry.
    SeatEvent(SeatEvent<'a>),
    /// A newer bind took the seat.
    FenceEvicted(FenceEvicted),
}

impl<'a> Egress<'a> {
    /// Encode the envelope, header included.
    ///
    /// # Errors
    ///
    /// The payload or a length does not fit the format.
    pub fn encode(&self) -> Result<Vec<u8>, EnvelopeError> {
        let mut out = Vec::new();
        match self {
            Self::SeatBound(message) => {
                put_header(&mut out, egress_kind::SEAT_BOUND, 35)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.epoch.to_le_bytes());
                out.extend_from_slice(&message.generation.to_le_bytes());
                out.extend_from_slice(&message.replay_from_seq.to_le_bytes());
                out.extend_from_slice(&message.next_seq.to_le_bytes());
                out.push(u8::from(message.resynced));
            }
            Self::BindRejected(message) => {
                put_header(&mut out, egress_kind::BIND_REJECTED, 3)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.push(message.reason);
            }
            Self::SeatEvent(message) => {
                if message.liquidity > 2 {
                    return Err(EnvelopeError::BadEnum(message.liquidity));
                }
                if message.payload_kind > KIND_MALFORMED_REJECT {
                    return Err(EnvelopeError::BadEnum(message.payload_kind));
                }
                if message.payload.is_empty() {
                    return Err(EnvelopeError::BadLength);
                }
                if message.payload_kind == KIND_MALFORMED_REJECT
                    && message.payload.len() != MALFORMED_REJECT_LEN
                {
                    return Err(EnvelopeError::BadLength);
                }
                if message.replace_req_cl_ord_id.is_some()
                    && message.payload_kind != KIND_ENGINE_EVENT
                {
                    return Err(EnvelopeError::BadEnum(message.payload_kind));
                }
                let mut flags = 0_u8;
                if message.resync {
                    flags |= FLAG_RESYNC;
                }
                if message.replace_req_cl_ord_id.is_some() {
                    flags |= FLAG_REPLACE_REQ;
                }
                if message.payload_kind == KIND_MALFORMED_REJECT {
                    if flags != 0 {
                        return Err(EnvelopeError::BadEnum(flags));
                    }
                    if message.liquidity != 0 {
                        return Err(EnvelopeError::BadEnum(message.liquidity));
                    }
                }
                let optional = usize::from(message.replace_req_cl_ord_id.is_some()) * 20;
                let payload_len =
                    u32::try_from(message.payload.len()).map_err(|_| EnvelopeError::TooLarge)?;
                let body_len = 29_usize
                    .checked_add(optional)
                    .and_then(|length| length.checked_add(4))
                    .and_then(|length| length.checked_add(message.payload.len()))
                    .ok_or(EnvelopeError::TooLarge)?;
                put_header(&mut out, egress_kind::SEAT_EVENT, body_len)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.epoch.to_le_bytes());
                out.extend_from_slice(&message.generation.to_le_bytes());
                out.extend_from_slice(&message.seat_seq.to_le_bytes());
                out.push(message.liquidity);
                out.push(message.payload_kind);
                out.push(flags);
                if let Some(id) = message.replace_req_cl_ord_id {
                    out.extend_from_slice(id.as_bytes());
                }
                out.extend_from_slice(&payload_len.to_le_bytes());
                out.extend_from_slice(message.payload);
            }
            Self::FenceEvicted(message) => {
                put_header(&mut out, egress_kind::FENCE_EVICTED, 11)?;
                out.extend_from_slice(&message.seat_id.to_le_bytes());
                out.extend_from_slice(&message.new_epoch.to_le_bytes());
                out.push(message.reason);
            }
        }
        Ok(out)
    }

    /// Decode one complete egress envelope.
    ///
    /// # Errors
    ///
    /// [`EnvelopeError`] for a short, unknown or inconsistent record.
    pub fn decode(bytes: &'a [u8]) -> Result<Self, EnvelopeError> {
        let (kind, payload) = split(bytes)?;
        match kind {
            egress_kind::SEAT_BOUND => {
                let mut reader = Reader::new(exact(payload, 35)?);
                let seat_id = reader.u16()?;
                let epoch = reader.u64()?;
                let generation = reader.u64()?;
                let replay_from_seq = reader.u64()?;
                let next_seq = reader.u64()?;
                let resynced = match reader.u8()? {
                    0 => false,
                    1 => true,
                    other => return Err(EnvelopeError::BadEnum(other)),
                };
                Ok(Self::SeatBound(SeatBound {
                    seat_id,
                    epoch,
                    generation,
                    replay_from_seq,
                    next_seq,
                    resynced,
                }))
            }
            egress_kind::BIND_REJECTED => {
                let mut reader = Reader::new(exact(payload, 3)?);
                Ok(Self::BindRejected(BindRejected {
                    seat_id: reader.u16()?,
                    reason: reader.u8()?,
                }))
            }
            egress_kind::SEAT_EVENT => {
                let mut reader = Reader::new(payload);
                let seat_id = reader.u16()?;
                let epoch = reader.u64()?;
                let generation = reader.u64()?;
                let seat_seq = reader.u64()?;
                let liquidity = reader.u8()?;
                if liquidity > 2 {
                    return Err(EnvelopeError::BadEnum(liquidity));
                }
                let payload_kind = reader.u8()?;
                if payload_kind > KIND_MALFORMED_REJECT {
                    return Err(EnvelopeError::BadEnum(payload_kind));
                }
                let flags = reader.u8()?;
                if flags & !(FLAG_RESYNC | FLAG_REPLACE_REQ) != 0 {
                    return Err(EnvelopeError::BadEnum(flags));
                }
                if payload_kind == KIND_MALFORMED_REJECT {
                    if flags != 0 {
                        return Err(EnvelopeError::BadEnum(flags));
                    }
                    if liquidity != 0 {
                        return Err(EnvelopeError::BadEnum(liquidity));
                    }
                }
                let replace_req_cl_ord_id = if flags & FLAG_REPLACE_REQ != 0 {
                    let bytes: [u8; ClientOrderId::LENGTH] = reader
                        .take(ClientOrderId::LENGTH)?
                        .try_into()
                        .map_err(|_| EnvelopeError::Truncated)?;
                    Some(ClientOrderId::from_padded_bytes(bytes))
                } else {
                    None
                };
                let declared =
                    usize::try_from(reader.u32()?).map_err(|_| EnvelopeError::TooLarge)?;
                if declared != reader.remaining() {
                    return Err(EnvelopeError::BadLength);
                }
                let event_payload = reader.take(declared)?;
                if event_payload.is_empty() {
                    return Err(EnvelopeError::BadLength);
                }
                if payload_kind == KIND_MALFORMED_REJECT
                    && event_payload.len() != MALFORMED_REJECT_LEN
                {
                    return Err(EnvelopeError::BadLength);
                }
                if replace_req_cl_ord_id.is_some() && payload_kind != KIND_ENGINE_EVENT {
                    return Err(EnvelopeError::BadEnum(payload_kind));
                }
                Ok(Self::SeatEvent(SeatEvent {
                    seat_id,
                    epoch,
                    generation,
                    seat_seq,
                    liquidity,
                    payload_kind,
                    resync: flags & FLAG_RESYNC != 0,
                    replace_req_cl_ord_id,
                    payload: event_payload,
                }))
            }
            egress_kind::FENCE_EVICTED => {
                let mut reader = Reader::new(exact(payload, 11)?);
                Ok(Self::FenceEvicted(FenceEvicted {
                    seat_id: reader.u16()?,
                    new_epoch: reader.u64()?,
                    reason: reader.u8()?,
                }))
            }
            other => Err(EnvelopeError::UnknownKind(other)),
        }
    }
}

fn put_header(out: &mut Vec<u8>, kind: u8, payload_len: usize) -> Result<(), EnvelopeError> {
    let payload_len = u32::try_from(payload_len).map_err(|_| EnvelopeError::TooLarge)?;
    out.push(MAGIC);
    out.push(VERSION);
    out.push(kind);
    out.push(0);
    out.extend_from_slice(&payload_len.to_le_bytes());
    Ok(())
}

fn split(bytes: &[u8]) -> Result<(u8, &[u8]), EnvelopeError> {
    let header = bytes.get(..HEADER_LEN).ok_or(EnvelopeError::Truncated)?;
    if header[0] != MAGIC {
        return Err(EnvelopeError::BadMagic(header[0]));
    }
    if header[1] != VERSION {
        return Err(EnvelopeError::BadVersion(header[1]));
    }
    if header[3] != 0 {
        return Err(EnvelopeError::NonZeroReserved(header[3]));
    }
    let kind = header[2];
    let declared = u32::from_le_bytes(header[4..8].try_into().expect("four")) as usize;
    let payload = &bytes[HEADER_LEN..];
    if declared != payload.len() {
        return Err(EnvelopeError::BadLength);
    }
    Ok((kind, payload))
}

fn exact(payload: &[u8], length: usize) -> Result<&[u8], EnvelopeError> {
    if payload.len() == length {
        Ok(payload)
    } else {
        Err(EnvelopeError::BadLength)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], EnvelopeError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(EnvelopeError::Truncated)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(EnvelopeError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, EnvelopeError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, EnvelopeError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("two")))
    }

    fn u32(&mut self) -> Result<u32, EnvelopeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("four")))
    }

    fn u64(&mut self) -> Result<u64, EnvelopeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("eight")))
    }
}

/// [`RejectMalformed::reason`] values owned by the service layer.
pub mod service_reject_reason {
    /// `cl_ord_id` was zero or unparsable.
    pub const ZERO_OR_INVALID_CL_ORD_ID: u8 = 101;
    /// The quantity was zero or exceeded `i64::MAX`.
    pub const QTY_OVERFLOW_OR_ZERO: u8 = 102;
    /// A market order carried a non-zero price, or a limit order had no price.
    pub const INVALID_WIRE_PRICE: u8 = 103;
    /// An enum byte was not one this build knows.
    pub const INVALID_WIRE_ENUM: u8 = 104;
}

/// [`BindRejected::reason`] values.
pub mod bind_reject_reason {
    /// The seat is pinned to another account, or the account to another seat.
    pub const ACCOUNT_CONFLICT: u8 = 1;
    /// The seat is not in the shared configuration.
    pub const UNKNOWN_SEAT: u8 = 2;
    /// The seat id or account id is invalid.
    pub const INVALID_SEAT: u8 = 3;
}

/// [`UnbindSeat::reason`] values.
pub mod unbind_reason {
    /// The client closed the connection normally.
    pub const CLIENT_CLOSED: u8 = 0;
    /// The client stopped reading and was disconnected.
    pub const SLOW_CONSUMER: u8 = 1;
    /// The operator released the seat.
    pub const OPERATOR: u8 = 2;
}

/// [`FenceEvicted::reason`] values.
pub mod fence_reason {
    /// A newer `BindSeat` took the seat.
    pub const SUPERSEDED: u8 = 1;
    /// The outbox overflowed while the seat was bound.
    pub const OUTBOX_OVERFLOW: u8 = 2;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_order_id() -> ClientOrderId {
        ClientOrderId::from_padded_bytes(*b"00000000000000000042")
    }

    fn event_payload() -> Vec<u8> {
        vec![7_u8; 106]
    }

    #[test]
    fn header_is_exact() {
        let bytes = Ingress::BindSeat(BindSeat {
            seat_id: 1,
            account_id: 2,
            last_journal_seq: 3,
        })
        .encode()
        .expect("encode");
        assert_eq!(&bytes[..8], &[0xDB, 1, 1, 0, 14, 0, 0, 0]);
        assert_eq!(&bytes[8..], &[1, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn ingress_round_trips() {
        let command = [1_u8, 2, 3, 4];
        let messages = [
            Ingress::BindSeat(BindSeat {
                seat_id: 9,
                account_id: 0x1234_5678,
                last_journal_seq: u64::MAX,
            }),
            Ingress::SeatCommand(SeatCommand {
                seat_id: 9,
                epoch: 7,
                command: &command,
            }),
            Ingress::AckSeat(AckSeat {
                seat_id: 9,
                epoch: 7,
                up_to_seq: 42,
            }),
            Ingress::RejectMalformed(RejectMalformed {
                seat_id: 9,
                epoch: 7,
                raw_cl_ord_id: 0,
                raw_orig_cl_ord_id: u64::MAX,
                is_cancel_or_replace: true,
                reason: service_reject_reason::ZERO_OR_INVALID_CL_ORD_ID,
            }),
            Ingress::UnbindSeat(UnbindSeat {
                seat_id: 9,
                epoch: 7,
                reason: unbind_reason::SLOW_CONSUMER,
            }),
        ];
        for message in messages {
            let bytes = message.encode().expect("encode");
            assert_eq!(Ingress::decode(&bytes), Ok(message));
        }
    }

    #[test]
    fn egress_round_trips() {
        let payload = event_payload();
        let without_replace = Egress::SeatEvent(SeatEvent {
            seat_id: 3,
            epoch: 5,
            generation: 2,
            seat_seq: 11,
            liquidity: 1,
            payload_kind: KIND_ENGINE_EVENT,
            resync: true,
            replace_req_cl_ord_id: None,
            payload: &payload,
        });
        let with_replace = Egress::SeatEvent(SeatEvent {
            seat_id: 3,
            epoch: 5,
            generation: 2,
            seat_seq: 12,
            liquidity: 0,
            payload_kind: KIND_ENGINE_EVENT,
            resync: false,
            replace_req_cl_ord_id: Some(client_order_id()),
            payload: &payload,
        });
        let malformed_payload = [0_u8; MALFORMED_REJECT_LEN];
        let malformed = Egress::SeatEvent(SeatEvent {
            seat_id: 3,
            epoch: 5,
            generation: 2,
            seat_seq: 13,
            liquidity: 0,
            payload_kind: KIND_MALFORMED_REJECT,
            resync: false,
            replace_req_cl_ord_id: None,
            payload: &malformed_payload,
        });
        let messages = [
            Egress::SeatBound(SeatBound {
                seat_id: 3,
                epoch: 5,
                generation: 2,
                replay_from_seq: 11,
                next_seq: 14,
                resynced: true,
            }),
            Egress::BindRejected(BindRejected {
                seat_id: 3,
                reason: bind_reject_reason::ACCOUNT_CONFLICT,
            }),
            without_replace,
            with_replace,
            malformed,
            Egress::FenceEvicted(FenceEvicted {
                seat_id: 3,
                new_epoch: 6,
                reason: fence_reason::SUPERSEDED,
            }),
        ];
        for message in messages {
            let bytes = message.encode().expect("encode");
            assert_eq!(Egress::decode(&bytes), Ok(message));
        }
    }

    #[test]
    fn seat_event_layout_is_stable() {
        let payload = event_payload();
        let message = Egress::SeatEvent(SeatEvent {
            seat_id: 0x0102,
            epoch: 0x0304_0506_0708_090A,
            generation: 2,
            seat_seq: 3,
            liquidity: 1,
            payload_kind: KIND_ENGINE_EVENT,
            resync: true,
            replace_req_cl_ord_id: Some(client_order_id()),
            payload: &payload,
        });
        let bytes = message.encode().expect("encode");
        assert_eq!(bytes[0], MAGIC);
        assert_eq!(bytes[2], egress_kind::SEAT_EVENT);
        let body = &bytes[HEADER_LEN..];
        assert_eq!(&body[..2], &[0x02, 0x01]);
        assert_eq!(&body[2..10], &0x0304_0506_0708_090Au64.to_le_bytes());
        assert_eq!(&body[10..18], &2_u64.to_le_bytes());
        assert_eq!(&body[18..26], &3_u64.to_le_bytes());
        assert_eq!(body[26], 1);
        assert_eq!(body[27], KIND_ENGINE_EVENT);
        assert_eq!(body[28], FLAG_RESYNC | FLAG_REPLACE_REQ);
        assert_eq!(&body[29..49], client_order_id().as_bytes());
        assert_eq!(&body[49..53], &(payload.len() as u32).to_le_bytes());
        assert_eq!(&body[53..], payload.as_slice());
    }

    #[test]
    fn rejects_bad_headers() {
        let good = Ingress::AckSeat(AckSeat {
            seat_id: 1,
            epoch: 2,
            up_to_seq: 3,
        })
        .encode()
        .expect("encode");

        assert_eq!(Ingress::decode(&[]), Err(EnvelopeError::Truncated));
        assert_eq!(Ingress::decode(&good[..4]), Err(EnvelopeError::Truncated));

        let mut bad = good.clone();
        bad[0] = 0;
        assert_eq!(Ingress::decode(&bad), Err(EnvelopeError::BadMagic(0)));

        let mut bad = good.clone();
        bad[1] = 2;
        assert_eq!(Ingress::decode(&bad), Err(EnvelopeError::BadVersion(2)));

        let mut bad = good.clone();
        bad[3] = 1;
        assert_eq!(
            Ingress::decode(&bad),
            Err(EnvelopeError::NonZeroReserved(1))
        );

        let mut bad = good.clone();
        bad[4] = 0;
        assert_eq!(Ingress::decode(&bad), Err(EnvelopeError::BadLength));

        let mut bad = good;
        bad[2] = 99;
        assert_eq!(Ingress::decode(&bad), Err(EnvelopeError::UnknownKind(99)));
    }

    #[test]
    fn rejects_bad_egress_values() {
        let payload = event_payload();
        let good = Egress::SeatEvent(SeatEvent {
            seat_id: 1,
            epoch: 2,
            generation: 3,
            seat_seq: 4,
            liquidity: 2,
            payload_kind: KIND_ENGINE_EVENT,
            resync: false,
            replace_req_cl_ord_id: None,
            payload: &payload,
        })
        .encode()
        .expect("encode");

        let mut bad = good.clone();
        bad[HEADER_LEN + 26] = 3;
        assert_eq!(Egress::decode(&bad), Err(EnvelopeError::BadEnum(3)));

        let mut bad = good.clone();
        bad[HEADER_LEN + 27] = 2;
        assert_eq!(Egress::decode(&bad), Err(EnvelopeError::BadEnum(2)));

        let mut bad = good.clone();
        bad[HEADER_LEN + 28] = 4;
        assert_eq!(Egress::decode(&bad), Err(EnvelopeError::BadEnum(4)));

        let mut bad = good;
        let length = u32::from_le_bytes(bad[HEADER_LEN + 29..HEADER_LEN + 33].try_into().unwrap());
        bad[HEADER_LEN + 29..HEADER_LEN + 33].copy_from_slice(&(length + 1).to_le_bytes());
        assert_eq!(Egress::decode(&bad), Err(EnvelopeError::BadLength));
    }

    #[test]
    fn malformed_reject_has_a_fixed_length() {
        let short = [0_u8; MALFORMED_REJECT_LEN - 1];
        let message = Egress::SeatEvent(SeatEvent {
            seat_id: 1,
            epoch: 2,
            generation: 3,
            seat_seq: 4,
            liquidity: 0,
            payload_kind: KIND_MALFORMED_REJECT,
            resync: false,
            replace_req_cl_ord_id: None,
            payload: &short,
        });
        assert_eq!(message.encode(), Err(EnvelopeError::BadLength));
    }
}
