//! The M3 wire frame for market data and recovery.
//!
//! Fixed little-endian fields, a one-byte version and a one-byte kind. The
//! design asks for SBE; the schema toolchain is not wired into this repository
//! yet, so this is the small internal frame M3 uses. It is versioned and
//! documented so SBE can replace it without changing the library's message
//! types. Trailing bytes are ignored, which lets a test pad a frame to one
//! datagram for the driver's loss generator.

use deepbook_types::Side;

use crate::message::{BookAction, Incremental, Level, RecoveryRequest, RecoveryResponse, Snapshot};

const VERSION: u8 = 1;
const MAX_LEVELS: usize = 1 << 20;
const MAX_INCREMENTALS: usize = 1 << 20;

const KIND_BOOK_UPDATE: u8 = 1;
const KIND_TRADE: u8 = 2;
const KIND_SNAPSHOT: u8 = 3;
const KIND_RECOVERY_REQUEST: u8 = 4;
const KIND_RECOVERY_SNAPSHOT: u8 = 5;
const KIND_RECOVERY_INCREMENTALS: u8 = 6;
const KIND_RECOVERY_UP_TO_DATE: u8 = 7;
const KIND_RECOVERY_UNKNOWN_SYMBOL: u8 = 8;

/// One frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WireMessage {
    /// An incremental.
    Incremental(Incremental),
    /// A broadcast snapshot.
    Snapshot(Snapshot),
    /// A recovery request.
    RecoveryRequest(RecoveryRequest),
    /// A recovery response.
    RecoveryResponse(RecoveryResponse),
}

/// A decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    /// Fewer bytes than the frame needs.
    Truncated,
    /// The first byte is not a version this build knows.
    BadVersion(u8),
    /// The kind byte is not one this build knows.
    UnknownKind(u8),
    /// A level or incremental count is implausibly large.
    TooMany(u32),
    /// A response carried a message of the wrong kind.
    WrongKind,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(formatter, "truncated market-data frame"),
            Self::BadVersion(version) => write!(formatter, "unknown wire version {version}"),
            Self::UnknownKind(kind) => write!(formatter, "unknown message kind {kind}"),
            Self::TooMany(count) => write!(formatter, "implausible item count {count}"),
            Self::WrongKind => write!(formatter, "response carried the wrong message kind"),
        }
    }
}

impl std::error::Error for WireError {}

/// Encode one frame.
#[must_use]
pub fn encode(message: &WireMessage) -> Vec<u8> {
    let mut out = Vec::with_capacity(32);
    out.push(VERSION);
    match message {
        WireMessage::Incremental(incremental) => {
            out.push(incremental_kind(*incremental));
            encode_incremental_fields(&mut out, *incremental);
        }
        WireMessage::Snapshot(snapshot) => {
            out.push(KIND_SNAPSHOT);
            encode_snapshot(&mut out, snapshot);
        }
        WireMessage::RecoveryRequest(request) => {
            out.push(KIND_RECOVERY_REQUEST);
            out.extend_from_slice(&request.symbol.to_le_bytes());
            out.extend_from_slice(&request.last_md_seq.to_le_bytes());
        }
        WireMessage::RecoveryResponse(response) => match response {
            RecoveryResponse::Snapshot(snapshot) => {
                out.push(KIND_RECOVERY_SNAPSHOT);
                encode_snapshot(&mut out, snapshot);
            }
            RecoveryResponse::Incrementals {
                symbol,
                from_md_seq,
                incrementals,
            } => {
                out.push(KIND_RECOVERY_INCREMENTALS);
                out.extend_from_slice(&symbol.to_le_bytes());
                out.extend_from_slice(&from_md_seq.to_le_bytes());
                out.extend_from_slice(
                    &u32::try_from(incrementals.len())
                        .unwrap_or(u32::MAX)
                        .to_le_bytes(),
                );
                for incremental in incrementals {
                    out.push(incremental_kind(*incremental));
                    out.push(VERSION);
                    encode_incremental_fields(&mut out, *incremental);
                }
            }
            RecoveryResponse::UpToDate { symbol, md_seq } => {
                out.push(KIND_RECOVERY_UP_TO_DATE);
                out.extend_from_slice(&symbol.to_le_bytes());
                out.extend_from_slice(&md_seq.to_le_bytes());
            }
            RecoveryResponse::UnknownSymbol { symbol } => {
                out.push(KIND_RECOVERY_UNKNOWN_SYMBOL);
                out.extend_from_slice(&symbol.to_le_bytes());
            }
        },
    }
    out
}

/// Decode one frame. Trailing bytes are ignored.
///
/// # Errors
///
/// [`WireError`] for a short, unknown or inconsistent frame.
pub fn decode(bytes: &[u8]) -> Result<WireMessage, WireError> {
    let mut reader = Reader::new(bytes);
    let version = reader.u8()?;
    if version != VERSION {
        return Err(WireError::BadVersion(version));
    }
    let kind = reader.u8()?;
    match kind {
        KIND_BOOK_UPDATE | KIND_TRADE => {
            let amount = decode_incremental(&mut reader, kind)?;
            Ok(WireMessage::Incremental(amount))
        }
        KIND_SNAPSHOT => Ok(WireMessage::Snapshot(decode_snapshot(&mut reader)?)),
        KIND_RECOVERY_REQUEST => Ok(WireMessage::RecoveryRequest(RecoveryRequest {
            symbol: reader.u16()?,
            last_md_seq: reader.u64()?,
        })),
        KIND_RECOVERY_SNAPSHOT => Ok(WireMessage::RecoveryResponse(RecoveryResponse::Snapshot(
            decode_snapshot(&mut reader)?,
        ))),
        KIND_RECOVERY_INCREMENTALS => {
            let symbol = reader.u16()?;
            let from_md_seq = reader.u64()?;
            let count = reader.u32()?;
            if usize::try_from(count).unwrap_or(usize::MAX) > MAX_INCREMENTALS {
                return Err(WireError::TooMany(count));
            }
            let mut incrementals = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let sub_kind = reader.u8()?;
                if sub_kind != KIND_BOOK_UPDATE && sub_kind != KIND_TRADE {
                    return Err(WireError::WrongKind);
                }
                let version = reader.u8()?;
                if version != VERSION {
                    return Err(WireError::BadVersion(version));
                }
                incrementals.push(decode_incremental(&mut reader, sub_kind)?);
            }
            Ok(WireMessage::RecoveryResponse(
                RecoveryResponse::Incrementals {
                    symbol,
                    from_md_seq,
                    incrementals,
                },
            ))
        }
        KIND_RECOVERY_UP_TO_DATE => Ok(WireMessage::RecoveryResponse(RecoveryResponse::UpToDate {
            symbol: reader.u16()?,
            md_seq: reader.u64()?,
        })),
        KIND_RECOVERY_UNKNOWN_SYMBOL => Ok(WireMessage::RecoveryResponse(
            RecoveryResponse::UnknownSymbol {
                symbol: reader.u16()?,
            },
        )),
        other => Err(WireError::UnknownKind(other)),
    }
}

fn incremental_kind(incremental: Incremental) -> u8 {
    match incremental {
        Incremental::BookUpdate { .. } => KIND_BOOK_UPDATE,
        Incremental::Trade { .. } => KIND_TRADE,
    }
}

fn encode_incremental_fields(out: &mut Vec<u8>, incremental: Incremental) {
    match incremental {
        Incremental::BookUpdate {
            md_seq,
            symbol,
            side,
            price,
            qty,
            action,
        } => {
            out.extend_from_slice(&md_seq.to_le_bytes());
            out.extend_from_slice(&symbol.to_le_bytes());
            out.push(side_byte(side));
            out.push(action_byte(action));
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
        Incremental::Trade {
            md_seq,
            symbol,
            price,
            qty,
            aggressor,
        } => {
            out.extend_from_slice(&md_seq.to_le_bytes());
            out.extend_from_slice(&symbol.to_le_bytes());
            out.push(side_byte(aggressor));
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
    }
}

fn decode_incremental(reader: &mut Reader<'_>, kind: u8) -> Result<Incremental, WireError> {
    let md_seq = reader.u64()?;
    let symbol = reader.u16()?;
    match kind {
        KIND_BOOK_UPDATE => {
            let side = side_from(reader.u8()?)?;
            let action = action_from(reader.u8()?)?;
            let price = reader.i64()?;
            let qty = reader.i64()?;
            Ok(Incremental::BookUpdate {
                md_seq,
                symbol,
                side,
                price,
                qty,
                action,
            })
        }
        KIND_TRADE => {
            let aggressor = side_from(reader.u8()?)?;
            let price = reader.i64()?;
            let qty = reader.i64()?;
            Ok(Incremental::Trade {
                md_seq,
                symbol,
                price,
                qty,
                aggressor,
            })
        }
        _ => Err(WireError::WrongKind),
    }
}

fn encode_snapshot(out: &mut Vec<u8>, snapshot: &Snapshot) {
    out.extend_from_slice(&snapshot.md_seq.to_le_bytes());
    out.extend_from_slice(&snapshot.symbol.to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(snapshot.bids.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u32::try_from(snapshot.asks.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for level in snapshot.bids.iter().chain(&snapshot.asks) {
        out.extend_from_slice(&level.price.to_le_bytes());
        out.extend_from_slice(&level.qty.to_le_bytes());
    }
}

fn decode_snapshot(reader: &mut Reader<'_>) -> Result<Snapshot, WireError> {
    let md_seq = reader.u64()?;
    let symbol = reader.u16()?;
    let bids = reader.u32()?;
    let asks = reader.u32()?;
    let total = usize::try_from(bids)
        .unwrap_or(usize::MAX)
        .saturating_add(usize::try_from(asks).unwrap_or(usize::MAX));
    if total > MAX_LEVELS {
        return Err(WireError::TooMany(bids.saturating_add(asks)));
    }
    let mut bid_levels = Vec::with_capacity(bids as usize);
    let mut ask_levels = Vec::with_capacity(asks as usize);
    for _ in 0..bids {
        bid_levels.push(Level {
            price: reader.i64()?,
            qty: reader.i64()?,
        });
    }
    for _ in 0..asks {
        ask_levels.push(Level {
            price: reader.i64()?,
            qty: reader.i64()?,
        });
    }
    Ok(Snapshot {
        md_seq,
        symbol,
        bids: bid_levels,
        asks: ask_levels,
    })
}

fn side_byte(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn side_from(byte: u8) -> Result<Side, WireError> {
    match byte {
        0 => Ok(Side::Buy),
        1 => Ok(Side::Sell),
        other => Err(WireError::UnknownKind(other)),
    }
}

fn action_byte(action: BookAction) -> u8 {
    match action {
        BookAction::Add => 0,
        BookAction::Update => 1,
        BookAction::Delete => 2,
    }
}

fn action_from(byte: u8) -> Result<BookAction, WireError> {
    match byte {
        0 => Ok(BookAction::Add),
        1 => Ok(BookAction::Update),
        2 => Ok(BookAction::Delete),
        other => Err(WireError::UnknownKind(other)),
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

    fn take(&mut self, length: usize) -> Result<&'a [u8], WireError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(WireError::Truncated)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(WireError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, WireError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64, WireError> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update() -> Incremental {
        Incremental::BookUpdate {
            md_seq: 9,
            symbol: 1,
            side: Side::Sell,
            price: 10_025,
            qty: 7,
            action: BookAction::Update,
        }
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            md_seq: 9,
            symbol: 1,
            bids: vec![Level {
                price: 10_000,
                qty: 3,
            }],
            asks: vec![Level {
                price: 10_025,
                qty: 7,
            }],
        }
    }

    #[test]
    fn every_message_round_trips() {
        let messages = [
            WireMessage::Incremental(update()),
            WireMessage::Incremental(Incremental::Trade {
                md_seq: 10,
                symbol: 1,
                price: 10_025,
                qty: 2,
                aggressor: Side::Buy,
            }),
            WireMessage::Snapshot(snapshot()),
            WireMessage::RecoveryRequest(RecoveryRequest {
                symbol: 1,
                last_md_seq: 9,
            }),
            WireMessage::RecoveryResponse(RecoveryResponse::Snapshot(snapshot())),
            WireMessage::RecoveryResponse(RecoveryResponse::Incrementals {
                symbol: 1,
                from_md_seq: 9,
                incrementals: vec![update()],
            }),
            WireMessage::RecoveryResponse(RecoveryResponse::UpToDate {
                symbol: 1,
                md_seq: 9,
            }),
            WireMessage::RecoveryResponse(RecoveryResponse::UnknownSymbol { symbol: 2 }),
        ];
        for message in messages {
            let bytes = encode(&message);
            assert_eq!(Ok(message), decode(&bytes));
        }
    }

    #[test]
    fn trailing_padding_is_ignored() {
        let mut bytes = encode(&WireMessage::Incremental(update()));
        bytes.extend_from_slice(&[0_u8; 995]);
        assert_eq!(Ok(WireMessage::Incremental(update())), decode(&bytes));
    }

    #[test]
    fn a_short_frame_is_refused() {
        let bytes = encode(&WireMessage::Incremental(update()));
        for length in 0..bytes.len() {
            assert!(decode(&bytes[..length]).is_err(), "length {length}");
        }
    }

    #[test]
    fn a_bad_version_or_kind_is_refused() {
        assert_eq!(
            Err(WireError::BadVersion(7)),
            decode(&[7, KIND_TRADE, 0, 0, 0, 0, 0, 0, 0, 0, 0])
        );
        assert_eq!(Err(WireError::UnknownKind(99)), decode(&[VERSION, 99]));
    }

    #[test]
    fn an_incremental_response_rejects_a_non_incremental_payload() {
        let mut bytes = vec![VERSION, KIND_RECOVERY_INCREMENTALS];
        bytes.extend_from_slice(&1_u16.to_le_bytes());
        bytes.extend_from_slice(&1_u64.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.push(KIND_SNAPSHOT);
        bytes.push(VERSION);
        assert_eq!(Err(WireError::WrongKind), decode(&bytes));
    }
}
