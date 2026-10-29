//! The legacy `DBS1` snapshot record.
//!
//! The cluster's `on_take_snapshot` wrote this record before the SBE schema
//! (ADR 0005) and still writes it during the migration's first phase. The
//! format is a versioned little-endian record: the slab slots, their
//! generations and the free-list order are preserved, so a restored engine has
//! the same order ids and the same future allocation order as the engine that
//! wrote the snapshot.

use std::fmt;

use deepbook_engine_core::ids::ClientOrderId;
use deepbook_engine_core::{
    BookSnapshot, EngineSnapshot, LevelSnapshot, OrderSlotSnapshot, SeenEntry, SeenState,
};
use deepbook_types::{OrderState, Price, Side};

pub(super) const MAGIC: [u8; 4] = *b"DBS1";
const VERSION: u8 = 1;
const MAX_ITEMS: usize = 1 << 24;

/// A snapshot encode/decode failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecError {
    /// The record does not start with the snapshot magic.
    BadMagic,
    /// The version byte is not one this build knows.
    BadVersion(u8),
    /// The record ends before the field it promised.
    Truncated,
    /// An item count is implausibly large.
    BadCount(u32),
    /// An enum byte is not one this build knows.
    BadEnum(u8),
    /// A length does not fit the encoding.
    TooLarge,
    /// The generated codec could not be driven (an invariant failure, not a
    /// property of the input).
    Sbe,
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => write!(formatter, "not an engine snapshot"),
            Self::BadVersion(version) => write!(formatter, "unknown snapshot version {version}"),
            Self::Truncated => write!(formatter, "truncated engine snapshot"),
            Self::BadCount(count) => write!(formatter, "implausible item count {count}"),
            Self::BadEnum(byte) => write!(formatter, "unknown enum value {byte}"),
            Self::TooLarge => write!(formatter, "value does not fit the snapshot encoding"),
            Self::Sbe => write!(formatter, "generated snapshot codec failure"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Encode an engine snapshot.
///
/// Production only reads `DBS1` now (the writer emits SBE `DBS2`); this stays
/// for tests and for tooling that needs to produce a legacy fixture.
#[allow(dead_code)]
#[must_use]
pub fn encode(snapshot: &EngineSnapshot) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&snapshot.symbol.to_le_bytes());
    out.extend_from_slice(&snapshot.last_log_pos.to_le_bytes());
    out.push(u8::from(snapshot.kill_all));
    put_count(&mut out, snapshot.killed_accounts.len());
    for account in &snapshot.killed_accounts {
        out.extend_from_slice(&account.to_le_bytes());
    }
    put_count(&mut out, snapshot.open_orders.len());
    for (account, count) in &snapshot.open_orders {
        out.extend_from_slice(&account.to_le_bytes());
        out.extend_from_slice(&u32::try_from(*count).unwrap_or(u32::MAX).to_le_bytes());
    }
    put_count(&mut out, snapshot.seen.len());
    for entry in &snapshot.seen {
        out.extend_from_slice(&entry.account.to_le_bytes());
        out.extend_from_slice(entry.cl_ord_id.as_bytes());
        out.extend_from_slice(&entry.seen.order_id.to_le_bytes());
        out.push(seen_state_byte(entry.seen.state));
        put_price(&mut out, entry.seen.price);
        out.extend_from_slice(&entry.seen.qty.to_le_bytes());
        out.extend_from_slice(&entry.seen.filled.to_le_bytes());
        out.extend_from_slice(&entry.seen.notional.to_le_bytes());
        out.push(side_byte(entry.seen.side));
        out.extend_from_slice(&entry.seen.symbol.to_le_bytes());
    }

    let book = &snapshot.book;
    put_count(&mut out, book.generations.len());
    out.extend_from_slice(&book.generations);
    put_count(&mut out, book.free.len());
    for slot in &book.free {
        out.extend_from_slice(&slot.to_le_bytes());
    }
    out.extend_from_slice(&book.free_head.to_le_bytes());
    put_count(&mut out, book.slots.len());
    for slot in &book.slots {
        out.extend_from_slice(&slot.slot.to_le_bytes());
        out.extend_from_slice(&slot.id.to_le_bytes());
        out.extend_from_slice(&slot.price.to_le_bytes());
        out.extend_from_slice(&slot.qty.to_le_bytes());
        out.extend_from_slice(&slot.filled.to_le_bytes());
        out.extend_from_slice(&slot.notional.to_le_bytes());
        out.extend_from_slice(&slot.account.to_le_bytes());
        out.extend_from_slice(&slot.prev.to_le_bytes());
        out.extend_from_slice(&slot.next.to_le_bytes());
        out.push(side_byte(slot.side));
        out.push(state_byte(slot.state));
        out.push(u8::from(slot.is_market));
        out.extend_from_slice(slot.cl_ord_id.as_bytes());
    }
    put_levels(&mut out, &book.bids);
    put_levels(&mut out, &book.asks);
    put_price(&mut out, book.last_trade);
    out.extend_from_slice(&book.md_seq.to_le_bytes());
    out
}

/// Decode an engine snapshot.
///
/// # Errors
///
/// [`CodecError`] for a short, unknown or inconsistent record.
pub fn decode(bytes: &[u8]) -> Result<EngineSnapshot, CodecError> {
    let mut reader = Reader::new(bytes);
    if reader.take(4)? != MAGIC {
        return Err(CodecError::BadMagic);
    }
    let version = reader.u8()?;
    if version != VERSION {
        return Err(CodecError::BadVersion(version));
    }
    let symbol = reader.u16()?;
    let last_log_pos = reader.i64()?;
    let kill_all = reader.u8()? != 0;

    let killed_count = reader.count()?;
    let mut killed_accounts = Vec::with_capacity(killed_count);
    for _ in 0..killed_count {
        killed_accounts.push(reader.u32()?);
    }

    let open_count = reader.count()?;
    let mut open_orders = Vec::with_capacity(open_count);
    for _ in 0..open_count {
        open_orders.push((reader.u32()?, reader.u32()? as usize));
    }

    let seen_count = reader.count()?;
    let mut seen = Vec::with_capacity(seen_count);
    for _ in 0..seen_count {
        let account = reader.u32()?;
        let cl_ord_id = reader.client_order_id()?;
        let order_id = reader.u32()?;
        let state = seen_state_from(reader.u8()?)?;
        let price = reader.price()?;
        let qty = reader.i64()?;
        let filled = reader.i64()?;
        let notional = reader.i128()?;
        let side = side_from(reader.u8()?)?;
        let symbol = reader.u16()?;
        seen.push(SeenEntry {
            account,
            cl_ord_id,
            seen: deepbook_engine_core::Seen {
                order_id,
                state,
                price,
                qty,
                filled,
                notional,
                side,
                symbol,
            },
        });
    }

    let generation_count = reader.count()?;
    let generations = reader.take(generation_count)?.to_vec();
    let free_count = reader.count()?;
    let mut free = Vec::with_capacity(free_count);
    for _ in 0..free_count {
        free.push(reader.u32()?);
    }
    let free_head = reader.u32()?;

    let slot_count = reader.count()?;
    let mut slots = Vec::with_capacity(slot_count);
    for _ in 0..slot_count {
        slots.push(OrderSlotSnapshot {
            slot: reader.u32()?,
            id: reader.u32()?,
            price: reader.i64()?,
            qty: reader.i64()?,
            filled: reader.i64()?,
            notional: reader.i128()?,
            account: reader.u32()?,
            prev: reader.u32()?,
            next: reader.u32()?,
            side: side_from(reader.u8()?)?,
            state: state_from(reader.u8()?)?,
            is_market: reader.u8()? != 0,
            // The legacy DBS1 record predates DAY expiry; restore as GTC.
            is_day: false,
            cl_ord_id: reader.client_order_id()?,
        });
    }
    let bids = take_levels(&mut reader)?;
    let asks = take_levels(&mut reader)?;
    let last_trade = reader.price()?;
    let md_seq = reader.u64()?;

    Ok(EngineSnapshot {
        symbol,
        last_log_pos,
        book: BookSnapshot {
            slots,
            generations,
            free,
            free_head,
            bids,
            asks,
            last_trade,
            md_seq,
        },
        seen,
        open_orders,
        kill_all,
        killed_accounts,
    })
}

fn put_count(out: &mut Vec<u8>, count: usize) {
    out.extend_from_slice(&u32::try_from(count).unwrap_or(u32::MAX).to_le_bytes());
}

fn put_price(out: &mut Vec<u8>, price: Option<Price>) {
    match price {
        Some(price) => {
            out.push(1);
            out.extend_from_slice(&price.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn put_levels(out: &mut Vec<u8>, levels: &[LevelSnapshot]) {
    put_count(out, levels.len());
    for level in levels {
        out.extend_from_slice(&level.price.to_le_bytes());
        out.extend_from_slice(&level.total_qty.to_le_bytes());
        out.extend_from_slice(&level.head.to_le_bytes());
        out.extend_from_slice(&level.tail.to_le_bytes());
    }
}

fn take_levels(reader: &mut Reader<'_>) -> Result<Vec<LevelSnapshot>, CodecError> {
    let count = reader.count()?;
    let mut levels = Vec::with_capacity(count);
    for _ in 0..count {
        levels.push(LevelSnapshot {
            price: reader.i64()?,
            total_qty: reader.i64()?,
            head: reader.u32()?,
            tail: reader.u32()?,
        });
    }
    Ok(levels)
}

fn side_byte(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn side_from(byte: u8) -> Result<Side, CodecError> {
    match byte {
        0 => Ok(Side::Buy),
        1 => Ok(Side::Sell),
        other => Err(CodecError::BadEnum(other)),
    }
}

fn state_byte(state: OrderState) -> u8 {
    match state {
        OrderState::New => 0,
        OrderState::Partial => 1,
        OrderState::Filled => 2,
        OrderState::Canceled => 3,
        OrderState::Rejected => 4,
    }
}

fn state_from(byte: u8) -> Result<OrderState, CodecError> {
    match byte {
        0 => Ok(OrderState::New),
        1 => Ok(OrderState::Partial),
        2 => Ok(OrderState::Filled),
        3 => Ok(OrderState::Canceled),
        4 => Ok(OrderState::Rejected),
        other => Err(CodecError::BadEnum(other)),
    }
}

fn seen_state_byte(state: SeenState) -> u8 {
    match state {
        SeenState::Active => 0,
        SeenState::Filled => 1,
        SeenState::Canceled => 2,
        SeenState::Rejected => 3,
    }
}

fn seen_state_from(byte: u8) -> Result<SeenState, CodecError> {
    match byte {
        0 => Ok(SeenState::Active),
        1 => Ok(SeenState::Filled),
        2 => Ok(SeenState::Canceled),
        3 => Ok(SeenState::Rejected),
        other => Err(CodecError::BadEnum(other)),
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

    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CodecError::Truncated)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(CodecError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("two")))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("four")))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("eight")))
    }

    fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().expect("eight")))
    }

    fn i128(&mut self) -> Result<i128, CodecError> {
        Ok(i128::from_le_bytes(
            self.take(16)?.try_into().expect("sixteen"),
        ))
    }

    fn count(&mut self) -> Result<usize, CodecError> {
        let count = self.u32()?;
        if usize::try_from(count).unwrap_or(usize::MAX) > MAX_ITEMS {
            Err(CodecError::BadCount(count))
        } else {
            Ok(count as usize)
        }
    }

    fn price(&mut self) -> Result<Option<Price>, CodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.i64()?)),
            other => Err(CodecError::BadEnum(other)),
        }
    }

    fn client_order_id(&mut self) -> Result<ClientOrderId, CodecError> {
        let bytes: [u8; ClientOrderId::LENGTH] = self
            .take(ClientOrderId::LENGTH)?
            .try_into()
            .map_err(|_| CodecError::Truncated)?;
        Ok(ClientOrderId::from_padded_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepbook_engine_core::{
        CancelOrder, Command, Engine, EngineConfig, NewOrder, OrdType, ReplaceOrder, RiskConfig,
        TimeInForce,
    };
    use deepbook_types::{AccountId, LogPos, Price, Qty};

    fn config() -> EngineConfig {
        EngineConfig {
            symbol: 1,
            max_orders: 64,
            recent_order_window: 64,
            risk: RiskConfig::disabled(),
        }
    }

    fn cl(value: &str) -> ClientOrderId {
        value.parse().expect("client order id")
    }

    fn limit(account: AccountId, id: &str, side: Side, price: Price, qty: Qty) -> Command {
        Command::NewOrder(NewOrder {
            symbol: 1,
            account,
            cl_ord_id: cl(id),
            side,
            price: Some(price),
            qty,
            ord_type: OrdType::Limit,
            time_in_force: TimeInForce::GoodTillCanceled,
            post_only: false,
        })
    }

    #[test]
    fn a_snapshot_round_trips_and_restores_the_same_engine() {
        let mut engine = Engine::new(config());
        let commands = [
            limit(1, "S1", Side::Sell, 100, 10),
            limit(2, "B1", Side::Buy, 99, 5),
            limit(3, "S2", Side::Sell, 101, 20),
            Command::CancelOrder(CancelOrder {
                account: 2,
                cl_ord_id: cl("C1"),
                orig_cl_ord_id: cl("B1"),
            }),
            Command::ReplaceOrder(ReplaceOrder {
                account: 1,
                cl_ord_id: cl("R1"),
                orig_cl_ord_id: cl("S1"),
                price: Some(102),
                qty: 8,
                ord_type: OrdType::Limit,
                time_in_force: TimeInForce::GoodTillCanceled,
                post_only: false,
            }),
        ];
        let mut events = Vec::new();
        for (index, command) in commands.iter().enumerate() {
            engine.on_command(command, index as LogPos + 1, &mut events);
        }

        let snapshot = engine.snapshot();
        let bytes = encode(&snapshot);
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(snapshot, decoded);

        let mut restored = Engine::restore(config(), &decoded).expect("restore");
        assert_eq!(engine.book_hash(), restored.book_hash());
        assert_eq!(engine.view(), restored.view());

        let continuation = [
            limit(3, "B2", Side::Buy, 100, 5),
            limit(4, "S3", Side::Sell, 99, 7),
        ];
        let mut first = Vec::new();
        let mut second = Vec::new();
        for (index, command) in continuation.iter().enumerate() {
            let log = index as LogPos + 1;
            engine.on_command(command, log, &mut first);
            restored.on_command(command, log, &mut second);
        }
        assert_eq!(first, second);
        assert_eq!(engine.book_hash(), restored.book_hash());
    }

    #[test]
    fn a_short_or_foreign_record_is_refused() {
        let snapshot = Engine::new(config()).snapshot();
        let bytes = encode(&snapshot);
        assert_eq!(Err(CodecError::BadMagic), decode(&[0; 8]));
        for length in 0..bytes.len() {
            assert!(decode(&bytes[..length]).is_err(), "length {length}");
        }
    }
}
