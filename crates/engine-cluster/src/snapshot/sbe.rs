//! The SBE snapshot codec (schema id 200, version 2).
//!
//! The generated codec (`deepbook-codec-snapshot`) reads and writes one
//! `EngineSnapshot` message. The generated decoders index the buffer without
//! checking it, so [`decode`] validates the whole record with [`validate`]
//! before wrapping it: a short or corrupt record must be an error, never a
//! panic in the container.
//!
//! The record is framed as `b"DBS2"` + the SBE message, so [`super::decode`]
//! can dispatch on the magic and an old `DBS1` record stays readable.

use std::convert::TryInto;

use deepbook_codec_snapshot::engine_snapshot_codec::decoder::EngineSnapshotDecoder;
use deepbook_codec_snapshot::engine_snapshot_codec::encoder::{
    AsksEncoder, BidsEncoder, EngineSnapshotEncoder, FreeEncoder, KilledAccountsEncoder,
    OpenOrdersEncoder, SeenEncoder, SlotsEncoder,
};
use deepbook_codec_snapshot::engine_snapshot_codec::{SBE_BLOCK_LENGTH, SBE_TEMPLATE_ID};
use deepbook_codec_snapshot::message_header_codec::ENCODED_LENGTH as HEADER_LENGTH;
use deepbook_codec_snapshot::message_header_codec::decoder::MessageHeaderDecoder;
use deepbook_codec_snapshot::order_state::OrderState as SbeOrderState;
use deepbook_codec_snapshot::seen_state::SeenState as SbeSeenState;
use deepbook_codec_snapshot::side::Side as SbeSide;
use deepbook_codec_snapshot::{ReadBuf, SBE_SCHEMA_ID, SBE_SCHEMA_VERSION, SbeErr, WriteBuf};
use deepbook_engine_core::ids::ClientOrderId;
use deepbook_engine_core::{
    BookSnapshot, EngineSnapshot, LevelSnapshot, OrderSlotSnapshot, Seen, SeenEntry, SeenState,
};
use deepbook_types::{OrderState, Side};

use super::CodecError;

/// The frame magic; the SBE message follows it.
pub(super) const MAGIC: [u8; 4] = *b"DBS2";

/// The largest item count either codec accepts. Kept in step with the legacy
/// codec so a corrupt count is refused before anything is allocated.
const MAX_ITEMS: usize = 1 << 24;

// The fixed block lengths of the schema. `validate` needs them to walk the
// record without invoking the generated decoder; a mismatch is a schema change
// that must bump the version.
const MESSAGE_BLOCK: usize = SBE_BLOCK_LENGTH as usize;
const KILLED_ACCOUNTS_BLOCK: usize = 4;
const OPEN_ORDERS_BLOCK: usize = 8;
const SEEN_BLOCK: usize = 73;
const FREE_BLOCK: usize = 4;
/// Version 2 appended `isDay` to the slot block (83 bytes in version 1).
const SLOTS_BLOCK: usize = 84;
const SLOTS_BLOCK_V1: usize = 83;
const BIDS_BLOCK: usize = 24;
const ASKS_BLOCK: usize = 24;

/// Encode an engine snapshot as `DBS2` + SBE.
///
/// # Errors
///
/// A count does not fit the encoding, or an encoded length overflows `usize`.
pub fn encode(snapshot: &EngineSnapshot) -> Result<Vec<u8>, CodecError> {
    let body = encoded_body_length(snapshot)?;
    let total = MAGIC
        .len()
        .checked_add(HEADER_LENGTH)
        .and_then(|length| length.checked_add(body))
        .ok_or(CodecError::TooLarge)?;
    let mut buffer = vec![0_u8; total];
    buffer[..MAGIC.len()].copy_from_slice(&MAGIC);

    let message_offset = MAGIC.len() + HEADER_LENGTH;
    let encoder = EngineSnapshotEncoder::default().wrap(WriteBuf::new(&mut buffer), message_offset);
    let mut header = encoder.header(MAGIC.len());
    let mut message = header.parent()?;
    message
        .symbol(snapshot.symbol)
        .last_log_pos(snapshot.last_log_pos)
        .kill_all(u8::from(snapshot.kill_all))
        .free_head(snapshot.book.free_head)
        .md_seq(snapshot.book.md_seq)
        .has_last_trade(u8::from(snapshot.book.last_trade.is_some()))
        .last_trade(snapshot.book.last_trade.unwrap_or_default());

    let mut group = message.killed_accounts_encoder(
        count(snapshot.killed_accounts.len())?,
        KilledAccountsEncoder::default(),
    );
    for account in &snapshot.killed_accounts {
        group.advance()?;
        group.account(*account);
    }

    let message = group.parent()?;
    let mut group = message.open_orders_encoder(
        count(snapshot.open_orders.len())?,
        OpenOrdersEncoder::default(),
    );
    for (account, open_count) in &snapshot.open_orders {
        group.advance()?;
        group.account(*account).open_count(count(*open_count)?);
    }

    let message = group.parent()?;
    let mut group = message.seen_encoder(count(snapshot.seen.len())?, SeenEncoder::default());
    for entry in &snapshot.seen {
        let seen = &entry.seen;
        let (notional_hi, notional_lo) = split_notional(seen.notional);
        group.advance()?;
        group
            .account(entry.account)
            .cl_ord_id(entry.cl_ord_id.as_bytes())
            .order_id(seen.order_id)
            .state(seen_state_to_sbe(seen.state))
            .has_price(u8::from(seen.price.is_some()))
            .price(seen.price.unwrap_or_default())
            .qty(seen.qty)
            .filled(seen.filled)
            .notional_hi(notional_hi)
            .notional_lo(notional_lo)
            .side(side_to_sbe(seen.side))
            .symbol(seen.symbol);
    }

    let message = group.parent()?;
    let mut group = message.free_encoder(count(snapshot.book.free.len())?, FreeEncoder::default());
    for slot in &snapshot.book.free {
        group.advance()?;
        group.slot(*slot);
    }

    let message = group.parent()?;
    let mut group =
        message.slots_encoder(count(snapshot.book.slots.len())?, SlotsEncoder::default());
    for slot in &snapshot.book.slots {
        let (notional_hi, notional_lo) = split_notional(slot.notional);
        group.advance()?;
        group
            .slot(slot.slot)
            .order_id(slot.id)
            .price(slot.price)
            .qty(slot.qty)
            .filled(slot.filled)
            .notional_hi(notional_hi)
            .notional_lo(notional_lo)
            .account(slot.account)
            .prev(slot.prev)
            .next(slot.next)
            .side(side_to_sbe(slot.side))
            .state(order_state_to_sbe(slot.state))
            .is_market(u8::from(slot.is_market))
            .cl_ord_id(slot.cl_ord_id.as_bytes())
            .is_day(u8::from(slot.is_day));
    }

    let message = group.parent()?;
    let mut group = message.bids_encoder(count(snapshot.book.bids.len())?, BidsEncoder::default());
    for level in &snapshot.book.bids {
        group.advance()?;
        group
            .price(level.price)
            .total_qty(level.total_qty)
            .head(level.head)
            .tail(level.tail);
    }

    let message = group.parent()?;
    let mut group = message.asks_encoder(count(snapshot.book.asks.len())?, AsksEncoder::default());
    for level in &snapshot.book.asks {
        group.advance()?;
        group
            .price(level.price)
            .total_qty(level.total_qty)
            .head(level.head)
            .tail(level.tail);
    }

    let encoded = {
        let mut message = group.parent()?;
        message.generations(&snapshot.book.generations);
        MAGIC.len() + HEADER_LENGTH + message.encoded_length()
    };
    debug_assert_eq!(encoded, buffer.len());
    Ok(buffer)
}

/// Decode a `DBS2` payload (the SBE message, without the frame magic).
///
/// # Errors
///
/// The record is truncated, belongs to another schema or version, or carries
/// an unknown enum value.
pub fn decode(payload: &[u8]) -> Result<EngineSnapshot, CodecError> {
    validate(payload)?;

    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(payload), 0);
    let decoder = EngineSnapshotDecoder::default().header(header, 0);

    let symbol = decoder.symbol();
    let last_log_pos = decoder.last_log_pos();
    let kill_all = decoder.kill_all() != 0;
    let free_head = decoder.free_head();
    let md_seq = decoder.md_seq();
    let last_trade = (decoder.has_last_trade() != 0).then(|| decoder.last_trade());

    let mut group = decoder.killed_accounts_decoder();
    let mut killed_accounts = Vec::with_capacity(group.count() as usize);
    while group.advance()?.is_some() {
        killed_accounts.push(group.account());
    }

    let decoder = group.parent()?;
    let mut group = decoder.open_orders_decoder();
    let mut open_orders = Vec::with_capacity(group.count() as usize);
    while group.advance()?.is_some() {
        open_orders.push((group.account(), group.open_count() as usize));
    }

    let decoder = group.parent()?;
    let mut group = decoder.seen_decoder();
    let mut seen = Vec::with_capacity(group.count() as usize);
    while group.advance()?.is_some() {
        let account = group.account();
        let cl_ord_id = ClientOrderId::from_padded_bytes(group.cl_ord_id());
        let order_id = group.order_id();
        let state = seen_state_from_sbe(group.state())?;
        let price = (group.has_price() != 0).then(|| group.price());
        let qty = group.qty();
        let filled = group.filled();
        let notional = join_notional(group.notional_hi(), group.notional_lo());
        let side = side_from_sbe(group.side())?;
        let symbol = group.symbol();
        seen.push(SeenEntry {
            account,
            cl_ord_id,
            seen: Seen {
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

    let decoder = group.parent()?;
    let mut group = decoder.free_decoder();
    let mut free = Vec::with_capacity(group.count() as usize);
    while group.advance()?.is_some() {
        free.push(group.slot());
    }

    let decoder = group.parent()?;
    let mut group = decoder.slots_decoder();
    let mut slots = Vec::with_capacity(group.count() as usize);
    while group.advance()?.is_some() {
        slots.push(OrderSlotSnapshot {
            slot: group.slot(),
            id: group.order_id(),
            price: group.price(),
            qty: group.qty(),
            filled: group.filled(),
            notional: join_notional(group.notional_hi(), group.notional_lo()),
            account: group.account(),
            prev: group.prev(),
            next: group.next(),
            side: side_from_sbe(group.side())?,
            state: order_state_from_sbe(group.state())?,
            is_market: group.is_market() != 0,
            // A version-1 record has no `isDay` field and the generated
            // decoder answers 0xff for it; only an explicit 1 is DAY.
            is_day: group.is_day() == 1,
            cl_ord_id: ClientOrderId::from_padded_bytes(group.cl_ord_id()),
        });
    }

    let decoder = group.parent()?;
    let mut group = decoder.bids_decoder();
    let mut bids = Vec::with_capacity(group.count() as usize);
    while group.advance()?.is_some() {
        bids.push(LevelSnapshot {
            price: group.price(),
            total_qty: group.total_qty(),
            head: group.head(),
            tail: group.tail(),
        });
    }

    let decoder = group.parent()?;
    let mut group = decoder.asks_decoder();
    let mut asks = Vec::with_capacity(group.count() as usize);
    while group.advance()?.is_some() {
        asks.push(LevelSnapshot {
            price: group.price(),
            total_qty: group.total_qty(),
            head: group.head(),
            tail: group.tail(),
        });
    }

    let mut decoder = group.parent()?;
    let coordinates = decoder.generations_decoder();
    let generations = decoder.generations_slice(coordinates).to_vec();

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

/// Walk the record's framing without decoding any field. This is what turns a
/// truncated or hostile record into [`CodecError`] instead of a panic inside
/// the generated decoder.
fn validate(payload: &[u8]) -> Result<(), CodecError> {
    let mut cursor = Cursor::new(payload);
    let block_length = cursor.u16()? as usize;
    let template_id = cursor.u16()?;
    let schema_id = cursor.u16()?;
    let version = cursor.u16()?;

    if template_id != SBE_TEMPLATE_ID || schema_id != SBE_SCHEMA_ID {
        return Err(CodecError::BadMagic);
    }
    if version != 1 && version != SBE_SCHEMA_VERSION {
        return Err(CodecError::BadVersion(version.min(u16::from(u8::MAX)) as u8));
    }
    if block_length != MESSAGE_BLOCK {
        return Err(CodecError::BadVersion(version as u8));
    }
    cursor.skip(block_length)?;

    // Version 1 predates the `isDay` field, so its slot block is one byte
    // shorter. An old record stays readable; its DAY orders restore as GTC.
    let slots_block = if version == 1 {
        SLOTS_BLOCK_V1
    } else {
        SLOTS_BLOCK
    };
    for expected in [
        KILLED_ACCOUNTS_BLOCK,
        OPEN_ORDERS_BLOCK,
        SEEN_BLOCK,
        FREE_BLOCK,
        slots_block,
        BIDS_BLOCK,
        ASKS_BLOCK,
    ] {
        let group_block = cursor.u16()? as usize;
        let count = cursor.u32()?;
        if group_block != expected {
            return Err(CodecError::BadVersion(version as u8));
        }
        if count as usize > MAX_ITEMS {
            return Err(CodecError::BadCount(count));
        }
        let length = group_block
            .checked_mul(count as usize)
            .ok_or(CodecError::TooLarge)?;
        cursor.skip(length)?;
    }

    let length = cursor.u32()?;
    if length as usize > MAX_ITEMS {
        return Err(CodecError::BadCount(length));
    }
    cursor.skip(length as usize)?;
    if !cursor.is_empty() {
        return Err(CodecError::Truncated);
    }
    Ok(())
}

fn encoded_body_length(snapshot: &EngineSnapshot) -> Result<usize, CodecError> {
    let mut length = MESSAGE_BLOCK;
    length = group_length(
        length,
        KILLED_ACCOUNTS_BLOCK,
        snapshot.killed_accounts.len(),
    )?;
    length = group_length(length, OPEN_ORDERS_BLOCK, snapshot.open_orders.len())?;
    length = group_length(length, SEEN_BLOCK, snapshot.seen.len())?;
    length = group_length(length, FREE_BLOCK, snapshot.book.free.len())?;
    length = group_length(length, SLOTS_BLOCK, snapshot.book.slots.len())?;
    length = group_length(length, BIDS_BLOCK, snapshot.book.bids.len())?;
    length = group_length(length, ASKS_BLOCK, snapshot.book.asks.len())?;
    length
        .checked_add(4)
        .and_then(|value| value.checked_add(snapshot.book.generations.len()))
        .ok_or(CodecError::TooLarge)
}

fn group_length(base: usize, block: usize, items: usize) -> Result<usize, CodecError> {
    let body = block.checked_mul(items).ok_or(CodecError::TooLarge)?;
    base.checked_add(6)
        .and_then(|value| value.checked_add(body))
        .ok_or(CodecError::TooLarge)
}

fn count(value: usize) -> Result<u32, CodecError> {
    u32::try_from(value).map_err(|_| CodecError::TooLarge)
}

fn split_notional(value: i128) -> (i64, u64) {
    ((value >> 64) as i64, value as u64)
}

fn join_notional(hi: i64, lo: u64) -> i128 {
    (i128::from(hi) << 64) | i128::from(lo)
}

fn side_to_sbe(value: Side) -> SbeSide {
    match value {
        Side::Buy => SbeSide::Buy,
        Side::Sell => SbeSide::Sell,
    }
}

fn side_from_sbe(value: SbeSide) -> Result<Side, CodecError> {
    match value {
        SbeSide::Buy => Ok(Side::Buy),
        SbeSide::Sell => Ok(Side::Sell),
        SbeSide::NullVal => Err(CodecError::BadEnum(u8::from(SbeSide::NullVal))),
    }
}

fn order_state_to_sbe(value: OrderState) -> SbeOrderState {
    match value {
        OrderState::New => SbeOrderState::New,
        OrderState::Partial => SbeOrderState::Partial,
        OrderState::Filled => SbeOrderState::Filled,
        OrderState::Canceled => SbeOrderState::Canceled,
        OrderState::Rejected => SbeOrderState::Rejected,
    }
}

fn order_state_from_sbe(value: SbeOrderState) -> Result<OrderState, CodecError> {
    match value {
        SbeOrderState::New => Ok(OrderState::New),
        SbeOrderState::Partial => Ok(OrderState::Partial),
        SbeOrderState::Filled => Ok(OrderState::Filled),
        SbeOrderState::Canceled => Ok(OrderState::Canceled),
        SbeOrderState::Rejected => Ok(OrderState::Rejected),
        SbeOrderState::NullVal => Err(CodecError::BadEnum(u8::from(SbeOrderState::NullVal))),
    }
}

fn seen_state_to_sbe(value: SeenState) -> SbeSeenState {
    match value {
        SeenState::Active => SbeSeenState::Active,
        SeenState::Filled => SbeSeenState::Filled,
        SeenState::Canceled => SbeSeenState::Canceled,
        SeenState::Rejected => SbeSeenState::Rejected,
    }
}

fn seen_state_from_sbe(value: SbeSeenState) -> Result<SeenState, CodecError> {
    match value {
        SbeSeenState::Active => Ok(SeenState::Active),
        SbeSeenState::Filled => Ok(SeenState::Filled),
        SbeSeenState::Canceled => Ok(SeenState::Canceled),
        SbeSeenState::Rejected => Ok(SeenState::Rejected),
        SbeSeenState::NullVal => Err(CodecError::BadEnum(u8::from(SbeSeenState::NullVal))),
    }
}

impl From<SbeErr> for CodecError {
    fn from(_: SbeErr) -> Self {
        Self::Sbe
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CodecError::TooLarge)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(CodecError::Truncated)?;
        self.offset = end;
        Ok(slice)
    }

    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| CodecError::Truncated)?,
        ))
    }

    fn skip(&mut self, length: usize) -> Result<(), CodecError> {
        self.take(length).map(|_| ())
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notional_splits_and_joins_at_the_extremes() {
        for value in [i128::MIN, -1, 0, 1, i128::MAX] {
            let (hi, lo) = split_notional(value);
            assert_eq!(join_notional(hi, lo), value, "{value}");
        }
    }
}
