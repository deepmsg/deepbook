//! The slab, price levels and the book view.
//!
//! `orders` is a slab of [`OrderHot`]; a free slot is linked through `next`.
//! A level is a FIFO doubly linked list of bare slot indices. `cold` holds the
//! 20-byte client ids, so the matching path touches one cold entry only when
//! it needs an id for an event.
//!
//! Price levels are `BTreeMap`s in MVP (the design's M6 replaces them with a
//! price band and a bitmap). A new level allocates; matching at an existing
//! level does not.

use deepbook_types::{
    AccountId, EMPTY_ORDER_ID, OrderId, OrderState, Price, Qty, Seq, Side, SymbolId,
};

use crate::band::{
    PRODUCTION_BAND_TICKS, PRODUCTION_L0_BITS, PriceBandSide, RECENTER_MARGIN, RECENTER_STEP_TICKS,
    STEP_TICKS,
};
use crate::ids::{ClientOrderId, EMPTY_SLOT, MAX_SLOTS, next_generation, pack, slot_of};
use crate::snapshot::{BookSnapshot, LevelSnapshot, OrderSlotSnapshot, SnapshotError};

/// One resting order's hot fields.
///
/// `repr(C, align(64))` pins the record to one cache line: the fields pack to
/// exactly 64 bytes in declaration order, and the alignment makes the slab's
/// base 64-byte aligned so no element straddles two lines. The layout is
/// asserted below; changing a field means updating the assertions and ADR
/// 0003's note on the record.
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct OrderHot {
    pub id: OrderId,
    pub price: Price,
    pub qty: Qty,
    pub filled: Qty,
    pub notional: i128,
    pub account: AccountId,
    pub prev: u32,
    pub next: u32,
    pub side: Side,
    pub state: OrderState,
    pub is_market: bool,
    /// Whether the order dies at the end of the trading day.
    pub is_day: bool,
}

const _: () = {
    use core::mem::{align_of, offset_of, size_of};
    assert!(size_of::<OrderHot>() == 64);
    assert!(align_of::<OrderHot>() == 64);
    // The padding slot between `id` and `price` is part of the layout: `Price`
    // is 8-byte aligned and `id` is 4 bytes.
    assert!(offset_of!(OrderHot, id) == 0);
    assert!(offset_of!(OrderHot, price) == 8);
    assert!(offset_of!(OrderHot, notional) == 32);
    assert!(offset_of!(OrderHot, account) == 48);
    assert!(offset_of!(OrderHot, side) == 60);
    assert!(offset_of!(OrderHot, is_day) == 63);
};

impl OrderHot {
    fn free(next: u32) -> Self {
        Self {
            id: EMPTY_ORDER_ID,
            price: 0,
            qty: 0,
            filled: 0,
            notional: 0,
            account: 0,
            prev: EMPTY_SLOT,
            next,
            side: Side::Buy,
            state: OrderState::Rejected,
            is_market: false,
            is_day: false,
        }
    }

    /// Remaining quantity.
    #[must_use]
    pub(crate) const fn leaves(&self) -> Qty {
        self.qty - self.filled
    }

    /// Average fill price in ticks, rounded down.
    #[must_use]
    pub(crate) fn average_price(&self) -> Price {
        if self.filled == 0 {
            0
        } else {
            i64::try_from(self.notional / i128::from(self.filled)).unwrap_or(0)
        }
    }
}

/// The cold fields.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OrderCold {
    pub cl_ord_id: ClientOrderId,
}

/// One price level's FIFO list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PriceLevel {
    pub total_qty: Qty,
    pub head: u32,
    pub tail: u32,
}

impl PriceLevel {
    /// An empty level: no quantity and no orders.
    pub(crate) const EMPTY: Self = Self {
        total_qty: 0,
        head: EMPTY_SLOT,
        tail: EMPTY_SLOT,
    };
}

/// A level as seen by a test or the differential oracle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelView {
    /// Price.
    pub price: Price,
    /// Total quantity.
    pub qty: Qty,
    /// Orders in FIFO order.
    pub orders: Vec<OrderView>,
}

/// An order as seen by a test or the differential oracle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderView {
    /// Client order id.
    pub cl_ord_id: ClientOrderId,
    /// Account.
    pub account: AccountId,
    /// Side.
    pub side: Side,
    /// Price.
    pub price: Price,
    /// Original quantity.
    pub qty: Qty,
    /// Filled quantity.
    pub filled: Qty,
}

/// The deterministic book view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookView {
    /// Symbol.
    pub symbol: SymbolId,
    /// Bids, best first.
    pub bids: Vec<LevelView>,
    /// Asks, best first.
    pub asks: Vec<LevelView>,
    /// Last trade price.
    pub last_trade: Option<Price>,
    /// Market-data sequence.
    pub md_seq: Seq,
}

/// The order slab and the two sides of the book.
pub(crate) struct Book {
    pub symbol: SymbolId,
    pub bids: PriceBandSide,
    pub asks: PriceBandSide,
    /// The low end of both bands. Both sides share it so one recenter moves
    /// them together.
    base_price: Price,
    /// Whether a price has anchored the bands yet.
    base_initialized: bool,
    pub orders: Vec<OrderHot>,
    pub cold: Vec<OrderCold>,
    generations: Vec<u8>,
    free_head: u32,
    pub last_trade: Option<Price>,
    pub md_seq: Seq,
}

impl Book {
    /// A book with `capacity` slab slots and the production band geometry.
    pub fn new(symbol: SymbolId, capacity: usize) -> Self {
        Self::new_with_band(symbol, capacity, PRODUCTION_L0_BITS)
    }

    /// A book with a chosen band geometry. A test passes a small `l0_bits` so
    /// a recenter moves whole `l1` words and `l0` bits with small arrays.
    pub(crate) fn new_with_band(symbol: SymbolId, capacity: usize, l0_bits: usize) -> Self {
        let capacity = capacity.clamp(1, MAX_SLOTS);
        let mut orders = Vec::with_capacity(capacity);
        let mut cold = Vec::with_capacity(capacity);
        let mut generations = Vec::with_capacity(capacity);
        for slot in 0..capacity {
            let next = if slot + 1 < capacity {
                u32::try_from(slot + 1).expect("capacity <= 2^24")
            } else {
                EMPTY_SLOT
            };
            orders.push(OrderHot::free(next));
            cold.push(OrderCold::default());
            generations.push(0);
        }
        Self {
            symbol,
            bids: PriceBandSide::new(l0_bits),
            asks: PriceBandSide::new(l0_bits),
            base_price: 0,
            base_initialized: false,
            orders,
            cold,
            generations,
            free_head: 0,
            last_trade: None,
            md_seq: 0,
        }
    }

    /// The low end of the band, or `None` before the first resting limit
    /// order anchored it.
    #[must_use]
    pub(crate) const fn base_price(&self) -> Option<Price> {
        if self.base_initialized {
            Some(self.base_price)
        } else {
            None
        }
    }

    /// The band's width in ticks.
    fn band_ticks(&self) -> i64 {
        self.bids.band_ticks()
    }

    /// Anchor the band on `price` if nothing has yet.
    pub(crate) fn ensure_base(&mut self, price: Price) {
        if !self.base_initialized {
            self.base_price = target_base(price, self.band_ticks(), STEP_TICKS);
            self.base_initialized = true;
        }
    }

    fn side(&self, side: Side) -> &PriceBandSide {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut PriceBandSide {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Allocate a slot for a new order.
    // The fields mirror the order being created; a builder would only move
    // the same arguments.
    #[allow(clippy::too_many_arguments)]
    pub fn allocate(
        &mut self,
        account: AccountId,
        side: Side,
        price: Price,
        qty: Qty,
        cl_ord_id: ClientOrderId,
        is_market: bool,
        is_day: bool,
    ) -> Option<(OrderId, u32)> {
        if self.free_head == EMPTY_SLOT {
            return None;
        }
        let slot = self.free_head;
        let index = slot as usize;
        self.free_head = self.orders[index].next;
        let generation = next_generation(self.generations[index]);
        self.generations[index] = generation;
        let id = pack(generation, slot);
        self.orders[index] = OrderHot {
            id,
            price,
            qty,
            filled: 0,
            notional: 0,
            account,
            prev: EMPTY_SLOT,
            next: EMPTY_SLOT,
            side,
            state: OrderState::New,
            is_market,
            is_day,
        };
        self.cold[index] = OrderCold { cl_ord_id };
        Some((id, slot))
    }

    /// Return a slot to the free list.
    pub fn free(&mut self, slot: u32) {
        let index = slot as usize;
        let next = self.free_head;
        self.orders[index] = OrderHot::free(next);
        self.free_head = slot;
    }

    /// The slot for an id, if the generation matches.
    #[must_use]
    pub fn find(&self, id: OrderId) -> Option<u32> {
        let slot = slot_of(id);
        let index = usize::try_from(slot).ok()?;
        let order = self.orders.get(index)?;
        (order.id == id).then_some(slot)
    }

    /// The best opposite price for an incoming `side`.
    #[must_use]
    pub fn best_opposite(&self, side: Side) -> Option<Price> {
        let base = self.base_price;
        match side {
            Side::Buy => self.asks.best(false, base),
            Side::Sell => self.bids.best(true, base),
        }
    }

    /// A level, if it exists.
    #[must_use]
    pub fn level(&self, side: Side, price: Price) -> Option<PriceLevel> {
        let base = self.base_price;
        self.side(side).level(price, base)
    }

    /// A level for mutation, if it exists.
    pub(crate) fn level_mut(&mut self, side: Side, price: Price) -> Option<&mut PriceLevel> {
        let base = self.base_price;
        self.side_mut(side).level_mut(price, base)
    }

    /// Append a resting order to the tail of its level.
    pub fn insert_resting(&mut self, slot: u32) {
        let order = self.orders[slot as usize];
        self.ensure_base(order.price);
        let base = self.base_price;
        let band = match order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let level = band.level_or_insert(order.price, base);
        self.orders[slot as usize].prev = level.tail;
        self.orders[slot as usize].next = EMPTY_SLOT;
        if level.tail != EMPTY_SLOT {
            self.orders[level.tail as usize].next = slot;
        } else {
            level.head = slot;
        }
        level.tail = slot;
        level.total_qty += order.leaves();
    }

    /// Remove an order from its level, leaving the slot allocated.
    pub fn unlink(&mut self, slot: u32) {
        let order = self.orders[slot as usize];
        let base = self.base_price;
        let leaves = order.leaves();
        {
            let band = match order.side {
                Side::Buy => &mut self.bids,
                Side::Sell => &mut self.asks,
            };
            let Some(level) = band.level_mut(order.price, base) else {
                return;
            };
            if order.prev != EMPTY_SLOT {
                self.orders[order.prev as usize].next = order.next;
            } else {
                level.head = order.next;
            }
            if order.next != EMPTY_SLOT {
                self.orders[order.next as usize].prev = order.prev;
            } else {
                level.tail = order.prev;
            }
            level.total_qty -= leaves;
            self.orders[slot as usize].prev = EMPTY_SLOT;
            self.orders[slot as usize].next = EMPTY_SLOT;
        }
        self.side_mut(order.side)
            .release_if_empty(order.price, base);
    }

    /// The total quantity at a level, zero when absent.
    #[must_use]
    pub fn level_qty(&self, side: Side, price: Price) -> Qty {
        self.level(side, price).map_or(0, |level| level.total_qty)
    }

    /// Increment and return the market-data sequence.
    pub fn next_md_seq(&mut self) -> Seq {
        self.md_seq += 1;
        self.md_seq
    }

    /// A deterministic view for tests and the differential oracle.
    #[must_use]
    pub fn view(&self) -> BookView {
        BookView {
            symbol: self.symbol,
            bids: self.side_view(Side::Buy),
            asks: self.side_view(Side::Sell),
            last_trade: self.last_trade,
            md_seq: self.md_seq,
        }
    }

    /// The levels of one side ascending by price, outliers included.
    pub(crate) fn levels_ascending(
        &self,
        side: Side,
    ) -> impl Iterator<Item = (Price, PriceLevel)> + '_ {
        let band = self.side(side);
        let base = self.base_price;
        let top = base.saturating_add(self.band_ticks());
        band.outliers()
            .range(..base)
            .map(|(price, level)| (*price, *level))
            .chain(band.in_band_ascending(base))
            .chain(
                band.outliers()
                    .range(top..)
                    .map(|(price, level)| (*price, *level)),
            )
    }

    /// The levels of one side descending by price, outliers included.
    pub(crate) fn levels_descending(
        &self,
        side: Side,
    ) -> impl Iterator<Item = (Price, PriceLevel)> + '_ {
        let band = self.side(side);
        let base = self.base_price;
        let top = base.saturating_add(self.band_ticks());
        band.outliers()
            .range(top..)
            .rev()
            .map(|(price, level)| (*price, *level))
            .chain(band.in_band_descending(base))
            .chain(
                band.outliers()
                    .range(..base)
                    .rev()
                    .map(|(price, level)| (*price, *level)),
            )
    }

    fn side_view(&self, side: Side) -> Vec<LevelView> {
        let levels: Vec<(Price, PriceLevel)> = match side {
            Side::Buy => self.levels_descending(Side::Buy).collect(),
            Side::Sell => self.levels_ascending(Side::Sell).collect(),
        };
        levels
            .into_iter()
            .map(|(price, level)| {
                let mut orders = Vec::new();
                let mut slot = level.head;
                while slot != EMPTY_SLOT {
                    let order = self.orders[slot as usize];
                    orders.push(OrderView {
                        cl_ord_id: self.cold[slot as usize].cl_ord_id,
                        account: order.account,
                        side: order.side,
                        price: order.price,
                        qty: order.qty,
                        filled: order.filled,
                    });
                    slot = order.next;
                }
                LevelView {
                    price,
                    qty: level.total_qty,
                    orders,
                }
            })
            .collect()
    }

    /// A deterministic hash of the book and order states.
    #[must_use]
    pub fn hash(&self) -> u64 {
        let mut hasher = Fnv::new();
        hasher.write_u16(self.symbol);
        hasher.write_i64(self.last_trade.unwrap_or(i64::MIN));
        hasher.write_u64(self.md_seq);
        for (price, level) in self.levels_descending(Side::Buy) {
            hasher.write_u8(0);
            hasher.write_i64(price);
            hasher.write_i64(level.total_qty);
            self.hash_level(level, &mut hasher);
        }
        for (price, level) in self.levels_ascending(Side::Sell) {
            hasher.write_u8(1);
            hasher.write_i64(price);
            hasher.write_i64(level.total_qty);
            self.hash_level(level, &mut hasher);
        }
        hasher.finish()
    }

    fn hash_level(&self, level: PriceLevel, hasher: &mut Fnv) {
        let mut slot = level.head;
        while slot != EMPTY_SLOT {
            let order = self.orders[slot as usize];
            hasher.write_u32(order.id);
            hasher.write_u32(order.account);
            hasher.write_i64(order.qty);
            hasher.write_i64(order.filled);
            hasher.write_i64(order.price);
            hasher.write_u8(match order.side {
                Side::Buy => 0,
                Side::Sell => 1,
            });
            hasher.write_bytes(self.cold[slot as usize].cl_ord_id.as_bytes());
            slot = order.next;
        }
    }
    /// The book state for a snapshot.
    #[must_use]
    pub fn snapshot(&self) -> BookSnapshot {
        let mut slots = Vec::new();
        let mut free = Vec::new();
        for (index, order) in self.orders.iter().enumerate() {
            if order.id == EMPTY_ORDER_ID {
                free.push(u32::try_from(index).expect("slot <= 2^24"));
            } else {
                slots.push(OrderSlotSnapshot {
                    slot: u32::try_from(index).expect("slot <= 2^24"),
                    id: order.id,
                    price: order.price,
                    qty: order.qty,
                    filled: order.filled,
                    notional: order.notional,
                    account: order.account,
                    prev: order.prev,
                    next: order.next,
                    side: order.side,
                    state: order.state,
                    is_market: order.is_market,
                    is_day: order.is_day,
                    cl_ord_id: self.cold[index].cl_ord_id,
                });
            }
        }
        BookSnapshot {
            slots,
            generations: self.generations.clone(),
            free,
            free_head: self.free_head,
            bids: self
                .levels_ascending(Side::Buy)
                .map(|(price, level)| LevelSnapshot {
                    price,
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                })
                .collect(),
            asks: self
                .levels_ascending(Side::Sell)
                .map(|(price, level)| LevelSnapshot {
                    price,
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                })
                .collect(),
            base_price: self.base_price(),
            last_trade: self.last_trade,
            md_seq: self.md_seq,
        }
    }

    /// Rebuild a book from a snapshot.
    ///
    /// # Errors
    ///
    /// The snapshot's capacity, slots, free list or levels are inconsistent.
    pub fn restore(
        snapshot: &BookSnapshot,
        capacity: usize,
        symbol: SymbolId,
    ) -> Result<Self, SnapshotError> {
        if snapshot.generations.len() != capacity {
            return Err(SnapshotError::WrongCapacity {
                expected: capacity,
                actual: snapshot.generations.len(),
            });
        }
        let mut orders = vec![OrderHot::free(EMPTY_SLOT); capacity];
        let mut cold = vec![OrderCold::default(); capacity];
        let mut present = vec![false; capacity];

        for (index, slot) in snapshot.free.iter().enumerate() {
            let slot_index = usize::try_from(*slot).map_err(|_| SnapshotError::BadSlot(*slot))?;
            if slot_index >= capacity {
                return Err(SnapshotError::BadSlot(*slot));
            }
            if present[slot_index] {
                return Err(SnapshotError::DuplicateSlot(*slot));
            }
            present[slot_index] = true;
            let next = snapshot.free.get(index + 1).copied().unwrap_or(EMPTY_SLOT);
            orders[slot_index] = OrderHot::free(next);
        }
        let expected_head = snapshot.free.first().copied().unwrap_or(EMPTY_SLOT);
        if snapshot.free_head != expected_head {
            return Err(SnapshotError::Corrupt(
                "free-list head does not match the free slots".to_owned(),
            ));
        }

        for entry in &snapshot.slots {
            let slot_index =
                usize::try_from(entry.slot).map_err(|_| SnapshotError::BadSlot(entry.slot))?;
            if slot_index >= capacity {
                return Err(SnapshotError::BadSlot(entry.slot));
            }
            if present[slot_index] {
                return Err(SnapshotError::DuplicateSlot(entry.slot));
            }
            present[slot_index] = true;
            orders[slot_index] = OrderHot {
                id: entry.id,
                price: entry.price,
                qty: entry.qty,
                filled: entry.filled,
                notional: entry.notional,
                account: entry.account,
                prev: entry.prev,
                next: entry.next,
                side: entry.side,
                state: entry.state,
                is_market: entry.is_market,
                is_day: entry.is_day,
            };
            cold[slot_index] = OrderCold {
                cl_ord_id: entry.cl_ord_id,
            };
        }
        for (slot, present) in present.iter().enumerate() {
            if !present {
                return Err(SnapshotError::MissingSlot(
                    u32::try_from(slot).expect("slot <= 2^24"),
                ));
            }
        }

        let base = snapshot
            .base_price
            .unwrap_or_else(|| derive_base(&snapshot.bids, &snapshot.asks, PRODUCTION_BAND_TICKS));
        let anchored = snapshot.base_price.is_some()
            || !(snapshot.bids.is_empty() && snapshot.asks.is_empty());

        let mut bids = PriceBandSide::new(PRODUCTION_L0_BITS);
        for level in &snapshot.bids {
            check_level(level, capacity)?;
            bids.restore_level(
                level.price,
                PriceLevel {
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                },
                base,
            );
        }
        let mut asks = PriceBandSide::new(PRODUCTION_L0_BITS);
        for level in &snapshot.asks {
            check_level(level, capacity)?;
            asks.restore_level(
                level.price,
                PriceLevel {
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                },
                base,
            );
        }

        Ok(Self {
            symbol,
            bids,
            asks,
            base_price: base,
            base_initialized: anchored,
            orders,
            cold,
            generations: snapshot.generations.clone(),
            free_head: snapshot.free_head,
            last_trade: snapshot.last_trade,
            md_seq: snapshot.md_seq,
        })
    }

    /// Move the band when the last trade has come within
    /// [`RECENTER_MARGIN`] of an edge, or left the band altogether.
    pub(crate) fn recenter_if_needed(&mut self) {
        if !self.base_initialized {
            return;
        }
        let Some(trade) = self.last_trade else {
            return;
        };
        let base = self.base_price;
        let band = self.band_ticks();
        let offset = i128::from(trade) - i128::from(base);
        let margin = i128::from(RECENTER_MARGIN);
        if offset >= margin && offset < i128::from(band) - margin {
            return;
        }
        let new_base = target_base(trade, band, RECENTER_STEP_TICKS);
        if new_base == base {
            return;
        }
        self.bids.recenter(base, new_base);
        self.asks.recenter(base, new_base);
        self.base_price = new_base;
        debug_assert!(self.bids.bitmap_is_consistent());
        debug_assert!(self.asks.bitmap_is_consistent());
    }
}

/// The base that centres `price` in a band of `band_ticks`, aligned down to
/// `step`.
fn target_base(price: Price, band_ticks: i64, step: i64) -> Price {
    let centered = i128::from(price) - i128::from(band_ticks) / 2;
    let step = i128::from(step);
    let aligned = centered.div_euclid(step) * step;
    Price::try_from(aligned).unwrap_or(price)
}

/// The base a version-2 snapshot did not record: the mid of the best bid and
/// best ask, or the only side's best price, centred in the band.
fn derive_base(bids: &[LevelSnapshot], asks: &[LevelSnapshot], band_ticks: i64) -> Price {
    let best_bid = bids.iter().map(|level| level.price).max();
    let best_ask = asks.iter().map(|level| level.price).min();
    let anchor = match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => {
            i64::try_from((i128::from(bid) + i128::from(ask)) / 2).unwrap_or(bid)
        }
        (Some(price), None) | (None, Some(price)) => price,
        (None, None) => 0,
    };
    target_base(anchor, band_ticks, STEP_TICKS)
}

fn check_level(level: &LevelSnapshot, capacity: usize) -> Result<(), SnapshotError> {
    // A snapshot's levels are the non-empty ones; an empty level would come
    // back as a phantom bit with no orders behind it.
    if level.head == EMPTY_SLOT {
        return Err(SnapshotError::BadLevel { price: level.price });
    }
    for slot in [level.head, level.tail] {
        if slot != EMPTY_SLOT && usize::try_from(slot).map_or(true, |slot| slot >= capacity) {
            return Err(SnapshotError::BadLevel { price: level.price });
        }
    }
    Ok(())
}

/// A small deterministic FNV-1a hasher.
struct Fnv(u64);

impl Fnv {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.write_bytes(&[value]);
    }

    fn write_u16(&mut self, value: u16) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_u32(&mut self, value: u32) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_i64(&mut self, value: i64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rest(book: &mut Book, side: Side, price: Price, qty: Qty, id: &str) {
        let cl_ord_id: ClientOrderId = id.parse().expect("client order id");
        let (_, slot) = book
            .allocate(1, side, price, qty, cl_ord_id, false, false)
            .expect("slot");
        book.insert_resting(slot);
    }

    #[test]
    fn a_trade_at_the_edge_moves_the_band_not_the_book() {
        // Sixteen l1 words: a 65,536-tick band, so the production recenter
        // step is one quarter of it and a shift fits these small arrays.
        let mut book = Book::new_with_band(1, 8, 16);
        rest(&mut book, Side::Sell, 10_000, 5, "S1");
        rest(&mut book, Side::Buy, 9_000, 3, "B1");
        rest(&mut book, Side::Buy, 39_000, 2, "B2");

        // The first limit order anchored the band: 10,000 - band/2 = -22,768,
        // aligned down to 4,096 -> -24,576.
        assert_eq!(book.base_price(), Some(-24_576));
        // 39,000 is inside the band but within RECENTER_MARGIN of its top.
        book.last_trade = Some(39_000);
        let view = book.view();
        let hash = book.hash();
        let levels: Vec<(Price, PriceLevel)> = book.levels_descending(Side::Buy).collect();

        book.recenter_if_needed();

        assert_eq!(book.base_price(), Some(0));
        assert_eq!(book.view(), view, "the recenter moved a level");
        assert_eq!(book.hash(), hash);
        assert_eq!(
            book.levels_descending(Side::Buy).collect::<Vec<_>>(),
            levels
        );
        assert!(book.bids.bitmap_is_consistent());
        assert!(book.asks.bitmap_is_consistent());
    }

    #[test]
    fn a_level_outside_the_band_stays_in_the_book() {
        let mut book = Book::new_with_band(1, 8, 16);
        rest(&mut book, Side::Sell, 10_000, 5, "S1");
        // The band is [-24,576, 40,960); this bid sits below it.
        rest(&mut book, Side::Buy, -30_000, 1, "B1");

        assert_eq!(book.bids.outliers_below(), 1);
        assert_eq!(book.best_opposite(Side::Sell), Some(-30_000));
        assert_eq!(
            book.level(Side::Buy, -30_000).map(|level| level.total_qty),
            Some(1)
        );
        let view = book.view();
        assert_eq!(view.bids.len(), 1);
        assert_eq!(view.bids[0].price, -30_000);
    }
}
