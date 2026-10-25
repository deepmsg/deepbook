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

use std::collections::BTreeMap;

use deepbook_types::{
    AccountId, EMPTY_ORDER_ID, OrderId, OrderState, Price, Qty, Seq, Side, SymbolId,
};

use crate::ids::{ClientOrderId, EMPTY_SLOT, MAX_SLOTS, next_generation, pack, slot_of};
use crate::snapshot::{BookSnapshot, LevelSnapshot, OrderSlotSnapshot, SnapshotError};

/// One resting order's hot fields.
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
}

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
#[derive(Clone, Copy, Debug)]
pub(crate) struct PriceLevel {
    pub total_qty: Qty,
    pub head: u32,
    pub tail: u32,
}

impl PriceLevel {
    fn empty() -> Self {
        Self {
            total_qty: 0,
            head: EMPTY_SLOT,
            tail: EMPTY_SLOT,
        }
    }
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
    pub bids: BTreeMap<Price, PriceLevel>,
    pub asks: BTreeMap<Price, PriceLevel>,
    pub orders: Vec<OrderHot>,
    pub cold: Vec<OrderCold>,
    generations: Vec<u8>,
    free_head: u32,
    pub last_trade: Option<Price>,
    pub md_seq: Seq,
}

impl Book {
    /// A book with `capacity` slab slots.
    pub fn new(symbol: SymbolId, capacity: usize) -> Self {
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
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders,
            cold,
            generations,
            free_head: 0,
            last_trade: None,
            md_seq: 0,
        }
    }

    /// Allocate a slot for a new order.
    pub fn allocate(
        &mut self,
        account: AccountId,
        side: Side,
        price: Price,
        qty: Qty,
        cl_ord_id: ClientOrderId,
        is_market: bool,
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
        match side {
            Side::Buy => self.asks.first_key_value().map(|(price, _)| *price),
            Side::Sell => self.bids.last_key_value().map(|(price, _)| *price),
        }
    }

    /// A level, if it exists.
    #[must_use]
    pub fn level(&self, side: Side, price: Price) -> Option<PriceLevel> {
        match side {
            Side::Buy => self.bids.get(&price).copied(),
            Side::Sell => self.asks.get(&price).copied(),
        }
    }

    /// Append a resting order to the tail of its level.
    pub fn insert_resting(&mut self, slot: u32) {
        let order = self.orders[slot as usize];
        let Book {
            bids, asks, orders, ..
        } = self;
        let level = match order.side {
            Side::Buy => bids.entry(order.price).or_insert_with(PriceLevel::empty),
            Side::Sell => asks.entry(order.price).or_insert_with(PriceLevel::empty),
        };
        orders[slot as usize].prev = level.tail;
        orders[slot as usize].next = EMPTY_SLOT;
        if level.tail != EMPTY_SLOT {
            orders[level.tail as usize].next = slot;
        } else {
            level.head = slot;
        }
        level.tail = slot;
        level.total_qty += order.leaves();
    }

    /// Remove an order from its level, leaving the slot allocated.
    pub fn unlink(&mut self, slot: u32) {
        let order = self.orders[slot as usize];
        let Book {
            orders, bids, asks, ..
        } = self;
        let level = match order.side {
            Side::Buy => bids.get_mut(&order.price),
            Side::Sell => asks.get_mut(&order.price),
        };
        let Some(level) = level else {
            return;
        };
        if order.prev != EMPTY_SLOT {
            orders[order.prev as usize].next = order.next;
        } else {
            level.head = order.next;
        }
        if order.next != EMPTY_SLOT {
            orders[order.next as usize].prev = order.prev;
        } else {
            level.tail = order.prev;
        }
        level.total_qty -= order.leaves();
        orders[slot as usize].prev = EMPTY_SLOT;
        orders[slot as usize].next = EMPTY_SLOT;

        if level.head == EMPTY_SLOT {
            let price = order.price;
            match order.side {
                Side::Buy => {
                    bids.remove(&price);
                }
                Side::Sell => {
                    asks.remove(&price);
                }
            }
        }
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

    fn side_view(&self, side: Side) -> Vec<LevelView> {
        let levels: Vec<(Price, PriceLevel)> = match side {
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .map(|(price, level)| (*price, *level))
                .collect(),
            Side::Sell => self
                .asks
                .iter()
                .map(|(price, level)| (*price, *level))
                .collect(),
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
        for (price, level) in self.bids.iter().rev() {
            hasher.write_u8(0);
            hasher.write_i64(*price);
            hasher.write_i64(level.total_qty);
            self.hash_level(*level, &mut hasher);
        }
        for (price, level) in &self.asks {
            hasher.write_u8(1);
            hasher.write_i64(*price);
            hasher.write_i64(level.total_qty);
            self.hash_level(*level, &mut hasher);
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
                .bids
                .iter()
                .map(|(price, level)| LevelSnapshot {
                    price: *price,
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(price, level)| LevelSnapshot {
                    price: *price,
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                })
                .collect(),
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

        let mut bids = BTreeMap::new();
        for level in &snapshot.bids {
            check_level(level, capacity)?;
            bids.insert(
                level.price,
                PriceLevel {
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                },
            );
        }
        let mut asks = BTreeMap::new();
        for level in &snapshot.asks {
            check_level(level, capacity)?;
            asks.insert(
                level.price,
                PriceLevel {
                    total_qty: level.total_qty,
                    head: level.head,
                    tail: level.tail,
                },
            );
        }

        Ok(Self {
            symbol,
            bids,
            asks,
            orders,
            cold,
            generations: snapshot.generations.clone(),
            free_head: snapshot.free_head,
            last_trade: snapshot.last_trade,
            md_seq: snapshot.md_seq,
        })
    }
}

fn check_level(level: &LevelSnapshot, capacity: usize) -> Result<(), SnapshotError> {
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
