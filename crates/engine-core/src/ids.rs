//! Order ids, client order ids and the bounded idempotency window.
//!
//! A slab slot is reused after an order reaches a terminal state, so the bare
//! slot is not a stable FIX `OrderID` (review P0-3). An [`OrderId`] is
//! `generation << 24 | slot`; lookups verify the generation. The linked lists
//! inside a price level store the bare slot index, not the packed id.
//!
//! [`IdMap`] is a fixed-capacity, open-addressed table with tombstones. It
//! makes the duplicate-ClOrdID check allocation-free after construction: an
//! insert either finds an occupied slot, a tombstone or an empty one. When the
//! window is full the oldest key is evicted.

use std::collections::VecDeque;
use std::fmt;

use deepbook_types::{AccountId, EMPTY_ORDER_ID, OrderId, Price, Qty, Side, SymbolId};

use crate::snapshot::{SeenEntry, SnapshotError};

/// Bits of the slab index in an [`OrderId`].
pub const SLOT_BITS: u32 = 24;
/// Mask of the slab index.
pub const SLOT_MASK: u32 = (1 << SLOT_BITS) - 1;
/// Most slab slots the packed id can address.
pub const MAX_SLOTS: usize = 1 << SLOT_BITS;
/// Highest generation. 255 is reserved so a packed id never equals
/// [`EMPTY_ORDER_ID`].
pub const MAX_GENERATION: u8 = 254;

/// The slot sentinel used by the free list and the level links.
pub const EMPTY_SLOT: u32 = u32::MAX;

/// Pack a generation and a slot.
#[must_use]
pub const fn pack(generation: u8, slot: u32) -> OrderId {
    debug_assert!(generation <= MAX_GENERATION);
    debug_assert!(slot <= SLOT_MASK);
    (generation as u32) << SLOT_BITS | slot
}

/// The slot inside a packed id.
#[must_use]
pub const fn slot_of(id: OrderId) -> u32 {
    id & SLOT_MASK
}

/// The generation inside a packed id.
#[must_use]
pub const fn generation_of(id: OrderId) -> u8 {
    (id >> SLOT_BITS) as u8
}

/// The next generation for a reused slot, wrapping inside `1..=254`.
#[must_use]
pub const fn next_generation(previous: u8) -> u8 {
    let next = previous.wrapping_add(1);
    if next == 0 || next > MAX_GENERATION {
        1
    } else {
        next
    }
}

/// A client order id, exactly the FIX 4.4 20-byte field.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ClientOrderId([u8; 20]);

impl ClientOrderId {
    /// The fixed width.
    pub const LENGTH: usize = 20;

    /// Build from bytes, NUL-padding to 20.
    ///
    /// # Errors
    ///
    /// The value is empty, longer than 20, or contains NUL.
    pub fn new(value: &[u8]) -> Result<Self, IdError> {
        if value.is_empty() {
            return Err(IdError::Empty);
        }
        if value.len() > Self::LENGTH {
            return Err(IdError::TooLong {
                length: value.len(),
            });
        }
        if value.contains(&0) {
            return Err(IdError::EmbeddedNul);
        }
        let mut bytes = [0_u8; Self::LENGTH];
        bytes[..value.len()].copy_from_slice(value);
        Ok(Self(bytes))
    }

    /// The raw padded bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LENGTH] {
        &self.0
    }

    /// Rebuild from the 20 padded bytes of a snapshot or wire record.
    #[must_use]
    pub const fn from_padded_bytes(bytes: [u8; Self::LENGTH]) -> Self {
        Self(bytes)
    }

    /// The bytes before the NUL padding.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let length = self
            .0
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(Self::LENGTH);
        &self.0[..length]
    }
}

impl std::str::FromStr for ClientOrderId {
    type Err = IdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.as_bytes())
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", String::from_utf8_lossy(self.as_slice()))
    }
}

impl fmt::Debug for ClientOrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", String::from_utf8_lossy(self.as_slice()))
    }
}

/// Why a client order id could not be built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdError {
    /// Empty.
    Empty,
    /// More than 20 bytes.
    TooLong {
        /// The length supplied.
        length: usize,
    },
    /// A NUL byte, which is the padding character.
    EmbeddedNul,
}

impl fmt::Display for IdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(formatter, "client order id is empty"),
            Self::TooLong { length } => {
                write!(formatter, "client order id is {length} bytes, max 20")
            }
            Self::EmbeddedNul => write!(formatter, "client order id contains NUL"),
        }
    }
}

impl std::error::Error for IdError {}

/// The state the idempotency window remembers for a client order id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeenState {
    /// The order is resting in the book.
    Active,
    /// Fully filled.
    Filled,
    /// Canceled (including an IOC's remainder).
    Canceled,
    /// Rejected before it rested.
    Rejected,
}

/// What the engine remembers after a client order id is used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seen {
    /// The packed order id.
    pub order_id: OrderId,
    /// The order's state.
    pub state: SeenState,
    /// The last known price (limit only).
    pub price: Option<Price>,
    /// The original quantity.
    pub qty: Qty,
    /// Filled quantity.
    pub filled: Qty,
    /// Filled notional, for the average price.
    pub notional: i128,
    /// Side.
    pub side: Side,
    /// Symbol.
    pub symbol: SymbolId,
}

impl Default for Seen {
    fn default() -> Self {
        Self {
            order_id: EMPTY_ORDER_ID,
            state: SeenState::Rejected,
            price: None,
            qty: 0,
            filled: 0,
            notional: 0,
            side: Side::Buy,
            symbol: 0,
        }
    }
}

impl Seen {
    /// Whether the order is terminal.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self.state, SeenState::Active)
    }
}

type Key = (AccountId, ClientOrderId);

#[derive(Clone, Copy, Default)]
struct Slot {
    key: Option<Key>,
    seen: Seen,
    occupied: bool,
    tombstone: bool,
}

/// The bounded `(account, ClOrdID)` window.
pub struct IdMap {
    slots: Vec<Slot>,
    order: VecDeque<Key>,
    len: usize,
    tombstones: usize,
    max_entries: usize,
}

impl IdMap {
    /// A table that remembers `capacity` client order ids.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(2);
        let slots_len = capacity.next_power_of_two();
        Self {
            slots: vec![Slot::default(); slots_len],
            order: VecDeque::with_capacity(capacity),
            len: 0,
            tombstones: 0,
            max_entries: capacity,
        }
    }

    /// How many entries are live.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the window is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Look up a client order id.
    #[must_use]
    pub fn get(&self, account: AccountId, cl_ord_id: ClientOrderId) -> Option<Seen> {
        let key = (account, cl_ord_id);
        let (found, _) = probe(&self.slots, &key);
        found.map(|index| self.slots[index].seen)
    }

    /// Insert or replace an entry. Returns the previous value, if any.
    ///
    /// When the window is full, the oldest key is evicted first.
    pub fn insert(
        &mut self,
        account: AccountId,
        cl_ord_id: ClientOrderId,
        seen: Seen,
    ) -> Option<Seen> {
        let key = (account, cl_ord_id);
        let (found, mut index) = probe(&self.slots, &key);
        if let Some(found) = found {
            let previous = self.slots[found].seen;
            self.slots[found].seen = seen;
            return Some(previous);
        }

        if self.len >= self.max_entries {
            self.evict_oldest();
        }
        if self.len + self.tombstones >= self.slots.len() {
            self.rehash();
            let (_, fresh) = probe(&self.slots, &key);
            index = fresh;
        }

        self.slots[index] = Slot {
            key: Some(key),
            seen,
            occupied: true,
            tombstone: false,
        };
        self.len += 1;
        self.order.push_back(key);
        None
    }

    /// Replace the remembered state of an existing key.
    pub fn set(&mut self, account: AccountId, cl_ord_id: ClientOrderId, seen: Seen) -> bool {
        let key = (account, cl_ord_id);
        let (found, _) = probe(&self.slots, &key);
        if let Some(index) = found {
            self.slots[index].seen = seen;
            true
        } else {
            false
        }
    }

    /// Mark one key terminal with its final snapshot.
    pub fn mark(&mut self, account: AccountId, cl_ord_id: ClientOrderId, seen: Seen) -> bool {
        self.set(account, cl_ord_id, seen)
    }

    /// The live entries, in insertion order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<SeenEntry> {
        self.order
            .iter()
            .filter_map(|(account, cl_ord_id)| {
                self.get(*account, *cl_ord_id).map(|seen| SeenEntry {
                    account: *account,
                    cl_ord_id: *cl_ord_id,
                    seen,
                })
            })
            .collect()
    }

    /// Rebuild a window from snapshot entries.
    ///
    /// # Errors
    ///
    /// More entries than the configured capacity.
    pub fn restore(entries: &[SeenEntry], capacity: usize) -> Result<Self, SnapshotError> {
        if entries.len() > capacity {
            return Err(SnapshotError::WindowTooSmall {
                entries: entries.len(),
                capacity,
            });
        }
        let mut map = Self::new(capacity);
        for entry in entries {
            map.insert(entry.account, entry.cl_ord_id, entry.seen);
        }
        Ok(map)
    }

    fn evict_oldest(&mut self) {
        while let Some(key) = self.order.pop_front() {
            let (found, _) = probe(&self.slots, &key);
            if let Some(index) = found {
                self.slots[index] = Slot {
                    key: None,
                    seen: Seen::default(),
                    occupied: false,
                    tombstone: true,
                };
                self.len -= 1;
                self.tombstones += 1;
                return;
            }
        }
    }

    fn rehash(&mut self) {
        let old = std::mem::take(&mut self.slots);
        let mut fresh = vec![Slot::default(); old.len()];
        let mut live = VecDeque::with_capacity(self.max_entries);
        let mut count = 0;
        for key in self.order.drain(..) {
            let (found, _) = probe(&old, &key);
            let Some(index) = found else {
                continue;
            };
            let seen = old[index].seen;
            let (_, insert) = probe(&fresh, &key);
            fresh[insert] = Slot {
                key: Some(key),
                seen,
                occupied: true,
                tombstone: false,
            };
            live.push_back(key);
            count += 1;
        }
        self.slots = fresh;
        self.order = live;
        self.len = count;
        self.tombstones = 0;
    }
}

fn probe(slots: &[Slot], key: &Key) -> (Option<usize>, usize) {
    let mask = slots.len() - 1;
    let mut index = hash(key) & mask;
    let mut first_tombstone = None;
    for _ in 0..slots.len() {
        let slot = &slots[index];
        if slot.occupied && slot.key == Some(*key) {
            return (Some(index), index);
        }
        if !slot.occupied && !slot.tombstone {
            return (None, first_tombstone.unwrap_or(index));
        }
        if slot.tombstone && first_tombstone.is_none() {
            first_tombstone = Some(index);
        }
        index = (index + 1) & mask;
    }
    (None, first_tombstone.unwrap_or(0))
}

fn hash(key: &Key) -> usize {
    // FNV-1a; deterministic and no randomized hasher in the state.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key.0.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for byte in key.1.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    usize::try_from(hash).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cl(value: &str) -> ClientOrderId {
        value.parse().expect("cl ord id")
    }

    #[test]
    fn a_client_order_id_pads_and_trims() {
        let id = cl("C-1");
        assert_eq!(20, id.as_bytes().len());
        assert_eq!(b"C-1", id.as_slice());
        assert_eq!(b"C-1", &id.as_bytes()[..3]);
        assert_eq!(0, id.as_bytes()[3]);
    }

    #[test]
    fn a_client_order_id_refuses_bad_input() {
        assert_eq!(Err(IdError::Empty), ClientOrderId::new(b""));
        assert!(matches!(
            ClientOrderId::new(&[b'x'; 21]),
            Err(IdError::TooLong { length: 21 })
        ));
        assert_eq!(Err(IdError::EmbeddedNul), ClientOrderId::new(b"A\0B"));
    }

    #[test]
    fn packing_keeps_the_generation_and_slot_apart() {
        let id = pack(7, 1234);
        assert_eq!(7, generation_of(id));
        assert_eq!(1234, slot_of(id));
        assert_ne!(EMPTY_ORDER_ID, pack(MAX_GENERATION, SLOT_MASK));
        assert_eq!(1, next_generation(0));
        assert_eq!(1, next_generation(MAX_GENERATION));
        assert_eq!(2, next_generation(1));
    }

    #[test]
    fn the_window_finds_and_updates_entries() {
        let mut map = IdMap::new(8);
        let seen = Seen {
            order_id: pack(1, 3),
            state: SeenState::Active,
            price: Some(100),
            qty: 10,
            filled: 0,
            notional: 0,
            side: Side::Buy,
            symbol: 1,
        };
        assert!(map.insert(9, cl("C-1"), seen).is_none());
        assert_eq!(Some(seen), map.get(9, cl("C-1")));
        let mut terminal = seen;
        terminal.state = SeenState::Canceled;
        assert!(map.mark(9, cl("C-1"), terminal));
        assert_eq!(Some(terminal), map.get(9, cl("C-1")));
        assert_eq!(1, map.len());
    }

    #[test]
    fn the_window_evicts_the_oldest_key() {
        let mut map = IdMap::new(3);
        for index in 0..3 {
            let id = cl(&format!("C-{index}"));
            let seen = Seen {
                order_id: pack(1, index),
                state: SeenState::Active,
                ..Seen::default()
            };
            map.insert(1, id, seen);
        }
        assert_eq!(3, map.len());
        let fourth = Seen {
            order_id: pack(1, 3),
            state: SeenState::Active,
            ..Seen::default()
        };
        map.insert(1, cl("C-3"), fourth);
        assert_eq!(3, map.len());
        assert!(map.get(1, cl("C-0")).is_none());
        assert!(map.get(1, cl("C-3")).is_some());
    }

    #[test]
    fn the_window_reuses_tombstones() {
        let mut map = IdMap::new(2);
        let seen = Seen::default();
        map.insert(1, cl("A"), seen);
        map.insert(1, cl("B"), seen);
        map.insert(1, cl("C"), seen); // evicts A
        assert_eq!(2, map.len());
        assert!(map.get(1, cl("A")).is_none());
        assert!(map.get(1, cl("B")).is_some());
        assert!(map.get(1, cl("C")).is_some());
    }
}
